package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"heckel.io/ntlb/balancer"
	"heckel.io/ntlb/config"
	"heckel.io/ntlb/match"
)

const (
	upstreamMaxIdleConnsPerHost = 256
	upstreamIdleConnTimeout     = 90 * time.Second

	// copyBufferSize is the buffer each stream holds while it waits for its next message. The
	// reverse proxy's default of 32 KiB would be half of the memory a stream costs; ntfy's
	// events are small, and larger bodies (attachments) just take more reads.
	copyBufferSize = 4 * 1024

	// upgradedDownBufferSize and upgradedUpBufferSize are the copy buffers of an upgraded connection
	// (WebSocket), upstream to client and back
	upgradedDownBufferSize = 4 * 1024
	upgradedUpBufferSize   = 1024

	statusClientClosedRequest = 499 // nginx's status for a request that ended before a response
)

// Headers the proxy sets or reads
const (
	headerConnection      = "Connection"
	headerXForwardedFor   = "X-Forwarded-For"
	headerAcceptEncoding  = "Accept-Encoding"
	headerAcceptRanges    = "Accept-Ranges"
	headerContentEncoding = "Content-Encoding"
	headerContentLength   = "Content-Length"
	headerContentType     = "Content-Type"
	headerETag            = "ETag"
	headerVary            = "Vary"
	encodingGzip          = "gzip"
	connectionClose       = "close"
)

var (
	// clientBodyTimeout bounds how long a request body may stall, like nginx's client_body_timeout
	// (a variable for the tests); upstream.timeouts.read bounds it too, if shorter
	clientBodyTimeout = 60 * time.Second
)

var (
	errNoUpstream          = errors.New("no upstream available")
	errUpstreamReadTimeout = errors.New("upstream read timeout")
	errUpstreamClosed      = errors.New("upstream closed the connection")
	errBodyRetired         = errors.New("request body handed to another attempt")
)

// proxyContextKey is the request context key for the *proxyRequest
type proxyContextKey struct{}

// proxy forwards requests to the upstream the balancer picks, streaming both bodies, like
// nginx with proxy_buffering and proxy_request_buffering off
type proxy struct {
	balancer *balancer.Balancer
	metrics  *metrics
	reverse  *httputil.ReverseProxy
	conns    atomic.Pointer[map[string]*upstreamConns] // Per pool, by balancer.PoolKey
	config   atomic.Pointer[config.Config]
	active   map[*proxyRequest]struct{}
	mu       sync.Mutex // Protects active
}

// upstreamConns are a pool's (a site's upstream or one of its named upstreams) connections to its
// servers, set up by its timeouts and keepalive
type upstreamConns struct {
	transport *http.Transport
	dialer    *net.Dialer
	timeouts  config.Timeouts
	keepalive config.Keepalive
}

// proxyRequest is the per-request state shared between ServeHTTP, the transport, the stream path
// and the access log. Cleanup is registered as finishers, run when the request ends: when the
// handler returns, or, for a stream the stream path detached from the handler, when it ends.
type proxyRequest struct {
	key            []byte
	kind           string // kindRequest, kindStream or kindWebSocket
	session        *balancer.Session
	conns          *upstreamConns // The pool's, or the request's own if a reload removed the pool meanwhile
	cancel         context.CancelCauseFunc
	env            *match.Env   // The request as expressions see it, if Server.observe made one
	site           string       // The site serving the request, "" if none does
	pool           string       // The site's upstream a proxy rule sent the request to, "" for default
	siteConf       *config.Site // Set by Server.observe; dropped once the request is under way
	errorPages     *config.ErrorPages
	rule           string // The rule that decided the request, if any
	upstream       string // Set once an upstream answered
	upstreamStatus int
	status         int   // Set by the stream path, which may bypass the ResponseWriter
	bytes          int64 // Body bytes written by the stream path
	readTimeout    time.Duration
	detached       bool // The stream path took over, and runs the finishers itself
	finishers      []func()
	finishOnce     sync.Once
}

func newProxy(b *balancer.Balancer, m *metrics, conf *config.Config) *proxy {
	p := &proxy{
		balancer: b,
		metrics:  m,
		active:   make(map[*proxyRequest]struct{}),
	}
	p.reverse = &httputil.ReverseProxy{
		Rewrite:       p.rewrite,
		Transport:     p,
		FlushInterval: -1, // Flush every write: ntfy streams messages
		ErrorHandler:  p.handleError,
		ErrorLog:      slog.NewLogLogger(slog.Default().Handler(), slog.LevelDebug), // Every drained or abandoned stream logs a copy error
		BufferPool:    &bufferPool{},
	}
	p.update(conf)
	return p
}

// ServeHTTP proxies one request as a session the balancer can drain. While the load balancer
// drains, it turns away new subscriptions: their clients would otherwise reconnect here, only to
// be drained again, so the closes would snowball towards the deadline. Publishes and other short
// requests are still served until the drain is done, so no message is lost.
func (p *proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conf := p.config.Load()
	pr := proxyRequestFromContext(r.Context()) // Set (and finished) by Server.observe
	env := pr.env
	site := pr.siteConf
	pr.siteConf = nil // A detached stream must not keep an old config alive
	if site == nil {
		pr.env, pr.kind = nil, kindRequest
		writeError(w, pr.errorPages, httpErrNoSite)
		return
	}
	pr.site = site.Name
	subscription := isSubscription(env, site.Streams)
	pr.env = nil // Its header() holds the whole request, which a detached stream must not keep
	upgrade := r.Header.Get("Upgrade") != ""
	// Only bodyless GETs take the stream path, which writes a GET; WebSockets, other methods and
	// requests with a body stay on the reverse proxy
	stream := subscription && r.Method == http.MethodGet && !upgrade && r.ContentLength == 0
	switch {
	case subscription && upgrade:
		pr.kind = kindWebSocket
	case subscription:
		pr.kind = kindStream
	default:
		pr.kind = kindRequest
	}
	if p.balancer.Draining() && (p.balancer.Drained() || subscription) {
		p.metrics.refused.Inc()
		w.Header().Set(headerConnection, connectionClose)
		writeError(w, pr.errorPages, httpErrDraining)
		return
	}
	parent := r.Context()
	if stream && r.ProtoMajor == 1 {
		parent = context.WithoutCancel(parent) // Detached from the handler, see serveStream
	}
	ctx, cancel := context.WithCancelCause(parent)
	pr.key, pr.cancel = clientKey(r.RemoteAddr, conf.ClientKey), cancel
	pool := site.UpstreamNamed(pr.pool)
	if pool == nil {
		// A reload's new rules met the site of the old config, which lacks the upstream they name
		pr.kind = kindRequest
		writeError(w, pr.errorPages, httpErrUpstreamMissing)
		return
	}
	pr.pool = pool.Name
	pr.readTimeout = pool.Timeouts.Read
	pr.conns = (*p.conns.Load())[balancer.PoolKey(site.Name, pool.Name)]
	if pr.conns == nil {
		// A reload removed the pool after the request was routed: its servers still drain (see
		// Balancer.Pick), through connections of the request's own
		pr.conns = newUpstreamConns(pool)
		pr.onFinish(pr.conns.transport.CloseIdleConnections)
	}
	pr.session = p.balancer.NewSession(cancel)
	if subscription {
		gauge := p.metrics.streams.WithLabelValues(streamProtocol(pr.kind, r.ProtoMajor))
		gauge.Inc()
		pr.onFinish(func() {
			gauge.Dec()
			switch cause := context.Cause(ctx); {
			case errors.Is(cause, balancer.ErrDrained):
				p.metrics.streamsClosed.WithLabelValues(closedDrained).Inc()
			case errors.Is(cause, errUpstreamReadTimeout):
				p.metrics.streamsClosed.WithLabelValues(closedReadTimeout).Inc()
			}
		})
	}
	pr.onFinish(func() {
		p.balancer.Close(pr.session)
		cancel(nil)
	})
	p.track(pr)
	r = r.WithContext(withProxyRequest(ctx, pr))
	if stream {
		p.serveStream(unwrapGzip(w), r, pr)
		return
	}

	// Bound reads from and writes to the client, and let a drain interrupt both
	cc := &clientConn{
		ctrl: http.NewResponseController(w),
	}
	defer cc.end() // Before the finishers cancel ctx: a finished request must not touch the connection
	if r.Body != nil && r.Body != http.NoBody {
		r.Body = &clientBody{
			rc:      r.Body,
			conn:    cc,
			timeout: min(clientBodyTimeout, pr.readTimeout),
		}
	}
	stop := context.AfterFunc(ctx, func() {
		safely("request cancel", cc.interrupt)
	})
	defer stop()
	p.reverse.ServeHTTP(&deadlineWriter{
		ResponseWriter: w,
		conn:           cc,
		timeout:        pr.readTimeout,
	}, r)
}

// RoundTrip sends the request to the best upstream for the client, and to the next ones as
// long as that is safe, mirroring nginx's "proxy_next_upstream error timeout http_503": a
// connection failure is retried for any request, since it never reached the node; any other
// error only for idempotent methods; a 503 (e.g. a draining node) only if no body was sent yet.
// A request whose body was partly sent is never resent, as there is no copy of it.
func (p *proxy) RoundTrip(req *http.Request) (*http.Response, error) {
	pr := proxyRequestFromContext(req.Context())
	var body *trackingBody
	if req.Body != nil && req.Body != http.NoBody {
		body = &trackingBody{rc: req.Body}
	}
	send := func(transport *http.Transport, addr string) (*http.Response, int, func() bool, error) {
		// A shallow copy per attempt, with its own URL: req is the reverse proxy's copy already,
		// and a transport does not change a request's headers (Clone copied them every time)
		out := req.WithContext(req.Context())
		target := *req.URL
		target.Scheme, target.Host = "http", addr
		out.URL = &target
		var attempt *attemptBody
		if body != nil {
			attempt = body.attempt()
			out.Body = attempt
		}
		untouched := func() bool {
			return attempt == nil || !attempt.retire()
		}
		resp, err := transport.RoundTrip(out)
		if err != nil {
			return nil, 0, untouched, err
		}
		return resp, resp.StatusCode, untouched, nil
	}
	drop := func(resp *http.Response) {
		resp.Body.Close()
	}
	resp, err := tryUpstreams(p, req.Context(), pr, req.Method, send, drop)
	if err != nil {
		return nil, err
	}
	timeout := pr.readTimeout
	timer := time.AfterFunc(timeout, func() {
		pr.cancel(errUpstreamReadTimeout)
	})
	if rwc, ok := resp.Body.(io.ReadWriteCloser); ok && resp.StatusCode == http.StatusSwitchingProtocols {
		resp.Body = &upgradedBody{ // Must stay an io.ReadWriteCloser
			rwc:     rwc,
			timer:   timer,
			timeout: timeout,
			cancel:  pr.cancel,
		}
	} else {
		resp.Body = &idleTimeoutBody{
			rc:      resp.Body,
			timer:   timer,
			timeout: timeout,
		}
	}
	return resp, nil
}

// tryUpstreams sends a request to the upstreams Pick returns for the client, best first, by the
// rules RoundTrip describes, trying an upstream's other addresses first if one does not accept
// the connection. send makes one attempt on an address and returns the answer, its status, and
// a function that reports whether none of the request body was sent (it also stops that
// attempt's reads); drop discards an answer that is retried (a 503). The session is attached to
// the answer it returns. Shared by the reverse proxy and the stream path.
func tryUpstreams[T any](p *proxy, ctx context.Context, pr *proxyRequest, method string, send func(transport *http.Transport, addr string) (T, int, func() bool, error), drop func(T)) (T, error) {
	var zero T
	candidates := p.balancer.Pick(pr.site, pr.pool, pr.key)
	if len(candidates) == 0 {
		return zero, errNoUpstream
	}
	for i, u := range candidates {
		last := i == len(candidates)-1
		for try := 0; ; try++ {
			addr := p.balancer.Reserve(pr.session, u)
			answer, status, untouched, err := send(pr.conns.transport, addr)
			if err != nil {
				fresh := untouched()
				if ctx.Err() != nil {
					return zero, err // Client went away or the stream was drained
				}
				p.balancer.RecordAddressFailure(u, addr)
				if isDialError(err) && fresh && try+1 < u.AddressCount() {
					slog.Debug("Upstream address failed, trying another address", "upstream", u.Name(), "addr", addr, "error", err)
					p.metrics.retries.WithLabelValues(retryConnect).Inc()
					continue // Another port of the same node: its clients stay where their limits are
				}
				if !last && fresh && retryable(err, method) {
					slog.Debug("Upstream failed, trying the next one", "upstream", u.Name(), "error", err)
					p.metrics.retries.WithLabelValues(retryReason(err)).Inc()
					break
				}
				slog.Debug("Upstream failed", "upstream", u.Name(), "error", err)
				return zero, err
			}
			if status == http.StatusServiceUnavailable && !last && untouched() {
				drop(answer)
				p.metrics.retries.WithLabelValues(retry503).Inc()
				break
			}
			p.balancer.Attach(pr.session, u, addr)
			pr.upstream, pr.upstreamStatus = u.Name(), status
			return answer, nil
		}
	}
	return zero, errNoUpstream // Not reached: the last candidate always returns
}

// track registers an open request, so Shutdown can end what the drain did not
func (p *proxy) track(pr *proxyRequest) {
	p.mu.Lock()
	p.active[pr] = struct{}{}
	p.mu.Unlock()
	pr.onFinish(func() {
		p.mu.Lock()
		delete(p.active, pr)
		p.mu.Unlock()
	})
}

// closeAll cancels every open request (as drained) and waits until all of them finished, their
// finishers (access log line, metrics) included, or ctx is done
func (p *proxy) closeAll(ctx context.Context) error {
	p.mu.Lock()
	for pr := range p.active {
		pr.cancel(balancer.ErrDrained)
	}
	p.mu.Unlock()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		p.mu.Lock()
		n := len(p.active)
		p.mu.Unlock()
		if n == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%d request(s) still open: %w", n, ctx.Err())
		case <-ticker.C:
		}
	}
}

// update applies a (re)loaded config to the upstream connections: a pool whose timeouts and
// keepalive did not change keeps its connections; otherwise open connections keep running on the
// old transport, and its idle ones are closed.
func (p *proxy) update(conf *config.Config) {
	var old map[string]*upstreamConns
	if prev := p.conns.Load(); prev != nil {
		old = *prev
	}
	conns := make(map[string]*upstreamConns, len(conf.Sites))
	for _, site := range conf.Sites {
		for _, pool := range site.AllUpstreams() {
			key := balancer.PoolKey(site.Name, pool.Name)
			if c, ok := old[key]; ok && c.timeouts == *pool.Timeouts && c.keepalive == *pool.Keepalive {
				conns[key] = c
			} else {
				conns[key] = newUpstreamConns(pool)
			}
		}
	}
	p.config.Store(conf)
	p.conns.Store(&conns)
	for name, c := range old {
		if conns[name] != c {
			c.transport.CloseIdleConnections()
		}
	}
}

func newUpstreamConns(u *config.Upstream) *upstreamConns {
	ka := u.Keepalive.KeepAliveConfig()
	dialer := &net.Dialer{
		Timeout:         u.Timeouts.Connect,
		KeepAliveConfig: ka,
	}
	if !ka.Enable {
		dialer.KeepAlive = -1
	}
	return &upstreamConns{
		transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				conn, err := dialer.DialContext(ctx, network, addr)
				if err != nil {
					return nil, &dialError{err: err}
				}
				return conn, nil
			},
			MaxIdleConnsPerHost:   upstreamMaxIdleConnsPerHost,
			IdleConnTimeout:       upstreamIdleConnTimeout,
			ResponseHeaderTimeout: u.Timeouts.Read,
			DisableCompression:    true, // Pass encodings through as they are
		},
		dialer:    dialer,
		timeouts:  *u.Timeouts,
		keepalive: *u.Keepalive,
	}
}

// rewrite keeps the client's Host and appends the client IP to X-Forwarded-For
func (p *proxy) rewrite(r *httputil.ProxyRequest) {
	r.Out.Host = r.In.Host
	r.Out.Header.Set(headerXForwardedFor, forwardedFor(r.In))
}

func (p *proxy) handleError(w http.ResponseWriter, r *http.Request, err error) {
	cause := context.Cause(r.Context())
	pages := proxyRequestFromContext(r.Context()).errorPages
	switch {
	case errors.Is(cause, balancer.ErrDrained):
		w.Header().Set(headerConnection, connectionClose)
		writeError(w, pages, httpErrDrained)
	case errors.Is(cause, errUpstreamReadTimeout), isTimeout(err):
		writeError(w, pages, httpErrUpstreamTimeout)
	case r.Context().Err() != nil:
		// Client went away, nobody to answer
	case errors.Is(err, errNoUpstream):
		writeError(w, pages, httpErrNoHealthyUpstream)
	default:
		writeError(w, pages, httpErrUpstreamFailed)
	}
}

// onFinish registers f to run when the request ends; finishers run in reverse order
func (pr *proxyRequest) onFinish(f func()) {
	pr.finishers = append(pr.finishers, f)
}

// finish runs the finishers, once
func (pr *proxyRequest) finish() {
	pr.finishOnce.Do(func() {
		for i := len(pr.finishers) - 1; i >= 0; i-- {
			pr.finishers[i]()
		}
	})
}

// finalStatus is the status as the client saw it: 101 for an upgrade (written on the hijacked
// connection), and 499 like nginx if the request ended before any response
func (pr *proxyRequest) finalStatus() int {
	switch {
	case pr.status != 0:
		return pr.status
	case pr.upstreamStatus == http.StatusSwitchingProtocols:
		return http.StatusSwitchingProtocols
	default:
		return statusClientClosedRequest
	}
}

// dialError marks a failure to connect: the request never reached that upstream
type dialError struct {
	err error
}

func (e *dialError) Error() string {
	return e.err.Error()
}

func (e *dialError) Unwrap() error {
	return e.err
}

func withProxyRequest(ctx context.Context, pr *proxyRequest) context.Context {
	return context.WithValue(ctx, proxyContextKey{}, pr)
}

func proxyRequestFromContext(ctx context.Context) *proxyRequest {
	pr, _ := ctx.Value(proxyContextKey{}).(*proxyRequest)
	return pr
}

func clientKey(remoteAddr string, key *config.ClientKey) []byte {
	addrPort, err := netip.ParseAddrPort(remoteAddr)
	if err != nil {
		return []byte(remoteAddr)
	}
	return balancer.ClientKey(addrPort.Addr(), key.IPv4PrefixBits, key.IPv6PrefixBits)
}

// isSubscription returns true for a long-lived stream, see config.Streams
func isSubscription(env *match.Env, streams *config.Streams) bool {
	return streams.Program.Match(env)
}

// retryable returns true if a request whose body was not touched may go to the next upstream
// after err: always if it never reached the upstream, else only if it is idempotent
func retryable(err error, method string) bool {
	if isDialError(err) {
		return true
	}
	return method == http.MethodGet || method == http.MethodHead || method == http.MethodOptions
}

func isDialError(err error) bool {
	var dialErr *dialError
	return errors.As(err, &dialErr)
}

func retryReason(err error) string {
	if isDialError(err) {
		return retryConnect
	}
	return retryError
}
