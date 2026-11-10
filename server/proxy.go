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
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"heckel.io/ntfy-lb/balancer"
	"heckel.io/ntfy-lb/config"
	"heckel.io/ntfy-lb/match"
)

const (
	upstreamMaxIdleConnsPerHost = 256
	upstreamIdleConnTimeout     = 90 * time.Second

	// copyBufferSize is the buffer each stream holds while it waits for its next message. The
	// reverse proxy's default of 32 KiB would be half of the memory a stream costs; ntfy's
	// events are small, and larger bodies (attachments) just take more reads.
	copyBufferSize = 4 * 1024

	statusClientClosedRequest = 499 // nginx's status for a request that ended before a response
)

var (
	errNoUpstream          = errors.New("no upstream available")
	errUpstreamReadTimeout = errors.New("upstream read timeout")
)

// proxyContextKey is the request context key for the *proxyRequest
type proxyContextKey struct{}

// proxy forwards requests to the upstream the balancer picks, streaming both bodies, like
// nginx with proxy_buffering and proxy_request_buffering off
type proxy struct {
	balancer  *balancer.Balancer
	metrics   *metrics
	reverse   *httputil.ReverseProxy
	transport atomic.Pointer[http.Transport]
	dialer    atomic.Pointer[net.Dialer]
	config    atomic.Pointer[config.Config]
	active    map[*proxyRequest]struct{}
	mu        sync.Mutex // Protects active
}

// proxyRequest is the per-request state shared between ServeHTTP, the transport, the stream path
// and the access log. Cleanup is registered as finishers, run when the request ends: when the
// handler returns, or, for a stream the stream path detached from the handler, when it ends.
type proxyRequest struct {
	key            []byte
	kind           string // kindRequest, kindStream or kindWebSocket
	session        *balancer.Session
	cancel         context.CancelCauseFunc
	env            *match.Env // The request as expressions see it, if Server.observe made one
	rule           string     // The rule that decided the request, if any
	upstream       string     // Set once an upstream answered
	upstreamStatus int
	status         int   // Set by the stream path, which may bypass the ResponseWriter
	bytes          int64 // Body bytes written by the stream path
	readTimeout    time.Duration
	detached       bool // The stream path took over, and runs the finishers itself
	finishers      []func()
	finishOnce     sync.Once
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

// finishUnlessDetached is deferred by the handlers: it finishes the request unless the stream
// path detached it
func (pr *proxyRequest) finishUnlessDetached() {
	if !pr.detached {
		pr.finish()
	}
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

// trackingBody wraps a request body so a failed attempt neither closes it (the transport closes
// a request body on error) nor hides whether any of it was sent, which decides a retry
type trackingBody struct {
	rc      io.ReadCloser
	touched atomic.Bool
}

func (b *trackingBody) Read(p []byte) (int, error) {
	b.touched.Store(true)
	return b.rc.Read(p)
}

var errBodyRetired = errors.New("request body handed to another attempt")

// attemptBody is one attempt's handle on the shared request body. The transport may still read
// a failed attempt's body after RoundTrip returned (its writer runs on), so before the next
// attempt the old handle is retired: it reads nothing more, and retire tells whether it read
// anything, which rules out the retry.
type attemptBody struct {
	body    *trackingBody
	retired bool
	read    bool
	mu      sync.Mutex // Protects retired and read
}

func (b *trackingBody) attempt() *attemptBody {
	return &attemptBody{body: b}
}

func (a *attemptBody) Read(p []byte) (int, error) {
	a.mu.Lock()
	if a.retired {
		a.mu.Unlock()
		return 0, errBodyRetired
	}
	a.read = true
	a.mu.Unlock()
	return a.body.Read(p)
}

func (a *attemptBody) Close() error {
	return nil // The server closes the real body when the handler returns
}

// retire stops the handle from reading and returns true if the body was touched by any attempt
func (a *attemptBody) retire() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.retired = true
	return a.read || a.body.touched.Load()
}

// clientBody is the request body as read from the client: each read must make progress within
// the timeout (a client that stalls an upload is cut, like nginx's client_body_timeout), and a
// drain interrupts a blocked read (see ServeHTTP)
type clientBody struct {
	rc      io.ReadCloser
	conn    *clientConn
	timeout time.Duration
}

func (b *clientBody) Read(p []byte) (int, error) {
	b.conn.setReadDeadline(time.Now().Add(b.timeout))
	return b.rc.Read(p)
}

func (b *clientBody) Close() error {
	return b.rc.Close()
}

// clientConn sets deadlines on the client connection for as long as the handler runs. A drain
// cancels requests from other goroutines, and the transport may read a request body after the
// handler returned; after that, the ResponseWriter must not be touched (an HTTP/2 one panics),
// so the handler's end and every deadline change are serialized.
type clientConn struct {
	ctrl *http.ResponseController
	done bool
	mu   sync.Mutex // Protects done, and serializes ctrl calls with the handler's end
}

func (c *clientConn) setReadDeadline(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.done {
		c.ctrl.SetReadDeadline(t)
	}
}

func (c *clientConn) setWriteDeadline(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.done {
		c.ctrl.SetWriteDeadline(t)
	}
}

// interrupt makes blocked reads and writes on the client connection fail right away
func (c *clientConn) interrupt() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.done {
		c.ctrl.SetReadDeadline(time.Now())
		c.ctrl.SetWriteDeadline(time.Now())
	}
}

// end is called when the handler returns; from then on, nothing touches the connection
func (c *clientConn) end() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.done = true
}

// deadlineWriter bounds every write to the client by the timeout, so a client that stops reading
// is cut instead of holding the handler (and the upstream) forever. Unwrap keeps Flush and
// Hijack reachable for the reverse proxy.
type deadlineWriter struct {
	http.ResponseWriter
	conn    *clientConn
	timeout time.Duration
}

func (w *deadlineWriter) Write(p []byte) (int, error) {
	w.conn.setWriteDeadline(time.Now().Add(w.timeout))
	return w.ResponseWriter.Write(p)
}

func (w *deadlineWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (b *trackingBody) Close() error {
	return nil // The server closes the real body when the handler returns
}

// idleTimeoutBody cancels the request when the upstream sends nothing for the timeout, like
// nginx's proxy_read_timeout
type idleTimeoutBody struct {
	rc      io.ReadCloser
	timer   *time.Timer
	timeout time.Duration
}

func (b *idleTimeoutBody) Read(p []byte) (int, error) {
	n, err := b.rc.Read(p)
	if n > 0 {
		b.timer.Reset(b.timeout)
	}
	return n, err
}

func (b *idleTimeoutBody) Close() error {
	b.timer.Stop()
	return b.rc.Close()
}

// bufferPool hands out the reverse proxy's copy buffers
type bufferPool struct {
	pool sync.Pool
}

func (p *bufferPool) Get() []byte {
	if b, ok := p.pool.Get().(*[]byte); ok {
		return *b
	}
	return make([]byte, copyBufferSize)
}

func (p *bufferPool) Put(b []byte) {
	p.pool.Put(&b)
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
	pr := proxyRequestFromContext(r.Context()) // Set (and finished) by Server.observe, if any
	if pr == nil {
		pr = &proxyRequest{}
		defer pr.finishUnlessDetached()
	}
	env := pr.env
	if env == nil {
		env = match.NewEnv(r)
	}
	subscription := isSubscription(env, conf.Streams)
	pr.env = nil                                                                    // Its header() holds the whole request, which a detached stream must not keep
	stream := subscription && r.Header.Get("Upgrade") == "" && r.ContentLength == 0 // WebSockets and requests with a body stay on the reverse proxy
	switch {
	case stream:
		pr.kind = kindStream
	case subscription:
		pr.kind = kindWebSocket
	default:
		pr.kind = kindRequest
	}
	if p.balancer.Draining() && (p.balancer.Drained() || subscription) {
		p.metrics.refused.Inc()
		w.Header().Set("Connection", "close")
		http.Error(w, "load balancer is draining", http.StatusServiceUnavailable)
		return
	}
	parent := r.Context()
	if stream && r.ProtoMajor == 1 {
		parent = context.WithoutCancel(parent) // Detached from the handler, see serveStream
	}
	ctx, cancel := context.WithCancelCause(parent)
	pr.key, pr.cancel = clientKey(r.RemoteAddr, conf.ClientKey), cancel
	pr.readTimeout = conf.UpstreamReadTimeout
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
		p.serveStream(w, r, pr)
		return
	}

	// Bound reads from and writes to the client, and let a drain interrupt both
	cc := &clientConn{ctrl: http.NewResponseController(w)}
	defer cc.end() // Before the finishers cancel ctx: a finished request must not touch the connection
	if r.Body != nil && r.Body != http.NoBody {
		r.Body = &clientBody{rc: r.Body, conn: cc, timeout: conf.UpstreamReadTimeout}
	}
	stop := context.AfterFunc(ctx, func() {
		safely("request cancel", cc.interrupt)
	})
	defer stop()
	p.reverse.ServeHTTP(&deadlineWriter{ResponseWriter: w, conn: cc, timeout: conf.UpstreamReadTimeout}, r)
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

// RoundTrip sends the request to the best upstream for the client, and to the next ones as
// long as that is safe, mirroring nginx's "proxy_next_upstream error timeout http_503": a
// connection failure is retried for any request, since it never reached the node; any other
// error only for idempotent methods; a 503 (e.g. a draining node) only if no body was sent yet.
// A request whose body was partly sent is never resent, as there is no copy of it.
func (p *proxy) RoundTrip(req *http.Request) (*http.Response, error) {
	pr := proxyRequestFromContext(req.Context())
	candidates := p.balancer.Pick(pr.key)
	if len(candidates) == 0 {
		return nil, errNoUpstream
	}
	var body *trackingBody
	if req.Body != nil && req.Body != http.NoBody {
		body = &trackingBody{rc: req.Body}
	}
	transport := p.transport.Load()
	for i, u := range candidates {
		last := i == len(candidates)-1
		for try := 0; ; try++ {
			addr := p.balancer.Reserve(pr.session, u)
			out := req.Clone(req.Context())
			out.URL.Scheme, out.URL.Host = "http", addr
			var attempt *attemptBody
			if body != nil {
				attempt = body.attempt()
				out.Body = attempt
			}
			resp, err := transport.RoundTrip(out)
			if err != nil {
				untouched := attempt == nil || !attempt.retire()
				if req.Context().Err() != nil {
					return nil, err // Client went away or the stream was drained
				}
				p.balancer.RecordAddressFailure(u, addr)
				if isDialError(err) && untouched && try+1 < u.AddressCount() {
					slog.Debug("Upstream address failed, trying another address", "upstream", u.Name(), "addr", addr, "error", err)
					p.metrics.retries.WithLabelValues(retryConnect).Inc()
					continue // Another port of the same node: its clients stay where their limits are
				}
				if !last && untouched && retryable(err, req.Method) {
					slog.Debug("Upstream failed, trying the next one", "upstream", u.Name(), "error", err)
					p.metrics.retries.WithLabelValues(retryReason(err)).Inc()
					break
				}
				slog.Debug("Upstream failed", "upstream", u.Name(), "error", err)
				return nil, err
			}
			if resp.StatusCode == http.StatusServiceUnavailable && !last && (attempt == nil || !attempt.retire()) {
				resp.Body.Close()
				p.metrics.retries.WithLabelValues(retry503).Inc()
				break
			}
			p.balancer.Attach(pr.session, u, addr)
			pr.upstream, pr.upstreamStatus = u.Name(), resp.StatusCode
			if resp.StatusCode != http.StatusSwitchingProtocols { // An upgraded body must stay an io.ReadWriteCloser
				timeout := pr.readTimeout
				resp.Body = &idleTimeoutBody{
					rc:      resp.Body,
					timeout: timeout,
					timer: time.AfterFunc(timeout, func() {
						pr.cancel(errUpstreamReadTimeout)
					}),
				}
			}
			return resp, nil
		}
	}
	return nil, errNoUpstream // Not reached: the last candidate always returns
}

// update applies a (re)loaded config to the upstream connections. Open connections keep running
// on the old transport; its idle ones are closed.
func (p *proxy) update(conf *config.Config) {
	ka := conf.UpstreamKeepalive.KeepAliveConfig()
	dialer := &net.Dialer{
		Timeout:         conf.UpstreamConnectTimeout,
		KeepAliveConfig: ka,
	}
	if !ka.Enable {
		dialer.KeepAlive = -1
	}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := dialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, &dialError{err: err}
			}
			return conn, nil
		},
		MaxIdleConnsPerHost:   upstreamMaxIdleConnsPerHost,
		IdleConnTimeout:       upstreamIdleConnTimeout,
		ResponseHeaderTimeout: conf.UpstreamReadTimeout,
		DisableCompression:    true, // Pass encodings through as they are
	}
	p.config.Store(conf)
	p.dialer.Store(dialer)
	if old := p.transport.Swap(transport); old != nil {
		old.CloseIdleConnections()
	}
}

// rewrite keeps the client's Host and appends the client IP to X-Forwarded-For
func (p *proxy) rewrite(r *httputil.ProxyRequest) {
	r.Out.Host = r.In.Host
	r.Out.Header.Set("X-Forwarded-For", forwardedFor(r.In))
}

// forwardedFor returns the request's X-Forwarded-For with the client IP appended, like nginx's
// $proxy_add_x_forwarded_for, which is what ntfy's behind-proxy expects
func forwardedFor(r *http.Request) string {
	clientIP, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		clientIP = r.RemoteAddr
	}
	if prior := r.Header.Values("X-Forwarded-For"); len(prior) > 0 {
		return strings.Join(prior, ", ") + ", " + clientIP
	}
	return clientIP
}

func (p *proxy) handleError(w http.ResponseWriter, r *http.Request, err error) {
	cause := context.Cause(r.Context())
	switch {
	case errors.Is(cause, balancer.ErrDrained):
		w.Header().Set("Connection", "close")
		w.WriteHeader(http.StatusServiceUnavailable)
	case errors.Is(cause, errUpstreamReadTimeout), isTimeout(err):
		w.WriteHeader(http.StatusGatewayTimeout)
	case r.Context().Err() != nil:
		// Client went away, nobody to answer
	case errors.Is(err, errNoUpstream):
		w.WriteHeader(http.StatusServiceUnavailable)
	default:
		w.WriteHeader(http.StatusBadGateway)
	}
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

func touched(body *trackingBody) bool {
	return body != nil && body.touched.Load()
}

func isTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// safely runs f and contains a panic: f runs in a goroutine of our own (a detached stream, a
// cancel callback), where a panic would end the process, and with it every stream on this load
// balancer, which then all reconnect at once
func safely(where string, f func()) {
	defer func() {
		if v := recover(); v != nil && v != http.ErrAbortHandler {
			slog.Error("Recovered from panic", "where", where, "panic", v, "stack", string(debug.Stack()))
		}
	}()
	f()
}
