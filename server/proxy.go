package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"heckel.io/ntfy-lb/balancer"
	"heckel.io/ntfy-lb/config"
)

const (
	upstreamMaxIdleConnsPerHost = 256
	upstreamIdleConnTimeout     = 90 * time.Second

	// copyBufferSize is the buffer each stream holds while it waits for its next message. The
	// reverse proxy's default of 32 KiB would be half of the memory a stream costs; ntfy's
	// events are small, and larger bodies (attachments) just take more reads.
	copyBufferSize = 4 * 1024
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
	balancer    *balancer.Balancer
	reverse     *httputil.ReverseProxy
	transport   atomic.Pointer[http.Transport]
	dialer      atomic.Pointer[net.Dialer]
	readTimeout atomic.Int64 // time.Duration
}

// proxyRequest is the per-request state shared between ServeHTTP, the transport, the stream path
// and the access log. Cleanup is registered as finishers, run when the request ends: when the
// handler returns, or, for a stream the stream path detached from the handler, when it ends.
type proxyRequest struct {
	key            []byte
	session        *balancer.Session
	cancel         context.CancelCauseFunc
	upstream       string // Set once an upstream answered
	upstreamStatus int
	status         int   // Set by the stream path, which may bypass the ResponseWriter
	bytes          int64 // Body bytes written by the stream path
	detached       bool  // The stream path took over, and runs the finishers itself
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

func newProxy(b *balancer.Balancer, conf *config.Config) *proxy {
	p := &proxy{
		balancer: b,
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
	if p.balancer.Draining() && (p.balancer.Drained() || isSubscription(r)) {
		w.Header().Set("Connection", "close")
		http.Error(w, "load balancer is draining", http.StatusServiceUnavailable)
		return
	}
	pr := proxyRequestFromContext(r.Context()) // Set (and finished) by the access log, if any
	if pr == nil {
		pr = &proxyRequest{}
		defer pr.finishUnlessDetached()
	}
	parent := r.Context()
	if isStream(r) && r.ProtoMajor == 1 {
		parent = context.WithoutCancel(parent) // Detached from the handler, see serveStream
	}
	ctx, cancel := context.WithCancelCause(parent)
	pr.key, pr.cancel = clientKey(r.RemoteAddr), cancel
	pr.session = p.balancer.NewSession(cancel)
	pr.onFinish(func() {
		p.balancer.Close(pr.session)
		cancel(nil)
	})
	r = r.WithContext(withProxyRequest(ctx, pr))
	if isStream(r) {
		p.serveStream(w, r, pr)
		return
	}
	p.reverse.ServeHTTP(w, r)
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
		addr := u.NextAddress()
		out := req.Clone(req.Context())
		out.URL.Scheme, out.URL.Host = "http", addr
		if body != nil {
			out.Body = body
		}
		resp, err := transport.RoundTrip(out)
		if err != nil {
			if req.Context().Err() != nil {
				return nil, err // Client went away or the stream was drained
			}
			p.balancer.RecordFailure(u)
			if !last && retryable(err, req.Method, body) {
				slog.Debug("Upstream failed, trying the next one", "upstream", u.Name(), "error", err)
				continue
			}
			slog.Debug("Upstream failed", "upstream", u.Name(), "error", err)
			return nil, err
		}
		if resp.StatusCode == http.StatusServiceUnavailable && !last && !touched(body) {
			resp.Body.Close()
			continue
		}
		p.balancer.Attach(pr.session, u, addr)
		pr.upstream, pr.upstreamStatus = u.Name(), resp.StatusCode
		if resp.StatusCode != http.StatusSwitchingProtocols { // An upgraded body must stay an io.ReadWriteCloser
			timeout := time.Duration(p.readTimeout.Load())
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
	p.readTimeout.Store(int64(conf.UpstreamReadTimeout))
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

func clientKey(remoteAddr string) []byte {
	addrPort, err := netip.ParseAddrPort(remoteAddr)
	if err != nil {
		return []byte(remoteAddr)
	}
	return balancer.ClientKey(addrPort.Addr())
}

// isSubscription returns true for an ntfy subscription stream: GET /<topics>/json|sse|raw|ws,
// unless it only polls
func isSubscription(r *http.Request) bool {
	if r.Method != http.MethodGet {
		return false
	}
	i := strings.LastIndexByte(r.URL.Path, '/')
	if i <= 0 {
		return false
	}
	switch r.URL.Path[i+1:] {
	case "json", "sse", "raw", "ws":
	default:
		return false
	}
	switch strings.ToLower(r.URL.Query().Get("poll")) {
	case "1", "yes", "true":
		return false
	}
	return true
}

func retryable(err error, method string, body *trackingBody) bool {
	if touched(body) {
		return false
	}
	var dialErr *dialError
	if errors.As(err, &dialErr) {
		return true
	}
	return method == http.MethodGet || method == http.MethodHead || method == http.MethodOptions
}

func touched(body *trackingBody) bool {
	return body != nil && body.touched.Load()
}

func isTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
