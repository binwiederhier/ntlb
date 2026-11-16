package server

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"heckel.io/ntlb/balancer"
	"heckel.io/ntlb/config"
	"heckel.io/ntlb/match"
)

var (
	// maxPendingConnsPerIP caps a client's connections that have not sent a request yet (a
	// variable for the tests)
	maxPendingConnsPerIP = 256
)

// connInfoKey is the request context key for the connection's *connInfo
type connInfoKey struct{}

// connTracker follows the HTTP/1 connections of a public listener, to account for the requests
// Go rejects before any handler runs: malformed or oversized ones, and ones cut off while their
// headers arrive (also by the header timeout). nginx logs those as 400s, and CrowdSec bans on
// them; here they go to the access logs and the response rules as 400s without an upstream.
type connTracker struct {
	conns   sync.Map                       // net.Conn -> *connInfo
	config  *atomic.Pointer[config.Config] // The client key, as the limits and bans count a client
	pending *pendingConns
	refused prometheus.Counter // Connections closed right away by the pending cap
	onBad   func(remoteAddr string)
}

// pendingConns counts each client's connections that have not sent a request yet, over all
// public listeners: one costs memory until the header timeout (HTTP/2: the idle timeout), and no
// rate limit sees it, so one client could hold any number. Once a request arrived, the limit rule
// counts the client instead. Legitimate clients hold few: a request follows the handshake right
// away, and browsers open only a handful of HTTP/2 connections ahead of time.
type pendingConns struct {
	counts map[string]int
	mu     sync.Mutex // Protects counts
}

// countingListener counts a public listener's client connections from accept to close. At the
// listener, not in ConnState: a hijacked connection (a stream, a WebSocket) is still open.
type countingListener struct {
	net.Listener
	metrics *metrics
}

// countedConn is an accepted connection, uncounted once on its first Close
type countedConn struct {
	net.Conn
	metrics *metrics
	once    sync.Once
}

// connInfo is one connection's state: reading a request (active) and whether a handler got it
type connInfo struct {
	active  atomic.Bool
	handled atomic.Bool
	pending *pendingConns
	key     string      // The client, for pending
	counted atomic.Bool // In pending, until the first request or the close
}

// connContext registers a new connection, as http.Server.ConnContext
func (t *connTracker) connContext(ctx context.Context, c net.Conn) context.Context {
	ap, _ := netip.ParseAddrPort(c.RemoteAddr().String()) // Always ip:port from a TCP listener
	key := t.config.Load().ClientKey
	info := &connInfo{
		pending: t.pending,
		key:     string(balancer.ClientKey(ap.Addr(), key.IPv4PrefixBits, key.IPv6PrefixBits)),
	}
	if t.pending.add(info.key) {
		info.counted.Store(true)
	} else {
		t.refused.Inc()
		c.Close() // Go then sees a closed connection, and drops it
	}
	t.conns.Store(c, info)
	return context.WithValue(ctx, connInfoKey{}, info)
}

// connState follows a connection, as http.Server.ConnState. Go turns a connection active once it
// read a byte of a request, also one it then rejects; a rejected request reaches no handler, and
// Go closes the connection.
func (t *connTracker) connState(c net.Conn, state http.ConnState) {
	v, ok := t.conns.Load(c)
	if !ok {
		return
	}
	info := v.(*connInfo)
	switch state {
	case http.StateActive:
		info.active.Store(true)
		info.handled.Store(false)
	case http.StateIdle:
		info.active.Store(false)
	case http.StateHijacked:
		t.conns.Delete(c)
		info.release()
	case http.StateClosed:
		t.conns.Delete(c)
		info.release()
		if info.active.Load() && !info.handled.Load() && !isHTTP2(c) {
			t.onBad(c.RemoteAddr().String())
		}
	}
}

func (l *countingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.metrics.connectionsAccepted.Inc()
	l.metrics.connections.Inc()
	return &countedConn{
		Conn:    c,
		metrics: l.metrics,
	}, nil
}

func (c *countedConn) Close() error {
	c.once.Do(c.metrics.connections.Dec)
	return c.Conn.Close()
}

// CloseWrite half-closes, which net/http does before closing a connection, so the client reads
// an early response (e.g. an error before the body was read) before the reset
func (c *countedConn) CloseWrite() error {
	if cw, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return nil // Nothing to half-close: Close follows
}

// ReadFrom keeps the TCP connection's (sendfile, splice) for static files and copies
func (c *countedConn) ReadFrom(r io.Reader) (int64, error) {
	if rf, ok := c.Conn.(io.ReaderFrom); ok {
		return rf.ReadFrom(r)
	}
	return io.Copy(struct{ io.Writer }{c.Conn}, r)
}

// WriteTo keeps the TCP connection's (splice)
func (c *countedConn) WriteTo(w io.Writer) (int64, error) {
	if wt, ok := c.Conn.(io.WriterTo); ok {
		return wt.WriteTo(w)
	}
	return io.Copy(w, struct{ io.Reader }{c.Conn})
}

// markHandled records that a handler got the connection's request
func markHandled(ctx context.Context) {
	if info, ok := ctx.Value(connInfoKey{}).(*connInfo); ok {
		info.handled.Store(true)
		info.release()
	}
}

// release takes the connection out of its client's pending count, once
func (i *connInfo) release() {
	if i.counted.CompareAndSwap(true, false) {
		i.pending.remove(i.key)
	}
}

// add counts a pending connection of a client, and reports whether it is within the cap
func (p *pendingConns) add(key string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.counts[key] >= maxPendingConnsPerIP {
		return false
	}
	p.counts[key]++
	return true
}

func (p *pendingConns) remove(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.counts[key] <= 1 {
		delete(p.counts, key)
	} else {
		p.counts[key]--
	}
}

// badRequest accounts for a request Go rejected before the handler, like nginx's own 400s: an
// access log line (request "-"), the response rules, and the metrics
func (s *Server) badRequest(remoteAddr string) {
	ap, _ := netip.ParseAddrPort(remoteAddr) // Always ip:port from a TCP listener
	ip := ap.Addr().Unmap().String()
	pr := &proxyRequest{
		kind: kindBadRequest,
	}
	env := &match.ResponseEnv{
		IP:     ip,
		Status: http.StatusBadRequest,
	}
	entry := &accessLogEntry{
		start:      time.Now(),
		remoteAddr: ip,
		request:    "-",
	}
	s.writeAccessLog(entry, pr, env)
	s.rules.CheckResponse(env)
	s.metrics.requests.WithLabelValues("", "", "400", kindBadRequest).Inc()
}

// isHTTP2 reports whether a connection speaks HTTP/2, where a connection without a request is
// normal (browsers open them ahead of time)
func isHTTP2(c net.Conn) bool {
	tc, ok := c.(*tls.Conn)
	return ok && tc.ConnectionState().NegotiatedProtocol == "h2"
}
