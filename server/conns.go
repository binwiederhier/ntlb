package server

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"sync"
	"sync/atomic"
	"syscall"
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

	// maxPendingConnsPerNetwork caps the pending connections of all clients of a network
	// (pendingIPv4PrefixBits, pendingIPv6PrefixBits), so that addresses do not multiply the cap
	// above: a /48 holds 65,536 client keys of /64 (a variable for the tests)
	maxPendingConnsPerNetwork = 8192

	// maxClientConns caps all open client connections of the public listeners, like nginx's
	// worker_connections: each stream also holds an upstream connection, and both fit in the
	// unit's LimitNOFILE (1M) with room for the logs and the health checks. Past it, new
	// connections are closed right away, rather than every upstream dial failing (a variable for
	// the tests).
	maxClientConns int64 = 400_000
)

const (
	pendingIPv4PrefixBits = 24
	pendingIPv6PrefixBits = 48
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
	counts   map[string]int // By client key
	networks map[string]int // By network (pendingIPv4PrefixBits, pendingIPv6PrefixBits)
	mu       sync.Mutex     // Protects counts and networks
}

// countingListener counts a public listener's client connections from accept to close. At the
// listener, not in ConnState: a hijacked connection (a stream, a WebSocket) is still open.
type countingListener struct {
	net.Listener
	metrics *metrics
	open    *atomic.Int64 // Of all public listeners, for maxClientConns
}

// countedConn is an accepted connection, uncounted once on its first Close
type countedConn struct {
	net.Conn
	metrics *metrics
	open    *atomic.Int64
	once    sync.Once
}

// connInfo is one connection's state: reading a request (active) and whether a handler got it
type connInfo struct {
	active  atomic.Bool
	handled atomic.Bool
	pending *pendingConns
	key     string      // The client, for pending
	network string      // The client's network, for pending
	counted atomic.Bool // In pending, until the first request or the close
}

// connContext registers a new connection, as http.Server.ConnContext
func (t *connTracker) connContext(ctx context.Context, c net.Conn) context.Context {
	ap, _ := netip.ParseAddrPort(c.RemoteAddr().String()) // Always ip:port from a TCP listener
	key := t.config.Load().ClientKey
	info := &connInfo{
		pending: t.pending,
		key:     string(balancer.ClientKey(ap.Addr(), key.IPv4PrefixBits, key.IPv6PrefixBits)),
		network: string(balancer.ClientKey(ap.Addr(), pendingIPv4PrefixBits, pendingIPv6PrefixBits)),
	}
	if t.pending.add(info.key, info.network) {
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

// Accept returns the next connection within maxClientConns; one past it is closed right away
func (l *countingListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		if l.open.Add(1) > maxClientConns {
			l.open.Add(-1)
			l.metrics.connectionsRefused.Inc()
			c.Close()
			continue
		}
		l.metrics.connectionsAccepted.Inc()
		l.metrics.connections.Inc()
		return &countedConn{
			Conn:    c,
			metrics: l.metrics,
			open:    l.open,
		}, nil
	}
}

func (c *countedConn) Close() error {
	c.once.Do(func() {
		c.metrics.connections.Dec()
		c.open.Add(-1)
	})
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

// SyscallConn keeps the TCP connection's, so the stream path can wait for a plain-HTTP client's
// bytes without a buffer (copyStream)
func (c *countedConn) SyscallConn() (syscall.RawConn, error) {
	if sc, ok := c.Conn.(syscall.Conn); ok {
		return sc.SyscallConn()
	}
	return nil, errors.ErrUnsupported
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
		i.pending.remove(i.key, i.network)
	}
}

// add counts a pending connection of a client and its network, and reports whether both are
// within their caps
func (p *pendingConns) add(key, network string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.counts[key] >= maxPendingConnsPerIP || p.networks[network] >= maxPendingConnsPerNetwork {
		return false
	}
	p.counts[key]++
	p.networks[network]++
	return true
}

func (p *pendingConns) remove(key, network string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	decrement(p.counts, key)
	decrement(p.networks, network)
}

// decrement counts down a key, deleting it at zero so the map does not keep every client
func decrement(counts map[string]int, key string) {
	if counts[key] <= 1 {
		delete(counts, key)
	} else {
		counts[key]--
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
