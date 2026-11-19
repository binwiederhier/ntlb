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
	"heckel.io/ntlb/accesslog"
	"heckel.io/ntlb/clientkey"
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

	// maxOpenConnsPerClient and maxOpenConnsPerNetwork cap a client's open connections, and its
	// network's (pendingIPv4PrefixBits, pendingIPv6PrefixBits), from accept to close, requests
	// sent or not, like nginx's limit_conn: a request that never ends (a stalled upload, a stream)
	// costs one limit token, so without them a few networks could hold maxClientConns and lock
	// everyone else out. ntfy.sh (lb-2, 2026-11-19, 53k connections): at most 38 per IPv4 address
	// or /24, 48 per IPv6 address, 79 per /48 (vars for the tests).
	maxOpenConnsPerClient  = 500
	maxOpenConnsPerNetwork = 2000
)

const (
	pendingIPv4PrefixBits = 24
	pendingIPv6PrefixBits = 48
)

// Reasons a connection is refused, ntlb_connections_refused_total's label
const (
	refusedClient      = "client"       // Pending
	refusedNetwork     = "network"      // Pending
	refusedClientOpen  = "client_open"  // Open
	refusedNetworkOpen = "network_open" // Open
	refusedTotal       = "total"
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
	pending *connCounts
	refused *prometheus.CounterVec // Connections closed right away by a pending cap, by reason
	onBad   func(remoteAddr string)
}

// connCounts counts connections by client and by network over all public listeners, within a
// cap for each. Two of them: the pending connections, which have not sent a request yet (one
// costs memory until the header timeout, HTTP/2: the idle timeout, and no rate limit sees it, so
// one client could hold any number; legitimate clients hold few, as a request follows the
// handshake right away and browsers open only a handful of HTTP/2 connections ahead of time),
// and all open ones, whose requests the limit rule sees, but only once each.
type connCounts struct {
	maxClient      int
	maxNetwork     int
	refusedClient  string // The refused reasons when a cap is reached
	refusedNetwork string
	counts         map[string]int // By client key
	networks       map[string]int // By network (pendingIPv4PrefixBits, pendingIPv6PrefixBits)
	total          int
	mu             sync.Mutex // Protects counts, networks and total
}

// countingListener counts a public listener's client connections from accept to close. At the
// listener, not in ConnState: a hijacked connection (a stream, a WebSocket) is still open.
type countingListener struct {
	net.Listener
	metrics  *metrics
	open     *atomic.Int64                  // Of all public listeners, for maxClientConns
	byClient *connCounts                    // The same by client and network, for maxOpenConnsPerClient and maxOpenConnsPerNetwork
	config   *atomic.Pointer[config.Config] // The client key
}

// countedConn is an accepted connection, uncounted once on its first Close
type countedConn struct {
	net.Conn
	metrics  *metrics
	open     *atomic.Int64
	byClient *connCounts
	key      string // The client, for byClient
	network  string // The client's network, for byClient
	once     sync.Once
}

// connInfo is one connection's state: reading a request (active) and whether a handler got it
type connInfo struct {
	active  atomic.Bool
	handled atomic.Bool
	pending *connCounts
	key     string      // The client, for pending
	network string      // The client's network, for pending
	counted atomic.Bool // In pending, until the first request or the close
}

// connContext registers a new connection, as http.Server.ConnContext
func (t *connTracker) connContext(ctx context.Context, c net.Conn) context.Context {
	remote := c.RemoteAddr().String() // Always ip:port from a TCP listener
	info := &connInfo{
		pending: t.pending,
		key:     string(clientKey(remote, t.config.Load().ClientKey)),
		network: string(clientkey.Of(clientkey.Parse(remote), pendingIPv4PrefixBits, pendingIPv6PrefixBits)),
	}
	if refused := t.pending.add(info.key, info.network); refused == "" {
		info.counted.Store(true)
	} else {
		t.refused.WithLabelValues(refused).Inc()
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
			l.metrics.connectionsRefused.WithLabelValues(refusedTotal).Inc()
			c.Close()
			continue
		}
		remote := c.RemoteAddr().String() // Always ip:port from a TCP listener
		key := string(clientKey(remote, l.config.Load().ClientKey))
		network := string(clientkey.Of(clientkey.Parse(remote), pendingIPv4PrefixBits, pendingIPv6PrefixBits))
		if refused := l.byClient.add(key, network); refused != "" {
			l.open.Add(-1)
			l.metrics.connectionsRefused.WithLabelValues(refused).Inc()
			c.Close()
			continue
		}
		l.metrics.connectionsAccepted.Inc()
		l.metrics.connections.Inc()
		return &countedConn{
			Conn:     c,
			metrics:  l.metrics,
			open:     l.open,
			byClient: l.byClient,
			key:      key,
			network:  network,
		}, nil
	}
}

func (c *countedConn) Close() error {
	c.once.Do(func() {
		c.metrics.connections.Dec()
		c.open.Add(-1)
		c.byClient.remove(c.key, c.network)
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

// newConnCounts creates counts with a cap per client and per network, and the refused reasons
// for each
func newConnCounts(maxClient, maxNetwork int, refusedClient, refusedNetwork string) *connCounts {
	return &connCounts{
		maxClient:      maxClient,
		maxNetwork:     maxNetwork,
		refusedClient:  refusedClient,
		refusedNetwork: refusedNetwork,
		counts:         make(map[string]int),
		networks:       make(map[string]int),
	}
}

// add counts a connection of a client and its network if both are within their caps; otherwise
// it returns the reason of the cap that refuses it
func (p *connCounts) add(key, network string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.counts[key] >= p.maxClient {
		return p.refusedClient
	} else if p.networks[network] >= p.maxNetwork {
		return p.refusedNetwork
	}
	p.counts[key]++
	p.networks[network]++
	p.total++
	return ""
}

func (p *connCounts) remove(key, network string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	decrement(p.counts, key)
	decrement(p.networks, network)
	p.total--
}

// count is the number of connections counted, for the metrics
func (p *connCounts) count() float64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return float64(p.total)
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
	entry := &accesslog.Entry{
		Start:      time.Now(),
		RemoteAddr: ip,
		Request:    "-",
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
