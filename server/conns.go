package server

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"heckel.io/ntlb/match"
)

// connInfoKey is the request context key for the connection's *connInfo
type connInfoKey struct{}

// connTracker follows the HTTP/1 connections of a public listener, to account for the requests
// Go rejects before any handler runs: malformed or oversized ones, and ones cut off while their
// headers arrive (also by the header timeout). nginx logs those as 400s, and CrowdSec bans on
// them; here they go to the access logs and the response rules as 400s without an upstream.
type connTracker struct {
	conns sync.Map // net.Conn -> *connInfo
	onBad func(remoteAddr string)
}

// connInfo is one connection's state: reading a request (active) and whether a handler got it
type connInfo struct {
	active  atomic.Bool
	handled atomic.Bool
}

// connContext registers a new connection, as http.Server.ConnContext
func (t *connTracker) connContext(ctx context.Context, c net.Conn) context.Context {
	info := &connInfo{}
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
	case http.StateClosed:
		t.conns.Delete(c)
		if info.active.Load() && !info.handled.Load() && !isHTTP2(c) {
			t.onBad(c.RemoteAddr().String())
		}
	}
}

// markHandled records that a handler got the connection's request
func markHandled(ctx context.Context) {
	if info, ok := ctx.Value(connInfoKey{}).(*connInfo); ok {
		info.handled.Store(true)
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
