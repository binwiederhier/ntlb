package server

import (
	"errors"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"strings"
)

// forwardedFor returns the request's X-Forwarded-For with the client IP appended, like nginx's
// $proxy_add_x_forwarded_for, which is what ntfy's behind-proxy expects
func forwardedFor(r *http.Request) string {
	clientIP, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		clientIP = r.RemoteAddr
	}
	if prior := r.Header.Values(headerXForwardedFor); len(prior) > 0 {
		return strings.Join(prior, ", ") + ", " + clientIP
	}
	return clientIP
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
