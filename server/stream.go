package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"syscall"
	"time"
)

// The stream path proxies ntfy subscriptions (GET /<topics>/json|sse|raw, not WebSockets) with
// as little memory per open stream as possible, since there are tens of thousands of them and
// they are idle almost all the time. Compared to httputil.ReverseProxy over http.Transport, it
// saves the transport's two goroutines and bufio buffers per connection, holds no copy buffer
// while waiting, and for HTTP/1.1 clients also drops the server's buffers and background reader:
//
//   - Upstream: a raw TCP connection per stream and an HTTP/1.0 request, so ntfy answers with a
//     plain body until it closes the connection (no chunking), which is copied as is.
//   - Waiting: the netpoller waits for upstream data (waitReadable) without a buffer; only then is
//     a pooled buffer taken for the read and the write.
//   - HTTP/1.1 clients: the connection is hijacked and the response written by hand, with
//     "Connection: close". The client is never read again (a subscription sends nothing more), so
//     a client that went away is noticed on the next write, at the latest ntfy's keepalive message
//     (45s), or by TCP keepalive. That leaves one goroutine per stream.
//   - HTTP/2 clients: written through the ResponseWriter; a client going away resets the stream,
//     which cancels the request context.
//
// Draining and the upstream read timeout close the upstream connection, which ends the copy.

// hopHeaders are the hop-by-hop headers, never forwarded (RFC 9110, section 7.6.1)
var hopHeaders = []string{
	"Connection",
	"Keep-Alive",
	"Proxy-Connection",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"TE",
	"Trailer",
	"Transfer-Encoding",
	"Upgrade",
}

const (
	streamHeaderBufferSize = 1024 // Reads the upstream's response headers; dropped after
)

// serveStream proxies a subscription on the stream path. pr.session is open; the request context
// is canceled to drain it. An HTTP/1.1 stream is detached: its connection is hijacked and copied
// by a new goroutine while the handler returns, so that the server's per-connection state (its
// bufio buffers, the parsed request) is freed rather than held for the life of the stream.
func (p *proxy) serveStream(w http.ResponseWriter, r *http.Request, pr *proxyRequest) {
	upstream, resp, buffered, err := p.connectStream(r, pr)
	if err != nil {
		p.handleError(w, r, err)
		return
	}
	stop := context.AfterFunc(r.Context(), func() {
		upstream.Close() // Drained, read timeout, or an HTTP/2 client went away
	})
	pr.onFinish(func() {
		stop()
		upstream.Close()
	})
	pr.status = resp.StatusCode
	timeout := time.Duration(p.readTimeout.Load())
	if r.ProtoMajor == 1 {
		head := responseHead(resp)
		client, _, err := http.NewResponseController(w).Hijack()
		if err == nil {
			pr.detached = true
			go copyStreamHTTP1(client, upstream, head, buffered, timeout, pr)
			return
		}
	}
	p.copyStreamHTTP2(w, upstream, resp, buffered, timeout, pr)
}

// connectStream sends the request to the best upstream for the client, with the same retries as
// the proxy's RoundTrip (no body, so any error before the response is safe to retry). It returns
// the connection, the response headers and the body bytes read along with them.
func (p *proxy) connectStream(r *http.Request, pr *proxyRequest) (net.Conn, *http.Response, []byte, error) {
	candidates := p.balancer.Pick(pr.key)
	if len(candidates) == 0 {
		return nil, nil, nil, errNoUpstream
	}
	for i, u := range candidates {
		last := i == len(candidates)-1
		addr := u.NextAddress()
		conn, resp, buffered, err := p.roundTripStream(r, addr)
		if err != nil {
			if r.Context().Err() != nil {
				return nil, nil, nil, err
			}
			p.balancer.RecordFailure(u)
			if !last {
				continue
			}
			return nil, nil, nil, err
		}
		if resp.StatusCode == http.StatusServiceUnavailable && !last {
			conn.Close()
			continue
		}
		p.balancer.Attach(pr.session, u, addr)
		pr.upstream, pr.upstreamStatus = u.Name(), resp.StatusCode
		return conn, resp, buffered, nil
	}
	return nil, nil, nil, errNoUpstream // Not reached: the last candidate always returns
}

// roundTripStream dials addr, writes the request as HTTP/1.0 and reads the response headers
func (p *proxy) roundTripStream(r *http.Request, addr string) (net.Conn, *http.Response, []byte, error) {
	conn, err := p.dialer.Load().DialContext(r.Context(), "tcp", addr)
	if err != nil {
		return nil, nil, nil, &dialError{err: err}
	}
	stop := context.AfterFunc(r.Context(), func() {
		conn.Close()
	})
	defer stop()
	conn.SetDeadline(time.Now().Add(time.Duration(p.readTimeout.Load()))) // Like the transport's ResponseHeaderTimeout
	header := r.Header.Clone()
	removeHopHeaders(header)
	header.Set("X-Forwarded-For", forwardedFor(r))
	bw := bufio.NewWriterSize(conn, streamHeaderBufferSize)
	fmt.Fprintf(bw, "GET %s HTTP/1.0\r\nHost: %s\r\n", r.URL.RequestURI(), r.Host)
	header.Write(bw)
	bw.WriteString("\r\n")
	if err := bw.Flush(); err != nil {
		conn.Close()
		return nil, nil, nil, err
	}
	br := bufio.NewReaderSize(conn, streamHeaderBufferSize)
	resp, err := http.ReadResponse(br, r)
	if err != nil {
		conn.Close()
		return nil, nil, nil, err
	}
	buffered, _ := br.Peek(br.Buffered())
	conn.SetDeadline(time.Time{})
	return conn, resp, append([]byte(nil), buffered...), nil
}

// responseHead renders the response line and headers for a hijacked HTTP/1.1 client. The body
// ends when the connection closes.
func responseHead(resp *http.Response) []byte {
	header := resp.Header.Clone()
	removeHopHeaders(header)
	head := &bytes.Buffer{}
	fmt.Fprintf(head, "HTTP/1.1 %d %s\r\n", resp.StatusCode, http.StatusText(resp.StatusCode))
	header.Write(head)
	head.WriteString("Connection: close\r\n\r\n")
	return head.Bytes()
}

// copyStreamHTTP1 writes the response to a hijacked client connection and copies the body until
// either side is done, then finishes the request
func copyStreamHTTP1(client net.Conn, upstream net.Conn, head, buffered []byte, timeout time.Duration, pr *proxyRequest) {
	defer pr.finish()
	defer client.Close()
	client.SetDeadline(time.Time{})
	write := func(b []byte) error {
		client.SetWriteDeadline(time.Now().Add(timeout)) // A client that takes nothing for this long is gone
		n, err := client.Write(b)
		pr.bytes += int64(n)
		return err
	}
	if _, err := client.Write(head); err != nil {
		return
	}
	if len(buffered) > 0 {
		if err := write(buffered); err != nil {
			return
		}
	}
	copyStream(upstream, timeout, write)
}

// copyStreamHTTP2 writes the response through the ResponseWriter, flushing every write
func (p *proxy) copyStreamHTTP2(w http.ResponseWriter, upstream net.Conn, resp *http.Response, buffered []byte, timeout time.Duration, pr *proxyRequest) {
	rc := http.NewResponseController(w)
	header := resp.Header.Clone()
	removeHopHeaders(header)
	for k, v := range header {
		w.Header()[k] = v
	}
	w.WriteHeader(resp.StatusCode)
	write := func(b []byte) error {
		rc.SetWriteDeadline(time.Now().Add(timeout))
		n, err := w.Write(b)
		pr.bytes += int64(n)
		if err != nil {
			return err
		}
		return rc.Flush()
	}
	if len(buffered) > 0 {
		if err := write(buffered); err != nil {
			return
		}
	} else if err := rc.Flush(); err != nil { // Send the headers right away: the stream is open
		return
	}
	copyStream(upstream, timeout, write)
}

// copyStream copies upstream to write until either fails. It waits for upstream data without a
// buffer and takes one from the pool only to read and write. timeout bounds each wait, like
// nginx's proxy_read_timeout.
func copyStream(upstream net.Conn, timeout time.Duration, write func([]byte) error) {
	sc, ok := upstream.(syscall.Conn)
	if !ok {
		return
	}
	raw, err := sc.SyscallConn()
	if err != nil {
		return
	}
	for {
		upstream.SetReadDeadline(time.Now().Add(timeout))
		if err := waitReadable(raw); err != nil {
			return
		}
		buf := streamBufferPool.Get()
		n, err := upstream.Read(buf)
		if n > 0 {
			if werr := write(buf[:n]); werr != nil {
				streamBufferPool.Put(buf)
				return
			}
		}
		streamBufferPool.Put(buf)
		if err != nil {
			return
		}
	}
}

// waitReadable blocks until the connection has data, EOF or an error, honoring its read
// deadline. It peeks one byte: the netpoller is edge-triggered, so it may only wait after the
// socket reported EAGAIN, or it could sleep on data that is already there.
func waitReadable(raw syscall.RawConn) error {
	var peek [1]byte
	return raw.Read(func(fd uintptr) bool {
		_, _, err := syscall.Recvfrom(int(fd), peek[:], syscall.MSG_PEEK|syscall.MSG_DONTWAIT)
		return !errors.Is(err, syscall.EAGAIN)
	})
}

func removeHopHeaders(h http.Header) {
	for _, field := range h.Values("Connection") {
		for _, name := range strings.Split(field, ",") {
			h.Del(strings.TrimSpace(name))
		}
	}
	for _, name := range hopHeaders {
		h.Del(name)
	}
}

// isStream returns true for requests that take the stream path: subscriptions, but not
// WebSockets, which the reverse proxy upgrades
func isStream(r *http.Request) bool {
	return isSubscription(r) && r.Header.Get("Upgrade") == ""
}

var (
	streamBufferPool = &bufferPool{}
)
