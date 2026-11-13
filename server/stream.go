package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"strconv"
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
var (
	hopHeaders = []string{
		headerConnection,
		"Keep-Alive",
		"Proxy-Connection",
		"Proxy-Authenticate",
		"Proxy-Authorization",
		"TE",
		"Trailer",
		"Transfer-Encoding",
		"Upgrade",
	}
)

// forwardedHeaders are dropped from client requests, like httputil.ReverseProxy's Rewrite does:
// only the load balancer says who the client is (X-Forwarded-For)
var (
	forwardedHeaders = []string{
		"Forwarded",
		"X-Forwarded-Host",
		"X-Forwarded-Proto",
	}
)

const (
	streamHeaderBufferSize = 1024      // Reads the upstream's response headers; dropped after
	maxUpstreamHeaderBytes = 64 * 1024 // Like the server's limit for client request headers
)

var (
	errUpstreamHeadersTooLarge = errors.New("upstream response headers too large")
)

// serveStream proxies a subscription on the stream path. pr.session is open; the request context
// is canceled to drain it. An HTTP/1.1 stream is detached: its connection is hijacked and copied
// by a new goroutine while the handler returns, so that the server's per-connection state (its
// bufio buffers, the parsed request) is freed rather than held for the life of the stream.
func (p *proxy) serveStream(w http.ResponseWriter, r *http.Request, pr *proxyRequest) {
	us, err := p.connectStream(r, pr)
	if err != nil {
		p.handleError(w, r, err)
		return
	}
	stop := context.AfterFunc(r.Context(), func() {
		safely("stream cancel", func() {
			us.conn.Close() // Drained, read timeout, or an HTTP/2 client went away
		})
	})
	pr.onFinish(func() {
		stop()
		us.conn.Close()
	})
	pr.status = us.resp.StatusCode
	if r.ProtoMajor == 1 {
		head := responseHead(us.resp)
		client, _, err := http.NewResponseController(w).Hijack()
		if err == nil {
			pr.detached = true
			us.resp = nil // Its headers are in head; a stream holds them for hours otherwise
			stopClient := context.AfterFunc(r.Context(), func() {
				safely("stream cancel", func() {
					client.Close() // A drain must not wait for a blocked write to the client
				})
			})
			go safely("stream copy", func() {
				defer stopClient()
				copyStreamHTTP1(client, us, head, pr)
			})
			return
		}
	}
	cc := &clientConn{
		ctrl: http.NewResponseController(w),
	}
	stopClient := context.AfterFunc(r.Context(), func() {
		safely("stream cancel", cc.interrupt) // A drain must not wait for a write blocked on the client's window
	})
	err = copyStreamHTTP2(w, us, cc, pr)
	stopClient()
	cc.end()
	if err != nil {
		panic(http.ErrAbortHandler) // Resets the stream: a cut must not look like a complete response
	}
}

// upstreamStream is an upstream connection on the stream path, after the response headers
type upstreamStream struct {
	conn     net.Conn
	resp     *http.Response // Dropped by a detached stream once its head is written
	body     io.Reader      // The framed body (chunked, Content-Length), if not raw
	raw      bool           // Close-delimited body: copy conn's bytes as they are (buffered first)
	buffered []byte         // Body bytes read along with the headers, if raw
}

// connectStream sends the request to the best upstream for the client, with the same retries as
// the proxy's RoundTrip (no body, so any error before the response is safe to retry), trying the
// other addresses of an upstream first if one does not accept the connection
func (p *proxy) connectStream(r *http.Request, pr *proxyRequest) (*upstreamStream, error) {
	send := func(_ *http.Transport, addr string) (*upstreamStream, int, func() bool, error) {
		us, err := p.roundTripStream(r, addr)
		if err != nil {
			return nil, 0, noBodySent, err
		}
		return us, us.resp.StatusCode, noBodySent, nil
	}
	drop := func(us *upstreamStream) {
		us.conn.Close()
	}
	return tryUpstreams(p, r.Context(), pr, r.Method, send, drop)
}

// noBodySent is a stream's answer to "was any of the request body sent": it has none
func noBodySent() bool {
	return true
}

// roundTripStream dials addr, writes the request as HTTP/1.0 and reads the response headers,
// skipping informational (1xx) responses and reading at most maxUpstreamHeaderBytes of headers
func (p *proxy) roundTripStream(r *http.Request, addr string) (*upstreamStream, error) {
	pr := proxyRequestFromContext(r.Context())
	conn, err := pr.conns.dialer.DialContext(r.Context(), "tcp", addr)
	if err != nil {
		return nil, &dialError{err: err}
	}
	stop := context.AfterFunc(r.Context(), func() {
		conn.Close()
	})
	defer stop()
	conn.SetDeadline(time.Now().Add(pr.readTimeout)) // Like the transport's ResponseHeaderTimeout
	header := r.Header.Clone()
	removeHopHeaders(header)
	for _, name := range forwardedHeaders {
		header.Del(name)
	}
	header.Set(headerXForwardedFor, forwardedFor(r))
	bw := bufio.NewWriterSize(conn, streamHeaderBufferSize)
	fmt.Fprintf(bw, "GET %s HTTP/1.0\r\nHost: %s\r\n", r.URL.RequestURI(), r.Host)
	header.Write(bw)
	bw.WriteString("\r\n")
	if err := bw.Flush(); err != nil {
		conn.Close()
		return nil, err
	}
	limited := &limitedReader{r: conn, n: maxUpstreamHeaderBytes}
	br := bufio.NewReaderSize(limited, streamHeaderBufferSize)
	var resp *http.Response
	for {
		resp, err = http.ReadResponse(br, r)
		if err != nil {
			conn.Close()
			return nil, err
		}
		if resp.StatusCode >= 200 || resp.StatusCode == http.StatusSwitchingProtocols {
			break
		}
	}
	limited.n = math.MaxInt64 // Headers done; the body is not limited
	conn.SetDeadline(time.Time{})
	resp.Request = nil // The client's request, which a stream must not keep
	us := &upstreamStream{
		conn: conn,
		resp: resp,
		raw:  resp.ContentLength < 0 && len(resp.TransferEncoding) == 0 && resp.Body != http.NoBody,
	}
	if us.raw {
		buffered, _ := br.Peek(br.Buffered())
		us.buffered = append([]byte(nil), buffered...)
		resp.Body = http.NoBody // It reads through br, which the raw copy does not need
	} else {
		us.body = resp.Body
	}
	return us, nil
}

// responseHead renders the response line and headers for a hijacked HTTP/1.1 client. The body
// ends when the connection closes.
func responseHead(resp *http.Response) []byte {
	header := resp.Header.Clone()
	removeHopHeaders(header)
	if resp.ContentLength >= 0 && len(resp.TransferEncoding) == 0 {
		header.Set("Content-Length", strconv.FormatInt(resp.ContentLength, 10))
	}
	head := &bytes.Buffer{}
	fmt.Fprintf(head, "HTTP/1.1 %d %s\r\n", resp.StatusCode, http.StatusText(resp.StatusCode))
	header.Write(head)
	head.WriteString("Connection: close\r\n\r\n")
	return head.Bytes()
}

// copyStreamHTTP1 writes the response to a hijacked client connection and copies the body until
// either side is done, then finishes the request
func copyStreamHTTP1(client net.Conn, us *upstreamStream, head []byte, pr *proxyRequest) {
	defer pr.finish()
	defer client.Close()
	client.SetDeadline(time.Time{})
	write := func(b []byte) error {
		client.SetWriteDeadline(time.Now().Add(pr.readTimeout)) // A client that takes nothing for this long is gone
		n, err := client.Write(b)
		pr.bytes += int64(n)
		return err
	}
	client.SetWriteDeadline(time.Now().Add(pr.readTimeout))
	if _, err := client.Write(head); err != nil {
		return
	}
	copyBody(us, pr.readTimeout, write)
}

// copyStreamHTTP2 writes the response through the ResponseWriter, flushing every write. It
// returns an error if the stream did not end cleanly (upstream cut, read timeout, drain).
func copyStreamHTTP2(w http.ResponseWriter, us *upstreamStream, cc *clientConn, pr *proxyRequest) error {
	rc := cc.ctrl
	header := us.resp.Header.Clone()
	removeHopHeaders(header)
	for k, v := range header {
		w.Header()[k] = v
	}
	w.WriteHeader(us.resp.StatusCode)
	if err := rc.Flush(); err != nil { // Send the headers right away: the stream is open
		return err
	}
	write := func(b []byte) error {
		cc.setWriteDeadline(time.Now().Add(pr.readTimeout))
		n, err := w.Write(b)
		pr.bytes += int64(n)
		if err != nil {
			return err
		}
		return rc.Flush()
	}
	return copyBody(us, pr.readTimeout, write)
}

// copyBody copies the upstream body to write: a close-delimited body byte for byte (see
// copyStream), any other by its framing (chunked, Content-Length), as such bodies are short.
// It returns nil only if the body ended cleanly.
func copyBody(us *upstreamStream, timeout time.Duration, write func([]byte) error) error {
	if !us.raw {
		buf := streamBufferPool.Get()
		defer streamBufferPool.Put(buf)
		us.conn.SetReadDeadline(time.Now().Add(timeout))
		for {
			n, err := us.body.Read(buf)
			if n > 0 {
				if werr := write(buf[:n]); werr != nil {
					return werr
				}
				us.conn.SetReadDeadline(time.Now().Add(timeout))
			}
			if errors.Is(err, io.EOF) {
				return nil
			} else if err != nil {
				return err
			}
		}
	}
	if len(us.buffered) > 0 {
		if err := write(us.buffered); err != nil {
			return err
		}
		us.buffered = nil
	}
	return copyStream(us.conn, timeout, write)
}

// copyStream copies upstream to write until either fails. It waits for upstream data without a
// buffer and takes one from the pool only to read and write. timeout bounds each wait, like
// nginx's proxy_read_timeout. It returns nil if the upstream closed the stream cleanly (EOF).
func copyStream(upstream net.Conn, timeout time.Duration, write func([]byte) error) error {
	sc, ok := upstream.(syscall.Conn)
	if !ok {
		return errors.New("upstream connection has no file descriptor")
	}
	raw, err := sc.SyscallConn()
	if err != nil {
		return err
	}
	for {
		upstream.SetReadDeadline(time.Now().Add(timeout))
		if err := waitReadable(raw); err != nil {
			return err
		}
		buf := streamBufferPool.Get()
		n, err := upstream.Read(buf)
		if n > 0 {
			if werr := write(buf[:n]); werr != nil {
				streamBufferPool.Put(buf)
				return werr
			}
		}
		streamBufferPool.Put(buf)
		if errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return err
		}
	}
}

// limitedReader reads at most n bytes from r, then fails; n can be raised as it goes
type limitedReader struct {
	r io.Reader
	n int64
}

func (l *limitedReader) Read(p []byte) (int, error) {
	if l.n <= 0 {
		return 0, errUpstreamHeadersTooLarge
	}
	if int64(len(p)) > l.n {
		p = p[:l.n]
	}
	n, err := l.r.Read(p)
	l.n -= int64(n)
	return n, err
}

// waitReadable blocks until the connection has data, EOF or an error, honoring its read
// deadline. It peeks one byte: the netpoller is edge-triggered, so it may only wait after the
// socket reported EAGAIN, or it could sleep on data that is already there. A socket error (e.g.
// a reset) is returned here: the peek consumes it, so the next read would only see EOF.
func waitReadable(raw syscall.RawConn) error {
	var peek [1]byte
	var sockErr error
	if err := raw.Read(func(fd uintptr) bool {
		_, _, err := syscall.Recvfrom(int(fd), peek[:], syscall.MSG_PEEK|syscall.MSG_DONTWAIT)
		if errors.Is(err, syscall.EAGAIN) {
			return false
		}
		sockErr = err
		return true
	}); err != nil {
		return err
	}
	if sockErr != nil {
		return os.NewSyscallError("recvfrom", sockErr)
	}
	return nil
}

func removeHopHeaders(h http.Header) {
	for _, field := range h.Values(headerConnection) {
		for _, name := range strings.Split(field, ",") {
			h.Del(strings.TrimSpace(name))
		}
	}
	for _, name := range hopHeaders {
		h.Del(name)
	}
}

var (
	streamBufferPool = &bufferPool{}
)
