package server

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Wrappers around the request and response bodies and the client connection, which bound
// reads and writes by timeouts, let drains interrupt them, and track what a retry may resend.

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

func (b *trackingBody) attempt() *attemptBody {
	return &attemptBody{body: b}
}

func (b *trackingBody) Close() error {
	return nil // The server closes the real body when the handler returns
}

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
	ctrl        *http.ResponseController
	upgraded    net.Conn // The hijacked connection of an upgrade (WebSocket), once there is one
	interrupted bool     // Deadlines are not extended any more
	done        bool
	mu          sync.Mutex // Protects upgraded, interrupted and done, and serializes ctrl calls with the handler's end
}

func (c *clientConn) setReadDeadline(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.done && !c.interrupted {
		c.ctrl.SetReadDeadline(t)
	}
}

func (c *clientConn) setWriteDeadline(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.done && !c.interrupted {
		c.ctrl.SetWriteDeadline(t)
	}
}

// interrupt makes blocked reads and writes on the client connection fail right away, also on a
// hijacked one, which the upgrade copy then closes. Later deadlines do not undo it.
func (c *clientConn) interrupt() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.interrupted = true
	if c.upgraded != nil {
		c.upgraded.SetDeadline(time.Now())
	} else if !c.done {
		c.ctrl.SetReadDeadline(time.Now())
		c.ctrl.SetWriteDeadline(time.Now())
	}
}

// hijack takes over the connection for an upgrade, keeping it so interrupt can reach it
func (c *clientConn) hijack() (net.Conn, *bufio.ReadWriter, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	conn, brw, err := c.ctrl.Hijack()
	if err == nil {
		c.upgraded = conn
	}
	return conn, brw, err
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

// Hijack is what the reverse proxy takes over an upgraded connection with
func (w *deadlineWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return w.conn.hijack()
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

// upgradedBody is the upstream side of an upgraded connection (WebSocket). Traffic either way
// keeps it open; nothing for the timeout cancels the request, like nginx's proxy_read_timeout. When
// the upstream ends it (ntfy closes a WebSocket that stopped answering pings), the request is
// canceled too: the upgrade copy would otherwise wait for the client side, which a vanished
// client never ends.
type upgradedBody struct {
	rwc     io.ReadWriteCloser
	timer   *time.Timer
	timeout time.Duration
	cancel  context.CancelCauseFunc
}

func (b *upgradedBody) Read(p []byte) (int, error) {
	n, err := b.rwc.Read(p)
	if n > 0 {
		b.timer.Reset(b.timeout)
	}
	if err != nil {
		b.cancel(errUpstreamClosed)
	}
	return n, err
}

func (b *upgradedBody) Write(p []byte) (int, error) {
	n, err := b.rwc.Write(p)
	if n > 0 {
		b.timer.Reset(b.timeout)
	}
	return n, err
}

func (b *upgradedBody) Close() error {
	b.timer.Stop()
	return b.rwc.Close()
}

// WriteTo copies the upstream side to the client with a small buffer: the reverse proxy's io.Copy
// would take 32 KiB, held for the whole WebSocket while it waits for the next message
func (b *upgradedBody) WriteTo(w io.Writer) (int64, error) {
	return copySmall(w, b, upgradedDownBufferSize)
}

// ReadFrom copies the client side to the upstream, like WriteTo; clients send small frames
// (pings, pongs; ntfy reads at most 64 bytes per message)
func (b *upgradedBody) ReadFrom(r io.Reader) (int64, error) {
	return copySmall(b, r, upgradedUpBufferSize)
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
