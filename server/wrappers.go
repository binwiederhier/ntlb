package server

import (
	"io"
	"net/http"
	"sync"
	"time"
)

// Wrappers around the request and response bodies and the client connection, which bound
// reads and writes by timeouts, let drains interrupt them, and track what a retry may resend.

// attemptBody is one attempt's handle on the request body. It never closes the body (the
// transport closes a request body on error; the server closes the real one when the handler
// returns). The transport may still read a failed attempt's body after RoundTrip returned (its
// writer runs on), so before the next attempt the old handle is retired: it reads nothing more,
// and retire tells whether it read anything, which rules out the retry. So a later attempt only
// ever follows ones that read nothing, and its own reads are all that count.
type attemptBody struct {
	rc      io.ReadCloser
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
	return a.rc.Read(p)
}

func (a *attemptBody) Close() error {
	return nil // The server closes the real body when the handler returns
}

// retire stops the handle from reading and returns true if it read any of the body
func (a *attemptBody) retire() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.retired = true
	return a.read
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
	b.conn.startRead(b.timeout)
	defer b.conn.stopRead()
	return b.rc.Read(p)
}

func (b *clientBody) Close() error {
	return b.rc.Close()
}

// clientConn bounds reads from and writes to the client connection for as long as the handler
// runs. A drain cancels requests from other goroutines, and the transport may read a request body
// after the handler returned; after that, the ResponseWriter must not be touched (an HTTP/2 one
// panics), so the handler's end and every deadline change are serialized.
//
// A read or a write that takes longer than its timeout interrupts the connection, by a timer of
// its own, instead of a deadline set before each one: on HTTP/2, a deadline change is a round trip
// to the connection's goroutine, for every message of tens of thousands of streams.
type clientConn struct {
	ctrl        *http.ResponseController
	readTimer   *time.Timer // Interrupts a read that takes too long, see startRead
	writeTimer  *time.Timer // Interrupts a write that takes too long, see startWrite
	interrupted bool        // Deadlines are not extended any more
	done        bool
	mu          sync.Mutex // Protects all fields but ctrl, and serializes ctrl calls with the handler's end
}

// startRead bounds the read that follows (until stopRead) by timeout
func (c *clientConn) startRead(timeout time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.readTimer = c.startNoLock(c.readTimer, timeout)
}

func (c *clientConn) stopRead() {
	c.mu.Lock()
	defer c.mu.Unlock()
	stopTimer(c.readTimer)
}

// startWrite bounds the write that follows (until stopWrite), flushes included, by timeout
func (c *clientConn) startWrite(timeout time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writeTimer = c.startNoLock(c.writeTimer, timeout)
}

func (c *clientConn) stopWrite() {
	c.mu.Lock()
	defer c.mu.Unlock()
	stopTimer(c.writeTimer)
}

// startNoLock arms timer (made on first use) to interrupt the connection after timeout
func (c *clientConn) startNoLock(timer *time.Timer, timeout time.Duration) *time.Timer {
	if c.done || c.interrupted {
		return timer
	} else if timer == nil {
		return time.AfterFunc(timeout, func() {
			safely("client timeout", c.interrupt)
		})
	}
	timer.Reset(timeout)
	return timer
}

// interrupt makes blocked reads and writes on the client connection fail right away. Later
// deadlines do not undo it.
func (c *clientConn) interrupt() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.interrupted = true
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
	stopTimer(c.readTimer)
	stopTimer(c.writeTimer)
}

// deadlineWriter bounds every write to the client by the timeout, so a client that stops reading
// is cut instead of holding the handler (and the upstream) forever. Unwrap keeps Flush reachable
// for the reverse proxy.
type deadlineWriter struct {
	http.ResponseWriter
	conn    *clientConn
	timeout time.Duration
}

func (w *deadlineWriter) Write(p []byte) (int, error) {
	w.conn.startWrite(w.timeout)
	defer w.conn.stopWrite()
	return w.ResponseWriter.Write(p)
}

// FlushError bounds a flush like a write: an HTTP/2 flush blocks on the client's window
func (w *deadlineWriter) FlushError() error {
	w.conn.startWrite(w.timeout)
	defer w.conn.stopWrite()
	return http.NewResponseController(w.ResponseWriter).Flush()
}

func (w *deadlineWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
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

// stopTimer stops a timer, if there is one
func stopTimer(timer *time.Timer) {
	if timer != nil {
		timer.Stop()
	}
}

// bufferPool hands out the copy buffers of the reverse proxy and the stream path
type bufferPool struct {
	buffers sync.Pool // *[]byte with a buffer
	holders sync.Pool // *[]byte without one, reused by Put instead of a new pointer each time
}

func (p *bufferPool) Get() []byte {
	h, ok := p.buffers.Get().(*[]byte)
	if !ok {
		return make([]byte, copyBufferSize)
	}
	b := *h
	*h = nil
	p.holders.Put(h)
	return b
}

func (p *bufferPool) Put(b []byte) {
	h, ok := p.holders.Get().(*[]byte)
	if !ok {
		h = new([]byte)
	}
	*h = b
	p.buffers.Put(h)
}
