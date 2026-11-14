package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAttemptBody(t *testing.T) {
	// Each attempt reads the shared body through its own handle; a retired handle reads nothing,
	// so a late read by a failed attempt cannot eat what the next attempt needs
	shared := &trackingBody{rc: io.NopCloser(strings.NewReader("hello"))}
	first := shared.attempt()
	require.False(t, first.retire()) // Untouched: safe to retry
	n, err := first.Read(make([]byte, 5))
	require.Zero(t, n)
	require.ErrorIs(t, err, errBodyRetired)
	second := shared.attempt()
	buf := make([]byte, 2)
	n, err = second.Read(buf)
	require.NoError(t, err)
	require.Equal(t, "he", string(buf[:n]))
	require.True(t, second.retire()) // Touched: never retry
	require.True(t, touched(shared))
}

func TestClientConn_NoDeadlinePerWrite(t *testing.T) {
	// Writes and flushes are bounded without setting a deadline each time: on HTTP/2, every
	// deadline change is a round trip to the connection's goroutine
	w := newDeadlineRecorder()
	cc := &clientConn{
		ctrl: http.NewResponseController(w),
	}
	dw := &deadlineWriter{
		ResponseWriter: w,
		conn:           cc,
		timeout:        time.Minute,
	}
	for range 100 {
		_, err := dw.Write([]byte("x"))
		require.NoError(t, err)
		require.NoError(t, http.NewResponseController(dw).Flush())
	}
	cc.end()
	require.Equal(t, 100, w.flushes)
	require.Zero(t, w.writeDeadlines.Load())
}

func TestClientConn_NoDeadlinePerRead(t *testing.T) {
	// Like writes, request body reads are bounded without a deadline per read
	w := newDeadlineRecorder()
	cc := &clientConn{
		ctrl: http.NewResponseController(w),
	}
	body := &clientBody{
		rc:      io.NopCloser(strings.NewReader(strings.Repeat("x", 100))),
		conn:    cc,
		timeout: time.Minute,
	}
	data, err := io.ReadAll(iotest.OneByteReader(body))
	require.NoError(t, err)
	require.Len(t, data, 100)
	cc.end()
	require.Zero(t, w.readDeadlines.Load())
}

func TestClientConn_StuckWriteIsInterrupted(t *testing.T) {
	// A write that does not complete within the timeout is interrupted (the connection's deadline
	// set to the past, which fails it), while the time between writes does not count
	w := newDeadlineRecorder()
	cc := &clientConn{
		ctrl: http.NewResponseController(w),
	}
	dw := &deadlineWriter{
		ResponseWriter: w,
		conn:           cc,
		timeout:        100 * time.Millisecond,
	}
	_, err := dw.Write([]byte("x"))
	require.NoError(t, err)
	time.Sleep(300 * time.Millisecond) // Idle between writes: not cut
	_, err = dw.Write([]byte("x"))
	require.NoError(t, err)
	w.block.Store(true)
	start := time.Now()
	_, err = dw.Write([]byte("x"))
	require.ErrorIs(t, err, os.ErrDeadlineExceeded)
	require.Less(t, time.Since(start), 5*time.Second)
	cc.end()
}

// deadlineRecorder is a ResponseWriter that counts deadline changes; with block set, a write
// blocks until a deadline in the past is set
type deadlineRecorder struct {
	*httptest.ResponseRecorder
	readDeadlines  atomic.Int32
	writeDeadlines atomic.Int32
	flushes        int
	block          atomic.Bool
	expired        chan struct{}
	once           sync.Once
}

func newDeadlineRecorder() *deadlineRecorder {
	return &deadlineRecorder{
		ResponseRecorder: httptest.NewRecorder(),
		expired:          make(chan struct{}),
	}
}

func (w *deadlineRecorder) Write(p []byte) (int, error) {
	if w.block.Load() {
		select {
		case <-w.expired:
			return 0, os.ErrDeadlineExceeded
		case <-time.After(10 * time.Second):
			return 0, io.ErrNoProgress // Never interrupted
		}
	}
	return w.ResponseRecorder.Write(p)
}

func (w *deadlineRecorder) FlushError() error {
	w.flushes++
	return nil
}

func (w *deadlineRecorder) SetReadDeadline(t time.Time) error {
	w.readDeadlines.Add(1)
	w.expire(t)
	return nil
}

func (w *deadlineRecorder) SetWriteDeadline(t time.Time) error {
	w.writeDeadlines.Add(1)
	w.expire(t)
	return nil
}

func (w *deadlineRecorder) expire(t time.Time) {
	if !t.IsZero() && !t.After(time.Now()) {
		w.once.Do(func() {
			close(w.expired)
		})
	}
}
