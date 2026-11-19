package server

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestListeners_HTTP2StalledConnectionIsClosed(t *testing.T) {
	// An HTTP/2 client that stops reading its TCP connection, with flow-control windows large
	// enough that the server never waits on them, blocks the connection's frame writer in the
	// kernel. Deadlines on its streams only queue resets behind that writer, so neither the read
	// timeout nor a drain ends the request; the connection's write timeout must
	defer func(timeout time.Duration) { clientSendTimeout = timeout }(clientSendTimeout)
	clientSendTimeout = 500 * time.Millisecond
	app1 := newTestUpstream(t, "app1")
	app1.on("/flood", func(w http.ResponseWriter, r *http.Request) {
		chunk := strings.Repeat("x", 64*1024)
		for r.Context().Err() == nil {
			if _, err := io.WriteString(w, chunk); err != nil {
				return
			}
			w.(http.Flusher).Flush()
		}
	})
	s := newTLSTestServer(t, "timeouts: {read: 500ms}", app1)
	stall := make(chan struct{})
	defer close(stall)
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
		ForceAttemptHTTP2: true,
		HTTP2: &http.HTTP2Config{
			MaxReceiveBufferPerConnection: 1 << 30,
			MaxReceiveBufferPerStream:     1 << 30,
		},
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			return &stallingConn{Conn: conn, after: 256 * 1024, stall: stall}, nil
		},
	}
	defer transport.CloseIdleConnections()
	resp, err := (&http.Client{Transport: transport}).Get("https://" + s.HTTPSAddr() + "/flood")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 2, resp.ProtoMajor)
	go io.Copy(io.Discard, resp.Body) // Reads until the connection stalls
	start := time.Now()
	for s.balancer.Status().Streams != 0 {
		require.Less(t, time.Since(start), 10*time.Second) // The timeout, plus up to 5s that TLS waits to say goodbye
		time.Sleep(20 * time.Millisecond)
	}
}

// stallingConn stops reading after the given number of bytes, until stall is closed
type stallingConn struct {
	net.Conn
	after int64
	read  atomic.Int64
	stall chan struct{}
}

func (c *stallingConn) Read(p []byte) (int, error) {
	if c.read.Load() >= c.after {
		<-c.stall
		return 0, net.ErrClosed
	}
	n, err := c.Conn.Read(p)
	c.read.Add(int64(n))
	return n, err
}

func TestListeners_HTTP2Limits(t *testing.T) {
	// nginx's http2_max_concurrent_streams (128), not Go's 250; and a ping when a connection sent
	// nothing for 2m, so a client that vanished is noticed even while its streams only receive
	h2 := newHTTPServer("test", http.NotFoundHandler()).HTTP2
	require.Equal(t, 128, h2.MaxConcurrentStreams)
	require.Equal(t, 2*time.Minute, h2.SendPingTimeout)
}

func TestListeners_SideStalledBodyIsCut(t *testing.T) {
	// A side listener (here health, which may face the internet) answers a request whose body
	// never comes, then Go waits for that body before the next request: the read timeout must
	// end the connection, or each one holds a socket and a goroutine forever
	t.Cleanup(func(d time.Duration) func() { return func() { sideReadTimeout = d } }(sideReadTimeout))
	sideReadTimeout = 500 * time.Millisecond
	s, _ := newTestServer(t, "listen-health: '127.0.0.1:0'", newTestUpstream(t, "app1"))
	c, err := net.Dial("tcp", s.HealthAddr())
	require.NoError(t, err)
	defer c.Close()
	_, err = io.WriteString(c, "GET /v1/ready HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 1\r\n\r\n")
	require.NoError(t, err)
	require.NoError(t, c.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, err = io.ReadAll(c) // The answer, then EOF once the server gives up on the body
	require.NoError(t, err, "the connection must be closed, not left waiting")
}

func TestListeners_SideConnectionsAreCapped(t *testing.T) {
	// A side listener takes at most maxSideConns connections at once; the next waits in the
	// backlog until one closes, so a flood of them cannot take the process's file descriptors
	t.Cleanup(func(n int) func() { return func() { maxSideConns = n } }(maxSideConns))
	maxSideConns = 2
	s, _ := newTestServer(t, "listen-health: '127.0.0.1:0'", newTestUpstream(t, "app1"))
	var held []net.Conn
	for range maxSideConns {
		c, err := net.Dial("tcp", s.HealthAddr())
		require.NoError(t, err)
		defer c.Close()
		held = append(held, c)
	}
	answered := make(chan struct{})
	go func() {
		defer close(answered)
		client := &http.Client{Timeout: 5 * time.Second}
		if resp, err := client.Get("http://" + s.HealthAddr() + "/v1/ready"); err == nil {
			resp.Body.Close()
		}
	}()
	select {
	case <-answered:
		t.Fatal("answered past the cap")
	case <-time.After(300 * time.Millisecond):
	}
	held[0].Close()
	select {
	case <-answered:
	case <-time.After(3 * time.Second):
		t.Fatal("not answered after a connection closed")
	}
}

func TestListeners_CappedListenerCloseEndsAccept(t *testing.T) {
	// Shutdown closes the listener; an Accept that waits for a slot must return then, or the
	// server's Serve never ends
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	capped := newCappedListener(ln, 1)
	c, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer c.Close()
	first, err := capped.Accept()
	require.NoError(t, err)
	defer first.Close()
	done := make(chan error, 1)
	go func() {
		_, err := capped.Accept() // Waits for the slot first holds
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, capped.Close())
	select {
	case err := <-done:
		require.ErrorIs(t, err, net.ErrClosed)
	case <-time.After(2 * time.Second):
		t.Fatal("Accept still waiting after Close")
	}
}
