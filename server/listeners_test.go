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
