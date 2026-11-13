package server

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	// testWebSocketRequest is a WebSocket handshake for /mytopic/ws, as browsers send it
	testWebSocketRequest = "GET /mytopic/ws HTTP/1.1\r\nHost: example.com\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: chat\r\n\r\n"

	// testWebSocketAccept is the upstream's answer to testWebSocketRequest
	testWebSocketAccept = "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\nSec-WebSocket-Protocol: chat\r\n\r\n"
)

// newWebSocketUpstream is a raw upstream that accepts every WebSocket handshake and then hands the
// connection (and the bytes read past the handshake) to then; it answers health checks
func newWebSocketUpstream(t testing.TB, name string, then func(conn net.Conn, head string, r *bufio.Reader)) *testUpstream {
	return newRawUpstream(t, name, func(conn net.Conn) {
		head, r := readRequestHead(conn)
		if answerReady(conn, head) {
			return
		}
		io.WriteString(conn, testWebSocketAccept)
		then(conn, head, r)
	})
}

// dialWebSocket sends request to addr and returns the connection, the reader past the response
// head, and the response
func dialWebSocket(t testing.TB, addr, request string) (net.Conn, *bufio.Reader, *http.Response) {
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	t.Cleanup(func() {
		conn.Close()
	})
	_, err = io.WriteString(conn, request)
	require.NoError(t, err)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	r := bufio.NewReader(conn)
	resp, err := http.ReadResponse(r, nil)
	require.NoError(t, err)
	return conn, r, resp
}

func TestWebSocket_Handshake(t *testing.T) {
	// The upstream gets the handshake as HTTP/1.1 with its upgrade headers and the client's IP;
	// the client gets the upstream's answer with its upgrade headers, not "Connection: close"
	heads := make(chan string, 1)
	app1 := newWebSocketUpstream(t, "app1", func(conn net.Conn, head string, r *bufio.Reader) {
		heads <- head
		io.Copy(conn, r) // Echo
	})
	s, _ := newTestServer(t, "", app1)
	conn, r, resp := dialWebSocket(t, s.HTTPAddr(), testWebSocketRequest)
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	require.Equal(t, "websocket", resp.Header.Get("Upgrade"))
	require.Equal(t, "Upgrade", resp.Header.Get("Connection"))
	require.Equal(t, "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=", resp.Header.Get("Sec-WebSocket-Accept"))
	require.Equal(t, "chat", resp.Header.Get("Sec-WebSocket-Protocol"))
	head := <-heads
	require.True(t, strings.HasPrefix(head, "GET /mytopic/ws HTTP/1.1\r\n"), head)
	for _, line := range []string{"Host: example.com", "Upgrade: websocket", "Connection: Upgrade", "Sec-Websocket-Key: dGhlIHNhbXBsZSBub25jZQ==", "Sec-Websocket-Version: 13", "X-Forwarded-For: 127.0.0.1"} {
		require.Contains(t, strings.ToLower(head), strings.ToLower(line+"\r\n"))
	}

	// Both ways, also more than any buffer
	message := strings.Repeat("x", 100*1024) + "\n"
	go io.WriteString(conn, message)
	line, err := r.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, message, line)
}

func TestWebSocket_NotUpgraded(t *testing.T) {
	// An upstream that refuses the upgrade (ntfy: 401, 403, 429) is relayed as it is, body included
	app1 := newRawUpstream(t, "app1", func(conn net.Conn) {
		head, _ := readRequestHead(conn)
		if answerReady(conn, head) {
			return
		}
		io.WriteString(conn, "HTTP/1.1 403 Forbidden\r\nContent-Type: application/json\r\nContent-Length: 27\r\n\r\n{\"code\":40301,\"http\":403}\n\n")
		io.Copy(io.Discard, conn)
	})
	s, _ := newTestServer(t, "", app1)
	_, r, resp := dialWebSocket(t, s.HTTPAddr(), testWebSocketRequest)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "{\"code\":40301,\"http\":403}\n\n", string(body))
	_, err = r.ReadByte()
	require.ErrorIs(t, err, io.EOF) // And the connection ends
}

func TestWebSocket_RetriesA503(t *testing.T) {
	// A draining node answers 503 before upgrading: the handshake goes to the next one
	app1 := newRawUpstream(t, "app1", func(conn net.Conn) {
		head, _ := readRequestHead(conn)
		if answerReady(conn, head) {
			return
		}
		io.WriteString(conn, "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n")
	})
	app2 := newWebSocketUpstream(t, "app2", func(conn net.Conn, head string, r *bufio.Reader) {
		io.Copy(conn, r)
	})
	s, _ := newTestServer(t, "", app1, app2)
	for i := range 10 { // Whichever the hash picks first
		conn, r, resp := dialWebSocket(t, s.HTTPAddr(), strings.Replace(testWebSocketRequest, "/mytopic/", fmt.Sprintf("/topic%d/", i), 1))
		require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
		fmt.Fprint(conn, "ping\n")
		line, err := r.ReadString('\n')
		require.NoError(t, err)
		require.Equal(t, "ping\n", line)
		conn.Close()
	}
}

func TestWebSocket_BytesAlongWithTheHandshake(t *testing.T) {
	// A client's first frame in the same packet as its handshake, and an upstream's first frame
	// in the same packet as its 101, both get through
	firsts := make(chan string, 1)
	app1 := newRawUpstream(t, "app1", func(conn net.Conn) {
		head, r := readRequestHead(conn)
		if answerReady(conn, head) {
			return
		}
		io.WriteString(conn, testWebSocketAccept+"hello from upstream\n")
		line, _ := r.ReadString('\n')
		firsts <- line
		io.Copy(io.Discard, r)
	})
	s, _ := newTestServer(t, "", app1)
	_, r, resp := dialWebSocket(t, s.HTTPAddr(), testWebSocketRequest+"hello from client\n")
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	line, err := r.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "hello from upstream\n", line)
	select {
	case line := <-firsts:
		require.Equal(t, "hello from client\n", line)
	case <-time.After(5 * time.Second):
		t.Fatal("upstream did not get the client's first bytes")
	}
}

func TestWebSocket_ClientClosesEndsUpstream(t *testing.T) {
	// A client that goes away ends the upstream connection, and the session
	ended := make(chan struct{})
	app1 := newWebSocketUpstream(t, "app1", func(conn net.Conn, head string, r *bufio.Reader) {
		io.Copy(io.Discard, r)
		close(ended)
	})
	s, _ := newTestServer(t, "", app1)
	conn, _, resp := dialWebSocket(t, s.HTTPAddr(), testWebSocketRequest)
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	waitFor(t, func() bool {
		return s.balancer.Status().Streams == 1
	})
	conn.Close()
	select {
	case <-ended:
	case <-time.After(5 * time.Second):
		t.Fatal("upstream connection still open")
	}
	waitFor(t, func() bool {
		return s.balancer.Status().Streams == 0
	})
}

func TestWebSocket_TrafficEitherWayKeepsItOpen(t *testing.T) {
	// The read timeout counts from the last bytes in either direction: a client that only sends
	// (pongs), or an upstream that only sends (messages, pings), keeps the WebSocket open
	got := make(chan string, 100)
	app1 := newWebSocketUpstream(t, "app1", func(conn net.Conn, head string, r *bufio.Reader) {
		if strings.Contains(head, "/talker/") {
			for range 6 {
				io.WriteString(conn, "message\n")
				time.Sleep(100 * time.Millisecond)
			}
			io.Copy(io.Discard, r)
			return
		}
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				return
			}
			got <- line
		}
	})
	s, _ := newTestServer(t, "timeouts: {read: 300ms}", app1)

	// Only the client sends
	conn, r, resp := dialWebSocket(t, s.HTTPAddr(), testWebSocketRequest)
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	for range 6 {
		time.Sleep(100 * time.Millisecond)
		_, err := io.WriteString(conn, "pong\n")
		require.NoError(t, err)
		require.Equal(t, "pong\n", <-got)
	}

	// Then nothing either way: cut after the timeout
	start := time.Now()
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, err := r.ReadByte()
	require.Error(t, err)
	require.NotErrorIs(t, err, os.ErrDeadlineExceeded)
	require.Less(t, time.Since(start), 3*time.Second)

	// Only the upstream sends
	_, r, resp = dialWebSocket(t, s.HTTPAddr(), strings.Replace(testWebSocketRequest, "/mytopic/", "/talker/", 1))
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	for range 6 {
		line, err := r.ReadString('\n')
		require.NoError(t, err)
		require.Equal(t, "message\n", line)
	}
}

func TestWebSocket_OverTLS(t *testing.T) {
	// A TLS client connection cannot be waited on without reading (its records are buffered in
	// crypto/tls), so it takes the buffered copy; both ways still work
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	writeTestCert(t, certFile, keyFile, "test")
	app1 := newWebSocketUpstream(t, "app1", func(conn net.Conn, head string, r *bufio.Reader) {
		io.Copy(conn, r)
	})
	s, _ := newTestServer(t, "listen-https: '127.0.0.3:0'\ncert-file: "+certFile+"\nkey-file: "+keyFile, app1)
	conn, err := tls.Dial("tcp", s.HTTPSAddr(), &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"http/1.1"},
	})
	require.NoError(t, err)
	defer conn.Close()
	_, err = io.WriteString(conn, testWebSocketRequest)
	require.NoError(t, err)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	r := bufio.NewReader(conn)
	resp, err := http.ReadResponse(r, nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	for range 3 {
		fmt.Fprint(conn, "ping\n")
		line, err := r.ReadString('\n')
		require.NoError(t, err)
		require.Equal(t, "ping\n", line)
	}
}

func TestWebSocket_MetricsAndAccessLog(t *testing.T) {
	// A WebSocket counts as one, with the 101 and the bytes sent to the client
	app1 := newWebSocketUpstream(t, "app1", func(conn net.Conn, head string, r *bufio.Reader) {
		io.WriteString(conn, "12345\n")
		io.Copy(io.Discard, r)
	})
	accessLog := filepath.Join(t.TempDir(), "access.log")
	s, _ := newTestServer(t, "access-logs: [{file: "+accessLog+"}]", app1)
	conn, r, resp := dialWebSocket(t, s.HTTPAddr(), testWebSocketRequest)
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	_, err := r.ReadString('\n')
	require.NoError(t, err)
	_, metrics := get(t, http.DefaultClient, "http://"+s.AdminAddr()+"/metrics")
	require.Contains(t, metrics, `ntlb_streams{protocol="websocket"} 1`)
	conn.Close()
	waitFor(t, func() bool {
		_, metrics := get(t, http.DefaultClient, "http://"+s.AdminAddr()+"/metrics")
		return strings.Contains(metrics, `ntlb_requests_total{code="101",kind="websocket",site="s",upstream="app1"} 1`)
	})
	waitFor(t, func() bool { // The bytes after the 101, like nginx's $body_bytes_sent
		line, _ := readFileString(accessLog)
		return strings.Contains(line, `"GET /mytopic/ws HTTP/1.1" 101 6 `)
	})
}

func TestWebSocket_MemoryPerWebSocket(t *testing.T) {
	// The live heap and goroutines an open WebSocket holds, test side included (two connections
	// and the upstream's goroutine, all small): one copy per direction, no buffer while waiting
	skipUnderRace(t)
	app1 := newRawUpstream(t, "app1", func(conn net.Conn) {
		head := readHeadBytewise(conn)
		if answerReady(conn, head) {
			return
		}
		io.WriteString(conn, testWebSocketAccept+"start\n")
		conn.Read(make([]byte, 1)) // Until the load balancer closes the WebSocket
	})
	s, _ := newTestServer(t, "", app1)
	heap, goroutines := memoryPerConn(t, 1000, func() net.Conn {
		conn, err := net.Dial("tcp", s.HTTPAddr())
		require.NoError(t, err)
		io.WriteString(conn, testWebSocketRequest)
		for !strings.HasSuffix(readHeadBytewise(conn), "start\n") {
		}
		return conn
	})
	t.Logf("per WebSocket: %.0f bytes of heap, %.2f goroutines", heap, goroutines)
	require.LessOrEqual(t, heap, float64(maxHeapPerWebSocket))
	require.LessOrEqual(t, goroutines, 3.05) // A copy per direction, and the upstream's
}

// maxHeapPerWebSocket is TestWebSocket_MemoryPerWebSocket's budget, test side included
const maxHeapPerWebSocket = 8 * 1024
