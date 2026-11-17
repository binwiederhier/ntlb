package server

import (
	"bufio"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// sendRaw writes raw bytes to a listener and returns the first line of the answer, "" if none
func sendRaw(t testing.TB, addr, data string) string {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.Write([]byte(data))
	require.NoError(t, err)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	line, _ := bufio.NewReader(conn).ReadString('\n')
	return strings.TrimSpace(line)
}

// closedByServer reports whether the server closed conn (read within 500ms ends other than by
// the deadline)
func closedByServer(conn net.Conn) bool {
	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	_, err := conn.Read(make([]byte, 1))
	var netErr net.Error
	return !errors.As(err, &netErr) || !netErr.Timeout()
}

// dialFrom connects to addr from a loopback address of choice (any of 127.0.0.0/8 on Linux)
func dialFrom(t testing.TB, ip, addr string) net.Conn {
	dialer := &net.Dialer{
		LocalAddr: &net.TCPAddr{IP: net.ParseIP(ip)},
	}
	conn, err := dialer.Dial("tcp", addr)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

func TestConns_BadRequestsAreLoggedAndStruck(t *testing.T) {
	// Requests that Go rejects before the handler (garbage, oversized headers, a request cut off
	// while its headers arrive) are logged like nginx's own 400s and count for the response rules
	app1 := newTestUpstream(t, "app1")
	dir := t.TempDir()
	accessLog, badLog, banLog := filepath.Join(dir, "access.log"), filepath.Join(dir, "access_error_400.log"), filepath.Join(dir, "ban.log")
	s, _ := newTestServer(t, `
access-logs:
  - {file: `+badLog+`, match: 'status == 400 && upstream_status == 0'}
  - {file: `+accessLog+`}
bans: {file: `+banLog+`, threshold: 3}
rules: [{name: bad-requests, on: response, match: 'status == 400 && upstream_status == 0', strike: 1}]
`, app1)
	addr := s.HTTPAddr()
	require.Equal(t, "HTTP/1.1 400 Bad Request", sendRaw(t, addr, "\x16\x03\x01\x00{\x01\x00\x00w\r\n\r\n"))
	require.Equal(t, "HTTP/1.1 400 Bad Request", sendRaw(t, addr, "GET Kvissle&message=x y HTTP/1.1\r\nHost: a\r\n\r\n"))
	require.Equal(t, "HTTP/1.1 431 Request Header Fields Too Large", sendRaw(t, addr, "GET / HTTP/1.1\r\nHost: a\r\nX-Big: "+strings.Repeat("a", 70*1024)+"\r\n\r\n"))
	cut, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	io.WriteString(cut, "GET /mytopic/json HTTP/1.1\r\nHo")
	cut.(*net.TCPConn).CloseWrite() // Cut off: the server reads EOF right away, not after 30s
	cut.SetReadDeadline(time.Now().Add(5 * time.Second))
	io.Copy(io.Discard, cut) // Until the server closed it
	cut.Close()
	waitFor(t, func() bool {
		b, _ := os.ReadFile(badLog)
		return strings.Count(string(b), "\n") == 4
	})
	b, _ := os.ReadFile(badLog)
	require.Contains(t, string(b), `127.0.0.1 - - [`)
	require.Contains(t, string(b), `] "-" 400 0 "-" "-" http_host=- site=- request_time=`)
	waitFor(t, func() bool {
		b, _ := os.ReadFile(banLog)
		return strings.Contains(string(b), " 127.0.0.1 127.0.0.1/32 bad-requests\n")
	})
	_, metrics := adminDo(t, s, http.MethodGet, "/metrics", "")
	require.Contains(t, metrics, `ntlb_requests_total{code="400",kind="bad_request",site="",upstream=""} 4`)
}

func TestConns_GoodConnectionsAreNotCounted(t *testing.T) {
	// Kept-alive connections that end idle, connections that never send a byte, and HTTP/2
	// connections without a request are no bad requests
	app1 := newTestUpstream(t, "app1")
	dir := t.TempDir()
	badLog := filepath.Join(dir, "access_error_400.log")
	certFile, keyFile := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	writeTestCert(t, certFile, keyFile, "test")
	s, _ := newTestServer(t, "access-logs: [{file: "+badLog+"}]\nlisten-https: '127.0.0.3:0'\ncert-file: "+certFile+"\nkey-file: "+keyFile, app1)

	// Two requests on one connection, then it closes idle
	conn, err := net.Dial("tcp", s.HTTPAddr())
	require.NoError(t, err)
	r := bufio.NewReader(conn)
	for range 2 {
		_, err = conn.Write([]byte("GET /name HTTP/1.1\r\nHost: a\r\n\r\n"))
		require.NoError(t, err)
		resp, err := http.ReadResponse(r, nil)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close()
	}
	conn.Close()

	// Connected, nothing sent
	conn, err = net.Dial("tcp", s.HTTPAddr())
	require.NoError(t, err)
	conn.Close()

	// HTTP/2 connection without a request
	tlsConn, err := tls.Dial("tcp", s.HTTPSAddr(), &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"h2"},
	})
	require.NoError(t, err)
	require.Equal(t, "h2", tlsConn.ConnectionState().NegotiatedProtocol)
	tlsConn.Close()

	// A good request after all that is the only line
	status, _ := get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/name?last")
	require.Equal(t, http.StatusOK, status)
	waitFor(t, func() bool {
		b, _ := os.ReadFile(badLog)
		return strings.Contains(string(b), "/name?last")
	})
	time.Sleep(200 * time.Millisecond) // Room for a wrong line to show up
	b, _ := os.ReadFile(badLog)
	require.Equal(t, 3, strings.Count(string(b), "\n"), string(b)) // The two kept-alive requests and the last
	require.NotContains(t, string(b), `"-" 400`)
}

func TestConns_BadRequestOverTLS(t *testing.T) {
	// HTTPS with HTTP/1.1: garbage after the handshake counts too
	app1 := newTestUpstream(t, "app1")
	dir := t.TempDir()
	badLog := filepath.Join(dir, "access_error_400.log")
	certFile, keyFile := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	writeTestCert(t, certFile, keyFile, "test")
	s, _ := newTestServer(t, "access-logs: [{file: "+badLog+"}]\nlisten-https: '127.0.0.3:0'\ncert-file: "+certFile+"\nkey-file: "+keyFile, app1)
	conn, err := tls.Dial("tcp", s.HTTPSAddr(), &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"http/1.1"},
	})
	require.NoError(t, err)
	_, err = conn.Write([]byte("\x01\x02garbage\r\n\r\n"))
	require.NoError(t, err)
	line, _ := bufio.NewReader(conn).ReadString('\n')
	conn.Close()
	require.Equal(t, "HTTP/1.1 400 Bad Request", strings.TrimSpace(line))
	waitFor(t, func() bool {
		b, _ := os.ReadFile(badLog)
		return strings.Contains(string(b), `"-" 400 0`)
	})
}

func TestConns_CountedUntilClosed(t *testing.T) {
	// Like nginx's active connections: open client connections, whether idle, serving a request,
	// or hijacked by a stream or a WebSocket
	app1 := newTestUpstream(t, "app1")
	app1.on("/echo/ws", upgradeThen(func(conn net.Conn) {
		io.Copy(conn, conn)
	}))
	s, _ := newTestServer(t, "", app1)
	metric := func() string {
		_, metrics := adminDo(t, s, http.MethodGet, "/metrics", "")
		return metrics
	}

	// Idle, a stream, a WebSocket
	idle, err := net.Dial("tcp", s.HTTPAddr())
	require.NoError(t, err)
	stream, closeStream := openStream(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/bcast/json")
	require.NotNil(t, stream)
	ws, _ := openUpgrade(t, s.HTTPAddr(), "/echo/ws")
	waitFor(t, func() bool {
		return strings.Contains(metric(), "ntlb_connections 3\n")
	})
	require.Contains(t, metric(), "ntlb_connections_accepted_total 3\n")

	// A stream sees its client gone on the next write (ntfy's keepalive), like a write to any closed connection
	idle.Close()
	closeStream()
	ws.Close()
	waitFor(t, func() bool {
		app1.broadcast.send("message") // The first write after the close still succeeds, the next one fails
		return strings.Contains(metric(), "ntlb_connections 0\n")
	})
}

func TestConns_CountedConnKeepsTCPMethods(t *testing.T) {
	// net/http half-closes a connection before closing it (CloseWrite, so an early error response
	// is not lost to a reset), sendfile needs ReadFrom, and the stream path waits for a plain-HTTP
	// client's bytes in the netpoller (copyStream, SyscallConn): the counting wrapper keeps them
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err == nil {
			io.Copy(io.Discard, c)
			c.Close()
		}
	}()
	inner, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	var conn net.Conn = &countedConn{
		Conn:    inner,
		metrics: newMetrics(nil),
		open:    &atomic.Int64{},
	}
	defer conn.Close()
	_, ok := conn.(interface{ CloseWrite() error })
	require.True(t, ok, "CloseWrite")
	_, ok = conn.(io.ReaderFrom)
	require.True(t, ok, "ReaderFrom")
	_, ok = conn.(io.WriterTo)
	require.True(t, ok, "WriterTo")
	sc, ok := conn.(syscall.Conn)
	require.True(t, ok, "SyscallConn")
	_, err = sc.SyscallConn()
	require.NoError(t, err)
	require.NoError(t, conn.(interface{ CloseWrite() error }).CloseWrite())
}

func TestConns_PendingPerIPCapped(t *testing.T) {
	// A client may hold only so many connections that never sent a request (each costs memory
	// until the header timeout, and no rate limit sees them); one that sent a request, or closed,
	// makes room
	old := maxPendingConnsPerIP
	t.Cleanup(func() {
		maxPendingConnsPerIP = old // After the server's cleanup, which runs first
	})
	maxPendingConnsPerIP = 2
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "", app1)
	dial := func() net.Conn {
		conn, err := net.Dial("tcp", s.HTTPAddr())
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })
		return conn
	}
	c1, c2 := dial(), dial()
	require.True(t, closedByServer(dial()))
	require.False(t, closedByServer(c2))

	// A request on c1 makes room
	io.WriteString(c1, "GET /v1/health HTTP/1.1\r\nHost: x\r\n\r\n")
	resp, err := http.ReadResponse(bufio.NewReader(c1), nil)
	require.NoError(t, err)
	resp.Body.Close()
	require.False(t, closedByServer(dial()))
	require.True(t, closedByServer(dial()))

	// So does closing one
	c2.Close()
	waitFor(t, func() bool {
		return !closedByServer(dial())
	})
}

func TestConns_PendingCapUsesClientKey(t *testing.T) {
	// The cap counts a client as the limits and bans do: with a /24 client key, two addresses of
	// one /24 share it
	old := maxPendingConnsPerIP
	t.Cleanup(func() {
		maxPendingConnsPerIP = old // After the server's cleanup, which runs first
	})
	maxPendingConnsPerIP = 1
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "client-key: {ipv4-prefix-bits: 24}", app1)
	c1 := dialFrom(t, "127.0.0.1", s.HTTPAddr())
	require.True(t, closedByServer(dialFrom(t, "127.0.0.2", s.HTTPAddr())))
	require.False(t, closedByServer(c1))
	require.False(t, closedByServer(dialFrom(t, "127.0.1.1", s.HTTPAddr()))) // Another /24
}

func TestConns_PendingPerNetworkCapped(t *testing.T) {
	// Many clients of one network (a /24, or an IPv6 /48 of many /64 client keys) share a second,
	// larger cap, so that one attacker cannot multiply the per-client cap by its addresses
	defer func(old int) {
		maxPendingConnsPerNetwork = old
	}(maxPendingConnsPerNetwork)
	maxPendingConnsPerNetwork = 2
	s, _ := newTestServer(t, "", newTestUpstream(t, "app1"))
	c1 := dialFrom(t, "127.0.0.1", s.HTTPAddr())
	dialFrom(t, "127.0.0.2", s.HTTPAddr())
	require.True(t, closedByServer(dialFrom(t, "127.0.0.3", s.HTTPAddr())))
	require.False(t, closedByServer(c1))
	require.False(t, closedByServer(dialFrom(t, "127.0.1.1", s.HTTPAddr()))) // Another /24
}

func TestConns_TotalCapped(t *testing.T) {
	// All client connections together stay below the file descriptor limit, with room for the
	// upstream connections: past the cap, new ones are closed right away, like nginx's
	// worker_connections, rather than failing the upstream dials of everyone
	defer func(old int64) {
		maxClientConns = old
	}(maxClientConns)
	maxClientConns = 2
	s, _ := newTestServer(t, "", newTestUpstream(t, "app1"))
	c1 := dialFrom(t, "127.0.0.1", s.HTTPAddr())
	dialFrom(t, "127.0.1.1", s.HTTPAddr())
	require.True(t, closedByServer(dialFrom(t, "127.0.2.1", s.HTTPAddr())))
	c1.Close()
	waitFor(t, func() bool {
		return !closedByServer(dialFrom(t, "127.0.3.1", s.HTTPAddr()))
	})
}
