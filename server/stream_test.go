package server

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// readLine reads one line from a stream, failing after 3s
func readLine(t *testing.T, r *bufio.Reader) string {
	t.Helper()
	result := make(chan string, 1)
	go func() {
		line, err := r.ReadString('\n')
		if err != nil {
			result <- "error: " + err.Error()
			return
		}
		result <- line
	}()
	select {
	case line := <-result:
		return line
	case <-time.After(3 * time.Second):
		t.Fatal("no line within 3s")
		return ""
	}
}

func openSubscription(t *testing.T, client *http.Client, url string) (*http.Response, *bufio.Reader) {
	resp, err := client.Get(url)
	require.NoError(t, err)
	t.Cleanup(func() {
		resp.Body.Close()
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	r := bufio.NewReader(resp.Body)
	require.Equal(t, "start\n", readLine(t, r))
	return resp, r
}

func TestStream_HTTP1(t *testing.T) {
	// Subscriptions take the stream path: raw HTTP/1.0 to the upstream (a body without chunking
	// to copy as is) and a hijacked client connection, closed when the stream ends
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "", app1)
	req, _ := http.NewRequest(http.MethodGet, "http://"+s.HTTPAddr()+"/mytopic/json?since=all", nil)
	req.Host = "ntfy.example.com"
	resp, err := clientFrom("127.0.0.7").Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	r := bufio.NewReader(resp.Body)
	require.Equal(t, "start\n", readLine(t, r))
	require.Equal(t, "application/x-ndjson", resp.Header.Get("Content-Type"))
	require.True(t, resp.Close)
	require.Equal(t, "HTTP/1.0", app1.lastProto.Load())
	require.Equal(t, "host=ntfy.example.com xff=127.0.0.7", app1.lastHeaders.Load())
	app1.push <- "hello"
	require.Equal(t, "hello\n", readLine(t, r))
	app1.push <- "world"
	require.Equal(t, "world\n", readLine(t, r))
	require.Equal(t, 1, s.balancer.Status().Upstreams[0].Streams)

	// The upstream ending the stream ends it for the client
	app1.push <- testStreamEnd
	_, err = r.ReadString('\n')
	require.ErrorIs(t, err, io.EOF)
	waitFor(t, func() bool {
		return s.balancer.Status().Streams == 0
	})
}

func TestStream_HTTP2(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	writeTestCert(t, certFile, keyFile, "test")
	s, _ := newTestServer(t, fmt.Sprintf("listen-https: '127.0.0.3:0'\ncert-file: %s\nkey-file: %s", certFile, keyFile), app1)
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
			ForceAttemptHTTP2: true,
		},
	}
	resp, r := openSubscription(t, client, "https://"+s.HTTPSAddr()+"/mytopic/json")
	require.Equal(t, 2, resp.ProtoMajor)
	require.Equal(t, "HTTP/1.0", app1.lastProto.Load())
	app1.push <- "hello"
	require.Equal(t, "hello\n", readLine(t, r))

	// An HTTP/2 client going away is noticed right away (stream reset)
	resp.Body.Close()
	waitFor(t, func() bool {
		return s.balancer.Status().Streams == 0
	})
}

func TestStream_ClientGoneReleasesSession(t *testing.T) {
	// A hijacked HTTP/1.1 client is not read from, so it is found gone at the next write, at the
	// latest ntfy's keepalive message
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "", app1)
	resp, _ := openSubscription(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/mytopic/json")
	resp.Body.Close()
	waitFor(t, func() bool {
		select {
		case app1.push <- "keepalive":
		default:
		}
		return s.balancer.Status().Streams == 0
	})
}

func TestStream_DrainCuts(t *testing.T) {
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	s, _ := newTestServer(t, "", app1, app2)
	_, r := openSubscription(t, clientForUpstream(t, s, "app1"), "http://"+s.HTTPAddr()+"/mytopic/json")
	require.NoError(t, s.balancer.DrainUpstream("app1", 0))
	require.True(t, strings.HasPrefix(readLine(t, r), "error"))
	waitFor(t, func() bool {
		return s.balancer.Status().Streams == 0
	})
}

func TestStream_RetryOnConnectError(t *testing.T) {
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	s, _ := newTestServer(t, "", app1, app2)
	client := clientForUpstream(t, s, "app1")
	app1.server.Close()
	openSubscription(t, client, "http://"+s.HTTPAddr()+"/mytopic/json")
	require.Equal(t, "HTTP/1.0", app2.lastProto.Load())
	require.Equal(t, uint64(1), s.balancer.Status().Upstreams[0].Failures)
}

func TestStream_RetryOn503(t *testing.T) {
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	s, _ := newTestServer(t, "", app1, app2)
	client := clientForUpstream(t, s, "app1")
	app1.hits.Store(0)
	app2.hits.Store(0)
	status, body := get(t, client, "http://"+s.HTTPAddr()+"/mytopic/json?status=503")
	require.Equal(t, http.StatusServiceUnavailable, status)
	require.Equal(t, "status body", body)
	require.Equal(t, int64(1), app1.hits.Load())
	require.Equal(t, int64(1), app2.hits.Load())
}

func TestStream_ForwardsErrorStatus(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "", app1)
	status, body := get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/mytopic/json?status=429")
	require.Equal(t, http.StatusTooManyRequests, status)
	require.Equal(t, "status body", body)
	require.Equal(t, "HTTP/1.0", app1.lastProto.Load())
}

func TestStream_UpstreamReadTimeout(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "upstream-read-timeout: 300ms", app1)
	resp, err := http.Get("http://" + s.HTTPAddr() + "/silent/json")
	require.NoError(t, err)
	defer resp.Body.Close()
	start := time.Now()
	io.ReadAll(resp.Body)
	require.Less(t, time.Since(start), 3*time.Second)
	waitFor(t, func() bool {
		return s.balancer.Status().Streams == 0
	})
}

func TestStream_AccessLog(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	logFile := filepath.Join(t.TempDir(), "access.log")
	s, _ := newTestServer(t, "access-logs: [{file: "+logFile+"}]", app1)
	resp, r := openSubscription(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/mytopic/json")
	app1.push <- "hello"
	readLine(t, r)
	app1.push <- testStreamEnd
	io.ReadAll(resp.Body)
	waitFor(t, func() bool {
		b, _ := readFileString(logFile)
		return strings.Contains(b, `"GET /mytopic/json HTTP/1.1" 200 12 "-"`) && strings.Contains(b, "upstream=app1 upstream_status=200")
	})
}

func TestStream_ResponseWireFormat(t *testing.T) {
	// A hijacked HTTP/1.1 stream is close-delimited: no chunk sizes in the body, Connection: close
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "", app1)
	conn, err := net.Dial("tcp", s.HTTPAddr())
	require.NoError(t, err)
	defer conn.Close()
	io.WriteString(conn, "GET /mytopic/json HTTP/1.1\r\nHost: ntfy.example.com\r\n\r\n")
	app1.push <- `{"event":"message"}`
	app1.push <- testStreamEnd
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	raw, err := io.ReadAll(conn)
	require.NoError(t, err)
	head, body, ok := strings.Cut(string(raw), "\r\n\r\n")
	require.True(t, ok)
	require.True(t, strings.HasPrefix(head, "HTTP/1.1 200 OK\r\n"), head)
	require.Contains(t, head, "\r\nConnection: close")
	require.Contains(t, head, "\r\nContent-Type: application/x-ndjson")
	require.NotContains(t, head, "Transfer-Encoding")
	require.Equal(t, "start\n{\"event\":\"message\"}\n", body)
}

func TestStream_RequestWireFormat(t *testing.T) {
	// The upstream gets an HTTP/1.0 request with the client's target, Host and headers, plus
	// X-Forwarded-For, and nothing hop-by-hop
	heads := make(chan string, 1)
	app1 := newRawUpstream(t, "app1", func(conn net.Conn) {
		head, _ := readRequestHead(conn)
		if answerReady(conn, head) {
			return
		}
		heads <- head
		io.WriteString(conn, "HTTP/1.0 200 OK\r\nContent-Type: text/plain\r\n\r\nstart\n")
	})
	s, _ := newTestServer(t, "", app1)
	req, _ := http.NewRequest(http.MethodGet, "http://"+s.HTTPAddr()+"/mytopic/json?since=all&x=%20", nil)
	req.Host = "ntfy.example.com"
	req.Header.Set("Authorization", "Bearer tk_123")
	req.Header.Set("Connection", "keep-alive")
	resp, err := clientFrom("127.0.0.9").Do(req)
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, "start\n", string(body))
	head := <-heads
	require.True(t, strings.HasPrefix(head, "GET /mytopic/json?since=all&x=%20 HTTP/1.0\r\nHost: ntfy.example.com\r\n"), head)
	require.Contains(t, head, "\r\nAuthorization: Bearer tk_123\r\n")
	require.Contains(t, head, "\r\nX-Forwarded-For: 127.0.0.9\r\n")
	require.NotContains(t, head, "Connection")
	require.True(t, strings.HasSuffix(head, "\r\n\r\n"))
}

func TestStream_MalformedUpstreamResponse(t *testing.T) {
	app1 := newRawUpstream(t, "app1", func(conn net.Conn) {
		head, _ := readRequestHead(conn)
		if answerReady(conn, head) {
			return
		}
		io.WriteString(conn, "this is not http\r\n\r\n")
	})
	s, _ := newTestServer(t, "", app1)
	status, _ := get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/mytopic/json")
	require.Equal(t, http.StatusBadGateway, status)
	waitFor(t, func() bool {
		return s.balancer.Status().Streams == 0
	})
}

func TestStream_UpstreamClosesBeforeAnswering(t *testing.T) {
	// A subscription is a GET, so a node that drops it unanswered is skipped
	app1 := newRawUpstream(t, "app1", func(conn net.Conn) {
		head, _ := readRequestHead(conn)
		answerReady(conn, head)
	})
	app2 := newTestUpstream(t, "app2")
	s, _ := newTestServer(t, "", app1, app2)
	openSubscription(t, clientForRank(t, s, "app1", "app2"), "http://"+s.HTTPAddr()+"/mytopic/json")
	require.Equal(t, uint64(1), s.balancer.Status().Upstreams[0].Failures)
}

func TestStream_InformationalResponseSkipped(t *testing.T) {
	// A 1xx before the real answer (e.g. 103 Early Hints) is not mistaken for the response
	app1 := newRawUpstream(t, "app1", func(conn net.Conn) {
		head, _ := readRequestHead(conn)
		if answerReady(conn, head) {
			return
		}
		io.WriteString(conn, "HTTP/1.1 103 Early Hints\r\nLink: </a.css>\r\n\r\nHTTP/1.0 200 OK\r\nContent-Type: text/plain\r\n\r\nstart\n")
	})
	s, _ := newTestServer(t, "", app1)
	status, body := get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/mytopic/json")
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "start\n", body)
}

func TestStream_LargeMessagesAndBursts(t *testing.T) {
	// Messages larger than the copy buffer, and many at once, arrive intact and in order
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "", app1)
	_, r := openSubscription(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/mytopic/json")
	big := strings.Repeat("x", 300*1024)
	go func() {
		app1.push <- big
		for i := 0; i < 1000; i++ {
			app1.push <- fmt.Sprintf("m%d", i)
		}
	}()
	require.Equal(t, big+"\n", readLine(t, r))
	for i := 0; i < 1000; i++ {
		require.Equal(t, fmt.Sprintf("m%d\n", i), readLine(t, r))
	}
}

func TestStream_PartialWritesForwardedRightAway(t *testing.T) {
	// The stream path copies bytes as they come, without waiting for a line or a full buffer
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "", app1)
	next := make(chan struct{})
	app1.on("/partial/json", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "par")
		w.(http.Flusher).Flush()
		<-next
		io.WriteString(w, "tial\n")
	})
	resp, err := http.Get("http://" + s.HTTPAddr() + "/partial/json")
	require.NoError(t, err)
	defer resp.Body.Close()
	buf := make([]byte, 3)
	_, err = io.ReadFull(resp.Body, buf)
	require.NoError(t, err)
	require.Equal(t, "par", string(buf))
	close(next)
	rest, _ := io.ReadAll(resp.Body)
	require.Equal(t, "tial\n", string(rest))
}

func TestStream_UpstreamDiesMidStream(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "", app1)
	_, r := openSubscription(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/mytopic/json")
	app1.server.CloseClientConnections()
	_, err := r.ReadString('\n')
	require.Error(t, err)
	waitFor(t, func() bool {
		return s.balancer.Status().Streams == 0
	})
}

func TestStream_SlowClientIsCut(t *testing.T) {
	// A client that stops reading is cut once a write blocks for upstream-read-timeout, instead
	// of holding the stream (and the upstream) forever
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "upstream-read-timeout: 500ms", app1)
	chunk := strings.Repeat("y", 64*1024)
	app1.on("/flood/json", func(w http.ResponseWriter, r *http.Request) {
		for r.Context().Err() == nil {
			if _, err := io.WriteString(w, chunk); err != nil {
				return
			}
		}
	})
	conn, err := net.Dial("tcp", s.HTTPAddr())
	require.NoError(t, err)
	defer conn.Close()
	conn.(*net.TCPConn).SetReadBuffer(4096)
	io.WriteString(conn, "GET /flood/json HTTP/1.1\r\nHost: x\r\n\r\n") // Never read
	waitFor(t, func() bool {
		return s.balancer.Status().Streams == 1
	})
	waitFor(t, func() bool {
		return s.balancer.Status().Streams == 0
	})
}

func TestStream_TLSHTTP1(t *testing.T) {
	// An HTTP/1.1 client over TLS is hijacked too
	app1 := newTestUpstream(t, "app1")
	s := newTLSTestServer(t, "", app1)
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			TLSNextProto:    map[string]func(string, *tls.Conn) http.RoundTripper{}, // No HTTP/2
		},
	}
	resp, r := openSubscription(t, client, "https://"+s.HTTPSAddr()+"/mytopic/json")
	require.Equal(t, 1, resp.ProtoMajor)
	require.True(t, resp.Close)
	app1.push <- "hello"
	require.Equal(t, "hello\n", readLine(t, r))
	app1.push <- testStreamEnd
	_, err := r.ReadString('\n')
	require.ErrorIs(t, err, io.EOF)
}

func TestStream_ManyClients(t *testing.T) {
	// 200 HTTP/1.1 streams from 200 IPs and 100 HTTP/2 streams on one connection all get every
	// message; a drain then closes all of them
	app1, app2, app3 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2"), newTestUpstream(t, "app3")
	s := newTLSTestServer(t, "", app1, app2, app3)
	h2 := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
			ForceAttemptHTTP2: true,
		},
	}
	var readers []*bufio.Reader
	for i := 0; i < 200; i++ {
		_, r := openSubscription(t, clientFrom(fmt.Sprintf("127.0.%d.%d", 10+i/200, 1+i%200)), "http://"+s.HTTPAddr()+"/bcast/json")
		readers = append(readers, r)
	}
	for i := 0; i < 100; i++ {
		resp, r := openSubscription(t, h2, "https://"+s.HTTPSAddr()+"/bcast/json")
		require.Equal(t, 2, resp.ProtoMajor)
		readers = append(readers, r)
	}
	waitFor(t, func() bool {
		return app1.broadcast.count()+app2.broadcast.count()+app3.broadcast.count() == 300
	})
	require.Equal(t, 300, s.balancer.Status().Streams)
	for i := 0; i < 3; i++ {
		msg := fmt.Sprintf("msg%d", i)
		for _, u := range []*testUpstream{app1, app2, app3} {
			u.broadcast.send(msg)
		}
		for _, r := range readers {
			require.Equal(t, msg+"\n", readLine(t, r))
		}
	}
	s.balancer.Drain(500 * time.Millisecond)
	waitFor(t, func() bool {
		return s.balancer.Status().Streams == 0
	})
	waitFor(t, func() bool {
		return app1.broadcast.count()+app2.broadcast.count()+app3.broadcast.count() == 0
	})
}

func TestStream_OneGoroutinePerHTTP1Stream(t *testing.T) {
	// The memory claim in docs/design.md: a hijacked HTTP/1.1 stream leaves the copy goroutine
	// and nothing else of the load balancer (no server conn goroutine, no transport goroutines)
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "", app1)
	const n = 50
	for i := 0; i < n; i++ {
		openSubscription(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/mytopic/json")
	}
	waitFor(t, func() bool {
		return s.balancer.Status().Streams == n
	})
	http.DefaultClient.CloseIdleConnections() // Our keep-alive connection to the admin API
	time.Sleep(100 * time.Millisecond)        // Let the handlers return
	copying, serving, transport := goroutineCounts()
	require.Equal(t, n, copying)
	require.Zero(t, serving)
	require.Zero(t, transport)
}

func TestStream_NoGoroutineLeak(t *testing.T) {
	// Streams that end in every possible way leave no goroutine behind
	app1 := newTestUpstream(t, "app1")
	s := newTLSTestServer(t, "upstream-read-timeout: 300ms", app1)
	h2 := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
			ForceAttemptHTTP2: true,
		},
	}
	before := runtime.NumGoroutine()
	for i := 0; i < 20; i++ {
		resp, _ := openSubscription(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/mytopic/json")
		resp.Body.Close() // Client gone (noticed on the next write)
		resp, _ = openSubscription(t, h2, "https://"+s.HTTPSAddr()+"/mytopic/json")
		resp.Body.Close()                                                 // HTTP/2 reset
		get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/silent/json") // Read timeout
	}
	app1.push <- testStreamEnd
	waitFor(t, func() bool {
		select {
		case app1.push <- "keepalive":
		default:
		}
		return s.balancer.Status().Streams == 0
	})
	h2.CloseIdleConnections()
	http.DefaultClient.CloseIdleConnections()
	waitFor(t, func() bool {
		return runtime.NumGoroutine() <= before+5
	})
}

// goroutineCounts counts the load balancer's stream copy goroutines, its server connection
// goroutines (not the test upstream's) and its transport goroutines
func goroutineCounts() (copying, serving, transport int) {
	buf := make([]byte, 16<<20)
	buf = buf[:runtime.Stack(buf, true)]
	for _, g := range strings.Split(string(buf), "\n\n") {
		switch {
		case strings.Contains(g, "server.copyStreamHTTP1"):
			copying++
		case strings.Contains(g, "net/http.(*conn).serve") && !strings.Contains(g, "testUpstream"):
			serving++
		case strings.Contains(g, "net/http.(*persistConn)") && strings.Contains(g, "server.(*proxy)"):
			transport++
		}
	}
	return copying, serving, transport
}

// newTLSTestServer is newTestServer with an HTTPS listener too
func newTLSTestServer(t *testing.T, extra string, upstreams ...*testUpstream) *Server {
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	writeTestCert(t, certFile, keyFile, "test")
	s, _ := newTestServer(t, fmt.Sprintf("listen-https: '127.0.0.3:0'\ncert-file: %s\nkey-file: %s\n%s", certFile, keyFile, extra), upstreams...)
	return s
}

func TestStream_DrainCutsNonReadingClient(t *testing.T) {
	// A drain closes the client connection too, even if a write to it is blocked
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "upstream-read-timeout: 1h", app1)
	chunk := strings.Repeat("y", 64*1024)
	app1.on("/flood/json", func(w http.ResponseWriter, r *http.Request) {
		for r.Context().Err() == nil {
			if _, err := io.WriteString(w, chunk); err != nil {
				return
			}
		}
	})
	conn, err := net.Dial("tcp", s.HTTPAddr())
	require.NoError(t, err)
	defer conn.Close()
	conn.(*net.TCPConn).SetReadBuffer(4096)
	io.WriteString(conn, "GET /flood/json HTTP/1.1\r\nHost: x\r\n\r\n")
	waitFor(t, func() bool {
		return s.balancer.Status().Streams == 1
	})
	time.Sleep(200 * time.Millisecond) // Let the write block
	require.NoError(t, s.balancer.DrainUpstream("app1", 0))
	waitFor(t, func() bool {
		return s.balancer.Status().Streams == 0
	})
}

func TestStream_HTTP2AbnormalEndIsAnError(t *testing.T) {
	// An upstream connection that breaks (reset) mid-stream resets the HTTP/2 stream instead of
	// ending it cleanly, so the client cannot mistake a cut for a complete response. (A close is
	// a clean end: that is how a close-delimited body ends.)
	app1 := newRawUpstream(t, "app1", func(conn net.Conn) {
		head, _ := readRequestHead(conn)
		if answerReady(conn, head) {
			return
		}
		io.WriteString(conn, "HTTP/1.0 200 OK\r\nContent-Type: text/plain\r\n\r\nstart\npartial")
		time.Sleep(200 * time.Millisecond)
		conn.(*net.TCPConn).SetLinger(0) // Close with RST
	})
	s := newTLSTestServer(t, "", app1)
	h2 := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
			ForceAttemptHTTP2: true,
		},
	}
	resp, r := openSubscription(t, h2, "https://"+s.HTTPSAddr()+"/mytopic/json")
	require.Equal(t, 2, resp.ProtoMajor)
	_, err := io.ReadAll(r)
	require.Error(t, err)
}

func TestStream_HTTP2CleanEndIsClean(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	s := newTLSTestServer(t, "", app1)
	h2 := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
			ForceAttemptHTTP2: true,
		},
	}
	_, r := openSubscription(t, h2, "https://"+s.HTTPSAddr()+"/mytopic/json")
	app1.push <- testStreamEnd
	_, err := io.ReadAll(r)
	require.NoError(t, err)
}

func TestStream_UpstreamFraming(t *testing.T) {
	// Upstream responses that are not close-delimited are decoded by their framing: chunked
	// bodies lose their chunk sizes, Content-Length bodies end at their length even if the
	// upstream keeps the connection open, bodyless statuses have no body; 1xx are skipped
	responses := map[string]string{
		"/chunked/json":   "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n",
		"/length/json":    "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello",
		"/nocontent/json": "HTTP/1.1 204 No Content\r\n\r\n",
		"/early/json":     "HTTP/1.1 103 Early Hints\r\nLink: </a.css>\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok",
	}
	app1 := newRawUpstream(t, "app1", func(conn net.Conn) {
		head, _ := readRequestHead(conn)
		if answerReady(conn, head) {
			return
		}
		path := strings.Fields(head)[1]
		io.WriteString(conn, responses[path])
		time.Sleep(5 * time.Second) // Keep the connection open: the framing must end the response
	})
	s := newTLSTestServer(t, "", app1)
	h2 := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
			ForceAttemptHTTP2: true,
		},
	}
	tests := []struct {
		path   string
		status int
		body   string
	}{
		{"/chunked/json", 200, "abcde"},
		{"/length/json", 200, "hello"},
		{"/nocontent/json", 204, ""},
		{"/early/json", 200, "ok"},
	}
	for _, tt := range tests {
		for _, base := range []string{"http://" + s.HTTPAddr(), "https://" + s.HTTPSAddr()} {
			client := http.DefaultClient
			if strings.HasPrefix(base, "https") {
				client = h2
			}
			start := time.Now()
			status, body := get(t, client, base+tt.path)
			require.Equal(t, tt.status, status, base+tt.path)
			require.Equal(t, tt.body, body, base+tt.path)
			require.Less(t, time.Since(start), 3*time.Second, base+tt.path)
		}
	}
}

func TestStream_UpstreamHeaderLimit(t *testing.T) {
	// A broken upstream sending endless headers is cut off, not buffered
	app1 := newRawUpstream(t, "app1", func(conn net.Conn) {
		head, _ := readRequestHead(conn)
		if answerReady(conn, head) {
			return
		}
		io.WriteString(conn, "HTTP/1.0 200 OK\r\nX-Big: ")
		chunk := strings.Repeat("a", 64*1024)
		for { // Forever, until the load balancer gives up
			if _, err := io.WriteString(conn, chunk); err != nil {
				return
			}
		}
	})
	s, _ := newTestServer(t, "", app1)
	start := time.Now()
	status, _ := get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/mytopic/json")
	require.Equal(t, http.StatusBadGateway, status)
	require.Less(t, time.Since(start), 3*time.Second)
}

func TestStream_ForwardedHeadersSanitized(t *testing.T) {
	// Like the reverse proxy path: client-supplied Forwarded and X-Forwarded-Host/Proto are dropped
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "", app1)
	app1.on("/fwd/json", func(http.ResponseWriter, *http.Request) {})
	app1.on("/fwd", func(http.ResponseWriter, *http.Request) {})
	for _, path := range []string{"/fwd/json", "/fwd"} {
		req, _ := http.NewRequest(http.MethodGet, "http://"+s.HTTPAddr()+path, nil)
		req.Header.Set("Forwarded", "for=1.2.3.4")
		req.Header.Set("X-Forwarded-Host", "evil.example.com")
		req.Header.Set("X-Forwarded-Proto", "https")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		got := app1.lastRequest.Load().(*http.Request)
		require.Empty(t, got.Header.Get("Forwarded"), path)
		require.Empty(t, got.Header.Get("X-Forwarded-Host"), path)
		require.Empty(t, got.Header.Get("X-Forwarded-Proto"), path)
	}
}
