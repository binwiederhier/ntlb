package server

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"heckel.io/ntlb/balancer"
	"heckel.io/ntlb/config"
	"heckel.io/ntlb/match"
)

func TestProxy_StreamsResponse(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "", app1)
	_, closeStream := openStream(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/stream") // Only returns if the first line was flushed through
	defer closeStream()
	waitFor(t, func() bool {
		return s.balancer.Status().Streams == 1
	})
}

func TestProxy_StreamsRequestBody(t *testing.T) {
	// The upstream sees the first bytes of the body while the client is still sending it
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "", app1)
	pr, pw := io.Pipe()
	result := make(chan string, 1)
	go func() {
		resp, err := http.Post("http://"+s.HTTPAddr()+"/body-progress", "text/plain", pr)
		if err != nil {
			result <- err.Error()
			return
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		result <- string(body)
	}()
	_, err := pw.Write([]byte("hello"))
	require.NoError(t, err)
	select {
	case got := <-app1.bodyCh:
		require.Equal(t, "hello", got)
	case <-time.After(3 * time.Second):
		t.Fatal("upstream did not receive the first body bytes while the body was still open")
	}
	pw.Write([]byte(" world"))
	pw.Close()
	require.Equal(t, "hello world", <-result)
}

func TestProxy_ForwardedHeaders(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "", app1)
	req, _ := http.NewRequest(http.MethodGet, "http://"+s.HTTPAddr()+"/headers", nil)
	req.Host = "ntfy.example.com"
	req.Header.Set("X-Forwarded-For", "9.9.9.9")
	resp, err := clientFrom("127.0.0.7").Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	require.Equal(t, "host=ntfy.example.com xff=9.9.9.9, 127.0.0.7", string(body))
}

func TestProxy_RetryOnConnectError(t *testing.T) {
	// A POST whose upstream refuses the connection goes to the next one: it never reached a node
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	s, _ := newTestServer(t, "", app1, app2)
	client := clientForUpstream(t, s, "app1")
	app1.server.Close()
	resp, err := client.Post("http://"+s.HTTPAddr()+"/body-progress", "text/plain", strings.NewReader("hello world"))
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "hello world", string(body))
	require.Equal(t, uint64(1), s.balancer.Status().Upstreams[0].Failures)
}

func TestProxy_RetryOn503(t *testing.T) {
	// A 503 for a request whose body was not sent yet goes to the next upstream (a draining
	// node); one whose body the upstream already read does not
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	s, _ := newTestServer(t, "", app1, app2)
	client := clientForUpstream(t, s, "app1")
	app1.hits.Store(0)
	app2.hits.Store(0)
	status, _ := get(t, client, "http://"+s.HTTPAddr()+"/fail503")
	require.Equal(t, http.StatusServiceUnavailable, status) // Both say 503: the last answer is passed on
	require.Equal(t, int64(1), app1.hits.Load())
	require.Equal(t, int64(1), app2.hits.Load())

	resp, err := client.Post("http://"+s.HTTPAddr()+"/read-then-503", "text/plain", strings.NewReader("some body"))
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	require.Equal(t, int64(2), app1.hits.Load())
	require.Equal(t, int64(1), app2.hits.Load())
}

func TestProxy_UpstreamReadTimeout(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "upstream-read-timeout: 300ms", app1)
	resp, err := http.Get("http://" + s.HTTPAddr() + "/silent")
	require.NoError(t, err)
	defer resp.Body.Close()
	start := time.Now()
	_, err = io.ReadAll(resp.Body)
	require.Error(t, err) // Cut, not a clean end
	require.Less(t, time.Since(start), 3*time.Second)
}

func TestProxy_WebSocketUpgrade(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "", app1)
	conn, err := net.Dial("tcp", s.HTTPAddr())
	require.NoError(t, err)
	defer conn.Close()
	fmt.Fprintf(conn, "GET /ws HTTP/1.1\r\nHost: x\r\nUpgrade: test\r\nConnection: Upgrade\r\n\r\n")
	r := bufio.NewReader(conn)
	resp, err := http.ReadResponse(r, nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	fmt.Fprint(conn, "ping\n")
	line, err := r.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "ping\n", line)

	// Draining the upstream cuts the upgraded connection too
	require.NoError(t, s.balancer.DrainUpstream("app1", 0))
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, err = r.ReadString('\n')
	require.Error(t, err)
	var netErr net.Error
	require.False(t, errors.As(err, &netErr) && netErr.Timeout(), "connection was not cut")
}

func TestProxy_DrainedRejectsNewRequests(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "", app1)
	s.balancer.Drain(0)
	waitFor(t, func() bool {
		return s.balancer.Drained()
	})
	resp, err := http.Get("http://" + s.HTTPAddr() + "/name")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	require.True(t, resp.Close)
	s.balancer.Undrain()
	status, body := get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/name")
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "app1", body)
}

func TestProxy_DrainingRefusesNewSubscriptions(t *testing.T) {
	// While draining, new subscriptions are turned away so their clients go elsewhere instead of
	// reconnecting here only to be drained again; publishes and polls are still served
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "", app1)
	r, closeStream := openStream(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/mytopic/json")
	defer closeStream()
	s.balancer.Drain(time.Hour)
	tests := []struct {
		method, path string
		status       int
	}{
		{http.MethodGet, "/mytopic/json", http.StatusServiceUnavailable},
		{http.MethodGet, "/a,b/sse", http.StatusServiceUnavailable},
		{http.MethodGet, "/mytopic/raw?since=all", http.StatusServiceUnavailable},
		{http.MethodGet, "/mytopic/ws", http.StatusServiceUnavailable},
		{http.MethodGet, "/mytopic/json?poll=1", http.StatusOK},
		{http.MethodPost, "/mytopic", http.StatusNotFound}, // Reached the upstream
		{http.MethodGet, "/name", http.StatusOK},
	}
	for _, tt := range tests {
		req, _ := http.NewRequest(tt.method, "http://"+s.HTTPAddr()+tt.path, nil)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, tt.status, resp.StatusCode, tt.method+" "+tt.path)
		if tt.status == http.StatusServiceUnavailable {
			require.True(t, resp.Close)
		}
	}
	require.False(t, streamClosed(r)) // Open streams are closed by the drain's pacing, not here
}

func TestIsSubscription(t *testing.T) {
	ntfy, err := config.Parse([]byte("listen-http: ':80'\nsites: [{name: s, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]"))
	require.NoError(t, err)
	custom, err := config.Parse([]byte("listen-http: ':80'\nsites: [{name: s, streams: {match: 'method == \"GET\" && path == \"/events\" && !flag(\"once\")'}, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]"))
	require.NoError(t, err)
	disabled, err := config.Parse([]byte("listen-http: ':80'\nsites: [{name: s, streams: {match: 'false'}, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]"))
	require.NoError(t, err)
	tests := []struct {
		name    string
		streams *config.Streams
		method  string
		target  string
		header  string
		want    bool
	}{
		{"json", ntfy.Sites[0].Streams, http.MethodGet, "/mytopic/json", "", true},
		{"sse", ntfy.Sites[0].Streams, http.MethodGet, "/mytopic/sse", "", true},
		{"raw", ntfy.Sites[0].Streams, http.MethodGet, "/mytopic/raw?since=all", "", true},
		{"ws", ntfy.Sites[0].Streams, http.MethodGet, "/mytopic/ws", "", true},
		{"multiple topics", ntfy.Sites[0].Streams, http.MethodGet, "/a,b,c/json", "", true},
		{"poll=1", ntfy.Sites[0].Streams, http.MethodGet, "/mytopic/json?poll=1", "", false},
		{"poll=true", ntfy.Sites[0].Streams, http.MethodGet, "/mytopic/json?poll=TRUE", "", false},
		{"po=yes", ntfy.Sites[0].Streams, http.MethodGet, "/mytopic/json?po=yes", "", false},
		{"x-poll header", ntfy.Sites[0].Streams, http.MethodGet, "/mytopic/json", "X-Poll: 1", false},
		{"poll header", ntfy.Sites[0].Streams, http.MethodGet, "/mytopic/json", "Poll: true", false},
		{"poll=0", ntfy.Sites[0].Streams, http.MethodGet, "/mytopic/json?poll=0", "", true},
		{"header wins over query", ntfy.Sites[0].Streams, http.MethodGet, "/mytopic/json?poll=1", "X-Poll: no", true},
		{"header poll over query", ntfy.Sites[0].Streams, http.MethodGet, "/mytopic/json?poll=0", "Po: 1", false},
		{"first query param wins", ntfy.Sites[0].Streams, http.MethodGet, "/mytopic/json?x-poll=0&poll=1", "", true},
		{"later query param if first unset", ntfy.Sites[0].Streams, http.MethodGet, "/mytopic/json?po=1", "", false},
		{"publish", ntfy.Sites[0].Streams, http.MethodPost, "/mytopic", "", false},
		{"post to json", ntfy.Sites[0].Streams, http.MethodPost, "/mytopic/json", "", false},
		{"topic page", ntfy.Sites[0].Streams, http.MethodGet, "/mytopic", "", false},
		{"nested path", ntfy.Sites[0].Streams, http.MethodGet, "/v1/account/json", "", false},
		{"bad topic", ntfy.Sites[0].Streams, http.MethodGet, "/my%20topic/json", "", false},
		{"root", ntfy.Sites[0].Streams, http.MethodGet, "/json", "", false},
		{"custom", custom.Sites[0].Streams, http.MethodGet, "/events", "", true},
		{"custom excluded", custom.Sites[0].Streams, http.MethodGet, "/events?once=1", "", false},
		{"custom ignores ntfy", custom.Sites[0].Streams, http.MethodGet, "/mytopic/json", "", false},
		{"custom ignores poll", custom.Sites[0].Streams, http.MethodGet, "/events?poll=1", "", true},
		{"disabled", disabled.Sites[0].Streams, http.MethodGet, "/mytopic/json", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(tt.method, tt.target, nil)
			if tt.header != "" {
				name, value, _ := strings.Cut(tt.header, ": ")
				r.Header.Set(name, value)
			}
			require.Equal(t, tt.want, isSubscription(match.NewEnv(r), tt.streams))
		})
	}
}

func TestProxy_CustomStreams(t *testing.T) {
	// A non-ntfy backend: /stream is a stream (so it takes the stream path, with an HTTP/1.0
	// upstream request), ntfy's subscription paths are ordinary requests
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "streams: {match: 'path == \"/stream\"'}", app1)
	_, closeStream := openStream(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/stream")
	closeStream()
	require.Equal(t, "HTTP/1.0", app1.lastProto.Load())
	_, closeStream = openStream(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/mytopic/json")
	closeStream()
	require.Equal(t, "HTTP/1.1", app1.lastProto.Load())
}

func TestProxy_StreamsDisabled(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "streams: {match: 'false'}", app1)
	_, closeStream := openStream(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/mytopic/json")
	closeStream()
	require.Equal(t, "HTTP/1.1", app1.lastProto.Load())
	s.balancer.Drain(time.Hour) // Nothing counts as a subscription, so nothing is turned away
	status, _ := get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/mytopic/json?poll=1")
	require.Equal(t, http.StatusOK, status)
}

func TestProxy_HashPrefix(t *testing.T) {
	// With an IPv4 /8 key, every loopback client is the same client
	app1, app2, app3 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2"), newTestUpstream(t, "app3")
	s, _ := newTestServer(t, "client-key: {ipv4-prefix-bits: 8}", app1, app2, app3)
	seen := make(map[string]bool)
	for i := 1; i <= 20; i++ {
		_, name := get(t, clientFrom(fmt.Sprintf("127.0.0.%d", i)), "http://"+s.HTTPAddr()+"/name")
		seen[name] = true
	}
	require.Len(t, seen, 1)
}

func TestProxy_AllUpstreamsDown(t *testing.T) {
	// Every upstream refuses: 502 for any method, and a failure counted on each
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	s, _ := newTestServer(t, "", app1, app2)
	app1.server.Close()
	app2.server.Close()
	status, _ := get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/name")
	require.Equal(t, http.StatusBadGateway, status)
	resp, err := http.Post("http://"+s.HTTPAddr()+"/mytopic", "text/plain", strings.NewReader("hi"))
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusBadGateway, resp.StatusCode)
	for _, u := range s.balancer.Status().Upstreams {
		require.Equal(t, uint64(2), u.Failures, u.Name)
	}
	waitFor(t, func() bool {
		return s.balancer.Status().Streams == 0
	})
}

func TestProxy_NoUpstreamInRotation(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "", app1)
	require.NoError(t, s.balancer.DrainUpstream("app1", 0))
	status, _ := get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/name")
	require.Equal(t, http.StatusServiceUnavailable, status)
	status, _ = get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/mytopic/json")
	require.Equal(t, http.StatusServiceUnavailable, status)
}

func TestProxy_ResponseHeaderTimeout(t *testing.T) {
	// An upstream that does not answer within upstream-read-timeout is a 504, on both paths
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "upstream-read-timeout: 200ms", app1)
	slow := func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}
	app1.on("/slow", slow)
	app1.on("/slow/json", slow)
	start := time.Now()
	status, _ := get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/slow")
	require.Equal(t, http.StatusGatewayTimeout, status)
	status, _ = get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/slow/json")
	require.Equal(t, http.StatusGatewayTimeout, status)
	require.Less(t, time.Since(start), 3*time.Second)
}

func TestProxy_ConnectionResetRetriedOnlyIfIdempotent(t *testing.T) {
	// app1 reads the request and drops the connection without answering. A GET may go to the
	// next upstream; a POST (whose body app1 got) must not be sent again.
	var hits1 atomic.Int64
	app1 := newRawUpstream(t, "app1", func(conn net.Conn) {
		head, r := readRequestHead(conn)
		if answerReady(conn, head) {
			return
		}
		hits1.Add(1)
		if strings.HasPrefix(head, "POST") {
			r.Read(make([]byte, 100))
		}
	})
	app2 := newTestUpstream(t, "app2")
	s, _ := newTestServer(t, "", app1, app2)
	client := clientForRank(t, s, "app1", "app2")
	app2.hits.Store(0)
	status, body := get(t, client, "http://"+s.HTTPAddr()+"/name")
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "app2", body)
	require.Equal(t, int64(1), hits1.Load())

	resp, err := client.Post("http://"+s.HTTPAddr()+"/mytopic", "text/plain", strings.NewReader("a message"))
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusBadGateway, resp.StatusCode)
	require.Equal(t, int64(2), hits1.Load())
	require.Equal(t, int64(1), app2.hits.Load()) // Only the GET
}

func TestProxy_PartlySentBodyNotResent(t *testing.T) {
	// app1 refuses after it read part of a streamed body: the rest cannot be replayed, so the
	// request fails instead of reaching app2 without its beginning
	app1 := newRawUpstream(t, "app1", func(conn net.Conn) {
		head, r := readRequestHead(conn)
		if answerReady(conn, head) {
			return
		}
		io.ReadFull(r, make([]byte, 5))
	})
	app2 := newTestUpstream(t, "app2")
	s, _ := newTestServer(t, "", app1, app2)
	client := clientForRank(t, s, "app1", "app2")
	app2.hits.Store(0)
	pr, pw := io.Pipe()
	go func() {
		pw.Write([]byte("hello"))
		time.Sleep(200 * time.Millisecond)
		pw.Write([]byte(" world"))
		pw.Close()
	}()
	req, _ := http.NewRequest(http.MethodPut, "http://"+s.HTTPAddr()+"/body-progress", pr)
	resp, err := client.Do(req)
	if err == nil {
		resp.Body.Close()
		require.Equal(t, http.StatusBadGateway, resp.StatusCode)
	}
	require.Equal(t, int64(0), app2.hits.Load())
}

func TestProxy_LargeBodies(t *testing.T) {
	// 16 MiB up and down, byte for byte, through 4 KiB copy buffers
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "", app1)
	big := make([]byte, 16<<20)
	for i := range big {
		big[i] = byte(i * 7)
	}
	app1.on("/echo", func(w http.ResponseWriter, r *http.Request) {
		io.Copy(w, r.Body)
	})
	resp, err := http.Post("http://"+s.HTTPAddr()+"/echo", "application/octet-stream", bytes.NewReader(big))
	require.NoError(t, err)
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.True(t, bytes.Equal(big, got))
}

func TestProxy_HopByHopHeaders(t *testing.T) {
	// Neither direction forwards hop-by-hop headers, including those named in Connection; both
	// paths (reverse proxy and stream path)
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "", app1)
	respond := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Connection", "X-Up-Hop")
		w.Header().Set("X-Up-Hop", "1")
		w.Header().Set("Keep-Alive", "timeout=5")
		w.Header().Set("X-Up-End", "1")
		io.WriteString(w, "ok")
	}
	app1.on("/hop", respond)
	app1.on("/hop/json", respond)
	for _, path := range []string{"/hop", "/hop/json"} {
		req, _ := http.NewRequest(http.MethodGet, "http://"+s.HTTPAddr()+path, nil)
		req.Header.Set("Connection", "X-Down-Hop")
		req.Header.Set("X-Down-Hop", "1")
		req.Header.Set("Proxy-Authorization", "secret")
		req.Header.Set("TE", "trailers")
		req.Header.Set("X-Down-End", "1")
		req.Header.Set("Authorization", "Bearer tk_abc")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		io.ReadAll(resp.Body)
		resp.Body.Close()
		got := app1.lastRequest.Load().(*http.Request)
		require.Empty(t, got.Header.Get("X-Down-Hop"), path)
		require.Empty(t, got.Header.Get("Proxy-Authorization"), path)
		if path == "/hop/json" { // The reverse proxy keeps "TE: trailers" on purpose (gRPC)
			require.Empty(t, got.Header.Get("TE"), path)
		}
		require.Equal(t, "1", got.Header.Get("X-Down-End"), path)
		require.Equal(t, "Bearer tk_abc", got.Header.Get("Authorization"), path)
		if path == "/hop" { // On an HTTP/1.0 response, Go's server replaces the handler's Connection header
			require.Empty(t, resp.Header.Get("X-Up-Hop"), path)
		}
		require.Empty(t, resp.Header.Get("Keep-Alive"), path)
		require.Equal(t, "1", resp.Header.Get("X-Up-End"), path)
	}
}

func TestProxy_PathAndQueryPreserved(t *testing.T) {
	// The request target reaches the upstream as the client sent it, escaping included
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "", app1)
	app1.on("/q/json", func(http.ResponseWriter, *http.Request) {})
	for _, target := range []string{
		"/a%2Fb/c?x=1&y=%20z&x=2",
		"/q/json?since=all&filter=a%26b", // Stream path
		"/v1/account?q=%E2%9C%93",
	} {
		get(t, http.DefaultClient, "http://"+s.HTTPAddr()+target)
		require.Equal(t, target, app1.lastRequest.Load().(*http.Request).RequestURI, target)
	}
}

func TestProxy_ResponsePassthrough(t *testing.T) {
	// Status, repeated headers and content encodings pass through untouched (no transparent gzip)
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "", app1)
	app1.on("/gz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Set-Cookie", "a=1")
		w.Header().Add("Set-Cookie", "b=2")
		w.Header().Set("Content-Encoding", "gzip")
		w.WriteHeader(http.StatusTeapot)
		w.Write([]byte{0x1f, 0x8b, 1, 2, 3})
	})
	req, _ := http.NewRequest(http.MethodGet, "http://"+s.HTTPAddr()+"/gz", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err := http.DefaultTransport.RoundTrip(req) // No transparent decompression in the test client either
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusTeapot, resp.StatusCode)
	require.Equal(t, []string{"a=1", "b=2"}, resp.Header.Values("Set-Cookie"))
	require.Equal(t, "gzip", resp.Header.Get("Content-Encoding"))
	require.Equal(t, []byte{0x1f, 0x8b, 1, 2, 3}, body)
	require.Equal(t, "gzip", app1.lastRequest.Load().(*http.Request).Header.Get("Accept-Encoding"))
}

func TestProxy_Head(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "", app1)
	resp, err := http.Head("http://" + s.HTTPAddr() + "/name")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, http.MethodHead, app1.lastRequest.Load().(*http.Request).Method)
}

func TestProxy_ClientGoneReleasesSessionAndUpstream(t *testing.T) {
	// A client that goes away mid-stream ends the upstream request too (reverse proxy path)
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "", app1)
	upstreamDone := make(chan struct{})
	app1.on("/hold", func(w http.ResponseWriter, r *http.Request) {
		defer close(upstreamDone)
		io.WriteString(w, "start\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	})
	_, closeStream := openStream(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/hold")
	waitFor(t, func() bool {
		return s.balancer.Status().Streams == 1
	})
	closeStream()
	select {
	case <-upstreamDone:
	case <-time.After(3 * time.Second):
		t.Fatal("upstream request not canceled")
	}
	waitFor(t, func() bool {
		return s.balancer.Status().Streams == 0
	})
}

func TestProxy_GetWithBodyNotAStream(t *testing.T) {
	// The stream path sends no request body, so a GET that has one goes the regular way, body and all
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "", app1)
	app1.on("/body/json", func(w http.ResponseWriter, r *http.Request) {
		io.Copy(w, r.Body)
	})
	req, _ := http.NewRequest(http.MethodGet, "http://"+s.HTTPAddr()+"/body/json", strings.NewReader("hello"))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "hello", string(body))
	require.Equal(t, "HTTP/1.1", app1.lastProto.Load())
}

func TestProxy_ReloadUnderLoad(t *testing.T) {
	// Requests and streams keep working while the config is reloaded over and over
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	s, filename := newTestServer(t, "", app1, app2)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var failures atomic.Int64
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := clientFrom(fmt.Sprintf("127.0.1.%d", i+1))
			for {
				select {
				case <-stop:
					return
				default:
				}
				resp, err := client.Get("http://" + s.HTTPAddr() + "/name")
				if err != nil {
					failures.Add(1)
					continue
				}
				io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					failures.Add(1)
				}
			}
		}()
	}
	for i := 0; i < 20; i++ {
		writeConfig(t, filename, fmt.Sprintf("upstream-read-timeout: %dm\nstreams: {match: %q}", i+1, streamsToggle(i)), app1, app2)
		requireReload(t, s)
	}
	close(stop)
	wg.Wait()
	require.Zero(t, failures.Load())
}

// clientForRank returns a client from a loopback IP whose rendezvous order starts with names
func clientForRank(t *testing.T, s *Server, names ...string) *http.Client {
	for i := 1; i < 250; i++ {
		ip := fmt.Sprintf("127.0.2.%d", i)
		candidates := s.balancer.Pick("s", clientKey(ip+":1", s.config.Load().ClientKey))
		ok := len(candidates) >= len(names)
		for j := 0; ok && j < len(names); j++ {
			ok = candidates[j].Name() == names[j]
		}
		if ok {
			return clientFrom(ip)
		}
	}
	t.Fatalf("no client IP ranks %v first", names)
	return nil
}

func TestProxy_StalledUploadTimesOut(t *testing.T) {
	// A client that sends headers and part of a body, then nothing, is cut after
	// upstream-read-timeout instead of holding a handler and an upstream connection forever
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "upstream-read-timeout: 300ms", app1)
	conn, err := net.Dial("tcp", s.HTTPAddr())
	require.NoError(t, err)
	defer conn.Close()
	io.WriteString(conn, "POST /body-progress HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\nhello")
	waitFor(t, func() bool {
		return s.balancer.Status().Streams == 1
	})
	start := time.Now()
	waitFor(t, func() bool {
		return s.balancer.Status().Streams == 0
	})
	require.Less(t, time.Since(start), 3*time.Second)
}

func TestProxy_DrainInterruptsStalledUpload(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "upstream-read-timeout: 1h", app1)
	conn, err := net.Dial("tcp", s.HTTPAddr())
	require.NoError(t, err)
	defer conn.Close()
	io.WriteString(conn, "POST /body-progress HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\nhello")
	waitFor(t, func() bool {
		return s.balancer.Status().Streams == 1
	})
	s.balancer.Drain(0) // Not an upstream drain: the request has no response yet, so it is not attached to app1
	waitFor(t, func() bool {
		return s.balancer.Status().Streams == 0
	})
}

func TestProxy_NonReadingDownloadIsCut(t *testing.T) {
	// On the reverse proxy path too, a client that stops reading is cut once a write blocks for
	// upstream-read-timeout, and a drain cuts it right away
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "upstream-read-timeout: 500ms", app1)
	chunk := strings.Repeat("y", 64*1024)
	app1.on("/flood", func(w http.ResponseWriter, r *http.Request) {
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
	io.WriteString(conn, "GET /flood HTTP/1.1\r\nHost: x\r\n\r\n") // Never read
	waitFor(t, func() bool {
		return s.balancer.Status().Streams == 1
	})
	waitFor(t, func() bool {
		return s.balancer.Status().Streams == 0
	})
}

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

func TestProxy_DeadAddressTriesSameUpstreamFirst(t *testing.T) {
	// An upstream with a dead port and a live one still serves its clients (they keep the node
	// that holds their rate limits) instead of moving them to another node; both paths
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	dead, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	deadAddr := dead.Addr().String()
	dead.Close()
	filename := filepath.Join(t.TempDir(), "config.yml")
	conf := fmt.Sprintf("listen-http: '127.0.0.1:0'\nlisten-admin: '127.0.0.1:0'\nsites:\n  - name: s\n    health-check: {interval: 1h, timeout: 1s}\n    upstreams:\n      - name: app1\n        addresses: [%q, %q]\n      - name: app2\n        addresses: [%q]\n", app1.addr(), deadAddr, app2.addr())
	require.NoError(t, os.WriteFile(filename, []byte(conf), 0600))
	s := startTestServer(t, filename) // Health checks probe the first address, which is alive
	client := clientForRank(t, s, "app1", "app2")
	_, closeStream := openStream(t, client, "http://"+s.HTTPAddr()+"/stream") // Now the dead address has fewer streams
	defer closeStream()
	for i := 0; i < 4; i++ {
		status, body := get(t, client, "http://"+s.HTTPAddr()+"/name")
		require.Equal(t, http.StatusOK, status)
		require.Equal(t, "app1", body)
	}
	app1.on("/once/json", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "app1")
	})
	for i := 0; i < 4; i++ {
		_, body := get(t, client, "http://"+s.HTTPAddr()+"/once/json")
		require.Equal(t, "app1", body)
	}
}

func TestProxy_CancelRacesHandlerEnd(t *testing.T) {
	// A drain cancels requests at any moment, also while their handler returns. Canceling must
	// not touch a ResponseWriter whose handler is done (for HTTP/2 that is a nil dereference that
	// takes the process down, seen in the lab soak).
	app1 := newTestUpstream(t, "app1")
	s := newTLSTestServer(t, "", app1)
	app1.on("/echo", func(w http.ResponseWriter, r *http.Request) {
		io.Copy(w, r.Body)
	})
	h2 := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
			ForceAttemptHTTP2: true,
		},
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				resp, err := h2.Post("https://"+s.HTTPSAddr()+"/echo", "text/plain", strings.NewReader("hello"))
				if err == nil {
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}
			}
		}()
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		s.proxy.mu.Lock()
		for pr := range s.proxy.active {
			pr.cancel(balancer.ErrDrained)
		}
		s.proxy.mu.Unlock()
		time.Sleep(100 * time.Microsecond)
	}
	close(stop)
	wg.Wait()
}

func TestSafely(t *testing.T) {
	// A panic in a goroutine of our own (detached stream, cancel callback) is logged and
	// contained: it must not take down the process and every stream with it
	ran := false
	require.NotPanics(t, func() {
		safely("test", func() {
			ran = true
			panic("boom")
		})
	})
	require.True(t, ran)
	require.NotPanics(t, func() {
		safely("test", func() {
			panic(http.ErrAbortHandler) // Our own way to cut a stream: not an error
		})
	})
}

// streamsToggle turns streams off on even reloads and back to the default on odd ones
func streamsToggle(i int) string {
	if i%2 == 0 {
		return "false"
	}
	return config.DefaultStreamsMatch
}

func TestProxy_Sites(t *testing.T) {
	// Two sites with an upstream of the same name each, picked by Host; global rules apply to
	// both, site rules to their site only
	a1, b1 := newTestUpstream(t, "app1"), newTestUpstream(t, "app1")
	filename := filepath.Join(t.TempDir(), "config.yml")
	require.NoError(t, os.WriteFile(filename, []byte(fmt.Sprintf(`
listen-http: '127.0.0.1:0'
listen-admin: '127.0.0.1:0'
rules: [{name: global, match: 'site == "a" && query == "global"', action: block}]
sites:
  - name: a
    hosts: [a.com]
    health-check: {interval: 1h, timeout: 1s}
    upstreams: [{name: app1, addresses: [%q]}]
  - name: b
    hosts: ["*.b.com"]
    health-check: {interval: 1h, timeout: 1s}
    upstreams: [{name: app1, addresses: [%q]}]
    rules: [{name: blocked, match: 'query == "blocked"', action: block, status: 451}]
`, a1.addr(), b1.addr())), 0600))
	s := startTestServer(t, filename)
	getHost := func(host, path string) int {
		req, err := http.NewRequest(http.MethodGet, "http://"+s.HTTPAddr()+path, nil)
		require.NoError(t, err)
		req.Host = host
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		io.ReadAll(resp.Body)
		resp.Body.Close()
		return resp.StatusCode
	}

	// By host
	require.Equal(t, http.StatusOK, getHost("a.com", "/name"))
	require.Equal(t, int64(1), a1.hits.Load())
	require.Equal(t, http.StatusOK, getHost("www.B.com:80", "/name"))
	require.Equal(t, int64(1), b1.hits.Load())

	// No site for the host, and none takes the rest
	require.Equal(t, http.StatusNotFound, getHost("c.com", "/name"))
	require.Equal(t, int64(2), a1.hits.Load()+b1.hits.Load())

	// Rules
	require.Equal(t, http.StatusForbidden, getHost("a.com", "/name?global"))
	require.Equal(t, http.StatusOK, getHost("x.b.com", "/name?global"))
	require.Equal(t, 451, getHost("x.b.com", "/name?blocked"))
	require.Equal(t, http.StatusOK, getHost("a.com", "/name?blocked"))

	// Upstreams are named per site in the status and the drains
	status := s.balancer.Status()
	require.Equal(t, "a", status.Upstreams[0].Site)
	require.Equal(t, "b", status.Upstreams[1].Site)
	require.NoError(t, s.balancer.DrainUpstream("b/app1", 0))
	waitFor(t, func() bool {
		return getHost("x.b.com", "/name") == http.StatusServiceUnavailable
	})
	require.Equal(t, http.StatusOK, getHost("a.com", "/name"))
}

func TestProxy_SiteStreams(t *testing.T) {
	// Each site decides which of its requests are streams
	a1, b1 := newTestUpstream(t, "app1"), newTestUpstream(t, "app1")
	filename := filepath.Join(t.TempDir(), "config.yml")
	require.NoError(t, os.WriteFile(filename, []byte(fmt.Sprintf(`
listen-http: '127.0.0.1:0'
listen-admin: '127.0.0.1:0'
sites:
  - {name: a, hosts: [a.com], health-check: {interval: 1h, timeout: 1s}, upstreams: [{name: app1, addresses: [%q]}]}
  - {name: b, streams: {match: 'false'}, health-check: {interval: 1h, timeout: 1s}, upstreams: [{name: app1, addresses: [%q]}]}
`, a1.addr(), b1.addr())), 0600))
	s := startTestServer(t, filename)
	for _, host := range []string{"a.com", "other.com"} {
		req, err := http.NewRequest(http.MethodGet, "http://"+s.HTTPAddr()+"/bcast/json", nil)
		require.NoError(t, err)
		req.Host = host
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
	}
	waitFor(t, func() bool {
		_, metrics := get(t, http.DefaultClient, "http://"+s.AdminAddr()+"/metrics")
		return strings.Contains(metrics, `ntlb_streams{protocol="http1"} 1`) && s.balancer.Status().Streams == 2
	})
}
