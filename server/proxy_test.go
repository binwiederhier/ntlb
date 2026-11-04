package server

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
