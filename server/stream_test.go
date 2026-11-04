package server

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
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
	s, _ := newTestServer(t, fmt.Sprintf("listen-https: '127.0.0.1:0'\ncert-file: %s\nkey-file: %s", certFile, keyFile), app1)
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
	s, _ := newTestServer(t, "access-log: "+logFile, app1)
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
