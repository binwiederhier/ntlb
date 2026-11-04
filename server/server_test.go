package server

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// testUpstream is a fake ntfy node. Paths:
//
//	/v1/ready      200, or 503 if notReady is set
//	/name          the upstream's name
//	/stream        "start\n", then blocks until the client goes away
//	/headers       the request's Host and X-Forwarded-For
//	/fail503       503
//	/read-then-503 reads the body, then 503
//	/body-progress reads 5 bytes, reports them on bodyCh, then echoes the rest
//	/silent        sends the headers, then nothing
//	/ws            a raw upgrade that echoes every byte
//	/*/json        an ntfy subscription: "start\n", then every message sent to push, until
//	               testStreamEnd; "polled" with ?poll=1; ?status=N answers N; /silent/json sends
//	               the headers only
type testUpstream struct {
	name        string
	server      *httptest.Server
	notReady    atomic.Bool
	bodyCh      chan string
	push        chan string
	hits        atomic.Int64
	lastProto   atomic.Value // string
	lastHeaders atomic.Value // string
}

// testStreamEnd sent to testUpstream.push ends the subscription from the upstream side
const testStreamEnd = "!end"

func newTestUpstream(t *testing.T, name string) *testUpstream {
	u := &testUpstream{
		name:   name,
		bodyCh: make(chan string, 10),
		push:   make(chan string, 10),
	}
	u.server = httptest.NewServer(http.HandlerFunc(u.handle))
	t.Cleanup(func() {
		u.server.CloseClientConnections()
		u.server.Close()
	})
	return u
}

func (u *testUpstream) handle(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/v1/ready" {
		u.hits.Add(1)
	}
	if strings.HasSuffix(r.URL.Path, "/json") { // An ntfy subscription, see the doc comment above
		u.lastProto.Store(r.Proto)
		u.lastHeaders.Store(fmt.Sprintf("host=%s xff=%s", r.Host, r.Header.Get("X-Forwarded-For")))
		if r.URL.Query().Get("poll") == "1" {
			io.WriteString(w, "polled")
			return
		} else if status := r.URL.Query().Get("status"); status != "" {
			code, _ := strconv.Atoi(status)
			w.WriteHeader(code)
			io.WriteString(w, "status body")
			return
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusOK)
		if r.URL.Path != "/silent/json" {
			io.WriteString(w, "start\n")
		}
		w.(http.Flusher).Flush()
		for {
			select {
			case <-r.Context().Done():
				return
			case m := <-u.push:
				if m == testStreamEnd {
					return
				}
				io.WriteString(w, m+"\n")
				w.(http.Flusher).Flush()
			}
		}
	}
	switch r.URL.Path {
	case "/v1/ready":
		if u.notReady.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	case "/name":
		io.WriteString(w, u.name)
	case "/stream":
		io.WriteString(w, "start\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	case "/headers":
		fmt.Fprintf(w, "host=%s xff=%s", r.Host, r.Header.Get("X-Forwarded-For"))
	case "/fail503":
		w.WriteHeader(http.StatusServiceUnavailable)
	case "/read-then-503":
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusServiceUnavailable)
	case "/body-progress":
		buf := make([]byte, 5)
		if _, err := io.ReadFull(r.Body, buf); err != nil {
			return
		}
		u.bodyCh <- string(buf)
		rest, _ := io.ReadAll(r.Body)
		w.Write(append(buf, rest...))
	case "/silent":
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	case "/ws":
		conn, brw, err := http.NewResponseController(w).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		brw.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: test\r\nConnection: Upgrade\r\n\r\n")
		brw.Flush()
		io.Copy(conn, brw)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (u *testUpstream) addr() string {
	return strings.TrimPrefix(u.server.URL, "http://")
}

func upstreamsYAML(upstreams ...*testUpstream) string {
	var b strings.Builder
	b.WriteString("upstreams:\n")
	for _, u := range upstreams {
		fmt.Fprintf(&b, "  - name: %s\n    addresses: [%q]\n", u.name, u.addr())
	}
	return b.String()
}

func writeConfig(t *testing.T, filename, extra string, upstreams ...*testUpstream) {
	conf := "listen-http: '127.0.0.1:0'\nlisten-admin: '127.0.0.1:0'\nhealth-check: {interval: 1h, timeout: 1s}\n" + extra + "\n" + upstreamsYAML(upstreams...)
	require.NoError(t, os.WriteFile(filename, []byte(conf), 0600))
}

func newTestServer(t *testing.T, extra string, upstreams ...*testUpstream) (*Server, string) {
	filename := filepath.Join(t.TempDir(), "config.yml")
	writeConfig(t, filename, extra, upstreams...)
	s, err := New(filename)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Serve(ctx)
	}()
	t.Cleanup(func() {
		s.Shutdown(0)
		cancel()
		<-done
	})
	waitFor(t, func() bool {
		resp, err := http.Get("http://" + s.AdminAddr() + "/v1/health")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return true
	})
	return s, filename
}

// clientFrom returns an HTTP client whose connections come from a given loopback IP, so tests
// can play many clients
func clientFrom(ip string) *http.Client {
	dialer := &net.Dialer{LocalAddr: &net.TCPAddr{IP: net.ParseIP(ip)}}
	return &http.Client{
		Transport: &http.Transport{
			DialContext:       dialer.DialContext,
			DisableKeepAlives: true,
		},
	}
}

func get(t *testing.T, client *http.Client, url string) (int, string) {
	resp, err := client.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(body)
}

func waitFor(t *testing.T, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

// testStream is an open /stream response whose end a single background reader watches
type testStream struct {
	closed chan struct{}
}

// openStream starts a stream request and returns once its first line arrived, plus a func to
// close it from the client side
func openStream(t *testing.T, client *http.Client, url string) (*testStream, func()) {
	resp, err := client.Get(url)
	require.NoError(t, err)
	r := bufio.NewReader(resp.Body)
	line, err := r.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "start\n", line)
	s := &testStream{closed: make(chan struct{})}
	go func() {
		defer close(s.closed)
		io.Copy(io.Discard, r)
	}()
	return s, func() {
		resp.Body.Close()
	}
}

// streamClosed returns true if the stream ended (EOF or error) within a second
func streamClosed(s *testStream) bool {
	select {
	case <-s.closed:
		return true
	case <-time.After(time.Second):
		return false
	}
}

func TestServer_StickyAndConsistent(t *testing.T) {
	// Each client IP sticks to one upstream, and a second load balancer with the same upstream
	// names (listed in another order) routes it to the same one
	app1, app2, app3 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2"), newTestUpstream(t, "app3")
	s1, _ := newTestServer(t, "", app1, app2, app3)
	s2, _ := newTestServer(t, "", app3, app1, app2)
	seen := make(map[string]bool)
	for i := 1; i <= 30; i++ {
		client := clientFrom(fmt.Sprintf("127.0.0.%d", i))
		_, first := get(t, client, "http://"+s1.HTTPAddr()+"/name")
		_, again := get(t, client, "http://"+s1.HTTPAddr()+"/name")
		_, other := get(t, client, "http://"+s2.HTTPAddr()+"/name")
		require.Equal(t, first, again)
		require.Equal(t, first, other)
		seen[first] = true
	}
	require.Len(t, seen, 3)
}

func TestServer_DrainUpstreamClosesStreams(t *testing.T) {
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	s, _ := newTestServer(t, "", app1, app2)
	r1, close1 := openStream(t, clientForUpstream(t, s, "app1"), "http://"+s.HTTPAddr()+"/stream")
	defer close1()
	r2, close2 := openStream(t, clientForUpstream(t, s, "app2"), "http://"+s.HTTPAddr()+"/stream")
	defer close2()
	require.NoError(t, s.balancer.DrainUpstream("app1", 0))
	waitFor(t, func() bool {
		return streamClosed(r1)
	})
	require.False(t, streamClosed(r2))
}

func TestServer_ReloadRemovesAndAddsUpstreams(t *testing.T) {
	app1, app2, app3 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2"), newTestUpstream(t, "app3")
	s, filename := newTestServer(t, "drain-duration: 1ms", app1, app2)
	r1, close1 := openStream(t, clientForUpstream(t, s, "app1"), "http://"+s.HTTPAddr()+"/stream")
	defer close1()
	r2, close2 := openStream(t, clientForUpstream(t, s, "app2"), "http://"+s.HTTPAddr()+"/stream")
	defer close2()

	// app2 leaves: its stream is drained, app1's keeps running; app3 joins
	writeConfig(t, filename, "drain-duration: 1ms", app1, app3)
	require.NoError(t, s.Reload())
	waitFor(t, func() bool {
		return streamClosed(r2)
	})
	require.False(t, streamClosed(r1))
	clientForUpstream(t, s, "app3")
	waitFor(t, func() bool {
		return len(s.balancer.Status().Upstreams) == 2
	})

	// A broken config is rejected and changes nothing
	require.NoError(t, os.WriteFile(filename, []byte("listen-http: ':80'\nupstreams: []"), 0600))
	require.Error(t, s.Reload())
	require.Len(t, s.balancer.Status().Upstreams, 2)
}

func TestServer_DrainAndShutdown(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "", app1)
	r, closeStream := openStream(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/stream")
	defer closeStream()
	done := make(chan struct{})
	go func() {
		s.Shutdown(1500 * time.Millisecond)
		close(done)
	}()
	waitFor(t, func() bool {
		status, _ := get(t, http.DefaultClient, "http://"+s.AdminAddr()+"/v1/ready")
		return status == http.StatusServiceUnavailable
	})
	waitFor(t, func() bool {
		return streamClosed(r)
	})
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not finish")
	}
}

func TestServer_HTTPSAndCertReload(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	writeTestCert(t, certFile, keyFile, "first")
	s, _ := newTestServer(t, fmt.Sprintf("listen-https: '127.0.0.1:0'\ncert-file: %s\nkey-file: %s", certFile, keyFile), app1)
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
			ForceAttemptHTTP2: true,
		},
	}
	resp, err := client.Get("https://" + s.HTTPSAddr() + "/name")
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, "app1", string(body))
	require.Equal(t, 2, resp.ProtoMajor)
	require.Equal(t, "first", resp.TLS.PeerCertificates[0].Subject.CommonName)

	// A reload picks up a renewed certificate
	writeTestCert(t, certFile, keyFile, "second")
	require.NoError(t, s.Reload())
	client.CloseIdleConnections()
	resp, err = client.Get("https://" + s.HTTPSAddr() + "/name")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, "second", resp.TLS.PeerCertificates[0].Subject.CommonName)
}

// clientForUpstream returns a client whose IP the server routes to the given upstream first
func clientForUpstream(t *testing.T, s *Server, name string) *http.Client {
	for i := 1; i < 255; i++ {
		client := clientFrom(fmt.Sprintf("127.0.1.%d", i))
		if _, body := get(t, client, "http://"+s.HTTPAddr()+"/name"); body == name {
			return client
		}
	}
	t.Fatalf("no client IP routes to %s", name)
	return nil
}

func writeTestCert(t *testing.T, certFile, keyFile, commonName string) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject: pkix.Name{
			CommonName: commonName,
		},
		NotBefore: time.Now().Add(-time.Hour),
		NotAfter:  time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600))
	require.NoError(t, os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0600))
}

func readFileString(filename string) (string, error) {
	b, err := os.ReadFile(filename)
	return string(b), err
}
