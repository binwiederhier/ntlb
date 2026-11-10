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
	"runtime"
	"strconv"
	"strings"
	"sync"
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
//	every request  records lastProto, lastHeaders and lastRequest (except /v1/ready)
//	on(path, h)    registers a handler that takes precedence over everything below
//	/bcast/json    a subscription that gets every message sent to broadcast
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
	lastRequest atomic.Value // *http.Request, a clone without the body
	handlers    sync.Map     // path -> http.HandlerFunc, checked before the paths above
	broadcast   *testBroadcast
}

// testBroadcast fans messages out to every open /bcast/json subscription of an upstream
type testBroadcast struct {
	subscribers map[chan string]struct{}
	mu          sync.Mutex
}

func (b *testBroadcast) subscribe() chan string {
	ch := make(chan string, 100)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subscribers[ch] = struct{}{}
	return ch
}

func (b *testBroadcast) unsubscribe(ch chan string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.subscribers, ch)
}

func (b *testBroadcast) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.subscribers)
}

func (b *testBroadcast) send(m string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.subscribers {
		ch <- m
	}
}

// on registers a handler for a path, overriding the built-in ones
func (u *testUpstream) on(path string, h http.HandlerFunc) {
	u.handlers.Store(path, h)
}

// testStreamEnd sent to testUpstream.push ends the subscription from the upstream side
const (
	testStreamEnd = "!end"
)

func newTestUpstream(t *testing.T, name string) *testUpstream {
	u := &testUpstream{
		name:   name,
		bodyCh: make(chan string, 10),
		push:   make(chan string, 10),
		broadcast: &testBroadcast{
			subscribers: make(map[chan string]struct{}),
		},
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
		u.lastProto.Store(r.Proto)
		u.lastHeaders.Store(fmt.Sprintf("host=%s xff=%s", r.Host, r.Header.Get("X-Forwarded-For")))
		u.lastRequest.Store(r.Clone(context.Background()))
	}
	if h, ok := u.handlers.Load(r.URL.Path); ok {
		h.(http.HandlerFunc)(w, r)
		return
	}
	if r.URL.Path == "/bcast/json" {
		ch := u.broadcast.subscribe()
		defer u.broadcast.unsubscribe(ch)
		w.Header().Set("Content-Type", "application/x-ndjson")
		io.WriteString(w, "start\n")
		w.(http.Flusher).Flush()
		for {
			select {
			case <-r.Context().Done():
				return
			case m := <-ch:
				io.WriteString(w, m+"\n")
				w.(http.Flusher).Flush()
			}
		}
	}
	if strings.HasSuffix(r.URL.Path, "/json") { // An ntfy subscription, see the doc comment above
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

// A site set up like ntfy's (packaging/sites.d/ntfy.yml): its subscriptions as streams, its
// readiness probe, hashing; the tests play ntfy
const (
	testStreamsMatch = `method == "GET" && path matches "^/[-_A-Za-z0-9]{1,64}(,[-_A-Za-z0-9]{1,64})*/(json|sse|raw|ws)$" && !flag("x-poll", "poll", "po")`
	testHealthPath   = "/v1/ready" // ntfy's readiness, which the test upstreams answer
	testNtfySite     = "health-check: {path: " + testHealthPath + ", interval: 1h, timeout: 1s}, balance: hash, streams: {match: '" + testStreamsMatch + "'}"
)

// siteKeys are the settings writeConfig moves from extra into the site
var (
	siteKeys = []string{"streams:", "cert-file:", "key-file:", "listen-https:"}
)

func isSiteSetting(line string) bool {
	for _, key := range siteKeys {
		if strings.HasPrefix(line, key) {
			return true
		}
	}
	return false
}

// upstreamsYAML is a site "s" with the upstreams
func upstreamsYAML(upstreams ...*testUpstream) string {
	return siteYAML(nil, upstreams...)
}

// siteYAML is a site "s" with the settings (one per line) and the upstreams, set up like ntfy's
// (readiness path, hashing, its subscriptions as streams, unless the settings have streams)
func siteYAML(settings []string, upstreams ...*testUpstream) string {
	var b strings.Builder
	b.WriteString("sites:\n  - name: s\n    listen-http: '127.0.0.1:0'\n    health-check: {path: " + testHealthPath + ", interval: 1h, timeout: 1s}\n    balance: hash\n")
	streams := false
	for _, setting := range settings {
		b.WriteString("    " + setting + "\n")
		streams = streams || strings.HasPrefix(setting, "streams:")
	}
	if !streams {
		b.WriteString("    streams: {match: '" + testStreamsMatch + "'}\n")
	}
	b.WriteString("    upstreams:\n")
	for _, u := range upstreams {
		fmt.Fprintf(&b, "      - name: %s\n        addresses: [%q]\n", u.name, u.addr())
	}
	return b.String()
}

// writeConfig writes a config with the global settings in extra and one site "s" with the
// upstreams; the siteKeys lines of extra go into the site
func writeConfig(t *testing.T, filename, extra string, upstreams ...*testUpstream) {
	var global, site []string
	for _, line := range strings.Split(extra, "\n") {
		if isSiteSetting(line) {
			site = append(site, line)
		} else {
			global = append(global, line)
		}
	}
	conf := "listen-admin: '127.0.0.1:0'\n" + strings.Join(global, "\n") + "\n" + siteYAML(site, upstreams...)
	require.NoError(t, os.WriteFile(filename, []byte(conf), 0600))
}

func newTestServer(t *testing.T, extra string, upstreams ...*testUpstream) (*Server, string) {
	filename := filepath.Join(t.TempDir(), "config.yml")
	writeConfig(t, filename, extra, upstreams...)
	return startTestServer(t, filename), filename
}

// startTestServer runs a server from a config file until the test ends
func startTestServer(t *testing.T, filename string) *Server {
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
	return s
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
	requireReload(t, s)
	waitFor(t, func() bool {
		return streamClosed(r2)
	})
	require.False(t, streamClosed(r1))
	clientForUpstream(t, s, "app3")
	waitFor(t, func() bool {
		return len(s.balancer.Status().Upstreams) == 2
	})

	// A broken config is rejected and changes nothing
	require.NoError(t, os.WriteFile(filename, []byte("sites: [{name: s, listen-http: ':80', upstreams: []}]"), 0600))
	_, err := s.Reload()
	require.Error(t, err)
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
	s, _ := newTestServer(t, fmt.Sprintf("listen-https: '127.0.0.3:0'\ncert-file: %s\nkey-file: %s", certFile, keyFile), app1)
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
	requireReload(t, s)
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
		DNSNames:  []string{commonName},
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

func requireReload(t *testing.T, s *Server) {
	t.Helper()
	restart, err := s.Reload()
	require.NoError(t, err)
	require.Empty(t, restart)
}

func TestServer_ReloadReportsRestartRequired(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	s, filename := newTestServer(t, "", app1)
	conf := "listen-admin: '127.0.0.1:0'\nclient-keepalive: {idle: 7m}\ndrain-duration: 9m\n" + upstreamsYAML(app1)
	require.NoError(t, os.WriteFile(filename, []byte(conf), 0600))
	restart, err := s.Reload()
	require.NoError(t, err)
	require.Equal(t, []string{"client-keepalive"}, restart)
	require.Equal(t, 9*time.Minute, s.config.Load().DrainDuration) // The rest is applied
	status, _ := get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/name")
	require.Equal(t, http.StatusOK, status)
}

func TestServer_ReloadOpensAndClosesListeners(t *testing.T) {
	// A site on a new address is served right after the reload; an address no site uses any more
	// stops taking connections, while the streams on it keep running
	a1, b1 := newTestUpstream(t, "app1"), newTestUpstream(t, "app1")
	dir := t.TempDir()
	filename := filepath.Join(dir, "config.yml")
	siteA := fmt.Sprintf("  - {name: a, listen-http: '127.0.0.1:0', "+testNtfySite+", upstreams: [{name: app1, addresses: [%q]}]}\n", a1.addr())
	siteB := fmt.Sprintf("  - {name: b, listen-http: '127.0.0.2:0', "+testNtfySite+", upstreams: [{name: app1, addresses: [%q]}]}\n", b1.addr())
	write := func(sites string) {
		require.NoError(t, os.WriteFile(filename, []byte("listen-admin: '127.0.0.1:0'\nsites:\n"+sites), 0600))
	}
	write(siteA)
	s := startTestServer(t, filename)
	require.Empty(t, s.ListenerAddr("127.0.0.2:0"))

	// Added
	write(siteA + siteB)
	requireReload(t, s)
	addrB := s.ListenerAddr("127.0.0.2:0")
	require.NotEmpty(t, addrB)
	status, _ := get(t, http.DefaultClient, "http://"+addrB+"/name")
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, int64(1), b1.hits.Load())
	stream, closeStream := openStream(t, http.DefaultClient, "http://"+addrB+"/bcast/json")
	defer closeStream()

	// Removed: no new connections, the open stream stays
	write(siteA)
	requireReload(t, s)
	require.Empty(t, s.ListenerAddr("127.0.0.2:0"))
	_, err := http.Get("http://" + addrB + "/name")
	require.Error(t, err)
	select {
	case <-stream.closed:
		t.Fatal("stream on the removed listener was cut")
	case <-time.After(300 * time.Millisecond):
	}
	require.Equal(t, 1, s.balancer.Status().Streams)
	status, _ = get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/name")
	require.Equal(t, http.StatusOK, status)

	// A kept-alive connection to the removed address is closed (HTTP/2: GOAWAY), and the removed
	// server is forgotten once its last connection is gone; a stream it hijacked runs on
	write(siteA + siteB)
	requireReload(t, s)
	addrB = s.ListenerAddr("127.0.0.2:0")
	idle, err := net.Dial("tcp", addrB)
	require.NoError(t, err)
	defer idle.Close()
	fmt.Fprint(idle, "GET /name HTTP/1.1\r\nHost: x\r\n\r\n")
	resp, err := http.ReadResponse(bufio.NewReader(idle), nil)
	require.NoError(t, err)
	io.ReadAll(resp.Body)
	resp.Body.Close()
	write(siteA)
	requireReload(t, s)
	idle.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, err = idle.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF)
	waitFor(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return len(s.retired) == 0
	})
	require.Equal(t, 1, s.balancer.Status().Streams) // The first stream, hijacked

	// An address cannot switch between HTTP and HTTPS without a restart
	writeTestCert(t, filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem"), "a.com")
	write(fmt.Sprintf("  - {name: a, listen-https: '127.0.0.1:0', cert-file: %s, key-file: %s, upstreams: [{name: app1, addresses: [%q]}]}\n", filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem"), a1.addr()))
	_, err = s.Reload()
	require.ErrorContains(t, err, "restart")

	// A new address that cannot be bound rejects the reload
	write(siteA + "  - {name: c, listen-http: '192.0.2.1:80', upstreams: [{name: x, addresses: ['127.0.0.1:1']}]}\n")
	_, err = s.Reload()
	require.Error(t, err)
	require.Len(t, s.config.Load().Sites, 1)
}

func TestServer_ReloadAppliesStreamsAndHash(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	s, filename := newTestServer(t, "", app1)
	_, closeStream := openStream(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/mytopic/json")
	closeStream()
	require.Equal(t, "HTTP/1.0", app1.lastProto.Load())
	writeConfig(t, filename, "streams: {match: 'false'}\nclient-key: {ipv4-prefix-bits: 8}", app1)
	requireReload(t, s)
	_, closeStream = openStream(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/mytopic/json")
	closeStream()
	require.Equal(t, "HTTP/1.1", app1.lastProto.Load())
	require.Equal(t, 8, s.config.Load().ClientKey.IPv4PrefixBits)
}

// newRawUpstream is an upstream that hands every connection to handle, for malformed or
// misbehaving responses. Health checks get garbage too, so it is used fail-open.
func newRawUpstream(t *testing.T, name string, handle func(conn net.Conn)) *testUpstream {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				handle(conn)
			}()
		}
	}()
	t.Cleanup(func() {
		ln.Close()
	})
	return &testUpstream{
		name: name,
		server: &httptest.Server{
			URL: "http://" + ln.Addr().String(),
		},
	}
}

// readRequestHead reads a request's head from a raw upstream connection
func readRequestHead(conn net.Conn) (string, *bufio.Reader) {
	r := bufio.NewReader(conn)
	var head strings.Builder
	for {
		line, err := r.ReadString('\n')
		head.WriteString(line)
		if err != nil || line == "\r\n" {
			return head.String(), r
		}
	}
}

// answerReady answers a raw upstream's health check, returning true if head was one
func answerReady(conn net.Conn, head string) bool {
	if !strings.HasPrefix(head, "GET /v1/ready ") {
		return false
	}
	io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
	return true
}

func TestServer_ShutdownAfterContextCanceled(t *testing.T) {
	// Even with the background loops stopped (Serve's context canceled first), Shutdown closes
	// every stream, detached ones included, and returns
	app1 := newTestUpstream(t, "app1")
	filename := filepath.Join(t.TempDir(), "config.yml")
	writeConfig(t, filename, "", app1)
	s, err := New(filename)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	go s.Serve(ctx)
	waitFor(t, func() bool {
		return s.HTTPAddr() != "" && s.balancer.Healthy()
	})
	_, r := openSubscription(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/mytopic/json")
	_, closeStream := openStream(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/stream")
	defer closeStream()
	cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Shutdown(100 * time.Millisecond)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Shutdown did not return")
	}
	_, err = r.ReadString('\n')
	require.Error(t, err)
	require.Zero(t, s.balancer.Status().Streams)
}

func TestServer_NewFailureLeaksNothing(t *testing.T) {
	// A listener that cannot bind fails New, and closes what New had opened already
	app1 := newTestUpstream(t, "app1")
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer busy.Close()
	filename := filepath.Join(t.TempDir(), "config.yml")
	conf := fmt.Sprintf("listen-admin: %q\naccess-logs: [{file: %s}]\n%s", busy.Addr().String(), filepath.Join(t.TempDir(), "access.log"), upstreamsYAML(app1))
	require.NoError(t, os.WriteFile(filename, []byte(conf), 0600))
	before := runtime.NumGoroutine()
	for i := 0; i < 10; i++ {
		_, err := New(filename)
		require.Error(t, err)
	}
	waitFor(t, func() bool {
		return runtime.NumGoroutine() <= before
	})
}

func TestServer_ReloadAddsSiteFile(t *testing.T) {
	// A site file dropped into an include directory takes traffic after a reload
	a1, b1 := newTestUpstream(t, "app1"), newTestUpstream(t, "app1")
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "sites.d"), 0700))
	filename := filepath.Join(dir, "config.yml")
	require.NoError(t, os.WriteFile(filename, []byte(fmt.Sprintf(`
listen-admin: '127.0.0.1:0'
include: [sites.d/*.yml]
sites: [{name: a, listen-http: '127.0.0.1:0', `+testNtfySite+`, upstreams: [{name: app1, addresses: [%q]}]}]
`, a1.addr())), 0600))
	s := startTestServer(t, filename)
	getHost := func(host string) {
		req, err := http.NewRequest(http.MethodGet, "http://"+s.HTTPAddr()+"/name", nil)
		require.NoError(t, err)
		req.Host = host
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		io.ReadAll(resp.Body)
		resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}
	getHost("b.com")
	require.Equal(t, int64(1), a1.hits.Load())

	require.NoError(t, os.WriteFile(filepath.Join(dir, "sites.d", "b.yml"), []byte(fmt.Sprintf("sites: [{name: b, listen-http: '127.0.0.1:0', hosts: [b.com], "+testNtfySite+", upstreams: [{name: app1, addresses: [%q]}]}]\n", b1.addr())), 0600))
	requireReload(t, s)
	getHost("b.com")
	require.Equal(t, int64(1), b1.hits.Load())
	getHost("a.com")
	require.Equal(t, int64(2), a1.hits.Load())
	require.Len(t, s.balancer.Status().Upstreams, 2)
}

func TestServer_NoReloadWhileShuttingDown(t *testing.T) {
	// A reload during the shutdown drain is refused: it would open listeners and log files that
	// the shutdown already passed over
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "", app1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Shutdown(500 * time.Millisecond)
	}()
	waitFor(t, func() bool {
		return s.balancer.Draining()
	})
	_, err := s.Reload()
	require.ErrorContains(t, err, "shutting down")
	<-done
}

func TestServer_InvalidHostOrTargetRejected(t *testing.T) {
	// HTTP/1.1 rejects these in Go's parser; HTTP/2 lets them through as :authority and :path, so
	// the load balancer checks them itself: 400, like nginx, logged and struck as such
	for name, mutate := range map[string]func(r *http.Request){
		"space in path":     func(r *http.Request) { r.RequestURI = "/x?a b" },
		"not origin-form":   func(r *http.Request) { r.RequestURI = "x/y" },
		"slash in host":     func(r *http.Request) { r.Host = "evil.com/.ntfy.sh" },
		"space in host":     func(r *http.Request) { r.Host = "a b" },
		"quote in host":     func(r *http.Request) { r.Host = `a"b` },
		"bad port":          func(r *http.Request) { r.Host = "ntfy.sh:x" },
		"control character": func(r *http.Request) { r.RequestURI = "/x\x7f" },
	} {
		r := httptest.NewRequest("GET", "/x", nil)
		mutate(r)
		require.False(t, validRequest(r), name)
	}
	for name, mutate := range map[string]func(r *http.Request){
		"plain":            func(r *http.Request) {},
		"host with port":   func(r *http.Request) { r.Host = "ntfy.sh:443" },
		"ipv6 host":        func(r *http.Request) { r.Host = "[2001:db8::1]:443" },
		"no host":          func(r *http.Request) { r.Host = "" },
		"escaped and utf8": func(r *http.Request) { r.RequestURI = "/a%20b/json?x=\xc3\xa4&y=%22" },
		"raw quote":        func(r *http.Request) { r.RequestURI = `/mytopic?message="hi"` }, // Scripts do; ntfy takes it
		"asterisk options": func(r *http.Request) { r.Method, r.RequestURI = "OPTIONS", "*" },
	} {
		r := httptest.NewRequest("GET", "/x", nil)
		mutate(r)
		require.True(t, validRequest(r), name)
	}
}
