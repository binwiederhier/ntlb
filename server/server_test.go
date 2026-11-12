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
	testNtfySite     = "streams: {match: '" + testStreamsMatch + "'}"
	testNtfyUpstream = "balance: hash, healthcheck: {path: " + testHealthPath + ", interval: 1h, timeout: 1s}"
)

// siteKeys are the settings writeConfig moves from extra into the site, upstreamKeys those it
// moves into the site's upstream block
var (
	siteKeys     = []string{"streams:", "cert-file:", "key-file:", "listen-https:"}
	upstreamKeys = []string{"timeouts:", "keepalive:"}
)

func isSiteSetting(line string) bool {
	return hasAnyPrefix(line, siteKeys) || hasAnyPrefix(line, upstreamKeys)
}

func hasAnyPrefix(line string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(line, prefix) {
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
	b.WriteString("sites:\n  - name: s\n    listen-http: '127.0.0.1:0'\n")
	var upstream []string
	streams := false
	for _, setting := range settings {
		if hasAnyPrefix(setting, upstreamKeys) {
			upstream = append(upstream, setting)
			continue
		}
		b.WriteString("    " + setting + "\n")
		streams = streams || strings.HasPrefix(setting, "streams:")
	}
	if !streams {
		b.WriteString("    streams: {match: '" + testStreamsMatch + "'}\n")
	}
	b.WriteString("    upstreams:\n      default:\n        balance: hash\n        healthcheck: {path: " + testHealthPath + ", interval: 1h, timeout: 1s}\n")
	for _, setting := range upstream {
		b.WriteString("        " + setting + "\n")
	}
	b.WriteString("        servers:\n")
	for _, u := range upstreams {
		fmt.Fprintf(&b, "        - name: %s\n          addresses: [%q]\n", u.name, u.addr())
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
	require.NoError(t, os.WriteFile(filename, []byte("sites: [{name: s, listen-http: ':80', upstreams: {default: {servers: []}}}]"), 0600))
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
	siteA := fmt.Sprintf("  - {name: a, listen-http: '127.0.0.1:0', "+testNtfySite+", upstreams: {default: {"+testNtfyUpstream+", servers: [{name: app1, addresses: [%q]}]}}}\n", a1.addr())
	siteB := fmt.Sprintf("  - {name: b, listen-http: '127.0.0.2:0', "+testNtfySite+", upstreams: {default: {"+testNtfyUpstream+", servers: [{name: app1, addresses: [%q]}]}}}\n", b1.addr())
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
	write(fmt.Sprintf("  - {name: a, listen-https: '127.0.0.1:0', cert-file: %s, key-file: %s, upstreams: {default: {servers: [{name: app1, addresses: [%q]}]}}}\n", filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem"), a1.addr()))
	_, err = s.Reload()
	require.ErrorContains(t, err, "restart")

	// A new address that cannot be bound rejects the reload
	write(siteA + "  - {name: c, listen-http: '192.0.2.1:80', upstreams: {default: {servers: [{name: x, addresses: ['127.0.0.1:1']}]}}}\n")
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
sites: [{name: a, listen-http: '127.0.0.1:0', `+testNtfySite+`, upstreams: {default: {`+testNtfyUpstream+`, servers: [{name: app1, addresses: [%q]}]}}}]
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

	require.NoError(t, os.WriteFile(filepath.Join(dir, "sites.d", "b.yml"), []byte(fmt.Sprintf("sites: [{name: b, listen-http: '127.0.0.1:0', hosts: [b.com], "+testNtfySite+", upstreams: {default: {"+testNtfyUpstream+", servers: [{name: app1, addresses: [%q]}]}}}]\n", b1.addr())), 0600))
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

// ntfyClient sends requests for ntfy.sh to a server from one loopback IP, over HTTP or HTTPS,
// without following redirects
type ntfyClient struct {
	t      *testing.T
	s      *Server
	client *http.Client
}

func newNtfyClient(t *testing.T, s *Server, ip string) *ntfyClient {
	dialer := &net.Dialer{
		LocalAddr: &net.TCPAddr{IP: net.ParseIP(ip)},
	}
	return &ntfyClient{
		t: t,
		s: s,
		client: &http.Client{
			Transport: &http.Transport{
				DialContext:       dialer.DialContext,
				DisableKeepAlives: true,
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true,
					ServerName:         "ntfy.sh",
				},
			},
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

// do sends a request and returns the response with its body read
func (c *ntfyClient) do(method, scheme, target, body string, header ...string) (*http.Response, string) {
	c.t.Helper()
	addr := c.s.HTTPAddr()
	if scheme == "https" {
		addr = c.s.HTTPSAddr()
	}
	req, err := http.NewRequest(method, scheme+"://"+addr+target, strings.NewReader(body))
	require.NoError(c.t, err)
	req.Host = "ntfy.sh"
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	resp, err := c.client.Do(req)
	require.NoError(c.t, err)
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	require.NoError(c.t, err)
	return resp, string(b)
}

// newNtfyPackagedServer runs the shipped config (packaging/config.yml and sites.d/ntfy.yml) as is,
// but for what a test cannot have: addresses, files and certificates in a temporary directory,
// the test upstreams, loopback IPs in the limit exemptions, and a small limit burst
func newNtfyPackagedServer(t *testing.T, upstreams ...*testUpstream) (*Server, string) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "sites.d"), 0755))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "html", "_next"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "html", "index.html"), []byte("<html>the website</html>"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "html", "_next", "app.js"), []byte("console.log('app')"), 0644))
	writeTestCert(t, filepath.Join(dir, "fullchain.pem"), filepath.Join(dir, "privkey.pem"), "ntfy.sh")
	main, err := os.ReadFile("../packaging/config.yml")
	require.NoError(t, err)
	site, err := os.ReadFile("../packaging/sites.d/ntfy.yml")
	require.NoError(t, err)
	mainConf := strings.NewReplacer(
		`listen-admin: "127.0.0.1:2590"`, `listen-admin: "127.0.0.1:0"`,
		"/var/log/ntlb/", dir+"/",
	).Replace(string(main))
	var servers strings.Builder
	for _, u := range upstreams {
		fmt.Fprintf(&servers, "          - name: %s\n            addresses: [%q]\n", u.name, u.addr())
	}
	siteConf := strings.NewReplacer(
		`listen-http: ":80"`, `listen-http: "127.0.0.1:0"`,
		`listen-https: ":443"`, `listen-https: "127.0.0.3:0"`,
		"/etc/ntlb/certs/", dir+"/",
		"/var/www/website/html", filepath.Join(dir, "html"),
		`"198.51.100.7", "203.0.113.0/24"`, `"127.0.0.41", "203.0.113.0/24"`,
		"burst: 1000", "burst: 3",
	).Replace(string(site))
	i := strings.Index(siteConf, "        servers:\n")
	j := strings.Index(siteConf, "\n\n    # Rules of this site")
	require.True(t, i > 0 && j > i, "servers section not found")
	siteConf = siteConf[:i] + "        servers:\n" + servers.String() + siteConf[j+1:]
	require.NoError(t, os.WriteFile(filepath.Join(dir, "config.yml"), []byte(mainConf), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "sites.d", "ntfy.yml"), []byte(siteConf), 0600))
	return startTestServer(t, filepath.Join(dir, "config.yml")), dir
}

// stream opens a subscription and returns once its first line arrived, plus a func to close it
func (c *ntfyClient) stream(scheme, target string) func() {
	c.t.Helper()
	addr := c.s.HTTPAddr()
	if scheme == "https" {
		addr = c.s.HTTPSAddr()
	}
	req, err := http.NewRequest(http.MethodGet, scheme+"://"+addr+target, nil)
	require.NoError(c.t, err)
	req.Host = "ntfy.sh"
	resp, err := c.client.Do(req)
	require.NoError(c.t, err)
	line, err := bufio.NewReader(resp.Body).ReadString('\n')
	require.NoError(c.t, err)
	require.Equal(c.t, "start\n", line)
	return func() {
		resp.Body.Close()
	}
}

func TestServer_NtfyPackagedConfig(t *testing.T) {
	// The ntfy.sh setup end to end, as shipped, with three fake ntfy nodes
	app1, app3, app5 := newTestUpstream(t, "app1"), newTestUpstream(t, "app3"), newTestUpstream(t, "app5")
	for _, u := range []*testUpstream{app1, app3, app5} {
		name := u.name
		u.on("/mytopic", func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			fmt.Fprintf(w, "%s %s %s", name, r.Method, body)
		})
		u.on("/polltopic/json", func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, "polled") // Whatever the poll flag, so a misclassified poll cannot hang
		})
		u.on("/v1/stats", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"messages":`+strings.Repeat("1", 2000)+`}`)
		})
	}
	s, dir := newNtfyPackagedServer(t, app1, app3, app5)
	metrics := func() string {
		_, m := adminDo(t, s, http.MethodGet, "/metrics", "")
		return m
	}
	readLog := func(name string) string {
		b, _ := os.ReadFile(filepath.Join(dir, name))
		return string(b)
	}

	t.Run("subscriptions are streams, polls are not", func(t *testing.T) {
		c := newNtfyClient(t, s, "127.0.0.11")
		closeHTTPS := c.stream("https", "/mytopic/json")
		closeHTTP := c.stream("http", "/a,b_c-d/json?since=10m") // Not redirected: old clients subscribe over HTTP
		defer closeHTTPS()
		defer closeHTTP()
		require.Contains(t, metrics(), `ntlb_streams{protocol="http1"} 2`)
		poller := newNtfyClient(t, s, "127.0.0.14") // Another client: 5 requests at once would hit the limit
		for _, poll := range [][]string{{"/polltopic/json?poll=1"}, {"/polltopic/json?po=1"}, {"/polltopic/json", "X-Poll", "1"}} {
			resp, body := poller.do(http.MethodGet, "https", poll[0], "", poll[1:]...)
			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.Equal(t, "polled", body)
		}
		require.Contains(t, metrics(), `ntlb_streams{protocol="http1"} 2`)
		require.Regexp(t, `ntlb_requests_total\{code="200",kind="request",site="ntfy",upstream="app\d"\} 3`, metrics())
	})

	t.Run("publishes, any method, sticky per client", func(t *testing.T) {
		c := newNtfyClient(t, s, "127.0.0.12")
		_, first := c.do(http.MethodPost, "https", "/mytopic", "hello")
		node := strings.Fields(first)[0]
		require.Equal(t, node+" POST hello", first)
		_, body := c.do(http.MethodPut, "http", "/mytopic", "over http") // Publishing over HTTP is not redirected
		require.Equal(t, node+" PUT over http", body)
		nodes := make(map[string]bool)
		for i := 100; i < 130; i++ {
			_, body := newNtfyClient(t, s, fmt.Sprintf("127.0.2.%d", i)).do(http.MethodPost, "https", "/mytopic", "x")
			nodes[strings.Fields(body)[0]] = true
		}
		require.Len(t, nodes, 3) // Clients spread over all nodes
	})

	t.Run("redirects and the website", func(t *testing.T) {
		c := newNtfyClient(t, s, "127.0.0.13")
		hits := app1.hits.Load() + app3.hits.Load() + app5.hits.Load()
		for target, location := range map[string]string{
			"/docs":          "https://docs.ntfy.sh/",
			"/docs/publish/": "https://docs.ntfy.sh/publish/",
		} {
			for _, scheme := range []string{"http", "https"} {
				resp, _ := c.do(http.MethodGet, scheme, target, "")
				require.Equal(t, http.StatusFound, resp.StatusCode, target)
				require.Equal(t, location, resp.Header.Get("Location"), target)
			}
		}
		for _, target := range []string{"/", "/mytopic", "/static/x.css", "/_next/app.js"} {
			resp, _ := c.do(http.MethodGet, "http", target, "")
			require.Equal(t, http.StatusFound, resp.StatusCode, target)
			require.Equal(t, "https://ntfy.sh"+target, resp.Header.Get("Location"), target)
		}
		resp, body := c.do(http.MethodGet, "https", "/", "")
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Equal(t, "<html>the website</html>", body)
		resp, body = c.do(http.MethodGet, "https", "/_next/app.js", "")
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Equal(t, "console.log('app')", body)
		resp, _ = c.do(http.MethodGet, "https", "/_next/missing.js", "")
		require.Equal(t, http.StatusNotFound, resp.StatusCode)
		require.Equal(t, hits, app1.hits.Load()+app3.hits.Load()+app5.hits.Load()) // None reached a node
	})

	t.Run("abusive topics are blocked and banned", func(t *testing.T) {
		resp, _ := newNtfyClient(t, s, "127.0.0.21").do(http.MethodGet, "https", "/spam-offers/json", "")
		require.Equal(t, http.StatusForbidden, resp.StatusCode)
		resp, _ = newNtfyClient(t, s, "127.0.0.22").do(http.MethodPost, "https", "/news,bad-bets", "x") // Inside a topic list
		require.Equal(t, http.StatusForbidden, resp.StatusCode)
		resp, _ = newNtfyClient(t, s, "127.0.0.22").do(http.MethodGet, "https", "/bad-betsy/json?poll=1", "") // A near miss
		require.Equal(t, http.StatusOK, resp.StatusCode)
		waitFor(t, func() bool {
			bans := readLog("ban.log")
			return strings.Contains(bans, " 127.0.0.21 127.0.0.21/32 ntfy/abuse-topics\n") &&
				strings.Contains(bans, " 127.0.0.22 127.0.0.22/32 ntfy/abuse-topics\n")
		})

		// 150+ topics at once: blocked, 20 strikes, not banned yet
		topics := strings.TrimSuffix(strings.Repeat("t,", 151), ",")
		resp, _ = newNtfyClient(t, s, "127.0.0.23").do(http.MethodGet, "https", "/"+topics+"/json", "")
		require.Equal(t, http.StatusForbidden, resp.StatusCode)

		// The global abuse rule, for every site
		resp, _ = newNtfyClient(t, s, "127.0.0.24").do(http.MethodGet, "https", "/mytopic/json?poll=1", "", "User-Agent", "NTFY-Spy/1.0")
		require.Equal(t, http.StatusForbidden, resp.StatusCode)
		waitFor(t, func() bool {
			return strings.Contains(readLog("access_error.log"), `"GET /mytopic/json?poll=1 HTTP/1.1" 403`)
		})
		require.NotContains(t, readLog("ban.log"), "127.0.0.23")
		require.Contains(t, metrics(), `ntlb_rule_matches_total{rule="ntfy/abuse-topic-count"} 1`)
		require.Contains(t, metrics(), `ntlb_rule_matches_total{rule="abuse"} 1`)
	})

	t.Run("the request limit, and its exemptions", func(t *testing.T) {
		c := newNtfyClient(t, s, "127.0.0.31")
		for range 4 { // burst 3: 4 at once
			resp, _ := c.do(http.MethodPost, "https", "/mytopic", "x")
			require.Equal(t, http.StatusOK, resp.StatusCode)
		}
		resp, _ := c.do(http.MethodPost, "https", "/mytopic", "x")
		require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode) // nginx's limit_req status
		exempt := newNtfyClient(t, s, "127.0.0.41")
		for range 10 {
			resp, _ := exempt.do(http.MethodPost, "https", "/mytopic", "x")
			require.Equal(t, http.StatusOK, resp.StatusCode)
		}
		require.Contains(t, metrics(), `ntlb_rule_matches_total{rule="ntfy/limit"} 1`)
	})

	t.Run("gzip for large JSON, never for subscriptions", func(t *testing.T) {
		c := newNtfyClient(t, s, "127.0.0.51")
		resp, _ := c.do(http.MethodGet, "https", "/v1/stats", "", "Accept-Encoding", "gzip")
		require.Equal(t, "gzip", resp.Header.Get("Content-Encoding"))
		resp, _ = c.do(http.MethodGet, "https", "/mytopic/json?poll=1", "", "Accept-Encoding", "gzip")
		require.Empty(t, resp.Header.Get("Content-Encoding"))
	})

	t.Run("garbage is a 400, logged and struck", func(t *testing.T) {
		require.Equal(t, "HTTP/1.1 400 Bad Request", sendRaw(t, s.HTTPAddr(), "\x16\x03\x01garbage\r\n\r\n"))
		waitFor(t, func() bool {
			return strings.Contains(readLog("access_error.log"), `"-" 400 0 "-" "-" http_host=- site=-`)
		})
	})

	t.Run("access logs split by status", func(t *testing.T) {
		waitFor(t, func() bool {
			return strings.Contains(readLog("access.log"), `"POST /mytopic HTTP/1.1" 200`)
		})
		require.NotContains(t, readLog("access.log"), `" 403 `)
		require.NotContains(t, readLog("access_error.log"), `" 200 `)
	})
}

func TestServer_HotReloadUpstreamSettings(t *testing.T) {
	// Backups, weights and timeouts change on reload for new requests; an open stream stays
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	for _, u := range []*testUpstream{app1, app2} {
		u.on("/slow", func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(300 * time.Millisecond)
			io.WriteString(w, "slow")
		})
	}
	filename := filepath.Join(t.TempDir(), "config.yml")
	write := func(upstream string) {
		conf := fmt.Sprintf(`listen-admin: '127.0.0.1:0'
sites:
  - name: s
    listen-http: '127.0.0.1:0'
    upstreams:
      default:
        balance: round-robin
        healthcheck: {path: /v1/ready, interval: 1h, timeout: 1s}
%s`, upstream)
		require.NoError(t, os.WriteFile(filename, []byte(conf), 0600))
	}
	servers := func(app1Settings, app2Settings string) string {
		return fmt.Sprintf("        servers:\n          - {name: app1, addresses: [%q]%s}\n          - {name: app2, addresses: [%q]%s}\n", app1.addr(), app1Settings, app2.addr(), app2Settings)
	}
	picks := func(s *Server, n int) map[string]int {
		counts := make(map[string]int)
		for range n {
			_, name := get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/name")
			counts[name]++
		}
		return counts
	}
	write(servers("", ", backup: true"))
	s := startTestServer(t, filename)
	stream, closeStream := openStream(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/stream") // On app1
	defer closeStream()
	require.Equal(t, map[string]int{"app1": 10}, picks(s, 10))

	// app2 no backup any more, with 3 times app1's share
	write(servers("", ", weight: 3"))
	requireReload(t, s)
	require.Equal(t, map[string]int{"app1": 2, "app2": 6}, picks(s, 8))

	// app1 the backup now
	write(servers(", backup: true", ""))
	requireReload(t, s)
	require.Equal(t, map[string]int{"app2": 10}, picks(s, 10))
	status, _ := get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/slow")
	require.Equal(t, http.StatusOK, status)

	// A shorter read timeout: the slow answer is a 504 now
	write("        timeouts: {read: 100ms}\n" + servers(", backup: true", ""))
	requireReload(t, s)
	status, _ = get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/slow")
	require.Equal(t, http.StatusGatewayTimeout, status)

	// The stream on app1 outlived all of it: neither backup nor timeout settings move it
	require.False(t, streamClosed(stream))
}

func TestServer_BackupTakesOverWhenOthersFail(t *testing.T) {
	// A backup is the last retry when the others fail before the health checks noticed (taking
	// over once they are unhealthy: balancer.TestStrategy_Backup)
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	filename := filepath.Join(t.TempDir(), "config.yml")
	require.NoError(t, os.WriteFile(filename, []byte(fmt.Sprintf(`listen-admin: '127.0.0.1:0'
sites:
  - name: s
    listen-http: '127.0.0.1:0'
    upstreams:
      default:
        healthcheck: {path: /v1/ready, interval: 1h, timeout: 1s, fails: 1}
        servers:
          - {name: app1, addresses: [%q]}
          - {name: spare, addresses: [%q], backup: true}
`, app1.addr(), app2.addr())), 0600))
	s := startTestServer(t, filename)
	_, name := get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/name")
	require.Equal(t, "app1", name)

	// app1 goes away: the very next request is retried on the backup
	app1.server.Close()
	_, name = get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/name")
	require.Equal(t, "app2", name)
	_, metrics := adminDo(t, s, http.MethodGet, "/metrics", "")
	require.Contains(t, metrics, `ntlb_retries_total{reason="connect"} 1`)
}

func TestServer_InformationalResponseIsNotTheStatus(t *testing.T) {
	// A 103 Early Hints before the final 404: metrics, the access log and response rules see 404
	app1 := newRawUpstream(t, "app1", func(conn net.Conn) {
		head, _ := readRequestHead(conn)
		if answerReady(conn, head) {
			return
		}
		io.WriteString(conn, "HTTP/1.1 103 Early Hints\r\nLink: </x>\r\n\r\nHTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nContent-Length: 3\r\n\r\nno!")
	})
	accessLog := filepath.Join(t.TempDir(), "access.log")
	s, _ := newTestServer(t, "access-logs: [{file: "+accessLog+", match: 'status == 404'}]", app1)
	status, body := get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/missing")
	require.Equal(t, http.StatusNotFound, status)
	require.Equal(t, "no!", body)
	waitFor(t, func() bool {
		b, _ := os.ReadFile(accessLog)
		return strings.Contains(string(b), `"GET /missing HTTP/1.1" 404 3`)
	})
	_, metrics := adminDo(t, s, http.MethodGet, "/metrics", "")
	require.Contains(t, metrics, `ntlb_requests_total{code="404",kind="request",site="s",upstream="app1"} 1`)
	require.NotContains(t, metrics, `code="103"`)
}
