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
	"regexp"
	"runtime"
	"runtime/debug"
	"strconv"
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

func newTestUpstream(t testing.TB, name string) *testUpstream {
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
func writeConfig(t testing.TB, filename, extra string, upstreams ...*testUpstream) {
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

func newTestServer(t testing.TB, extra string, upstreams ...*testUpstream) (*Server, string) {
	filename := filepath.Join(t.TempDir(), "config.yml")
	writeConfig(t, filename, extra, upstreams...)
	return startTestServer(t, filename), filename
}

// startTestServer runs a server from a config file until the test ends
func startTestServer(t testing.TB, filename string) *Server {
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

func get(t testing.TB, client *http.Client, url string) (int, string) {
	resp, err := client.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(body)
}

func waitFor(t testing.TB, f func() bool) {
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
func openStream(t testing.TB, client *http.Client, url string) (*testStream, func()) {
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
func clientForUpstream(t testing.TB, s *Server, name string) *http.Client {
	for i := 1; i < 255; i++ {
		client := clientFrom(fmt.Sprintf("127.0.1.%d", i))
		if _, body := get(t, client, "http://"+s.HTTPAddr()+"/name"); body == name {
			return client
		}
	}
	t.Fatalf("no client IP routes to %s", name)
	return nil
}

func writeTestCert(t testing.TB, certFile, keyFile, commonName string) {
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

func requireReload(t testing.TB, s *Server) {
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
func newRawUpstream(t testing.TB, name string, handle func(conn net.Conn)) *testUpstream {
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
	t      testing.TB
	s      *Server
	client *http.Client
}

func newNtfyClient(t testing.TB, s *Server, ip string) *ntfyClient {
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
func newNtfyPackagedServer(t testing.TB, upstreams ...*testUpstream) (*Server, string) {
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
		resp, body := newNtfyClient(t, s, "127.0.0.21").do(http.MethodGet, "https", "/spam-offers/json", "")
		require.Equal(t, http.StatusForbidden, resp.StatusCode)
		require.Equal(t, `{"code":40399,"http":403,"error":"blocked","origin":"ntlb"}`+"\n", body)      // JSON, like ntfy's errors
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
		require.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
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

// Reloads: rules, limits, sites and upstreams added, changed and removed while the server runs.
// Each test writes whole configs (reloadConfig) and checks requests between reloads.

// reloadServers is a site's upstream block in YAML flow style, set up like ntfy's
func reloadServers(upstreams ...*testUpstream) string {
	servers := make([]string, 0, len(upstreams))
	for _, u := range upstreams {
		servers = append(servers, fmt.Sprintf("{name: %s, addresses: [%q]}", u.name, u.addr()))
	}
	return "{" + testNtfyUpstream + ", servers: [" + strings.Join(servers, ", ") + "]}"
}

// reloadSite is one site in YAML flow style: a name, its hosts (none: the listener's default
// site), its default upstream, and more settings (rules, named upstreams via upstreams, ...)
func reloadSite(name, hosts string, upstream *testUpstream, more ...string) string {
	settings := []string{"name: " + name, "listen-http: '127.0.0.1:0'", testNtfySite}
	if hosts != "" {
		settings = append(settings, "hosts: ["+hosts+"]")
	}
	upstreams := "default: " + reloadServers(upstream)
	for _, m := range more {
		if named, ok := strings.CutPrefix(m, "upstreams+"); ok {
			upstreams += ", " + named
		} else {
			settings = append(settings, m)
		}
	}
	settings = append(settings, "upstreams: {"+upstreams+"}")
	return "  - {" + strings.Join(settings, ", ") + "}\n"
}

// reloadConfig writes a config with global settings (YAML lines) and sites (see reloadSite)
func reloadConfig(t testing.TB, filename, global string, sites ...string) {
	conf := "listen-admin: '127.0.0.1:0'\ndrain-duration: 1s\n" + global + "\nsites:\n" + strings.Join(sites, "")
	require.NoError(t, os.WriteFile(filename, []byte(conf), 0600))
}

// startReloadServer writes the first config and starts a server on it
func startReloadServer(t testing.TB, global string, sites ...string) (*Server, string) {
	filename := filepath.Join(t.TempDir(), "config.yml")
	reloadConfig(t, filename, global, sites...)
	return startTestServer(t, filename), filename
}

// hostGet sends a GET for path with a Host header from a client IP (one connection per request,
// so every request sees the current config), without following redirects
func hostGet(t testing.TB, s *Server, ip, host, path string) (int, string, http.Header) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "http://"+s.HTTPAddr()+path, nil)
	require.NoError(t, err)
	req.Host = host
	client := clientFrom(ip)
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(body), resp.Header
}

// openHostStream opens a subscription with a Host header, see openStream
func openHostStream(t testing.TB, s *Server, host, path string) (*testStream, func()) {
	req, err := http.NewRequest(http.MethodGet, "http://"+s.HTTPAddr()+path, nil)
	require.NoError(t, err)
	req.Host = host
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	r := bufio.NewReader(resp.Body)
	line, err := r.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "start\n", line)
	stream := &testStream{closed: make(chan struct{})}
	go func() {
		defer close(stream.closed)
		io.Copy(io.Discard, r)
	}()
	return stream, func() {
		resp.Body.Close()
	}
}

// statusOf is a request's status, see hostGet
func statusOf(t testing.TB, s *Server, ip, host, path string) int {
	t.Helper()
	status, _, _ := hostGet(t, s, ip, host, path)
	return status
}

func TestServer_ReloadAddsAndRemovesEachRuleKind(t *testing.T) {
	// Each kind of rule takes effect with the reload that adds it, and is gone with the one that
	// removes it
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "page.txt"), []byte("from disk"), 0644))
	for _, tc := range []struct {
		name   string
		rule   string
		status int
		body   string
		more   []string
	}{
		{name: "block", rule: `{name: r, match: 'query == "x"', action: block}`, status: http.StatusForbidden},
		{name: "block with status", rule: `{name: r, match: 'query == "x"', action: block, status: 451}`, status: 451},
		{name: "redirect", rule: `{name: r, match: 'query == "x"', action: redirect, location: '"https://example.com" + path'}`, status: http.StatusFound},
		{name: "static", rule: `{name: r, match: 'path == "/page.txt"', action: static, root: ` + root + `}`, status: http.StatusOK, body: "from disk"},
		{name: "limit", rule: `{name: r, action: limit, rate: 1/h, burst: 0}`, status: http.StatusTooManyRequests},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app1 := newTestUpstream(t, "app1")
			path := "/name?x"
			if tc.name == "static" {
				path = "/page.txt"
			}
			s, filename := startReloadServer(t, "", reloadSite("s", "", app1))
			before := statusOf(t, s, "127.0.0.1", "a.com", path) // 404 for the static file: the upstream has none

			reloadConfig(t, filename, "", reloadSite("s", "", app1, "rules: ["+tc.rule+"]"))
			requireReload(t, s)
			if tc.name == "limit" {
				require.Equal(t, http.StatusOK, statusOf(t, s, "127.0.0.2", "a.com", path)) // The first one is within the rate
			}
			status, body, header := hostGet(t, s, "127.0.0.2", "a.com", path)
			require.Equal(t, tc.status, status)
			if tc.body != "" {
				require.Equal(t, tc.body, body)
			}
			if tc.name == "redirect" {
				require.Equal(t, "https://example.com/name", header.Get("Location"))
			}

			reloadConfig(t, filename, "", reloadSite("s", "", app1))
			requireReload(t, s)
			status, body, _ = hostGet(t, s, "127.0.0.2", "a.com", path)
			require.Equal(t, before, status) // The upstream's answer again
			require.NotEqual(t, "from disk", body)
		})
	}
}

func TestServer_ReloadChangesRuleSettings(t *testing.T) {
	// A rule's match, status and location change with a reload, under the same name
	app1 := newTestUpstream(t, "app1")
	block := func(path string, status int) string {
		return fmt.Sprintf(`rules: [{name: r, match: 'query == "%s"', action: block, status: %d}]`, path, status)
	}
	s, filename := startReloadServer(t, "", reloadSite("s", "", app1, block("a", 403)))
	require.Equal(t, http.StatusForbidden, statusOf(t, s, "127.0.0.1", "x", "/name?a"))
	require.Equal(t, http.StatusOK, statusOf(t, s, "127.0.0.1", "x", "/name?b"))

	// Another path
	reloadConfig(t, filename, "", reloadSite("s", "", app1, block("b", 403)))
	requireReload(t, s)
	require.Equal(t, http.StatusOK, statusOf(t, s, "127.0.0.1", "x", "/name?a"))
	require.Equal(t, http.StatusForbidden, statusOf(t, s, "127.0.0.1", "x", "/name?b"))

	// Another status
	reloadConfig(t, filename, "", reloadSite("s", "", app1, block("b", 451)))
	requireReload(t, s)
	require.Equal(t, 451, statusOf(t, s, "127.0.0.1", "x", "/name?b"))

	// A block becomes a redirect, and its location and status change
	redirect := func(location string, status int) string {
		return fmt.Sprintf(`rules: [{name: r, match: 'query == "b"', action: redirect, location: '"%s"', status: %d}]`, location, status)
	}
	reloadConfig(t, filename, "", reloadSite("s", "", app1, redirect("https://one.example/", 302)))
	requireReload(t, s)
	status, _, header := hostGet(t, s, "127.0.0.1", "x", "/name?b")
	require.Equal(t, http.StatusFound, status)
	require.Equal(t, "https://one.example/", header.Get("Location"))
	reloadConfig(t, filename, "", reloadSite("s", "", app1, redirect("https://two.example/", 301)))
	requireReload(t, s)
	status, _, header = hostGet(t, s, "127.0.0.1", "x", "/name?b")
	require.Equal(t, http.StatusMovedPermanently, status)
	require.Equal(t, "https://two.example/", header.Get("Location"))
}

func TestServer_ReloadReordersRules(t *testing.T) {
	// The first rule that decides wins, in the order of the reloaded config
	app1 := newTestUpstream(t, "app1")
	allow := `{name: allow-me, match: 'ip == "127.0.0.5"', action: allow}`
	block := `{name: block-all, match: 'query == "x"', action: block}`
	s, filename := startReloadServer(t, "", reloadSite("s", "", app1, "rules: ["+allow+", "+block+"]"))
	require.Equal(t, http.StatusOK, statusOf(t, s, "127.0.0.5", "x", "/name?x"))
	require.Equal(t, http.StatusForbidden, statusOf(t, s, "127.0.0.6", "x", "/name?x"))

	reloadConfig(t, filename, "", reloadSite("s", "", app1, "rules: ["+block+", "+allow+"]"))
	requireReload(t, s)
	require.Equal(t, http.StatusForbidden, statusOf(t, s, "127.0.0.5", "x", "/name?x"))

	reloadConfig(t, filename, "", reloadSite("s", "", app1, "rules: ["+allow+", "+block+"]"))
	requireReload(t, s)
	require.Equal(t, http.StatusOK, statusOf(t, s, "127.0.0.5", "x", "/name?x"))
}

func TestServer_ReloadMovesRulesBetweenGlobalAndSite(t *testing.T) {
	// A global rule applies to every site, a site's rule to that site only; global rules come
	// first, so a global allow skips a site's block
	a1, b1 := newTestUpstream(t, "a1"), newTestUpstream(t, "b1")
	block := `rules: [{name: r, match: 'query == "x"', action: block}]`
	siteA := func(more ...string) string { return reloadSite("a", "a.com", a1, more...) }
	siteB := func(more ...string) string { return reloadSite("b", "b.com", b1, more...) }
	s, filename := startReloadServer(t, block, siteA(), siteB())
	require.Equal(t, http.StatusForbidden, statusOf(t, s, "127.0.0.1", "a.com", "/name?x"))
	require.Equal(t, http.StatusForbidden, statusOf(t, s, "127.0.0.1", "b.com", "/name?x"))

	// Into site a only
	reloadConfig(t, filename, "", siteA(block), siteB())
	requireReload(t, s)
	require.Equal(t, http.StatusForbidden, statusOf(t, s, "127.0.0.1", "a.com", "/name?x"))
	require.Equal(t, http.StatusOK, statusOf(t, s, "127.0.0.1", "b.com", "/name?x"))

	// Into site b only
	reloadConfig(t, filename, "", siteA(), siteB(block))
	requireReload(t, s)
	require.Equal(t, http.StatusOK, statusOf(t, s, "127.0.0.1", "a.com", "/name?x"))
	require.Equal(t, http.StatusForbidden, statusOf(t, s, "127.0.0.1", "b.com", "/name?x"))

	// A global allow for one client goes before site b's block
	reloadConfig(t, filename, `rules: [{name: vip, match: 'ip == "127.0.0.7"', action: allow}]`, siteA(), siteB(block))
	requireReload(t, s)
	require.Equal(t, http.StatusOK, statusOf(t, s, "127.0.0.7", "b.com", "/name?x"))
	require.Equal(t, http.StatusForbidden, statusOf(t, s, "127.0.0.8", "b.com", "/name?x"))
	_, metrics := adminDo(t, s, http.MethodGet, "/metrics", "")
	require.Contains(t, metrics, `ntlb_rule_matches_total{rule="b/r"} 2`) // Once per step above
}

func TestServer_ReloadLimitBuckets(t *testing.T) {
	// A limit keeps its buckets over a reload while its name, rate and burst and the client key
	// stay, also with a new match or new rules around it; anything else starts it over
	app1 := newTestUpstream(t, "app1")
	limit := func(name, match string, burst int) string {
		return fmt.Sprintf(`{name: %s, match: '%s', action: limit, rate: 1/h, burst: %d}`, name, match, burst)
	}
	site := func(rules ...string) string {
		return reloadSite("s", "", app1, "rules: ["+strings.Join(rules, ", ")+"]")
	}
	s, filename := startReloadServer(t, "", site(limit("l", `path != ""`, 0)))
	use := func(ip string) int {
		return statusOf(t, s, ip, "x", "/name")
	}
	require.Equal(t, http.StatusOK, use("127.0.0.11"))
	require.Equal(t, http.StatusTooManyRequests, use("127.0.0.11"))
	reload := func(global string, rules ...string) {
		t.Helper()
		reloadConfig(t, filename, global, site(rules...))
		requireReload(t, s)
	}

	// Another match, and a rule in front: the same limit
	reload("", `{name: other, match: 'path == "/nope"', action: block}`, limit("l", `method == "GET"`, 0))
	require.Equal(t, http.StatusTooManyRequests, use("127.0.0.11"))

	// Another burst: a new limit
	reload("", limit("l", `method == "GET"`, 1))
	require.Equal(t, http.StatusOK, use("127.0.0.11"))
	require.Equal(t, http.StatusOK, use("127.0.0.11"))
	require.Equal(t, http.StatusTooManyRequests, use("127.0.0.11"))

	// Removed and added back: a new limit
	reload("")
	require.Equal(t, http.StatusOK, use("127.0.0.11"))
	reload("", limit("l", `method == "GET"`, 1))
	require.Equal(t, http.StatusOK, use("127.0.0.11"))
	require.Equal(t, http.StatusOK, use("127.0.0.11"))
	require.Equal(t, http.StatusTooManyRequests, use("127.0.0.11"))

	// Another client key: a new limit, and clients of one /24 now share a bucket
	reload("client-key: {ipv4-prefix-bits: 24}", limit("l", `method == "GET"`, 1))
	require.Equal(t, http.StatusOK, use("127.0.0.11"))
	require.Equal(t, http.StatusOK, use("127.0.0.12"))
	require.Equal(t, http.StatusTooManyRequests, use("127.0.0.13"))

	// The same rule moved to the global rules is another rule ("l", not "s/l"): a new limit
	reloadConfig(t, filename, "client-key: {ipv4-prefix-bits: 24}\nrules: ["+limit("l", `method == "GET"`, 1)+"]", reloadSite("s", "", app1))
	requireReload(t, s)
	require.Equal(t, http.StatusOK, use("127.0.0.13"))
}

func TestServer_ReloadLimitOfOneSiteLeavesOthers(t *testing.T) {
	// Changing one site's limit starts that limit over, not the other site's of the same name
	a1, b1 := newTestUpstream(t, "a1"), newTestUpstream(t, "b1")
	limit := func(burst int) string {
		return fmt.Sprintf("rules: [{name: l, action: limit, rate: 1/h, burst: %d}]", burst)
	}
	s, filename := startReloadServer(t, "", reloadSite("a", "a.com", a1, limit(0)), reloadSite("b", "b.com", b1, limit(0)))
	for _, host := range []string{"a.com", "b.com"} {
		require.Equal(t, http.StatusOK, statusOf(t, s, "127.0.0.21", host, "/name"))
		require.Equal(t, http.StatusTooManyRequests, statusOf(t, s, "127.0.0.21", host, "/name"))
	}
	reloadConfig(t, filename, "", reloadSite("a", "a.com", a1, limit(5)), reloadSite("b", "b.com", b1, limit(0)))
	requireReload(t, s)
	require.Equal(t, http.StatusOK, statusOf(t, s, "127.0.0.21", "a.com", "/name"))
	require.Equal(t, http.StatusTooManyRequests, statusOf(t, s, "127.0.0.21", "b.com", "/name"))
}

func TestServer_ReloadRejectedKeepsRunningConfig(t *testing.T) {
	// A config that does not validate is rejected as a whole: the rules, sites and upstreams of
	// the running config stay in force
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	good := []string{
		reloadSite("a", "a.com", app1, `rules: [{name: r, match: 'query == "x"', action: block}]`),
		reloadSite("b", "b.com", app2),
	}
	s, filename := startReloadServer(t, "", good...)
	for name, bad := range map[string][]string{
		"bad expression":      {reloadSite("a", "a.com", app1, `rules: [{name: r, match: 'path ==', action: block}]`)},
		"unknown action":      {reloadSite("a", "a.com", app1, `rules: [{name: r, action: drop}]`)},
		"unknown key":         {reloadSite("a", "a.com", app1, `rules: [{name: r, action: block, colour: red}]`)},
		"limit with status":   {reloadSite("a", "a.com", app1, `rules: [{name: r, action: limit, rate: 1/s, status: 503}]`)},
		"strike without bans": {reloadSite("a", "a.com", app1, `rules: [{name: r, action: block, strike: 1}]`)},
		"duplicate rule":      {reloadSite("a", "a.com", app1, `rules: [{name: r, action: block}, {name: r, action: block}]`)},
		"proxy to nothing":    {reloadSite("a", "a.com", app1, `rules: [{name: r, action: proxy, upstream: nope}]`)},
		"duplicate site":      {reloadSite("a", "a.com", app1), reloadSite("a", "c.com", app2)},
		"duplicate host":      {reloadSite("a", "a.com", app1), reloadSite("b", "a.com", app2)},
		"no default upstream": {"  - {name: a, listen-http: '127.0.0.1:0', upstreams: {x: " + reloadServers(app1) + "}}\n"},
		"no sites":            {},
	} {
		reloadConfig(t, filename, "", bad...)
		_, err := s.Reload()
		require.Error(t, err, name)
		require.Equal(t, http.StatusForbidden, statusOf(t, s, "127.0.0.1", "a.com", "/name?x"), name)
		_, body, _ := hostGet(t, s, "127.0.0.1", "b.com", "/name")
		require.Equal(t, "app2", body, name)
	}

	// A good config after all of them applies
	reloadConfig(t, filename, "", reloadSite("a", "a.com", app1), reloadSite("b", "b.com", app2))
	requireReload(t, s)
	require.Equal(t, http.StatusOK, statusOf(t, s, "127.0.0.1", "a.com", "/name?x"))
}

func TestServer_ReloadAddsAndRemovesSites(t *testing.T) {
	// Sites come and go with reloads: a new site's hosts reach its upstreams right away, a
	// removed site's hosts fall back to the listener's default site, and without one, to a 404
	a1, b1, d1 := newTestUpstream(t, "a1"), newTestUpstream(t, "b1"), newTestUpstream(t, "d1")
	siteA, siteB, siteD := reloadSite("a", "a.com", a1), reloadSite("b", "b.com", b1), reloadSite("d", "", d1)
	s, filename := startReloadServer(t, "errors: {format: json}", siteA, siteD)
	name := func(host string) string {
		_, body, _ := hostGet(t, s, "127.0.0.1", host, "/name")
		return body
	}
	require.Equal(t, "a1", name("a.com"))
	require.Equal(t, "d1", name("b.com"))

	// Site b added
	reloadConfig(t, filename, "errors: {format: json}", siteA, siteB, siteD)
	requireReload(t, s)
	require.Equal(t, "b1", name("b.com"))
	require.Equal(t, "a1", name("a.com"))
	require.Len(t, s.balancer.Status().Upstreams, 3)

	// Site b removed again
	reloadConfig(t, filename, "errors: {format: json}", siteA, siteD)
	requireReload(t, s)
	require.Equal(t, "d1", name("b.com"))

	// The default site removed: unknown hosts get ntlb's 404
	reloadConfig(t, filename, "errors: {format: json}", siteA, siteB)
	requireReload(t, s)
	status, body, _ := hostGet(t, s, "127.0.0.1", "unknown.com", "/name")
	require.Equal(t, http.StatusNotFound, status)
	require.Contains(t, body, `"code":40499`)
	require.Equal(t, "b1", name("b.com"))

	// And added back
	reloadConfig(t, filename, "errors: {format: json}", siteA, siteB, siteD)
	requireReload(t, s)
	require.Equal(t, "d1", name("unknown.com"))
}

func TestServer_ReloadChangesSiteHosts(t *testing.T) {
	// A site's hosts change with a reload: exact names, then a wildcard, which an exact name of
	// another site beats
	a1, b1, d1 := newTestUpstream(t, "a1"), newTestUpstream(t, "b1"), newTestUpstream(t, "d1")
	siteD := reloadSite("d", "", d1)
	s, filename := startReloadServer(t, "", reloadSite("a", "a.com", a1), siteD)
	name := func(host string) string {
		_, body, _ := hostGet(t, s, "127.0.0.1", host, "/name")
		return body
	}
	require.Equal(t, "a1", name("a.com"))
	require.Equal(t, "d1", name("a.org"))

	reloadConfig(t, filename, "", reloadSite("a", "a.org", a1), siteD)
	requireReload(t, s)
	require.Equal(t, "d1", name("a.com"))
	require.Equal(t, "a1", name("a.org"))

	reloadConfig(t, filename, "", reloadSite("a", "'*.example.com'", a1), reloadSite("b", "b.example.com", b1), siteD)
	requireReload(t, s)
	require.Equal(t, "a1", name("x.example.com"))
	require.Equal(t, "a1", name("y.x.example.com"))
	require.Equal(t, "b1", name("b.example.com"))
	require.Equal(t, "d1", name("example.com"))
}

func TestServer_ReloadRemovedSiteDrainsItsStreams(t *testing.T) {
	// A removed site's streams are closed at the pace of drain-duration (1s here), its other
	// site's streams stay open, and a renamed site is a new one
	a1, b1 := newTestUpstream(t, "a1"), newTestUpstream(t, "b1")
	s, filename := startReloadServer(t, "", reloadSite("a", "a.com", a1), reloadSite("b", "b.com", b1))
	streamA, closeA := openHostStream(t, s, "a.com", "/mytopic/json")
	defer closeA()
	streamB, closeB := openHostStream(t, s, "b.com", "/mytopic/json")
	defer closeB()

	reloadConfig(t, filename, "", reloadSite("a", "a.com", a1))
	requireReload(t, s)
	waitFor(t, func() bool {
		return streamClosed(streamB)
	})
	require.False(t, streamClosed(streamA))
	waitFor(t, func() bool {
		return len(s.balancer.Status().Upstreams) == 1 // b/b1 forgotten once drained
	})

	// Renamed: site c, with the same upstream as a, is new, and a's streams drain
	reloadConfig(t, filename, "", reloadSite("c", "a.com", a1))
	requireReload(t, s)
	waitFor(t, func() bool {
		return streamClosed(streamA)
	})
	_, body, _ := hostGet(t, s, "127.0.0.1", "a.com", "/name")
	require.Equal(t, "a1", body)
	waitFor(t, func() bool {
		status := s.balancer.Status()
		return len(status.Upstreams) == 1 && status.Upstreams[0].Site == "c"
	})
}

func TestServer_ReloadMovesSiteToAnotherListener(t *testing.T) {
	// A site moved to a new listen address answers there after the reload, and no longer on
	// the old one, where the remaining site takes its hosts
	a1, b1 := newTestUpstream(t, "a1"), newTestUpstream(t, "b1")
	siteA := reloadSite("a", "", a1)
	siteB := func(listen string) string {
		return strings.Replace(reloadSite("b", "b.com", b1), "'127.0.0.1:0'", "'"+listen+"'", 1)
	}
	s, filename := startReloadServer(t, "", siteA, siteB("127.0.0.1:0"))
	_, body, _ := hostGet(t, s, "127.0.0.1", "b.com", "/name")
	require.Equal(t, "b1", body)

	reloadConfig(t, filename, "", siteA, siteB("127.0.0.2:0"))
	requireReload(t, s)
	_, body, _ = hostGet(t, s, "127.0.0.1", "b.com", "/name")
	require.Equal(t, "a1", body) // The old listener's default site
	req, err := http.NewRequest(http.MethodGet, "http://"+s.ListenerAddr("127.0.0.2:0")+"/name", nil)
	require.NoError(t, err)
	req.Host = "b.com"
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, "b1", string(b))
}

func TestServer_ReloadNamedUpstreamsAndProxyRules(t *testing.T) {
	// A named upstream and the proxy rule that routes to it arrive and leave with one reload; a
	// rule that names a missing upstream is rejected
	app1, api1 := newTestUpstream(t, "app1"), newTestUpstream(t, "api1")
	route := `rules: [{name: to-api, match: 'path startsWith "/api/"', action: proxy, upstream: api}]`
	api := "upstreams+api: " + reloadServers(api1)
	s, filename := startReloadServer(t, "", reloadSite("s", "", app1))
	api1.on("/api/name", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "api1")
	})
	app1.on("/api/name", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "app1")
	})
	name := func() string {
		_, body, _ := hostGet(t, s, "127.0.0.1", "x", "/api/name")
		return body
	}
	require.Equal(t, "app1", name())

	reloadConfig(t, filename, "", reloadSite("s", "", app1, api, route))
	requireReload(t, s)
	require.Equal(t, "api1", name())

	// The upstream without the rule: unused
	reloadConfig(t, filename, "", reloadSite("s", "", app1, api))
	requireReload(t, s)
	require.Equal(t, "app1", name())

	// The rule without the upstream: rejected, the last config stays
	reloadConfig(t, filename, "", reloadSite("s", "", app1, route))
	_, err := s.Reload()
	require.ErrorContains(t, err, `unknown upstream "api"`)
	require.Equal(t, "app1", name())

	// Both again, then the rule without an upstream: the default
	reloadConfig(t, filename, "", reloadSite("s", "", app1, api, route))
	requireReload(t, s)
	require.Equal(t, "api1", name())
	reloadConfig(t, filename, "", reloadSite("s", "", app1, api, `rules: [{name: to-api, match: 'path startsWith "/api/"', action: proxy}]`))
	requireReload(t, s)
	require.Equal(t, "app1", name())
}

func TestServer_ReloadMovesServerBetweenUpstreams(t *testing.T) {
	// A server moved from the default upstream to a named one keeps its open streams, and new
	// requests reach it only through the proxy rule
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	s, filename := startReloadServer(t, "", "  - {name: s, listen-http: '127.0.0.1:0', "+testNtfySite+", upstreams: {default: "+reloadServers(app2)+"}}\n")
	stream, closeStream := openHostStream(t, s, "x", "/mytopic/json")
	defer closeStream()
	require.Equal(t, int64(1), app2.hits.Load())

	reloadConfig(t, filename, "", "  - {name: s, listen-http: '127.0.0.1:0', "+testNtfySite+
		", rules: [{name: to-two, match: 'path == \"/two\"', action: proxy, upstream: two}]"+
		", upstreams: {default: "+reloadServers(app1)+", two: "+reloadServers(app2)+"}}\n")
	requireReload(t, s)
	require.False(t, streamClosed(stream))
	_, body, _ := hostGet(t, s, "127.0.0.1", "x", "/name")
	require.Equal(t, "app1", body)
	app2.on("/two", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "app2")
	})
	_, body, _ = hostGet(t, s, "127.0.0.1", "x", "/two")
	require.Equal(t, "app2", body)
	for _, u := range s.balancer.Status().Upstreams {
		if u.Name == "app2" {
			require.Equal(t, "two", u.Pool)
			require.Equal(t, 1, u.Streams)
		}
	}
}

func TestServer_ReloadChangesErrorPages(t *testing.T) {
	// Error pages switch format with a reload, globally and per site
	app1 := newTestUpstream(t, "app1")
	template := filepath.Join(t.TempDir(), "error.txt")
	require.NoError(t, os.WriteFile(template, []byte("custom {{.Code}}"), 0600))
	block := `rules: [{name: r, match: 'query == "x"', action: block}]`
	s, filename := startReloadServer(t, "", reloadSite("s", "", app1, block))
	_, body, header := hostGet(t, s, "127.0.0.1", "x", "/name?x")
	require.Equal(t, "text/html; charset=utf-8", header.Get("Content-Type"))
	require.Contains(t, body, "Error 40399")
	for global, want := range map[string]string{
		"errors: {format: json}":               `{"code":40399,"http":403,"error":"blocked","origin":"ntlb"}` + "\n",
		"errors: {format: text}":               "Error 40399: blocked\n",
		"errors: {template: " + template + "}": "custom 40399",
	} {
		reloadConfig(t, filename, global, reloadSite("s", "", app1, block))
		requireReload(t, s)
		_, body, _ = hostGet(t, s, "127.0.0.1", "x", "/name?x")
		require.Equal(t, want, body, global)
	}

	// The site's own replaces the global one
	reloadConfig(t, filename, "errors: {format: text}", reloadSite("s", "", app1, block, "errors: {format: json}"))
	requireReload(t, s)
	_, body, _ = hostGet(t, s, "127.0.0.1", "x", "/name?x")
	require.Contains(t, body, `"code":40399`)

	// A template that cannot be read rejects the reload
	reloadConfig(t, filename, "errors: {template: /nonexistent/error.html}", reloadSite("s", "", app1, block))
	_, err := s.Reload()
	require.ErrorContains(t, err, "errors.template")
	_, body, _ = hostGet(t, s, "127.0.0.1", "x", "/name?x")
	require.Contains(t, body, `"code":40399`)
}

func TestServer_ReloadStormKeepsServing(t *testing.T) {
	// Many reloads that add and remove rules, limits and sites while clients keep sending: no
	// request fails for anything but a rule, and an open stream of a site that stays survives
	a1, b1, d1 := newTestUpstream(t, "a1"), newTestUpstream(t, "b1"), newTestUpstream(t, "d1")
	configs := [][]string{
		{reloadSite("a", "a.com", a1), reloadSite("d", "", d1)},
		{reloadSite("a", "a.com", a1, `rules: [{name: r, match: 'query == "x"', action: block}]`), reloadSite("b", "b.com", b1), reloadSite("d", "", d1)},
		{reloadSite("a", "a.com", a1, `rules: [{name: l, match: 'query == "x"', action: limit, rate: 1/h, burst: 0}]`), reloadSite("b", "b.com", b1, `rules: [{name: r, match: 'query == "x"', action: block}]`), reloadSite("d", "", d1)},
	}
	globals := []string{"", `rules: [{name: g, match: 'query == "g"', action: block}]`}
	s, filename := startReloadServer(t, "", configs[0]...)
	stream, closeStream := openHostStream(t, s, "a.com", "/mytopic/json")
	defer closeStream()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	var failures atomic.Int64
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			hosts := []string{"a.com", "b.com", "other.com"}
			for n := 0; ctx.Err() == nil; n++ {
				host := hosts[(i+n)%len(hosts)]
				req, _ := http.NewRequest(http.MethodGet, "http://"+s.HTTPAddr()+[]string{"/name", "/name?x", "/name?g"}[n%3], nil)
				req.Host = host
				resp, err := clientFrom(fmt.Sprintf("127.0.1.%d", i+1)).Do(req)
				if err != nil {
					failures.Add(1)
					t.Logf("%s %s: %v", host, req.URL.Path, err)
					continue
				}
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				switch resp.StatusCode {
				case http.StatusOK, http.StatusForbidden, http.StatusTooManyRequests:
				default:
					failures.Add(1)
					t.Logf("%s %s: %d %s", host, req.URL, resp.StatusCode, regexp.MustCompile(`Error [0-9]+: [^<]*`).FindString(string(body)))
				}
			}
		}()
	}
	for i := range 40 {
		reloadConfig(t, filename, globals[i%len(globals)], configs[i%len(configs)]...)
		requireReload(t, s)
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	wg.Wait()
	require.Zero(t, failures.Load())
	require.False(t, streamClosed(stream))

	// The last config is the one in force
	reloadConfig(t, filename, "", configs[1]...)
	requireReload(t, s)
	require.Equal(t, http.StatusForbidden, statusOf(t, s, "127.0.0.1", "a.com", "/name?x"))
	_, body, _ := hostGet(t, s, "127.0.0.1", "b.com", "/name")
	require.Equal(t, "b1", body)
}

func TestServer_ReloadServesRequestsRoutedBefore(t *testing.T) {
	// A request routed by the config before a reload that removed its site, or the upstream block
	// a proxy rule sent it to, is still served by the removed servers while they drain, instead
	// of a 503 for every request in flight during the reload
	a1, b1, api1 := newTestUpstream(t, "a1"), newTestUpstream(t, "b1"), newTestUpstream(t, "api1")
	api := "upstreams+api: " + reloadServers(api1)
	s, filename := startReloadServer(t, "", reloadSite("a", "a.com", a1), reloadSite("b", "b.com", b1, api))
	serveRoutedBy := func(conf *config.Config, site, pool string) (int, string) {
		r := httptest.NewRequest(http.MethodGet, "http://"+site+".com/name", nil)
		r.RemoteAddr = "127.0.0.1:1234"
		env := match.NewEnv(r)
		env.Site = site
		pr := &proxyRequest{
			env:        env,
			site:       site,
			pool:       pool,
			siteConf:   conf.Site(site),
			errorPages: conf.ErrorPagesFor(nil),
		}
		w := httptest.NewRecorder()
		s.proxy.ServeHTTP(w, r.WithContext(withProxyRequest(r.Context(), pr)))
		pr.finish()
		return w.Code, w.Body.String()
	}
	before := s.config.Load()

	// The named upstream removed
	reloadConfig(t, filename, "", reloadSite("a", "a.com", a1), reloadSite("b", "b.com", b1))
	requireReload(t, s)
	status, body := serveRoutedBy(before, "b", "api")
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "api1", body)

	// The whole site removed
	reloadConfig(t, filename, "", reloadSite("a", "a.com", a1))
	requireReload(t, s)
	status, body = serveRoutedBy(before, "b", "")
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "b1", body)
}

func TestServer_ReloadChangesHealthCheck(t *testing.T) {
	// A new health check path applies from the next round: a path the server fails takes it out
	// (with the only server out, requests fail open to it), and the old path brings it back
	app1 := newTestUpstream(t, "app1")
	site := func(path string) string {
		return "  - {name: s, listen-http: '127.0.0.1:0', " + testNtfySite + ", upstreams: {default: {healthcheck: {path: " + path +
			", interval: 50ms, timeout: 40ms, fails: 1, passes: 1}, servers: [{name: app1, addresses: [" + strconv.Quote(app1.addr()) + "]}]}}}\n"
	}
	s, filename := startReloadServer(t, "", site(testHealthPath))
	require.True(t, s.balancer.Healthy())

	reloadConfig(t, filename, "", site("/nope"))
	requireReload(t, s)
	waitFor(t, func() bool {
		return !s.balancer.Healthy()
	})
	_, body, _ := hostGet(t, s, "127.0.0.1", "x", "/name")
	require.Equal(t, "app1", body) // Fail open

	reloadConfig(t, filename, "", site(testHealthPath))
	requireReload(t, s)
	waitFor(t, func() bool {
		return s.balancer.Healthy()
	})
}

func TestServer_ReloadAddsServerThatIsDown(t *testing.T) {
	// A server added by a reload is probed before it takes traffic: one that is not ready never
	// gets a request, and joins once it is
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	app2.notReady.Store(true)
	site := func(upstreams ...*testUpstream) string {
		return "  - {name: s, listen-http: '127.0.0.1:0', " + testNtfySite + ", upstreams: {default: " +
			strings.Replace(reloadServers(upstreams...), "interval: 1h, timeout: 1s", "interval: 50ms, timeout: 40ms", 1) + "}}\n"
	}
	s, filename := startReloadServer(t, "", site(app1))
	reloadConfig(t, filename, "", site(app1, app2))
	requireReload(t, s)
	for i := range 50 {
		statusOf(t, s, fmt.Sprintf("127.0.2.%d", i+1), "x", "/name")
	}
	require.Zero(t, app2.hits.Load())

	app2.notReady.Store(false)
	waitFor(t, func() bool {
		for _, u := range s.balancer.Status().Upstreams {
			if u.Name == "app2" && u.State == balancer.StateHealthy {
				return true
			}
		}
		return false
	})
	for i := range 50 {
		statusOf(t, s, fmt.Sprintf("127.0.2.%d", i+1), "x", "/name")
	}
	require.NotZero(t, app2.hits.Load())
}

func BenchmarkValidRequest(b *testing.B) {
	r := httptest.NewRequest(http.MethodGet, "/mytopic/json?poll=1", nil)
	r.Host = "ntfy.sh:443"
	b.ReportAllocs()
	for b.Loop() {
		if !validRequest(r) {
			b.Fatal("invalid")
		}
	}
}

func TestValidRequestAllocations(t *testing.T) {
	skipUnderRace(t)
	r := httptest.NewRequest(http.MethodGet, "/mytopic/json?poll=1", nil)
	for _, host := range []string{"ntfy.sh", "ntfy.sh:443", "[::1]:80", "203.0.113.7"} {
		r.Host = host
		require.Zero(t, testing.AllocsPerRun(100, func() {
			validRequest(r)
		}), host)
	}
}

// skipUnderRace skips an allocation budget under the race detector, which allocates on its own
func skipUnderRace(t *testing.T) {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, s := range info.Settings {
			if s.Key == "-race" && s.Value == "true" {
				t.Skip("allocation counts differ under -race")
			}
		}
	}
}
