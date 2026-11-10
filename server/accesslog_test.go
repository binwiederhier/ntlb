package server

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"heckel.io/ntlb/config"
)

func TestAccessLog_AccessLog(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	logFile := filepath.Join(t.TempDir(), "access.log")
	s, _ := newTestServer(t, "access-logs: [{file: "+logFile+"}]", app1)
	req, _ := http.NewRequest(http.MethodGet, "http://"+s.HTTPAddr()+"/name", nil)
	req.Header.Set("User-Agent", "ntfy-test")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	io.ReadAll(resp.Body)
	resp.Body.Close()
	waitFor(t, func() bool {
		b, _ := os.ReadFile(logFile)
		return strings.Contains(string(b), `"GET /name HTTP/1.1" 200 4 "-" "ntfy-test"`) && strings.Contains(string(b), "http_host="+s.HTTPAddr()+" site=s request_time=") && strings.Contains(string(b), "upstream=app1 upstream_status=200")
	})
}

func TestAccessLog_AbortedStream(t *testing.T) {
	// A stream that ends because the client went away (the proxy aborts the handler) is logged too
	app1 := newTestUpstream(t, "app1")
	logFile := filepath.Join(t.TempDir(), "access.log")
	s, _ := newTestServer(t, "access-logs: [{file: "+logFile+"}]", app1)
	_, closeStream := openStream(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/stream")
	closeStream()
	waitFor(t, func() bool {
		b, _ := os.ReadFile(logFile)
		return strings.Contains(string(b), `"GET /stream HTTP/1.1" 200 6`)
	})
}

func TestAccessLog_ReloadChangesFile(t *testing.T) {
	// A reload can turn the access log on, move it, and turn it off
	app1 := newTestUpstream(t, "app1")
	dir := t.TempDir()
	first, second := filepath.Join(dir, "first.log"), filepath.Join(dir, "second.log")
	s, filename := newTestServer(t, "", app1)
	logged := func(file, path string) bool {
		b, _ := os.ReadFile(file)
		return strings.Contains(string(b), "GET "+path+" ")
	}
	getPath := func(path string) {
		status, _ := get(t, http.DefaultClient, "http://"+s.HTTPAddr()+path)
		require.Equal(t, http.StatusOK, status)
	}

	// Off -> on
	getPath("/name?0")
	writeConfig(t, filename, "access-logs: [{file: "+first+"}]", app1)
	_, err := s.Reload()
	require.NoError(t, err)
	getPath("/name?1")
	waitFor(t, func() bool { return logged(first, "/name?1") })

	// Moved
	writeConfig(t, filename, "access-logs: [{file: "+second+"}]", app1)
	_, err = s.Reload()
	require.NoError(t, err)
	getPath("/name?2")
	waitFor(t, func() bool { return logged(second, "/name?2") })

	// Off
	writeConfig(t, filename, "", app1)
	_, err = s.Reload()
	require.NoError(t, err)
	getPath("/name?3")
	time.Sleep(1200 * time.Millisecond) // Past a flush
	require.False(t, logged(first, "/name?0"))
	require.False(t, logged(first, "/name?2"))
	require.False(t, logged(second, "/name?3"))
	require.False(t, logged(second, "/name?1"))
}

func TestAccessLog_ReloadBadPathKeepsOld(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	logFile := filepath.Join(t.TempDir(), "access.log")
	s, filename := newTestServer(t, "access-logs: [{file: "+logFile+"}]", app1)
	writeConfig(t, filename, "access-logs: [{file: /nonexistent/dir/access.log}]\ndrain-duration: 1m", app1)
	_, err := s.Reload()
	require.Error(t, err)
	require.Equal(t, config.DefaultDrainDuration, s.config.Load().DrainDuration) // Nothing applied
	get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/name?still")
	waitFor(t, func() bool {
		b, _ := os.ReadFile(logFile)
		return strings.Contains(string(b), "GET /name?still ")
	})
}

func TestAccessLog_PerSite(t *testing.T) {
	// A site with access logs of its own logs there; the others, and requests no site serves, to
	// the global ones
	a1, b1 := newTestUpstream(t, "app1"), newTestUpstream(t, "app1")
	dir := t.TempDir()
	global, siteLog := filepath.Join(dir, "access.log"), filepath.Join(dir, "a.log")
	filename := filepath.Join(dir, "config.yml")
	require.NoError(t, os.WriteFile(filename, []byte(fmt.Sprintf(`
listen-admin: '127.0.0.1:0'
access-logs: [{file: %s}]
sites:
  - {name: a, listen-http: '127.0.0.1:0', hosts: [a.com], access-logs: [{file: %s}], health-check: {interval: 1h, timeout: 1s}, upstreams: [{name: app1, addresses: [%q]}]}
  - {name: b, listen-http: '127.0.0.1:0', hosts: [b.com], health-check: {interval: 1h, timeout: 1s}, upstreams: [{name: app1, addresses: [%q]}]}
`, global, siteLog, a1.addr(), b1.addr())), 0600))
	s := startTestServer(t, filename)
	for _, host := range []string{"a.com", "b.com", "c.com"} {
		req, err := http.NewRequest(http.MethodGet, "http://"+s.HTTPAddr()+"/name?"+host, nil)
		require.NoError(t, err)
		req.Host = host
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		io.ReadAll(resp.Body)
		resp.Body.Close()
	}
	waitFor(t, func() bool {
		b, _ := os.ReadFile(global)
		return strings.Contains(string(b), "/name?c.com")
	})
	a, _ := os.ReadFile(siteLog)
	g, _ := os.ReadFile(global)
	require.Contains(t, string(a), "GET /name?a.com ")
	require.NotContains(t, string(a), "b.com ")
	require.Contains(t, string(g), "GET /name?b.com ")
	require.Contains(t, string(g), "GET /name?c.com HTTP/1.1\" 404")
	require.NotContains(t, string(g), "a.com ")
}

func TestAccessLog_StreamOutlivesLogRename(t *testing.T) {
	// A stream that ends after a reload moved the access log is logged to the new file
	app1 := newTestUpstream(t, "app1")
	dir := t.TempDir()
	first, second := filepath.Join(dir, "first.log"), filepath.Join(dir, "second.log")
	s, filename := newTestServer(t, "access-logs: [{file: "+first+"}]", app1)
	_, closeStream := openStream(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/stream")
	writeConfig(t, filename, "access-logs: [{file: "+second+"}]", app1)
	requireReload(t, s)
	closeStream()
	waitFor(t, func() bool {
		b, _ := os.ReadFile(second)
		return strings.Contains(string(b), "GET /stream ")
	})
}

func TestAccessLog_FieldsCannotForgeALine(t *testing.T) {
	// HTTP/2 lets a client put spaces and quotes into the method and :authority; like nginx, every
	// field escapes quotes, backslashes and non-printable bytes, and the unquoted ones also spaces,
	// so a line always parses into the same fields (CrowdSec reads them)
	r := httptest.NewRequest("GET", "/x", nil)
	r.Method = `GET /x HTTP/2.0" 200 1 "-" "curl" http_host=ntfy.sh #`
	r.Host = `a b site=evil`
	r.Header.Set("User-Agent", "ua\twith\x1b[31mcolor\x7f and \xff")
	r.Header.Set("Referer", `ref"\`)
	r.Proto = `HTTP/2.0 "x"`
	e := newAccessLogEntry(r, time.Now())
	require.Equal(t, `GET\x20/x\x20HTTP/2.0\x22\x20200\x201\x20\x22-\x22\x20\x22curl\x22\x20http_host=ntfy.sh\x20# /x HTTP/2.0\x20\x22x\x22`, e.request)
	require.Equal(t, `a\x20b\x20site=evil`, e.host)
	require.Equal(t, `ua\x09with\x1B[31mcolor\x7F and \xFF`, e.userAgent)
	require.Equal(t, `ref\x22\x5C`, e.referer)
}

func TestAccessLog_AuthQueryRedacted(t *testing.T) {
	// The web app sends credentials as ?auth=; they must not end up in the access log
	for target, want := range map[string]string{
		"/mytopic/ws?auth=QmFzaWMgZm9v":           "/mytopic/ws?auth=REDACTED",
		"/mytopic/json?poll=1&auth=abc&since=all": "/mytopic/json?poll=1&auth=REDACTED&since=all",
		"/mytopic/json?Auth=abc":                  "/mytopic/json?Auth=REDACTED",
		"/mytopic/json?author=me":                 "/mytopic/json?author=me",
		"/auth=x/json":                            "/auth=x/json",
	} {
		e := newAccessLogEntry(httptest.NewRequest("GET", target, nil), time.Now())
		require.Equal(t, "GET "+want+" HTTP/1.1", e.request, target)
	}
}

func TestAccessLog_LongFieldsTruncated(t *testing.T) {
	// Fields are capped, so one request cannot write a 64 KiB line
	r := httptest.NewRequest("GET", "/"+strings.Repeat("a", 20000), nil)
	r.Header.Set("User-Agent", strings.Repeat("b", 20000))
	e := newAccessLogEntry(r, time.Now())
	require.LessOrEqual(t, len(e.request), len("GET ")+maxLogField+len("...")+len(" HTTP/1.1"))
	require.True(t, strings.HasSuffix(e.userAgent, "..."))
	require.LessOrEqual(t, len(e.userAgent), maxLogField+len("..."))
}
