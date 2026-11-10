package server

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// noRedirects is a client that shows redirects instead of following them
var (
	noRedirects = &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Timeout: 3 * time.Second,
	}
)

// notFound makes /missing on an upstream answer 404
func notFound(u *testUpstream) {
	u.on("/missing", func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
}

func TestServer_RuleBlocks(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	accessLogFile := filepath.Join(t.TempDir(), "access.log")
	s, _ := newTestServer(t, "access-logs: [{file: "+accessLogFile+"}]"+`
rules:
  - name: abuse
    match: path startsWith "/nz_"
    action: block
  - name: spy
    match: header("User-Agent") matches "(?i)ntfy-spy"
    action: block
    status: 451
`, app1)

	// Blocked before any upstream sees it, also subscriptions
	status, body := get(t, noRedirects, "http://"+s.HTTPAddr()+"/nz_x/json") // Unblocked, an endless subscription
	require.Equal(t, http.StatusForbidden, status)
	require.Equal(t, "Forbidden\n", body)
	req, _ := http.NewRequest(http.MethodGet, "http://"+s.HTTPAddr()+"/name", nil)
	req.Header.Set("User-Agent", "ntfy-spy/1.0")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, 451, resp.StatusCode)
	require.Zero(t, app1.hits.Load())

	// Everything else passes
	status, _ = get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/name")
	require.Equal(t, http.StatusOK, status)

	// The access log names the rule that decided
	waitFor(t, func() bool {
		b, _ := os.ReadFile(accessLogFile)
		return strings.Count(string(b), "\n") == 3
	})
	b, err := os.ReadFile(accessLogFile)
	require.NoError(t, err)
	require.Contains(t, string(b), `"GET /nz_x/json HTTP/1.1" 403 `)
	require.Contains(t, string(b), " upstream=- upstream_status=- rule=abuse\n")
	require.Contains(t, string(b), "rule=spy\n")
	require.Contains(t, string(b), "upstream=app1 upstream_status=200\n") // No rule field when none decided

	// Counted per rule
	_, metrics := adminDo(t, s, http.MethodGet, "/metrics", "")
	require.Contains(t, metrics, `ntlb_rule_matches_total{rule="abuse"} 1`)
	require.Contains(t, metrics, `ntlb_rule_matches_total{rule="spy"} 1`)
	require.Contains(t, metrics, `ntlb_requests_total{code="403",kind="request",site="s",upstream=""} 1`)
}

func TestServer_RuleLimits(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, `
rules:
  - name: exempt
    match: ipIn(ip, "127.0.0.2")
    action: allow
  - name: limit
    action: limit
    rate: 1/h
    burst: 1
`, app1)
	a, exempt := clientFrom("127.0.0.1"), clientFrom("127.0.0.2")
	for _, want := range []int{200, 200, 429, 429} {
		status, _ := get(t, a, "http://"+s.HTTPAddr()+"/name")
		require.Equal(t, want, status)
	}
	for range 4 {
		status, _ := get(t, exempt, "http://"+s.HTTPAddr()+"/name")
		require.Equal(t, http.StatusOK, status)
	}
	require.EqualValues(t, 6, app1.hits.Load())
}

func TestServer_RuleRedirects(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, `
rules:
  - name: https
    match: scheme == "http" && path == "/"
    action: redirect
    location: '"https://" + host + uri'
  - name: docs
    match: path startsWith "/docs/"
    action: redirect
    location: '"https://docs.ntfy.sh/" + trimPrefix(path, "/docs/")'
    status: 301
`, app1)
	resp, err := noRedirects.Get("http://" + s.HTTPAddr() + "/?x=1")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusFound, resp.StatusCode)
	require.Equal(t, "https://"+s.HTTPAddr()+"/?x=1", resp.Header.Get("Location"))
	resp, err = noRedirects.Get("http://" + s.HTTPAddr() + "/docs/publish/")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusMovedPermanently, resp.StatusCode)
	require.Equal(t, "https://docs.ntfy.sh/publish/", resp.Header.Get("Location"))
	require.Zero(t, app1.hits.Load())
}

func TestServer_RuleRedirectEmptyLocation(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "rules: [{name: r, action: redirect, location: 'query'}]", app1)
	status, _ := get(t, noRedirects, "http://"+s.HTTPAddr()+"/x")
	require.Equal(t, http.StatusInternalServerError, status)
}

func TestServer_RuleStatic(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	dir := t.TempDir()
	root := filepath.Join(dir, "html")
	require.NoError(t, os.MkdirAll(filepath.Join(root, "_next", "static"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "index.html"), []byte("<h1>ntfy</h1>"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "_next", "static", "app.js"), []byte("app()"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "secret.txt"), []byte("secret"), 0644))
	s, _ := newTestServer(t, `
rules:
  - name: website
    match: path == "/" || path startsWith "/_next/" || path contains ".."
    action: static
    root: `+root+`
    fallback: /index.html
  - name: no-fallback
    match: path startsWith "/assets/"
    action: static
    root: `+root+`
`, app1)
	base := "http://" + s.HTTPAddr()

	// A file, by its path
	resp, err := http.Get(base + "/_next/static/app.js")
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "app()", string(body))
	require.Contains(t, resp.Header.Get("Content-Type"), "javascript")
	require.NotEmpty(t, resp.Header.Get("Last-Modified"))

	// A directory or a missing file: the fallback, like try_files $uri /index.html
	for _, path := range []string{"/", "/_next/", "/_next/missing.js"} {
		status, body := get(t, http.DefaultClient, base+path)
		require.Equal(t, http.StatusOK, status, path)
		require.Equal(t, "<h1>ntfy</h1>", body, path)
	}

	// Nothing outside the root, also with an unnormalized path on the wire
	conn, err := net.Dial("tcp", s.HTTPAddr())
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.Write([]byte("GET /_next/../../secret.txt HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"))
	require.NoError(t, err)
	raw, _ := io.ReadAll(conn)
	require.NotContains(t, string(raw), "secret")

	// No fallback: 404
	status, _ := get(t, http.DefaultClient, base+"/assets/missing.css")
	require.Equal(t, http.StatusNotFound, status)

	// HEAD and conditional requests work
	req, _ := http.NewRequest(http.MethodHead, base+"/_next/static/app.js", nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, "5", resp.Header.Get("Content-Length"))
	req, _ = http.NewRequest(http.MethodGet, base+"/_next/static/app.js", nil)
	req.Header.Set("If-Modified-Since", time.Now().Add(time.Hour).UTC().Format(http.TimeFormat))
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusNotModified, resp.StatusCode)
	require.Zero(t, app1.hits.Load())
}

func TestServer_Bans(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	notFound(app1)
	banLog := filepath.Join(t.TempDir(), "ban.log")
	s, _ := newTestServer(t, "bans: {file: "+banLog+", threshold: 10}"+`
rules:
  - name: abuse
    match: path startsWith "/nz_"
    action: block
    strike: 4
  - name: not-found
    on: response
    match: status == 404 && upstream_status == 404
    strike: 1
`, app1)
	a, b := clientFrom("127.0.0.1"), clientFrom("127.0.0.2")
	for range 3 {
		get(t, a, "http://"+s.HTTPAddr()+"/nz_x")
	}
	for range 11 {
		get(t, b, "http://"+s.HTTPAddr()+"/missing")
	}
	waitFor(t, func() bool {
		data, _ := os.ReadFile(banLog)
		return strings.Count(string(data), "\n") == 2
	})
	data, err := os.ReadFile(banLog)
	require.NoError(t, err)
	require.Contains(t, string(data), " 127.0.0.1 127.0.0.1/32 abuse\n")
	require.Contains(t, string(data), " 127.0.0.2 127.0.0.2/32 not-found\n")
	_, metrics := adminDo(t, s, http.MethodGet, "/metrics", "")
	require.Contains(t, metrics, "ntlb_bans_total 2")
}

func TestServer_SiteBans(t *testing.T) {
	// A site's own ban file gets the bans of its requests; a reload opens it like any log file
	app1 := newTestUpstream(t, "app1")
	dir := t.TempDir()
	globalLog, siteLog := filepath.Join(dir, "ban.log"), filepath.Join(dir, "s-ban.log")
	filename := filepath.Join(dir, "config.yml")
	require.NoError(t, os.WriteFile(filename, []byte(fmt.Sprintf(`
listen-admin: '127.0.0.1:0'
bans: {file: %s, threshold: 100}
rules: [{name: abuse, match: 'path startsWith "/nz_"', action: block, strike: 101}]
sites:
  - {name: s, listen-http: '127.0.0.1:0', bans: {file: %s, threshold: 1}, `+testNtfySite+`, upstreams: [{name: app1, addresses: [%q]}]}
`, globalLog, siteLog, app1.addr())), 0600))
	s := startTestServer(t, filename)
	get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/nz_x")
	waitFor(t, func() bool {
		data, _ := os.ReadFile(siteLog)
		return strings.Contains(string(data), " 127.0.0.1 127.0.0.1/32 abuse\n")
	})
	data, _ := os.ReadFile(globalLog)
	require.Empty(t, string(data))
}

func TestServer_AccessLogsByStatus(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	notFound(app1)
	dir := t.TempDir()
	okLog, errLog := filepath.Join(dir, "access.log"), filepath.Join(dir, "access_error.log")
	s, _ := newTestServer(t, "access-logs: [{file: "+okLog+", match: 'status < 400'}, {file: "+errLog+"}]", app1)
	get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/name")
	get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/missing")
	waitFor(t, func() bool {
		a, _ := os.ReadFile(okLog)
		b, _ := os.ReadFile(errLog)
		return strings.Contains(string(a), "GET /name ") && strings.Contains(string(b), "GET /missing ")
	})
	a, _ := os.ReadFile(okLog)
	b, _ := os.ReadFile(errLog)
	require.NotContains(t, string(a), "/missing")
	require.NotContains(t, string(b), "/name")
}

func TestServer_RulesHotReload(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	s, filename := newTestServer(t, "", app1)
	stream, closeStream := openStream(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/mytopic/json")
	defer closeStream()

	// A new block rule turns new requests away, but leaves open streams alone
	writeConfig(t, filename, "rules: [{name: mytopic, match: 'path startsWith \"/mytopic/\"', action: block}]", app1)
	requireReload(t, s)
	status, _ := get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/mytopic/json?poll=1")
	require.Equal(t, http.StatusForbidden, status)
	require.False(t, streamClosed(stream))

	// A broken rule is rejected with the whole config, and the old rules stay
	writeConfig(t, filename, "rules: [{name: mytopic, match: 'path matches \"(\"', action: block}]", app1)
	_, err := s.Reload()
	require.ErrorContains(t, err, "invalid match")
	status, _ = get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/mytopic/json?poll=1")
	require.Equal(t, http.StatusForbidden, status)

	// Without the rule, everything passes again
	writeConfig(t, filename, "", app1)
	requireReload(t, s)
	status, _ = get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/mytopic/json?poll=1")
	require.Equal(t, http.StatusOK, status)
	require.False(t, streamClosed(stream))
}

func TestServer_NewFailsOnBanFile(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	filename := filepath.Join(t.TempDir(), "config.yml")
	writeConfig(t, filename, "bans: {file: /nonexistent/dir/ban.log, threshold: 1}", app1)
	_, err := New(filename)
	require.Error(t, err)
}

func TestServer_RuleStaticStaysInRoot(t *testing.T) {
	// A symlink in the root that points out of it is not followed; one inside it is
	app1 := newTestUpstream(t, "app1")
	dir := t.TempDir()
	root := filepath.Join(dir, "html")
	require.NoError(t, os.MkdirAll(filepath.Join(root, "sub"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "secret.txt"), []byte("secret"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "sub", "inside.txt"), []byte("inside"), 0644))
	require.NoError(t, os.Symlink(filepath.Join(dir, "secret.txt"), filepath.Join(root, "escape.txt")))
	require.NoError(t, os.Symlink(filepath.Join(dir), filepath.Join(root, "up")))
	require.NoError(t, os.Symlink("sub/inside.txt", filepath.Join(root, "link.txt")))
	s, _ := newTestServer(t, "rules: [{name: website, match: 'path != \"/v1/health\"', action: static, root: "+root+"}]", app1)
	for path, want := range map[string]int{
		"/escape.txt":     http.StatusNotFound,
		"/up/secret.txt":  http.StatusNotFound,
		"/link.txt":       http.StatusOK,
		"/sub/inside.txt": http.StatusOK,
	} {
		status, body := get(t, http.DefaultClient, "http://"+s.HTTPAddr()+path)
		require.Equal(t, want, status, path)
		require.NotContains(t, body, "secret", path)
	}
}

func TestServer_RuleStaticOnlyCleanPaths(t *testing.T) {
	// A rule matches the path as sent, so a path with dot segments could reach files outside what
	// the rule meant to serve; those and dotfiles are a 404
	app1 := newTestUpstream(t, "app1")
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "s"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(root, ".env"), []byte("secret"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "private.txt"), []byte("secret"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "s", "ok.txt"), []byte("ok"), 0644))
	s, _ := newTestServer(t, "rules: [{name: site, match: 'path startsWith \"/s\"', action: static, root: "+root+"}]", app1)
	conn := func(target string) int {
		resp := rawRoundTrip(t, s.HTTPAddr(), "GET "+target+" HTTP/1.1\r\nHost: a\r\n\r\n")
		return resp.StatusCode
	}
	require.Equal(t, http.StatusOK, conn("/s/ok.txt"))
	require.Equal(t, http.StatusNotFound, conn("/s/../private.txt"))
	require.Equal(t, http.StatusNotFound, conn("/s/../.env"))
	require.Equal(t, http.StatusNotFound, conn("/s/./ok.txt"))
	require.Equal(t, http.StatusNotFound, conn("/s//ok.txt"))
	require.Equal(t, http.StatusNotFound, conn("/s/.hidden"))
}
