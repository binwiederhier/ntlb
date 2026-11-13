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
  - {name: a, listen-http: '127.0.0.1:0', hosts: [a.com], access-logs: [{file: %s}], `+testNtfySite+`, upstreams: {default: {`+testNtfyUpstream+`, servers: [{name: app1, addresses: [%q]}]}}}
  - {name: b, listen-http: '127.0.0.1:0', hosts: [b.com], `+testNtfySite+`, upstreams: {default: {`+testNtfyUpstream+`, servers: [{name: app1, addresses: [%q]}]}}}
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

func FuzzAccessLog_Escape(f *testing.F) {
	// No field can end its quotes or the line, whatever a client sends; an unquoted one has no
	// spaces either, and every field stays bounded
	f.Add("GET /a b \"x\" \\ \x00\xff\n")
	f.Add(strings.Repeat("\x01", 5000))
	f.Fuzz(func(t *testing.T, s string) {
		for _, out := range []string{escape(s), escapeUnquoted(s)} {
			require.LessOrEqual(t, len(out), maxLogField+4+3) // One escape past the limit, then "..."
			for i := 0; i < len(out); i++ {
				c := out[i]
				require.True(t, c >= 0x20 && c <= 0x7e && c != '"', "byte %q in %q", c, out)
				if c == '\\' {
					require.Equal(t, byte('x'), out[i+1], out)
				}
			}
		}
		require.NotContains(t, escapeUnquoted(s), " ")
	})
}

func FuzzAccessLog_RedactAuth(f *testing.F) {
	f.Add("/t/json?auth=secret&x=1")
	f.Add("/t/json?AUTH=a=b&auth")
	f.Fuzz(func(t *testing.T, uri string) {
		out := redactAuth(uri)
		_, query, ok := strings.Cut(out, "?")
		if !ok {
			require.Equal(t, uri, out)
			return
		}
		for p := range strings.SplitSeq(query, "&") {
			if key, value, ok := strings.Cut(p, "="); ok && strings.EqualFold(key, "auth") {
				require.Equal(t, "REDACTED", value)
			}
		}
	})
}

// An access log line is written for every request

func benchAccessLog(tb testing.TB) (*accessLog, *http.Request, *proxyRequest) {
	f, err := os.Create(filepath.Join(tb.TempDir(), "access.log"))
	require.NoError(tb, err)
	l := newAccessLogFile(f)
	tb.Cleanup(l.close)
	r := httptest.NewRequest(http.MethodGet, "/mytopic/json?poll=1&since=1730000000", nil)
	r.RemoteAddr = "203.0.113.7:4321"
	r.Header.Set("User-Agent", "ntfy/1.27.0 (Android; 14)")
	pr := &proxyRequest{
		site:           "ntfy",
		upstream:       "app3",
		upstreamStatus: http.StatusOK,
		bytes:          120,
	}
	return l, r, pr
}

func BenchmarkAccessLog_Write(b *testing.B) {
	l, r, pr := benchAccessLog(b)
	b.ReportAllocs()
	for b.Loop() {
		l.write(newAccessLogEntry(r, time.Now()), pr, http.StatusOK)
	}
}

func TestAccessLog_WriteAllocations(t *testing.T) {
	skipUnderRace(t)
	// The entry and its request line when the request starts, the line when it ends
	l, r, pr := benchAccessLog(t)
	allocs := testing.AllocsPerRun(100, func() {
		l.write(newAccessLogEntry(r, time.Now()), pr, http.StatusOK)
	})
	require.LessOrEqual(t, allocs, 4.0)
}

func TestAccessLog_LineFormat(t *testing.T) {
	// The line, byte for byte, as nginx's combined_ssl plus the key=value fields: the formula
	// here is the reference the writer must match, whatever it does to be cheap
	dir := t.TempDir()
	start := time.Now().Add(-1500 * time.Millisecond)
	for name, tc := range map[string]struct {
		target, ua, referer, host string
		pr                        *proxyRequest
		status                    int
	}{
		"proxied":       {target: "/mytopic/json?poll=1", ua: "ntfy/1.27.0 (Android; 14)", host: "ntfy.sh", pr: &proxyRequest{site: "ntfy", upstream: "app3", upstreamStatus: 200, bytes: 120}, status: 200},
		"rule":          {target: "/sp_x/json", ua: "curl/8", host: "ntfy.sh", pr: &proxyRequest{site: "ntfy", rule: "ntfy/abuse"}, status: 403},
		"no site":       {target: "/", host: "other", pr: &proxyRequest{}, status: 404},
		"escaped":       {target: "/a?auth=secret&x=\"1\"", ua: "bad\"agent\\x", referer: "https://x/?a b", host: "ntfy.sh", pr: &proxyRequest{site: "ntfy", upstream: "app5", upstreamStatus: 502, bytes: 0}, status: 502},
		"long":          {target: "/" + strings.Repeat("a", 5000), ua: strings.Repeat("u", 4096), host: "ntfy.sh", pr: &proxyRequest{site: "ntfy"}, status: 200},
		"exactly max":   {target: "/", ua: strings.Repeat("u", maxLogField), host: "ntfy.sh", pr: &proxyRequest{site: "ntfy"}, status: 200},
		"upstream 101":  {target: "/mytopic/ws", ua: "ntfy", host: "ntfy.sh", pr: &proxyRequest{site: "ntfy", upstream: "app3", upstreamStatus: 101, bytes: 4096}, status: 101},
		"auth in path?": {target: "/auth?Auth=x&AUTH=y&auth", host: "ntfy.sh", pr: &proxyRequest{site: "ntfy"}, status: 200},
	} {
		f, err := os.Create(filepath.Join(dir, strings.ReplaceAll(name, " ", "-")+".log"))
		require.NoError(t, err)
		l := newAccessLogFile(f)
		r := httptest.NewRequest(http.MethodGet, "http://x"+tc.target, nil)
		r.RequestURI = tc.target
		r.RemoteAddr = "203.0.113.7:4321"
		r.Host = tc.host
		if tc.ua != "" {
			r.Header.Set("User-Agent", tc.ua)
		}
		if tc.referer != "" {
			r.Header.Set("Referer", tc.referer)
		}
		e := newAccessLogEntry(r, start)
		l.write(e, tc.pr, tc.status)
		l.close()
		b, err := os.ReadFile(f.Name())
		require.NoError(t, err)

		upstream, upstreamStatus := "-", "-"
		if tc.pr.upstream != "" {
			upstream, upstreamStatus = tc.pr.upstream, fmt.Sprint(tc.pr.upstreamStatus)
		}
		site := "-"
		if tc.pr.site != "" {
			site = tc.pr.site
		}
		want := fmt.Sprintf("%s - - [%s] \"%s\" %d %d \"%s\" \"%s\" http_host=%s site=%s request_time=",
			"203.0.113.7", start.Format(accessLogTimeFormat),
			escapeUnquoted("GET")+" "+escapeUnquoted(redactAuth(tc.target))+" "+escapeUnquoted("HTTP/1.1"),
			tc.status, tc.pr.bytes, orDash(escape(tc.referer)), orDash(escape(tc.ua)), orDash(escapeUnquoted(tc.host)), site)
		line := string(b)
		require.True(t, strings.HasPrefix(line, want), "%s:\n%s\n%s", name, line, want)
		rest := strings.TrimPrefix(line, want) // The request time varies: "N.NNN upstream=..."
		seconds, rest, _ := strings.Cut(rest, " ")
		require.Regexp(t, `^\d+\.\d{3}$`, seconds, name)
		tail := fmt.Sprintf("upstream=%s upstream_status=%s", upstream, upstreamStatus)
		if tc.pr.rule != "" {
			tail += " rule=" + tc.pr.rule
		}
		require.Equal(t, tail+"\n", rest, name)
	}
}

func TestAccessLog_EscapeAndRedact(t *testing.T) {
	// The values the writer relies on, pinned
	require.Equal(t, `bad\x22agent\x5Cx`, escape(`bad"agent\x`))
	require.Equal(t, `a\x20b`, escapeUnquoted("a b"))
	require.Equal(t, "a b", escape("a b"))
	require.Equal(t, "plain", escape("plain"))
	require.Equal(t, strings.Repeat("u", maxLogField)+"...", escape(strings.Repeat("u", maxLogField)))
	require.Equal(t, strings.Repeat("u", maxLogField-1), escape(strings.Repeat("u", maxLogField-1)))
	require.Equal(t, strings.Repeat("u", maxLogField)+"...", escape(strings.Repeat("u", maxLogField+10)))
	require.Equal(t, "/a?auth=REDACTED&x=1", redactAuth("/a?auth=secret&x=1"))
	require.Equal(t, "/a?Auth=REDACTED&AUTH=REDACTED&auth", redactAuth("/a?Auth=x&AUTH=y&auth"))
	require.Equal(t, "/a?x=1&y=2", redactAuth("/a?x=1&y=2"))
	require.Equal(t, "/auth", redactAuth("/auth"))
}
