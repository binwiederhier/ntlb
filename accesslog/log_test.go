package accesslog

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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
	e := NewEntry(r, time.Now())
	require.Equal(t, `GET\x20/x\x20HTTP/2.0\x22\x20200\x201\x20\x22-\x22\x20\x22curl\x22\x20http_host=ntfy.sh\x20# /x HTTP/2.0\x20\x22x\x22`, e.Request)
	require.Equal(t, `a\x20b\x20site=evil`, e.Host)
	require.Equal(t, `ua\x09with\x1B[31mcolor\x7F and \xFF`, e.UserAgent)
	require.Equal(t, `ref\x22\x5C`, e.Referer)
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
		e := NewEntry(httptest.NewRequest("GET", target, nil), time.Now())
		require.Equal(t, "GET "+want+" HTTP/1.1", e.Request, target)
	}
}

func TestAccessLog_LongFieldsTruncated(t *testing.T) {
	// Fields are capped, so one request cannot write a 64 KiB line
	r := httptest.NewRequest("GET", "/"+strings.Repeat("a", 20000), nil)
	r.Header.Set("User-Agent", strings.Repeat("b", 20000))
	e := NewEntry(r, time.Now())
	require.LessOrEqual(t, len(e.Request), len("GET ")+maxLogField+len("...")+len(" HTTP/1.1"))
	require.True(t, strings.HasSuffix(e.UserAgent, "..."))
	require.LessOrEqual(t, len(e.UserAgent), maxLogField+len("..."))
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

func benchLog(tb testing.TB) (*Log, *http.Request, Outcome) {
	f, err := os.Create(filepath.Join(tb.TempDir(), "access.log"))
	require.NoError(tb, err)
	l := newLog(f)
	tb.Cleanup(l.close)
	r := httptest.NewRequest(http.MethodGet, "/mytopic/json?poll=1&since=1730000000", nil)
	r.RemoteAddr = "203.0.113.7:4321"
	r.Header.Set("User-Agent", "ntfy/1.27.0 (Android; 14)")
	o := Outcome{
		Status:         http.StatusOK,
		Site:           "ntfy",
		Upstream:       "app3",
		UpstreamStatus: http.StatusOK,
		Bytes:          120,
	}
	return l, r, o
}

func BenchmarkAccessLog_Write(b *testing.B) {
	l, r, o := benchLog(b)
	b.ReportAllocs()
	for b.Loop() {
		l.Write(NewEntry(r, time.Now()), o)
	}
}

func TestAccessLog_WriteAllocations(t *testing.T) {
	skipUnderRace(t)
	// The entry and its request line when the request starts, the line when it ends
	l, r, o := benchLog(t)
	allocs := testing.AllocsPerRun(100, func() {
		l.Write(NewEntry(r, time.Now()), o)
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
		o                         Outcome
		status                    int
	}{
		"proxied":       {target: "/mytopic/json?poll=1", ua: "ntfy/1.27.0 (Android; 14)", host: "ntfy.sh", o: Outcome{Site: "ntfy", Upstream: "app3", UpstreamStatus: 200, Bytes: 120}, status: 200},
		"rule":          {target: "/sp_x/json", ua: "curl/8", host: "ntfy.sh", o: Outcome{Site: "ntfy", Rule: "ntfy/abuse"}, status: 403},
		"no site":       {target: "/", host: "other", o: Outcome{}, status: 404},
		"escaped":       {target: "/a?auth=secret&x=\"1\"", ua: "bad\"agent\\x", referer: "https://x/?a b", host: "ntfy.sh", o: Outcome{Site: "ntfy", Upstream: "app5", UpstreamStatus: 502, Bytes: 0}, status: 502},
		"long":          {target: "/" + strings.Repeat("a", 5000), ua: strings.Repeat("u", 4096), host: "ntfy.sh", o: Outcome{Site: "ntfy"}, status: 200},
		"exactly max":   {target: "/", ua: strings.Repeat("u", maxLogField), host: "ntfy.sh", o: Outcome{Site: "ntfy"}, status: 200},
		"upstream 101":  {target: "/mytopic/ws", ua: "ntfy", host: "ntfy.sh", o: Outcome{Site: "ntfy", Upstream: "app3", UpstreamStatus: 101, Bytes: 4096}, status: 101},
		"auth in path?": {target: "/auth?Auth=x&AUTH=y&auth", host: "ntfy.sh", o: Outcome{Site: "ntfy"}, status: 200},
	} {
		f, err := os.Create(filepath.Join(dir, strings.ReplaceAll(name, " ", "-")+".log"))
		require.NoError(t, err)
		l := newLog(f)
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
		e := NewEntry(r, start)
		o := tc.o
		o.Status = tc.status
		l.Write(e, o)
		l.close()
		b, err := os.ReadFile(f.Name())
		require.NoError(t, err)

		upstream, upstreamStatus := "-", "-"
		if tc.o.Upstream != "" {
			upstream, upstreamStatus = tc.o.Upstream, fmt.Sprint(tc.o.UpstreamStatus)
		}
		site := "-"
		if tc.o.Site != "" {
			site = tc.o.Site
		}
		want := fmt.Sprintf("%s - - [%s] \"%s\" %d %d \"%s\" \"%s\" http_host=%s site=%s request_time=",
			"203.0.113.7", start.Format(timeFormat),
			escapeUnquoted("GET")+" "+escapeUnquoted(redactAuth(tc.target))+" "+escapeUnquoted("HTTP/1.1"),
			tc.status, tc.o.Bytes, orDash(escape(tc.referer)), orDash(escape(tc.ua)), orDash(escapeUnquoted(tc.host)), site)
		line := string(b)
		require.True(t, strings.HasPrefix(line, want), "%s:\n%s\n%s", name, line, want)
		rest := strings.TrimPrefix(line, want) // The request time varies: "N.NNN upstream=..."
		seconds, rest, _ := strings.Cut(rest, " ")
		require.Regexp(t, `^\d+\.\d{3}$`, seconds, name)
		tail := fmt.Sprintf("upstream=%s upstream_status=%s", upstream, upstreamStatus)
		if tc.o.Rule != "" {
			tail += " rule=" + tc.o.Rule
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
	require.Equal(t, "/a?%61uth=REDACTED&x=1", redactAuth("/a?%61uth=secret&x=1")) // ntfy decodes the name
	require.Equal(t, "/a?%41UT%48=REDACTED", redactAuth("/a?%41UT%48=secret"))
	require.Equal(t, "/a?%zzauth=x", redactAuth("/a?%zzauth=x")) // Not a valid escape: not auth either
}

// flakyWriter fails its first fails writes, then takes everything
type flakyWriter struct {
	fails   int
	written bytes.Buffer
}

func (w *flakyWriter) Write(p []byte) (int, error) {
	if w.fails > 0 {
		w.fails--
		return 0, errors.New("no space left on device")
	}
	return w.written.Write(p)
}

func TestAccessLog_RecoversFromWriteError(t *testing.T) {
	// A failed write (a full disk) loses what was buffered, but not every line after it: bufio's
	// errors are sticky, and the ban file must keep reaching the ban tool once there is room again
	fw := &flakyWriter{
		fails: 1,
	}
	l := &Log{
		out: fw,
		w:   bufio.NewWriter(fw),
	}
	l.WriteLine([]byte("lost\n"))
	l.flush()
	l.WriteLine([]byte("kept\n"))
	l.flush()
	require.Equal(t, "kept\n", fw.written.String())
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
