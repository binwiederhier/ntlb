package server

import (
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"heckel.io/ntlb/config"
)

const testErrorRules = `
rules:
  - {name: abuse, match: 'path startsWith "/sp_"', action: block}
  - {name: spy, match: 'path startsWith "/spy"', action: block, status: 451}
  - {name: limit, match: 'path startsWith "/limited"', action: limit, rate: 1/h, burst: 0}
`

// getResponse is get with the response's content type
func getResponse(t testing.TB, url string) (int, string, string) {
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, resp.Header.Get("Content-Type"), string(body)
}

func TestErrorPages_HTMLByDefault(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, testErrorRules, app1)
	status, contentType, body := getResponse(t, "http://"+s.HTTPAddr()+"/sp_x/json")
	require.Equal(t, http.StatusForbidden, status)
	require.Equal(t, "text/html; charset=utf-8", contentType)
	require.Contains(t, body, "<title>403 Forbidden</title>")
	require.Contains(t, body, "Error 40399: blocked")
}

func TestErrorPages_JSON(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "errors: {format: json}"+testErrorRules, app1)

	// Like ntfy's errors, with codes of their own (ntfy's count up from 01, ntlb's down from 99) and
	// an origin: a code that names the reason, and the status, which a block rule can change
	status, contentType, body := getResponse(t, "http://"+s.HTTPAddr()+"/sp_x/json")
	require.Equal(t, http.StatusForbidden, status)
	require.Equal(t, "application/json", contentType)
	require.Equal(t, `{"code":40399,"http":403,"error":"blocked","origin":"ntlb"}`+"\n", body)
	_, _, body = getResponse(t, "http://"+s.HTTPAddr()+"/spy")
	require.Equal(t, `{"code":40399,"http":451,"error":"blocked","origin":"ntlb"}`+"\n", body)
	getResponse(t, "http://"+s.HTTPAddr()+"/limited") // Within the burst
	_, _, body = getResponse(t, "http://"+s.HTTPAddr()+"/limited")
	require.Equal(t, `{"code":42999,"http":429,"error":"rate limited","origin":"ntlb"}`+"\n", body)

	// Upstream answers pass through as they are
	status, contentType, body = getResponse(t, "http://"+s.HTTPAddr()+"/name")
	require.Equal(t, http.StatusOK, status)
	require.NotEqual(t, "application/json", contentType)
	require.NotContains(t, body, `"code"`)
}

func TestErrorPages_Text(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "errors: {format: text}"+testErrorRules, app1)
	status, contentType, body := getResponse(t, "http://"+s.HTTPAddr()+"/sp_x")
	require.Equal(t, http.StatusForbidden, status)
	require.Equal(t, "text/plain; charset=utf-8", contentType)
	require.Equal(t, "Error 40399: blocked\n", body)
}

func TestErrorPages_Template(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	template := filepath.Join(t.TempDir(), "error.html")
	require.NoError(t, os.WriteFile(template, []byte("<b>{{.HTTP}} {{.Status}}</b> {{.Code}} {{.Error}}"), 0600))
	s, _ := newTestServer(t, "errors: {template: "+template+"}"+testErrorRules, app1)
	status, contentType, body := getResponse(t, "http://"+s.HTTPAddr()+"/spy")
	require.Equal(t, 451, status)
	require.Equal(t, "text/html; charset=utf-8", contentType)
	require.Equal(t, "<b>451 Unavailable For Legal Reasons</b> 40399 blocked", body)
}

func TestErrorPages_UpstreamFailed(t *testing.T) {
	// ntlb's own answer when the upstream fails, which used to have no body
	app1 := newRawUpstream(t, "app1", func(conn net.Conn) {
		head, _ := readRequestHead(conn)
		if answerReady(conn, head) {
			return
		}
		io.WriteString(conn, "this is not http\r\n\r\n")
	})
	s, _ := newTestServer(t, "errors: {format: json}", app1)
	for _, path := range []string{"/name", "/mytopic/json"} { // The reverse proxy and the stream path
		status, _, body := getResponse(t, "http://"+s.HTTPAddr()+path)
		require.Equal(t, http.StatusBadGateway, status, path)
		require.Equal(t, `{"code":50299,"http":502,"error":"upstream failed","origin":"ntlb"}`+"\n", body, path)
	}
}

// discardWriter is a ResponseWriter that keeps nothing, so benchmarks measure writeError alone
type discardWriter struct {
	header http.Header
}

func (w *discardWriter) Header() http.Header {
	return w.header
}

func (w *discardWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

func (w *discardWriter) WriteHeader(int) {
}

func BenchmarkWriteError(b *testing.B) {
	// Blocks and limits answer with an error page; a flood of them is the case to be cheap for
	for _, format := range []string{config.ErrorFormatHTML, config.ErrorFormatJSON, config.ErrorFormatText} {
		b.Run(format, func(b *testing.B) {
			pages := &config.ErrorPages{Format: format}
			w := &discardWriter{header: make(http.Header)}
			b.ReportAllocs()
			for b.Loop() {
				writeError(w, pages, httpErrRateLimited)
			}
		})
	}
}

func TestWriteErrorAllocations(t *testing.T) {
	skipUnderRace(t)
	for format, limit := range map[string]float64{
		config.ErrorFormatHTML: 3,
		config.ErrorFormatJSON: 3,
		config.ErrorFormatText: 3,
	} {
		pages := &config.ErrorPages{Format: format}
		w := &discardWriter{header: make(http.Header)}
		allocs := testing.AllocsPerRun(100, func() {
			writeError(w, pages, httpErrRateLimited)
		})
		t.Logf("%s: %.0f", format, allocs)
		assert.LessOrEqual(t, allocs, limit, format)
	}
}
