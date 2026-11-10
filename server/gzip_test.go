package server

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"heckel.io/ntlb/config"
)

const (
	testGzip = "gzip: {types: [text/html, text/plain, application/json]}\n"
)

var (
	gzipBody = strings.Repeat(`{"id":"abc","event":"message","topic":"mytopic"}`+"\n", 100) // ~5 KiB
)

// gzipGet sends a request with the given Accept-Encoding (none if "") and returns the response
// and its body, decompressed if the response is gzip-encoded
func gzipGet(t *testing.T, method, url, acceptEncoding string, header ...string) (*http.Response, string) {
	t.Helper()
	req, err := http.NewRequest(method, url, nil)
	require.NoError(t, err)
	if acceptEncoding != "" {
		req.Header.Set("Accept-Encoding", acceptEncoding) // Set by hand: the transport does not decompress then
	}
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	resp, err := (&http.Transport{DisableCompression: true}).RoundTrip(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	var body io.Reader = resp.Body
	if resp.Header.Get("Content-Encoding") == "gzip" {
		zr, err := gzip.NewReader(resp.Body)
		require.NoError(t, err)
		body = zr
	}
	b, err := io.ReadAll(body)
	require.NoError(t, err)
	return resp, string(b)
}

func TestGzip_ProxiedResponses(t *testing.T) {
	// Like nginx's gzip with gzip_proxied any: listed types, large enough, 200/403/404 only, for
	// HTTP/1.1+ clients that accept gzip, never twice
	app1 := newTestUpstream(t, "app1")
	respond := func(status int, contentType string, header ...string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if contentType == "" {
				w.Header()["Content-Type"] = nil // No sniffing
			} else {
				w.Header().Set("Content-Type", contentType)
			}
			for i := 0; i+1 < len(header); i += 2 {
				w.Header().Set(header[i], header[i+1])
			}
			w.WriteHeader(status)
			if r.Method != http.MethodHead {
				io.WriteString(w, gzipBody[:len(gzipBody)/2])
				w.(http.Flusher).Flush()
				io.WriteString(w, gzipBody[len(gzipBody)/2:])
			}
		}
	}
	app1.on("/v1/account", respond(http.StatusOK, "application/json; charset=utf-8", "ETag", `"abc"`))
	app1.on("/v1/length", respond(http.StatusOK, "application/json", "Content-Length", strconv.Itoa(len(gzipBody))))
	app1.on("/v1/missing", respond(http.StatusNotFound, "application/json"))
	app1.on("/v1/created", respond(http.StatusCreated, "application/json"))
	app1.on("/v1/ndjson", respond(http.StatusOK, "application/x-ndjson"))
	app1.on("/v1/untyped", respond(http.StatusOK, ""))
	app1.on("/v1/encoded", respond(http.StatusOK, "text/plain", "Content-Encoding", "br"))
	app1.on("/v1/small", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"ok":true}`)
	})
	app1.on("/v1/small-length", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", "1000")
		io.WriteString(w, strings.Repeat("a", 1000))
	})
	s, _ := newTestServer(t, testGzip, app1)
	base := "http://" + s.HTTPAddr()

	// Compressed: the length goes, the ETag is weakened, caches learn it varies
	resp, body := gzipGet(t, http.MethodGet, base+"/v1/account", "gzip, deflate, br")
	require.Equal(t, "gzip", resp.Header.Get("Content-Encoding"))
	require.Equal(t, gzipBody, body)
	require.Equal(t, int64(-1), resp.ContentLength)
	require.Equal(t, `W/"abc"`, resp.Header.Get("ETag"))
	require.Equal(t, "Accept-Encoding", resp.Header.Get("Vary"))
	for path, acceptEncoding := range map[string]string{
		"/v1/length":  "gzip",
		"/v1/missing": "deflate;q=1, GZIP;q=0.5",
	} {
		resp, body := gzipGet(t, http.MethodGet, base+path, acceptEncoding)
		require.Equal(t, "gzip", resp.Header.Get("Content-Encoding"), path)
		require.Equal(t, gzipBody, body, path)
	}

	// Not compressed
	for name, c := range map[string]struct{ path, acceptEncoding string }{
		"no accept-encoding":   {"/v1/account", ""},
		"gzip refused":         {"/v1/account", "gzip;q=0, deflate"},
		"other encodings only": {"/v1/account", "br, deflate"},
		"status":               {"/v1/created", "gzip"},
		"type not listed":      {"/v1/ndjson", "gzip"},
		"no type":              {"/v1/untyped", "gzip"},
		"already encoded":      {"/v1/encoded", "gzip"},
		"small":                {"/v1/small", "gzip"},
		"small, with length":   {"/v1/small-length", "gzip"},
	} {
		resp, body := gzipGet(t, http.MethodGet, base+c.path, c.acceptEncoding)
		if c.path == "/v1/encoded" {
			require.Equal(t, "br", resp.Header.Get("Content-Encoding"), name)
		} else {
			require.Empty(t, resp.Header.Get("Content-Encoding"), name)
		}
		require.Empty(t, resp.Header.Get("Vary"), name)
		require.NotEmpty(t, body, name)
	}
	resp, body = gzipGet(t, http.MethodHead, base+"/v1/account", "gzip")
	require.Empty(t, resp.Header.Get("Content-Encoding"))
	require.Empty(t, body)
	resp, body = gzipGet(t, http.MethodGet, base+"/v1/small-length", "gzip")
	require.Equal(t, int64(1000), resp.ContentLength)
	require.Len(t, body, 1000)

	// HTTP/1.0 clients get none, like nginx's default gzip_http_version 1.1
	resp = rawRoundTrip(t, s.HTTPAddr(), "GET /v1/account HTTP/1.0\r\nHost: x\r\nAccept-Encoding: gzip\r\n\r\n")
	require.Empty(t, resp.Header.Get("Content-Encoding"))
}

func TestGzip_Streams(t *testing.T) {
	// A compressed response that the upstream flushes reaches the client as it goes (sync
	// flushes), so a slow response is no stalled one; subscriptions are never compressed
	app1 := newTestUpstream(t, "app1")
	next := make(chan struct{})
	app1.on("/mytopic/raw", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		io.WriteString(w, gzipBody)
		w.(http.Flusher).Flush()
		select {
		case <-next:
		case <-r.Context().Done():
		}
		io.WriteString(w, "last\n")
	})
	s, _ := newTestServer(t, testGzip, app1)
	base := "http://" + s.HTTPAddr()

	// A poll (no stream): compressed, and the first part arrives before the rest is sent
	req, err := http.NewRequest(http.MethodGet, base+"/mytopic/raw?poll=1", nil)
	require.NoError(t, err)
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err := (&http.Transport{DisableCompression: true}).RoundTrip(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, "gzip", resp.Header.Get("Content-Encoding"))
	zr, err := gzip.NewReader(resp.Body)
	require.NoError(t, err)
	first := make([]byte, len(gzipBody))
	_, err = io.ReadFull(zr, first)
	require.NoError(t, err)
	require.Equal(t, gzipBody, string(first))
	next <- struct{}{}
	rest, err := io.ReadAll(zr)
	require.NoError(t, err)
	require.Equal(t, "last\n", string(rest))

	// The same as a subscription: as is
	req, err = http.NewRequest(http.MethodGet, base+"/mytopic/raw", nil)
	require.NoError(t, err)
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err = (&http.Transport{DisableCompression: true}).RoundTrip(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Empty(t, resp.Header.Get("Content-Encoding"))
	first = make([]byte, len(gzipBody))
	_, err = io.ReadFull(resp.Body, first)
	require.NoError(t, err)
	require.Equal(t, gzipBody, string(first))
	next <- struct{}{}
}

func TestGzip_UpgradesUntouched(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	app1.on("/echo/ws", upgradeThen(func(conn net.Conn) {
		io.Copy(conn, conn)
	}))
	s, _ := newTestServer(t, testGzip, app1)
	conn, err := net.Dial("tcp", s.HTTPAddr())
	require.NoError(t, err)
	defer conn.Close()
	fmt.Fprint(conn, "GET /echo/ws HTTP/1.1\r\nHost: x\r\nUpgrade: test\r\nConnection: Upgrade\r\nAccept-Encoding: gzip\r\n\r\n")
	r := bufio.NewReader(conn)
	resp, err := http.ReadResponse(r, nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	require.Empty(t, resp.Header.Get("Content-Encoding"))
	fmt.Fprint(conn, "ping\n")
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	line, err := r.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "ping\n", line)
}

func TestGzip_StaticAndRules(t *testing.T) {
	// Files served by a static rule are compressed too (not ranges of them), and the access log
	// counts the bytes sent; a site's own gzip settings replace the global ones
	app1 := newTestUpstream(t, "app1")
	dir := t.TempDir()
	root := filepath.Join(dir, "html")
	require.NoError(t, os.MkdirAll(root, 0755))
	page := "<html>" + strings.Repeat("<p>ntfy</p>", 300) + "</html>"
	require.NoError(t, os.WriteFile(filepath.Join(root, "index.html"), []byte(page), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "app.css"), []byte(strings.Repeat("p{}", 1000)), 0644))
	accessLog := filepath.Join(dir, "access.log")
	s, _ := newTestServer(t, testGzip+`
access-logs: [{file: `+accessLog+`}]
rules:
  - {name: website, match: 'path == "/" || path endsWith ".css"', action: static, root: `+root+`, fallback: /index.html}
  - {name: blocked, match: 'path == "/blocked"', action: block}
`, app1)
	base := "http://" + s.HTTPAddr()

	// The page, compressed, and the log line with its compressed size
	req, err := http.NewRequest(http.MethodGet, base+"/", nil)
	require.NoError(t, err)
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err := (&http.Transport{DisableCompression: true}).RoundTrip(req)
	require.NoError(t, err)
	compressed, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, "gzip", resp.Header.Get("Content-Encoding"))
	require.Empty(t, resp.Header.Get("Accept-Ranges"))
	require.Less(t, len(compressed), len(page)/4)
	zr, err := gzip.NewReader(bytes.NewReader(compressed))
	require.NoError(t, err)
	decompressed, err := io.ReadAll(zr)
	require.NoError(t, err)
	require.Equal(t, page, string(decompressed))
	waitFor(t, func() bool {
		b, _ := os.ReadFile(accessLog)
		return strings.Contains(string(b), fmt.Sprintf(`"GET / HTTP/1.1" 200 %d `, len(compressed)))
	})

	// Not listed (text/css), a range, a short block page: as is
	resp, body := gzipGet(t, http.MethodGet, base+"/app.css", "gzip")
	require.Empty(t, resp.Header.Get("Content-Encoding"))
	require.Len(t, body, 3000)
	resp, body = gzipGet(t, http.MethodGet, base+"/", "gzip", "Range", "bytes=0-5")
	require.Equal(t, http.StatusPartialContent, resp.StatusCode)
	require.Empty(t, resp.Header.Get("Content-Encoding"))
	require.Equal(t, "<html>", body)
	resp, _ = gzipGet(t, http.MethodGet, base+"/blocked", "gzip")
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
	require.Empty(t, resp.Header.Get("Content-Encoding"))
}

func TestGzip_HTTP2(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	app1.on("/v1/account", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, gzipBody)
	})
	s := newTLSTestServer(t, testGzip, app1)
	transport := &http.Transport{
		TLSClientConfig:    &tls.Config{InsecureSkipVerify: true},
		ForceAttemptHTTP2:  true,
		DisableCompression: true,
	}
	defer transport.CloseIdleConnections()
	req, err := http.NewRequest(http.MethodGet, "https://"+s.HTTPSAddr()+"/v1/account", nil)
	require.NoError(t, err)
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err := transport.RoundTrip(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 2, resp.ProtoMajor)
	require.Equal(t, "gzip", resp.Header.Get("Content-Encoding"))
	zr, err := gzip.NewReader(resp.Body)
	require.NoError(t, err)
	body, err := io.ReadAll(zr)
	require.NoError(t, err)
	require.Equal(t, gzipBody, string(body))
}

func TestGzip_Expect100Continue(t *testing.T) {
	// ntfy's own 100 Continue passes through the compressor untouched; the answer is compressed
	app1 := newTestUpstream(t, "app1")
	app1.on("/mytopic", func(w http.ResponseWriter, r *http.Request) {
		io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, gzipBody)
	})
	s, _ := newTestServer(t, testGzip, app1)
	conn, err := net.Dial("tcp", s.HTTPAddr())
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.Write([]byte("PUT /mytopic HTTP/1.1\r\nHost: ntfy.sh\r\nAccept-Encoding: gzip\r\nContent-Length: 5\r\nExpect: 100-continue\r\n\r\nhello"))
	require.NoError(t, err)
	r := bufio.NewReader(conn)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp, err := http.ReadResponse(r, nil)
	require.NoError(t, err)
	for resp.StatusCode == http.StatusContinue {
		resp, err = http.ReadResponse(r, nil)
		require.NoError(t, err)
	}
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "gzip", resp.Header.Get("Content-Encoding"))
	zr, err := gzip.NewReader(resp.Body)
	require.NoError(t, err)
	body, err := io.ReadAll(zr)
	require.NoError(t, err)
	require.Equal(t, gzipBody, string(body))
}

// flushErrorRecorder is a ResponseWriter whose flush fails, like a reset HTTP/2 stream
type flushErrorRecorder struct {
	*httptest.ResponseRecorder
}

func (r *flushErrorRecorder) FlushError() error {
	return errors.New("stream closed")
}

func TestGzip_WriterFlushErrorAndEarlyFlush(t *testing.T) {
	gz := &config.Gzip{Types: []string{"text/plain"}}
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Accept-Encoding", "gzip")

	// A failed flush reaches the caller (the stream path ends on it)
	w := newGzipWriter(&flushErrorRecorder{httptest.NewRecorder()}, req, gz, &proxyRequest{kind: kindRequest})
	require.EqualError(t, http.NewResponseController(w).Flush(), "stream closed")

	// A flush before the header sends a 200 as is: a later body must not be compressed then
	rec := httptest.NewRecorder()
	w = newGzipWriter(rec, req, gz, &proxyRequest{kind: kindRequest})
	w.Header().Set("Content-Type", "text/plain")
	http.NewResponseController(w).Flush()
	io.WriteString(w, gzipBody)
	w.(*gzipWriter).close()
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, rec.Header().Get("Content-Encoding") == "gzip", !strings.HasPrefix(rec.Body.String(), gzipBody))
}

func TestGzip_NotOnSubscriptions(t *testing.T) {
	// A subscription never compresses, so it does not keep a wrapper for its whole life either
	req := httptest.NewRequest(http.MethodGet, "/mytopic/json", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()
	w := newGzipWriter(rec, req, &config.Gzip{Types: []string{"text/plain"}}, &proxyRequest{kind: kindRequest})
	require.Same(t, rec, unwrapGzip(w))
}
