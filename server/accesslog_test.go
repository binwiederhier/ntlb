package server

import (
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAccessLog_AccessLog(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	logFile := filepath.Join(t.TempDir(), "access.log")
	s, _ := newTestServer(t, "access-log: "+logFile, app1)
	req, _ := http.NewRequest(http.MethodGet, "http://"+s.HTTPAddr()+"/name", nil)
	req.Header.Set("User-Agent", "ntfy-test")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	io.ReadAll(resp.Body)
	resp.Body.Close()
	waitFor(t, func() bool {
		b, _ := os.ReadFile(logFile)
		return strings.Contains(string(b), `"GET /name HTTP/1.1" 200 4 "-" "ntfy-test"`) && strings.Contains(string(b), "upstream=app1 upstream_status=200")
	})
}

func TestAccessLog_AbortedStream(t *testing.T) {
	// A stream that ends because the client went away (the proxy aborts the handler) is logged too
	app1 := newTestUpstream(t, "app1")
	logFile := filepath.Join(t.TempDir(), "access.log")
	s, _ := newTestServer(t, "access-log: "+logFile, app1)
	_, closeStream := openStream(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/stream")
	closeStream()
	waitFor(t, func() bool {
		b, _ := os.ReadFile(logFile)
		return strings.Contains(string(b), `"GET /stream HTTP/1.1" 200 6`)
	})
}
