package server

import (
	"io"
	"net/http"
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
