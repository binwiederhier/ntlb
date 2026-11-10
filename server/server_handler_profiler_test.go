package server

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestServer_ProfilerListener(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "listen-profiler: '127.0.0.1:0'", app1)
	base := "http://" + s.ProfilerAddr()
	status, body := get(t, http.DefaultClient, base+"/debug/pprof/")
	require.Equal(t, http.StatusOK, status)
	require.Contains(t, body, "goroutine")
	for _, path := range []string{"/debug/pprof/heap?debug=1", "/debug/pprof/goroutine?debug=2", "/debug/pprof/allocs?debug=1", "/debug/pprof/profile?seconds=1", "/debug/pprof/trace?seconds=1"} {
		status, body := get(t, http.DefaultClient, base+path)
		require.Equal(t, http.StatusOK, status, path)
		require.NotEmpty(t, body, path)
	}
	status, body = get(t, http.DefaultClient, base+"/debug/pprof/goroutine?debug=2")
	require.Equal(t, http.StatusOK, status)
	require.True(t, strings.Contains(body, "goroutine "), "a goroutine dump")

	// Nothing else: no admin API, no metrics
	for _, path := range []string{"/v1/status", "/metrics", "/v1/health"} {
		status, _ := get(t, http.DefaultClient, base+path)
		require.Equal(t, http.StatusNotFound, status, path)
	}
}

func TestServer_NoProfilerListener(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "", app1)
	require.Empty(t, s.ProfilerAddr())
}
