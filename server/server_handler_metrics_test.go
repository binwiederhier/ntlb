package server

import (
	"net"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestServer_MetricsListener(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "listen-metrics: '127.0.0.1:0'", app1)
	status, body := get(t, http.DefaultClient, "http://"+s.MetricsAddr()+"/metrics")
	require.Equal(t, http.StatusOK, status)
	require.Contains(t, body, "ntlb_ready")

	// Nothing else: no admin API, no pprof
	for _, path := range []string{"/v1/status", "/debug/pprof/", "/v1/health"} {
		status, _ := get(t, http.DefaultClient, "http://"+s.MetricsAddr()+path)
		require.Equal(t, http.StatusNotFound, status, path)
	}
	resp, err := http.Post("http://"+s.MetricsAddr()+"/v1/reload", "", nil)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestServer_NoMetricsListener(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "", app1)
	require.Empty(t, s.MetricsAddr())
}

func TestServer_MetricsListenerBusy(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer busy.Close()
	filename := filepath.Join(t.TempDir(), "config.yml")
	writeConfig(t, filename, "listen-metrics: '"+busy.Addr().String()+"'", app1)
	_, err = New(filename)
	require.Error(t, err)
}
