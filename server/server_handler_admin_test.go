package server

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"heckel.io/ntfy-lb/balancer"
)

func adminDo(t *testing.T, s *Server, method, path, body string) (int, string) {
	req, err := http.NewRequest(method, "http://"+s.AdminAddr()+path, strings.NewReader(body))
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func adminStatus(t *testing.T, s *Server) *balancer.Status {
	code, body := adminDo(t, s, http.MethodGet, "/v1/status", "")
	require.Equal(t, http.StatusOK, code)
	status := &balancer.Status{}
	require.NoError(t, json.Unmarshal([]byte(body), status))
	return status
}

func TestAdmin_HealthAndReady(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "", app1)
	code, body := adminDo(t, s, http.MethodGet, "/v1/health", "")
	require.Equal(t, http.StatusOK, code)
	require.JSONEq(t, `{"healthy":true}`, body)
	code, body = adminDo(t, s, http.MethodGet, "/v1/ready", "")
	require.Equal(t, http.StatusOK, code)
	require.JSONEq(t, `{"ready":true,"draining":false}`, body)

	// Not ready while draining, still healthy
	code, _ = adminDo(t, s, http.MethodPost, "/v1/drain", `{"duration":"1h"}`)
	require.Equal(t, http.StatusOK, code)
	code, body = adminDo(t, s, http.MethodGet, "/v1/ready", "")
	require.Equal(t, http.StatusServiceUnavailable, code)
	require.JSONEq(t, `{"ready":false,"draining":true}`, body)
	code, _ = adminDo(t, s, http.MethodGet, "/v1/health", "")
	require.Equal(t, http.StatusOK, code)
	code, _ = adminDo(t, s, http.MethodDelete, "/v1/drain", "")
	require.Equal(t, http.StatusOK, code)
	code, _ = adminDo(t, s, http.MethodGet, "/v1/ready", "")
	require.Equal(t, http.StatusOK, code)
}

func TestAdmin_Unhealthy(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	app1.notReady.Store(true)
	s, _ := newTestServer(t, "", app1)
	code, body := adminDo(t, s, http.MethodGet, "/v1/health", "")
	require.Equal(t, http.StatusServiceUnavailable, code)
	require.JSONEq(t, `{"healthy":false}`, body)
}

func TestAdmin_DrainUpstream(t *testing.T) {
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	s, _ := newTestServer(t, "", app1, app2)
	code, _ := adminDo(t, s, http.MethodPost, "/v1/upstreams/app1/drain", `{"duration":"10m"}`)
	require.Equal(t, http.StatusOK, code)
	status := adminStatus(t, s)
	require.Equal(t, balancer.StateDraining, status.Upstreams[0].State)
	require.WithinDuration(t, time.Now().Add(10*time.Minute), *status.Upstreams[0].DrainDeadline, time.Minute)
	code, _ = adminDo(t, s, http.MethodDelete, "/v1/upstreams/app1/drain", "")
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, balancer.StateHealthy, adminStatus(t, s).Upstreams[0].State)

	// Errors
	code, _ = adminDo(t, s, http.MethodPost, "/v1/upstreams/nope/drain", "")
	require.Equal(t, http.StatusNotFound, code)
	code, _ = adminDo(t, s, http.MethodPost, "/v1/upstreams/app1/drain", `{"duration":"banana"}`)
	require.Equal(t, http.StatusBadRequest, code)
}

func TestAdmin_DrainDefaultDuration(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "drain-duration: 7m", app1)
	code, _ := adminDo(t, s, http.MethodPost, "/v1/upstreams/app1/drain", "")
	require.Equal(t, http.StatusOK, code)
	require.WithinDuration(t, time.Now().Add(7*time.Minute), *adminStatus(t, s).Upstreams[0].DrainDeadline, time.Minute)
}

func TestAdmin_Reload(t *testing.T) {
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	s, filename := newTestServer(t, "", app1)
	writeConfig(t, filename, "", app1, app2)
	code, _ := adminDo(t, s, http.MethodPost, "/v1/reload", "")
	require.Equal(t, http.StatusOK, code)
	require.Len(t, adminStatus(t, s).Upstreams, 2)

	writeConfig(t, filename, "listen-https: ':443'", app1)
	code, body := adminDo(t, s, http.MethodPost, "/v1/reload", "")
	require.Equal(t, http.StatusBadRequest, code)
	require.Contains(t, body, "cert-file")
}

func TestAdmin_Metrics(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "", app1)
	get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/name")
	code, body := adminDo(t, s, http.MethodGet, "/metrics", "")
	require.Equal(t, http.StatusOK, code)
	require.Contains(t, body, "ntfy_lb_ready 1\n")
	require.Contains(t, body, `ntfy_lb_upstream_healthy{upstream="app1"} 1`)
	require.Contains(t, body, `ntfy_lb_upstream_requests_total{upstream="app1"} 1`)
}
