package server

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"heckel.io/ntlb/balancer"
)

func adminDo(t testing.TB, s *Server, method, path, body string) (int, string) {
	req, err := http.NewRequest(method, "http://"+s.AdminAddr()+path, strings.NewReader(body))
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func adminStatus(t testing.TB, s *Server) *balancer.Status {
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

	// By site/name, escaped
	code, _ = adminDo(t, s, http.MethodPost, "/v1/upstreams/s%2Fapp2/drain", "")
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, balancer.StateDraining, adminStatus(t, s).Upstreams[1].State)
	code, _ = adminDo(t, s, http.MethodDelete, "/v1/upstreams/s%2Fapp2/drain", "")
	require.Equal(t, http.StatusOK, code)
	code, _ = adminDo(t, s, http.MethodPost, "/v1/upstreams/x%2Fapp2/drain", "")
	require.Equal(t, http.StatusNotFound, code)
}

func TestAdmin_DrainUpstreamAmbiguous(t *testing.T) {
	a1, b1 := newTestUpstream(t, "app1"), newTestUpstream(t, "app1")
	filename := filepath.Join(t.TempDir(), "config.yml")
	require.NoError(t, os.WriteFile(filename, []byte(fmt.Sprintf(`
listen-admin: '127.0.0.1:0'
sites:
  - {name: a, listen-http: '127.0.0.1:0', hosts: [a.com], `+testNtfySite+`, upstreams: {default: {`+testNtfyUpstream+`, servers: [{name: app1, addresses: [%q]}]}}}
  - {name: b, listen-http: '127.0.0.1:0', `+testNtfySite+`, upstreams: {default: {`+testNtfyUpstream+`, servers: [{name: app1, addresses: [%q]}]}}}
`, a1.addr(), b1.addr())), 0600))
	s := startTestServer(t, filename)
	code, body := adminDo(t, s, http.MethodPost, "/v1/upstreams/app1/drain", "")
	require.Equal(t, http.StatusBadRequest, code)
	require.Contains(t, body, "site/name")
	code, _ = adminDo(t, s, http.MethodDelete, "/v1/upstreams/app1/drain", "")
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
	code, body := adminDo(t, s, http.MethodPost, "/v1/reload", "")
	require.Equal(t, http.StatusOK, code)
	require.JSONEq(t, `{"success":true}`, body)
	require.Len(t, adminStatus(t, s).Upstreams, 2)

	writeConfig(t, filename, "client-keepalive: {count: 9}", app1, app2)
	code, body = adminDo(t, s, http.MethodPost, "/v1/reload", "")
	require.Equal(t, http.StatusOK, code)
	require.JSONEq(t, `{"success":true,"restart_required":["client-keepalive"]}`, body)

	writeConfig(t, filename, "listen-https: ':443'", app1)
	code, body = adminDo(t, s, http.MethodPost, "/v1/reload", "")
	require.Equal(t, http.StatusBadRequest, code)
	require.Contains(t, body, "cert-file")
}

func TestAdmin_Metrics(t *testing.T) {
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	s, _ := newTestServer(t, "", app1, app2)
	client := clientForRank(t, s, "app1", "app2")
	get(t, client, "http://"+s.HTTPAddr()+"/name")
	get(t, client, "http://"+s.HTTPAddr()+"/fail503") // Retried on app2, which says 503 too
	_, r := openSubscription(t, client, "http://"+s.HTTPAddr()+"/mytopic/json")
	_ = r
	require.NoError(t, s.balancer.DrainUpstream("app1", 0))
	waitFor(t, func() bool {
		return s.balancer.Status().Streams == 0
	})
	s.balancer.Drain(time.Hour)
	get(t, client, "http://"+s.HTTPAddr()+"/mytopic/json") // Refused while draining
	code, body := adminDo(t, s, http.MethodGet, "/metrics", "")
	require.Equal(t, http.StatusOK, code)
	for _, want := range []string{
		"ntlb_ready 0\n",
		"ntlb_draining 1\n",
		`ntlb_upstream_healthy{site="s",upstream="app1"} 1`,
		`ntlb_upstream_draining{site="s",upstream="app1"} 1`,
		`ntlb_upstream_streams{site="s",upstream="app2"} 0`,
		`ntlb_requests_total{code="200",kind="request",site="s",upstream="app1"} 1`,
		`ntlb_requests_total{code="503",kind="request",site="s",upstream="app2"} 1`,
		`ntlb_requests_total{code="200",kind="stream",site="s",upstream="app1"} 1`,
		`ntlb_requests_total{code="503",kind="stream",site="s",upstream=""} 1`,
		`ntlb_retries_total{reason="503"} 1`,
		`ntlb_refused_total 1`,
		`ntlb_streams_closed_total{reason="drained"} 1`,
		`ntlb_streams{protocol="http1"} 0`,
		`ntlb_request_duration_seconds_count{kind="request"} 2`,
		`ntlb_stream_duration_seconds_count 1`,
		"go_goroutines ",
		"process_resident_memory_bytes ",
	} {
		require.Contains(t, body, want)
	}
}

func TestAdmin_MetricsStreamsByProtocol(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	s := newTLSTestServer(t, "", app1)
	h2 := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
			ForceAttemptHTTP2: true,
		},
	}
	openSubscription(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/mytopic/json")
	openSubscription(t, h2, "https://"+s.HTTPSAddr()+"/mytopic/json")
	_, body := adminDo(t, s, http.MethodGet, "/metrics", "")
	require.Contains(t, body, `ntlb_streams{protocol="http1"} 1`)
	require.Contains(t, body, `ntlb_streams{protocol="http2"} 1`)
}
