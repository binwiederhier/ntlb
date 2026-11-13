package server

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestServer_HealthListener(t *testing.T) {
	// listen-health serves only /v1/ready, on a port of its own, so that an outside health check
	// (e.g. Route53's) can reach it without it shadowing a path of the sites
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "listen-health: '127.0.0.1:0'", app1)
	status, body := get(t, http.DefaultClient, "http://"+s.HealthAddr()+"/v1/ready")
	require.Equal(t, http.StatusOK, status)
	require.JSONEq(t, `{"ready":true,"draining":false,"withdrawing":false}`, body)

	// Nothing else: no admin API, no metrics, no pprof
	for _, path := range []string{"/v1/status", "/v1/health", "/metrics", "/debug/pprof/"} {
		status, _ := get(t, http.DefaultClient, "http://"+s.HealthAddr()+path)
		require.Equal(t, http.StatusNotFound, status, path)
	}
	resp, err := http.Post("http://"+s.HealthAddr()+"/v1/reload", "", nil)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)

	// The site's own /v1/ready is still the upstream's
	status, body = get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/v1/ready")
	require.Equal(t, http.StatusOK, status)
	require.NotContains(t, body, "withdrawing")
}

func TestServer_NoHealthListener(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "", app1)
	require.Empty(t, s.HealthAddr())
}

func TestServer_ShutdownWithdrawsBeforeDraining(t *testing.T) {
	// Like ntfy's drain: first withdrawing for the delay (not ready, so an outside health check
	// takes the load balancer out of DNS, but everything is still served, new streams included,
	// since clients with a cached address keep coming), then the paced drain, then the exit
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "listen-health: '127.0.0.1:0'", app1)
	r, closeStream := openStream(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/stream")
	defer closeStream()
	done := make(chan struct{})
	start := time.Now()
	go func() {
		s.Shutdown(time.Second, 300*time.Millisecond)
		close(done)
	}()
	waitFor(t, func() bool {
		status, _ := get(t, http.DefaultClient, "http://"+s.HealthAddr()+"/v1/ready")
		return status == http.StatusServiceUnavailable
	})
	_, body := get(t, http.DefaultClient, "http://"+s.HealthAddr()+"/v1/ready")
	require.JSONEq(t, `{"ready":false,"draining":false,"withdrawing":true}`, body)
	status, _ := get(t, http.DefaultClient, "http://"+s.AdminAddr()+"/v1/ready")
	require.Equal(t, http.StatusServiceUnavailable, status)

	// Withdrawing: the open stream stays, a new subscription and a request are served
	r2, closeStream2 := openStream(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/mytopic/json")
	defer closeStream2()
	status, body = get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/name")
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "app1", body)
	require.False(t, streamClosed(r))

	// Then the drain: both streams closed, new subscriptions refused, then the exit
	waitFor(t, func() bool {
		return streamClosed(r) && streamClosed(r2)
	})
	require.GreaterOrEqual(t, time.Since(start), time.Second)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not finish")
	}
}
