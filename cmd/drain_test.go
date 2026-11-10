package cmd

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"heckel.io/ntlb/balancer"
	"heckel.io/ntlb/config"
)

func TestDrainUndrain(t *testing.T) {
	s, _ := newTestServer(t, "app1")
	_, err := run(t, s, "drain", "--duration", "1h")
	require.NoError(t, err)
	out, err := run(t, s, "status")
	require.NoError(t, err)
	require.Contains(t, out, "draining")
	_, err = run(t, s, "undrain")
	require.NoError(t, err)
	out, err = run(t, s, "status")
	require.NoError(t, err)
	require.NotContains(t, out, "draining")
}

func TestUpstreamDrainUndrain(t *testing.T) {
	s, _ := newTestServer(t, "app1", "app2")
	_, err := run(t, s, "upstream", "drain", "app2", "--duration", "30m")
	require.NoError(t, err)
	out, err := run(t, s, "status")
	require.NoError(t, err)
	require.Regexp(t, `app2\s+draining`, out)
	_, err = run(t, s, "upstream", "undrain", "app2")
	require.NoError(t, err)
	out, err = run(t, s, "status")
	require.NoError(t, err)
	require.Regexp(t, `app2\s+healthy`, out)

	_, err = run(t, s, "upstream", "drain", "nope")
	require.ErrorContains(t, err, "upstream not found")
	_, err = run(t, s, "upstream", "drain")
	require.Error(t, err)
}

func TestReload(t *testing.T) {
	s, filename := newTestServer(t, "app1", "app2")
	good, err := os.ReadFile(filename)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filename, []byte("listen-http: ':80'"), 0600))
	_, err = run(t, s, "reload")
	require.ErrorContains(t, err, "at least one site")
	require.NoError(t, os.WriteFile(filename, good, 0600))
	out, err := run(t, s, "reload")
	require.NoError(t, err)
	require.Equal(t, "Config reloaded\n", out)

	// Settings a reload cannot apply are reported
	changed := strings.Replace(string(good), "listen-http: '127.0.0.1:0'", "listen-http: '127.0.0.1:1'", 1)
	require.NoError(t, os.WriteFile(filename, []byte(changed), 0600))
	out, err = run(t, s, "reload")
	require.NoError(t, err)
	require.Equal(t, "Config reloaded, but these settings need a restart: listen-http\n", out)
}

func TestCheckConfig(t *testing.T) {
	_, filename := newTestServer(t, "app1")
	app := New()
	require.NoError(t, app.Run(context.Background(), []string{"ntlb", "check-config", "--config", filename}))
	require.NoError(t, os.WriteFile(filename, []byte("listen-http: ':80'"), 0600))
	require.ErrorContains(t, app.Run(context.Background(), []string{"ntlb", "check-config", "--config", filename}), "at least one site")
}

func TestDrain_Duration(t *testing.T) {
	f := newFakeAdmin(t, http.StatusOK, `{"success":true}`)
	out, err := runApp("--admin-url", f.srv.URL, "drain", "-d", "10m")
	require.NoError(t, err)
	require.Equal(t, "Draining the load balancer\n", out)
	require.Equal(t, http.MethodPost, f.method)
	require.Equal(t, "/v1/drain", f.path)
	require.JSONEq(t, `{"duration":"10m"}`, f.body)

	// A new app does not remember the last run's flags
	_, err = runApp("--admin-url", f.srv.URL, "drain")
	require.NoError(t, err)
	require.JSONEq(t, `{}`, f.body)
}

func TestDrain_InvalidDuration(t *testing.T) {
	s, _ := newTestServer(t, "app1")
	for _, d := range []string{"abc", "-5m", "10"} {
		_, err := run(t, s, "drain", "-d", d)
		require.EqualError(t, err, `invalid duration "`+d+`"`)
		_, err = run(t, s, "upstream", "drain", "app1", "-d", d)
		require.EqualError(t, err, `invalid duration "`+d+`"`)
	}
	out, err := run(t, s, "status")
	require.NoError(t, err)
	require.Contains(t, out, "Load balancer: ready")
	require.Regexp(t, `app1\s+healthy`, out)
}

func TestDrain_DefaultDurationFromConfig(t *testing.T) {
	s, _ := newTestServer(t, "app1")
	_, err := run(t, s, "drain")
	require.NoError(t, err)
	out, err := run(t, s, "status", "--json")
	require.NoError(t, err)
	status := &balancer.Status{}
	require.NoError(t, json.Unmarshal([]byte(out), status))
	require.True(t, status.Draining)
	require.WithinDuration(t, time.Now().Add(config.DefaultDrainDuration), *status.DrainDeadline, 10*time.Second)
}

func TestDrain_Error(t *testing.T) {
	f := newFakeAdmin(t, http.StatusInternalServerError, `{"error":"boom"}`)
	for _, args := range [][]string{{"drain"}, {"undrain"}, {"reload"}, {"upstream", "drain", "a"}, {"upstream", "undrain", "a"}} {
		out, err := runApp(append([]string{"--admin-url", f.srv.URL}, args...)...)
		require.EqualError(t, err, "boom", args)
		require.Empty(t, out)
	}
}

func TestUpstreamDrain_Output(t *testing.T) {
	f := newFakeAdmin(t, http.StatusOK, `{"success":true}`)
	out, err := runApp("--admin-url", f.srv.URL, "upstream", "drain", "-d", "1m", "app2")
	require.NoError(t, err)
	require.Equal(t, "Draining upstream app2\n", out)
	require.Equal(t, "/v1/upstreams/app2/drain", f.path)
	require.JSONEq(t, `{"duration":"1m"}`, f.body)
	out, err = runApp("--admin-url", f.srv.URL, "upstream", "undrain", "app2")
	require.NoError(t, err)
	require.Equal(t, "Upstream app2 undrained\n", out)
	require.Equal(t, http.MethodDelete, f.method)
}

func TestUpstreamDrain_ArgCount(t *testing.T) {
	f := newFakeAdmin(t, http.StatusOK, `{"success":true}`)
	for _, args := range [][]string{{"upstream", "drain"}, {"upstream", "drain", "a", "b"}, {"upstream", "undrain"}, {"upstream", "undrain", "a", "b"}} {
		_, err := runApp(append([]string{"--admin-url", f.srv.URL}, args...)...)
		require.EqualError(t, err, "expected exactly one upstream name", args)
	}
	require.Empty(t, f.method)
}

func TestUpstreamUndrain_Unknown(t *testing.T) {
	s, _ := newTestServer(t, "app1")
	_, err := run(t, s, "upstream", "undrain", "nope")
	require.EqualError(t, err, "upstream not found")
}
