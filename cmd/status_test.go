package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"heckel.io/ntlb/balancer"
	"heckel.io/ntlb/server"
)

// newTestServer runs a load balancer in front of fake upstreams that answer every request with 200
func newTestServer(t *testing.T, names ...string) (*server.Server, string) {
	filename := writeTestConfig(t, "", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}), names...)
	return startTestServer(t, filename), filename
}

// writeTestConfig writes a config file with extra settings and one upstream per name, each served
// by handler, and returns its path
func writeTestConfig(t *testing.T, extra string, handler http.Handler, names ...string) string {
	var b strings.Builder
	b.WriteString("listen-admin: '127.0.0.1:0'\n")
	b.WriteString(extra)
	b.WriteString("sites:\n  - name: s\n    listen-http: '127.0.0.1:0'\n    upstream:\n      health-check: {interval: 1h, timeout: 1s}\n      servers:\n")
	for _, name := range names {
		upstream := httptest.NewServer(handler)
		t.Cleanup(upstream.Close)
		fmt.Fprintf(&b, "      - name: %s\n        addresses: [%q]\n", name, strings.TrimPrefix(upstream.URL, "http://"))
	}
	filename := filepath.Join(t.TempDir(), "config.yml")
	require.NoError(t, os.WriteFile(filename, []byte(b.String()), 0600))
	return filename
}

// startTestServer serves the config file and waits until the load balancer is ready
func startTestServer(t *testing.T, filename string) *server.Server {
	s, err := server.New(filename)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	go s.Serve(ctx)
	t.Cleanup(func() {
		s.Shutdown(0)
		cancel()
	})
	waitReady(t, s.AdminAddr())
	return s
}

// waitReady waits until the admin API at addr reports ready
func waitReady(t *testing.T, addr string) {
	require.Eventually(t, func() bool {
		resp, err := http.Get("http://" + addr + "/v1/ready")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 5*time.Second, 20*time.Millisecond)
}

func run(t *testing.T, s *server.Server, args ...string) (string, error) {
	app := New()
	var out bytes.Buffer
	app.Writer, app.ErrWriter = &out, &out
	err := app.Run(context.Background(), append([]string{"ntlb", "--admin-url", "http://" + s.AdminAddr()}, args...))
	return out.String(), err
}

func TestStatus(t *testing.T) {
	s, _ := newTestServer(t, "app1", "app2")
	out, err := run(t, s, "status")
	require.NoError(t, err)
	require.Contains(t, out, "ready")
	require.Regexp(t, `app1\s+healthy`, out)
	require.Regexp(t, `app2\s+healthy`, out)
}

func TestStatus_JSON(t *testing.T) {
	s, _ := newTestServer(t, "app1")
	out, err := run(t, s, "status", "--json")
	require.NoError(t, err)
	status := &balancer.Status{}
	require.NoError(t, json.Unmarshal([]byte(out), status))
	require.True(t, status.Ready)
	require.Equal(t, "app1", status.Upstreams[0].Name)
}

func TestStatus_Unreachable(t *testing.T) {
	app := New()
	var out bytes.Buffer
	app.Writer, app.ErrWriter = &out, &out
	err := app.Run(context.Background(), []string{"ntlb", "--admin-url", "http://127.0.0.1:1", "status"})
	require.Error(t, err)
}

// Status output for every state, against a fake admin API

func TestLBState(t *testing.T) {
	deadline := time.Now().Add(time.Hour + 500*time.Millisecond)
	tests := []struct {
		name   string
		status *balancer.Status
		state  string
	}{
		{
			name:   "ready",
			status: &balancer.Status{Healthy: true, Ready: true},
			state:  `^ready$`,
		},
		{
			name:   "unhealthy",
			status: &balancer.Status{},
			state:  `^unhealthy \(no healthy upstream, sending to all\)$`,
		},
		{
			name:   "draining",
			status: &balancer.Status{Healthy: true, Draining: true, DrainDeadline: &deadline},
			state:  `^draining, done in (1h0m0s|59m59s) \(\d\d:\d\d:\d\d\)$`,
		},
		{
			name:   "drained wins over draining",
			status: &balancer.Status{Healthy: true, Draining: true, Drained: true, DrainDeadline: &deadline},
			state:  `^drained \(turning requests away\)$`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Regexp(t, tt.state, lbState(tt.status))
		})
	}
}

func TestUpstreamNote(t *testing.T) {
	deadline := time.Now().Add(90*time.Second + 500*time.Millisecond)
	tests := []struct {
		name     string
		upstream *balancer.UpstreamStatus
		note     string
	}{
		{
			name:     "healthy",
			upstream: &balancer.UpstreamStatus{State: balancer.StateHealthy, Healthy: true},
			note:     `^$`,
		},
		{
			name:     "unhealthy with last error",
			upstream: &balancer.UpstreamStatus{State: balancer.StateUnhealthy, LastError: "connection refused"},
			note:     `^last check: connection refused$`,
		},
		{
			name:     "draining",
			upstream: &balancer.UpstreamStatus{State: balancer.StateDraining, Healthy: true, DrainDeadline: &deadline},
			note:     `^drained in 1m(30|29)s \(\d\d:\d\d:\d\d\)$`,
		},
		{
			name:     "draining without deadline",
			upstream: &balancer.UpstreamStatus{State: balancer.StateDraining, Healthy: true},
			note:     `^$`,
		},
		{
			name:     "draining and unhealthy",
			upstream: &balancer.UpstreamStatus{State: balancer.StateDraining, DrainDeadline: &deadline, LastError: "timeout"},
			note:     `^drained in 1m(30|29)s \(\d\d:\d\d:\d\d\); unhealthy; last check: timeout$`,
		},
		{
			name:     "drained",
			upstream: &balancer.UpstreamStatus{State: balancer.StateDrained, Healthy: true},
			note:     `^$`,
		},
		{
			name:     "drained and unhealthy",
			upstream: &balancer.UpstreamStatus{State: balancer.StateDrained},
			note:     `^unhealthy$`,
		},
		{
			name:     "removed from config",
			upstream: &balancer.UpstreamStatus{State: balancer.StateDraining, Healthy: true, Removed: true, DrainDeadline: &deadline},
			note:     `^removed from config; drained in 1m(30|29)s \(\d\d:\d\d:\d\d\)$`,
		},
		{
			name:     "removed and drained",
			upstream: &balancer.UpstreamStatus{State: balancer.StateDrained, Healthy: true, Removed: true},
			note:     `^removed from config$`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Regexp(t, tt.note, upstreamNote(tt.upstream))
		})
	}
}

func TestStatus_AllStates(t *testing.T) {
	deadline := time.Now().Add(time.Hour + 500*time.Millisecond)
	status := &balancer.Status{
		Healthy:       true,
		Draining:      true,
		DrainDeadline: &deadline,
		Streams:       42,
		Upstreams: []*balancer.UpstreamStatus{
			{
				Site:      "ntfy",
				Name:      "app1",
				State:     balancer.StateHealthy,
				Healthy:   true,
				Addresses: []string{"10.0.0.1:2586", "10.0.0.1:2587"},
				Streams:   40,
				Requests:  1000,
				Failures:  3,
			},
			{
				Site:      "ntfy",
				Name:      "app2",
				State:     balancer.StateUnhealthy,
				Addresses: []string{"10.0.0.2:2586"},
				LastError: "connection refused",
			},
			{
				Site:          "ntfy",
				Name:          "app3",
				State:         balancer.StateDraining,
				Addresses:     []string{"10.0.0.3:2586"},
				Streams:       2,
				DrainDeadline: &deadline,
			},
			{
				Site:      "ntfy",
				Name:      "app4",
				State:     balancer.StateDrained,
				Healthy:   true,
				Removed:   true,
				Addresses: []string{"10.0.0.4:2586"},
			},
		},
	}
	b, err := json.Marshal(status)
	require.NoError(t, err)
	f := newFakeAdmin(t, http.StatusOK, string(b))
	out, err := runApp("--admin-url", f.srv.URL, "status")
	require.NoError(t, err)
	require.Equal(t, "/v1/status", f.path)
	lines := strings.Split(out, "\n")
	require.Regexp(t, `^Load balancer: draining, done in (1h0m0s|59m59s) \(\d\d:\d\d:\d\d\), 42 stream\(s\)$`, lines[0])
	require.Empty(t, lines[1])
	require.Regexp(t, `^SITE\s+UPSTREAM\s+STATE\s+STREAMS\s+REQUESTS\s+FAILURES\s+ADDRESSES\s+NOTE$`, lines[2])
	require.Regexp(t, `^ntfy\s+app1\s+healthy\s+40\s+1000\s+3\s+10\.0\.0\.1:2586,10\.0\.0\.1:2587\s*$`, lines[3])
	require.Regexp(t, `^ntfy\s+app2\s+unhealthy\s+0\s+0\s+0\s+10\.0\.0\.2:2586\s+last check: connection refused$`, lines[4])
	require.Regexp(t, `^ntfy\s+app3\s+draining\s+2\s+0\s+0\s+10\.0\.0\.3:2586\s+drained in (1h0m0s|59m59s) \(\d\d:\d\d:\d\d\); unhealthy$`, lines[5])
	require.Regexp(t, `^ntfy\s+app4\s+drained\s+0\s+0\s+0\s+10\.0\.0\.4:2586\s+removed from config$`, lines[6])
	require.Len(t, lines, 8) // Trailing newline

	// The columns line up
	col := strings.Index(lines[2], "ADDRESSES")
	for _, line := range lines[3:7] {
		require.Equal(t, "10.0.0.", line[col:col+7], line)
	}

	// --json and -j print the status as it came
	for _, flag := range []string{"--json", "-j"} {
		out, err = runApp("--admin-url", f.srv.URL, "status", flag)
		require.NoError(t, err)
		require.JSONEq(t, string(b), out)
		require.True(t, strings.HasPrefix(out, "{\n  \""), "indented")
	}
}

func TestStatus_LBDrainedAndUnhealthy(t *testing.T) {
	f := newFakeAdmin(t, http.StatusOK, `{"drained":true,"draining":true,"streams":0,"upstreams":[]}`)
	out, err := runApp("--admin-url", f.srv.URL, "status")
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(out, "Load balancer: drained (turning requests away), 0 stream(s)\n"))
	f = newFakeAdmin(t, http.StatusOK, `{"streams":7,"upstreams":[{"name":"a","state":"unhealthy"}]}`)
	out, err = runApp("--admin-url", f.srv.URL, "status")
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(out, "Load balancer: unhealthy (no healthy upstream, sending to all), 7 stream(s)\n"))
}

func TestStatus_LiveDrains(t *testing.T) {
	// The real load balancer's states, as the status command shows them
	s, _ := newTestServer(t, "app1", "app2")
	_, err := run(t, s, "upstream", "drain", "app1", "-d", "1h")
	require.NoError(t, err)
	_, err = run(t, s, "upstream", "drain", "app2", "-d", "0s")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		out, err := run(t, s, "status")
		require.NoError(t, err)
		return regexp.MustCompile(`app2\s+drained\s`).MatchString(out)
	}, 5*time.Second, 20*time.Millisecond)
	out, err := run(t, s, "status")
	require.NoError(t, err)
	require.Regexp(t, `app1\s+draining\s.*drained in (1h0m0s|59m59s) \(`, out)
	require.Regexp(t, `app2\s+drained\s`, out)
	require.Contains(t, out, "Load balancer: unhealthy")
	_, err = run(t, s, "drain", "-d", "0s")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		out, err := run(t, s, "status")
		require.NoError(t, err)
		return strings.Contains(out, "Load balancer: drained (turning requests away)")
	}, 5*time.Second, 20*time.Millisecond)
}

func TestStatus_Error(t *testing.T) {
	f := newFakeAdmin(t, http.StatusInternalServerError, `{"error":"broken"}`)
	out, err := runApp("--admin-url", f.srv.URL, "status")
	require.EqualError(t, err, "broken")
	require.Empty(t, out)
	_, err = runApp("--admin-url", f.srv.URL, "status", "--json")
	require.EqualError(t, err, "broken")
}
