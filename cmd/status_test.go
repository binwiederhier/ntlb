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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"heckel.io/ntfy-lb/balancer"
	"heckel.io/ntfy-lb/server"
)

// newTestServer runs a load balancer in front of fake upstreams that answer every request with 200
func newTestServer(t *testing.T, names ...string) (*server.Server, string) {
	var b strings.Builder
	b.WriteString("listen-http: '127.0.0.1:0'\nlisten-admin: '127.0.0.1:0'\nhealth-check: {interval: 1h, timeout: 1s}\nupstreams:\n")
	for _, name := range names {
		upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		t.Cleanup(upstream.Close)
		fmt.Fprintf(&b, "  - name: %s\n    addresses: [%q]\n", name, strings.TrimPrefix(upstream.URL, "http://"))
	}
	filename := filepath.Join(t.TempDir(), "config.yml")
	require.NoError(t, os.WriteFile(filename, []byte(b.String()), 0600))
	s, err := server.New(filename)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	go s.Serve(ctx)
	t.Cleanup(func() {
		s.Shutdown(0)
		cancel()
	})
	require.Eventually(t, func() bool {
		resp, err := http.Get("http://" + s.AdminAddr() + "/v1/ready")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 5*time.Second, 20*time.Millisecond)
	return s, filename
}

func run(t *testing.T, s *server.Server, args ...string) (string, error) {
	app := New()
	var out bytes.Buffer
	app.Writer, app.ErrWriter = &out, &out
	err := app.Run(context.Background(), append([]string{"ntfy-lb", "--admin-url", "http://" + s.AdminAddr()}, args...))
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
	err := app.Run(context.Background(), []string{"ntfy-lb", "--admin-url", "http://127.0.0.1:1", "status"})
	require.Error(t, err)
}
