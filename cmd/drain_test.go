package cmd

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
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
	require.ErrorContains(t, err, "at least one upstream")
	require.NoError(t, os.WriteFile(filename, good, 0600))
	_, err = run(t, s, "reload")
	require.NoError(t, err)
}

func TestCheckConfig(t *testing.T) {
	_, filename := newTestServer(t, "app1")
	app := New()
	require.NoError(t, app.Run(context.Background(), []string{"ntfy-lb", "check-config", "--config", filename}))
	require.NoError(t, os.WriteFile(filename, []byte("listen-http: ':80'"), 0600))
	require.ErrorContains(t, app.Run(context.Background(), []string{"ntfy-lb", "check-config", "--config", filename}), "at least one upstream")
}
