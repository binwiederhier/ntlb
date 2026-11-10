package cmd

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
	"heckel.io/ntlb/config"
)

// fakeAdmin is an admin API on 127.0.0.1 that answers every request with status and body, and
// records the last request
type fakeAdmin struct {
	srv    *httptest.Server
	method string
	path   string
	host   string
	body   string
}

func newFakeAdmin(t *testing.T, status int, body string) *fakeAdmin {
	return newFakeAdminOn(t, "127.0.0.1:0", status, body)
}

func newFakeAdminOn(t *testing.T, addr string, status int, body string) *fakeAdmin {
	f := &fakeAdmin{}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Skipf("cannot listen on %s: %s", addr, err.Error())
	}
	f.srv = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		f.method, f.path, f.host, f.body = r.Method, r.URL.EscapedPath(), r.Host, string(b)
		w.WriteHeader(status)
		io.WriteString(w, body)
	}))
	f.srv.Listener.Close()
	f.srv.Listener = ln
	f.srv.Start()
	t.Cleanup(f.srv.Close)
	return f
}

// port returns the fake admin API's port
func (f *fakeAdmin) port() string {
	_, port, _ := net.SplitHostPort(f.srv.Listener.Addr().String())
	return port
}

// runApp runs the CLI with exactly args (no --admin-url added) and returns its output
func runApp(args ...string) (string, error) {
	app := New()
	var out bytes.Buffer
	app.Writer, app.ErrWriter = &out, &out
	app.ExitErrHandler = func(context.Context, *cli.Command, error) {} // Do not os.Exit the test
	err := app.Run(context.Background(), append([]string{"ntlb"}, args...))
	return out.String(), err
}

// writeConfig writes a config file with the given listen-admin and returns its path
func writeConfig(t *testing.T, listenAdmin string) string {
	filename := filepath.Join(t.TempDir(), "config.yml")
	conf := "sites: [{name: s, listen-http: '127.0.0.1:0', upstreams: [{name: app1, addresses: ['127.0.0.1:1']}]}]\n"
	if listenAdmin != "" {
		conf += "listen-admin: '" + listenAdmin + "'\n"
	}
	require.NoError(t, os.WriteFile(filename, []byte(conf), 0600))
	return filename
}

// Admin URL resolution: --admin-url, else listen-admin from the config file, else the default

func TestNewClient_AdminURLFlag(t *testing.T) {
	f := newFakeAdmin(t, http.StatusOK, `{"success":true}`)
	other := newFakeAdmin(t, http.StatusOK, `{"success":true}`)

	// --admin-url wins over the config file
	filename := writeConfig(t, "127.0.0.1:"+other.port())
	_, err := runApp("--config", filename, "--admin-url", f.srv.URL+"/", "undrain")
	require.NoError(t, err)
	require.Equal(t, "/v1/drain", f.path)
	require.Empty(t, other.path)
}

func TestNewClient_ListenAdminFromConfig(t *testing.T) {
	tests := []struct {
		name        string
		listenAdmin func(port string) string
	}{
		{
			name:        "ipv4",
			listenAdmin: func(port string) string { return "127.0.0.1:" + port },
		},
		{
			name:        "empty host",
			listenAdmin: func(port string) string { return ":" + port },
		},
		{
			name:        "ipv4 wildcard",
			listenAdmin: func(port string) string { return "0.0.0.0:" + port },
		},
		{
			name:        "ipv6 wildcard",
			listenAdmin: func(port string) string { return "[::]:" + port },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeAdmin(t, http.StatusOK, `{"success":true}`)
			filename := writeConfig(t, tt.listenAdmin(f.port()))
			out, err := runApp("--config", filename, "undrain")
			require.NoError(t, err)
			require.Equal(t, "Load balancer undrained\n", out)
			require.Equal(t, "127.0.0.1:"+f.port(), f.host)
		})
	}
}

func TestNewClient_Env(t *testing.T) {
	f := newFakeAdmin(t, http.StatusOK, `{"success":true}`)
	t.Setenv("NTLB_ADMIN_URL", f.srv.URL)
	_, err := runApp("undrain")
	require.NoError(t, err)
	require.Equal(t, http.MethodDelete, f.method)

	// NTLB_CONFIG is read when there is no admin URL, also after an earlier run of another
	// app in the same process had --config
	g := newFakeAdmin(t, http.StatusOK, `{"success":true}`)
	h := newFakeAdmin(t, http.StatusOK, `{"success":true}`)
	t.Setenv("NTLB_ADMIN_URL", "")
	_, err = runApp("--config", writeConfig(t, "127.0.0.1:"+h.port()), "undrain")
	require.NoError(t, err)
	require.Equal(t, http.MethodDelete, h.method)
	t.Setenv("NTLB_CONFIG", writeConfig(t, "127.0.0.1:"+g.port()))
	_, err = runApp("undrain")
	require.NoError(t, err)
	require.Equal(t, http.MethodDelete, g.method)
}

func TestNewClient_DefaultListenAdmin(t *testing.T) {
	// Without a readable config file (missing, or no listen-admin), the default address is used
	f := newFakeAdminOn(t, config.DefaultListenAdmin, http.StatusOK, `{"success":true}`)
	_, err := runApp("--config", filepath.Join(t.TempDir(), "missing.yml"), "undrain")
	require.NoError(t, err)
	require.Equal(t, config.DefaultListenAdmin, f.host)
	f.host = ""
	_, err = runApp("--config", writeConfig(t, ""), "undrain")
	require.NoError(t, err)
	require.Equal(t, config.DefaultListenAdmin, f.host)
}

func TestNewClient_InvalidConfigFallsBack(t *testing.T) {
	f := newFakeAdminOn(t, config.DefaultListenAdmin, http.StatusOK, `{"success":true}`)
	filename := filepath.Join(t.TempDir(), "config.yml")
	require.NoError(t, os.WriteFile(filename, []byte("listen-admin: '127.0.0.1:1'\n"), 0600)) // No upstreams
	_, err := runApp("--config", filename, "undrain")
	require.NoError(t, err)
	require.Equal(t, config.DefaultListenAdmin, f.host)
}

// Global flags

func TestApp_FlagsAfterSubcommand(t *testing.T) {
	f := newFakeAdmin(t, http.StatusOK, `{"success":true}`)
	_, err := runApp("undrain", "--admin-url", f.srv.URL)
	require.NoError(t, err)
	require.Equal(t, "/v1/drain", f.path)
	_, err = runApp("upstream", "undrain", "app1", "-a", f.srv.URL)
	require.NoError(t, err)
	require.Equal(t, "/v1/upstreams/app1/drain", f.path)
}

func TestApp_LogLevel(t *testing.T) {
	t.Cleanup(func() {
		slog.SetLogLoggerLevel(slog.LevelInfo)
	})
	f := newFakeAdmin(t, http.StatusOK, `{"success":true}`)
	_, err := runApp("--log-level", "debug", "--admin-url", f.srv.URL, "undrain")
	require.NoError(t, err)
	require.True(t, slog.Default().Enabled(context.Background(), slog.LevelDebug))
	_, err = runApp("--log-level", "WARN", "--admin-url", f.srv.URL, "undrain")
	require.NoError(t, err)
	require.False(t, slog.Default().Enabled(context.Background(), slog.LevelInfo))
	require.True(t, slog.Default().Enabled(context.Background(), slog.LevelWarn))
	_, err = runApp("--log-level", "loud", "--admin-url", f.srv.URL, "undrain")
	require.EqualError(t, err, `invalid log level "loud"`)
}

func TestApp_LogLevelEnv(t *testing.T) {
	t.Cleanup(func() {
		slog.SetLogLoggerLevel(slog.LevelInfo)
	})
	t.Setenv("NTLB_LOG_LEVEL", "nope")
	_, err := runApp("check-config")
	require.EqualError(t, err, `invalid log level "nope"`)
}

func TestApp_UnknownCommand(t *testing.T) {
	out, err := runApp("frobnicate")
	require.Error(t, err)
	require.ErrorContains(t, err, "No help topic for 'frobnicate'")
	require.Empty(t, out)
}
