package config

import (
	"errors"
	"io/fs"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"heckel.io/ntlb/match"
)

func TestParse_Defaults(t *testing.T) {
	c, err := Parse([]byte(`
listen-http: ":80"
sites:
  - name: s
    upstreams:
      - name: app1
        addresses: ["10.0.0.1:2586", "10.0.0.1:2587"]
`))
	require.NoError(t, err)
	require.Equal(t, DefaultListenAdmin, c.ListenAdmin)
	require.Equal(t, DefaultHealthPath, c.Sites[0].HealthCheck.Path)
	require.Equal(t, DefaultHealthFails, c.Sites[0].HealthCheck.Fails)
	require.Equal(t, DefaultUpstreamReadTimeout, c.UpstreamReadTimeout)
	require.Equal(t, DefaultKeepaliveIdle, c.ClientKeepalive.Idle)
	require.Equal(t, DefaultKeepaliveCount, c.UpstreamKeepalive.Count)
	require.Len(t, c.Sites[0].Upstreams, 1)
	require.Equal(t, []string{"10.0.0.1:2586", "10.0.0.1:2587"}, c.Sites[0].Upstreams[0].Addresses)
}

func TestParse_Overrides(t *testing.T) {
	c, err := Parse([]byte(`
listen-https: ":443"
drain-duration: 10m
client-keepalive:
  idle: 3m
  interval: 30s
  count: 5
upstream-keepalive:
  disable: true
sites:
  - name: s
    cert-file: /tmp/cert.pem
    key-file: /tmp/key.pem
    health-check: {path: /v1/health, interval: 2s, timeout: 1s}
    upstreams:
      - name: app1
        addresses: ["10.0.0.1:2586"]
`))
	require.NoError(t, err)
	require.Equal(t, 10*time.Minute, c.DrainDuration)
	require.Equal(t, 3*time.Minute, c.ClientKeepalive.KeepAliveConfig().Idle)
	require.Equal(t, 5, c.ClientKeepalive.KeepAliveConfig().Count)
	require.False(t, c.UpstreamKeepalive.KeepAliveConfig().Enable)
	require.Equal(t, "/v1/health", c.Sites[0].HealthCheck.Path)
	require.Equal(t, 2*time.Second, c.Sites[0].HealthCheck.Interval)
}

func TestParse_Invalid(t *testing.T) {
	tests := []struct {
		name   string
		config string
		errMsg string
	}{
		{
			name:   "no listener",
			config: "sites: [{name: s, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]",
			errMsg: "listen-http or listen-https",
		},
		{
			name:   "https without cert",
			config: "listen-https: ':443'\nsites: [{name: s, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]",
			errMsg: "cert-file",
		},
		{
			name:   "no upstreams",
			config: "listen-http: ':80'",
			errMsg: "at least one site",
		},
		{
			name:   "duplicate name",
			config: "listen-http: ':80'\nsites: [{name: s, upstreams: [{name: a, addresses: ['1.2.3.4:80']}, {name: a, addresses: ['1.2.3.5:80']}]}]",
			errMsg: "duplicate upstream name",
		},
		{
			name:   "bad name",
			config: "listen-http: ':80'\nsites: [{name: s, upstreams: [{name: 'a b', addresses: ['1.2.3.4:80']}]}]",
			errMsg: "invalid upstream name",
		},
		{
			name:   "no addresses",
			config: "listen-http: ':80'\nsites: [{name: s, upstreams: [{name: a}]}]",
			errMsg: "no addresses",
		},
		{
			name:   "bad address",
			config: "listen-http: ':80'\nsites: [{name: s, upstreams: [{name: a, addresses: ['1.2.3.4']}]}]",
			errMsg: "invalid address",
		},
		{
			name:   "timeout not below interval",
			config: "listen-http: ':80'\nsites: [{name: s, health-check: {interval: 1s, timeout: 1s}, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]",
			errMsg: "health-check.timeout",
		},
		{
			name:   "bad streams expression",
			config: "listen-http: ':80'\nsites: [{name: s, streams: {match: 'path =='}, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]",
			errMsg: "streams.match",
		},
		{
			name:   "ipv4 prefix too long",
			config: "listen-http: ':80'\nclient-key: {ipv4-prefix-bits: 33}\nsites: [{name: s, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]",
			errMsg: "client-key.ipv4-prefix-bits",
		},
		{
			name:   "ipv6 prefix negative",
			config: "listen-http: ':80'\nclient-key: {ipv6-prefix-bits: -1}\nsites: [{name: s, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]",
			errMsg: "client-key.ipv6-prefix-bits",
		},
		{
			name:   "negative drain",
			config: "listen-http: ':80'\ndrain-duration: -1s\nsites: [{name: s, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]",
			errMsg: "negative",
		},
		{
			name:   "bad yaml",
			config: "listen-http: [",
			errMsg: "invalid config",
		},
		{
			name:   "negative shutdown drain",
			config: "listen-http: ':80'\nshutdown-drain-duration: -1s\nsites: [{name: s, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]",
			errMsg: "drain durations must not be negative",
		},
		{
			name:   "https without key",
			config: "listen-https: ':443'\nsites: [{name: s, cert-file: c, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]",
			errMsg: "cert-file and key-file go together",
		},
		{
			name:   "ipv4 prefix negative",
			config: "listen-http: ':80'\nclient-key: {ipv4-prefix-bits: -1}\nsites: [{name: s, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]",
			errMsg: "client-key.ipv4-prefix-bits",
		},
		{
			name:   "ipv6 prefix too long",
			config: "listen-http: ':80'\nclient-key: {ipv6-prefix-bits: 129}\nsites: [{name: s, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]",
			errMsg: "client-key.ipv6-prefix-bits",
		},
		{
			name:   "name too long",
			config: "listen-http: ':80'\nsites: [{name: s, upstreams: [{name: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa, addresses: ['1.2.3.4:80']}]}]",
			errMsg: "invalid upstream name",
		},
		{
			name:   "empty address",
			config: "listen-http: ':80'\nsites: [{name: s, upstreams: [{name: a, addresses: ['']}]}]",
			errMsg: "invalid address",
		},
		{
			name:   "empty upstreams list",
			config: "listen-http: ':80'\nsites: [{name: s, upstreams: []}]",
			errMsg: "at least one upstream",
		},
		{
			name:   "null upstream",
			config: "listen-http: ':80'\nsites: [{name: s, upstreams: [null]}]",
			errMsg: "upstreams[0] is empty",
		},
		{
			name:   "empty upstream list item",
			config: "listen-http: ':80'\nsites:\n  - name: s\n    upstreams:\n      - name: a\n        addresses: ['1.2.3.4:80']\n      -\n",
			errMsg: "upstreams[1] is empty",
		},
		{
			name:   "negative connect timeout",
			config: "listen-http: ':80'\nupstream-connect-timeout: -1s\nsites: [{name: s, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]",
			errMsg: "upstream-connect-timeout must be positive",
		},
		{
			name:   "negative read timeout",
			config: "listen-http: ':80'\nupstream-read-timeout: -1s\nsites: [{name: s, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]",
			errMsg: "upstream-read-timeout must be positive",
		},
		{
			name:   "negative health check interval and timeout",
			config: "listen-http: ':80'\nsites: [{name: s, health-check: {interval: -1s, timeout: -2s}, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]",
			errMsg: "health-check.interval must be positive",
		},
		{
			name:   "negative health check timeout",
			config: "listen-http: ':80'\nsites: [{name: s, health-check: {timeout: -2s}, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]",
			errMsg: "health-check.timeout must be positive",
		},
		{
			name:   "negative health check fails",
			config: "listen-http: ':80'\nsites: [{name: s, health-check: {fails: -1}, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]",
			errMsg: "health-check.fails must be positive",
		},
		{
			name:   "negative health check passes",
			config: "listen-http: ':80'\nsites: [{name: s, health-check: {passes: -1}, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]",
			errMsg: "health-check.passes must be positive",
		},
		{
			name:   "negative client keepalive idle",
			config: "listen-http: ':80'\nclient-keepalive: {idle: -1s}\nsites: [{name: s, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]",
			errMsg: "client-keepalive.idle must be positive",
		},
		{
			name:   "negative client keepalive interval",
			config: "listen-http: ':80'\nclient-keepalive: {interval: -1s}\nsites: [{name: s, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]",
			errMsg: "client-keepalive.interval must be positive",
		},
		{
			name:   "negative client keepalive count",
			config: "listen-http: ':80'\nclient-keepalive: {count: -1}\nsites: [{name: s, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]",
			errMsg: "client-keepalive.count must be positive",
		},
		{
			name:   "negative upstream keepalive idle",
			config: "listen-http: ':80'\nupstream-keepalive: {idle: -1s}\nsites: [{name: s, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]",
			errMsg: "upstream-keepalive.idle must be positive",
		},
		{
			name:   "negative upstream keepalive interval",
			config: "listen-http: ':80'\nupstream-keepalive: {interval: -1s}\nsites: [{name: s, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]",
			errMsg: "upstream-keepalive.interval must be positive",
		},
		{
			name:   "negative upstream keepalive count",
			config: "listen-http: ':80'\nupstream-keepalive: {count: -1}\nsites: [{name: s, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]",
			errMsg: "upstream-keepalive.count must be positive",
		},
		{
			name:   "unknown field",
			config: "listen-http: ':80'\ndrain-duraton: 1m\nsites: [{name: s, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]",
			errMsg: "field drain-duraton not found",
		},
		{
			name:   "unknown nested field",
			config: "listen-http: ':80'\nsites: [{name: s, health-check: {intervall: 1s}, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]",
			errMsg: "field intervall not found",
		},
		{
			name:   "unknown upstream field",
			config: "listen-http: ':80'\nsites: [{name: s, upstreams: [{name: a, address: ['1.2.3.4:80']}]}]",
			errMsg: "field address not found",
		},
		{
			name:   "bad duration",
			config: "listen-http: ':80'\ndrain-duration: soon\nsites: [{name: s, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]",
			errMsg: "invalid config",
		},
		{
			name:   "empty",
			config: "",
			errMsg: "listen-http or listen-https must be set",
		},
		{
			name:   "comments only",
			config: "# nothing here\n",
			errMsg: "listen-http or listen-https must be set",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse([]byte(tt.config))
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.errMsg)
		})
	}
}

func TestParse_StreamsDefaults(t *testing.T) {
	c, err := Parse([]byte("listen-http: ':80'\nsites: [{name: s, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]"))
	require.NoError(t, err)
	require.Equal(t, DefaultStreamsMatch, c.Sites[0].Streams.Match)
	for _, target := range []string{"/mytopic/json", "/a,b_c-d/sse", "/t/raw", "/t/ws", "/t/json?since=10m"} {
		require.True(t, isStream(c, "GET", target), target)
	}
	for _, target := range []string{"/a/b/json", "/mytopic", "/t/json/x", "/t/xml", "//json", "/a,/json", "/v1/health", "/t/json?poll=1", "/t/json?po=1"} {
		require.False(t, isStream(c, "GET", target), target)
	}
	require.False(t, isStream(c, "POST", "/mytopic/json"))
}

func TestParse_StreamsCustom(t *testing.T) {
	c, err := Parse([]byte(`
listen-http: ":80"
sites:
  - name: s
    streams:
      match: method == "GET" && path matches "^/events(/.*)?$" && !flag("once")
    upstreams: [{name: a, addresses: ['1.2.3.4:80']}]
`))
	require.NoError(t, err)
	require.True(t, isStream(c, "GET", "/events/foo"))
	require.False(t, isStream(c, "GET", "/events/foo?once=1"))
	require.False(t, isStream(c, "GET", "/mytopic/json"))
}

func TestParse_StreamsOff(t *testing.T) {
	c, err := Parse([]byte("listen-http: ':80'\nsites: [{name: s, streams: {match: 'false'}, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]"))
	require.NoError(t, err)
	require.False(t, isStream(c, "GET", "/mytopic/json"))
}

func isStream(c *Config, method, target string) bool {
	return c.Sites[0].Streams.Program.Match(match.NewEnv(httptest.NewRequest(method, target, nil)))
}

func TestParse_ClientKeyDefaultsAndOverrides(t *testing.T) {
	c, err := Parse([]byte("listen-http: ':80'\nsites: [{name: s, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]"))
	require.NoError(t, err)
	require.Equal(t, DefaultClientKeyIPv4PrefixBits, c.ClientKey.IPv4PrefixBits)
	require.Equal(t, DefaultClientKeyIPv6PrefixBits, c.ClientKey.IPv6PrefixBits)
	c, err = Parse([]byte("listen-http: ':80'\nclient-key: {ipv4-prefix-bits: 24, ipv6-prefix-bits: 128}\nsites: [{name: s, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]"))
	require.NoError(t, err)
	require.Equal(t, 24, c.ClientKey.IPv4PrefixBits)
	require.Equal(t, 128, c.ClientKey.IPv6PrefixBits)
}

func TestParse_PortRanges(t *testing.T) {
	c, err := Parse([]byte("listen-http: ':80'\nsites: [{name: s, upstreams: [{name: a, addresses: ['10.0.0.1:2586-2588', '[fd00::1]:9000-9001', 'node:80']}]}]"))
	require.NoError(t, err)
	require.Equal(t, []string{"10.0.0.1:2586", "10.0.0.1:2587", "10.0.0.1:2588", "[fd00::1]:9000", "[fd00::1]:9001", "node:80"}, c.Sites[0].Upstreams[0].Addresses)
	for _, bad := range []string{"10.0.0.1:2588-2586", "10.0.0.1:1-x", "10.0.0.1:0-5", "10.0.0.1:65535-65536", "10.0.0.1:1000-3000"} {
		_, err := Parse([]byte("listen-http: ':80'\nsites: [{name: s, upstreams: [{name: a, addresses: ['" + bad + "']}]}]"))
		require.ErrorContains(t, err, "invalid address", bad)
	}
}

func TestParse_DuplicateAddress(t *testing.T) {
	_, err := Parse([]byte("listen-http: ':80'\nsites: [{name: s, upstreams: [{name: a, addresses: ['10.0.0.1:2586-2587', '10.0.0.1:2587']}]}]"))
	require.ErrorContains(t, err, "duplicate address")
}

func TestParse_AccessLogs(t *testing.T) {
	c, err := Parse([]byte(`
listen-http: ":80"
access-logs:
  - {file: /var/log/nginx/access.log, match: status < 400}
  - {file: /var/log/nginx/access_error.log}
sites: [{name: s, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]
`))
	require.NoError(t, err)
	require.Len(t, c.AccessLogs, 2)
	env := &match.ResponseEnv{Status: 200}
	require.True(t, c.AccessLogs[0].Program.Match(env))
	require.Nil(t, c.AccessLogs[1].Program) // Everything that gets here
	for name, logs := range map[string]string{
		"no file":           "access-logs: [{match: status < 400}]",
		"request-only name": "access-logs: [{file: /x, match: 'header(\"X\") == \"\"'}]",
		"not boolean":       "access-logs: [{file: /x, match: status}]",
	} {
		_, err := Parse([]byte("listen-http: ':80'\n" + logs + "\nsites: [{name: s, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]"))
		require.ErrorContains(t, err, "access-logs", name)
	}
}

func TestParse_Bans(t *testing.T) {
	c, err := Parse([]byte("listen-http: ':80'\nbans: {file: /var/log/ntlb/ban.log, threshold: 100}\nsites: [{name: s, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]"))
	require.NoError(t, err)
	require.Equal(t, DefaultBansWindow, c.Bans.Window)
	require.Equal(t, 100.0, c.Bans.Threshold)
	for name, bans := range map[string]string{
		"no file":            "bans: {threshold: 100}",
		"no threshold":       "bans: {file: /x}",
		"negative threshold": "bans: {file: /x, threshold: -1}",
		"negative window":    "bans: {file: /x, threshold: 1, window: -1m}",
	} {
		_, err := Parse([]byte("listen-http: ':80'\n" + bans + "\nsites: [{name: s, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]"))
		require.ErrorContains(t, err, "bans", name)
	}
}

func TestRestartRequired(t *testing.T) {
	base := "sites: [{name: s, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]\n"
	a, err := Parse([]byte("listen-http: ':80'\n" + base))
	require.NoError(t, err)
	b, err := Parse([]byte("listen-http: ':80'\ndrain-duration: 1m\n" + base))
	require.NoError(t, err)
	require.Empty(t, a.RestartRequired(b))
	c, err := Parse([]byte("listen-http: ':81'\nlisten-admin: ':1'\nclient-keepalive: {idle: 2m}\nlisten-https: ':443'\n" + strings.Replace(base, "name: s,", "name: s, cert-file: c, key-file: k,", 1)))
	require.NoError(t, err)
	require.Equal(t, []string{"listen-http", "listen-https", "listen-admin", "client-keepalive"}, a.RestartRequired(c))
}

// Loading files

func TestLoad(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "config.yml")
	require.NoError(t, os.WriteFile(filename, []byte("listen-http: ':80'\nsites: [{name: s, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]"), 0600))
	c, err := Load(filename)
	require.NoError(t, err)
	require.Equal(t, ":80", c.ListenHTTP)
	require.Equal(t, "a", c.Sites[0].Upstreams[0].Name)
}

func TestLoad_Missing(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "missing.yml"))
	require.True(t, errors.Is(err, fs.ErrNotExist))
}

func TestLoad_Unreadable(t *testing.T) {
	// A directory cannot be read as a file, also not by root
	_, err := Load(t.TempDir())
	require.Error(t, err)
}

func TestLoad_Invalid(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "config.yml")
	require.NoError(t, os.WriteFile(filename, []byte("listen-http: ':80'"), 0600))
	_, err := Load(filename)
	require.ErrorContains(t, err, "invalid config: at least one site")
}

func TestLoad_PackagingExample(t *testing.T) {
	// The shipped example must always parse, also with unknown fields rejected
	c, err := Load("../packaging/config.yml")
	require.NoError(t, err)
	require.Equal(t, ":80", c.ListenHTTP)
	require.Equal(t, ":443", c.ListenHTTPS)
	require.Equal(t, DefaultListenAdmin, c.ListenAdmin)
	require.Equal(t, DefaultShutdownDrainDuration, c.ShutdownDrainDuration)
	require.Len(t, c.Sites, 1)
	require.Len(t, c.Sites[0].Upstreams, 3)
}

// Defaults for sections that are missing or explicitly null

func TestParse_NullSections(t *testing.T) {
	c, err := Parse([]byte(`
listen-http: ":80"
client-keepalive: ~
upstream-keepalive: null
client-key: ~
sites: [{name: s, health-check: null, streams: null, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]
`))
	require.NoError(t, err)
	require.Equal(t, DefaultKeepaliveIdle, c.ClientKeepalive.Idle)
	require.Equal(t, DefaultKeepaliveCount, c.UpstreamKeepalive.Count)
	require.Equal(t, DefaultHealthPath, c.Sites[0].HealthCheck.Path)
	require.Equal(t, DefaultStreamsMatch, c.Sites[0].Streams.Match)
	require.NotNil(t, c.Sites[0].Streams.Program)
	require.Equal(t, DefaultClientKeyIPv6PrefixBits, c.ClientKey.IPv6PrefixBits)
}

func TestParse_AllDefaults(t *testing.T) {
	c, err := Parse([]byte("listen-http: ':80'\nsites: [{name: s, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]"))
	require.NoError(t, err)
	require.Equal(t, DefaultUpstreamConnectTimeout, c.UpstreamConnectTimeout)
	require.Equal(t, DefaultUpstreamReadTimeout, c.UpstreamReadTimeout)
	require.Equal(t, DefaultDrainDuration, c.DrainDuration)
	require.Equal(t, DefaultShutdownDrainDuration, c.ShutdownDrainDuration)
	require.Equal(t, &HealthCheck{
		Path:     DefaultHealthPath,
		Interval: DefaultHealthInterval,
		Timeout:  DefaultHealthTimeout,
		Fails:    DefaultHealthFails,
		Passes:   DefaultHealthPasses,
	}, c.Sites[0].HealthCheck)
	defaultKeepalive := &Keepalive{
		Idle:     DefaultKeepaliveIdle,
		Interval: DefaultKeepaliveInterval,
		Count:    DefaultKeepaliveCount,
	}
	require.Equal(t, defaultKeepalive, c.ClientKeepalive)
	require.Equal(t, defaultKeepalive, c.UpstreamKeepalive)
	require.Empty(t, c.AccessLogs)
	require.Empty(t, c.ListenMetrics)
	require.Nil(t, c.Bans)
	require.Empty(t, c.ListenHTTPS)
}

// Keepalives

func TestKeepAliveConfig(t *testing.T) {
	c, err := Parse([]byte("listen-http: ':80'\nupstream-keepalive: {interval: 20s}\nsites: [{name: s, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]"))
	require.NoError(t, err)
	require.Equal(t, net.KeepAliveConfig{
		Enable:   true,
		Idle:     DefaultKeepaliveIdle,
		Interval: DefaultKeepaliveInterval,
		Count:    DefaultKeepaliveCount,
	}, c.ClientKeepalive.KeepAliveConfig())
	require.Equal(t, net.KeepAliveConfig{
		Enable:   true,
		Idle:     DefaultKeepaliveIdle,
		Interval: 20 * time.Second,
		Count:    DefaultKeepaliveCount,
	}, c.UpstreamKeepalive.KeepAliveConfig())
}

func TestKeepAliveConfig_Disable(t *testing.T) {
	c, err := Parse([]byte("listen-http: ':80'\nclient-keepalive: {disable: true, idle: 2m}\nsites: [{name: s, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]"))
	require.NoError(t, err)
	require.Equal(t, net.KeepAliveConfig{Enable: false}, c.ClientKeepalive.KeepAliveConfig())
	require.True(t, c.UpstreamKeepalive.KeepAliveConfig().Enable)
}

// Settings a reload cannot apply

func TestRestartRequired_Each(t *testing.T) {
	base := "sites: [{name: s, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]\n"
	a, err := Parse([]byte("listen-http: ':80'\n" + base))
	require.NoError(t, err)
	tests := []struct {
		name     string
		config   string
		settings []string
	}{
		{
			name:     "listen-http",
			config:   "listen-http: ':81'\n",
			settings: []string{"listen-http"},
		},
		{
			name:     "listen-metrics",
			config:   "listen-http: ':80'\nlisten-metrics: '10.0.0.1:9090'\n",
			settings: []string{"listen-metrics"},
		},
		{
			name:     "listen-admin",
			config:   "listen-http: ':80'\nlisten-admin: '127.0.0.1:1'\n",
			settings: []string{"listen-admin"},
		},
		{
			name:     "client-keepalive disable",
			config:   "listen-http: ':80'\nclient-keepalive: {disable: true}\n",
			settings: []string{"client-keepalive"},
		},
		{
			name:     "client-keepalive count",
			config:   "listen-http: ':80'\nclient-keepalive: {count: 9}\n",
			settings: []string{"client-keepalive"},
		},
		{
			name:   "reloadable settings only",
			config: "listen-http: ':80'\nupstream-keepalive: {disable: true}\nclient-key: {ipv4-prefix-bits: 24}\nupstream-read-timeout: 1m\naccess-logs: [{file: /tmp/x}]\n",
		},
		{
			name:   "same keepalive written out",
			config: "listen-http: ':80'\nclient-keepalive: {idle: 1m, interval: 15s, count: 4}\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := Parse([]byte(tt.config + base))
			require.NoError(t, err)
			require.Equal(t, tt.settings, a.RestartRequired(b))
			require.Equal(t, tt.settings, b.RestartRequired(a))
		})
	}
}
