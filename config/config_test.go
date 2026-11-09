package config

import (
	"errors"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParse_Defaults(t *testing.T) {
	c, err := Parse([]byte(`
listen-http: ":80"
upstreams:
  - name: app1
    addresses: ["10.0.0.1:2586", "10.0.0.1:2587"]
`))
	require.NoError(t, err)
	require.Equal(t, DefaultListenAdmin, c.ListenAdmin)
	require.Equal(t, DefaultHealthPath, c.HealthCheck.Path)
	require.Equal(t, DefaultHealthFails, c.HealthCheck.Fails)
	require.Equal(t, DefaultUpstreamReadTimeout, c.UpstreamReadTimeout)
	require.Equal(t, DefaultKeepaliveIdle, c.ClientKeepalive.Idle)
	require.Equal(t, DefaultKeepaliveCount, c.UpstreamKeepalive.Count)
	require.Len(t, c.Upstreams, 1)
	require.Equal(t, []string{"10.0.0.1:2586", "10.0.0.1:2587"}, c.Upstreams[0].Addresses)
}

func TestParse_Overrides(t *testing.T) {
	c, err := Parse([]byte(`
listen-https: ":443"
cert-file: /tmp/cert.pem
key-file: /tmp/key.pem
drain-duration: 10m
client-keepalive:
  idle: 3m
  interval: 30s
  count: 5
upstream-keepalive:
  disable: true
health-check:
  path: /v1/health
  interval: 2s
  timeout: 1s
upstreams:
  - name: app1
    addresses: ["10.0.0.1:2586"]
`))
	require.NoError(t, err)
	require.Equal(t, 10*time.Minute, c.DrainDuration)
	require.Equal(t, 3*time.Minute, c.ClientKeepalive.KeepAliveConfig().Idle)
	require.Equal(t, 5, c.ClientKeepalive.KeepAliveConfig().Count)
	require.False(t, c.UpstreamKeepalive.KeepAliveConfig().Enable)
	require.Equal(t, "/v1/health", c.HealthCheck.Path)
	require.Equal(t, 2*time.Second, c.HealthCheck.Interval)
}

func TestParse_Invalid(t *testing.T) {
	tests := []struct {
		name   string
		config string
		errMsg string
	}{
		{
			name:   "no listener",
			config: "upstreams: [{name: a, addresses: ['1.2.3.4:80']}]",
			errMsg: "listen-http or listen-https",
		},
		{
			name:   "https without cert",
			config: "listen-https: ':443'\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]",
			errMsg: "cert-file",
		},
		{
			name:   "no upstreams",
			config: "listen-http: ':80'",
			errMsg: "at least one upstream",
		},
		{
			name:   "duplicate name",
			config: "listen-http: ':80'\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}, {name: a, addresses: ['1.2.3.5:80']}]",
			errMsg: "duplicate upstream name",
		},
		{
			name:   "bad name",
			config: "listen-http: ':80'\nupstreams: [{name: 'a b', addresses: ['1.2.3.4:80']}]",
			errMsg: "invalid upstream name",
		},
		{
			name:   "no addresses",
			config: "listen-http: ':80'\nupstreams: [{name: a}]",
			errMsg: "no addresses",
		},
		{
			name:   "bad address",
			config: "listen-http: ':80'\nupstreams: [{name: a, addresses: ['1.2.3.4']}]",
			errMsg: "invalid address",
		},
		{
			name:   "timeout not below interval",
			config: "listen-http: ':80'\nhealth-check: {interval: 1s, timeout: 1s}\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]",
			errMsg: "health-check.timeout",
		},
		{
			name:   "bad streams regex",
			config: "listen-http: ':80'\nstreams: {path: '(['}\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]",
			errMsg: "streams.path",
		},
		{
			name:   "ipv4 prefix too long",
			config: "listen-http: ':80'\nhash: {ipv4-prefix-bits: 33}\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]",
			errMsg: "hash.ipv4-prefix-bits",
		},
		{
			name:   "ipv6 prefix negative",
			config: "listen-http: ':80'\nhash: {ipv6-prefix-bits: -1}\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]",
			errMsg: "hash.ipv6-prefix-bits",
		},
		{
			name:   "negative drain",
			config: "listen-http: ':80'\ndrain-duration: -1s\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]",
			errMsg: "negative",
		},
		{
			name:   "bad yaml",
			config: "listen-http: [",
			errMsg: "invalid config",
		},
		{
			name:   "negative shutdown drain",
			config: "listen-http: ':80'\nshutdown-drain-duration: -1s\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]",
			errMsg: "drain durations must not be negative",
		},
		{
			name:   "https without key",
			config: "listen-https: ':443'\ncert-file: c\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]",
			errMsg: "listen-https requires cert-file and key-file",
		},
		{
			name:   "ipv4 prefix negative",
			config: "listen-http: ':80'\nhash: {ipv4-prefix-bits: -1}\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]",
			errMsg: "hash.ipv4-prefix-bits",
		},
		{
			name:   "ipv6 prefix too long",
			config: "listen-http: ':80'\nhash: {ipv6-prefix-bits: 129}\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]",
			errMsg: "hash.ipv6-prefix-bits",
		},
		{
			name:   "name too long",
			config: "listen-http: ':80'\nupstreams: [{name: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa, addresses: ['1.2.3.4:80']}]",
			errMsg: "invalid upstream name",
		},
		{
			name:   "empty address",
			config: "listen-http: ':80'\nupstreams: [{name: a, addresses: ['']}]",
			errMsg: "invalid address",
		},
		{
			name:   "empty upstreams list",
			config: "listen-http: ':80'\nupstreams: []",
			errMsg: "at least one upstream",
		},
		{
			name:   "null upstream",
			config: "listen-http: ':80'\nupstreams: [null]",
			errMsg: "upstreams[0] is empty",
		},
		{
			name:   "empty upstream list item",
			config: "listen-http: ':80'\nupstreams:\n  - name: a\n    addresses: ['1.2.3.4:80']\n  -\n",
			errMsg: "upstreams[1] is empty",
		},
		{
			name:   "negative connect timeout",
			config: "listen-http: ':80'\nupstream-connect-timeout: -1s\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]",
			errMsg: "upstream-connect-timeout must be positive",
		},
		{
			name:   "negative read timeout",
			config: "listen-http: ':80'\nupstream-read-timeout: -1s\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]",
			errMsg: "upstream-read-timeout must be positive",
		},
		{
			name:   "negative health check interval and timeout",
			config: "listen-http: ':80'\nhealth-check: {interval: -1s, timeout: -2s}\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]",
			errMsg: "health-check.interval must be positive",
		},
		{
			name:   "negative health check timeout",
			config: "listen-http: ':80'\nhealth-check: {timeout: -2s}\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]",
			errMsg: "health-check.timeout must be positive",
		},
		{
			name:   "negative health check fails",
			config: "listen-http: ':80'\nhealth-check: {fails: -1}\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]",
			errMsg: "health-check.fails must be positive",
		},
		{
			name:   "negative health check passes",
			config: "listen-http: ':80'\nhealth-check: {passes: -1}\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]",
			errMsg: "health-check.passes must be positive",
		},
		{
			name:   "negative client keepalive idle",
			config: "listen-http: ':80'\nclient-keepalive: {idle: -1s}\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]",
			errMsg: "client-keepalive.idle must be positive",
		},
		{
			name:   "negative client keepalive interval",
			config: "listen-http: ':80'\nclient-keepalive: {interval: -1s}\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]",
			errMsg: "client-keepalive.interval must be positive",
		},
		{
			name:   "negative client keepalive count",
			config: "listen-http: ':80'\nclient-keepalive: {count: -1}\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]",
			errMsg: "client-keepalive.count must be positive",
		},
		{
			name:   "negative upstream keepalive idle",
			config: "listen-http: ':80'\nupstream-keepalive: {idle: -1s}\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]",
			errMsg: "upstream-keepalive.idle must be positive",
		},
		{
			name:   "negative upstream keepalive interval",
			config: "listen-http: ':80'\nupstream-keepalive: {interval: -1s}\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]",
			errMsg: "upstream-keepalive.interval must be positive",
		},
		{
			name:   "negative upstream keepalive count",
			config: "listen-http: ':80'\nupstream-keepalive: {count: -1}\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]",
			errMsg: "upstream-keepalive.count must be positive",
		},
		{
			name:   "unknown field",
			config: "listen-http: ':80'\ndrain-duraton: 1m\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]",
			errMsg: "field drain-duraton not found",
		},
		{
			name:   "unknown nested field",
			config: "listen-http: ':80'\nhealth-check: {intervall: 1s}\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]",
			errMsg: "field intervall not found",
		},
		{
			name:   "unknown upstream field",
			config: "listen-http: ':80'\nupstreams: [{name: a, address: ['1.2.3.4:80']}]",
			errMsg: "field address not found",
		},
		{
			name:   "bad duration",
			config: "listen-http: ':80'\ndrain-duration: soon\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]",
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
	c, err := Parse([]byte("listen-http: ':80'\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]"))
	require.NoError(t, err)
	require.False(t, c.Streams.Disable)
	require.Equal(t, DefaultStreamsPath, c.Streams.Path)
	require.Equal(t, DefaultStreamsExcludeParams, c.Streams.ExcludeParams)
	require.Equal(t, []string{"x-poll", "poll", "po"}, c.Streams.ExcludeParams) // Precedence order, like ntfy's readParam
	require.True(t, c.Streams.PathRegex.MatchString("/mytopic/json"))
	require.True(t, c.Streams.PathRegex.MatchString("/a,b_c-d/sse"))
	require.False(t, c.Streams.PathRegex.MatchString("/a/b/json"))
	require.False(t, c.Streams.PathRegex.MatchString("/mytopic"))
}

func TestParse_StreamsCustom(t *testing.T) {
	c, err := Parse([]byte(`
listen-http: ":80"
streams:
  path: "^/events(/.*)?$"
  exclude-params: [once]
upstreams: [{name: a, addresses: ['1.2.3.4:80']}]
`))
	require.NoError(t, err)
	require.True(t, c.Streams.PathRegex.MatchString("/events/foo"))
	require.False(t, c.Streams.PathRegex.MatchString("/mytopic/json"))
	require.Equal(t, []string{"once"}, c.Streams.ExcludeParams)
}

func TestParse_StreamsExcludeParamsEmpty(t *testing.T) {
	// An explicitly empty list means "no exclusions", not the default
	c, err := Parse([]byte("listen-http: ':80'\nstreams: {exclude-params: []}\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]"))
	require.NoError(t, err)
	require.Empty(t, c.Streams.ExcludeParams)
}

func TestParse_HashDefaultsAndOverrides(t *testing.T) {
	c, err := Parse([]byte("listen-http: ':80'\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]"))
	require.NoError(t, err)
	require.Equal(t, DefaultHashIPv4PrefixBits, c.Hash.IPv4PrefixBits)
	require.Equal(t, DefaultHashIPv6PrefixBits, c.Hash.IPv6PrefixBits)
	c, err = Parse([]byte("listen-http: ':80'\nhash: {ipv4-prefix-bits: 24, ipv6-prefix-bits: 128}\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]"))
	require.NoError(t, err)
	require.Equal(t, 24, c.Hash.IPv4PrefixBits)
	require.Equal(t, 128, c.Hash.IPv6PrefixBits)
}

func TestRestartRequired(t *testing.T) {
	base := "upstreams: [{name: a, addresses: ['1.2.3.4:80']}]\n"
	a, err := Parse([]byte("listen-http: ':80'\n" + base))
	require.NoError(t, err)
	b, err := Parse([]byte("listen-http: ':80'\ndrain-duration: 1m\n" + base))
	require.NoError(t, err)
	require.Empty(t, a.RestartRequired(b))
	c, err := Parse([]byte("listen-http: ':81'\nlisten-admin: ':1'\nclient-keepalive: {idle: 2m}\nlisten-https: ':443'\ncert-file: c\nkey-file: k\n" + base))
	require.NoError(t, err)
	require.Equal(t, []string{"listen-http", "listen-https", "listen-admin", "client-keepalive"}, a.RestartRequired(c))
}

// Loading files

func TestLoad(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "config.yml")
	require.NoError(t, os.WriteFile(filename, []byte("listen-http: ':80'\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]"), 0600))
	c, err := Load(filename)
	require.NoError(t, err)
	require.Equal(t, ":80", c.ListenHTTP)
	require.Equal(t, "a", c.Upstreams[0].Name)
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
	require.ErrorContains(t, err, "invalid config: at least one upstream")
}

func TestLoad_PackagingExample(t *testing.T) {
	// The shipped example must always parse, also with unknown fields rejected
	c, err := Load("../packaging/config.yml")
	require.NoError(t, err)
	require.Equal(t, ":80", c.ListenHTTP)
	require.Equal(t, ":443", c.ListenHTTPS)
	require.Equal(t, DefaultListenAdmin, c.ListenAdmin)
	require.Equal(t, DefaultShutdownDrainDuration, c.ShutdownDrainDuration)
	require.Len(t, c.Upstreams, 3)
}

// Defaults for sections that are missing or explicitly null

func TestParse_NullSections(t *testing.T) {
	c, err := Parse([]byte(`
listen-http: ":80"
client-keepalive: ~
upstream-keepalive: null
health-check: null
streams: null
hash: ~
upstreams: [{name: a, addresses: ['1.2.3.4:80']}]
`))
	require.NoError(t, err)
	require.Equal(t, DefaultKeepaliveIdle, c.ClientKeepalive.Idle)
	require.Equal(t, DefaultKeepaliveCount, c.UpstreamKeepalive.Count)
	require.Equal(t, DefaultHealthPath, c.HealthCheck.Path)
	require.Equal(t, DefaultStreamsPath, c.Streams.Path)
	require.NotNil(t, c.Streams.PathRegex)
	require.Equal(t, DefaultHashIPv6PrefixBits, c.Hash.IPv6PrefixBits)
}

func TestParse_AllDefaults(t *testing.T) {
	c, err := Parse([]byte("listen-http: ':80'\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]"))
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
	}, c.HealthCheck)
	defaultKeepalive := &Keepalive{
		Idle:     DefaultKeepaliveIdle,
		Interval: DefaultKeepaliveInterval,
		Count:    DefaultKeepaliveCount,
	}
	require.Equal(t, defaultKeepalive, c.ClientKeepalive)
	require.Equal(t, defaultKeepalive, c.UpstreamKeepalive)
	require.Empty(t, c.AccessLog)
	require.Empty(t, c.ListenHTTPS)
}

// Keepalives

func TestKeepAliveConfig(t *testing.T) {
	c, err := Parse([]byte("listen-http: ':80'\nupstream-keepalive: {interval: 20s}\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]"))
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
	c, err := Parse([]byte("listen-http: ':80'\nclient-keepalive: {disable: true, idle: 2m}\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]"))
	require.NoError(t, err)
	require.Equal(t, net.KeepAliveConfig{Enable: false}, c.ClientKeepalive.KeepAliveConfig())
	require.True(t, c.UpstreamKeepalive.KeepAliveConfig().Enable)
}

// Streams and hashing

func TestParse_StreamsDisable(t *testing.T) {
	c, err := Parse([]byte("listen-http: ':80'\nstreams: {disable: true}\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]"))
	require.NoError(t, err)
	require.True(t, c.Streams.Disable)
	require.NotNil(t, c.Streams.PathRegex) // Still compiled, so that a reload can turn it back on
}

func TestParse_StreamsDefaultPathMatches(t *testing.T) {
	c, err := Parse([]byte("listen-http: ':80'\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]"))
	require.NoError(t, err)
	for _, path := range []string{"/t/json", "/t/sse", "/t/raw", "/t/ws", "/a,b/json"} {
		require.True(t, c.Streams.PathRegex.MatchString(path), path)
	}
	for _, path := range []string{"/t/json/x", "/t/xml", "/t/", "//json", "/a,/json", "/v1/health"} {
		require.False(t, c.Streams.PathRegex.MatchString(path), path)
	}
}

// Settings a reload cannot apply

func TestRestartRequired_Each(t *testing.T) {
	base := "upstreams: [{name: a, addresses: ['1.2.3.4:80']}]\n"
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
			config: "listen-http: ':80'\nupstream-keepalive: {disable: true}\nhealth-check: {interval: 9s}\nhash: {ipv4-prefix-bits: 24}\nupstream-read-timeout: 1m\naccess-log: /tmp/x\n",
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
