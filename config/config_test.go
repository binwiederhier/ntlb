package config

import (
	"errors"
	"io/fs"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"heckel.io/ntlb/match"
)

func TestParse_Defaults(t *testing.T) {
	c, err := Parse([]byte(`
sites:
  - name: s
    listen-http: ":80"
    upstreams:
      default:
        servers:
        - name: app1
          addresses: ["10.0.0.1:2586", "10.0.0.1:2587"]
`))
	require.NoError(t, err)
	require.Equal(t, DefaultListenAdmin, c.ListenAdmin)
	up := c.Sites[0].DefaultUpstream()
	require.Equal(t, DefaultHealthPath, up.HealthCheck.Path)
	require.Equal(t, DefaultHealthFails, up.HealthCheck.Fails)
	require.Equal(t, DefaultUpstreamReadTimeout, up.Timeouts.Read)
	require.Equal(t, DefaultKeepaliveIdle, c.ClientKeepalive.Idle)
	require.Equal(t, DefaultKeepaliveCount, up.Keepalive.Count)
	require.Len(t, up.Servers, 1)
	require.Equal(t, []string{"10.0.0.1:2586", "10.0.0.1:2587"}, up.Servers[0].Addresses)
}

func TestParse_Overrides(t *testing.T) {
	c, err := Parse([]byte(`
drain-duration: 10m
client-keepalive:
  idle: 3m
  interval: 30s
  count: 5
sites:
  - name: s
    listen-https: ":443"
    cert-file: /tmp/cert.pem
    key-file: /tmp/key.pem
    upstreams:
      default:
        keepalive: {disable: true}
        healthcheck: {path: /v1/health, interval: 2s, timeout: 1s}
        servers:
        - name: app1
          addresses: ["10.0.0.1:2586"]
`))
	require.NoError(t, err)
	require.Equal(t, 10*time.Minute, c.DrainDuration)
	require.Equal(t, 3*time.Minute, c.ClientKeepalive.KeepAliveConfig().Idle)
	require.Equal(t, 5, c.ClientKeepalive.KeepAliveConfig().Count)
	require.False(t, c.Sites[0].DefaultUpstream().Keepalive.KeepAliveConfig().Enable)
	require.Equal(t, "/v1/health", c.Sites[0].DefaultUpstream().HealthCheck.Path)
	require.Equal(t, 2*time.Second, c.Sites[0].DefaultUpstream().HealthCheck.Interval)
}

func TestParse_Invalid(t *testing.T) {
	tests := []struct {
		name   string
		config string
		errMsg string
	}{
		{
			name:   "no listener",
			config: "sites: [{name: s, upstreams: {default: {servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]",
			errMsg: "listen-http or listen-https",
		},
		{
			name:   "https without cert",
			config: "sites: [{name: s, listen-https: ':443', upstreams: {default: {servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]",
			errMsg: "cert-file",
		},
		{
			name:   "no upstreams",
			config: "drain-duration: 1m",
			errMsg: "at least one site",
		},
		{
			name:   "duplicate name",
			config: "sites: [{name: s, listen-http: ':80', upstreams: {default: {servers: [{name: a, addresses: ['1.2.3.4:80']}, {name: a, addresses: ['1.2.3.5:80']}]}}}]",
			errMsg: "duplicate server name",
		},
		{
			name:   "bad name",
			config: "sites: [{name: s, listen-http: ':80', upstreams: {default: {servers: [{name: 'a b', addresses: ['1.2.3.4:80']}]}}}]",
			errMsg: "invalid server name",
		},
		{
			name:   "no addresses",
			config: "sites: [{name: s, listen-http: ':80', upstreams: {default: {servers: [{name: a}]}}}]",
			errMsg: "no addresses",
		},
		{
			name:   "bad address",
			config: "sites: [{name: s, listen-http: ':80', upstreams: {default: {servers: [{name: a, addresses: ['1.2.3.4']}]}}}]",
			errMsg: "invalid address",
		},
		{
			name:   "timeout not below interval",
			config: "sites: [{name: s, listen-http: ':80', upstreams: {default: {healthcheck: {interval: 1s, timeout: 1s}, servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]",
			errMsg: "healthcheck.timeout",
		},
		{
			name:   "bad streams expression",
			config: "sites: [{name: s, listen-http: ':80', streams: {match: 'path =='}, upstreams: {default: {servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]",
			errMsg: "streams.match",
		},
		{
			name:   "ipv4 prefix too long",
			config: "client-key: {ipv4-prefix-bits: 33}\nsites: [{name: s, listen-http: ':80', upstreams: {default: {servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]",
			errMsg: "client-key.ipv4-prefix-bits",
		},
		{
			name:   "ipv6 prefix negative",
			config: "client-key: {ipv6-prefix-bits: -1}\nsites: [{name: s, listen-http: ':80', upstreams: {default: {servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]",
			errMsg: "client-key.ipv6-prefix-bits",
		},
		{
			name:   "negative drain",
			config: "drain-duration: -1s\nsites: [{name: s, listen-http: ':80', upstreams: {default: {servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]",
			errMsg: "negative",
		},
		{
			name:   "bad yaml",
			config: "listen-http: [",
			errMsg: "invalid config",
		},
		{
			name:   "negative shutdown drain",
			config: "shutdown-drain-duration: -1s\nsites: [{name: s, listen-http: ':80', upstreams: {default: {servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]",
			errMsg: "drain durations must not be negative",
		},
		{
			name:   "https without key",
			config: "sites: [{name: s, listen-https: ':443', cert-file: c, upstreams: {default: {servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]",
			errMsg: "cert-file and key-file go together",
		},
		{
			name:   "ipv4 prefix negative",
			config: "client-key: {ipv4-prefix-bits: -1}\nsites: [{name: s, listen-http: ':80', upstreams: {default: {servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]",
			errMsg: "client-key.ipv4-prefix-bits",
		},
		{
			name:   "ipv6 prefix too long",
			config: "client-key: {ipv6-prefix-bits: 129}\nsites: [{name: s, listen-http: ':80', upstreams: {default: {servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]",
			errMsg: "client-key.ipv6-prefix-bits",
		},
		{
			name:   "name too long",
			config: "sites: [{name: s, listen-http: ':80', upstreams: {default: {servers: [{name: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa, addresses: ['1.2.3.4:80']}]}}}]",
			errMsg: "invalid server name",
		},
		{
			name:   "empty address",
			config: "sites: [{name: s, listen-http: ':80', upstreams: {default: {servers: [{name: a, addresses: ['']}]}}}]",
			errMsg: "invalid address",
		},
		{
			name:   "empty upstreams list",
			config: "sites: [{name: s, listen-http: ':80', upstreams: {default: {servers: []}}}]",
			errMsg: "at least one server",
		},
		{
			name:   "null upstream",
			config: "sites: [{name: s, listen-http: ':80', upstreams: {default: {servers: [null]}}}]",
			errMsg: "upstreams.default.servers[0] is empty",
		},
		{
			name:   "empty upstream list item",
			config: "sites:\n  - name: s\n    listen-http: ':80'\n    upstreams:\n      default:\n        servers:\n        - name: a\n          addresses: ['1.2.3.4:80']\n        -\n",
			errMsg: "upstreams.default.servers[1] is empty",
		},
		{
			name:   "negative connect timeout",
			config: "sites: [{name: s, listen-http: ':80', upstreams: {default: {timeouts: {connect: -1s}, servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]",
			errMsg: "upstreams.default.timeouts.connect must be positive",
		},
		{
			name:   "negative read timeout",
			config: "sites: [{name: s, listen-http: ':80', upstreams: {default: {timeouts: {read: -1s}, servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]",
			errMsg: "upstreams.default.timeouts.read must be positive",
		},
		{
			name:   "negative health check interval and timeout",
			config: "sites: [{name: s, listen-http: ':80', upstreams: {default: {healthcheck: {interval: -1s, timeout: -2s}, servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]",
			errMsg: "upstreams.default.healthcheck.interval must be positive",
		},
		{
			name:   "negative health check timeout",
			config: "sites: [{name: s, listen-http: ':80', upstreams: {default: {healthcheck: {timeout: -2s}, servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]",
			errMsg: "upstreams.default.healthcheck.timeout must be positive",
		},
		{
			name:   "negative health check fails",
			config: "sites: [{name: s, listen-http: ':80', upstreams: {default: {healthcheck: {fails: -1}, servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]",
			errMsg: "upstreams.default.healthcheck.fails must be positive",
		},
		{
			name:   "negative health check passes",
			config: "sites: [{name: s, listen-http: ':80', upstreams: {default: {healthcheck: {passes: -1}, servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]",
			errMsg: "upstreams.default.healthcheck.passes must be positive",
		},
		{
			name:   "negative client keepalive idle",
			config: "client-keepalive: {idle: -1s}\nsites: [{name: s, listen-http: ':80', upstreams: {default: {servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]",
			errMsg: "client-keepalive.idle must be positive",
		},
		{
			name:   "negative client keepalive interval",
			config: "client-keepalive: {interval: -1s}\nsites: [{name: s, listen-http: ':80', upstreams: {default: {servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]",
			errMsg: "client-keepalive.interval must be positive",
		},
		{
			name:   "negative client keepalive count",
			config: "client-keepalive: {count: -1}\nsites: [{name: s, listen-http: ':80', upstreams: {default: {servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]",
			errMsg: "client-keepalive.count must be positive",
		},
		{
			name:   "negative upstream keepalive idle",
			config: "sites: [{name: s, listen-http: ':80', upstreams: {default: {keepalive: {idle: -1s}, servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]",
			errMsg: "upstreams.default.keepalive.idle must be positive",
		},
		{
			name:   "negative upstream keepalive interval",
			config: "sites: [{name: s, listen-http: ':80', upstreams: {default: {keepalive: {interval: -1s}, servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]",
			errMsg: "upstreams.default.keepalive.interval must be positive",
		},
		{
			name:   "negative upstream keepalive count",
			config: "sites: [{name: s, listen-http: ':80', upstreams: {default: {keepalive: {count: -1}, servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]",
			errMsg: "upstreams.default.keepalive.count must be positive",
		},
		{
			name:   "unknown field",
			config: "drain-duraton: 1m\nsites: [{name: s, listen-http: ':80', upstreams: {default: {servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]",
			errMsg: "field drain-duraton not found",
		},
		{
			name:   "unknown nested field",
			config: "sites: [{name: s, listen-http: ':80', upstreams: {default: {healthcheck: {intervall: 1s}, servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]",
			errMsg: "field intervall not found",
		},
		{
			name:   "unknown upstream field",
			config: "sites: [{name: s, listen-http: ':80', upstreams: {default: {servers: [{name: a, address: ['1.2.3.4:80']}]}}}]",
			errMsg: "field address not found",
		},
		{
			name:   "bad duration",
			config: "drain-duration: soon\nsites: [{name: s, listen-http: ':80', upstreams: {default: {servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]",
			errMsg: "invalid config",
		},
		{
			name:   "empty",
			config: "",
			errMsg: "at least one site",
		},
		{
			name:   "comments only",
			config: "# nothing here\n",
			errMsg: "at least one site",
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

func TestParse_NoStreamsByDefault(t *testing.T) {
	// Which requests are long-lived streams is up to the site; the ntfy site's are in packaging
	c, err := Parse([]byte("sites: [{name: s, listen-http: ':80', upstreams: {default: {servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]"))
	require.NoError(t, err)
	for _, target := range []string{"/mytopic/json", "/t/sse", "/events"} {
		require.False(t, isStream(c, "GET", target), target)
	}
}

func TestParse_StreamsCustom(t *testing.T) {
	c, err := Parse([]byte(`
sites:
  - name: s
    listen-http: ":80"
    streams:
      match: method == "GET" && path matches "^/events(/.*)?$" && !flag("once")
    upstreams: {default: {servers: [{name: a, addresses: ['1.2.3.4:80']}]}}
`))
	require.NoError(t, err)
	require.True(t, isStream(c, "GET", "/events/foo"))
	require.False(t, isStream(c, "GET", "/events/foo?once=1"))
	require.False(t, isStream(c, "GET", "/mytopic/json"))
}

func TestParse_StreamsOff(t *testing.T) {
	c, err := Parse([]byte("sites: [{name: s, listen-http: ':80', streams: {match: 'false'}, upstreams: {default: {servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]"))
	require.NoError(t, err)
	require.False(t, isStream(c, "GET", "/mytopic/json"))
}

func isStream(c *Config, method, target string) bool {
	return c.Sites[0].Streams.Program.Match(match.NewEnv(httptest.NewRequest(method, target, nil)))
}

func TestParse_ClientKeyDefaultsAndOverrides(t *testing.T) {
	c, err := Parse([]byte("sites: [{name: s, listen-http: ':80', upstreams: {default: {servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]"))
	require.NoError(t, err)
	require.Equal(t, DefaultClientKeyIPv4PrefixBits, c.ClientKey.IPv4PrefixBits)
	require.Equal(t, DefaultClientKeyIPv6PrefixBits, c.ClientKey.IPv6PrefixBits)
	c, err = Parse([]byte("client-key: {ipv4-prefix-bits: 24, ipv6-prefix-bits: 128}\nsites: [{name: s, listen-http: ':80', upstreams: {default: {servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]"))
	require.NoError(t, err)
	require.Equal(t, 24, c.ClientKey.IPv4PrefixBits)
	require.Equal(t, 128, c.ClientKey.IPv6PrefixBits)
}

func TestParse_AccessLogs(t *testing.T) {
	c, err := Parse([]byte(`
access-logs:
  - {file: /var/log/nginx/access.log, match: status < 400}
  - {file: /var/log/nginx/access_error.log}
sites: [{name: s, listen-http: ":80", upstreams: {default: {servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]
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
		_, err := Parse([]byte(logs + "\nsites: [{name: s, listen-http: ':80', upstreams: {default: {servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]"))
		require.ErrorContains(t, err, "access-logs", name)
	}
}

func TestParse_Bans(t *testing.T) {
	c, err := Parse([]byte("bans: {file: /var/log/ntlb/ban.log, threshold: 100}\nsites: [{name: s, listen-http: ':80', upstreams: {default: {servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]"))
	require.NoError(t, err)
	require.Equal(t, DefaultBansWindow, c.Bans.Window)
	require.Equal(t, 100.0, c.Bans.Threshold)
	for name, bans := range map[string]string{
		"no file":            "bans: {threshold: 100}",
		"no threshold":       "bans: {file: /x}",
		"negative threshold": "bans: {file: /x, threshold: -1}",
		"negative window":    "bans: {file: /x, threshold: 1, window: -1m}",
	} {
		_, err := Parse([]byte(bans + "\nsites: [{name: s, listen-http: ':80', upstreams: {default: {servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]"))
		require.ErrorContains(t, err, "bans", name)
	}
}

func TestRestartRequired(t *testing.T) {
	site := func(listen string) string {
		return "sites: [{name: s, " + listen + ", cert-file: c, key-file: k, upstreams: {default: {servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]\n"
	}
	a, err := Parse([]byte(site("listen-http: ':80'")))
	require.NoError(t, err)
	b, err := Parse([]byte("drain-duration: 1m\n" + site("listen-http: ':80'")))
	require.NoError(t, err)
	require.Empty(t, a.RestartRequired(b))

	// Listen addresses of the sites are not on the list: a reload binds and closes them
	c, err := Parse([]byte("listen-admin: ':1'\nclient-keepalive: {idle: 2m}\n" + site("listen-http: ':81', listen-https: ':443'")))
	require.NoError(t, err)
	require.Equal(t, []string{"listen-admin", "client-keepalive"}, a.RestartRequired(c))
}

// Loading files

func TestLoad(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "config.yml")
	require.NoError(t, os.WriteFile(filename, []byte("sites: [{name: s, listen-http: ':80', upstreams: {default: {servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]"), 0600))
	c, err := Load(filename)
	require.NoError(t, err)
	require.Equal(t, ListenAddrs{":80"}, c.Sites[0].ListenHTTP)
	require.Equal(t, "a", c.Sites[0].DefaultUpstream().Servers[0].Name)
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
	require.NoError(t, os.WriteFile(filename, []byte("drain-duration: 1m"), 0600))
	_, err := Load(filename)
	require.ErrorContains(t, err, "invalid config: at least one site")
}

func TestLoad_PackagingExample(t *testing.T) {
	// The shipped example must always parse, also with unknown fields rejected
	c, err := Load("../packaging/config.yml")
	require.NoError(t, err)
	require.Equal(t, ListenAddrs{":80"}, c.Sites[0].ListenHTTP)
	require.Equal(t, ListenAddrs{":443"}, c.Sites[0].ListenHTTPS)
	require.Equal(t, DefaultListenAdmin, c.ListenAdmin)
	require.Equal(t, DefaultShutdownDrainDuration, c.ShutdownDrainDuration)
	require.Len(t, c.Sites, 1)
	require.Len(t, c.Sites[0].DefaultUpstream().Servers, 3)

	// The ntfy site: hashed, ntfy's readiness endpoint, its subscriptions as streams
	ntfy := c.Sites[0]
	require.Equal(t, BalanceHash, ntfy.DefaultUpstream().Balance)
	require.Equal(t, "/v1/ready", ntfy.DefaultUpstream().HealthCheck.Path)
	require.Equal(t, 3*time.Minute, ntfy.DefaultUpstream().Timeouts.Read)
	for _, target := range []string{"/mytopic/json", "/a,b_c-d/sse", "/t/raw", "/t/ws", "/t/json?since=10m"} {
		require.True(t, isStream(c, "GET", target), target)
	}
	for _, target := range []string{"/a/b/json", "/mytopic", "/t/json/x", "/t/xml", "//json", "/a,/json", "/v1/health", "/t/json?poll=1", "/t/json?po=1"} {
		require.False(t, isStream(c, "GET", target), target)
	}
	require.False(t, isStream(c, "POST", "/mytopic/json"))
}

// Defaults for sections that are missing or explicitly null

func TestParse_NullSections(t *testing.T) {
	c, err := Parse([]byte(`
client-keepalive: ~
client-key: ~
sites: [{name: s, listen-http: ":80", streams: null, upstreams: {default: {timeouts: null, keepalive: null, healthcheck: null, servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]
`))
	require.NoError(t, err)
	require.Equal(t, DefaultKeepaliveIdle, c.ClientKeepalive.Idle)
	require.Equal(t, DefaultKeepaliveCount, c.Sites[0].DefaultUpstream().Keepalive.Count)
	require.Equal(t, DefaultUpstreamReadTimeout, c.Sites[0].DefaultUpstream().Timeouts.Read)
	require.Equal(t, DefaultHealthPath, c.Sites[0].DefaultUpstream().HealthCheck.Path)
	require.Equal(t, DefaultStreamsMatch, c.Sites[0].Streams.Match)
	require.NotNil(t, c.Sites[0].Streams.Program)
	require.Equal(t, DefaultBalance, c.Sites[0].DefaultUpstream().Balance)
	require.Equal(t, DefaultClientKeyIPv6PrefixBits, c.ClientKey.IPv6PrefixBits)
}

func TestParse_AllDefaults(t *testing.T) {
	c, err := Parse([]byte("sites: [{name: s, listen-http: ':80', upstreams: {default: {servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]"))
	require.NoError(t, err)
	up := c.Sites[0].DefaultUpstream()
	require.Equal(t, 5*time.Second, up.Timeouts.Connect)
	require.Equal(t, time.Minute, up.Timeouts.Read) // nginx's proxy_read_timeout
	require.Equal(t, BalanceRoundRobin, up.Balance)
	require.Equal(t, "/", c.Sites[0].DefaultUpstream().HealthCheck.Path)
	require.Equal(t, DefaultDrainDuration, c.DrainDuration)
	require.Equal(t, DefaultShutdownDrainDuration, c.ShutdownDrainDuration)
	require.Equal(t, &HealthCheck{
		Path:     DefaultHealthPath,
		Interval: DefaultHealthInterval,
		Timeout:  DefaultHealthTimeout,
		Fails:    DefaultHealthFails,
		Passes:   DefaultHealthPasses,
	}, c.Sites[0].DefaultUpstream().HealthCheck)
	defaultKeepalive := &Keepalive{
		Idle:     DefaultKeepaliveIdle,
		Interval: DefaultKeepaliveInterval,
		Count:    DefaultKeepaliveCount,
	}
	require.Equal(t, defaultKeepalive, c.ClientKeepalive)
	require.Equal(t, defaultKeepalive, c.Sites[0].DefaultUpstream().Keepalive)
	require.Empty(t, c.AccessLogs)
	require.Empty(t, c.ListenMetrics)
	require.Nil(t, c.Bans)
	require.Len(t, c.Listeners, 1)
}

// Keepalives

func TestKeepAliveConfig(t *testing.T) {
	c, err := Parse([]byte("sites: [{name: s, listen-http: ':80', upstreams: {default: {keepalive: {interval: 20s}, servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]"))
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
	}, c.Sites[0].DefaultUpstream().Keepalive.KeepAliveConfig())
}

func TestKeepAliveConfig_Disable(t *testing.T) {
	c, err := Parse([]byte("client-keepalive: {disable: true, idle: 2m}\nsites: [{name: s, listen-http: ':80', upstreams: {default: {servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]"))
	require.NoError(t, err)
	require.Equal(t, net.KeepAliveConfig{Enable: false}, c.ClientKeepalive.KeepAliveConfig())
	require.True(t, c.Sites[0].DefaultUpstream().Keepalive.KeepAliveConfig().Enable)
}

// Settings a reload cannot apply

func TestRestartRequired_Each(t *testing.T) {
	base := "sites: [{name: s, listen-http: ':80', upstreams: {default: {servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]\n"
	a, err := Parse([]byte(base))
	require.NoError(t, err)
	tests := []struct {
		name     string
		config   string
		settings []string
	}{
		{
			name:     "listen-metrics",
			config:   "listen-metrics: '10.0.0.1:9090'\n",
			settings: []string{"listen-metrics"},
		},
		{
			name:     "listen-profiler",
			config:   "listen-profiler: '10.0.0.1:6060'\n",
			settings: []string{"listen-profiler"},
		},
		{
			name:     "listen-admin",
			config:   "listen-admin: '127.0.0.1:1'\n",
			settings: []string{"listen-admin"},
		},
		{
			name:     "client-keepalive disable",
			config:   "client-keepalive: {disable: true}\n",
			settings: []string{"client-keepalive"},
		},
		{
			name:     "client-keepalive count",
			config:   "client-keepalive: {count: 9}\n",
			settings: []string{"client-keepalive"},
		},
		{
			name:   "reloadable settings only",
			config: "client-key: {ipv4-prefix-bits: 24}\naccess-logs: [{file: /tmp/x}]\n",
		},
		{
			name:   "same keepalive written out",
			config: "client-keepalive: {idle: 1m, interval: 15s, count: 4}\n",
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

func FuzzParse(f *testing.F) {
	// Any input either parses or is an error: never a panic (a reload must not take the server down)
	for _, file := range []string{"../packaging/config.yml", "../packaging/sites.d/ntfy.yml"} {
		b, err := os.ReadFile(file)
		require.NoError(f, err)
		f.Add(b)
	}
	f.Add([]byte("sites: [{name: s, listen-http: ':80', upstreams: {default: {servers: [{name: a, addresses: ['1.2.3.4:1-3'], weight: 2}]}}}]"))
	f.Fuzz(func(t *testing.T, b []byte) {
		c, err := Parse(b)
		if err == nil {
			require.NotEmpty(t, c.Sites)
			for _, site := range c.Sites {
				require.NotEmpty(t, site.DefaultUpstream().Servers)
			}
		}
	})
}
