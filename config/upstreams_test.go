package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParse_Balance(t *testing.T) {
	for _, balance := range []string{BalanceRoundRobin, BalanceLeastConn, BalanceRandomTwo, BalanceHash} {
		c, err := Parse([]byte("sites: [{name: s, listen-http: ':80', upstreams: {default: {balance: " + balance + ", servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]"))
		require.NoError(t, err, balance)
		require.Equal(t, balance, c.Sites[0].DefaultUpstream().Balance)
	}
	_, err := Parse([]byte("sites: [{name: s, listen-http: ':80', upstreams: {default: {balance: ip-hash, servers: [{name: a, addresses: ['1.2.3.4:80']}]}}}]"))
	require.ErrorContains(t, err, "site s: invalid upstreams.default.balance \"ip-hash\"")
}

func TestParse_Upstream(t *testing.T) {
	// Everything about a site's servers is in its upstream block, also the timeouts and keepalive,
	// so sites can differ (ntfy's 3m read timeout next to a site with the default)
	c, err := Parse([]byte(`
sites:
  - name: a
    listen-http: ':80'
    upstreams:
      default:
        balance: least-conn
        timeouts: {connect: 2s, read: 3m}
        keepalive: {idle: 30s}
        healthcheck: {path: /ready}
        servers:
          - name: app1
            addresses: ['10.0.0.1:1-2']
  - name: b
    listen-http: ':81'
    upstreams:
      default:
        servers: [{name: app1, addresses: ['10.0.0.2:1']}]
`))
	require.NoError(t, err)
	a, b := c.Sites[0].DefaultUpstream(), c.Sites[1].DefaultUpstream()
	require.Equal(t, BalanceLeastConn, a.Balance)
	require.Equal(t, &Timeouts{Connect: 2 * time.Second, Read: 3 * time.Minute}, a.Timeouts)
	require.Equal(t, 30*time.Second, a.Keepalive.Idle)
	require.Equal(t, DefaultKeepaliveInterval, a.Keepalive.Interval)
	require.Equal(t, "/ready", a.HealthCheck.Path)
	require.Equal(t, DefaultHealthInterval, a.HealthCheck.Interval)
	require.Equal(t, []string{"10.0.0.1:1", "10.0.0.1:2"}, a.Servers[0].Addresses)

	// Defaults
	require.Equal(t, DefaultBalance, b.Balance)
	require.Equal(t, &Timeouts{Connect: DefaultUpstreamConnectTimeout, Read: DefaultUpstreamReadTimeout}, b.Timeouts)
	require.Equal(t, &Keepalive{Idle: DefaultKeepaliveIdle, Interval: DefaultKeepaliveInterval, Count: DefaultKeepaliveCount}, b.Keepalive)
	require.Equal(t, DefaultHealthPath, b.HealthCheck.Path)
}

func TestParse_UpstreamRequired(t *testing.T) {
	_, err := Parse([]byte("sites: [{name: a, listen-http: ':80'}]"))
	require.ErrorContains(t, err, "site a: upstreams.default is required")
	_, err = Parse([]byte("sites: [{name: a, listen-http: ':80', upstreams: {default: {balance: hash}}}]"))
	require.ErrorContains(t, err, "site a: at least one server is required (upstreams.default.servers)")
}

func TestParse_ServerWeightAndBackup(t *testing.T) {
	c, err := Parse([]byte("sites: [{name: s, listen-http: ':80', upstreams: {default: {servers: [{name: a, addresses: ['1.2.3.4:80'], weight: 3}, {name: b, addresses: ['1.2.3.5:80']}, {name: c, addresses: ['1.2.3.6:80'], backup: true}]}}}]"))
	require.NoError(t, err)
	servers := c.Sites[0].DefaultUpstream().Servers
	require.Equal(t, 3, servers[0].Weight)
	require.Equal(t, 1, servers[1].Weight) // Default
	require.False(t, servers[1].Backup)
	require.True(t, servers[2].Backup)
	for weight, errMsg := range map[string]string{
		"-1":   "server a: weight must be between 1 and 1000",
		"1001": "server a: weight must be between 1 and 1000",
	} {
		_, err := Parse([]byte("sites: [{name: s, listen-http: ':80', upstreams: {default: {servers: [{name: a, addresses: ['1.2.3.4:80'], weight: " + weight + "}]}}}]"))
		require.ErrorContains(t, err, errMsg, weight)
	}
	_, err = Parse([]byte("sites: [{name: s, listen-http: ':80', upstreams: {default: {servers: [{name: a, addresses: ['1.2.3.4:80'], backup: true}]}}}]"))
	require.ErrorContains(t, err, "site s: at least one server must not be a backup")
}

func TestParse_NamedUpstreams(t *testing.T) {
	// A site's upstreams by name: default takes what no rule routes, the others what proxy rules
	// send there, each with its own balance, timeouts and health check
	c, err := Parse([]byte(`
sites:
  - name: s
    listen-http: ':80'
    upstreams:
      app2:
        timeouts: {read: 5m}
        servers: [{name: b1, addresses: ['10.0.2.1:80']}]
      default:
        servers: [{name: web, addresses: ['10.0.0.1:80']}]
      app1:
        balance: least-conn
        healthcheck: {path: /healthz}
        servers:
          - {name: a1, addresses: ['10.0.1.1:80']}
          - {name: a2, addresses: ['10.0.1.2:80']}
    rules:
      - {name: to-app1, match: 'path startsWith "/app1/"', action: proxy, upstream: app1}
      - {name: to-default, match: 'path == "/x"', action: proxy, upstream: default}
      - {name: to-default-implied, match: 'path == "/y"', action: proxy}
`))
	require.NoError(t, err)
	site := c.Sites[0]

	// The default first, then the others by name, all with their defaults applied
	upstreams := site.AllUpstreams()
	require.Len(t, upstreams, 3)
	require.Equal(t, []string{UpstreamDefault, "app1", "app2"}, []string{upstreams[0].Name, upstreams[1].Name, upstreams[2].Name})
	require.Same(t, site.Upstreams[UpstreamDefault], site.DefaultUpstream())
	app1, app2 := site.Upstreams["app1"], site.Upstreams["app2"]
	require.Equal(t, BalanceLeastConn, app1.Balance)
	require.Equal(t, "/healthz", app1.HealthCheck.Path)
	require.Equal(t, DefaultHealthInterval, app1.HealthCheck.Interval)
	require.Equal(t, DefaultBalance, app2.Balance)
	require.Equal(t, 5*time.Minute, app2.Timeouts.Read)
	require.Equal(t, DefaultUpstreamConnectTimeout, app2.Timeouts.Connect)
	require.Equal(t, 4, c.UpstreamCount())

	// A proxy rule without an upstream goes to the default
	require.Equal(t, UpstreamDefault, site.Rules[2].Upstream)

	// Lookup by name, "" being the default
	require.Same(t, app1, site.UpstreamNamed("app1"))
	require.Same(t, site.DefaultUpstream(), site.UpstreamNamed(""))
	require.Nil(t, site.UpstreamNamed("nope"))

	// Proxy rules name their upstream
	require.Equal(t, ActionProxy, site.Rules[0].Action)
	require.Equal(t, "app1", site.Rules[0].Upstream)
}

func TestParse_NamedUpstreamsInvalid(t *testing.T) {
	const web = "default: {servers: [{name: web, addresses: ['10.0.0.1:80']}]}"
	for config, errMsg := range map[string]string{
		"upstreams: {app1: {servers: [{name: x, addresses: ['10.0.0.2:80']}]}}":               "site s: upstreams.default is required",
		"upstreams: {" + web + ", 'a b': {servers: [{name: x, addresses: ['10.0.0.2:80']}]}}": `site s: invalid upstream name "a b"`,
		"upstreams: {default: null}":                      "site s: upstreams.default is empty",
		"upstreams: {" + web + ", app1: null}":            "site s: upstreams.app1 is empty",
		"upstreams: {" + web + ", app1: {balance: hash}}": "site s: at least one server is required (upstreams.app1.servers)",
		"upstreams: {" + web + ", app1: {balance: x, servers: [{name: a, addresses: ['10.0.0.2:80']}]}}":                   `site s: invalid upstreams.app1.balance "x"`,
		"upstreams: {" + web + ", app1: {healthcheck: {interval: -1s}, servers: [{name: a, addresses: ['10.0.0.2:80']}]}}": "site s: upstreams.app1.healthcheck.interval must be positive",
		"upstreams: {" + web + ", app1: {timeouts: {read: -1s}, servers: [{name: a, addresses: ['10.0.0.2:80']}]}}":        "site s: upstreams.app1.timeouts.read must be positive",
		"upstream: {servers: [{name: x, addresses: ['10.0.0.2:80']}]}":                                                     "field upstream not found",

		// Server names are per site, over all its upstreams: they name the node in drains, the
		// status and the metrics
		"upstreams: {" + web + ", app1: {servers: [{name: web, addresses: ['10.0.0.2:80']}]}}": `site s: duplicate server name "web"`,

		// A proxy rule names an upstream of its site
		"upstreams: {" + web + "}, rules: [{name: r, action: proxy, upstream: nope}]":                 `site s: rule s/r: unknown upstream "nope"`,
		"upstreams: {" + web + "}, rules: [{name: r, action: proxy, upstream: default, status: 404}]": "site s: rule s/r: proxy takes upstream only",
		"upstreams: {" + web + "}, rules: [{name: r, action: block, upstream: default}]":              "site s: rule s/r: block takes status and strike only",
	} {
		_, err := Parse([]byte("sites: [{name: s, listen-http: ':80', " + config + "}]"))
		require.ErrorContains(t, err, errMsg, config)
	}

	// Upstreams are per site, so global rules cannot proxy
	_, err := Parse([]byte("rules: [{name: r, action: proxy, upstream: default}]\nsites: [{name: s, listen-http: ':80', upstreams: {" + web + "}}]"))
	require.ErrorContains(t, err, "rule r: proxy is only allowed in a site's rules")
}

func TestParse_PortRanges(t *testing.T) {
	c, err := Parse([]byte("sites: [{name: s, listen-http: ':80', upstreams: {default: {servers: [{name: a, addresses: ['10.0.0.1:2586-2588', '[fd00::1]:9000-9001', 'node:80']}]}}}]"))
	require.NoError(t, err)
	require.Equal(t, []string{"10.0.0.1:2586", "10.0.0.1:2587", "10.0.0.1:2588", "[fd00::1]:9000", "[fd00::1]:9001", "node:80"}, c.Sites[0].DefaultUpstream().Servers[0].Addresses)
	for _, bad := range []string{"10.0.0.1:2588-2586", "10.0.0.1:1-x", "10.0.0.1:x-5", "10.0.0.1:0-5", "10.0.0.1:65535-65536", "10.0.0.1:1000-3000"} {
		_, err := Parse([]byte("sites: [{name: s, listen-http: ':80', upstreams: {default: {servers: [{name: a, addresses: ['" + bad + "']}]}}}]"))
		require.ErrorContains(t, err, "invalid address", bad)
	}
}

func TestParse_DuplicateAddress(t *testing.T) {
	_, err := Parse([]byte("sites: [{name: s, listen-http: ':80', upstreams: {default: {servers: [{name: a, addresses: ['10.0.0.1:2586-2587', '10.0.0.1:2587']}]}}}]"))
	require.ErrorContains(t, err, "duplicate address")
}
