package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"heckel.io/ntlb/match"
)

const (
	testUp = "upstreams: [{name: x, addresses: ['1.2.3.4:80']}]"
)

func TestParse_Sites(t *testing.T) {
	c, err := Parse([]byte(`
sites:
  - name: ntfy
    listen-http: ":80"
    hosts: [ntfy.sh, "*.ntfy.sh"]
    upstreams: [{name: app1, addresses: ['10.0.0.1:2586']}]
    rules: [{name: limit, action: limit, rate: 1/s}]
  - name: other
    listen-http: [":80", ":8080"]
    hosts: [Example.COM., "*.eu.ntfy.sh"]
    health-check: {path: /healthz, interval: 10s}
    streams: {match: 'false'}
    upstreams: [{name: app1, addresses: ['10.0.0.2:80']}]
  - name: fallback
    listen-http: ":80"
    upstreams: [{name: web, addresses: ['10.0.0.3:80']}]
`))
	require.NoError(t, err)
	require.Len(t, c.Sites, 3)

	// Defaults are per site
	ntfy, other := c.Sites[0], c.Sites[1]
	require.Equal(t, DefaultHealthPath, ntfy.HealthCheck.Path)
	require.Equal(t, DefaultStreamsMatch, ntfy.Streams.Match)
	require.Equal(t, "/healthz", other.HealthCheck.Path)
	require.Equal(t, 10*time.Second, other.HealthCheck.Interval)
	require.Equal(t, DefaultHealthFails, other.HealthCheck.Fails)
	require.Equal(t, []string{"example.com", "*.eu.ntfy.sh"}, other.Hosts) // Normalized
	require.Equal(t, ListenAddrs{":80"}, ntfy.ListenHTTP)                  // A single address or a list
	require.Equal(t, ListenAddrs{":80", ":8080"}, other.ListenHTTP)

	// Site rules are named after their site
	require.Equal(t, "ntfy/limit", ntfy.Rules[0].ID)
	require.Equal(t, 3, c.UpstreamCount())

	// The listeners, in order of first use, with their sites
	require.Len(t, c.Listeners, 2)
	require.Equal(t, ":80", c.Listeners[0].Addr)
	require.False(t, c.Listeners[0].TLS)
	require.Equal(t, ":8080", c.Listeners[1].Addr)
	require.Equal(t, []*Site{ntfy, other, c.Sites[2]}, c.Listeners[0].Sites)
	require.Equal(t, []*Site{other}, c.Listeners[1].Sites)

	// Hosts: exact, wildcard (any depth), case, port and trailing dot; the rest goes to the
	// listener's site without hosts
	for host, site := range map[string]string{
		"ntfy.sh":          "ntfy",
		"NTFY.sh:443":      "ntfy",
		"ntfy.sh.":         "ntfy",
		"docs.ntfy.sh":     "ntfy",
		"a.b.ntfy.sh":      "ntfy",
		"x.eu.ntfy.sh":     "other", // The longest wildcard wins
		"eu.ntfy.sh":       "ntfy",
		"example.com:8080": "other",
		"www.example.com":  "fallback",
		"xntfy.sh":         "fallback",
		"10.0.0.1":         "fallback",
		"[2001:db8::1]:80": "fallback",
		"":                 "fallback",
	} {
		require.Equal(t, site, c.SiteFor(":80", host).Name, host)
	}

	// Each listener only has its own sites
	require.Equal(t, "other", c.SiteFor(":8080", "example.com").Name)
	require.Nil(t, c.SiteFor(":8080", "ntfy.sh"))
	require.Nil(t, c.SiteFor(":9999", "ntfy.sh"))
}

func TestParse_ListenersThatDoNotOverlap(t *testing.T) {
	// Other IPs or ports, and port 0 (any free port), bind side by side
	c, err := Parse([]byte("sites: [{name: a, listen-http: ['127.0.0.1:80', '127.0.0.1:0', '[::1]:80'], " + testUp + "}, {name: b, listen-http: ['127.0.0.2:80', '127.0.0.2:0', ':81'], hosts: [b.com], " + testUp + "}]"))
	require.NoError(t, err)
	require.Len(t, c.Listeners, 6)
}

func TestParse_SitesTLS(t *testing.T) {
	c, err := Parse([]byte(`
sites:
  - {name: a, listen-http: ":80", listen-https: ":443", hosts: [a.com], cert-file: /a.pem, key-file: /a.key, ` + testUp + `}
  - {name: b, listen-https: [":443", ":8443"], hosts: [b.com], cert-file: /b.pem, key-file: /b.key, ` + testUp + `}
`))
	require.NoError(t, err)
	require.Len(t, c.Listeners, 3)
	require.Equal(t, ":443", c.Listeners[1].Addr)
	require.True(t, c.Listeners[1].TLS)
	require.Len(t, c.Listeners[1].Sites, 2)
	require.Equal(t, "b", c.SiteFor(":8443", "b.com").Name)
}

func TestParse_SitesWithoutDefault(t *testing.T) {
	c, err := Parse([]byte("sites: [{name: a, listen-http: ':80', hosts: [a.com], " + testUp + "}]"))
	require.NoError(t, err)
	require.Equal(t, "a", c.SiteFor(":80", "a.com").Name)
	require.Nil(t, c.SiteFor(":80", "b.com"))
}

func TestParse_SiteRulesAndGlobalRules(t *testing.T) {
	c, err := Parse([]byte(`
bans: {file: /tmp/ban.log, threshold: 10}
rules: [{name: limit, action: limit, rate: 1/s, strike: 1}]
sites:
  - name: a
    listen-http: ':80'
    upstreams: [{name: x, addresses: ['1.2.3.4:80']}]
    rules: [{name: limit, action: limit, rate: 2/s, strike: 1}, {name: site, match: 'site == "a"', action: block}]
`))
	require.NoError(t, err)
	require.Equal(t, "limit", c.Rules[0].ID)
	require.Equal(t, "a/limit", c.Sites[0].Rules[0].ID) // Same name, other scope
}

func TestParse_SiteAccessLogs(t *testing.T) {
	// A site's access logs replace the global ones for its requests
	c, err := Parse([]byte(`
access-logs: [{file: /var/log/ntlb/access.log}]
sites:
  - {name: a, listen-http: ':80', hosts: [a.com], access-logs: [{file: /var/log/ntlb/a.log, match: status < 400}, {file: /var/log/ntlb/a_error.log}], ` + testUp + `}
  - {name: b, listen-http: ':80', ` + testUp + `}
`))
	require.NoError(t, err)
	require.Len(t, c.Sites[0].AccessLogs, 2)
	require.True(t, c.Sites[0].AccessLogs[0].Program.Match(&match.ResponseEnv{Status: 200}))
	require.Equal(t, "/var/log/ntlb/a.log", c.AccessLogsFor(c.Sites[0])[0].File)
	require.Equal(t, "/var/log/ntlb/access.log", c.AccessLogsFor(c.Sites[1])[0].File)
	require.Equal(t, "/var/log/ntlb/access.log", c.AccessLogsFor(nil)[0].File) // No site
	_, err = Parse([]byte("sites: [{name: a, listen-http: ':80', access-logs: [{match: status < 400}], " + testUp + "}]"))
	require.ErrorContains(t, err, "site a: access-logs[0]")
}

func TestParse_SitesInvalid(t *testing.T) {
	up := "listen-http: ':80', " + testUp
	tests := map[string]string{
		"no sites":                 "",
		"empty site":               "sites: [null]",
		"no name":                  "sites: [{" + up + "}]",
		"bad name":                 "sites: [{name: 'a/b', " + up + "}]",
		"duplicate name":           "sites: [{name: a, hosts: [a.com], " + up + "}, {name: a, hosts: [b.com], " + up + "}]",
		"no upstreams":             "sites: [{name: a, listen-http: ':80'}]",
		"no listener":              "sites: [{name: a, " + testUp + "}]",
		"empty listener":           "sites: [{name: a, listen-http: '', " + testUp + "}]",
		"listener not a string":    "sites: [{name: a, listen-http: {a: 1}, " + testUp + "}]",
		"bad listener":             "sites: [{name: a, listen-http: 'nope', " + testUp + "}]",
		"http and https":           "sites: [{name: a, listen-http: ':80', " + testUp + "}, {name: b, listen-https: ':80', cert-file: /c, key-file: /k, hosts: [b.com], " + testUp + "}]",
		"https without cert":       "sites: [{name: a, listen-https: ':443', " + testUp + "}]",
		"duplicate listener":       "sites: [{name: a, listen-http: [':80', ':80'], " + testUp + "}]",
		"overlapping listeners":    "sites: [{name: a, listen-http: ':80', " + testUp + "}, {name: b, listen-http: '0.0.0.0:80', hosts: [b.com], " + testUp + "}]",
		"overlapping ipv6":         "sites: [{name: a, listen-http: '[::]:80', " + testUp + "}, {name: b, listen-http: '10.0.0.1:80', hosts: [b.com], " + testUp + "}]",
		"same address respelled":   "sites: [{name: a, listen-http: '127.0.0.1:80', " + testUp + "}, {name: b, listen-http: '[::ffff:127.0.0.1]:80', hosts: [b.com], " + testUp + "}]",
		"duplicate upstream":       "sites: [{name: a, listen-http: ':80', upstreams: [{name: x, addresses: ['1.2.3.4:80']}, {name: x, addresses: ['1.2.3.5:80']}]}]",
		"two defaults":             "sites: [{name: a, " + up + "}, {name: b, " + up + "}]",
		"duplicate host":           "sites: [{name: a, hosts: [a.com], " + up + "}, {name: b, hosts: [A.com], " + up + "}]",
		"duplicate wildcard":       "sites: [{name: a, hosts: ['*.a.com'], " + up + "}, {name: b, hosts: ['*.a.com'], " + up + "}]",
		"empty host":               "sites: [{name: a, hosts: [''], " + up + "}]",
		"host with port":           "sites: [{name: a, hosts: ['a.com:80'], " + up + "}]",
		"bad wildcard":             "sites: [{name: a, hosts: ['a.*.com'], " + up + "}]",
		"bare wildcard":            "sites: [{name: a, hosts: ['*'], " + up + "}]",
		"cert without key":         "sites: [{name: a, cert-file: /x.pem, " + up + "}]",
		"bad health check":         "sites: [{name: a, health-check: {interval: 1s, timeout: 2s}, " + up + "}]",
		"bad streams":              "sites: [{name: a, streams: {match: 'status == 1'}, " + up + "}]",
		"bad site rule":            "sites: [{name: a, rules: [{name: r}], " + up + "}]",
		"duplicate site rule":      "sites: [{name: a, rules: [{name: r, action: block}, {name: r, action: block}], " + up + "}]",
		"site rule strike no bans": "sites: [{name: a, rules: [{name: r, action: block, strike: 1}], " + up + "}]",
		"top-level upstreams gone": "upstreams: [{name: x, addresses: ['1.2.3.4:80']}]",
		"top-level listen gone":    "listen-http: ':80'\nsites: [{name: a, " + up + "}]",
		"unknown site field":       "sites: [{name: a, path: /x, " + up + "}]",
	}
	for name, conf := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(conf))
			require.ErrorContains(t, err, "invalid config")
		})
	}
}

func TestLoad_IncludeMerges(t *testing.T) {
	// Included files are merged into the main file: lists are appended in file order (main file
	// first, then the files of each glob by name), anything else may be set once
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "conf.d"), 0700))
	writeFile(t, filepath.Join(dir, "config.yml"), `
include: [conf.d/*.yml, /nonexistent/*.yml]
rules: [{name: main, action: block, match: 'path == "/main"'}]
sites: [{name: main, listen-http: ':80', hosts: [main.com], `+testUp+`}]
`)
	writeFile(t, filepath.Join(dir, "conf.d", "b.yml"), `
sites:
  - {name: b1, listen-http: ':80', hosts: [b1.com], `+testUp+`}
  - {name: b2, listen-http: ':80', hosts: [b2.com], `+testUp+`}
`)
	writeFile(t, filepath.Join(dir, "conf.d", "a.yml"), `
bans: {file: /tmp/ban.log, threshold: 10}
rules: [{name: a, action: block, match: 'path == "/a"', strike: 1}]
sites: [{name: a, listen-http: ':80', hosts: [a.com], `+testUp+`}]
`)
	writeFile(t, filepath.Join(dir, "conf.d", "ignored.conf"), "garbage")
	c, err := Load(filepath.Join(dir, "config.yml"))
	require.NoError(t, err)
	names := make([]string, 0)
	for _, site := range c.Sites {
		names = append(names, site.Name)
	}
	require.Equal(t, []string{"main", "a", "b1", "b2"}, names)
	require.Equal(t, "main", c.Rules[0].Name)
	require.Equal(t, "a", c.Rules[1].Name)
	require.Equal(t, 10.0, c.Bans.Threshold)
	require.Equal(t, "b2", c.SiteFor(":80", "b2.com").Name)

	// A setting that is not a list may be set once
	writeFile(t, filepath.Join(dir, "conf.d", "c.yml"), "bans: {file: /tmp/other.log, threshold: 1}\n")
	_, err = Load(filepath.Join(dir, "config.yml"))
	require.ErrorContains(t, err, filepath.Join(dir, "conf.d", "c.yml")+": bans is set in another file")

	// Include only in the main file
	writeFile(t, filepath.Join(dir, "conf.d", "c.yml"), "include: [x/*.yml]\n")
	_, err = Load(filepath.Join(dir, "config.yml"))
	require.ErrorContains(t, err, "include is only allowed in the main file")

	// An error in a file names the file
	writeFile(t, filepath.Join(dir, "conf.d", "c.yml"), "sites: [{name: c, upstream: []}]\n")
	_, err = Load(filepath.Join(dir, "config.yml"))
	require.ErrorContains(t, err, "c.yml")
	writeFile(t, filepath.Join(dir, "conf.d", "c.yml"), "sites: [{name: c, listen-http: ':80', hosts: [c.com]}]\n")
	_, err = Load(filepath.Join(dir, "config.yml"))
	require.ErrorContains(t, err, "site c ("+filepath.Join(dir, "conf.d", "c.yml")+"): at least one upstream")

	// A site name taken twice, across files
	writeFile(t, filepath.Join(dir, "conf.d", "c.yml"), "sites: [{name: main, listen-http: ':80', hosts: [c.com], "+testUp+"}]\n")
	_, err = Load(filepath.Join(dir, "config.yml"))
	require.ErrorContains(t, err, "duplicate site name")

	// A directory that matches the glob
	require.NoError(t, os.Remove(filepath.Join(dir, "conf.d", "c.yml")))
	require.NoError(t, os.Mkdir(filepath.Join(dir, "conf.d", "dir.yml"), 0700))
	_, err = Load(filepath.Join(dir, "config.yml"))
	require.ErrorContains(t, err, "dir.yml")
}

func TestLoad_IncludeOnly(t *testing.T) {
	// Everything can come from included files
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "config.yml"), "include: ["+filepath.Join(dir, "*.site")+"]\n")
	_, err := Load(filepath.Join(dir, "config.yml"))
	require.ErrorContains(t, err, "at least one site")
	writeFile(t, filepath.Join(dir, "x.site"), "sites: [{name: x, listen-http: ':80', "+testUp+"}]\n")
	c, err := Load(filepath.Join(dir, "config.yml"))
	require.NoError(t, err)
	require.Equal(t, "x", c.Sites[0].Name)
}

func TestLoad_IncludeBadPattern(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "config.yml"), "include: ['[']\nsites: [{name: a, listen-http: ':80', "+testUp+"}]\n")
	_, err := Load(filepath.Join(dir, "config.yml"))
	require.ErrorContains(t, err, "include")
}

func writeFile(t *testing.T, filename, content string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filename, []byte(content), 0600))
}

func TestParse_SiteBans(t *testing.T) {
	// A site's bans settings replace the global ones for its requests; usually there are only global ones
	c, err := Parse([]byte(`
bans: {file: /var/log/ntlb/ban.log, threshold: 100}
sites:
  - {name: a, listen-http: ':80', hosts: [a.com], bans: {file: /var/log/ntlb/a-ban.log, threshold: 10}, rules: [{name: r, action: block, strike: 11}], ` + testUp + `}
  - {name: b, listen-http: ':80', ` + testUp + `}
`))
	require.NoError(t, err)
	require.Equal(t, DefaultBansWindow, c.Sites[0].Bans.Window)
	require.Equal(t, "/var/log/ntlb/a-ban.log", c.BansFor(c.Sites[0]).File)
	require.Equal(t, "/var/log/ntlb/ban.log", c.BansFor(c.Sites[1]).File)
	require.Equal(t, "/var/log/ntlb/ban.log", c.BansFor(nil).File)

	// Only site bans: strikes in that site's rules, and in global rules (they count for that
	// site's requests), are fine
	c, err = Parse([]byte(`
rules: [{name: g, action: block, strike: 1}]
sites: [{name: a, listen-http: ':80', bans: {file: /x, threshold: 1}, rules: [{name: r, action: block, strike: 1}], ` + testUp + `}]
`))
	require.NoError(t, err)
	require.Nil(t, c.BansFor(nil))

	// But a site without any bans cannot strike
	_, err = Parse([]byte(`
sites:
  - {name: a, listen-http: ':80', hosts: [a.com], bans: {file: /x, threshold: 1}, ` + testUp + `}
  - {name: b, listen-http: ':80', rules: [{name: r, action: block, strike: 1}], ` + testUp + `}
`))
	require.ErrorContains(t, err, "site b: rule b/r: strike needs a bans section")
	for name, bans := range map[string]string{
		"no file":         "{threshold: 1}",
		"no threshold":    "{file: /x}",
		"negative window": "{file: /x, threshold: 1, window: -1m}",
	} {
		_, err := Parse([]byte("sites: [{name: a, listen-http: ':80', bans: " + bans + ", " + testUp + "}]"))
		require.ErrorContains(t, err, "site a: bans", name)
	}
}

func TestConfig_Site(t *testing.T) {
	c, err := Parse([]byte("sites: [{name: a, listen-http: ':80', " + testUp + "}]"))
	require.NoError(t, err)
	require.Equal(t, "a", c.Site("a").Name)
	require.Nil(t, c.Site("b"))
	require.Nil(t, c.Site(""))
}

func TestParse_Gzip(t *testing.T) {
	// Global gzip settings, replaced by a site's own; types are matched without parameters
	c, err := Parse([]byte(`
gzip: {types: [text/html, Application/JSON]}
sites:
  - {name: a, listen-http: ':80', hosts: [a.com], gzip: {types: [text/css]}, ` + testUp + `}
  - {name: b, listen-http: ':80', ` + testUp + `}
`))
	require.NoError(t, err)
	require.Equal(t, []string{"text/css"}, c.GzipFor(c.Sites[0]).Types)
	require.Equal(t, []string{"text/html", "application/json"}, c.GzipFor(c.Sites[1]).Types)
	require.Equal(t, []string{"text/html", "application/json"}, c.GzipFor(nil).Types)

	// None: no compression
	c, err = Parse([]byte("sites: [{name: a, listen-http: ':80', " + testUp + "}]"))
	require.NoError(t, err)
	require.Nil(t, c.GzipFor(c.Sites[0]))

	for name, gzip := range map[string]string{
		"no types":       "{}",
		"not a type":     "{types: [html]}",
		"with parameter": "{types: ['text/html; charset=utf-8']}",
	} {
		_, err := Parse([]byte("gzip: " + gzip + "\nsites: [{name: a, listen-http: ':80', " + testUp + "}]"))
		require.ErrorContains(t, err, "gzip", name)
		_, err = Parse([]byte("sites: [{name: a, listen-http: ':80', gzip: " + gzip + ", " + testUp + "}]"))
		require.ErrorContains(t, err, "site a: gzip", name)
	}
}

func TestParse_Balance(t *testing.T) {
	for _, balance := range []string{BalanceRoundRobin, BalanceLeastConn, BalanceRandomTwo, BalanceHash} {
		c, err := Parse([]byte("sites: [{name: s, listen-http: ':80', balance: " + balance + ", upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]"))
		require.NoError(t, err, balance)
		require.Equal(t, balance, c.Sites[0].Balance)
	}
	_, err := Parse([]byte("sites: [{name: s, listen-http: ':80', balance: ip-hash, upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]"))
	require.ErrorContains(t, err, "site s: invalid balance \"ip-hash\"")
}
