package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const sitesTestBase = "listen-http: ':80'\n"

func TestParse_Sites(t *testing.T) {
	c, err := Parse([]byte(sitesTestBase + `
sites:
  - name: ntfy
    hosts: [ntfy.sh, "*.ntfy.sh"]
    upstreams: [{name: app1, addresses: ['10.0.0.1:2586']}]
    rules: [{name: limit, action: limit, rate: 1/s}]
  - name: other
    hosts: [Example.COM., "*.eu.ntfy.sh"]
    health-check: {path: /healthz, interval: 10s}
    streams: {match: 'false'}
    upstreams: [{name: app1, addresses: ['10.0.0.2:80']}]
  - name: fallback
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

	// Site rules are named after their site
	require.Equal(t, "ntfy/limit", ntfy.Rules[0].ID)
	require.Equal(t, 3, c.UpstreamCount())

	// Hosts: exact, wildcard (any depth), case, port and trailing dot; the rest goes to the site without hosts
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
		require.Equal(t, site, c.SiteFor(host).Name, host)
	}
}

func TestParse_SitesWithoutDefault(t *testing.T) {
	c, err := Parse([]byte(sitesTestBase + "sites: [{name: a, hosts: [a.com], upstreams: [{name: x, addresses: ['1.2.3.4:80']}]}]"))
	require.NoError(t, err)
	require.Equal(t, "a", c.SiteFor("a.com").Name)
	require.Nil(t, c.SiteFor("b.com"))
}

func TestParse_SiteRulesAndGlobalRules(t *testing.T) {
	c, err := Parse([]byte(sitesTestBase + `
bans: {file: /tmp/ban.log, threshold: 10}
rules: [{name: limit, action: limit, rate: 1/s, strike: 1}]
sites:
  - name: a
    upstreams: [{name: x, addresses: ['1.2.3.4:80']}]
    rules: [{name: limit, action: limit, rate: 2/s, strike: 1}, {name: site, match: 'site == "a"', action: block}]
`))
	require.NoError(t, err)
	require.Equal(t, "limit", c.Rules[0].ID)
	require.Equal(t, "a/limit", c.Sites[0].Rules[0].ID) // Same name, other scope
}

func TestParse_SitesInvalid(t *testing.T) {
	up := "upstreams: [{name: x, addresses: ['1.2.3.4:80']}]"
	tests := map[string]string{
		"no sites":                 "",
		"empty site":               "sites: [null]",
		"no name":                  "sites: [{" + up + "}]",
		"bad name":                 "sites: [{name: 'a/b', " + up + "}]",
		"duplicate name":           "sites: [{name: a, hosts: [a.com], " + up + "}, {name: a, hosts: [b.com], " + up + "}]",
		"no upstreams":             "sites: [{name: a}]",
		"duplicate upstream":       "sites: [{name: a, upstreams: [{name: x, addresses: ['1.2.3.4:80']}, {name: x, addresses: ['1.2.3.5:80']}]}]",
		"two defaults":             "sites: [{name: a, " + up + "}, {name: b, " + up + "}]",
		"duplicate host":           "sites: [{name: a, hosts: [a.com], " + up + "}, {name: b, hosts: [A.com], " + up + "}]",
		"duplicate wildcard":       "sites: [{name: a, hosts: ['*.a.com'], " + up + "}, {name: b, hosts: ['*.a.com'], " + up + "}]",
		"empty host":               "sites: [{name: a, hosts: [''], " + up + "}]",
		"host with port":           "sites: [{name: a, hosts: ['a.com:80'], " + up + "}]",
		"bad wildcard":             "sites: [{name: a, hosts: ['a.*.com'], " + up + "}]",
		"bare wildcard":            "sites: [{name: a, hosts: ['*'], " + up + "}]",
		"cert without key":         "sites: [{name: a, cert-file: /x.pem, " + up + "}]",
		"https without cert":       "listen-https: ':443'\nsites: [{name: a, " + up + "}]",
		"bad health check":         "sites: [{name: a, health-check: {interval: 1s, timeout: 2s}, " + up + "}]",
		"bad streams":              "sites: [{name: a, streams: {match: 'status == 1'}, " + up + "}]",
		"bad site rule":            "sites: [{name: a, rules: [{name: r}], " + up + "}]",
		"duplicate site rule":      "sites: [{name: a, rules: [{name: r, action: block}, {name: r, action: block}], " + up + "}]",
		"site rule strike no bans": "sites: [{name: a, rules: [{name: r, action: block, strike: 1}], " + up + "}]",
		"top-level upstreams gone": "upstreams: [{name: x, addresses: ['1.2.3.4:80']}]",
		"unknown site field":       "sites: [{name: a, path: /x, " + up + "}]",
	}
	for name, conf := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(sitesTestBase + conf))
			require.ErrorContains(t, err, "invalid config")
		})
	}
}

func TestLoad_Include(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "sites.d"), 0700))
	writeFile(t, filepath.Join(dir, "config.yml"), sitesTestBase+`
include: [sites.d/*.yml, /nonexistent/*.yml]
sites: [{name: main, hosts: [main.com], upstreams: [{name: x, addresses: ['1.2.3.4:80']}]}]
`)
	writeFile(t, filepath.Join(dir, "sites.d", "b.yml"), "hosts: [b.com]\nupstreams: [{name: x, addresses: ['1.2.3.5:80']}]\n")
	writeFile(t, filepath.Join(dir, "sites.d", "a.yml"), "name: named\nhosts: [a.com]\nupstreams: [{name: x, addresses: ['1.2.3.6:80']}]\n")
	writeFile(t, filepath.Join(dir, "sites.d", "ignored.conf"), "garbage")

	// Included sites come after the main file's, in file name order, named after the file unless named
	c, err := Load(filepath.Join(dir, "config.yml"))
	require.NoError(t, err)
	require.Len(t, c.Sites, 3)
	require.Equal(t, "main", c.Sites[0].Name)
	require.Equal(t, "named", c.Sites[1].Name)
	require.Equal(t, "b", c.Sites[2].Name)
	require.Equal(t, "b", c.SiteFor("b.com").Name)

	// An error names the file
	writeFile(t, filepath.Join(dir, "sites.d", "c.yml"), "hosts: [c.com]\nupstream: []\n")
	_, err = Load(filepath.Join(dir, "config.yml"))
	require.ErrorContains(t, err, "c.yml")

	// An invalid site names its file too
	writeFile(t, filepath.Join(dir, "sites.d", "c.yml"), "hosts: [c.com]\n")
	_, err = Load(filepath.Join(dir, "config.yml"))
	require.ErrorContains(t, err, "site c ("+filepath.Join(dir, "sites.d", "c.yml")+"): at least one upstream")

	// A directory that matches the glob
	require.NoError(t, os.Remove(filepath.Join(dir, "sites.d", "c.yml")))
	require.NoError(t, os.Mkdir(filepath.Join(dir, "sites.d", "dir.yml"), 0700))
	_, err = Load(filepath.Join(dir, "config.yml"))
	require.ErrorContains(t, err, "dir.yml")
	require.NoError(t, os.Remove(filepath.Join(dir, "sites.d", "dir.yml")))

	// A site name taken twice, across files
	writeFile(t, filepath.Join(dir, "sites.d", "c.yml"), "name: main\nhosts: [c.com]\nupstreams: [{name: x, addresses: ['1.2.3.4:80']}]\n")
	_, err = Load(filepath.Join(dir, "config.yml"))
	require.ErrorContains(t, err, "duplicate site name")
}

func TestLoad_IncludeOnly(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "config.yml"), sitesTestBase+"include: ["+filepath.Join(dir, "*.site")+"]\n")
	_, err := Load(filepath.Join(dir, "config.yml"))
	require.ErrorContains(t, err, "at least one site")
	writeFile(t, filepath.Join(dir, "x.site"), "upstreams: [{name: x, addresses: ['1.2.3.4:80']}]\n")
	c, err := Load(filepath.Join(dir, "config.yml"))
	require.NoError(t, err)
	require.Equal(t, "x", c.Sites[0].Name)
}

func TestLoad_IncludeBadPattern(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "config.yml"), sitesTestBase+"include: ['[']\nsites: [{name: a, upstreams: [{name: x, addresses: ['1.2.3.4:80']}]}]\n")
	_, err := Load(filepath.Join(dir, "config.yml"))
	require.ErrorContains(t, err, "include")
}

func writeFile(t *testing.T, filename, content string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filename, []byte(content), 0600))
}
