package config

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"heckel.io/ntlb/match"
)

const (
	rulesTestBase = "sites: [{name: s, listen-http: ':80', upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]\n"
	rulesTestBans = "bans: {file: /tmp/ban.log, threshold: 10}\n"
)

func TestParse_Rules(t *testing.T) {
	c, err := Parse([]byte(rulesTestBase + rulesTestBans + `
rules:
  - name: abuse
    match: method in ["GET", "POST"] && (ipIn(ip, "203.0.113.0/24") || path matches "^/nz_")
    action: block
    strike: 20
  - name: exempt
    match: ipIn(ip, "116.203.112.46", "176.126.240.0/24")
    action: allow
  - name: limit
    action: limit
    rate: 45/m
    burst: 1000
    strike: 0.5
  - name: docs
    match: path startsWith "/docs/"
    action: redirect
    location: '"https://docs.ntfy.sh/" + trimPrefix(path, "/docs/")'
  - name: website
    match: path == "/"
    action: static
    root: /var/www/website/html
    fallback: /index.html
  - name: bad-requests
    on: response
    match: status == 400 && upstream_status == 0
    strike: 1
`))
	require.NoError(t, err)
	require.Len(t, c.Rules, 6)

	// Block: 403 by default; the expression is compiled
	r := c.Rules[0]
	require.Equal(t, OnRequest, r.On)
	require.Equal(t, 403, r.Status)
	require.Equal(t, 20.0, r.Strike)
	require.True(t, r.Program.Match(match.NewEnv(httptest.NewRequest("GET", "/nz_x", nil))))
	require.Zero(t, c.Rules[1].Status)

	// Limit: 429 by default, rate parsed; no match means every request
	l := c.Rules[2]
	require.Equal(t, 429, l.Status)
	require.InDelta(t, 0.75, l.RatePerSecond, 1e-9)
	require.Equal(t, 1000, l.Burst)
	require.Nil(t, l.Program)

	// Redirect: 302 by default, location compiled as a string
	d := c.Rules[3]
	require.Equal(t, 302, d.Status)
	require.Equal(t, "https://docs.ntfy.sh/publish/", d.LocationProgram.String(match.NewEnv(httptest.NewRequest("GET", "/docs/publish/", nil))))

	// Static
	require.Equal(t, "/var/www/website/html", c.Rules[4].Root)
	require.Equal(t, "/index.html", c.Rules[4].Fallback)

	// Response rules see the outcome
	b := c.Rules[5]
	require.Equal(t, OnResponse, b.On)
	require.True(t, b.Program.Match(&match.ResponseEnv{Status: 400}))
}

func TestParse_RuleRates(t *testing.T) {
	for rate, perSecond := range map[string]float64{"10/s": 10, "45/m": 0.75, "36/h": 0.01, "1/s": 1} {
		c, err := Parse([]byte(rulesTestBase + "rules: [{name: l, action: limit, rate: '" + rate + "'}]"))
		require.NoError(t, err, rate)
		require.InDelta(t, perSecond, c.Rules[0].RatePerSecond, 1e-9, rate)
	}
}

func TestParse_RulesInvalid(t *testing.T) {
	tests := map[string]string{
		"no name":                   "rules: [{action: block}]",
		"bad name":                  "rules: [{name: 'a b', action: block}]",
		"duplicate name":            "rules: [{name: a, action: block}, {name: a, action: block}]",
		"no action":                 "rules: [{name: a, match: 'path == \"/x\"'}]",
		"unknown action":            "rules: [{name: a, action: drop}]",
		"unknown on":                "rules: [{name: a, on: later, action: block}]",
		"bad expression":            "rules: [{name: a, action: block, match: 'path =='}]",
		"unknown identifier":        "rules: [{name: a, action: block, match: 'agent == \"x\"'}]",
		"status in request rule":    "rules: [{name: a, action: block, match: 'status == 400'}]",
		"not boolean":               "rules: [{name: a, action: block, match: 'path'}]",
		"bad network":               "rules: [{name: a, action: block, match: 'ipIn(ip, \"1.2.3\")'}]",
		"bad status":                "rules: [{name: a, action: block, status: 99}]",
		"limit without rate":        "rules: [{name: a, action: limit}]",
		"bad rate":                  "rules: [{name: a, action: limit, rate: '45'}]",
		"bad rate unit":             "rules: [{name: a, action: limit, rate: '45/d'}]",
		"zero rate":                 "rules: [{name: a, action: limit, rate: '0/m'}]",
		"negative burst":            "rules: [{name: a, action: limit, rate: '1/s', burst: -1}]",
		"rate on block":             "rules: [{name: a, action: block, rate: '1/s'}]",
		"burst on block":            "rules: [{name: a, action: block, burst: 5}]",
		"status on allow":           "rules: [{name: a, action: allow, status: 403}]",
		"strike on allow":           "rules: [{name: a, action: allow, strike: 1}]",
		"strike without bans":       "rules: [{name: a, action: block, strike: 1}]",
		"negative strike":           "rules: [{name: a, action: block, strike: -1}]",
		"redirect without location": "rules: [{name: a, action: redirect}]",
		"redirect bad location":     "rules: [{name: a, action: redirect, location: 'path == \"/\"'}]",
		"redirect non-3xx":          "rules: [{name: a, action: redirect, location: '\"/x\"', status: 200}]",
		"location on block":         "rules: [{name: a, action: block, location: '\"/x\"'}]",
		"static without root":       "rules: [{name: a, action: static}]",
		"static relative fallback":  "rules: [{name: a, action: static, root: /srv, fallback: index.html}]",
		"static with status":        "rules: [{name: a, action: static, root: /srv, status: 200}]",
		"root on block":             "rules: [{name: a, action: block, root: /srv}]",
		"location on limit":         "rules: [{name: a, action: limit, rate: 1/s, location: '\"/x\"'}]",
		"rate on redirect":          "rules: [{name: a, action: redirect, location: '\"/x\"', rate: 1/s}]",
		"strike on redirect":        rulesTestBans + "rules: [{name: a, action: redirect, location: '\"/x\"', strike: 1}]",
		"location on static":        "rules: [{name: a, action: static, root: /srv, location: '\"/x\"'}]",
		"strike on static":          rulesTestBans + "rules: [{name: a, action: static, root: /srv, strike: 1}]",
		"response with action":      rulesTestBans + "rules: [{name: a, on: response, action: block, strike: 1}]",
		"response without strike":   rulesTestBans + "rules: [{name: a, on: response, match: 'status == 400'}]",
		"response with header()":    rulesTestBans + "rules: [{name: a, on: response, match: 'header(\"X\") == \"\"', strike: 1}]",
		"empty rule":                "rules: [null]",
		"unknown field":             "rules: [{name: a, action: block, path: ['x']}]",
		"rules-file is gone":        "rules-file: /etc/ntlb/rules.yml",
		"log is gone":               "rules: [{name: a, action: block, log: /tmp/x.log}]",
	}
	for name, rules := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(rulesTestBase + rules))
			require.Error(t, err)
			require.ErrorContains(t, err, "invalid config")
		})
	}
}

func TestParse_RuleErrorNamesTheRule(t *testing.T) {
	_, err := Parse([]byte(rulesTestBase + "rules: [{name: spy, action: block, match: 'header(\"User-Agent\") matches \"(\"'}]"))
	require.ErrorContains(t, err, "rule spy")
}

func TestLoad_PackagingExampleRules(t *testing.T) {
	// The shipped example's rules do what their comments say
	c, err := Load("../packaging/config.yml")
	require.NoError(t, err)
	byName := make(map[string]*Rule)
	for _, r := range append(c.Rules, c.Sites[0].Rules...) {
		byName[r.ID] = r
	}
	topics := func(n int) *match.Env {
		return match.NewEnv(httptest.NewRequest("GET", "/"+strings.Repeat("a,", n)+"a/json", nil))
	}
	require.True(t, byName["ntfy/abuse-topic-count"].Program.Match(topics(150)))
	require.False(t, byName["ntfy/abuse-topic-count"].Program.Match(topics(149)))
	require.Equal(t, ActionAllow, byName["ntfy/limit-exempt"].Action)
	require.Equal(t, 503, byName["ntfy/limit"].Status)
	require.True(t, byName["ntfy/abuse-topics"].Program.Match(match.NewEnv(httptest.NewRequest("GET", "/a,my-bets/json", nil))))
	require.False(t, byName["ntfy/abuse-topics"].Program.Match(match.NewEnv(httptest.NewRequest("GET", "/my-bets2/json", nil))))
	require.Greater(t, byName["ntfy/abuse-topics"].Strike, c.Bans.Threshold) // Bans on the first request
	require.Equal(t, OnResponse, byName["bad-requests"].On)

	// The docs redirect, also for /docs itself, before the HTTPS redirect (like nginx's locations)
	docs := func(path string) string {
		return byName["ntfy/docs-redirect"].LocationProgram.String(match.NewEnv(httptest.NewRequest("GET", path, nil)))
	}
	require.Equal(t, "https://docs.ntfy.sh/", docs("/docs"))
	require.Equal(t, "https://docs.ntfy.sh/", docs("/docs/"))
	require.Equal(t, "https://docs.ntfy.sh/publish/", docs("/docs/publish/"))
	order := make(map[string]int)
	for i, r := range c.Sites[0].Rules {
		order[r.Name] = i
	}
	require.Less(t, order["docs-redirect"], order["https-redirect"])
	require.Empty(t, byName["ntfy/website-assets"].Fallback) // A missing asset is a 404
}
