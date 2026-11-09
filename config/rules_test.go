package config

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"heckel.io/ntfy-lb/match"
)

const rulesTestBase = "listen-http: ':80'\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]\n"

func TestParse_Rules(t *testing.T) {
	c, err := Parse([]byte(rulesTestBase + `
rules:
  - name: abuse
    match: method in ["GET", "POST"] && (ipIn(ip, "203.0.113.0/24") || path matches "^/nz_")
    log: /var/log/ntfy-lb/abuse.log
  - name: exempt
    match: ipIn(ip, "116.203.112.46", "176.126.240.0/24")
    action: allow
  - name: limit
    action: limit
    rate: 45/m
    burst: 1000
`))
	require.NoError(t, err)
	require.Len(t, c.Rules, 3)

	// Block is the default action, 403 its default status; the expression is compiled
	r := c.Rules[0]
	require.Equal(t, ActionBlock, r.Action)
	require.Equal(t, 403, r.Status)
	require.Equal(t, "/var/log/ntfy-lb/abuse.log", r.Log)
	require.NotNil(t, r.Program)
	req := httptest.NewRequest("GET", "/nz_x", nil)
	require.True(t, r.Program.Match(match.NewEnv(req)))
	require.Zero(t, c.Rules[1].Status)

	// Limit: 429 by default, rate parsed; no match means every request
	l := c.Rules[2]
	require.Equal(t, 429, l.Status)
	require.InDelta(t, 0.75, l.RatePerSecond, 1e-9)
	require.Equal(t, 1000, l.Burst)
	require.Nil(t, l.Program)
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
		"no name":                 "rules: [{match: 'path == \"/x\"'}]",
		"bad name":                "rules: [{name: 'a b'}]",
		"duplicate name":          "rules: [{name: a}, {name: a}]",
		"unknown action":          "rules: [{name: a, action: drop}]",
		"bad expression":          "rules: [{name: a, match: 'path =='}]",
		"unknown identifier":      "rules: [{name: a, match: 'agent == \"x\"'}]",
		"not boolean":             "rules: [{name: a, match: 'path'}]",
		"bad network":             "rules: [{name: a, match: 'ipIn(ip, \"1.2.3\")'}]",
		"bad status":              "rules: [{name: a, status: 99}]",
		"limit without rate":      "rules: [{name: a, action: limit}]",
		"bad rate":                "rules: [{name: a, action: limit, rate: '45'}]",
		"bad rate unit":           "rules: [{name: a, action: limit, rate: '45/d'}]",
		"zero rate":               "rules: [{name: a, action: limit, rate: '0/m'}]",
		"negative burst":          "rules: [{name: a, action: limit, rate: '1/s', burst: -1}]",
		"rate on block":           "rules: [{name: a, rate: '1/s'}]",
		"burst on block":          "rules: [{name: a, burst: 5}]",
		"status on allow":         "rules: [{name: a, action: allow, status: 403}]",
		"log on allow":            "rules: [{name: a, action: allow, log: /tmp/x.log}]",
		"empty rule":              "rules: [null]",
		"unknown field":           "rules: [{name: a, path: ['x']}]",
		"missing rules-file":      "rules-file: /nonexistent/rules.yml",
		"rules-file unknown keys": "", // Filled in below
	}
	for name, rules := range tests {
		t.Run(name, func(t *testing.T) {
			if name == "rules-file unknown keys" {
				filename := filepath.Join(t.TempDir(), "rules.yml")
				require.NoError(t, os.WriteFile(filename, []byte("rule: []"), 0600))
				rules = "rules-file: " + filename
			}
			_, err := Parse([]byte(rulesTestBase + rules))
			require.Error(t, err)
			require.ErrorContains(t, err, "invalid config")
		})
	}
}

func TestParse_RuleErrorNamesTheRule(t *testing.T) {
	_, err := Parse([]byte(rulesTestBase + "rules: [{name: spy, match: 'header(\"User-Agent\") matches \"(\"'}]"))
	require.ErrorContains(t, err, "rule spy")
}

func TestLoad_RulesFile(t *testing.T) {
	dir := t.TempDir()
	rules := "rules:\n  - name: from-file\n    match: path startsWith \"/nz_\"\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "rules.yml"), []byte(rules), 0600))

	// A relative rules-file is relative to the config file; its rules come after the inline ones
	conf := rulesTestBase + "rules-file: rules.yml\nrules: [{name: inline}]\n"
	filename := filepath.Join(dir, "config.yml")
	require.NoError(t, os.WriteFile(filename, []byte(conf), 0600))
	c, err := Load(filename)
	require.NoError(t, err)
	require.Len(t, c.Rules, 2)
	require.Equal(t, "inline", c.Rules[0].Name)
	require.Equal(t, "from-file", c.Rules[1].Name)
	require.NotNil(t, c.Rules[1].Program)

	// Names must be unique across both
	require.NoError(t, os.WriteFile(filepath.Join(dir, "rules.yml"), []byte("rules: [{name: inline}]"), 0600))
	_, err = Load(filename)
	require.ErrorContains(t, err, "duplicate rule name")

	// An empty rules-file is fine
	require.NoError(t, os.WriteFile(filepath.Join(dir, "rules.yml"), nil, 0600))
	c, err = Load(filename)
	require.NoError(t, err)
	require.Len(t, c.Rules, 1)
}

func TestLoad_PackagingRulesExample(t *testing.T) {
	// The shipped example must always parse, also with unknown fields rejected
	conf := rulesTestBase + "rules-file: " + filepath.Join("..", "packaging", "rules.yml")
	c, err := Parse([]byte(conf))
	require.NoError(t, err)
	require.Len(t, c.Rules, 5)
	require.Equal(t, ActionAllow, c.Rules[3].Action)
	require.Equal(t, 503, c.Rules[4].Status)
	topics := func(n int) *match.Env {
		return match.NewEnv(httptest.NewRequest("GET", "/"+strings.Repeat("a,", n)+"a/json", nil))
	}
	require.True(t, c.Rules[2].Program.Match(topics(150)))
	require.False(t, c.Rules[2].Program.Match(topics(149)))
}
