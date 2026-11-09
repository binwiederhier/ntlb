package server

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"heckel.io/ntfy-lb/config"
)

func TestServer_RuleBlocks(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	dir := t.TempDir()
	abuseLog, accessLogFile := filepath.Join(dir, "abuse.log"), filepath.Join(dir, "access.log")
	s, _ := newTestServer(t, "access-log: "+accessLogFile+`
rules:
  - name: abuse
    match: path startsWith "/nz_"
    log: `+abuseLog+`
  - name: spy
    match: header("User-Agent") matches "(?i)ntfy-spy"
    status: 451
`, app1)

	// Blocked before any upstream sees it, also subscriptions
	quick := &http.Client{Timeout: 3 * time.Second} // Unblocked, this would be an endless subscription
	status, body := get(t, quick, "http://"+s.HTTPAddr()+"/nz_x/json")
	require.Equal(t, http.StatusForbidden, status)
	require.Equal(t, "Forbidden\n", body)
	req, _ := http.NewRequest(http.MethodGet, "http://"+s.HTTPAddr()+"/name", nil)
	req.Header.Set("User-Agent", "ntfy-spy/1.0")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, 451, resp.StatusCode)
	require.Zero(t, app1.hits.Load())

	// Everything else passes
	status, _ = get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/name")
	require.Equal(t, http.StatusOK, status)

	// The rule's log has its requests only; the access log has all of them, with the rule
	waitFor(t, func() bool {
		b, _ := os.ReadFile(accessLogFile)
		return strings.Count(string(b), "\n") == 3
	})
	b, err := os.ReadFile(abuseLog)
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	require.Len(t, lines, 1)
	require.Contains(t, lines[0], `"GET /nz_x/json HTTP/1.1" 403 `)
	require.Contains(t, lines[0], " upstream=- upstream_status=- rule=abuse")
	b, err = os.ReadFile(accessLogFile)
	require.NoError(t, err)
	require.Contains(t, string(b), "rule=abuse\n")
	require.Contains(t, string(b), "rule=spy\n")
	require.Contains(t, string(b), "upstream=app1 upstream_status=200\n") // No rule field when none decided

	// Counted per rule
	_, metrics := adminDo(t, s, http.MethodGet, "/metrics", "")
	require.Contains(t, metrics, `ntfy_lb_rule_matches_total{rule="abuse"} 1`)
	require.Contains(t, metrics, `ntfy_lb_rule_matches_total{rule="spy"} 1`)
	require.Contains(t, metrics, `ntfy_lb_requests_total{code="403",kind="request",upstream=""} 1`)
}

func TestServer_RuleLimits(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, `
rules:
  - name: exempt
    match: ipIn(ip, "127.0.0.2")
    action: allow
  - name: limit
    action: limit
    rate: 1/h
    burst: 1
`, app1)
	a, exempt := clientFrom("127.0.0.1"), clientFrom("127.0.0.2")
	for _, want := range []int{200, 200, 429, 429} {
		status, _ := get(t, a, "http://"+s.HTTPAddr()+"/name")
		require.Equal(t, want, status)
	}
	for range 4 {
		status, _ := get(t, exempt, "http://"+s.HTTPAddr()+"/name")
		require.Equal(t, http.StatusOK, status)
	}
	require.EqualValues(t, 6, app1.hits.Load())
}

func TestServer_RulesHotReload(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	dir := t.TempDir()
	rulesFile := filepath.Join(dir, "rules.yml")
	require.NoError(t, os.WriteFile(rulesFile, nil, 0600))
	s, _ := newTestServer(t, "rules-file: "+rulesFile, app1)
	stream, closeStream := openStream(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/mytopic/json")
	defer closeStream()

	// A new block rule turns new requests away, but leaves open streams alone
	log1 := filepath.Join(dir, "rules1.log")
	writeRules := func(rules string) {
		require.NoError(t, os.WriteFile(rulesFile, []byte(rules), 0600))
	}
	writeRules("rules: [{name: mytopic, match: 'path startsWith \"/mytopic/\"', log: " + log1 + "}]")
	requireReload(t, s)
	status, _ := get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/mytopic/json?poll=1")
	require.Equal(t, http.StatusForbidden, status)
	require.False(t, streamClosed(stream))

	// A broken rules file is rejected as a whole, and the old rules stay
	writeRules("rules: [{name: mytopic, match: 'path matches \"(\"'}]")
	_, err := s.Reload()
	require.ErrorContains(t, err, "invalid match")
	status, _ = get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/mytopic/json?poll=1")
	require.Equal(t, http.StatusForbidden, status)

	// So is a log file that cannot be opened
	writeRules("rules: [{name: mytopic, match: 'path == \"/x\"', log: " + filepath.Join(dir, "missing", "x.log") + "}]")
	_, err = s.Reload()
	require.Error(t, err)
	status, _ = get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/mytopic/json?poll=1")
	require.Equal(t, http.StatusForbidden, status)

	// Moving the log: new lines go to the new file, the old one is closed
	log2 := filepath.Join(dir, "rules2.log")
	writeRules("rules: [{name: mytopic, match: 'path startsWith \"/mytopic/\"', log: " + log2 + "}]")
	requireReload(t, s)
	get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/mytopic/json?poll=1")
	waitFor(t, func() bool {
		b, _ := os.ReadFile(log2)
		return strings.Count(string(b), "\n") == 1
	})
	b, err := os.ReadFile(log1)
	require.NoError(t, err)
	require.Equal(t, 3, strings.Count(string(b), "\n")) // The three while the first file was live

	// Without the rule, everything passes again
	writeRules("")
	requireReload(t, s)
	status, _ = get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/mytopic/json?poll=1")
	require.Equal(t, http.StatusOK, status)
	require.False(t, streamClosed(stream))
	require.Len(t, *s.ruleLogs.logs.Load(), 0)
}

func TestServer_RuleLogsShareFiles(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	abuseLog := filepath.Join(t.TempDir(), "abuse.log")
	s, _ := newTestServer(t, `
rules:
  - {name: a, match: 'path == "/a"', log: `+abuseLog+`}
  - {name: b, match: 'path == "/b"', log: `+abuseLog+`}
`, app1)
	get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/a")
	get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/b")
	require.Len(t, *s.ruleLogs.logs.Load(), 1)
	waitFor(t, func() bool {
		b, _ := os.ReadFile(abuseLog)
		return strings.Contains(string(b), "rule=a\n") && strings.Contains(string(b), "rule=b\n")
	})
}

func TestServer_NewFailsOnRuleLog(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	filename := filepath.Join(t.TempDir(), "config.yml")
	writeConfig(t, filename, "rules: [{name: a, log: /nonexistent/dir/a.log}]", app1)
	_, err := New(filename)
	require.Error(t, err)
}

func TestServer_RuleLogReopenedOnReload(t *testing.T) {
	// logrotate moves the file away, then reloads: the rule log is reopened at its path
	app1 := newTestUpstream(t, "app1")
	abuseLog := filepath.Join(t.TempDir(), "abuse.log")
	s, _ := newTestServer(t, "rules: [{name: a, match: 'path == \"/a\"', log: "+abuseLog+"}]", app1)
	get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/a")
	before := s.ruleLogs.get(abuseLog)
	require.NoError(t, os.Rename(abuseLog, abuseLog+".1"))
	requireReload(t, s)
	require.Same(t, before, s.ruleLogs.get(abuseLog)) // Same log, new file
	get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/a")
	waitFor(t, func() bool {
		b, _ := os.ReadFile(abuseLog)
		return strings.Count(string(b), "\n") == 1
	})
	b, err := os.ReadFile(abuseLog + ".1")
	require.NoError(t, err)
	require.Equal(t, 1, strings.Count(string(b), "\n"))
}

func TestOpenRuleLogFiles_ClosesOnFailure(t *testing.T) {
	dir := t.TempDir()
	conf := "listen-http: ':80'\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]\nrules:\n" +
		"  - {name: a, log: " + filepath.Join(dir, "a.log") + "}\n" +
		"  - {name: b, log: " + filepath.Join(dir, "a.log") + "}\n" +
		"  - {name: c, log: " + filepath.Join(dir, "missing", "c.log") + "}\n"
	c, err := config.Parse([]byte(conf))
	require.NoError(t, err)
	opened := openFDs(t)
	_, err = openRuleLogFiles(c)
	require.ErrorContains(t, err, "rule c")
	require.Equal(t, opened, openFDs(t))
}

// openFDs counts the open file descriptors of this process
func openFDs(t *testing.T) int {
	entries, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	return len(entries)
}
