package rules

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"heckel.io/ntlb/match"
)

const (
	testBans = "bans: {file: /tmp/ban.log, window: 10m, threshold: 10}\n"
)

func TestBans_BlockStrikes(t *testing.T) {
	e, now, lines := testEngineBans(t, testBans+"rules: [{name: abuse, match: 'path startsWith \"/sp_\"', action: block, strike: 5}]")

	// Up to the threshold: no ban; over it: one line
	decided(e, request("GET", "/sp_x", "192.0.2.1:1"))
	decided(e, request("GET", "/sp_x", "192.0.2.1:1"))
	require.Empty(t, *lines)
	decided(e, request("GET", "/sp_x", "192.0.2.1:1"))
	require.Equal(t, []string{"2026-11-09T12:00:00Z 192.0.2.1 192.0.2.1/32 abuse"}, *lines)

	// Once per window per client, however many more strikes come
	for range 10 {
		decided(e, request("GET", "/sp_x", "192.0.2.1:1"))
	}
	require.Len(t, *lines, 1)

	// Requests that pass add nothing; other clients have their own count
	decided(e, request("GET", "/mytopic", "192.0.2.2:1"))
	decided(e, request("GET", "/sp_x", "192.0.2.2:1"))
	require.Len(t, *lines, 1)

	// A window later, a client that keeps going is reported again
	*now = now.Add(10 * time.Minute)
	for range 3 {
		decided(e, request("GET", "/sp_x", "192.0.2.1:1"))
	}
	require.Equal(t, "2026-11-09T12:10:00Z 192.0.2.1 192.0.2.1/32 abuse", (*lines)[1])
}

func TestBans_StrikesLeakOverTheWindow(t *testing.T) {
	// threshold strikes within window: 10 per 10m, so one strike leaks per minute
	e, now, lines := testEngineBans(t, testBans+"rules: [{name: abuse, action: block, strike: 4}]")
	for range 2 {
		decided(e, request("GET", "/", "192.0.2.1:1"))
	}
	*now = now.Add(4 * time.Minute) // 8 - 4 = 4 left
	for range 1 {
		decided(e, request("GET", "/", "192.0.2.1:1"))
	}
	require.Empty(t, *lines) // 8
	decided(e, request("GET", "/", "192.0.2.1:1"))
	require.Len(t, *lines, 1) // 12
}

func TestBans_LimitStrikesPerLimitedRequest(t *testing.T) {
	e, _, lines := testEngineBans(t, testBans+"rules: [{name: limit, action: limit, rate: 1/h, burst: 4, strike: 2}]")
	for range 5 { // Within the limit: no strikes
		require.Equal(t, "", decided(e, request("GET", "/", "192.0.2.1:1")))
	}
	for range 5 {
		require.Equal(t, "limit", decided(e, request("GET", "/", "192.0.2.1:1")))
	}
	require.Empty(t, *lines) // 10 strikes
	decided(e, request("GET", "/", "192.0.2.1:1"))
	require.Equal(t, []string{"2026-11-09T12:00:00Z 192.0.2.1 192.0.2.1/32 limit"}, *lines)
}

func TestBans_ResponseRules(t *testing.T) {
	e, _, lines := testEngineBans(t, testBans+`
rules:
  - {name: bad-requests, on: response, match: 'status == 400 && upstream_status == 0', strike: 6}
  - {name: errors, on: response, match: 'status >= 400', strike: 1}
`)
	// Response rules decide nothing
	require.Equal(t, "", decided(e, request("GET", "/", "192.0.2.1:1")))

	// Every response rule that matches adds its strikes
	env := match.NewResponseEnv(match.NewEnv(request("GET", "/", "192.0.2.1:1")))
	env.Status = 400
	e.CheckResponse(env)
	require.Empty(t, *lines) // 7
	e.CheckResponse(env)
	require.Equal(t, []string{"2026-11-09T12:00:00Z 192.0.2.1 192.0.2.1/32 bad-requests"}, *lines) // 13: the line names the rule that crossed the threshold

	// From upstreams: only "errors"
	env = match.NewResponseEnv(match.NewEnv(request("GET", "/", "192.0.2.2:1")))
	env.Status, env.UpstreamStatus = 400, 400
	for range 10 {
		e.CheckResponse(env)
	}
	require.Len(t, *lines, 1)
	e.CheckResponse(env)
	require.Equal(t, "2026-11-09T12:00:00Z 192.0.2.2 192.0.2.2/32 errors", (*lines)[1])
}

func TestBans_PrefixIsClientKey(t *testing.T) {
	e, _, lines := testEngineBans(t, testBans+"rules: [{name: abuse, action: block, strike: 6}]")
	decided(e, request("GET", "/", "[2001:db8:1:2::1]:1"))
	decided(e, request("GET", "/", "[2001:db8:1:2::ffff]:1")) // Same /64
	require.Equal(t, []string{"2026-11-09T12:00:00Z 2001:db8:1:2::ffff 2001:db8:1:2::/64 abuse"}, *lines)

	e, _, lines = testEngineBans(t, testBans+"client-key: {ipv4-prefix-bits: 24}\nrules: [{name: abuse, action: block, strike: 6}]")
	decided(e, request("GET", "/", "192.0.2.1:1"))
	decided(e, request("GET", "/", "192.0.2.200:1"))
	require.Equal(t, []string{"2026-11-09T12:00:00Z 192.0.2.200 192.0.2.0/24 abuse"}, *lines)
}

func TestBans_UnparseableAddress(t *testing.T) {
	e, _, lines := testEngineBans(t, testBans+"rules: [{name: abuse, action: block, strike: 20}]")
	decided(e, request("GET", "/", "garbage"))
	require.Empty(t, *lines) // Nothing to ban
}

func TestBans_UpdateKeepsStrikes(t *testing.T) {
	rules := "rules: [{name: abuse, action: block, strike: 6}]"
	e, _, lines := testEngineBans(t, testBans+rules)
	decided(e, request("GET", "/", "192.0.2.1:1"))

	// Same bans settings: the strikes stay, also with other rules
	e.Update(testConfig(t, testBans+"rules: [{name: other, action: block, match: 'path == \"/x\"', strike: 1}, {name: abuse, action: block, strike: 6}]"))
	decided(e, request("GET", "/", "192.0.2.1:1"))
	require.Len(t, *lines, 1)

	// Other bans settings start over
	e.Update(testConfig(t, "bans: {file: /tmp/ban.log, window: 10m, threshold: 11}\n"+rules))
	decided(e, request("GET", "/", "192.0.2.3:1"))
	decided(e, request("GET", "/", "192.0.2.1:1"))
	require.Len(t, *lines, 1)

	// No bans section: no strikes counted
	e.Update(testConfig(t, "rules: [{name: abuse, action: block}]"))
	for range 5 {
		decided(e, request("GET", "/", "192.0.2.1:1"))
	}
	e.CheckResponse(match.NewResponseEnv(match.NewEnv(request("GET", "/", "192.0.2.1:1"))))
	require.Len(t, *lines, 1)
}

func TestBans_SweepForgetsRecoveredClients(t *testing.T) {
	e, now, _ := testEngineBans(t, testBans+"rules: [{name: abuse, action: block, strike: 1}]")
	for range 50 {
		decided(e, request("GET", "/", "192.0.2.1:1"))
	}
	for range 5 {
		decided(e, request("GET", "/", "192.0.2.2:1"))
	}
	b := e.state.Load().bans
	require.Len(t, b.clients, 2)

	// A client stays until its strikes leaked away and its ban window passed
	*now = now.Add(sweepInterval)
	decided(e, request("GET", "/", "192.0.2.3:1"))
	require.Len(t, b.clients, 3)
	*now = now.Add(time.Hour)
	decided(e, request("GET", "/", "192.0.2.3:1"))
	require.Len(t, b.clients, 1)
}

func TestBans_UpdateWithNewFile(t *testing.T) {
	// A reload that only moves the ban file writes there from then on
	conf := testConfig(t, "bans: {file: /tmp/old.log, threshold: 1}\nrules: [{name: abuse, action: block, strike: 2}]")
	var files []string
	e := New(conf, func(file, _ string) {
		files = append(files, file)
	})
	e.Update(testConfig(t, "bans: {file: /tmp/new.log, threshold: 1}\nrules: [{name: abuse, action: block, strike: 2}]"))
	decided(e, request("GET", "/", "192.0.2.1:1"))
	require.Equal(t, []string{"/tmp/new.log"}, files)
}
