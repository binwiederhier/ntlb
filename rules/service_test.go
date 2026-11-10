package rules

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"heckel.io/ntlb/config"
	"heckel.io/ntlb/match"
)

const (
	testBase = "sites: [{name: s, listen-http: ':80', upstreams: [{name: a, addresses: ['1.2.3.4:80']}]}]\n"
)

func testConfig(t *testing.T, rules string) *config.Config {
	t.Helper()
	conf, err := config.Parse([]byte(testBase + rules))
	require.NoError(t, err)
	return conf
}

// testEngine is an engine on a fake clock
func testEngine(t *testing.T, rules string) (*Engine, *time.Time) {
	t.Helper()
	e, now, _ := testEngineBans(t, rules)
	return e, now
}

// testEngineBans is testEngine that also returns the ban lines it writes
func testEngineBans(t *testing.T, rules string) (*Engine, *time.Time, *[]string) {
	t.Helper()
	now := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	var lines []string
	e := New(testConfig(t, rules), func(_, line string) {
		lines = append(lines, line)
	})
	e.now = func() time.Time {
		return now
	}
	return e, &now, &lines
}

func request(method, target, remoteAddr string, headers ...string) *http.Request {
	r := httptest.NewRequest(method, target, nil)
	r.RemoteAddr = remoteAddr
	for i := 0; i+1 < len(headers); i += 2 {
		r.Header.Set(headers[i], headers[i+1])
	}
	return r
}

// decided returns the ID of the rule that decided, or "" if the request passes
func decided(e *Engine, r *http.Request) string {
	return decidedOn(e, r, "")
}

// decidedOn is decided for a request to a site
func decidedOn(e *Engine, r *http.Request, site string) string {
	env := match.NewEnv(r)
	env.Site = site
	if rule := e.Check(r, env); rule != nil {
		return rule.ID
	}
	return ""
}

func TestEngine_SiteRules(t *testing.T) {
	conf, err := config.Parse([]byte(`
bans: {file: /tmp/ban.log, threshold: 10}
rules:
  - {name: abuse, match: 'path == "/abuse"', action: block}
  - {name: exempt, match: 'ip == "192.0.2.9"', action: allow}
  - {name: errors, on: response, match: 'status >= 500', strike: 4}
sites:
  - name: a
    listen-http: ':80'
    hosts: [a.com]
    upstreams: [{name: x, addresses: ['1.2.3.4:80']}]
    rules:
      - {name: abuse, match: 'path == "/site-abuse"', action: block, strike: 11}
      - {name: limit, action: limit, rate: 1/h}
      - {name: errors, on: response, match: 'status == 404', strike: 4}
  - name: b
    listen-http: ':80'
    hosts: [b.com]
    upstreams: [{name: x, addresses: ['1.2.3.4:80']}]
    rules:
      - {name: limit, action: limit, rate: 1/h}
`))
	require.NoError(t, err)
	var lines []string
	e := New(conf, func(_, line string) {
		lines = append(lines, line)
	})

	// Global rules come first, for every site and for requests no site serves
	require.Equal(t, "abuse", decidedOn(e, request("GET", "/abuse", "192.0.2.1:1"), "a"))
	require.Equal(t, "abuse", decidedOn(e, request("GET", "/abuse", "192.0.2.1:1"), ""))

	// Then the site's own; other sites' rules do not apply
	require.Equal(t, "a/abuse", decidedOn(e, request("GET", "/site-abuse", "192.0.2.1:1"), "a"))
	require.Equal(t, []string{"2026-11-09T12:00:00Z 192.0.2.1 192.0.2.1/32 a/abuse"[len("2026-11-09T12:00:00Z "):]}, trimTimes(lines))
	require.Equal(t, "", decidedOn(e, request("GET", "/site-abuse", "192.0.2.2:1"), "b"))

	// Limits are per site
	require.Equal(t, "", decidedOn(e, request("GET", "/", "192.0.2.3:1"), "a"))
	require.Equal(t, "a/limit", decidedOn(e, request("GET", "/", "192.0.2.3:1"), "a"))
	require.Equal(t, "", decidedOn(e, request("GET", "/", "192.0.2.3:1"), "b"))

	// A global allow skips the site's rules too
	require.Equal(t, "", decidedOn(e, request("GET", "/site-abuse", "192.0.2.9:1"), "a"))

	// Response rules: global and the site's
	env := match.NewResponseEnv(match.NewEnv(request("GET", "/", "192.0.2.4:1")))
	env.Site, env.Status = "a", 404
	e.CheckResponse(env)
	e.CheckResponse(env)
	require.Len(t, lines, 1)
	env.Status = 500
	e.CheckResponse(env) // 12
	require.Len(t, lines, 2)
	require.Contains(t, lines[1], " 192.0.2.4/32 errors")
	env.Site = "b"
	env.IP = "192.0.2.5"
	env.Status = 404
	for range 5 {
		e.CheckResponse(env) // Not a's rule
	}
	require.Len(t, lines, 2)
}

func TestEngine_SiteLimitKeptOnReload(t *testing.T) {
	site := "sites: [{name: a, listen-http: ':80', upstreams: [{name: x, addresses: ['1.2.3.4:80']}], rules: [{name: limit, action: limit, rate: 1/h}]}]"
	conf, err := config.Parse([]byte(site))
	require.NoError(t, err)
	e := New(conf, nil)
	require.Equal(t, "", decidedOn(e, request("GET", "/", "192.0.2.1:1"), "a"))
	conf, err = config.Parse([]byte("rules: [{name: limit, action: limit, rate: 1/h}]\n" + site))
	require.NoError(t, err)
	e.Update(conf)
	require.Equal(t, "a/limit", decidedOn(e, request("GET", "/", "192.0.2.1:1"), "a")) // Not the global one's bucket
}

func trimTimes(lines []string) []string {
	trimmed := make([]string, len(lines))
	for i, l := range lines {
		trimmed[i] = l[len("2026-11-09T12:00:00Z "):]
	}
	return trimmed
}

func TestEngine_Match(t *testing.T) {
	// The expressions themselves are tested in package match
	e, _ := testEngine(t, `
rules:
  - name: topic
    match: path matches "^/nz_"
    action: block
  - name: spy
    action: block
    match: method in ["GET", "POST"] && (header("User-Agent") matches "(?i)ntfy-spy" || ipIn(ip, "203.0.113.0/24"))
`)
	ua := []string{"User-Agent", "ntfy/1.0"}
	require.Equal(t, "", decided(e, request("GET", "/mytopic/json", "192.0.2.1:1234", ua...)))
	require.Equal(t, "topic", decided(e, request("GET", "/nz_abc/json", "192.0.2.1:1234", ua...)))
	require.Equal(t, "spy", decided(e, request("POST", "/x", "192.0.2.1:1234", "User-Agent", "NTFY-Spy/2")))
	require.Equal(t, "spy", decided(e, request("GET", "/x", "203.0.113.9:1234", ua...)))
	require.Equal(t, "", decided(e, request("PUT", "/x", "203.0.113.9:1234", ua...)))
}

func TestEngine_RuleWithoutFieldsMatchesEverything(t *testing.T) {
	e, _ := testEngine(t, "rules: [{name: all, action: block, status: 451}]")
	r := request("GET", "/", "192.0.2.1:1")
	rule := e.Check(r, match.NewEnv(r))
	require.NotNil(t, rule)
	require.Equal(t, 451, rule.Status)
}

func TestEngine_NoRules(t *testing.T) {
	e, _ := testEngine(t, "")
	r := request("GET", "/", "192.0.2.1:1")
	require.Nil(t, e.Check(r, match.NewEnv(r)))
}

func TestEngine_Order(t *testing.T) {
	// allow ends the evaluation, so exempt clients skip the limit but not the block above it
	e, _ := testEngine(t, `
rules:
  - name: abuse
    match: path startsWith "/bad"
    action: block
  - name: exempt
    match: ipIn(ip, "10.0.0.0/8")
    action: allow
  - name: limit
    action: limit
    rate: 1/h
  - name: after-limit
    match: path startsWith "/late"
    action: block
`)
	for range 5 {
		require.Equal(t, "", decided(e, request("GET", "/x", "10.1.2.3:1")))
	}
	require.Equal(t, "abuse", decided(e, request("GET", "/bad", "10.1.2.3:1")))

	// A limit that is not exceeded lets the evaluation go on
	require.Equal(t, "after-limit", decided(e, request("GET", "/late", "192.0.2.1:1")))
	require.Equal(t, "limit", decided(e, request("GET", "/x", "192.0.2.1:1")))
}

func TestEngine_Limit(t *testing.T) {
	e, now := testEngine(t, "rules: [{name: l, action: limit, rate: 1/s, burst: 2}]")
	a := func() string {
		return decided(e, request("GET", "/", "192.0.2.1:1"))
	}

	// 1 + burst at once, like limit_req nodelay
	for range 3 {
		require.Equal(t, "", a())
	}
	require.Equal(t, "l", a())

	// Refills at the rate, up to 1 + burst
	*now = now.Add(time.Second)
	require.Equal(t, "", a())
	require.Equal(t, "l", a())
	*now = now.Add(time.Hour)
	for range 3 {
		require.Equal(t, "", a())
	}
	require.Equal(t, "l", a())

	// Limited requests do not use up tokens
	*now = now.Add(500 * time.Millisecond)
	require.Equal(t, "l", a())
	*now = now.Add(500 * time.Millisecond)
	require.Equal(t, "", a())

	// Other clients have their own bucket
	require.Equal(t, "", decided(e, request("GET", "/", "192.0.2.2:1")))
}

func TestEngine_LimitUnparseableAddress(t *testing.T) {
	e, _ := testEngine(t, "rules: [{name: l, action: limit, rate: 1/h}]")
	require.Equal(t, "", decided(e, request("GET", "/", "garbage")))
	require.Equal(t, "l", decided(e, request("GET", "/", "also-garbage")))
}

func TestEngine_LimitKeyIsHashPrefix(t *testing.T) {
	// Like routing: the IPv6 /64 by default, so privacy addresses of one client share a bucket
	e, _ := testEngine(t, "rules: [{name: l, action: limit, rate: 1/h}]")
	require.Equal(t, "", decided(e, request("GET", "/", "[2001:db8:1:2::1]:1")))
	require.Equal(t, "l", decided(e, request("GET", "/", "[2001:db8:1:2::ffff]:1")))
	require.Equal(t, "", decided(e, request("GET", "/", "[2001:db8:1:3::1]:1")))

	e, _ = testEngine(t, "client-key: {ipv4-prefix-bits: 24}\nrules: [{name: l, action: limit, rate: 1/h}]")
	require.Equal(t, "", decided(e, request("GET", "/", "192.0.2.1:1")))
	require.Equal(t, "l", decided(e, request("GET", "/", "192.0.2.99:1")))
}

func TestEngine_UpdateKeepsUnchangedLimits(t *testing.T) {
	limits := "rules: [{name: l, action: limit, rate: 1/h}, {name: m, action: limit, rate: 1/h}]"
	e, _ := testEngine(t, limits)
	require.Equal(t, "", decided(e, request("GET", "/", "192.0.2.1:1")))
	require.Equal(t, "l", decided(e, request("GET", "/", "192.0.2.1:1")))

	// Same rule: the client is still limited; a new rule in front changes nothing about that
	e.Update(testConfig(t, "rules: [{name: new, action: block, match: 'path == \"/new\"'}, {name: l, action: limit, rate: 1/h}]"))
	require.Equal(t, "l", decided(e, request("GET", "/", "192.0.2.1:1")))
	require.Equal(t, "new", decided(e, request("GET", "/new", "192.0.2.1:1")))

	// A changed rate starts over
	e.Update(testConfig(t, "rules: [{name: l, action: limit, rate: 2/h}]"))
	require.Equal(t, "", decided(e, request("GET", "/", "192.0.2.1:1")))

	// A changed client key starts over too
	require.Equal(t, "l", decided(e, request("GET", "/", "192.0.2.1:1")))
	e.Update(testConfig(t, "client-key: {ipv4-prefix-bits: 24}\nrules: [{name: l, action: limit, rate: 2/h}]"))
	require.Equal(t, "", decided(e, request("GET", "/", "192.0.2.1:1")))

	// Removing all rules lets everything pass
	e.Update(testConfig(t, ""))
	require.Equal(t, "", decided(e, request("GET", "/", "192.0.2.1:1")))
}

func TestEngine_SweepForgetsFullBuckets(t *testing.T) {
	e, now := testEngine(t, "rules: [{name: l, action: limit, rate: 1/h, burst: 9}]")
	for i := range 100 {
		decided(e, request("GET", "/", "192.0.2."+strconv.Itoa(i)+":1"))
	}
	l := e.state.Load().rules[0].limiter
	require.Len(t, l.buckets, 100)

	// Not yet refilled: kept
	*now = now.Add(sweepInterval)
	decided(e, request("GET", "/", "192.0.2.1:1"))
	require.Len(t, l.buckets, 100)

	// Refilled: forgotten, except the one that just took a token
	*now = now.Add(time.Hour)
	decided(e, request("GET", "/", "192.0.2.1:1"))
	require.Len(t, l.buckets, 1)
}

// benchRules are shaped like ntfy.sh's nginx rules today: 13 topic regexes, a user agent, the
// topic count, 6 exempt networks and a limit
// benchRules are shaped like ntfy.sh's nginx rules today: 13 topic regexes, a user agent, the
// topic count, 6 exempt networks and a limit
const (
	benchRules = `
rules:
  - name: abuse-topics
    action: block
    match: path matches "^/(alex-kaggle-|nz_|nezha|my-bets($|/)|tuna_zamplia_alerts($|/)|gunbot-contabo-|medusa-one-page-deploy-|carmen-hub-7164-firetv($|/)|whitedeck-2fa-|abrotp_z1b6lgs4qfj($|/)|BADat($|/)|BACam($|/)|spam-)"
  - name: abuse-user-agents
    action: block
    match: header("User-Agent") matches "(?i)ntfy-spy"
  - name: abuse-topic-count
    action: block
    match: path matches "^/[^/]*(,[^/,]*){150,}"
  - name: limit-exempt
    match: ipIn(ip, "116.203.112.46/32", "132.226.42.65/32", "136.243.88.169/32", "176.126.240.0/24", "188.40.204.226/32", "45.43.14.223/32")
    action: allow
  - name: limit
    action: limit
    rate: 1000000000/s # Never exceeded: measure the full walk
`
)

func BenchmarkEngine_Check(b *testing.B) {
	conf, err := config.Parse([]byte(testBase + benchRules))
	require.NoError(b, err)
	e := New(conf, nil)
	r := request("GET", "/mytopic/json?since=1730000000", "203.0.113.7:4321", "User-Agent", "ntfy/1.27.0 (Android; 14)")
	b.ReportAllocs()
	for b.Loop() {
		if e.Check(r, match.NewEnv(r)) != nil {
			b.Fatal("unexpected decision")
		}
	}
}

func BenchmarkEngine_CheckParallel(b *testing.B) {
	conf, err := config.Parse([]byte(testBase + benchRules))
	require.NoError(b, err)
	e := New(conf, nil)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		r := request("GET", "/mytopic/json", "203.0.113.7:4321", "User-Agent", "ntfy/1.27.0 (Android; 14)")
		for pb.Next() {
			e.Check(r, match.NewEnv(r))
		}
	})
}

func TestEngine_RedirectAndStaticDecide(t *testing.T) {
	e, _ := testEngine(t, `
rules:
  - {name: docs, match: 'path startsWith "/docs"', action: redirect, location: '"https://docs.ntfy.sh/"'}
  - {name: site, match: 'path == "/"', action: static, root: /srv}
`)
	require.Equal(t, "docs", decided(e, request("GET", "/docs/x", "192.0.2.1:1")))
	require.Equal(t, "site", decided(e, request("GET", "/", "192.0.2.1:1")))
	require.Equal(t, "", decided(e, request("GET", "/mytopic", "192.0.2.1:1")))
}

func TestBans_PerSite(t *testing.T) {
	// A site with its own bans settings counts its requests' strikes there (global rules' too),
	// writing to its own file; other sites use the global ones
	conf, err := config.Parse([]byte(`
bans: {file: /tmp/ban.log, threshold: 10}
rules: [{name: abuse, match: 'path == "/abuse"', action: block, strike: 6}]
sites:
  - {name: a, listen-http: ':80', hosts: [a.com], bans: {file: /tmp/a-ban.log, threshold: 1}, upstreams: [{name: x, addresses: ['1.2.3.4:80']}]}
  - {name: b, listen-http: ':80', upstreams: [{name: x, addresses: ['1.2.3.4:80']}]}
`))
	require.NoError(t, err)
	var bans []string
	e := New(conf, func(file, line string) {
		bans = append(bans, file+" "+line[len("2026-11-09T12:00:00Z "):])
	})
	e.now = func() time.Time {
		return time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	}
	decidedOn(e, request("GET", "/abuse", "192.0.2.1:1"), "a") // 6 > 1
	require.Equal(t, []string{"/tmp/a-ban.log 192.0.2.1 192.0.2.1/32 abuse"}, bans)
	decidedOn(e, request("GET", "/abuse", "192.0.2.1:1"), "b") // 6, separate count
	require.Len(t, bans, 1)
	decidedOn(e, request("GET", "/abuse", "192.0.2.1:1"), "") // 12 > 10 globally
	require.Equal(t, "/tmp/ban.log 192.0.2.1 192.0.2.1/32 abuse", bans[1])

	// Response rules too
	env := match.NewResponseEnv(match.NewEnv(request("GET", "/", "192.0.2.2:1")))
	env.Site = "a"
	conf.Rules = append(conf.Rules, &config.Rule{ID: "errors", Name: "errors", On: config.OnResponse, Strike: 2})
	e.Update(conf)
	e.CheckResponse(env)
	require.Equal(t, "/tmp/a-ban.log 192.0.2.2 192.0.2.2/32 errors", bans[2])

	// A reload with the same site bans keeps its strikes: 192.0.2.2 was reported in this window
	e.Update(conf)
	e.CheckResponse(env)
	require.Len(t, bans, 3)
}

func TestEngine_TrackedClientsCapped(t *testing.T) {
	// Spraying client keys (an IPv6 /48 has 65k /64s) must not grow the limit buckets without
	// bound: over the cap, a new client goes untracked (and passes) until a sweep makes room
	defer func(old int) {
		maxTracked = old
	}(maxTracked)
	maxTracked = 3
	e, now := testEngine(t, "rules: [{name: limit, action: limit, rate: 1/h}]")
	for i := range 3 {
		decided(e, request("GET", "/", "[2001:db8:0:"+strconv.Itoa(i)+"::1]:1"))
	}
	limiter := e.state.Load().rules[0].limiter
	require.Len(t, limiter.buckets, 3)
	for range 3 {
		require.Equal(t, "", decided(e, request("GET", "/", "[2001:db8:0:99::1]:1"))) // Untracked: never limited
	}
	require.Len(t, limiter.buckets, 3)

	// At the cap, a new key sweeps at most every fullSweepInterval: a full sweep of 1M buckets
	// takes ~60 ms under the lock, which every new key of a spray would pay otherwise
	swept := limiter.lastSweep
	*now = now.Add(time.Second)
	decided(e, request("GET", "/", "[2001:db8:0:98::1]:1"))
	require.Equal(t, swept, limiter.lastSweep)
	*now = now.Add(fullSweepInterval)
	decided(e, request("GET", "/", "[2001:db8:0:98::1]:1"))
	require.NotEqual(t, swept, limiter.lastSweep)

	// Once the old buckets refilled, a sweep makes room again
	*now = now.Add(2 * time.Hour)
	decided(e, request("GET", "/", "[2001:db8:0:99::1]:1"))
	require.Equal(t, "limit", decided(e, request("GET", "/", "[2001:db8:0:99::1]:1")))
}

func TestBans_TrackedClientsCapped(t *testing.T) {
	defer func(old int) {
		maxTracked = old
	}(maxTracked)
	maxTracked = 2
	e, now, lines := testEngineBans(t, testBans+"rules: [{name: abuse, action: block, strike: 11}]")
	decided(e, request("GET", "/", "192.0.2.1:1"))
	decided(e, request("GET", "/", "192.0.2.2:1"))
	decided(e, request("GET", "/", "192.0.2.3:1")) // Over the cap: not counted
	require.Len(t, *lines, 2)
	b := e.state.Load().bans
	require.Len(t, b.clients, 2)

	// Reported clients stay for their whole window, so a spray keeps the map full: sweep at most
	// every fullSweepInterval then
	swept := b.lastSweep
	*now = now.Add(time.Second)
	decided(e, request("GET", "/", "192.0.2.4:1"))
	require.Equal(t, swept, b.lastSweep)
	*now = now.Add(fullSweepInterval)
	decided(e, request("GET", "/", "192.0.2.4:1"))
	require.NotEqual(t, swept, b.lastSweep)
}
