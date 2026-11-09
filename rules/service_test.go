package rules

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"heckel.io/ntfy-lb/config"
)

const testBase = "listen-http: ':80'\nupstreams: [{name: a, addresses: ['1.2.3.4:80']}]\n"

func testConfig(t *testing.T, rules string) *config.Config {
	t.Helper()
	conf, err := config.Parse([]byte(testBase + rules))
	require.NoError(t, err)
	return conf
}

// testEngine is an engine on a fake clock
func testEngine(t *testing.T, rules string) (*Engine, *time.Time) {
	t.Helper()
	now := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	e := New(testConfig(t, rules))
	e.now = func() time.Time {
		return now
	}
	return e, &now
}

func request(method, target, remoteAddr string, headers ...string) *http.Request {
	r := httptest.NewRequest(method, target, nil)
	r.RemoteAddr = remoteAddr
	for i := 0; i+1 < len(headers); i += 2 {
		r.Header.Set(headers[i], headers[i+1])
	}
	return r
}

// decided returns the name of the rule that decided, or "" if the request passes
func decided(e *Engine, r *http.Request) string {
	if rule := e.Check(r); rule != nil {
		return rule.Name
	}
	return ""
}

func TestEngine_Match(t *testing.T) {
	// The expressions themselves are tested in package match
	e, _ := testEngine(t, `
rules:
  - name: topic
    match: path matches "^/nz_"
  - name: spy
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
	e, _ := testEngine(t, "rules: [{name: all, status: 451}]")
	rule := e.Check(request("GET", "/", "192.0.2.1:1"))
	require.NotNil(t, rule)
	require.Equal(t, 451, rule.Status)
}

func TestEngine_NoRules(t *testing.T) {
	e, _ := testEngine(t, "")
	require.Nil(t, e.Check(request("GET", "/", "192.0.2.1:1")))
}

func TestEngine_Order(t *testing.T) {
	// allow ends the evaluation, so exempt clients skip the limit but not the block above it
	e, _ := testEngine(t, `
rules:
  - name: abuse
    match: path startsWith "/bad"
  - name: exempt
    match: ipIn(ip, "10.0.0.0/8")
    action: allow
  - name: limit
    action: limit
    rate: 1/h
  - name: after-limit
    match: path startsWith "/late"
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

	e, _ = testEngine(t, "hash: {ipv4-prefix-bits: 24}\nrules: [{name: l, action: limit, rate: 1/h}]")
	require.Equal(t, "", decided(e, request("GET", "/", "192.0.2.1:1")))
	require.Equal(t, "l", decided(e, request("GET", "/", "192.0.2.99:1")))
}

func TestEngine_UpdateKeepsUnchangedLimits(t *testing.T) {
	limits := "rules: [{name: l, action: limit, rate: 1/h}, {name: m, action: limit, rate: 1/h}]"
	e, _ := testEngine(t, limits)
	require.Equal(t, "", decided(e, request("GET", "/", "192.0.2.1:1")))
	require.Equal(t, "l", decided(e, request("GET", "/", "192.0.2.1:1")))

	// Same rule: the client is still limited; a new rule in front changes nothing about that
	e.Update(testConfig(t, "rules: [{name: new, match: 'path == \"/new\"'}, {name: l, action: limit, rate: 1/h}]"))
	require.Equal(t, "l", decided(e, request("GET", "/", "192.0.2.1:1")))
	require.Equal(t, "new", decided(e, request("GET", "/new", "192.0.2.1:1")))

	// A changed rate starts over
	e.Update(testConfig(t, "rules: [{name: l, action: limit, rate: 2/h}]"))
	require.Equal(t, "", decided(e, request("GET", "/", "192.0.2.1:1")))

	// A changed client key starts over too
	require.Equal(t, "l", decided(e, request("GET", "/", "192.0.2.1:1")))
	e.Update(testConfig(t, "hash: {ipv4-prefix-bits: 24}\nrules: [{name: l, action: limit, rate: 2/h}]"))
	require.Equal(t, "", decided(e, request("GET", "/", "192.0.2.1:1")))

	// Removing all rules lets everything pass
	e.Update(testConfig(t, ""))
	require.Nil(t, e.Check(request("GET", "/", "192.0.2.1:1")))
}

func TestEngine_SweepForgetsFullBuckets(t *testing.T) {
	e, now := testEngine(t, "rules: [{name: l, action: limit, rate: 1/h, burst: 9}]")
	for i := range 100 {
		e.Check(request("GET", "/", "192.0.2."+strconv.Itoa(i)+":1"))
	}
	l := e.state.Load().rules[0].limiter
	require.Len(t, l.buckets, 100)

	// Not yet refilled: kept
	*now = now.Add(sweepInterval)
	e.Check(request("GET", "/", "192.0.2.1:1"))
	require.Len(t, l.buckets, 100)

	// Refilled: forgotten, except the one that just took a token
	*now = now.Add(time.Hour)
	e.Check(request("GET", "/", "192.0.2.1:1"))
	require.Len(t, l.buckets, 1)
}

// benchRules are shaped like ntfy.sh's nginx rules today: 13 topic regexes, a user agent, the
// topic count, 6 exempt networks and a limit
// benchRules are shaped like ntfy.sh's nginx rules today: 13 topic regexes, a user agent, the
// topic count, 6 exempt networks and a limit
const benchRules = `
rules:
  - name: abuse-topics
    match: path matches "^/(alex-kaggle-|nz_|nezha|my-bets($|/)|tuna_zamplia_alerts($|/)|gunbot-contabo-|medusa-one-page-deploy-|carmen-hub-7164-firetv($|/)|whitedeck-2fa-|abrotp_z1b6lgs4qfj($|/)|BADat($|/)|BACam($|/)|spam-)"
  - name: abuse-user-agents
    match: header("User-Agent") matches "(?i)ntfy-spy"
  - name: abuse-topic-count
    match: path matches "^/[^/]*(,[^/,]*){150,}"
  - name: limit-exempt
    match: ipIn(ip, "116.203.112.46/32", "132.226.42.65/32", "136.243.88.169/32", "176.126.240.0/24", "188.40.204.226/32", "45.43.14.223/32")
    action: allow
  - name: limit
    action: limit
    rate: 1000000000/s # Never exceeded: measure the full walk
`

func BenchmarkEngine_Check(b *testing.B) {
	conf, err := config.Parse([]byte(testBase + benchRules))
	require.NoError(b, err)
	e := New(conf)
	r := request("GET", "/mytopic/json?since=1730000000", "203.0.113.7:4321", "User-Agent", "ntfy/1.27.0 (Android; 14)")
	b.ReportAllocs()
	for b.Loop() {
		if e.Check(r) != nil {
			b.Fatal("unexpected decision")
		}
	}
}

func BenchmarkEngine_CheckParallel(b *testing.B) {
	conf, err := config.Parse([]byte(testBase + benchRules))
	require.NoError(b, err)
	e := New(conf)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		r := request("GET", "/mytopic/json", "203.0.113.7:4321", "User-Agent", "ntfy/1.27.0 (Android; 14)")
		for pb.Next() {
			e.Check(r)
		}
	})
}
