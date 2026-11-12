package balancer

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"heckel.io/ntlb/config"
)

func newStrategyBalancer(t *testing.T, balance string, upstreams ...*testUpstream) *Balancer {
	t.Helper()
	b := New(newTestConfigBalance(balance, upstreams...))
	b.checkHealth(context.Background())
	return b
}

// newWeightedBalancer is a site "s" with the balance and the upstreams, each with its settings
// (e.g. "weight: 3" or "backup: true", "" for none)
func newWeightedBalancer(t *testing.T, balance string, upstreams []*testUpstream, settings []string) *Balancer {
	t.Helper()
	var b strings.Builder
	b.WriteString("sites:\n  - name: s\n    listen-http: ':0'\n    upstreams:\n      default:\n        balance: " + balance + "\n        healthcheck: {interval: 1h, timeout: 1s}\n        servers:\n")
	for i, u := range upstreams {
		fmt.Fprintf(&b, "        - {name: %s, addresses: [%q]", u.name, u.addr())
		if settings[i] != "" {
			b.WriteString(", " + settings[i])
		}
		b.WriteString("}\n")
	}
	bal := New(mustParse(t, b.String()))
	bal.checkHealth(context.Background())
	return bal
}

// firstPicks returns the first choice of n picks, by name
func firstPicks(b *Balancer, n int) map[string]int {
	picks := make(map[string]int)
	for i := range n {
		picks[b.Pick("s", "", keyFor(i))[0].Name()]++
	}
	return picks
}

func TestStrategy_RoundRobin(t *testing.T) {
	// In turn, whatever the client; the others follow as retries
	app1, app2, app3 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2"), newTestUpstream(t, "app3")
	b := newStrategyBalancer(t, config.BalanceRoundRobin, app1, app2, app3)
	require.Equal(t, map[string]int{"app1": 2, "app2": 2, "app3": 2}, firstPicks(b, 6))
	first := names(b.Pick("s", "", keyFor(1)))
	second := names(b.Pick("s", "", keyFor(1)))
	require.Len(t, first, 3)
	require.NotEqual(t, first[0], second[0]) // The same client too

	// An unhealthy upstream gets no turn (nor a retry), so the others split evenly
	app2.ready.Store(false)
	b.checkHealth(context.Background())
	b.checkHealth(context.Background())
	b.checkHealth(context.Background())
	require.Equal(t, map[string]int{"app1": 5, "app3": 5}, firstPicks(b, 10))
	require.NotContains(t, names(b.Pick("s", "", keyFor(1))), "app2")
}

func TestStrategy_LeastConn(t *testing.T) {
	// The fewest open requests first, attached or still connecting; in turn among equals
	app1, app2, app3 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2"), newTestUpstream(t, "app3")
	b := newStrategyBalancer(t, config.BalanceLeastConn, app1, app2, app3)
	require.Equal(t, map[string]int{"app1": 2, "app2": 2, "app3": 2}, firstPicks(b, 6))
	newTestSession(b, "app1")
	newTestSession(b, "app1")
	newTestSession(b, "app3")
	require.Equal(t, []string{"app2", "app3", "app1"}, names(b.Pick("s", "", keyFor(1))))

	// A reserved attempt counts as well: a burst of new requests spreads before any attaches
	s := b.NewSession(func(error) {})
	b.Reserve(s, b.upstream("app2"))
	b.Reserve(b.NewSession(func(error) {}), b.upstream("app2"))
	require.Equal(t, "app3", names(b.Pick("s", "", keyFor(1)))[0]) // 1, then app1 and app2 with 2 each
	b.Close(s)
	require.Equal(t, map[string]int{"app2": 1, "app3": 1}, firstPicks(b, 2)) // 1 each now: in turn
}

func TestStrategy_RandomTwo(t *testing.T) {
	// The less busy of two random upstreams: never the busiest of three, the other two about evenly
	app1, app2, app3 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2"), newTestUpstream(t, "app3")
	b := newStrategyBalancer(t, config.BalanceRandomTwo, app1, app2, app3)
	for range 5 {
		newTestSession(b, "app1")
	}
	picks := firstPicks(b, 1000)
	require.Zero(t, picks["app1"])
	require.Greater(t, picks["app2"], 350)
	require.Greater(t, picks["app3"], 350)
	require.Len(t, b.Pick("s", "", keyFor(1)), 3)

	// Two upstreams: always the less busy one
	b = newStrategyBalancer(t, config.BalanceRandomTwo, app1, app2)
	newTestSession(b, "app2")
	require.Equal(t, map[string]int{"app1": 100}, firstPicks(b, 100))
}

func TestStrategy_Hash(t *testing.T) {
	// The same client, the same order, on every load balancer with the same names
	app1, app2, app3 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2"), newTestUpstream(t, "app3")
	b := newStrategyBalancer(t, config.BalanceHash, app1, app2, app3)
	other := newStrategyBalancer(t, config.BalanceHash, app3, app2, app1)
	for i := range 100 {
		require.Equal(t, names(b.Pick("s", "", keyFor(i))), names(b.Pick("s", "", keyFor(i))))
		require.Equal(t, names(b.Pick("s", "", keyFor(i))), names(other.Pick("s", "", keyFor(i))))
	}
	picks := firstPicks(b, 3000)
	for _, name := range []string{"app1", "app2", "app3"} {
		require.Greater(t, picks[name], 800, name)
	}
}

func TestStrategy_TurnsOfRemovedSitesForgotten(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	b := newStrategyBalancer(t, config.BalanceRoundRobin, app1, newTestUpstream(t, "app2"))
	b.Pick("s", "", nil)
	_, ok := b.turns.Load("s/default")
	require.True(t, ok)
	conf := mustParse(t, "sites: [{name: other, listen-http: ':0', upstreams: {default: {balance: round-robin, healthcheck: {interval: 1h, timeout: 1s}, servers: [{name: app1, addresses: ['"+app1.addr()+"']}]}}}]")
	b.Update(conf)
	_, ok = b.turns.Load("s/default")
	require.False(t, ok)
}

func TestStrategy_WeightedRoundRobin(t *testing.T) {
	// Each server gets its weight's share of the turns, exactly
	app1, app2, app3 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2"), newTestUpstream(t, "app3")
	b := newWeightedBalancer(t, config.BalanceRoundRobin, []*testUpstream{app1, app2, app3}, []string{"weight: 3", "", "weight: 2"})
	require.Equal(t, map[string]int{"app1": 300, "app2": 100, "app3": 200}, firstPicks(b, 600))
	require.Len(t, b.Pick("s", "", keyFor(1)), 3) // The others are still the retries
}

func TestStrategy_WeightedLeastConn(t *testing.T) {
	// Open requests per weight: app1 (weight 3) with 2 open is less busy than app2 with 1
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	b := newWeightedBalancer(t, config.BalanceLeastConn, []*testUpstream{app1, app2}, []string{"weight: 3", ""})
	newTestSession(b, "app1")
	newTestSession(b, "app1")
	newTestSession(b, "app2")
	require.Equal(t, []string{"app1", "app2"}, names(b.Pick("s", "", keyFor(1))))
}

func TestStrategy_WeightedRandomTwo(t *testing.T) {
	// With no load, the two candidates are drawn by weight
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	b := newWeightedBalancer(t, config.BalanceRandomTwo, []*testUpstream{app1, app2}, []string{"weight: 9", ""})
	picks := firstPicks(b, 2000)
	require.Greater(t, picks["app1"], 1600) // ~90%; equal weights would be ~50%
}

func TestStrategy_WeightedHash(t *testing.T) {
	// Weighted rendezvous hashing: clients split by weight, every load balancer agrees, and with
	// equal weights the ranking is the unweighted one
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	b := newWeightedBalancer(t, config.BalanceHash, []*testUpstream{app1, app2}, []string{"weight: 3", ""})
	picks := firstPicks(b, 4000)
	require.InDelta(t, 3000, picks["app1"], 200)
	other := newWeightedBalancer(t, config.BalanceHash, []*testUpstream{app2, app1}, []string{"", "weight: 3"})
	equal := newWeightedBalancer(t, config.BalanceHash, []*testUpstream{app1, app2}, []string{"weight: 2", "weight: 2"})
	unweighted := newStrategyBalancer(t, config.BalanceHash, app1, app2)
	for i := range 200 {
		require.Equal(t, names(b.Pick("s", "", keyFor(i))), names(other.Pick("s", "", keyFor(i))))
		require.Equal(t, names(unweighted.Pick("s", "", keyFor(i))), names(equal.Pick("s", "", keyFor(i))))
	}
}

func TestStrategy_Backup(t *testing.T) {
	// A backup is only the last retry while any other server is healthy, and takes over when none is
	app1, app2, app3 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2"), newTestUpstream(t, "app3")
	b := newWeightedBalancer(t, config.BalanceRoundRobin, []*testUpstream{app1, app2, app3}, []string{"", "backup: true", ""})
	require.Equal(t, map[string]int{"app1": 50, "app3": 50}, firstPicks(b, 100))
	require.Equal(t, "app2", names(b.Pick("s", "", keyFor(1)))[2])

	// The others down: the backup only
	app1.ready.Store(false)
	app3.ready.Store(false)
	for range 3 {
		b.checkHealth(context.Background())
	}
	require.Equal(t, []string{"app2"}, names(b.Pick("s", "", keyFor(1))))

	// All down: fail open, the backup last
	app2.ready.Store(false)
	for range 3 {
		b.checkHealth(context.Background())
	}
	require.Equal(t, "app2", names(b.Pick("s", "", keyFor(1)))[2])
}

func TestStrategy_WeightAndBackupInStatus(t *testing.T) {
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	b := newWeightedBalancer(t, config.BalanceRoundRobin, []*testUpstream{app1, app2}, []string{"weight: 3", "backup: true"})
	status := b.Status().Upstreams
	require.Equal(t, 3, status[0].Weight)
	require.False(t, status[0].Backup)
	require.Equal(t, 1, status[1].Weight)
	require.True(t, status[1].Backup)
}
