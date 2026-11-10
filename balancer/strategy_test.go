package balancer

import (
	"context"
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

// firstPicks returns the first choice of n picks, by name
func firstPicks(b *Balancer, n int) map[string]int {
	picks := make(map[string]int)
	for i := range n {
		picks[b.Pick("s", keyFor(i))[0].Name()]++
	}
	return picks
}

func TestStrategy_RoundRobin(t *testing.T) {
	// In turn, whatever the client; the others follow as retries
	app1, app2, app3 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2"), newTestUpstream(t, "app3")
	b := newStrategyBalancer(t, config.BalanceRoundRobin, app1, app2, app3)
	require.Equal(t, map[string]int{"app1": 2, "app2": 2, "app3": 2}, firstPicks(b, 6))
	first := names(b.Pick("s", keyFor(1)))
	second := names(b.Pick("s", keyFor(1)))
	require.Len(t, first, 3)
	require.NotEqual(t, first[0], second[0]) // The same client too

	// An unhealthy upstream gets no turn (nor a retry), so the others split evenly
	app2.ready.Store(false)
	b.checkHealth(context.Background())
	b.checkHealth(context.Background())
	b.checkHealth(context.Background())
	require.Equal(t, map[string]int{"app1": 5, "app3": 5}, firstPicks(b, 10))
	require.NotContains(t, names(b.Pick("s", keyFor(1))), "app2")
}

func TestStrategy_LeastConn(t *testing.T) {
	// The fewest open requests first, attached or still connecting; in turn among equals
	app1, app2, app3 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2"), newTestUpstream(t, "app3")
	b := newStrategyBalancer(t, config.BalanceLeastConn, app1, app2, app3)
	require.Equal(t, map[string]int{"app1": 2, "app2": 2, "app3": 2}, firstPicks(b, 6))
	newTestSession(b, "app1")
	newTestSession(b, "app1")
	newTestSession(b, "app3")
	require.Equal(t, []string{"app2", "app3", "app1"}, names(b.Pick("s", keyFor(1))))

	// A reserved attempt counts as well: a burst of new requests spreads before any attaches
	s := b.NewSession(func(error) {})
	b.Reserve(s, b.upstream("app2"))
	b.Reserve(b.NewSession(func(error) {}), b.upstream("app2"))
	require.Equal(t, "app3", names(b.Pick("s", keyFor(1)))[0]) // 1, then app1 and app2 with 2 each
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
	require.Len(t, b.Pick("s", keyFor(1)), 3)

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
		require.Equal(t, names(b.Pick("s", keyFor(i))), names(b.Pick("s", keyFor(i))))
		require.Equal(t, names(b.Pick("s", keyFor(i))), names(other.Pick("s", keyFor(i))))
	}
	picks := firstPicks(b, 3000)
	for _, name := range []string{"app1", "app2", "app3"} {
		require.Greater(t, picks[name], 800, name)
	}
}

func TestStrategy_TurnsOfRemovedSitesForgotten(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	b := newStrategyBalancer(t, config.BalanceRoundRobin, app1, newTestUpstream(t, "app2"))
	b.Pick("s", nil)
	_, ok := b.turns.Load("s")
	require.True(t, ok)
	conf := mustParse(t, "sites: [{name: other, listen-http: ':0', upstream: {balance: round-robin, health-check: {interval: 1h, timeout: 1s}, servers: [{name: app1, addresses: ['"+app1.addr()+"']}]}}]")
	b.Update(conf)
	_, ok = b.turns.Load("s")
	require.False(t, ok)
}
