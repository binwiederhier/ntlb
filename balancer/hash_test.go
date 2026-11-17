package balancer

import (
	"fmt"
	"net/netip"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"heckel.io/ntlb/clientkey"
)

func TestRank_IndependentOfConfigOrder(t *testing.T) {
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("10.0.%d.%d", i/256, i%256))
		a := rank(key, []string{"app1", "app2", "app3"})
		b := rank(key, []string{"app3", "app1", "app2"})
		require.Equal(t, a, b)
		require.Len(t, a, 3)
	}
}

func TestRank_MinimalDisruption(t *testing.T) {
	// Removing one upstream moves only its own keys; adding one moves keys only to the new one
	before := []string{"app1", "app2", "app3", "app4"}
	removed := []string{"app1", "app2", "app4"}
	added := []string{"app1", "app2", "app3", "app4", "app5"}
	movedRemove, movedAdd := 0, 0
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		top := rank(key, before)[0]
		if top == "app3" {
			movedRemove++
		} else {
			require.Equal(t, top, rank(key, removed)[0])
		}
		if topAdded := rank(key, added)[0]; topAdded != top {
			require.Equal(t, "app5", topAdded)
			movedAdd++
		}
	}
	require.InDelta(t, 2500, movedRemove, 300)
	require.InDelta(t, 2000, movedAdd, 300)
}

func TestRank_Distribution(t *testing.T) {
	names := []string{"app1", "app2", "app3"}
	counts := make(map[string]int)
	for i := 0; i < 30000; i++ {
		key := clientkey.Of(netip.AddrFrom4([4]byte{byte(i >> 16), byte(i >> 8), byte(i), 7}), 32, 64)
		counts[rank(key, names)[0]]++
	}
	for _, name := range names {
		require.InDelta(t, 10000, counts[name], 600, name)
	}
}

func TestRank_Golden(t *testing.T) {
	// Pins the hash: two load balancers with different hash functions disagree on every client,
	// so a change here must be deliberate (and rolled out to all of them at once)
	names := []string{"app1", "app2", "app3"}
	tests := map[string][]string{}
	for _, ip := range []string{"1.2.3.4", "10.0.0.1", "192.168.1.1", "2001:db8::1", "2a01:4f8:1:2::5"} {
		tests[ip] = rank(clientkey.Of(netip.MustParseAddr(ip), 32, 64), names)
	}
	require.Equal(t, map[string][]string{
		"1.2.3.4":         {"app3", "app2", "app1"},
		"10.0.0.1":        {"app3", "app2", "app1"},
		"192.168.1.1":     {"app1", "app3", "app2"},
		"2001:db8::1":     {"app3", "app1", "app2"},
		"2a01:4f8:1:2::5": {"app2", "app3", "app1"},
	}, tests)
}

func TestRank_EdgeCases(t *testing.T) {
	key := []byte{10, 0, 0, 1}
	require.Empty(t, rank(key, nil))
	require.Equal(t, []string{"app1"}, rank(key, []string{"app1"}))

	// Equal scores (here: the same name twice) fall back to the name, so the order is stable
	ranked := rank(key, []string{"app2", "app1", "app2"})
	require.Len(t, ranked, 3)
	require.ElementsMatch(t, []string{"app1", "app2", "app2"}, ranked)
	require.Equal(t, rank(key, []string{"app1", "app2", "app2"}), ranked)
	if ranked[0] == "app2" {
		require.Equal(t, "app2", ranked[1])
	} else {
		require.Equal(t, []string{"app2", "app2"}, ranked[1:])
	}
}

func TestScore_Separator(t *testing.T) {
	// The separator keeps key and name apart: "ab"+"c" and "a"+"bc" score differently
	require.NotEqual(t, score([]byte("ab"), "c"), score([]byte("a"), "bc"))
	require.Equal(t, score([]byte("ab"), "c"), score([]byte("ab"), "c"))
}

func FuzzRankWeighted(f *testing.F) {
	// The ranking is a permutation of the names that does not depend on their order
	f.Add([]byte{203, 0, 113, 9}, "app1,app2,app3", uint8(3))
	f.Fuzz(func(t *testing.T, key []byte, list string, w uint8) {
		names := slices.Compact(slices.Sorted(strings.SplitSeq(list, ",")))
		weights := make([]int, len(names))
		for i := range weights {
			weights[i] = 1 + (int(w)+i)%7
		}
		ranked := rankWeighted(key, names, weights)
		require.ElementsMatch(t, names, ranked)
		reversed, reversedWeights := slices.Clone(names), slices.Clone(weights)
		slices.Reverse(reversed)
		slices.Reverse(reversedWeights)
		require.Equal(t, ranked, rankWeighted(key, reversed, reversedWeights))
	})
}

// rank is rankWeighted without weights
func rank(key []byte, names []string) []string {
	return rankWeighted(key, names, nil)
}

// rankWeighted is sortByHash on upstreams with the names and weights (nil: all 1)
func rankWeighted(key []byte, names []string, weights []int) []string {
	upstreams := make([]*Upstream, len(names))
	for i, name := range names {
		upstreams[i] = &Upstream{name: name, weight: 1}
		if weights != nil {
			upstreams[i].weight = weights[i]
		}
	}
	sortByHash(key, upstreams)
	ranked := make([]string, len(upstreams))
	for i, u := range upstreams {
		ranked[i] = u.name
	}
	return ranked
}
