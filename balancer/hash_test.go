package balancer

import (
	"fmt"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"
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
		key := ClientKey(netip.AddrFrom4([4]byte{byte(i >> 16), byte(i >> 8), byte(i), 7}), 32, 64)
		counts[rank(key, names)[0]]++
	}
	for _, name := range names {
		require.InDelta(t, 10000, counts[name], 600, name)
	}
}

func TestClientKey(t *testing.T) {
	v4a := ClientKey(netip.MustParseAddr("1.2.3.4"), 32, 64)
	v4b := ClientKey(netip.MustParseAddr("1.2.3.5"), 32, 64)
	v4mapped := ClientKey(netip.MustParseAddr("::ffff:1.2.3.4"), 32, 64)
	v6a := ClientKey(netip.MustParseAddr("2001:db8:1:2::1"), 32, 64)
	v6b := ClientKey(netip.MustParseAddr("2001:db8:1:2:ffff:eeee:dddd:cccc"), 32, 64)
	v6c := ClientKey(netip.MustParseAddr("2001:db8:1:3::1"), 32, 64)
	require.NotEqual(t, v4a, v4b)
	require.Equal(t, v4a, v4mapped)
	require.Equal(t, v6a, v6b) // Same /64, like ntfy's visitor-prefix-bits-ipv6
	require.NotEqual(t, v6a, v6c)
}

func TestClientKey_PrefixBits(t *testing.T) {
	key := func(s string, v4, v6 int) []byte {
		return ClientKey(netip.MustParseAddr(s), v4, v6)
	}
	require.Equal(t, key("10.1.2.3", 24, 64), key("10.1.2.200", 24, 64))
	require.NotEqual(t, key("10.1.2.3", 24, 64), key("10.1.3.3", 24, 64))
	require.Equal(t, key("10.1.2.3", 20, 64), key("10.1.15.3", 20, 64)) // Bits are masked, not just bytes
	require.NotEqual(t, key("10.1.2.3", 20, 64), key("10.1.16.3", 20, 64))
	require.NotEqual(t, key("2001:db8::1", 32, 128), key("2001:db8::2", 32, 128))
	require.Equal(t, key("2001:db8:1::1", 32, 48), key("2001:db8:1:ffff::1", 32, 48))
	require.NotEqual(t, key("2001:db8:1::1", 32, 48), key("2001:db8:2::1", 32, 48))
}

func TestRank_Golden(t *testing.T) {
	// Pins the hash: two load balancers with different hash functions disagree on every client,
	// so a change here must be deliberate (and rolled out to all of them at once)
	names := []string{"app1", "app2", "app3"}
	tests := map[string][]string{}
	for _, ip := range []string{"1.2.3.4", "10.0.0.1", "192.168.1.1", "2001:db8::1", "2a01:4f8:1:2::5"} {
		tests[ip] = rank(ClientKey(netip.MustParseAddr(ip), 32, 64), names)
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

func TestClientKey_InvalidPrefixBits(t *testing.T) {
	// Out-of-range prefix bits fall back to the whole address
	require.Equal(t, []byte{1, 2, 3, 4}, ClientKey(netip.MustParseAddr("1.2.3.4"), 33, 64))
	require.Len(t, ClientKey(netip.MustParseAddr("2001:db8::1"), 32, 129), 16)
}
