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
		key := ClientKey(netip.AddrFrom4([4]byte{byte(i >> 16), byte(i >> 8), byte(i), 7}))
		counts[rank(key, names)[0]]++
	}
	for _, name := range names {
		require.InDelta(t, 10000, counts[name], 600, name)
	}
}

func TestClientKey(t *testing.T) {
	v4a := ClientKey(netip.MustParseAddr("1.2.3.4"))
	v4b := ClientKey(netip.MustParseAddr("1.2.3.5"))
	v4mapped := ClientKey(netip.MustParseAddr("::ffff:1.2.3.4"))
	v6a := ClientKey(netip.MustParseAddr("2001:db8:1:2::1"))
	v6b := ClientKey(netip.MustParseAddr("2001:db8:1:2:ffff:eeee:dddd:cccc"))
	v6c := ClientKey(netip.MustParseAddr("2001:db8:1:3::1"))
	require.NotEqual(t, v4a, v4b)
	require.Equal(t, v4a, v4mapped)
	require.Equal(t, v6a, v6b) // Same /64, like ntfy's visitor-prefix-bits-ipv6
	require.NotEqual(t, v6a, v6c)
}
