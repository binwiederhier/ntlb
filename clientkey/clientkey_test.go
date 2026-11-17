package clientkey

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOf(t *testing.T) {
	v4a := Of(netip.MustParseAddr("1.2.3.4"), 32, 64)
	v4b := Of(netip.MustParseAddr("1.2.3.5"), 32, 64)
	v4mapped := Of(netip.MustParseAddr("::ffff:1.2.3.4"), 32, 64)
	v6a := Of(netip.MustParseAddr("2001:db8:1:2::1"), 32, 64)
	v6b := Of(netip.MustParseAddr("2001:db8:1:2:ffff:eeee:dddd:cccc"), 32, 64)
	v6c := Of(netip.MustParseAddr("2001:db8:1:3::1"), 32, 64)
	require.NotEqual(t, v4a, v4b)
	require.Equal(t, v4a, v4mapped)
	require.Equal(t, v6a, v6b) // Same /64, like ntfy's visitor-prefix-bits-ipv6
	require.NotEqual(t, v6a, v6c)
}

func TestOf_PrefixBits(t *testing.T) {
	key := func(s string, v4, v6 int) []byte {
		return Of(netip.MustParseAddr(s), v4, v6)
	}
	require.Equal(t, key("10.1.2.3", 24, 64), key("10.1.2.200", 24, 64))
	require.NotEqual(t, key("10.1.2.3", 24, 64), key("10.1.3.3", 24, 64))
	require.Equal(t, key("10.1.2.3", 20, 64), key("10.1.15.3", 20, 64)) // Bits are masked, not just bytes
	require.NotEqual(t, key("10.1.2.3", 20, 64), key("10.1.16.3", 20, 64))
	require.NotEqual(t, key("2001:db8::1", 32, 128), key("2001:db8::2", 32, 128))
	require.Equal(t, key("2001:db8:1::1", 32, 48), key("2001:db8:1:ffff::1", 32, 48))
	require.NotEqual(t, key("2001:db8:1::1", 32, 48), key("2001:db8:2::1", 32, 48))
}

func TestOf_InvalidPrefixBits(t *testing.T) {
	// Out-of-range prefix bits fall back to the whole address
	require.Equal(t, []byte{1, 2, 3, 4}, Of(netip.MustParseAddr("1.2.3.4"), 33, 64))
	require.Len(t, Of(netip.MustParseAddr("2001:db8::1"), 32, 129), 16)
}

func TestOf_InvalidAddr(t *testing.T) {
	// net/http always sets a valid remote address; an invalid one must not panic
	require.Empty(t, Of(netip.Addr{}, 32, 64))
}

func TestParse(t *testing.T) {
	require.Equal(t, netip.MustParseAddr("203.0.113.9"), Parse("203.0.113.9:1234"))
	require.Equal(t, netip.MustParseAddr("203.0.113.9"), Parse("[::ffff:203.0.113.9]:1234"))
	require.Equal(t, netip.MustParseAddr("2001:db8::1"), Parse("[2001:db8::1]:443"))
	require.False(t, Parse("garbage").IsValid())
}
