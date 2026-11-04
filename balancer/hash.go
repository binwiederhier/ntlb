package balancer

import (
	"net/netip"
	"slices"
)

const (
	// ipv6PrefixBits groups IPv6 clients by /64 when hashing, like ntfy's visitor-prefix-bits-ipv6
	// default: a client rotating through its privacy addresses stays on the node holding its limits
	ipv6PrefixBits = 64

	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211
)

// ClientKey returns the hash key for a client address: the IPv4 address, or the IPv6 /64
func ClientKey(addr netip.Addr) []byte {
	addr = addr.Unmap()
	if addr.Is4() {
		b := addr.As4()
		return b[:]
	}
	prefix, err := addr.Prefix(ipv6PrefixBits)
	if err != nil {
		b := addr.As16()
		return b[:]
	}
	b := prefix.Addr().As16()
	return b[:ipv6PrefixBits/8]
}

// rank orders upstream names by preference for a key, using rendezvous (highest random weight)
// hashing. The order depends only on the key and the names, not on the config order or the
// process, so every load balancer with the same upstream names routes a client the same way, and
// removing or adding an upstream only moves the clients that rank it first.
func rank(key []byte, names []string) []string {
	type scored struct {
		name  string
		score uint64
	}
	scores := make([]scored, len(names))
	for i, name := range names {
		scores[i] = scored{name: name, score: score(key, name)}
	}
	slices.SortFunc(scores, func(a, b scored) int {
		if a.score != b.score {
			if a.score > b.score {
				return -1
			}
			return 1
		}
		return compareStrings(a.name, b.name)
	})
	ranked := make([]string, len(scores))
	for i, s := range scores {
		ranked[i] = s.name
	}
	return ranked
}

// score hashes key and name with FNV-1a, followed by the splitmix64 finalizer, since FNV alone
// mixes the last bytes (the name) poorly into the high bits
func score(key []byte, name string) uint64 {
	h := uint64(fnvOffset64)
	for _, c := range key {
		h = (h ^ uint64(c)) * fnvPrime64
	}
	h = (h ^ 0xff) * fnvPrime64 // Separator, so key "ab"+name "c" differs from "a"+"bc"
	for i := 0; i < len(name); i++ {
		h = (h ^ uint64(name[i])) * fnvPrime64
	}
	h ^= h >> 30
	h *= 0xbf58476d1ce4e5b9
	h ^= h >> 27
	h *= 0x94d049bb133111eb
	h ^= h >> 31
	return h
}

func compareStrings(a, b string) int {
	if a < b {
		return -1
	} else if a > b {
		return 1
	}
	return 0
}
