package balancer

import (
	"net/netip"
	"slices"
	"strings"
)

const (
	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211
)

// ClientKey returns the hash key for a client address: the IPv4 address shortened to
// ipv4PrefixBits, or the IPv6 address shortened to ipv6PrefixBits (with 64, a client rotating
// through its privacy addresses stays on the node holding its rate limits). The key is the
// masked prefix, cut to whole bytes; with the defaults of 32 and 64 it is 4 or 8 bytes.
func ClientKey(addr netip.Addr, ipv4PrefixBits, ipv6PrefixBits int) []byte {
	addr = addr.Unmap()
	bits := ipv6PrefixBits
	if addr.Is4() {
		bits = ipv4PrefixBits
	}
	prefix, err := addr.Prefix(bits)
	if err != nil {
		return addr.AsSlice()
	}
	return prefix.Addr().AsSlice()[:(bits+7)/8]
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
		return strings.Compare(a.name, b.name)
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
