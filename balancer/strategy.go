package balancer

import (
	"math/rand/v2"
	"slices"
	"sync/atomic"

	"heckel.io/ntlb/config"
)

// sortFor sorts a site's healthy upstreams (in config order) by the site's balance, best first
func (b *Balancer) sortFor(site *config.Site, key []byte, healthy []*Upstream) {
	switch site.Balance {
	case config.BalanceHash:
		byName := make(map[string]*Upstream, len(healthy))
		names := make([]string, len(healthy))
		for i, u := range healthy {
			byName[u.name], names[i] = u, u.name
		}
		for i, name := range rank(key, names) {
			healthy[i] = byName[name]
		}
	case config.BalanceRoundRobin:
		b.rotate(site.Name, healthy)
	case config.BalanceLeastConn:
		b.rotate(site.Name, healthy) // Equals in turn
		sortByLoad(healthy)
	case config.BalanceRandomTwo:
		if len(healthy) < 2 {
			return
		}
		i, j := rand.N(len(healthy)), rand.N(len(healthy)-1)
		if j >= i {
			j++
		}
		if healthy[j].load() < healthy[i].load() {
			i, j = j, i
		}
		first, second := healthy[i], healthy[j]
		rest := slices.DeleteFunc(slices.Clone(healthy), func(u *Upstream) bool {
			return u == first || u == second
		})
		sortByLoad(rest)
		healthy[0], healthy[1] = first, second
		copy(healthy[2:], rest)
	}
}

// rotate turns the upstreams so that the site's next one comes first
func (b *Balancer) rotate(site string, upstreams []*Upstream) {
	if len(upstreams) < 2 {
		return
	}
	turn, ok := b.turns.Load(site)
	if !ok {
		turn, _ = b.turns.LoadOrStore(site, &atomic.Uint64{})
	}
	n := turn.(*atomic.Uint64).Add(1) - 1
	slices.Reverse(upstreams) // Rotating left by k: reverse all, then both parts
	k := len(upstreams) - int(n%uint64(len(upstreams)))
	slices.Reverse(upstreams[:k])
	slices.Reverse(upstreams[k:])
}

// sortByLoad orders upstreams by their open requests, keeping the order of equals
func sortByLoad(upstreams []*Upstream) {
	loads := make(map[*Upstream]int, len(upstreams))
	for _, u := range upstreams {
		loads[u] = u.load()
	}
	slices.SortStableFunc(upstreams, func(a, b *Upstream) int {
		return loads[a] - loads[b]
	})
}

// load is the upstream's open requests on this load balancer: attached, and reserved attempts
// that did not attach yet (a burst of new requests counts before any of them is answered)
func (u *Upstream) load() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	n := 0
	for _, c := range u.addrStreams {
		n += c
	}
	for _, c := range u.addrPending {
		n += c
	}
	return n
}
