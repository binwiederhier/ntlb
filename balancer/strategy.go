package balancer

import (
	"math/rand/v2"
	"slices"
	"sync/atomic"

	"heckel.io/ntlb/config"
)

// sortFor sorts some of a site's upstreams (in config order) by the site's balance, best first.
// turn is the site's round-robin turn for this pick (see nextTurn), for round-robin and least-conn.
func sortFor(site *config.Site, key []byte, turn uint64, upstreams []*Upstream) {
	switch site.Upstream.Balance {
	case config.BalanceHash:
		byName := make(map[string]*Upstream, len(upstreams))
		names := make([]string, len(upstreams))
		weights := make([]int, len(upstreams))
		for i, u := range upstreams {
			byName[u.name], names[i], weights[i] = u, u.name, u.weight
		}
		for i, name := range rankWeighted(key, names, weights) {
			upstreams[i] = byName[name]
		}
	case config.BalanceRoundRobin:
		rotate(upstreams, turn)
	case config.BalanceLeastConn:
		rotate(upstreams, turn) // Equals in turn
		sortByLoad(upstreams)
	case config.BalanceRandomTwo:
		sortRandomTwo(upstreams)
	}
}

// nextTurn returns the site's next round-robin turn
func (b *Balancer) nextTurn(site string) uint64 {
	turn, ok := b.turns.Load(site)
	if !ok {
		turn, _ = b.turns.LoadOrStore(site, &atomic.Uint64{})
	}
	return turn.(*atomic.Uint64).Add(1) - 1
}

// rotate turns the upstreams so that the turn's one comes first, the others after it in order.
// Over a round of turns (the sum of the weights), each upstream comes first weight times.
func rotate(upstreams []*Upstream, turn uint64) {
	if len(upstreams) < 2 {
		return
	}
	total := 0
	for _, u := range upstreams {
		total += u.weight
	}
	pos, first := int(turn%uint64(total)), 0
	for pos >= upstreams[first].weight {
		pos -= upstreams[first].weight
		first++
	}
	slices.Reverse(upstreams) // Rotating left by first: reverse all, then both parts
	k := len(upstreams) - first
	slices.Reverse(upstreams[:k])
	slices.Reverse(upstreams[k:])
}

// sortRandomTwo puts the less busy (per weight) of two upstreams drawn by weight first, the other
// second, and the rest by load: nginx's "random two least_conn"
func sortRandomTwo(upstreams []*Upstream) {
	if len(upstreams) < 2 {
		return
	}
	rest := slices.Clone(upstreams)
	i := drawByWeight(rest)
	first := rest[i]
	rest = slices.Delete(rest, i, i+1)
	j := drawByWeight(rest)
	second := rest[j]
	rest = slices.Delete(rest, j, j+1)
	if lessBusy(second.load(), second.weight, first.load(), first.weight) {
		first, second = second, first
	}
	sortByLoad(rest)
	upstreams[0], upstreams[1] = first, second
	copy(upstreams[2:], rest)
}

// drawByWeight returns the index of a random upstream, each drawn with its weight's share
func drawByWeight(upstreams []*Upstream) int {
	total := 0
	for _, u := range upstreams {
		total += u.weight
	}
	pos := rand.N(total)
	for i, u := range upstreams {
		if pos < u.weight {
			return i
		}
		pos -= u.weight
	}
	return len(upstreams) - 1 // Not reached
}

// sortByLoad orders upstreams by their open requests per weight, keeping the order of equals
func sortByLoad(upstreams []*Upstream) {
	loads := make(map[*Upstream]int, len(upstreams))
	for _, u := range upstreams {
		loads[u] = u.load()
	}
	slices.SortStableFunc(upstreams, func(a, b *Upstream) int {
		switch {
		case lessBusy(loads[a], a.weight, loads[b], b.weight):
			return -1
		case lessBusy(loads[b], b.weight, loads[a], a.weight):
			return 1
		}
		return 0
	})
}

// lessBusy reports whether load a per weight a is below load b per weight b, like nginx's
// least_conn with weights
func lessBusy(loadA, weightA, loadB, weightB int) bool {
	return loadA*weightB < loadB*weightA
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
