package balancer

import (
	"math/rand/v2"
	"slices"
	"sync/atomic"

	"heckel.io/ntlb/config"
)

// sortFor sorts some of a pool's upstreams (in config order) by the pool's balance, best first.
// turn is the pool's round-robin turn for this pick (see nextTurn), for round-robin and least-conn.
func sortFor(pool *config.Upstream, key []byte, turn uint64, upstreams []*Upstream) {
	switch pool.Balance {
	case config.BalanceHash:
		sortByHash(key, upstreams)
	case config.BalanceRoundRobin:
		rotate(upstreams, turn)
	case config.BalanceLeastConn:
		rotate(upstreams, turn) // Equals in turn
		sortByLoad(upstreams)
	case config.BalanceRandomTwo:
		sortRandomTwo(upstreams)
	}
}

// nextTurn returns a pool's next round-robin turn (key: its config.Upstream.ID)
func (b *Balancer) nextTurn(key string) uint64 {
	turn, ok := b.turns.Load(key)
	if !ok {
		turn, _ = b.turns.LoadOrStore(key, &atomic.Uint64{})
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
// second, and the rest by load: nginx's "random two least_conn". The rest keep their order among
// equals.
func sortRandomTwo(upstreams []*Upstream) {
	if len(upstreams) < 2 {
		return
	}
	moveTo(upstreams, drawByWeight(upstreams), 0)
	moveTo(upstreams, 1+drawByWeight(upstreams[1:]), 1)
	if lessBusy(upstreams[1].load(), upstreams[1].weight, upstreams[0].load(), upstreams[0].weight) {
		upstreams[0], upstreams[1] = upstreams[1], upstreams[0]
	}
	sortByLoad(upstreams[2:])
}

// moveTo moves upstreams[from] to index to (to <= from), shifting the ones in between up
func moveTo(upstreams []*Upstream, from, to int) {
	u := upstreams[from]
	copy(upstreams[to+1:from+1], upstreams[to:from])
	upstreams[to] = u
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

// sortByLoad orders upstreams by their open requests per weight, keeping the order of equals.
// Each load is read once; without allocating for up to scratchSize upstreams.
func sortByLoad(upstreams []*Upstream) {
	type loaded struct {
		u    *Upstream
		load int
	}
	var scratch [scratchSize]loaded
	loads := scratch[:0]
	for _, u := range upstreams {
		loads = append(loads, loaded{u: u, load: u.load()})
	}
	slices.SortStableFunc(loads, func(a, b loaded) int {
		switch {
		case lessBusy(a.load, a.u.weight, b.load, b.u.weight):
			return -1
		case lessBusy(b.load, b.u.weight, a.load, a.u.weight):
			return 1
		}
		return 0
	})
	for i, l := range loads {
		upstreams[i] = l.u
	}
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
