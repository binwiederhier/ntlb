package rules

import (
	"fmt"
	"net/netip"
	"sync"
	"time"

	"heckel.io/ntlb/config"
)

// bans counts strikes per client prefix (the client key) in a leaky bucket that holds threshold
// strikes and leaks threshold per window. A strike that overflows it reports the client: one ban
// line, at most once per window, so a client that keeps going is reported again after a window.
type bans struct {
	conf           *config.Bans
	ipv4PrefixBits int
	ipv6PrefixBits int
	perSecond      float64
	clients        map[netip.Prefix]*striker
	lastSweep      time.Time
	mu             sync.Mutex // Protects clients and lastSweep
}

type striker struct {
	room     float64   // Strikes the bucket can take before it overflows
	last     time.Time // When room was last updated
	reported time.Time // Last ban line, zero if none
}

func newBans(conf *config.Bans, key *config.ClientKey) *bans {
	return &bans{
		conf:           conf,
		ipv4PrefixBits: key.IPv4PrefixBits,
		ipv6PrefixBits: key.IPv6PrefixBits,
		perSecond:      conf.Threshold / conf.Window.Seconds(),
		clients:        make(map[netip.Prefix]*striker),
	}
}

// sameAs reports whether b counts the way conf and key say, so its state can carry over a reload
func (b *bans) sameAs(conf *config.Bans, key *config.ClientKey) bool {
	return b.conf.Threshold == conf.Threshold && b.conf.Window == conf.Window &&
		b.ipv4PrefixBits == key.IPv4PrefixBits && b.ipv6PrefixBits == key.IPv6PrefixBits
}

// strike adds weight strikes for the client; it returns the ban line if the client is reported
func (b *bans) strike(addr netip.Addr, weight float64, rule string, now time.Time) (string, bool) {
	addr = addr.Unmap()
	if !addr.IsValid() {
		return "", false
	}
	bits := b.ipv6PrefixBits
	if addr.Is4() {
		bits = b.ipv4PrefixBits
	}
	prefix, _ := addr.Prefix(bits) // The config checked the bits
	b.mu.Lock()
	defer b.mu.Unlock()
	if now.Sub(b.lastSweep) >= sweepInterval {
		b.sweepNoLock(now)
	}
	s, ok := b.clients[prefix]
	if !ok {
		s = &striker{
			room: b.conf.Threshold,
			last: now,
		}
		b.clients[prefix] = s
	}
	s.leak(now, b.perSecond, b.conf.Threshold)
	s.room -= weight
	if s.room >= 0 {
		return "", false
	}
	s.room = 0
	if !s.reported.IsZero() && now.Sub(s.reported) < b.conf.Window {
		return "", false
	}
	s.reported = now
	return fmt.Sprintf("%s %s %s %s", now.UTC().Format(time.RFC3339), addr, prefix, rule), true
}

// sweepNoLock forgets clients whose strikes leaked away and whose last report is a window old
func (b *bans) sweepNoLock(now time.Time) {
	for prefix, s := range b.clients {
		s.leak(now, b.perSecond, b.conf.Threshold)
		if s.room >= b.conf.Threshold && now.Sub(s.reported) >= b.conf.Window {
			delete(b.clients, prefix)
		}
	}
	b.lastSweep = now
}

func (s *striker) leak(now time.Time, perSecond, threshold float64) {
	if elapsed := now.Sub(s.last).Seconds(); elapsed > 0 {
		s.room = min(threshold, s.room+elapsed*perSecond)
		s.last = now
	}
}
