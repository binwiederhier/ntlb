package balancer

import (
	"cmp"
	"log/slog"
	"math"
	"slices"
	"time"
)

func newDrainState(now time.Time, d time.Duration, removal bool) *drainState {
	return &drainState{
		deadline: now.Add(d),
		last:     now,
		removal:  removal,
	}
}

// done returns true once the deadline passed
func (d *drainState) done(now time.Time) bool {
	return !now.Before(d.deadline)
}

// pick returns the sessions to close at this tick, oldest first (see drainState)
func (d *drainState) pick(now time.Time, sessions []*Session) []*Session {
	defer func() {
		d.last = now
	}()
	if d.done(now) {
		d.finished = true
	}
	if len(sessions) == 0 {
		return nil
	} else if d.finished {
		return sessions
	}
	elapsed, remaining := now.Sub(d.last), d.deadline.Sub(d.last)
	if elapsed <= 0 {
		return nil
	}
	d.credit += (float64(len(sessions)) - d.credit) * float64(elapsed) / float64(remaining) // Of what is not earned yet, so the credit grows linearly
	n := min(int(math.Floor(d.credit+0.5)), len(sessions))                                  // Rounded: evenly spaced, centered in their slots
	if n <= 0 {
		return nil
	}
	d.credit -= float64(n)
	slices.SortFunc(sessions, func(a, b *Session) int {
		return cmp.Compare(a.id, b.id)
	})
	return sessions[:n]
}

// drainLoop closes the streams of draining upstreams and of a draining load balancer
func (b *Balancer) drainLoop(done <-chan struct{}) {
	ticker := time.NewTicker(drainTickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case now := <-ticker.C:
			b.drainTick(now)
		}
	}
}

// drainTick cancels this tick's share of draining streams, and forgets removed upstreams that
// have fully drained
func (b *Balancer) drainTick(now time.Time) {
	victims := b.drainTickVictims(now)
	for _, s := range victims {
		s.cancel(ErrDrained)
	}
	if len(victims) > 0 {
		slog.Debug("Drained streams", "count", len(victims))
	}
}

func (b *Balancer) drainTickVictims(now time.Time) []*Session {
	b.mu.Lock()
	defer b.mu.Unlock()
	var all, orphans []*Session
	byUpstream := make(map[*Upstream][]*Session)
	for _, s := range b.sessions {
		if s.canceled {
			continue
		} else if s.upstream != nil && b.upstreams[s.upstream.name] != s.upstream {
			// Attached after its removed upstream was forgotten (Attach raced the last tick), so
			// no drain would ever close it
			orphans = append(orphans, s)
			continue
		}
		all = append(all, s)
		if s.upstream != nil && s.upstream.drain != nil {
			byUpstream[s.upstream] = append(byUpstream[s.upstream], s)
		}
	}
	var victims []*Session
	if b.drain != nil {
		victims = b.drain.pick(now, all)
	} else {
		for _, u := range b.upstreams {
			if u.drain != nil {
				victims = append(victims, u.drain.pick(now, byUpstream[u])...)
			}
		}
	}
	victims = append(victims, orphans...)
	for _, s := range victims {
		s.canceled = true
	}
	for name, u := range b.upstreams {
		if u.removed && u.drain != nil && u.drain.done(now) && u.streamCount() == 0 {
			slog.Info("Removed upstream drained, forgetting it", "upstream", name)
			delete(b.upstreams, name)
			b.order = slices.DeleteFunc(b.order, func(n string) bool {
				return n == name
			})
		}
	}
	return victims
}
