package balancer

import (
	"cmp"
	"context"
	"log/slog"
	"math"
	"slices"
	"time"
)

// DrainUpstream takes an upstream ("site/name", or the name if only one site has it) out of
// rotation and closes its streams over d
func (b *Balancer) DrainUpstream(name string, d time.Duration) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	u, err := b.findNoLock(name)
	if err != nil {
		return err
	}
	slog.Info("Draining upstream", "site", u.site, "upstream", u.name, "duration", d)
	u.drain = newDrainState(time.Now(), d, false)
	return nil
}

// UndrainUpstream puts a drained (or draining) upstream back into rotation
func (b *Balancer) UndrainUpstream(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	u, err := b.findNoLock(name)
	if err != nil {
		return err
	}
	slog.Info("Undraining upstream", "site", u.site, "upstream", u.name)
	u.drain = nil
	return nil
}

// Drain makes the load balancer unready and closes all its streams over d. Once drained, the
// server turns new requests away (see Drained).
func (b *Balancer) Drain(d time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	slog.Info("Draining load balancer", "duration", d)
	b.drain = newDrainState(time.Now(), d, false)
}

// Undrain ends a drain of the load balancer
func (b *Balancer) Undrain() {
	b.mu.Lock()
	defer b.mu.Unlock()
	slog.Info("Undraining load balancer")
	b.drain = nil
}

// Draining returns true if the load balancer is draining (or drained)
func (b *Balancer) Draining() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.drain != nil
}

// Drained returns true if the load balancer drained and closed its last stream at the deadline
func (b *Balancer) Drained() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.drain != nil && b.drain.finished
}

// WaitIdle blocks until no session is open, or ctx is done
func (b *Balancer) WaitIdle(ctx context.Context) error {
	for {
		b.mu.RLock()
		n := len(b.sessions)
		b.mu.RUnlock()
		if n == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitIdleInterval):
		}
	}
}

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

// drainTickVictims picks this tick's streams to close. It scans the sessions only while a drain
// runs: at ~100k streams the scan holds the lock for milliseconds, every tick. A drained upstream
// (finished, with no stream left) does not count: it may stay drained for days. (A session that
// attaches to a forgotten or drained upstream is closed by Attach.)
func (b *Balancer) drainTickVictims(now time.Time) []*Session {
	b.mu.Lock()
	defer b.mu.Unlock()
	var victims []*Session
	if b.drain != nil || b.upstreamDrainingNoLock() {
		var all []*Session // Only for the load balancer's drain
		byUpstream := make(map[*Upstream][]*Session)
		for _, s := range b.sessions {
			if s.canceled {
				continue
			}
			if b.drain != nil {
				all = append(all, s)
			}
			if s.upstream != nil && s.upstream.drain != nil {
				byUpstream[s.upstream] = append(byUpstream[s.upstream], s)
			}
		}
		if b.drain != nil {
			victims = b.drain.pick(now, all)
		} else {
			for _, u := range b.upstreams {
				if u.drain != nil {
					victims = append(victims, u.drain.pick(now, byUpstream[u])...)
				}
			}
		}
		for _, s := range victims {
			s.canceled = true
		}
	}
	for id, u := range b.upstreams {
		if u.removed && u.drain != nil && u.drain.done(now) && u.streamCount() == 0 {
			slog.Info("Removed upstream drained, forgetting it", "site", u.site, "upstream", u.name)
			delete(b.upstreams, id)
			b.order = slices.DeleteFunc(b.order, func(n string) bool {
				return n == id
			})
		}
	}
	return victims
}

// upstreamDrainingNoLock reports whether any upstream is draining (removed, or by command) and
// has something left to do: its deadline tick, or streams to close
func (b *Balancer) upstreamDrainingNoLock() bool {
	for _, u := range b.upstreams {
		if u.drain != nil && (!u.drain.finished || u.streamCount() > 0) {
			return true
		}
	}
	return false
}
