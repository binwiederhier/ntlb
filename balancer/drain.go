package balancer

import (
	"cmp"
	"context"
	"iter"
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

// quota returns how many of count open sessions to close at this tick: every one once the
// deadline passed, else the share that spreads them evenly over the time left (see drainState)
func (d *drainState) quota(now time.Time, count int) int {
	defer func() {
		d.last = now
	}()
	if d.done(now) {
		d.finished = true
	}
	if count == 0 {
		return 0
	} else if d.finished {
		return count
	}
	elapsed, remaining := now.Sub(d.last), d.deadline.Sub(d.last)
	if elapsed <= 0 {
		return 0
	}
	d.credit += (float64(count) - d.credit) * float64(elapsed) / float64(remaining) // Of what is not earned yet, so the credit grows linearly
	n := min(int(math.Floor(d.credit+0.5)), count)                                  // Rounded: evenly spaced, centered in their slots
	if n <= 0 {
		return 0
	}
	d.credit -= float64(n)
	return n
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

// drainTickVictims picks this tick's streams to close, oldest first. It scans the sessions only
// while a drain runs, and keeps only this tick's few of them: at ~100k streams the scan still
// holds the lock for about a millisecond, every tick. A drained upstream (finished, with no
// stream left) does not count: it may stay drained for days. (A session that attaches to a
// forgotten or drained upstream is closed by Attach.)
func (b *Balancer) drainTickVictims(now time.Time) []*Session {
	b.mu.Lock()
	defer b.mu.Unlock()
	var victims []*Session
	switch {
	case b.drain != nil:
		live := b.liveSessionsNoLock(nil)
		victims = oldestSessions(live, b.drain.quota(now, countSessions(live)))
	case b.upstreamDrainingNoLock():
		counts := make(map[*Upstream]int)
		for _, s := range b.sessions {
			if !s.canceled && s.upstream != nil && s.upstream.drain != nil {
				counts[s.upstream]++
			}
		}
		for _, u := range b.upstreams {
			if u.drain == nil {
				continue
			}
			if n := u.drain.quota(now, counts[u]); n > 0 {
				victims = append(victims, oldestSessions(b.liveSessionsNoLock(u), n)...)
			}
		}
	}
	for _, s := range victims {
		s.canceled = true
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

// liveSessionsNoLock yields the sessions not canceled yet, of an upstream (nil: all of them).
// The caller must hold Balancer.mu.
func (b *Balancer) liveSessionsNoLock(u *Upstream) iter.Seq[*Session] {
	return func(yield func(*Session) bool) {
		for _, s := range b.sessions {
			if !s.canceled && (u == nil || s.upstream == u) && !yield(s) {
				return
			}
		}
	}
}

// countSessions returns the number of sessions
func countSessions(sessions iter.Seq[*Session]) int {
	n := 0
	for range sessions {
		n++
	}
	return n
}

// oldestSessions returns the n oldest (lowest ID) of the sessions, oldest first. It keeps no more
// than n at a time, in a heap with the newest of them on top: a tick closes few of many.
func oldestSessions(sessions iter.Seq[*Session], n int) []*Session {
	if n <= 0 {
		return nil
	}
	heap := make([]*Session, 0, n)
	for s := range sessions {
		if len(heap) < n {
			heap = append(heap, s)
			for i := len(heap) - 1; i > 0 && heap[(i-1)/2].id < heap[i].id; i = (i - 1) / 2 {
				heap[i], heap[(i-1)/2] = heap[(i-1)/2], heap[i]
			}
		} else if s.id < heap[0].id {
			heap[0] = s
			for i := 0; ; {
				left, right, newest := 2*i+1, 2*i+2, i
				if left < len(heap) && heap[left].id > heap[newest].id {
					newest = left
				}
				if right < len(heap) && heap[right].id > heap[newest].id {
					newest = right
				}
				if newest == i {
					break
				}
				heap[i], heap[newest] = heap[newest], heap[i]
				i = newest
			}
		}
	}
	slices.SortFunc(heap, func(a, b *Session) int {
		return cmp.Compare(a.id, b.id)
	})
	return heap
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
