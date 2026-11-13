package balancer

import (
	"context"
	"time"
)

// NewSession registers a proxied request; cancel is called (with ErrDrained) to drain it.
// The caller must call Close when the request is done.
func (b *Balancer) NewSession(cancel context.CancelCauseFunc) *Session {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nextID++
	s := &Session{
		id:     b.nextID,
		cancel: cancel,
	}
	b.sessions[s.id] = s
	return s
}

// Reserve returns the address a session should try on an upstream, and counts the attempt
// towards that address until the session attaches, reserves again (a retry) or closes, so that
// concurrent attempts spread over the addresses.
func (b *Balancer) Reserve(s *Session, u *Upstream) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	s.releaseNoLock()
	u.mu.Lock()
	defer u.mu.Unlock()
	addr := u.nextAddressNoLock(time.Now())
	if _, ok := b.sessions[s.id]; ok { // A closed session would never release it
		u.addrPending[addr]++
		s.reserved, s.reservedAddr = u, addr
	}
	return addr
}

// Attach records that a session is being served by an upstream address. A session on an
// upstream the drain already forgot (the request picked it before a reload removed it) is closed
// right away: no drain would ever close it.
func (b *Balancer) Attach(s *Session, u *Upstream, addr string) {
	if b.attach(s, u, addr) {
		s.cancel(ErrDrained)
	}
}

// attach records the session, and reports whether its upstream was forgotten or is drained, so
// that the caller cancels it (outside the lock)
func (b *Balancer) attach(s *Session, u *Upstream, addr string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	s.releaseNoLock()
	s.upstream, s.addr = u, addr
	u.requests.Add(1)
	u.mu.Lock()
	u.addrStreams[addr]++
	delete(u.addrFails, addr)
	delete(u.addrRetry, addr)
	u.mu.Unlock()
	late := b.upstreams[u.id] != u || (u.drain != nil && u.drain.finished) // Forgotten, or drained: no tick closes it
	if late && !s.canceled {
		s.canceled = true
		return true
	}
	return false
}

// Close unregisters a session
func (b *Balancer) Close(s *Session) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.sessions[s.id]; !ok {
		return
	}
	delete(b.sessions, s.id)
	s.releaseNoLock()
	if s.upstream != nil {
		s.upstream.mu.Lock()
		defer s.upstream.mu.Unlock()
		s.upstream.addrStreams[s.addr]--
	}
}

// releaseNoLock gives back the session's reserved attempt, if any. The caller must hold
// Balancer.mu.
func (s *Session) releaseNoLock() {
	if s.reserved == nil {
		return
	}
	s.reserved.mu.Lock()
	defer s.reserved.mu.Unlock()
	s.reserved.addrPending[s.reservedAddr]--
	s.reserved, s.reservedAddr = nil, ""
}
