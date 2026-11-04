// Package balancer decides which upstream (ntfy node) serves a client, tracks the health of the
// upstreams and the streams running through them, and drains streams when an upstream or the
// load balancer itself is taken out of service.
package balancer

import (
	"context"
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"heckel.io/ntfy-lb/config"
)

const (
	drainTickInterval = time.Second
	waitIdleInterval  = 50 * time.Millisecond
)

// Balancer routes clients to upstreams by rendezvous hashing their IP over the healthy upstreams,
// and owns the health checks and the drains
type Balancer struct {
	config    *config.Config
	upstreams map[string]*Upstream
	order     []string // Config order, then removed upstreams still draining
	sessions  map[uint64]*Session
	nextID    uint64
	drain     *drainState // Draining the load balancer itself
	client    *http.Client
	mu        sync.RWMutex // Protects config, upstreams, order, sessions, nextID, drain, and each Upstream's removed and drain
}

// Upstream is one ntfy node and its addresses. Several addresses (usually one node's ports) let
// the load balancer hold more than ~63k connections to one node from a single source IP.
type Upstream struct {
	name        string
	removed     bool        // Protected by Balancer.mu
	drain       *drainState // Protected by Balancer.mu
	requests    atomic.Uint64
	failures    atomic.Uint64
	addresses   []string
	addrStreams map[string]int
	healthy     bool
	probed      bool
	fails       int
	passes      int
	lastCheck   time.Time
	lastError   string
	mu          sync.Mutex // Protects addresses, addrStreams, healthy, probed, fails, passes, lastCheck, lastError
}

// New creates a balancer for the upstreams in the config. Call Start to run the health checks.
func New(conf *config.Config) *Balancer {
	b := &Balancer{
		config:    conf,
		upstreams: make(map[string]*Upstream),
		sessions:  make(map[uint64]*Session),
		client: &http.Client{
			Transport: &http.Transport{
				DisableKeepAlives: true, // Every probe opens a new connection, like a new client would
			},
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
	for _, uc := range conf.Upstreams {
		b.upstreams[uc.Name] = newUpstream(uc)
		b.order = append(b.order, uc.Name)
	}
	return b
}

// Start probes every upstream once, so that a node that is down or draining takes no traffic
// from the start, and then runs the health check and drain loops until ctx is done
func (b *Balancer) Start(ctx context.Context) {
	b.checkHealth(ctx)
	go b.healthLoop(ctx)
	go b.drainLoop(ctx.Done())
}

// Pick returns the upstreams to try for a client key, best first: the healthy upstreams that are
// not draining. If none is healthy, it fails open and returns every upstream that is not
// draining, since a problem shared by all nodes (e.g. their database) is no reason to turn every
// client away. Returns nothing only if every upstream is draining.
func (b *Balancer) Pick(key []byte) []*Upstream {
	b.mu.RLock()
	defer b.mu.RUnlock()
	names := make([]string, 0, len(b.upstreams))
	for name, u := range b.upstreams {
		if !u.removed && u.drain == nil {
			names = append(names, name)
		}
	}
	var healthy, unhealthy []*Upstream
	for _, name := range rank(key, names) {
		if u := b.upstreams[name]; u.isHealthy() {
			healthy = append(healthy, u)
		} else {
			unhealthy = append(unhealthy, u)
		}
	}
	if len(healthy) > 0 {
		return healthy
	}
	return unhealthy
}

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

// Attach records that a session is being served by an upstream address
func (b *Balancer) Attach(s *Session, u *Upstream, addr string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s.upstream, s.addr = u, addr
	u.requests.Add(1)
	u.mu.Lock()
	defer u.mu.Unlock()
	u.addrStreams[addr]++
}

// Close unregisters a session
func (b *Balancer) Close(s *Session) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.sessions[s.id]; !ok {
		return
	}
	delete(b.sessions, s.id)
	if s.upstream != nil {
		s.upstream.mu.Lock()
		defer s.upstream.mu.Unlock()
		s.upstream.addrStreams[s.addr]--
	}
}

// RecordFailure counts a request that an upstream failed (connection error, timeout)
func (b *Balancer) RecordFailure(u *Upstream) {
	u.failures.Add(1)
}

// Update applies a reloaded config. New upstreams are probed before they take traffic. Upstreams
// that left the config drain over the config's drain-duration and are forgotten once drained;
// streams to every other upstream are untouched. Drains started by command survive the update.
func (b *Balancer) Update(conf *config.Config) error {
	// Probe the new upstreams first, outside the lock
	b.mu.RLock()
	var added []*Upstream
	for _, uc := range conf.Upstreams {
		if _, ok := b.upstreams[uc.Name]; !ok {
			added = append(added, newUpstream(uc))
		}
	}
	b.mu.RUnlock()
	b.probe(context.Background(), conf.HealthCheck, added)

	// Swap in the new set
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	wanted := make(map[string]bool)
	order := make([]string, 0, len(conf.Upstreams))
	for _, uc := range conf.Upstreams {
		wanted[uc.Name] = true
		order = append(order, uc.Name)
		u, ok := b.upstreams[uc.Name]
		if !ok {
			continue
		}
		if u.removed {
			slog.Info("Upstream re-added", "upstream", uc.Name)
			u.removed = false
			if u.drain != nil && u.drain.removal {
				u.drain = nil
			}
		}
		u.setAddresses(uc.Addresses)
	}
	for _, u := range added {
		slog.Info("Upstream added", "upstream", u.name, "healthy", u.isHealthy())
		b.upstreams[u.name] = u
	}
	for _, name := range b.order {
		if wanted[name] {
			continue
		}
		u := b.upstreams[name]
		if !u.removed {
			slog.Info("Upstream removed, draining it", "upstream", name, "duration", conf.DrainDuration)
			u.removed = true
			if u.drain == nil {
				u.drain = newDrainState(now, conf.DrainDuration, true)
			}
		}
		order = append(order, name)
	}
	b.order = order
	b.config = conf
	return nil
}

// DrainUpstream takes an upstream out of rotation and closes its streams over d
func (b *Balancer) DrainUpstream(name string, d time.Duration) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	u, ok := b.upstreams[name]
	if !ok || u.removed {
		return ErrUpstreamNotFound
	}
	slog.Info("Draining upstream", "upstream", name, "duration", d)
	u.drain = newDrainState(time.Now(), d, false)
	return nil
}

// UndrainUpstream puts a drained (or draining) upstream back into rotation
func (b *Balancer) UndrainUpstream(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	u, ok := b.upstreams[name]
	if !ok || u.removed {
		return ErrUpstreamNotFound
	}
	slog.Info("Undraining upstream", "upstream", name)
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

// Healthy returns true if at least one upstream in rotation is healthy
func (b *Balancer) Healthy() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.healthyNoLock()
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

// Status returns a snapshot of the load balancer and its upstreams
func (b *Balancer) Status() *Status {
	b.mu.RLock()
	defer b.mu.RUnlock()
	now := time.Now()
	healthy := b.healthyNoLock()
	status := &Status{
		Healthy:  healthy,
		Ready:    healthy && b.drain == nil,
		Draining: b.drain != nil,
		Streams:  len(b.sessions),
	}
	if b.drain != nil {
		status.Drained = b.drain.finished
		status.DrainDeadline = &b.drain.deadline
	}
	for _, name := range b.order {
		status.Upstreams = append(status.Upstreams, b.upstreams[name].status(now))
	}
	return status
}

// Name returns the upstream's name
func (u *Upstream) Name() string {
	return u.name
}

// NextAddress returns the upstream address with the fewest streams
func (u *Upstream) NextAddress() string {
	u.mu.Lock()
	defer u.mu.Unlock()
	best := u.addresses[0]
	for _, addr := range u.addresses[1:] {
		if u.addrStreams[addr] < u.addrStreams[best] {
			best = addr
		}
	}
	return best
}

func newUpstream(uc *config.Upstream) *Upstream {
	return &Upstream{
		name:        uc.Name,
		addresses:   slices.Clone(uc.Addresses),
		addrStreams: make(map[string]int),
		healthy:     true,
	}
}

func (b *Balancer) healthyNoLock() bool {
	for _, u := range b.upstreams {
		if !u.removed && u.drain == nil && u.isHealthy() {
			return true
		}
	}
	return false
}

func (b *Balancer) upstream(name string) *Upstream {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.upstreams[name]
}

// status returns the upstream's snapshot. The caller must hold Balancer.mu.
func (u *Upstream) status(now time.Time) *UpstreamStatus {
	u.mu.Lock()
	defer u.mu.Unlock()
	s := &UpstreamStatus{
		Name:      u.name,
		Healthy:   u.healthy,
		Removed:   u.removed,
		Addresses: slices.Clone(u.addresses),
		Streams:   u.streamCountNoLock(),
		Requests:  u.requests.Load(),
		Failures:  u.failures.Load(),
		LastError: u.lastError,
	}
	if !u.lastCheck.IsZero() {
		lastCheck := u.lastCheck
		s.LastCheck = &lastCheck
	}
	switch {
	case u.drain != nil && u.drain.finished && s.Streams == 0:
		s.State = StateDrained
	case u.drain != nil:
		s.State = StateDraining
	case u.healthy:
		s.State = StateHealthy
	default:
		s.State = StateUnhealthy
	}
	if u.drain != nil {
		deadline := u.drain.deadline
		s.DrainDeadline = &deadline
	}
	return s
}

func (u *Upstream) isHealthy() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.healthy
}

func (u *Upstream) setAddresses(addresses []string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.addresses = slices.Clone(addresses)
}

func (u *Upstream) streamCount() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.streamCountNoLock()
}

// streamCountNoLock sums the streams over all addresses. The caller must hold u.mu.
func (u *Upstream) streamCountNoLock() int {
	n := 0
	for _, c := range u.addrStreams {
		n += c
	}
	return n
}
