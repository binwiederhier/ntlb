// Package balancer decides which upstream (ntfy node) serves a client, tracks the health of the
// upstreams and the streams running through them, and drains streams when an upstream or the
// load balancer itself is taken out of service.
package balancer

import (
	"context"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"heckel.io/ntlb/config"
)

const (
	drainTickInterval = time.Second
	waitIdleInterval  = 50 * time.Millisecond
	addressBackoffMin = time.Second
	addressBackoffMax = 30 * time.Second
)

// Balancer routes requests to the healthy upstreams of their site, by the site's balance (round
// robin, least connections, random two, or rendezvous hashing of the client), and owns the health
// checks and the drains. Upstreams are keyed by their ID, "<site>/<name>".
type Balancer struct {
	config    *config.Config
	upstreams map[string]*Upstream
	order     []string // IDs in config order, then removed upstreams still draining
	sessions  map[uint64]*Session
	nextID    uint64
	drain     *drainState // Draining the load balancer itself
	turns     sync.Map    // Site name -> *atomic.Uint64, the next turn of round-robin and least-conn
	client    *http.Client
	mu        sync.RWMutex // Protects config, upstreams, order, sessions, nextID, drain, and each Upstream's removed and drain
}

// Upstream is one backend node and its addresses. Several addresses (usually one node's ports) let
// the load balancer hold more than ~63k connections to one node from a single source IP.
type Upstream struct {
	id          string // site/name
	site        string
	name        string
	removed     bool        // Protected by Balancer.mu
	drain       *drainState // Protected by Balancer.mu
	weight      int         // Protected by Balancer.mu
	backup      bool        // Protected by Balancer.mu
	requests    atomic.Uint64
	failures    atomic.Uint64
	addresses   []string
	generation  uint64               // Bumped when the addresses change, so a probe of the old ones is dropped
	addrStreams map[string]int       // Attached streams per address
	addrPending map[string]int       // Reserved attempts per address, not attached yet
	addrFails   map[string]int       // Consecutive failures per address
	addrRetry   map[string]time.Time // An address that failed is skipped until then
	addrDown    map[string]bool      // Failed its last health check: skipped while others are up
	health      *config.HealthCheck  // The site's health check
	lastProbe   time.Time            // Start of the last probe round that included the upstream
	probing     bool                 // A probe round is running
	healthy     bool
	probed      bool
	fails       int
	passes      int
	lastCheck   time.Time
	lastError   string
	mu          sync.Mutex // Protects addresses, generation, addrStreams, addrPending, addrFails, addrRetry, addrDown, health, lastProbe, probing, healthy, probed, fails, passes, lastCheck, lastError
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
	for _, site := range conf.Sites {
		for _, uc := range site.Upstream.Servers {
			u := newUpstream(site, uc)
			b.upstreams[u.id] = u
			b.order = append(b.order, u.id)
		}
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

// Pick returns the upstreams of a site to try, best first: the healthy upstreams that are not
// draining, then the healthy backups as the last retries; the healthy backups alone if no other
// upstream is healthy. If none is healthy, it fails open and returns every upstream that is not
// draining (backups last), since a problem shared by all nodes (e.g. their database) is no reason
// to turn every client away. Returns nothing only if every upstream of the site is draining. The
// order is the site's balance (strategy.go); the client key matters for balance hash only.
func (b *Balancer) Pick(site string, key []byte) []*Upstream {
	b.mu.RLock()
	defer b.mu.RUnlock()
	siteConf := b.config.Site(site)
	if siteConf == nil {
		return nil
	}
	var healthy, healthyBackups, unhealthy, unhealthyBackups []*Upstream
	for _, id := range b.order {
		u := b.upstreams[id]
		if u.site != site || u.removed || u.drain != nil {
			continue
		}
		switch ok := u.isHealthy(); {
		case ok && !u.backup:
			healthy = append(healthy, u)
		case ok:
			healthyBackups = append(healthyBackups, u)
		case !u.backup:
			unhealthy = append(unhealthy, u)
		default:
			unhealthyBackups = append(unhealthyBackups, u)
		}
	}
	var turn uint64
	if balance := siteConf.Upstream.Balance; balance == config.BalanceRoundRobin || balance == config.BalanceLeastConn {
		turn = b.nextTurn(site)
	}
	switch {
	case len(healthy) > 0:
		return sorted(siteConf, key, turn, healthy, healthyBackups)
	case len(healthyBackups) > 0:
		return sorted(siteConf, key, turn, healthyBackups, nil)
	}
	return sorted(siteConf, key, turn, unhealthy, unhealthyBackups) // Fail open
}

// sorted returns the upstreams sorted by the site's balance, then the backups sorted the same way
func sorted(site *config.Site, key []byte, turn uint64, upstreams, backups []*Upstream) []*Upstream {
	sortFor(site, key, turn, upstreams)
	sortFor(site, key, turn, backups)
	return append(upstreams, backups...)
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

// Reserve returns the address a session should try on an upstream, and counts the attempt
// towards that address until the session attaches, reserves again (a retry) or closes, so that
// concurrent attempts spread over the addresses. Use it instead of NextAddress for requests.
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

// RecordAddressFailure counts a request that an upstream failed on an address (connection error,
// timeout), and skips that address for new attempts for a while, longer with each failure in a row, since
// the health check only probes an upstream's first address
func (b *Balancer) RecordAddressFailure(u *Upstream, addr string) {
	u.failures.Add(1)
	u.mu.Lock()
	defer u.mu.Unlock()
	u.recordAddressFailureNoLock(addr, time.Now())
}

// Update applies a reloaded config. New upstreams are probed before they take traffic. Upstreams
// that left the config drain over the config's drain-duration and are forgotten once drained;
// streams to every other upstream are untouched. Drains started by command survive the update.
func (b *Balancer) Update(conf *config.Config) {
	for !b.tryUpdate(conf) {
		slog.Debug("Re-added upstream was forgotten while probing, probing again")
	}
}

// tryUpdate applies a reloaded config (see Update). It changes nothing and returns false if a
// removed upstream that the config re-adds was forgotten by the drain tick while the added ones
// were probed (it is a new upstream now, and must be probed too), or if an upstream's addresses
// changed in between.
func (b *Balancer) tryUpdate(conf *config.Config) bool {
	// Probe the new upstreams and the new addresses of existing ones first, outside the lock.
	// The probes of new addresses run on a copy, whose health replaces the upstream's below.
	b.mu.RLock()
	var added, changed []*Upstream
	addedIDs := make(map[string]bool)
	changedByID := make(map[string]*Upstream)
	for _, site := range conf.Sites {
		for _, uc := range site.Upstream.Servers {
			id := upstreamID(site.Name, uc.Name)
			if u, ok := b.upstreams[id]; !ok {
				added = append(added, newUpstream(site, uc))
				addedIDs[id] = true
			} else if !u.hasAddresses(uc.Addresses) {
				probed := newUpstream(site, uc)
				changed = append(changed, probed)
				changedByID[id] = probed
			}
		}
	}
	b.mu.RUnlock()
	b.probe(context.Background(), slices.Concat(added, changed))

	// Swap in the new set, unless an upstream went away or changed in between
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, site := range conf.Sites {
		for _, uc := range site.Upstream.Servers {
			id := upstreamID(site.Name, uc.Name)
			u, ok := b.upstreams[id]
			if !ok && !addedIDs[id] {
				return false
			} else if ok && !u.hasAddresses(uc.Addresses) && changedByID[id] == nil {
				return false
			}
		}
	}
	now := time.Now()
	wanted := make(map[string]bool)
	order := make([]string, 0, conf.UpstreamCount())
	for _, site := range conf.Sites {
		for _, uc := range site.Upstream.Servers {
			id := upstreamID(site.Name, uc.Name)
			wanted[id] = true
			order = append(order, id)
			if u, ok := b.upstreams[id]; ok {
				u.updateNoLock(site, uc, changedByID[id])
			}
		}
	}
	for _, u := range added {
		slog.Info("Upstream added", "site", u.site, "upstream", u.name, "healthy", u.isHealthy())
		b.upstreams[u.id] = u
	}
	for _, id := range b.order {
		if wanted[id] {
			continue
		}
		u := b.upstreams[id]
		if !u.removed {
			slog.Info("Upstream removed, draining it", "site", u.site, "upstream", u.name, "duration", conf.DrainDuration)
			u.removed = true
			if u.drain == nil {
				u.drain = newDrainState(now, conf.DrainDuration, true)
			}
		}
		order = append(order, id)
	}
	b.order = order
	b.config = conf
	b.turns.Range(func(site, _ any) bool {
		if conf.Site(site.(string)) == nil {
			b.turns.Delete(site) // The site is gone; a site of that name later starts over
		}
		return true
	})
	return true
}

// updateNoLock applies an upstream's reloaded config: back from removal, the site's health check, and
// the addresses and first-probe health of probed, if they changed. The caller must hold Balancer.mu.
func (u *Upstream) updateNoLock(site *config.Site, uc *config.Server, probed *Upstream) {
	if u.removed {
		slog.Info("Upstream re-added", "site", u.site, "upstream", u.name)
		u.removed = false
		if u.drain != nil && u.drain.removal {
			u.drain = nil
		}
	}
	if probed != nil && !u.hasAddresses(uc.Addresses) {
		slog.Info("Upstream addresses changed", "site", u.site, "upstream", u.name, "addresses", uc.Addresses, "healthy", probed.isHealthy())
		u.replaceAddressesNoLock(probed)
	}
	u.weight, u.backup = uc.Weight, uc.Backup
	u.mu.Lock()
	defer u.mu.Unlock()
	u.health = site.Upstream.HealthCheck
}

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
		status.Upstreams = append(status.Upstreams, b.upstreams[name].statusNoLock(now))
	}
	return status
}

// Name returns the upstream's name
func (u *Upstream) Name() string {
	return u.name
}

// AddressCount returns how many addresses the upstream has
func (u *Upstream) AddressCount() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return len(u.addresses)
}

func newUpstream(site *config.Site, uc *config.Server) *Upstream {
	return &Upstream{
		id:          upstreamID(site.Name, uc.Name),
		site:        site.Name,
		name:        uc.Name,
		weight:      uc.Weight,
		backup:      uc.Backup,
		health:      site.Upstream.HealthCheck,
		addresses:   slices.Clone(uc.Addresses),
		addrStreams: make(map[string]int),
		addrPending: make(map[string]int),
		addrFails:   make(map[string]int),
		addrRetry:   make(map[string]time.Time),
		addrDown:    make(map[string]bool),
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

// findNoLock returns the upstream in the config with an ID, or with a name that only one site
// has. The caller must hold b.mu.
func (b *Balancer) findNoLock(name string) (*Upstream, error) {
	if u, ok := b.upstreams[name]; ok && !u.removed {
		return u, nil
	}
	var found *Upstream
	for _, id := range b.order {
		if u := b.upstreams[id]; u.name == name && !u.removed {
			if found != nil {
				return nil, ErrUpstreamAmbiguous
			}
			found = u
		}
	}
	if found == nil {
		return nil, ErrUpstreamNotFound
	}
	return found, nil
}

func upstreamID(site, name string) string {
	return site + "/" + name
}

// statusNoLock returns the upstream's snapshot. The caller must hold Balancer.mu.
func (u *Upstream) statusNoLock(now time.Time) *UpstreamStatus {
	u.mu.Lock()
	defer u.mu.Unlock()
	s := &UpstreamStatus{
		Site:      u.site,
		Name:      u.name,
		Healthy:   u.healthy,
		Removed:   u.removed,
		Weight:    u.weight,
		Backup:    u.backup,
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

// nextAddressNoLock picks the least loaded address that is neither backing off nor failing its
// health checks; if none is, the least loaded one. Ties go to the first. The caller must hold u.mu.
func (u *Upstream) nextAddressNoLock(now time.Time) string {
	best, bestReady, bestLoad := "", false, 0
	for _, addr := range u.addresses {
		ready := !now.Before(u.addrRetry[addr]) && !u.addrDown[addr]
		load := u.addrStreams[addr] + u.addrPending[addr]
		switch {
		case best == "", ready && !bestReady, ready == bestReady && load < bestLoad:
			best, bestReady, bestLoad = addr, ready, load
		}
	}
	return best
}

// recordAddressFailureNoLock backs off an address: addressBackoffMin after the first failure in a
// row, doubling up to addressBackoffMax. The caller must hold u.mu.
func (u *Upstream) recordAddressFailureNoLock(addr string, now time.Time) {
	u.addrFails[addr]++
	backoff := addressBackoffMax
	if shift := u.addrFails[addr] - 1; shift < 16 {
		backoff = min(addressBackoffMin<<shift, addressBackoffMax)
	}
	u.addrRetry[addr] = now.Add(backoff)
}

func (u *Upstream) isHealthy() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.healthy
}

func (u *Upstream) hasAddresses(addresses []string) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return slices.Equal(u.addresses, addresses)
}

// replaceAddressesNoLock takes the addresses and the first-probe health of probed, a copy of u with
// the new addresses. The hysteresis starts over, and probes of the old addresses still in flight
// are dropped. Streams and their per-address counts stay. The caller must hold Balancer.mu.
func (u *Upstream) replaceAddressesNoLock(probed *Upstream) {
	probed.mu.Lock()
	defer probed.mu.Unlock()
	u.mu.Lock()
	defer u.mu.Unlock()
	u.addresses = slices.Clone(probed.addresses)
	u.generation++
	u.healthy, u.probed = probed.healthy, probed.probed
	u.fails, u.passes = 0, 0
	u.lastCheck, u.lastError = probed.lastCheck, probed.lastError
	u.addrDown = maps.Clone(probed.addrDown)
	for addr := range u.addrRetry {
		if !slices.Contains(u.addresses, addr) {
			delete(u.addrFails, addr)
			delete(u.addrRetry, addr)
		}
	}
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
