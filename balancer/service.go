// Package balancer decides which upstream (ntfy node) serves a client, tracks the health of the
// upstreams and the streams running through them, and drains streams when an upstream or the
// load balancer itself is taken out of service.
package balancer

import (
	"cmp"
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
	scratchSize       = 16 // Upstreams per pool that the sorts handle on the stack
)

// Balancer routes requests to the healthy upstreams of one of their site's upstream blocks (a
// pool: default, or one a proxy rule named), by the pool's balance (round robin, least
// connections, random two, or rendezvous hashing of the client), and owns the health checks and
// the drains. Upstreams are keyed by their ID, "<site>/<name>": names are unique over a site's pools.
type Balancer struct {
	config    *config.Config
	upstreams map[string]*Upstream
	order     []string // IDs in config order, then removed upstreams still draining
	sessions  map[uint64]*Session
	nextID    uint64
	drain     *drainState // Draining the load balancer itself
	turns     sync.Map    // Pool ID (config.Upstream.ID) -> *atomic.Uint64, the next turn of round-robin and least-conn
	client    *http.Client
	mu        sync.RWMutex // Protects config, upstreams, order, sessions, nextID, drain, and each Upstream's removed and drain
}

// Upstream is one backend node and its addresses. Several addresses (usually one node's ports) let
// the load balancer hold more than ~63k connections to one node from a single source IP.
type Upstream struct {
	id          string // site/name
	site        string
	name        string
	pool        string      // The name of the site's upstream block it is in; protected by Balancer.mu
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
	health      *config.HealthCheck  // The pool's health check
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
	for _, ps := range servers(conf) {
		u := newUpstream(ps.site, ps.pool, ps.server)
		b.upstreams[u.id] = u
		b.order = append(b.order, u.id)
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

// Pick returns the upstreams of a site's pool (the name of one of its upstream blocks, "" for
// default) to try, best first: the healthy upstreams that are not draining, then
// the healthy backups as the last retries; the healthy backups alone if no other upstream is
// healthy. If none is healthy, it fails open and returns every upstream that is not draining
// (backups last), since a problem shared by all nodes (e.g. their database) is no reason to turn
// every client away. A pool that a reload removed, or left without any of its servers, still
// returns the removed servers while they drain, in config order: a request routed by the config
// before the reload has nowhere else to go. Returns nothing only if every upstream of the pool
// is draining (by command), or the pool is gone and drained. The order is the pool's balance
// (strategy.go); the client key matters for balance hash only.
func (b *Balancer) Pick(site, pool string, key []byte) []*Upstream {
	b.mu.RLock()
	defer b.mu.RUnlock()
	var poolConf *config.Upstream
	if siteConf := b.config.Site(site); siteConf != nil {
		poolConf = siteConf.UpstreamNamed(pool)
	}
	poolName := cmp.Or(pool, config.UpstreamDefault)

	// Count the pool's servers by kind, then take the best kind that has any: one allocation
	var counts [pickKinds]int
	kinds := make([]pickKind, 0, scratchSize)
	live := false
	for _, id := range b.order {
		u := b.upstreams[id]
		if u.site != site || u.pool != poolName {
			continue
		}
		kind := u.pickKindNoLock()
		live = live || kind != pickRemoved
		counts[kind]++
		kinds = append(kinds, kind)
	}
	var first, second pickKind // The kinds to return, second after first (the backups)
	switch {
	case poolConf == nil || !live:
		first, second = pickRemoved, pickNone // Removed by a reload after the request was routed
	case counts[pickHealthy] > 0:
		first, second = pickHealthy, pickHealthyBackup
	case counts[pickHealthyBackup] > 0:
		first, second = pickHealthyBackup, pickNone
	default:
		first, second = pickUnhealthy, pickUnhealthyBackup // Fail open
	}
	candidates := make([]*Upstream, 0, counts[first]+counts[second])
	i := 0
	for _, id := range b.order {
		u := b.upstreams[id]
		if u.site != site || u.pool != poolName {
			continue
		}
		if kinds[i] == first {
			candidates = append(candidates, u)
		}
		i++
	}
	n := len(candidates)
	if second != pickNone {
		i = 0
		for _, id := range b.order {
			u := b.upstreams[id]
			if u.site != site || u.pool != poolName {
				continue
			}
			if kinds[i] == second {
				candidates = append(candidates, u)
			}
			i++
		}
	}
	if first == pickRemoved {
		return candidates // In config order: their pool and its balance are gone
	}
	var turn uint64
	if balance := poolConf.Balance; balance == config.BalanceRoundRobin || balance == config.BalanceLeastConn {
		turn = b.nextTurn(poolConf.ID)
	}
	sortFor(poolConf, key, turn, candidates[:n])
	sortFor(poolConf, key, turn, candidates[n:])
	return candidates
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
	for _, ps := range servers(conf) {
		site, pool, uc := ps.site, ps.pool, ps.server
		id := upstreamID(site, uc.Name)
		if u, ok := b.upstreams[id]; !ok {
			added = append(added, newUpstream(site, pool, uc))
			addedIDs[id] = true
		} else if !u.hasAddresses(uc.Addresses) {
			probed := newUpstream(site, pool, uc)
			changed = append(changed, probed)
			changedByID[id] = probed
		}
	}
	b.mu.RUnlock()
	b.probe(context.Background(), slices.Concat(added, changed))

	// Swap in the new set, unless an upstream went away or changed in between
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, ps := range servers(conf) {
		id := upstreamID(ps.site, ps.server.Name)
		u, ok := b.upstreams[id]
		if !ok && !addedIDs[id] {
			return false
		} else if ok && !u.hasAddresses(ps.server.Addresses) && changedByID[id] == nil {
			return false
		}
	}
	now := time.Now()
	wanted := make(map[string]bool)
	order := make([]string, 0, conf.UpstreamCount())
	for _, ps := range servers(conf) {
		site, pool, uc := ps.site, ps.pool, ps.server
		id := upstreamID(site, uc.Name)
		wanted[id] = true
		order = append(order, id)
		if u, ok := b.upstreams[id]; ok {
			u.updateNoLock(pool, uc, changedByID[id])
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
	pools := make(map[string]bool)
	for _, site := range conf.Sites {
		for _, pool := range site.AllUpstreams() {
			pools[pool.ID] = true
		}
	}
	b.turns.Range(func(key, _ any) bool {
		if !pools[key.(string)] {
			b.turns.Delete(key) // The pool is gone; a pool of that name later starts over
		}
		return true
	})
	return true
}

// updateNoLock applies an upstream's reloaded config: back from removal, its pool (it may move to
// another of the site's pools) and the pool's health check, and the addresses and first-probe
// health of probed, if they changed. The caller must hold Balancer.mu.
func (u *Upstream) updateNoLock(pool *config.Upstream, uc *config.Server, probed *Upstream) {
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
	if u.pool != pool.Name {
		slog.Info("Upstream moved to another upstream block", "site", u.site, "upstream", u.name, "from", u.pool, "to", pool.Name)
	}
	u.pool, u.weight, u.backup = pool.Name, uc.Weight, uc.Backup
	u.mu.Lock()
	defer u.mu.Unlock()
	u.health = pool.HealthCheck
}

// Healthy returns true if at least one upstream in rotation is healthy
func (b *Balancer) Healthy() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.healthyNoLock()
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

func newUpstream(site string, pool *config.Upstream, uc *config.Server) *Upstream {
	return &Upstream{
		id:          upstreamID(site, uc.Name),
		site:        site,
		name:        uc.Name,
		pool:        pool.Name,
		weight:      uc.Weight,
		backup:      uc.Backup,
		health:      pool.HealthCheck,
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

// servers returns every server of the config with its site's name and the pool it is in, in
// config order (a site's upstream first, then its named upstreams by name)
func servers(conf *config.Config) []*poolServer {
	all := make([]*poolServer, 0, conf.UpstreamCount())
	for _, site := range conf.Sites {
		for _, pool := range site.AllUpstreams() {
			for _, srv := range pool.Servers {
				all = append(all, &poolServer{
					site:   site.Name,
					pool:   pool,
					server: srv,
				})
			}
		}
	}
	return all
}

// statusNoLock returns the upstream's snapshot. The caller must hold Balancer.mu.
func (u *Upstream) statusNoLock(now time.Time) *UpstreamStatus {
	u.mu.Lock()
	defer u.mu.Unlock()
	s := &UpstreamStatus{
		Site:      u.site,
		Name:      u.name,
		Pool:      u.pool,
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

// pickKindNoLock is what Pick makes of the upstream. The caller must hold Balancer.mu.
func (u *Upstream) pickKindNoLock() pickKind {
	switch healthy := u.isHealthy(); {
	case u.removed:
		return pickRemoved
	case u.drain != nil:
		return pickDraining
	case healthy && !u.backup:
		return pickHealthy
	case healthy:
		return pickHealthyBackup
	case !u.backup:
		return pickUnhealthy
	default:
		return pickUnhealthyBackup
	}
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
