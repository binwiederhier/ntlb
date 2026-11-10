// Package rules checks requests against the configured rules (block, rate limit, allow, redirect
// or serve them, by their match expressions, see package match) and counts the rules' strikes per
// client into ban lines. Rules are replaced atomically on reload; rate
// limit state survives a reload for rules that did not change.
package rules

import (
	"iter"
	"net/http"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"heckel.io/ntlb/balancer"
	"heckel.io/ntlb/config"
	"heckel.io/ntlb/match"
)

const (
	// sweepInterval is how often a limiter forgets the buckets that refilled
	sweepInterval = time.Minute

	// fullSweepInterval is how often a full limiter or bans map (maxTracked) sweeps for room for
	// a new client; in between, new clients go untracked. A sweep of 1M entries takes ~60 ms under
	// the lock, which every new key of a spray would pay otherwise.
	fullSweepInterval = 5 * time.Second
)

var (
	// maxTracked caps the clients one limit or one bans section tracks, against key spraying (a
	// variable for the tests)
	maxTracked = 1_000_000
)

// Engine checks requests against the current rules
type Engine struct {
	state atomic.Pointer[state]
	onBan func(file, line string)
	now   func() time.Time
}

// state is one generation of rules, swapped as a whole on Update
type state struct {
	rules          []*rule            // Global rules
	sites          map[string][]*rule // Rules by site
	bans           *bans              // Global, nil if none
	siteBans       map[string]*bans   // Of the sites with their own bans settings
	ipv4PrefixBits int
	ipv6PrefixBits int
}

type rule struct {
	conf    *config.Rule
	limiter *limiter // Limit rules only
}

// limiter is a token bucket per client key
type limiter struct {
	perSecond float64
	capacity  float64
	buckets   map[string]*bucket
	lastSweep time.Time
	mu        sync.Mutex // Protects buckets and lastSweep
}

type bucket struct {
	tokens float64
	last   time.Time
}

// New creates an engine with the config's rules; onBan gets the ban lines and the file they go to
// (may be nil without bans)
func New(conf *config.Config, onBan func(file, line string)) *Engine {
	e := &Engine{
		now:   time.Now,
		onBan: onBan,
	}
	e.Update(conf)
	return e
}

// Update replaces the rules. A limit rule keeps its buckets if its name, rate and burst and the
// client key (the hash prefix bits) are unchanged, so a reload does not reset clients' limits.
func (e *Engine) Update(conf *config.Config) {
	old := e.state.Load()
	if old == nil {
		old = &state{} // First update: nothing to carry over
	}
	next := &state{
		sites:          make(map[string][]*rule),
		ipv4PrefixBits: conf.ClientKey.IPv4PrefixBits,
		ipv6PrefixBits: conf.ClientKey.IPv6PrefixBits,
	}
	next.rules = old.newRules(next, conf.Rules)
	for _, site := range conf.Sites {
		next.sites[site.Name] = old.newRules(next, site.Rules)
	}
	next.bans = old.reusableBans("", conf.Bans, conf.ClientKey)
	next.siteBans = make(map[string]*bans)
	for _, site := range conf.Sites {
		if site.Bans != nil {
			next.siteBans[site.Name] = old.reusableBans(site.Name, site.Bans, conf.ClientKey)
		}
	}
	e.state.Store(next)
}

// Check returns the rule that decides the request, or nil if it is to be proxied: the global
// rules first, then those of the request's site (env.Site)
func (e *Engine) Check(r *http.Request, env *match.Env) *config.Rule {
	s := e.state.Load()
	var key string
	for rl := range s.rulesFor(env.Site) {
		if rl.conf.On != config.OnRequest || (rl.conf.Program != nil && !rl.conf.Program.Match(env)) {
			continue
		}
		switch rl.conf.Action {
		case config.ActionAllow:
			return nil
		case config.ActionBlock:
			e.strike(s.bansFor(env.Site), remoteAddr(r), rl.conf)
			return rl.conf
		case config.ActionRedirect, config.ActionStatic:
			return rl.conf
		case config.ActionLimit:
			if key == "" {
				key = string(balancer.ClientKey(remoteAddr(r), s.ipv4PrefixBits, s.ipv6PrefixBits))
			}
			if !rl.limiter.take(key, e.now()) {
				e.strike(s.bansFor(env.Site), remoteAddr(r), rl.conf)
				return rl.conf
			}
		}
	}
	return nil
}

// CheckResponse adds the strikes of the response rules that match a request that ended
func (e *Engine) CheckResponse(env *match.ResponseEnv) {
	s := e.state.Load()
	b := s.bansFor(env.Site)
	if b == nil {
		return
	}
	var addr netip.Addr
	for rl := range s.rulesFor(env.Site) {
		if rl.conf.On != config.OnResponse || (rl.conf.Program != nil && !rl.conf.Program.Match(env)) {
			continue
		}
		if !addr.IsValid() {
			addr, _ = netip.ParseAddr(env.IP)
		}
		e.strike(b, addr, rl.conf)
	}
}

// reusableBans returns the old strike counts of a scope (a site, or "" for the global ones) if its
// settings are unchanged, else new ones; nil without settings
func (s *state) reusableBans(site string, conf *config.Bans, key *config.ClientKey) *bans {
	if conf == nil {
		return nil
	}
	var old *bans
	if site == "" {
		old = s.bans
	} else {
		old = s.siteBans[site]
	}
	if old != nil && old.sameAs(conf, key) {
		return old
	}
	return newBans(conf, key)
}

// bansFor returns the strike counts for a site's requests: its own, else the global ones
func (s *state) bansFor(site string) *bans {
	if b, ok := s.siteBans[site]; ok {
		return b
	}
	return s.bans
}

// rulesFor returns the global rules followed by the site's
func (s *state) rulesFor(site string) iter.Seq[*rule] {
	return func(yield func(*rule) bool) {
		for _, rl := range s.rules {
			if !yield(rl) {
				return
			}
		}
		for _, rl := range s.sites[site] {
			if !yield(rl) {
				return
			}
		}
	}
}

// newRules makes the rules of the next state, with the limiters of s that can carry over
func (s *state) newRules(next *state, confs []*config.Rule) []*rule {
	rules := make([]*rule, 0, len(confs))
	for _, c := range confs {
		r := &rule{
			conf: c,
		}
		if c.Action == config.ActionLimit {
			r.limiter = s.reusableLimiter(next, c)
			if r.limiter == nil {
				r.limiter = &limiter{
					perSecond: c.RatePerSecond,
					capacity:  float64(c.Burst + 1),
					buckets:   make(map[string]*bucket),
				}
			}
		}
		rules = append(rules, r)
	}
	return rules
}

// reusableLimiter returns the old limiter of the rule (by ID) if it can carry over into next
func (s *state) reusableLimiter(next *state, c *config.Rule) *limiter {
	if s.ipv4PrefixBits != next.ipv4PrefixBits || s.ipv6PrefixBits != next.ipv6PrefixBits {
		return nil
	}
	for r := range s.all() {
		if r.limiter != nil && r.conf.ID == c.ID && r.conf.RatePerSecond == c.RatePerSecond && r.conf.Burst == c.Burst {
			return r.limiter
		}
	}
	return nil
}

// all returns every rule, global and of all sites
func (s *state) all() iter.Seq[*rule] {
	return func(yield func(*rule) bool) {
		for _, rl := range s.rules {
			if !yield(rl) {
				return
			}
		}
		for _, rules := range s.sites {
			for _, rl := range rules {
				if !yield(rl) {
					return
				}
			}
		}
	}
}

// take takes a token from the client's bucket, and reports whether there was one
func (l *limiter) take(key string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if now.Sub(l.lastSweep) >= sweepInterval {
		l.sweepNoLock(now)
	}
	b, ok := l.buckets[key]
	if !ok && len(l.buckets) >= maxTracked {
		if now.Sub(l.lastSweep) >= fullSweepInterval {
			l.sweepNoLock(now)
		}
		if len(l.buckets) >= maxTracked {
			return true // Untracked: a spray of keys must not grow the map without bound
		}
	}
	if !ok {
		b = &bucket{
			tokens: l.capacity,
			last:   now,
		}
		l.buckets[key] = b
	}
	b.refill(now, l.perSecond, l.capacity)
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// sweepNoLock forgets the buckets that refilled: a new bucket is full anyway
func (l *limiter) sweepNoLock(now time.Time) {
	for key, b := range l.buckets {
		b.refill(now, l.perSecond, l.capacity)
		if b.tokens >= l.capacity {
			delete(l.buckets, key)
		}
	}
	l.lastSweep = now
}

func (b *bucket) refill(now time.Time, perSecond, capacity float64) {
	if elapsed := now.Sub(b.last).Seconds(); elapsed > 0 {
		b.tokens = min(capacity, b.tokens+elapsed*perSecond)
		b.last = now
	}
}

// remoteAddr is the client address, IPv4-mapped IPv6 addresses as IPv4; invalid if unparseable
// (net/http always sets host:port), which puts all such requests into one limit bucket
func remoteAddr(r *http.Request) netip.Addr {
	ap, _ := netip.ParseAddrPort(r.RemoteAddr)
	return ap.Addr().Unmap()
}

// strike adds a rule's strikes for a client to b, and hands the ban line on if that reports it
func (e *Engine) strike(b *bans, addr netip.Addr, rule *config.Rule) {
	if b == nil || rule.Strike == 0 {
		return
	}
	if line, ok := b.strike(addr, rule.Strike, rule.ID, e.now()); ok && e.onBan != nil {
		e.onBan(b.conf.File, line)
	}
}
