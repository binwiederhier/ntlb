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

// sweepInterval is how often a limiter forgets the buckets that refilled
const sweepInterval = time.Minute

// Engine checks requests against the current rules
type Engine struct {
	state atomic.Pointer[state]
	onBan func(line string)
	now   func() time.Time
}

// state is one generation of rules, swapped as a whole on Update
type state struct {
	rules          []*rule            // Global rules
	sites          map[string][]*rule // Rules by site
	bans           *bans
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

// New creates an engine with the config's rules; onBan gets the ban lines (may be nil without bans)
func New(conf *config.Config, onBan func(line string)) *Engine {
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
	next := &state{
		sites:          make(map[string][]*rule),
		ipv4PrefixBits: conf.ClientKey.IPv4PrefixBits,
		ipv6PrefixBits: conf.ClientKey.IPv6PrefixBits,
	}
	next.rules = old.newRules(next, conf.Rules)
	for _, site := range conf.Sites {
		next.sites[site.Name] = old.newRules(next, site.Rules)
	}
	if conf.Bans != nil {
		if old != nil && old.bans != nil && old.bans.sameAs(conf.Bans, conf.ClientKey) {
			next.bans = old.bans
		} else {
			next.bans = newBans(conf.Bans, conf.ClientKey)
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
			e.strike(s, remoteAddr(r), rl.conf)
			return rl.conf
		case config.ActionRedirect, config.ActionStatic:
			return rl.conf
		case config.ActionLimit:
			if key == "" {
				key = string(balancer.ClientKey(remoteAddr(r), s.ipv4PrefixBits, s.ipv6PrefixBits))
			}
			if !rl.limiter.take(key, e.now()) {
				e.strike(s, remoteAddr(r), rl.conf)
				return rl.conf
			}
		}
	}
	return nil
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
	if s == nil || s.ipv4PrefixBits != next.ipv4PrefixBits || s.ipv6PrefixBits != next.ipv6PrefixBits {
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

// CheckResponse adds the strikes of the response rules that match a request that ended
func (e *Engine) CheckResponse(env *match.ResponseEnv) {
	s := e.state.Load()
	if s.bans == nil {
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
		e.strike(s, addr, rl.conf)
	}
}

// strike adds a rule's strikes for a client, and hands the ban line on if that reports it
func (e *Engine) strike(s *state, addr netip.Addr, rule *config.Rule) {
	if s.bans == nil || rule.Strike == 0 {
		return
	}
	if line, ok := s.bans.strike(addr, rule.Strike, rule.ID, e.now()); ok && e.onBan != nil {
		e.onBan(line)
	}
}
