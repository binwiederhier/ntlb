// Package rules checks requests against the configured rules: block, rate limit or allow the
// requests their match expressions (package match) match. Rules are replaced atomically on reload; rate
// limit state survives a reload for rules that did not change.
package rules

import (
	"net/http"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"heckel.io/ntfy-lb/balancer"
	"heckel.io/ntfy-lb/config"
	"heckel.io/ntfy-lb/match"
)

// sweepInterval is how often a limiter forgets the buckets that refilled
const sweepInterval = time.Minute

// Engine checks requests against the current rules
type Engine struct {
	state atomic.Pointer[state]
	now   func() time.Time
}

// state is one generation of rules, swapped as a whole on Update
type state struct {
	rules          []*rule
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

// New creates an engine with the config's rules
func New(conf *config.Config) *Engine {
	e := &Engine{
		now: time.Now,
	}
	e.Update(conf)
	return e
}

// Update replaces the rules. A limit rule keeps its buckets if its name, rate and burst and the
// client key (the hash prefix bits) are unchanged, so a reload does not reset clients' limits.
func (e *Engine) Update(conf *config.Config) {
	old := e.state.Load()
	next := &state{
		ipv4PrefixBits: conf.Hash.IPv4PrefixBits,
		ipv6PrefixBits: conf.Hash.IPv6PrefixBits,
	}
	for _, c := range conf.Rules {
		r := &rule{
			conf: c,
		}
		if c.Action == config.ActionLimit {
			r.limiter = old.reusableLimiter(next, c)
			if r.limiter == nil {
				r.limiter = &limiter{
					perSecond: c.RatePerSecond,
					capacity:  float64(c.Burst + 1),
					buckets:   make(map[string]*bucket),
				}
			}
		}
		next.rules = append(next.rules, r)
	}
	e.state.Store(next)
}

// Check returns the rule that blocks or limits the request, or nil if it may pass
func (e *Engine) Check(r *http.Request) *config.Rule {
	s := e.state.Load()
	if len(s.rules) == 0 {
		return nil
	}
	var env *match.Env // Taken from the request once, for the first rule that needs it
	var key string
	for _, rl := range s.rules {
		if rl.conf.Program != nil {
			if env == nil {
				env = match.NewEnv(r)
			}
			if !rl.conf.Program.Match(env) {
				continue
			}
		}
		switch rl.conf.Action {
		case config.ActionAllow:
			return nil
		case config.ActionBlock:
			return rl.conf
		case config.ActionLimit:
			if key == "" {
				key = string(balancer.ClientKey(remoteAddr(r), s.ipv4PrefixBits, s.ipv6PrefixBits))
			}
			if !rl.limiter.take(key, e.now()) {
				return rl.conf
			}
		}
	}
	return nil
}

// reusableLimiter returns the old limiter of the rule if it can carry over into next
func (s *state) reusableLimiter(next *state, c *config.Rule) *limiter {
	if s == nil || s.ipv4PrefixBits != next.ipv4PrefixBits || s.ipv6PrefixBits != next.ipv6PrefixBits {
		return nil
	}
	for _, r := range s.rules {
		if r.limiter != nil && r.conf.Name == c.Name && r.conf.RatePerSecond == c.RatePerSecond && r.conf.Burst == c.Burst {
			return r.limiter
		}
	}
	return nil
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
