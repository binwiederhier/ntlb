package balancer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"slices"
	"sync"
	"time"

	"heckel.io/ntlb/config"
)

// maxDuration is a slack that makes every upstream due
const (
	maxDuration = time.Duration(math.MaxInt64)
)

// healthLoop probes each upstream every healthcheck interval of its pool. It wakes at the
// shortest interval and probes the upstreams that are due, give or take half a round, without
// waiting for the round before: an upstream still being probed is not due. The intervals are
// read every round, so a reload changes them.
func (b *Balancer) healthLoop(ctx context.Context) {
	for {
		interval := b.shortestHealthInterval()
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
		go b.probe(ctx, b.dueUpstreams(time.Now(), interval/2)) // A slow site must not hold up the next round
	}
}

// checkHealth runs one probe round over every upstream still in the config, in parallel
func (b *Balancer) checkHealth(ctx context.Context) {
	b.probe(ctx, b.dueUpstreams(time.Now(), maxDuration))
}

// dueUpstreams returns the upstreams still in the config whose last round started at least their
// interval minus slack ago, and marks them as probed now
func (b *Balancer) dueUpstreams(now time.Time, slack time.Duration) []*Upstream {
	b.mu.RLock()
	defer b.mu.RUnlock()
	upstreams := make([]*Upstream, 0, len(b.upstreams))
	for _, u := range b.upstreams {
		if !u.removed && u.dueForProbe(now, slack) {
			upstreams = append(upstreams, u)
		}
	}
	return upstreams
}

func (b *Balancer) shortestHealthInterval() time.Duration {
	b.mu.RLock()
	defer b.mu.RUnlock()
	shortest := time.Duration(maxDuration)
	for _, site := range b.config.Sites {
		for _, pool := range site.AllUpstreams() {
			shortest = min(shortest, pool.HealthCheck.Interval)
		}
	}
	return shortest
}

// probe checks the given upstreams in parallel, each with its pool's health check, and applies
// the results
func (b *Balancer) probe(ctx context.Context, upstreams []*Upstream) {
	var wg sync.WaitGroup
	for _, u := range upstreams {
		wg.Add(1)
		go func() {
			defer wg.Done()
			generation, hc, addresses := u.probeTargets()
			results := make([]error, len(addresses))
			var addrWG sync.WaitGroup
			for i, addr := range addresses {
				addrWG.Add(1)
				go func() {
					defer addrWG.Done()
					results[i] = b.probeOne(ctx, hc, addr)
				}()
			}
			addrWG.Wait()
			u.applyProbes(hc, generation, addresses, results)
			u.probeDone()
		}()
	}
	wg.Wait()
}

// probeOne checks one address of an upstream
func (b *Balancer) probeOne(ctx context.Context, hc *config.HealthCheck, addr string) error {
	url := fmt.Sprintf("http://%s%s", addr, hc.Path)
	ctx, cancel := context.WithTimeout(ctx, hc.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := b.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 399 { // Like HAProxy's httpchk: 2xx and 3xx (not followed) are up
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	return nil
}

// probeTargets returns the addresses to probe, all of them, the address generation they belong
// to, and the health check to probe them with
func (u *Upstream) probeTargets() (uint64, *config.HealthCheck, []string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.generation, u.health, slices.Clone(u.addresses)
}

// dueForProbe reports whether the upstream is not being probed and its last probe round started
// its interval minus slack ago; if so, it records now as the start of the next
func (u *Upstream) dueForProbe(now time.Time, slack time.Duration) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.probing || (!u.lastProbe.IsZero() && now.Sub(u.lastProbe) < u.health.Interval-slack) {
		return false
	}
	u.lastProbe, u.probing = now, true
	return true
}

func (u *Upstream) probeDone() {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.probing = false
}

// applyProbes applies a probe round over all addresses: the upstream passes if any address
// passed (so load balancers that list a node's ports in a different order agree on it), and
// the addresses that failed are skipped when picking one until they pass again. Every failed
// address shows in the last error.
func (u *Upstream) applyProbes(hc *config.HealthCheck, generation uint64, addresses []string, results []error) {
	var failed []error
	for i, err := range results {
		if err != nil {
			failed = append(failed, fmt.Errorf("%s: %w", addresses[i], err))
		}
	}
	var err error
	if len(failed) == len(results) {
		err = errors.Join(failed...)
	}
	u.applyProbe(hc, generation, err)
	u.mu.Lock()
	defer u.mu.Unlock()
	if generation != u.generation {
		return
	}
	for i, addr := range addresses {
		u.addrDown[addr] = results[i] != nil
	}
	if err == nil && len(failed) > 0 {
		u.lastError = errors.Join(failed...).Error()
	}
}

// applyProbe applies a probe result with hysteresis: an upstream leaves the rotation after
// hc.Fails failed probes in a row and comes back after hc.Passes good ones, so a slow probe or
// two does not flap it. The very first probe decides right away. A result for a generation of
// addresses that Update replaced since is dropped.
func (u *Upstream) applyProbe(hc *config.HealthCheck, generation uint64, err error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if generation != u.generation {
		return
	}
	u.lastCheck = time.Now()
	u.lastError = ""
	if err != nil {
		u.lastError = err.Error()
	}
	if !u.probed {
		u.probed = true
		u.healthy = err == nil
		if err != nil {
			slog.Warn("Upstream failed its first health check", "site", u.site, "upstream", u.name, "error", err)
		}
		return
	}
	if err == nil {
		u.fails = 0
		u.passes++
		if !u.healthy && u.passes >= hc.Passes {
			slog.Info("Upstream is healthy again", "site", u.site, "upstream", u.name)
			u.healthy = true
		}
	} else {
		u.passes = 0
		u.fails++
		if u.healthy && u.fails >= hc.Fails {
			slog.Warn("Upstream is unhealthy", "site", u.site, "upstream", u.name, "error", err)
			u.healthy = false
		}
	}
}
