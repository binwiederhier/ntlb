package balancer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"time"

	"heckel.io/ntfy-lb/config"
)

// healthLoop probes every upstream each health-check interval. The interval is read from the
// current config every round, so a reload changes it.
func (b *Balancer) healthLoop(ctx context.Context) {
	for {
		b.mu.RLock()
		interval := b.config.HealthCheck.Interval
		b.mu.RUnlock()
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
		b.checkHealth(ctx)
	}
}

// checkHealth runs one probe round over every upstream still in the config, in parallel
func (b *Balancer) checkHealth(ctx context.Context) {
	b.mu.RLock()
	hc := b.config.HealthCheck
	upstreams := make([]*Upstream, 0, len(b.upstreams))
	for _, u := range b.upstreams {
		if !u.removed {
			upstreams = append(upstreams, u)
		}
	}
	b.mu.RUnlock()
	b.probe(ctx, hc, upstreams)
}

// probe checks the given upstreams in parallel and applies the results
func (b *Balancer) probe(ctx context.Context, hc *config.HealthCheck, upstreams []*Upstream) {
	var wg sync.WaitGroup
	for _, u := range upstreams {
		wg.Add(1)
		go func() {
			defer wg.Done()
			generation, addresses := u.probeTargets()
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
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	return nil
}

// probeTargets returns the addresses to probe, all of them, and the address generation they
// belong to
func (u *Upstream) probeTargets() (uint64, []string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.generation, slices.Clone(u.addresses)
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
			slog.Warn("Upstream failed its first health check", "upstream", u.name, "error", err)
		}
		return
	}
	if err == nil {
		u.fails = 0
		u.passes++
		if !u.healthy && u.passes >= hc.Passes {
			slog.Info("Upstream is healthy again", "upstream", u.name)
			u.healthy = true
		}
	} else {
		u.passes = 0
		u.fails++
		if u.healthy && u.fails >= hc.Fails {
			slog.Warn("Upstream is unhealthy", "upstream", u.name, "error", err)
			u.healthy = false
		}
	}
}
