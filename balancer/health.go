package balancer

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
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
			u.applyProbe(hc, b.probeOne(ctx, hc, u))
		}()
	}
	wg.Wait()
}

// probeOne checks the upstream's first address: the addresses are usually one node's ports
func (b *Balancer) probeOne(ctx context.Context, hc *config.HealthCheck, u *Upstream) error {
	u.mu.Lock()
	url := fmt.Sprintf("http://%s%s", u.addresses[0], hc.Path)
	u.mu.Unlock()
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

// applyProbe applies a probe result with hysteresis: an upstream leaves the rotation after
// hc.Fails failed probes in a row and comes back after hc.Passes good ones, so a slow probe or
// two does not flap it. The very first probe decides right away.
func (u *Upstream) applyProbe(hc *config.HealthCheck, err error) {
	u.mu.Lock()
	defer u.mu.Unlock()
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
