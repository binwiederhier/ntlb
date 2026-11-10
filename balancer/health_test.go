package balancer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"heckel.io/ntlb/config"
)

func newProbeUpstream(addr string) *Upstream {
	return newUpstream(testSite, &config.Upstream{
		Name:      "app1",
		Addresses: []string{addr},
	})
}

func TestBalancer_ProbeOne(t *testing.T) {
	hc := &config.HealthCheck{
		Path:    config.DefaultHealthPath,
		Timeout: 200 * time.Millisecond,
		Fails:   config.DefaultHealthFails,
		Passes:  config.DefaultHealthPasses,
	}
	statusServer := func(status int) string {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != config.DefaultHealthPath {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if status == http.StatusMovedPermanently {
				http.Redirect(w, r, "/elsewhere", status) // Not followed, so it fails
				return
			}
			w.WriteHeader(status)
		}))
		t.Cleanup(server.Close)
		return strings.TrimPrefix(server.URL, "http://")
	}
	tests := []struct {
		name   string
		status int
		err    string
	}{
		{"200", http.StatusOK, ""},
		{"204", http.StatusNoContent, ""},
		{"299", 299, ""},
		{"301", http.StatusMovedPermanently, "unexpected status 301"},
		{"404", http.StatusNotFound, "unexpected status 404"},
		{"500", http.StatusInternalServerError, "unexpected status 500"},
		{"503", http.StatusServiceUnavailable, "unexpected status 503"},
	}
	b := New(newTestConfig(newTestUpstream(t, "app1")))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := b.probeOne(context.Background(), hc, statusServer(tt.status))
			if tt.err == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tt.err)
			}
		})
	}

	t.Run("connection refused", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		addr := listener.Addr().String()
		require.NoError(t, listener.Close())
		err = b.probeOne(context.Background(), hc, addr)
		require.Error(t, err)
		var opErr *net.OpError
		require.True(t, errors.As(err, &opErr), err)
	})

	t.Run("timeout", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-r.Context().Done():
			case <-time.After(5 * time.Second):
			}
		}))
		t.Cleanup(server.Close)
		start := time.Now()
		err := b.probeOne(context.Background(), hc, strings.TrimPrefix(server.URL, "http://"))
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Less(t, time.Since(start), 2*time.Second)
	})

	t.Run("canceled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := b.probeOne(ctx, hc, statusServer(http.StatusOK))
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("every address", func(t *testing.T) {
		u := newUpstream(testSite, &config.Upstream{
			Name:      "app1",
			Addresses: []string{"10.0.0.1:1", "10.0.0.1:2"},
		})
		generation, hc, addresses := u.probeTargets()
		require.Same(t, testSite.HealthCheck, hc)
		require.Equal(t, uint64(0), generation)
		require.Equal(t, []string{"10.0.0.1:1", "10.0.0.1:2"}, addresses)
	})

	t.Run("bad address", func(t *testing.T) {
		require.Error(t, b.probeOne(context.Background(), hc, "in valid:%%"))
	})
}

func TestUpstream_ApplyProbeFirstProbeDecides(t *testing.T) {
	// The first probe decides right away, whatever fails and passes say
	hc := &config.HealthCheck{
		Fails:  5,
		Passes: 5,
	}
	u := newProbeUpstream("127.0.0.1:1")
	require.True(t, u.isHealthy()) // Until probed
	u.applyProbe(hc, 0, errors.New("down"))
	require.False(t, u.isHealthy())
	require.Equal(t, "down", u.lastError)
	require.False(t, u.lastCheck.IsZero())

	u = newProbeUpstream("127.0.0.1:1")
	u.applyProbe(hc, 0, nil)
	require.True(t, u.isHealthy())
	require.Empty(t, u.lastError)

	// The first probe does not count towards the hysteresis: 4 more fails keep it healthy
	for i := 0; i < 4; i++ {
		u.applyProbe(hc, 0, errors.New("down"))
	}
	require.True(t, u.isHealthy())
	u.applyProbe(hc, 0, errors.New("down"))
	require.False(t, u.isHealthy())
}

func TestUpstream_ApplyProbeHysteresis(t *testing.T) {
	// Probe sequences ('+' pass, '-' fail) after a first good probe, and the state after each
	tests := []struct {
		name          string
		fails, passes int
		probes        string
		healthy       string // 'h' healthy or 'u' unhealthy after each probe
	}{
		{"fails 1 passes 1", 1, 1, "-+-+", "uhuh"},
		{"fails 3 passes 2", 3, 2, "---++", "hhuuh"},
		{"streak broken by a pass", 3, 2, "--+--+---", "hhhhhhhhu"},
		{"streak broken by a fail", 2, 3, "--++-+++", "huuuuuuh"},
		{"fails 2 passes 4", 2, 4, "--++++", "huuuuh"},
		{"fails 5 passes 1", 5, 1, "-----+", "hhhhuh"},
		{"passes while healthy", 2, 2, "++++--", "hhhhhu"},
		{"fails while unhealthy", 1, 2, "----++", "uuuuuh"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hc := &config.HealthCheck{
				Fails:  tt.fails,
				Passes: tt.passes,
			}
			u := newProbeUpstream("127.0.0.1:1")
			u.applyProbe(hc, 0, nil)
			for i, p := range tt.probes {
				var err error
				if p == '-' {
					err = errors.New("down")
				}
				u.applyProbe(hc, 0, err)
				require.Equal(t, tt.healthy[i] == 'h', u.isHealthy(), "after probe %d of %s", i+1, tt.probes)
				if err != nil {
					require.Equal(t, "down", u.lastError)
				} else {
					require.Empty(t, u.lastError)
				}
			}
		})
	}
}

func TestBalancer_HealthHysteresisFromConfig(t *testing.T) {
	// The probe rounds use fails and passes from the config, also after a reload
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	conf := newTestConfig(app1, app2)
	conf.Sites[0].HealthCheck.Fails = 1
	conf.Sites[0].HealthCheck.Passes = 3
	b := New(conf)
	b.checkHealth(context.Background())
	app2.ready.Store(false)
	b.checkHealth(context.Background())
	require.Equal(t, StateUnhealthy, b.Status().Upstreams[1].State)
	app2.ready.Store(true)
	b.checkHealth(context.Background())
	b.checkHealth(context.Background())
	require.Equal(t, StateUnhealthy, b.Status().Upstreams[1].State)
	b.checkHealth(context.Background())
	require.Equal(t, StateHealthy, b.Status().Upstreams[1].State)

	reloaded := newTestConfig(app1, app2)
	reloaded.Sites[0].HealthCheck.Fails = 2
	reloaded.Sites[0].HealthCheck.Passes = 1
	b.Update(reloaded)
	app2.ready.Store(false)
	b.checkHealth(context.Background())
	require.Equal(t, StateHealthy, b.Status().Upstreams[1].State)
	b.checkHealth(context.Background())
	require.Equal(t, StateUnhealthy, b.Status().Upstreams[1].State)
	app2.ready.Store(true)
	b.checkHealth(context.Background())
	require.Equal(t, StateHealthy, b.Status().Upstreams[1].State)
}

func TestBalancer_CheckHealthSkipsRemoved(t *testing.T) {
	// A removed upstream that still drains is not probed any more: its state stays as it was
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	b := newTestBalancer(t, app1, app2)
	conf := newTestConfig(app1)
	conf.DrainDuration = time.Hour
	b.Update(conf)
	lastCheck := *b.Status().Upstreams[1].LastCheck
	app2.server.Close()
	for i := 0; i < 5; i++ {
		b.checkHealth(context.Background())
	}
	status := b.Status().Upstreams[1]
	require.True(t, status.Healthy)
	require.Empty(t, status.LastError)
	require.Equal(t, lastCheck, *status.LastCheck)
}

func TestBalancer_HealthLoop(t *testing.T) {
	// The loop probes every interval, and returns when ctx is canceled
	var probes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		probes.Add(1)
	}))
	t.Cleanup(server.Close)
	app1 := &testUpstream{name: "app1", server: server}
	conf := newTestConfig(app1)
	conf.Sites[0].HealthCheck.Interval = 10 * time.Millisecond
	b := New(conf)
	ctx, cancel := context.WithCancel(context.Background())
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		b.healthLoop(ctx)
	}()
	require.Eventually(t, func() bool {
		return probes.Load() >= 3
	}, 3*time.Second, 5*time.Millisecond)
	cancel()
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("health loop did not stop")
	}
	n := probes.Load()
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, n, probes.Load())
}

func TestBalancer_HealthLoopStopsWhileWaiting(t *testing.T) {
	// Canceling ctx ends the wait for the next round, even with a long interval
	app1 := newTestUpstream(t, "app1")
	b := New(newTestConfig(app1)) // Interval 1h
	ctx, cancel := context.WithCancel(context.Background())
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		b.healthLoop(ctx)
	}()
	cancel()
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("health loop did not stop")
	}
}

func TestBalancer_ProbeOfReplacedAddressIsDiscarded(t *testing.T) {
	// A probe of an upstream's old address that is still in flight when Update replaces the
	// address must not decide the health of the new one
	var slow atomic.Bool
	probing, release := make(chan struct{}), make(chan struct{})
	old := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if slow.Load() {
			close(probing)
			<-release
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	t.Cleanup(old.Close)
	app1 := newTestUpstream(t, "app1")
	app2old := &testUpstream{name: "app2", server: old}
	conf := newTestConfig(app1, app2old)
	conf.Sites[0].HealthCheck.Fails = 1 // One failed probe would take it out
	b := New(conf)
	b.checkHealth(context.Background())
	slow.Store(true)
	checked := make(chan struct{})
	go func() {
		defer close(checked)
		b.checkHealth(context.Background())
	}()
	<-probing
	app2new := newTestUpstream(t, "app2")
	reloaded := newTestConfig(app1, app2new)
	reloaded.Sites[0].HealthCheck.Fails = 1
	b.Update(reloaded)
	close(release)
	<-checked
	status := b.Status().Upstreams[1]
	require.Equal(t, []string{app2new.addr()}, status.Addresses)
	require.Equal(t, StateHealthy, status.State)
	require.Empty(t, status.LastError)
}

func TestUpstream_ApplyProbeDropsOldGeneration(t *testing.T) {
	hc := &config.HealthCheck{
		Fails:  1,
		Passes: 1,
	}
	u := newProbeUpstream("127.0.0.1:1")
	u.applyProbe(hc, 0, nil)
	u.generation++
	u.applyProbe(hc, 0, errors.New("old address down"))
	require.True(t, u.isHealthy())
	require.Empty(t, u.lastError)
	u.applyProbe(hc, 1, errors.New("new address down"))
	require.False(t, u.isHealthy())
}

func TestBalancer_HealthProbesEveryAddress(t *testing.T) {
	// An upstream is healthy if any of its addresses is, whatever their order, so two load
	// balancers that list a node's ports in a different order agree on it; a dead address is
	// skipped when picking one
	live := newTestUpstream(t, "app1")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	dead := ln.Addr().String()
	ln.Close()
	for _, order := range [][]string{{dead, live.addr()}, {live.addr(), dead}} {
		conf, err := config.Parse([]byte(fmt.Sprintf("sites:\n  - name: s\n    listen-http: ':0'\n    health-check: {interval: 1h, timeout: 1s}\n    upstreams:\n      - name: app1\n        addresses: [%q, %q]\n", order[0], order[1])))
		require.NoError(t, err)
		b := New(conf)
		b.checkHealth(context.Background())
		status := b.Status().Upstreams[0]
		require.True(t, status.Healthy, order)
		require.Contains(t, status.LastError, dead, order) // The dead address is reported
		u := b.upstream("app1")
		for i := 0; i < 5; i++ {
			s := b.NewSession(func(error) {})
			require.Equal(t, live.addr(), b.Reserve(s, u), order)
		}
	}

	// All addresses down: unhealthy
	live.ready.Store(false)
	conf, err := config.Parse([]byte(fmt.Sprintf("sites:\n  - name: s\n    listen-http: ':0'\n    health-check: {interval: 1h, timeout: 1s}\n    upstreams:\n      - name: app1\n        addresses: [%q, %q]\n", dead, live.addr())))
	require.NoError(t, err)
	b := New(conf)
	b.checkHealth(context.Background())
	require.False(t, b.Status().Upstreams[0].Healthy)
}

func TestBalancer_HealthCheckPerSite(t *testing.T) {
	// One node, two sites that check it on their own paths and at their own intervals
	var defaultProbes, otherProbes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/other" {
			otherProbes.Add(1)
			return
		}
		defaultProbes.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(server.Close)
	addr := strings.TrimPrefix(server.URL, "http://")
	conf := mustParse(t, fmt.Sprintf(`
sites:
  - {name: a, listen-http: ':0', hosts: [a.com], health-check: {interval: 10ms, timeout: 5ms}, upstreams: [{name: x, addresses: [%q]}]}
  - {name: b, listen-http: ':0', health-check: {path: /other, interval: 1h, timeout: 1s}, upstreams: [{name: x, addresses: [%q]}]}
`, addr, addr))
	b := New(conf)
	b.checkHealth(context.Background())
	status := b.Status()
	require.False(t, status.Upstreams[0].Healthy)
	require.True(t, status.Upstreams[1].Healthy)
	require.Equal(t, int32(1), otherProbes.Load())

	// The loop probes each site at its interval
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.healthLoop(ctx)
	require.Eventually(t, func() bool {
		return defaultProbes.Load() >= 5
	}, 3*time.Second, 5*time.Millisecond)
	require.Equal(t, int32(1), otherProbes.Load())
}

// testSite is a site for upstreams made without a config
var (
	testSite = &config.Site{
		Name: "s",
		HealthCheck: &config.HealthCheck{
			Path:     config.DefaultHealthPath,
			Interval: time.Hour,
			Timeout:  time.Second,
			Fails:    config.DefaultHealthFails,
			Passes:   config.DefaultHealthPasses,
		},
	}
)

func TestBalancer_SlowSiteDoesNotDelayOthers(t *testing.T) {
	// A site whose probes hang until their timeout does not hold up the probes of another site
	var fast atomic.Int32
	fastServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fast.Add(1)
	}))
	t.Cleanup(fastServer.Close)
	release := make(chan struct{})
	slowServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(slowServer.Close)
	t.Cleanup(func() {
		close(release)
	})
	conf := mustParse(t, fmt.Sprintf(`
sites:
  - {name: a, listen-http: ':0', hosts: [a.com], health-check: {interval: 10ms, timeout: 5ms}, upstreams: [{name: x, addresses: [%q]}]}
  - {name: b, listen-http: ':0', health-check: {interval: 1h, timeout: 5s}, upstreams: [{name: x, addresses: [%q]}]}
`, strings.TrimPrefix(fastServer.URL, "http://"), strings.TrimPrefix(slowServer.URL, "http://")))
	b := New(conf)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.healthLoop(ctx)
	require.Eventually(t, func() bool {
		return fast.Load() >= 5
	}, time.Second, 5*time.Millisecond)
}
