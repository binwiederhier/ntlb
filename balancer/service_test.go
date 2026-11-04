package balancer

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"heckel.io/ntfy-lb/config"
)

// testUpstream is a fake ntfy node whose /v1/ready can be switched off
type testUpstream struct {
	name   string
	server *httptest.Server
	ready  atomic.Bool
}

// testSession records the cause it was canceled with
type testSession struct {
	*Session
	cause error
	mu    sync.Mutex
}

func (s *testSession) canceled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cause != nil
}

func newTestUpstream(t *testing.T, name string) *testUpstream {
	u := &testUpstream{name: name}
	u.ready.Store(true)
	u.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == config.DefaultHealthPath && !u.ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	t.Cleanup(u.server.Close)
	return u
}

func (u *testUpstream) addr() string {
	return strings.TrimPrefix(u.server.URL, "http://")
}

func newTestConfig(upstreams ...*testUpstream) *config.Config {
	var b strings.Builder
	b.WriteString("listen-http: ':0'\nhealth-check: {interval: 1h, timeout: 1s}\nupstreams:\n")
	for _, u := range upstreams {
		fmt.Fprintf(&b, "  - name: %s\n    addresses: [%q]\n", u.name, u.addr())
	}
	conf, err := config.Parse([]byte(b.String()))
	if err != nil {
		panic(err)
	}
	return conf
}

func newTestBalancer(t *testing.T, upstreams ...*testUpstream) *Balancer {
	b := New(newTestConfig(upstreams...))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	b.Start(ctx)
	return b
}

func newTestSession(b *Balancer, upstreamName string) *testSession {
	s := &testSession{}
	s.Session = b.NewSession(func(cause error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.cause = cause
	})
	if upstreamName != "" {
		u := b.upstream(upstreamName)
		b.Attach(s.Session, u, u.NextAddress())
	}
	return s
}

func countCanceled(sessions []*testSession) int {
	n := 0
	for _, s := range sessions {
		if s.canceled() {
			n++
		}
	}
	return n
}

func names(upstreams []*Upstream) []string {
	n := make([]string, len(upstreams))
	for i, u := range upstreams {
		n[i] = u.Name()
	}
	return n
}

func keyFor(i int) []byte {
	return ClientKey(netip.AddrFrom4([4]byte{10, byte(i >> 16), byte(i >> 8), byte(i)}))
}

// keyPreferring returns a client key whose first choice is the given upstream
func keyPreferring(b *Balancer, name string) []byte {
	for i := 0; ; i++ {
		if key := keyFor(i); b.Pick(key)[0].Name() == name {
			return key
		}
	}
}

func TestBalancer_PickSameAcrossInstances(t *testing.T) {
	// Two load balancers with the same upstream names, but different addresses and order, route
	// every client to the same upstream
	app1, app2, app3 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2"), newTestUpstream(t, "app3")
	b1 := newTestBalancer(t, app1, app2, app3)
	app1b, app2b, app3b := newTestUpstream(t, "app1"), newTestUpstream(t, "app2"), newTestUpstream(t, "app3")
	b2 := newTestBalancer(t, app3b, app1b, app2b)
	seen := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		key := keyFor(i)
		require.Equal(t, names(b1.Pick(key)), names(b2.Pick(key)))
		require.Equal(t, names(b1.Pick(key)), names(b1.Pick(key)))
		seen[b1.Pick(key)[0].Name()] = true
	}
	require.Len(t, seen, 3)
}

func TestBalancer_InitialProbe(t *testing.T) {
	// An upstream that fails the very first probe never takes traffic, e.g. a draining node
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	app2.ready.Store(false)
	b := newTestBalancer(t, app1, app2)
	for i := 0; i < 100; i++ {
		require.Equal(t, []string{"app1"}, names(b.Pick(keyFor(i))))
	}
	require.Equal(t, StateUnhealthy, b.Status().Upstreams[1].State)
}

func TestBalancer_HealthHysteresis(t *testing.T) {
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	b := newTestBalancer(t, app1, app2)
	key := keyPreferring(b, "app2")

	// Leaves after 3 failed probes in a row
	app2.ready.Store(false)
	b.checkHealth(context.Background())
	b.checkHealth(context.Background())
	require.Equal(t, "app2", b.Pick(key)[0].Name())
	b.checkHealth(context.Background())
	require.Equal(t, []string{"app1"}, names(b.Pick(key)))

	// Comes back after 2 good ones
	app2.ready.Store(true)
	b.checkHealth(context.Background())
	require.Equal(t, []string{"app1"}, names(b.Pick(key)))
	b.checkHealth(context.Background())
	require.Equal(t, "app2", b.Pick(key)[0].Name())
}

func TestBalancer_FailOpen(t *testing.T) {
	// With no healthy upstream, every upstream is a candidate rather than none
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	app1.ready.Store(false)
	app2.ready.Store(false)
	b := newTestBalancer(t, app1, app2)
	require.Len(t, b.Pick(keyFor(1)), 2)
	require.False(t, b.Healthy())
	require.False(t, b.Status().Ready)
}

func TestBalancer_DrainUpstreamPaced(t *testing.T) {
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	b := newTestBalancer(t, app1, app2)
	var sessions1, sessions2 []*testSession
	for i := 0; i < 10; i++ {
		sessions1 = append(sessions1, newTestSession(b, "app1"))
		sessions2 = append(sessions2, newTestSession(b, "app2"))
	}

	// Draining takes it out of rotation right away
	start := time.Now()
	require.NoError(t, b.DrainUpstream("app1", 10*time.Second))
	for i := 0; i < 100; i++ {
		require.Equal(t, []string{"app2"}, names(b.Pick(keyFor(i))))
	}
	require.Equal(t, StateDraining, b.Status().Upstreams[0].State)

	// Its streams are closed a few at a time, oldest first, all of them by the deadline
	b.drainTick(start.Add(time.Second))
	n := countCanceled(sessions1)
	require.Greater(t, n, 0)
	require.Less(t, n, 10)
	require.True(t, sessions1[0].canceled())
	require.False(t, sessions1[9].canceled())
	b.drainTick(start.Add(5 * time.Second))
	require.Greater(t, countCanceled(sessions1), n)
	b.drainTick(start.Add(10 * time.Second))
	require.Equal(t, 10, countCanceled(sessions1))
	require.Equal(t, 0, countCanceled(sessions2))
	for _, s := range sessions1 {
		require.ErrorIs(t, s.cause, ErrDrained)
	}

	// Undraining puts it back
	require.NoError(t, b.UndrainUpstream("app1"))
	require.Equal(t, StateHealthy, b.Status().Upstreams[0].State)
	keyPreferring(b, "app1")
	require.Error(t, b.DrainUpstream("nope", time.Second))
}

func TestBalancer_UpdateRemovesUpstream(t *testing.T) {
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	b := newTestBalancer(t, app1, app2)
	s1, s2 := newTestSession(b, "app1"), newTestSession(b, "app2")
	conf := newTestConfig(app1)
	conf.DrainDuration = 10 * time.Second
	start := time.Now()
	require.NoError(t, b.Update(conf))

	// Out of rotation, but still listed and its stream still running
	for i := 0; i < 100; i++ {
		require.Equal(t, []string{"app1"}, names(b.Pick(keyFor(i))))
	}
	status := b.Status()
	require.Len(t, status.Upstreams, 2)
	require.True(t, status.Upstreams[1].Removed)
	require.Equal(t, StateDraining, status.Upstreams[1].State)
	require.False(t, s2.canceled())

	// Gone once it is drained and its last stream closed
	b.drainTick(start.Add(10 * time.Second))
	require.True(t, s2.canceled())
	require.False(t, s1.canceled())
	b.Close(s2.Session)
	b.drainTick(start.Add(11 * time.Second))
	require.Len(t, b.Status().Upstreams, 1)
}

func TestBalancer_UpdateReaddsDrainingUpstream(t *testing.T) {
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	b := newTestBalancer(t, app1, app2)
	require.NoError(t, b.Update(newTestConfig(app1)))
	require.NoError(t, b.Update(newTestConfig(app1, app2)))
	require.Equal(t, StateHealthy, b.Status().Upstreams[1].State)
	keyPreferring(b, "app2")
}

func TestBalancer_UpdateAddsUpstream(t *testing.T) {
	// A new upstream is probed before it takes traffic
	app1, app2, app3 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2"), newTestUpstream(t, "app3")
	app3.ready.Store(false)
	b := newTestBalancer(t, app1)
	require.NoError(t, b.Update(newTestConfig(app1, app2, app3)))
	keyPreferring(b, "app2")
	for i := 0; i < 100; i++ {
		require.NotContains(t, names(b.Pick(keyFor(i))), "app3")
	}
}

func TestBalancer_UpdateKeepsRuntimeDrain(t *testing.T) {
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	b := newTestBalancer(t, app1, app2)
	require.NoError(t, b.DrainUpstream("app1", time.Minute))
	require.NoError(t, b.Update(newTestConfig(app1, app2)))
	require.Equal(t, StateDraining, b.Status().Upstreams[0].State)
}

func TestBalancer_Drain(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	b := newTestBalancer(t, app1)
	var sessions []*testSession
	for i := 0; i < 10; i++ {
		sessions = append(sessions, newTestSession(b, "app1"))
	}
	require.True(t, b.Status().Ready)
	start := time.Now()
	b.Drain(10 * time.Second)
	require.False(t, b.Status().Ready)
	require.True(t, b.Status().Draining)
	require.False(t, b.Drained())
	b.drainTick(start.Add(time.Second))
	require.Greater(t, countCanceled(sessions), 0)
	require.Less(t, countCanceled(sessions), 10)
	b.drainTick(start.Add(11 * time.Second))
	require.Equal(t, 10, countCanceled(sessions))
	require.True(t, b.Drained())
	b.Undrain()
	require.True(t, b.Status().Ready)
	require.False(t, b.Drained())
}

func TestBalancer_WaitIdle(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	b := newTestBalancer(t, app1)
	s := newTestSession(b, "app1")
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	require.True(t, errors.Is(b.WaitIdle(ctx), context.DeadlineExceeded))
	b.Close(s.Session)
	require.NoError(t, b.WaitIdle(context.Background()))
}

func TestBalancer_NextAddressLeastLoaded(t *testing.T) {
	b := New(mustParse(t, "listen-http: ':0'\nupstreams: [{name: app1, addresses: ['10.0.0.1:1', '10.0.0.1:2']}]"))
	u := b.upstream("app1")
	s1 := b.NewSession(func(error) {})
	a1 := u.NextAddress()
	b.Attach(s1, u, a1)
	a2 := u.NextAddress()
	require.NotEqual(t, a1, a2)
	b.Attach(b.NewSession(func(error) {}), u, a2)
	b.Close(s1)
	require.Equal(t, a1, u.NextAddress())
	require.Equal(t, 1, b.Status().Upstreams[0].Streams)
}

func mustParse(t *testing.T, s string) *config.Config {
	conf, err := config.Parse([]byte(s))
	require.NoError(t, err)
	return conf
}

func TestBalancer_DrainSpreadsFewStreams(t *testing.T) {
	// Fewer streams than ticks are spread over the whole drain, not closed one per tick upfront
	app1 := newTestUpstream(t, "app1")
	b := newTestBalancer(t, app1)
	var sessions []*testSession
	for i := 0; i < 4; i++ {
		sessions = append(sessions, newTestSession(b, "app1"))
	}
	start := time.Now()
	require.NoError(t, b.DrainUpstream("app1", 100*time.Second))
	closedAt := make(map[int]int) // Second -> canceled so far
	for sec := 1; sec <= 101; sec++ {
		b.drainTick(start.Add(time.Duration(sec) * time.Second))
		closedAt[sec] = countCanceled(sessions)
	}
	require.Equal(t, 0, closedAt[5])
	require.Equal(t, 1, closedAt[20])
	require.Equal(t, 2, closedAt[45])
	require.Equal(t, 3, closedAt[70])
	require.Equal(t, 4, closedAt[101])
}
