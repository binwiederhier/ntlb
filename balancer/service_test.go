package balancer

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"heckel.io/ntlb/config"
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
	return newTestConfigBalance(config.BalanceHash, upstreams...)
}

// newTestConfigBalance is a site "s" with the upstreams and the given balance
func newTestConfigBalance(balance string, upstreams ...*testUpstream) *config.Config {
	var b strings.Builder
	b.WriteString("sites:\n  - name: s\n    listen-http: ':0'\n    upstream:\n      balance: " + balance + "\n      health-check: {interval: 1h, timeout: 1s}\n      servers:\n")
	for _, u := range upstreams {
		fmt.Fprintf(&b, "      - name: %s\n        addresses: [%q]\n", u.name, u.addr())
	}
	conf, err := config.Parse([]byte(b.String()))
	if err != nil {
		panic(err)
	}
	return conf
}

// newTestBalancer runs the first health round like Start, but not the loops: a real drain tick
// in between the fake-time drainTick calls of a test would move the drain's last tick around
func newTestBalancer(t *testing.T, upstreams ...*testUpstream) *Balancer {
	b := New(newTestConfig(upstreams...))
	b.checkHealth(context.Background())
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
	return ClientKey(netip.AddrFrom4([4]byte{10, byte(i >> 16), byte(i >> 8), byte(i)}), 32, 64)
}

// keyPreferring returns a client key whose first choice is the given upstream
func keyPreferring(b *Balancer, name string) []byte {
	for i := 0; ; i++ {
		if key := keyFor(i); b.Pick("s", key)[0].Name() == name {
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
		require.Equal(t, names(b1.Pick("s", key)), names(b2.Pick("s", key)))
		require.Equal(t, names(b1.Pick("s", key)), names(b1.Pick("s", key)))
		seen[b1.Pick("s", key)[0].Name()] = true
	}
	require.Len(t, seen, 3)
}

func TestBalancer_InitialProbe(t *testing.T) {
	// An upstream that fails the very first probe never takes traffic, e.g. a draining node
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	app2.ready.Store(false)
	b := newTestBalancer(t, app1, app2)
	for i := 0; i < 100; i++ {
		require.Equal(t, []string{"app1"}, names(b.Pick("s", keyFor(i))))
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
	require.Equal(t, "app2", b.Pick("s", key)[0].Name())
	b.checkHealth(context.Background())
	require.Equal(t, []string{"app1"}, names(b.Pick("s", key)))

	// Comes back after 2 good ones
	app2.ready.Store(true)
	b.checkHealth(context.Background())
	require.Equal(t, []string{"app1"}, names(b.Pick("s", key)))
	b.checkHealth(context.Background())
	require.Equal(t, "app2", b.Pick("s", key)[0].Name())
}

func TestBalancer_FailOpen(t *testing.T) {
	// With no healthy upstream, every upstream is a candidate rather than none
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	app1.ready.Store(false)
	app2.ready.Store(false)
	b := newTestBalancer(t, app1, app2)
	require.Len(t, b.Pick("s", keyFor(1)), 2)
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
		require.Equal(t, []string{"app2"}, names(b.Pick("s", keyFor(i))))
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
	b.Update(conf)

	// Out of rotation, but still listed and its stream still running
	for i := 0; i < 100; i++ {
		require.Equal(t, []string{"app1"}, names(b.Pick("s", keyFor(i))))
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
	b.Update(newTestConfig(app1))
	b.Update(newTestConfig(app1, app2))
	require.Equal(t, StateHealthy, b.Status().Upstreams[1].State)
	keyPreferring(b, "app2")
}

func TestBalancer_UpdateAddsUpstream(t *testing.T) {
	// A new upstream is probed before it takes traffic
	app1, app2, app3 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2"), newTestUpstream(t, "app3")
	app3.ready.Store(false)
	b := newTestBalancer(t, app1)
	b.Update(newTestConfig(app1, app2, app3))
	keyPreferring(b, "app2")
	for i := 0; i < 100; i++ {
		require.NotContains(t, names(b.Pick("s", keyFor(i))), "app3")
	}
}

func TestBalancer_UpdateKeepsRuntimeDrain(t *testing.T) {
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	b := newTestBalancer(t, app1, app2)
	require.NoError(t, b.DrainUpstream("app1", time.Minute))
	b.Update(newTestConfig(app1, app2))
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
	b := New(mustParse(t, "sites: [{name: s, listen-http: ':0', upstream: {servers: [{name: app1, addresses: ['10.0.0.1:1', '10.0.0.1:2']}]}}]"))
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

func TestBalancer_RecordAddressFailureCounts(t *testing.T) {
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	b := newTestBalancer(t, app1, app2)
	u1 := b.upstream("app1")
	for range 3 {
		b.RecordAddressFailure(u1, u1.NextAddress())
	}
	newTestSession(b, "app1")
	status := b.Status()
	require.Equal(t, uint64(3), status.Upstreams[0].Failures)
	require.Equal(t, uint64(1), status.Upstreams[0].Requests)
	require.Equal(t, uint64(0), status.Upstreams[1].Failures)
	require.Equal(t, uint64(0), status.Upstreams[1].Requests)
}

func TestBalancer_StatusUpstreamStates(t *testing.T) {
	app1, app2, app3, app4 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2"), newTestUpstream(t, "app3"), newTestUpstream(t, "app4")
	app2.ready.Store(false)
	b := newTestBalancer(t, app1, app2, app3, app4)
	newTestSession(b, "app1")
	newTestSession(b, "app3")
	newTestSession(b, "app3")
	newTestSession(b, "") // Not attached (yet): counts for the load balancer only
	before := time.Now()
	require.NoError(t, b.DrainUpstream("app3", time.Minute))
	require.NoError(t, b.DrainUpstream("app4", time.Second))
	after := time.Now()
	b.drainTick(after.Add(time.Second)) // app4 is drained, app3 is not
	status := b.Status()

	// Load balancer
	require.True(t, status.Healthy)
	require.True(t, status.Ready)
	require.False(t, status.Draining)
	require.False(t, status.Drained)
	require.Nil(t, status.DrainDeadline)
	require.Equal(t, 4, status.Streams)
	require.Equal(t, []string{"app1", "app2", "app3", "app4"}, []string{status.Upstreams[0].Name, status.Upstreams[1].Name, status.Upstreams[2].Name, status.Upstreams[3].Name})

	// Healthy
	s := status.Upstreams[0]
	require.Equal(t, StateHealthy, s.State)
	require.True(t, s.Healthy)
	require.False(t, s.Removed)
	require.Equal(t, []string{app1.addr()}, s.Addresses)
	require.Equal(t, 1, s.Streams)
	require.Nil(t, s.DrainDeadline)
	require.NotNil(t, s.LastCheck)
	require.Empty(t, s.LastError)

	// Unhealthy
	s = status.Upstreams[1]
	require.Equal(t, StateUnhealthy, s.State)
	require.False(t, s.Healthy)
	require.Contains(t, s.LastError, ": unexpected status 503") // Prefixed with the address
	require.Nil(t, s.DrainDeadline)

	// Draining, with its deadline
	s = status.Upstreams[2]
	require.Equal(t, StateDraining, s.State)
	require.True(t, s.Healthy)
	require.Equal(t, 2, s.Streams)
	require.NotNil(t, s.DrainDeadline)
	require.False(t, s.DrainDeadline.Before(before.Add(time.Minute)))
	require.False(t, s.DrainDeadline.After(after.Add(time.Minute)))

	// Drained, still with its deadline
	s = status.Upstreams[3]
	require.Equal(t, StateDrained, s.State)
	require.Equal(t, 0, s.Streams)
	require.NotNil(t, s.DrainDeadline)

	// A drained upstream that gets a (late) stream is draining again until it is closed
	late := newTestSession(b, "app4")
	require.Equal(t, StateDraining, b.Status().Upstreams[3].State)
	b.Close(late.Session)
	require.Equal(t, StateDrained, b.Status().Upstreams[3].State)

	// A draining upstream that is also unhealthy is draining
	app3.ready.Store(false)
	for i := 0; i < 3; i++ {
		b.checkHealth(context.Background())
	}
	s = b.Status().Upstreams[2]
	require.Equal(t, StateDraining, s.State)
	require.False(t, s.Healthy)
}

func TestBalancer_StatusLoadBalancerDrain(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	b := newTestBalancer(t, app1)
	before := time.Now()
	b.Drain(time.Minute)
	after := time.Now()
	status := b.Status()
	require.True(t, status.Healthy)
	require.False(t, status.Ready)
	require.True(t, status.Draining)
	require.False(t, status.Drained)
	require.NotNil(t, status.DrainDeadline)
	require.False(t, status.DrainDeadline.Before(before.Add(time.Minute)))
	require.False(t, status.DrainDeadline.After(after.Add(time.Minute)))
	require.Equal(t, StateHealthy, status.Upstreams[0].State) // The upstreams are not draining

	// A snapshot does not change when the drain does
	deadline := *status.DrainDeadline
	b.Drain(time.Hour)
	require.Equal(t, deadline, *status.DrainDeadline)
	b.drainTick(time.Now().Add(2 * time.Hour))
	status = b.Status()
	require.True(t, status.Drained)
	require.NotNil(t, status.DrainDeadline)
	b.Undrain()
	status = b.Status()
	require.False(t, status.Draining)
	require.False(t, status.Drained)
	require.Nil(t, status.DrainDeadline)
	require.True(t, status.Ready)
}

func TestBalancer_Draining(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	b := newTestBalancer(t, app1)
	require.False(t, b.Draining())
	require.False(t, b.Drained())
	b.Drain(time.Second)
	require.True(t, b.Draining())
	require.False(t, b.Drained())
	b.drainTick(time.Now().Add(2 * time.Second))
	require.True(t, b.Draining())
	require.True(t, b.Drained())
	b.Undrain()
	require.False(t, b.Draining())
	require.False(t, b.Drained())
	b.Undrain() // Undraining when not draining is fine
	require.False(t, b.Draining())

	// An upstream drain is not a load balancer drain
	require.NoError(t, b.DrainUpstream("app1", time.Second))
	require.False(t, b.Draining())
}

func TestBalancer_CloseTwiceAndUnattached(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	b := newTestBalancer(t, app1)
	s1, s2, s3 := newTestSession(b, "app1"), newTestSession(b, "app1"), newTestSession(b, "")
	require.Equal(t, 3, b.Status().Streams)
	require.Equal(t, 2, b.Status().Upstreams[0].Streams)

	// Closing twice does not count twice
	b.Close(s1.Session)
	b.Close(s1.Session)
	require.Equal(t, 2, b.Status().Streams)
	require.Equal(t, 1, b.Status().Upstreams[0].Streams)

	// A session that never attached only counts for the load balancer
	b.Close(s3.Session)
	b.Close(s3.Session)
	require.Equal(t, 1, b.Status().Streams)
	require.Equal(t, 1, b.Status().Upstreams[0].Streams)
	b.Close(s2.Session)
	status := b.Status()
	require.Equal(t, 0, status.Streams)
	require.Equal(t, 0, status.Upstreams[0].Streams)
	require.NoError(t, b.WaitIdle(context.Background()))
}

func TestBalancer_DrainUndrainUpstreamUnknown(t *testing.T) {
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	b := newTestBalancer(t, app1, app2)
	require.ErrorIs(t, b.DrainUpstream("nope", time.Second), ErrUpstreamNotFound)
	require.ErrorIs(t, b.UndrainUpstream("nope"), ErrUpstreamNotFound)
	require.ErrorIs(t, b.DrainUpstream("", time.Second), ErrUpstreamNotFound)

	// A removed upstream is not found either, also while it still drains
	conf := newTestConfig(app1)
	conf.DrainDuration = time.Minute
	b.Update(conf)
	require.Len(t, b.Status().Upstreams, 2)
	require.ErrorIs(t, b.DrainUpstream("app2", time.Second), ErrUpstreamNotFound)
	require.ErrorIs(t, b.UndrainUpstream("app2"), ErrUpstreamNotFound)
	require.Equal(t, StateDraining, b.Status().Upstreams[1].State)
}

func TestBalancer_UndrainUpstreamNotDraining(t *testing.T) {
	// Undraining an upstream that is not draining changes nothing
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	app2.ready.Store(false)
	b := newTestBalancer(t, app1, app2)
	s := newTestSession(b, "app1")
	require.NoError(t, b.UndrainUpstream("app1"))
	require.NoError(t, b.UndrainUpstream("app2"))
	b.drainTick(time.Now().Add(time.Hour))
	require.False(t, s.canceled())
	status := b.Status()
	require.Equal(t, StateHealthy, status.Upstreams[0].State)
	require.Equal(t, StateUnhealthy, status.Upstreams[1].State) // Undrain does not make it healthy
	require.Equal(t, 1, status.Upstreams[0].Streams)
}

func TestBalancer_PickExcludesDrainingAndUnhealthy(t *testing.T) {
	app1, app2, app3, app4, app5 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2"), newTestUpstream(t, "app3"), newTestUpstream(t, "app4"), newTestUpstream(t, "app5")
	app2.ready.Store(false)
	b := newTestBalancer(t, app1, app2, app3, app4, app5)
	require.NoError(t, b.DrainUpstream("app1", time.Minute))
	all := []string{"app1", "app2", "app3", "app4", "app5"}
	for i := 0; i < 500; i++ {
		key := keyFor(i)
		var want []string
		for _, name := range rank(key, all) {
			if name != "app1" && name != "app2" {
				want = append(want, name)
			}
		}
		require.Equal(t, want, names(b.Pick("s", key)))
	}
}

func TestBalancer_PickFailOpenExcludesDraining(t *testing.T) {
	// Failing open returns the unhealthy upstreams in rank order, but never a draining one
	app1, app2, app3 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2"), newTestUpstream(t, "app3")
	app1.ready.Store(false)
	app2.ready.Store(false)
	b := newTestBalancer(t, app1, app2, app3)
	require.NoError(t, b.DrainUpstream("app3", time.Minute))
	require.False(t, b.Healthy())
	for i := 0; i < 200; i++ {
		key := keyFor(i)
		require.Equal(t, rank(key, []string{"app1", "app2"}), names(b.Pick("s", key)))
	}
}

func TestBalancer_PickFailOpenAtRuntime(t *testing.T) {
	// Upstreams that turn unhealthy one after the other: once the last one is gone, all of them
	// are candidates again, and the first one back takes everything
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	b := newTestBalancer(t, app1, app2)
	app1.ready.Store(false)
	app2.ready.Store(false)
	for i := 0; i < 3; i++ {
		b.checkHealth(context.Background())
	}
	require.False(t, b.Healthy())
	key := keyFor(1)
	require.Equal(t, rank(key, []string{"app1", "app2"}), names(b.Pick("s", key)))
	app2.ready.Store(true)
	b.checkHealth(context.Background())
	b.checkHealth(context.Background())
	require.True(t, b.Healthy())
	for i := 0; i < 100; i++ {
		require.Equal(t, []string{"app2"}, names(b.Pick("s", keyFor(i))))
	}
}

func TestBalancer_PickNothingWhenAllDraining(t *testing.T) {
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	app2.ready.Store(false)
	b := newTestBalancer(t, app1, app2)
	require.NoError(t, b.DrainUpstream("app1", time.Minute))
	require.NoError(t, b.DrainUpstream("app2", time.Minute))
	for i := 0; i < 100; i++ {
		require.Empty(t, b.Pick("s", keyFor(i)))
	}
	require.False(t, b.Healthy())
	require.False(t, b.Status().Ready)

	// Removed upstreams count as draining
	conf := newTestConfig(app1)
	b.Update(conf)
	require.Empty(t, b.Pick("s", keyFor(1)))
	require.NoError(t, b.UndrainUpstream("app1"))
	require.Equal(t, []string{"app1"}, names(b.Pick("s", keyFor(1))))
}

func TestBalancer_PickIgnoresLoadBalancerDrain(t *testing.T) {
	// A draining load balancer still routes the requests it serves (publishes)
	app1 := newTestUpstream(t, "app1")
	b := newTestBalancer(t, app1)
	b.Drain(time.Minute)
	require.Equal(t, []string{"app1"}, names(b.Pick("s", keyFor(1))))
}

func TestBalancer_UpdateRemoveAndReaddMidDrain(t *testing.T) {
	// Re-adding a removed upstream mid-drain cancels the drain: the streams still open stay open
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	b := newTestBalancer(t, app1, app2)
	u2 := b.upstream("app2")
	var sessions []*testSession
	for i := 0; i < 10; i++ {
		sessions = append(sessions, newTestSession(b, "app2"))
	}
	conf := newTestConfig(app1)
	conf.DrainDuration = 10 * time.Second
	b.Update(conf)
	start := b.Status().Upstreams[1].DrainDeadline.Add(-10 * time.Second)
	b.drainTick(start.Add(5 * time.Second))
	closed := countCanceled(sessions)
	require.InDelta(t, 5, closed, 1)
	for _, s := range sessions {
		if s.canceled() {
			b.Close(s.Session)
		}
	}
	b.Update(newTestConfig(app1, app2))
	for sec := 6; sec <= 30; sec++ {
		b.drainTick(start.Add(time.Duration(sec) * time.Second))
	}
	require.Equal(t, closed, countCanceled(sessions))
	require.Same(t, u2, b.upstream("app2"))
	status := b.Status().Upstreams[1]
	require.Equal(t, StateHealthy, status.State)
	require.False(t, status.Removed)
	require.Nil(t, status.DrainDeadline)
	require.Equal(t, 10-closed, status.Streams)
	keyPreferring(b, "app2")

	// Removing it again starts a new drain, with the new config's duration
	conf = newTestConfig(app1)
	conf.DrainDuration = time.Hour
	b.Update(conf)
	status = b.Status().Upstreams[1]
	require.True(t, status.Removed)
	require.True(t, status.DrainDeadline.After(time.Now().Add(59*time.Minute)))
}

func TestBalancer_UpdateRemovesUpstreamWithCommandDrain(t *testing.T) {
	// A command drain outlives the removal: the upstream drains on its deadline, not the
	// config's, and stays draining when it is re-added
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	b := newTestBalancer(t, app1, app2)
	s := newTestSession(b, "app2")
	require.NoError(t, b.DrainUpstream("app2", time.Minute))
	deadline := *b.Status().Upstreams[1].DrainDeadline
	conf := newTestConfig(app1)
	conf.DrainDuration = time.Second
	b.Update(conf)
	status := b.Status().Upstreams[1]
	require.True(t, status.Removed)
	require.Equal(t, StateDraining, status.State)
	require.Equal(t, deadline, *status.DrainDeadline)

	// Re-added: still draining by command
	b.Update(newTestConfig(app1, app2))
	status = b.Status().Upstreams[1]
	require.False(t, status.Removed)
	require.Equal(t, StateDraining, status.State)
	for i := 0; i < 100; i++ {
		require.Equal(t, []string{"app1"}, names(b.Pick("s", keyFor(i))))
	}

	// Removed again and past the command's deadline: closed and forgotten
	b.Update(conf)
	b.drainTick(deadline)
	require.True(t, s.canceled())
	b.Close(s.Session)
	b.drainTick(deadline.Add(time.Second))
	require.Len(t, b.Status().Upstreams, 1)
}

func TestBalancer_UpdateChangesAddressesWithStreams(t *testing.T) {
	b := New(mustParse(t, "sites: [{name: s, listen-http: ':0', upstream: {servers: [{name: app1, addresses: ['10.0.0.1:1', '10.0.0.1:2']}]}}]"))
	u := b.upstream("app1")
	attach := func() *Session {
		s := b.NewSession(func(error) {})
		b.Attach(s, u, u.NextAddress())
		return s
	}
	s1, s2, s3 := attach(), attach(), attach() // :1, :2, :1
	require.Equal(t, 2, u.addrStreams["10.0.0.1:1"])
	require.Equal(t, 1, u.addrStreams["10.0.0.1:2"])

	// :1 goes away, :3 is new: new streams go to the empty :3, the existing ones stay counted
	b.Update(mustParse(t, "sites: [{name: s, listen-http: ':0', upstream: {health-check: {timeout: 50ms}, servers: [{name: app1, addresses: ['10.0.0.1:2', '10.0.0.1:3']}]}}]"))
	require.Same(t, u, b.upstream("app1"))
	require.Equal(t, []string{"10.0.0.1:2", "10.0.0.1:3"}, b.Status().Upstreams[0].Addresses)
	require.Equal(t, 3, b.Status().Upstreams[0].Streams)
	require.Equal(t, "10.0.0.1:3", u.NextAddress())
	s4 := attach()
	require.Equal(t, 4, b.Status().Upstreams[0].Streams)
	s5 := attach() // Tie: the first address wins
	require.Equal(t, 2, u.addrStreams["10.0.0.1:2"])

	// Closing the streams of the old address counts them down where they were
	b.Close(s1)
	b.Close(s3)
	require.Equal(t, 0, u.addrStreams["10.0.0.1:1"])
	require.Equal(t, 3, b.Status().Upstreams[0].Streams)
	for _, s := range []*Session{s2, s4, s5} {
		b.Close(s)
	}
	require.Equal(t, 0, b.Status().Upstreams[0].Streams)
	for addr, n := range u.addrStreams {
		require.Zero(t, n, addr)
	}
}

func TestBalancer_UpdateIdenticalIsNoop(t *testing.T) {
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	b := newTestBalancer(t, app1, app2)
	u1, u2 := b.upstream("app1"), b.upstream("app2")
	var sessions []*testSession
	for i := 0; i < 5; i++ {
		sessions = append(sessions, newTestSession(b, "app1"), newTestSession(b, "app2"))
	}
	before := b.Status()
	for i := 0; i < 3; i++ {
		b.Update(newTestConfig(app1, app2))
	}
	b.drainTick(time.Now().Add(time.Hour))
	require.Equal(t, 0, countCanceled(sessions))
	require.Same(t, u1, b.upstream("app1"))
	require.Same(t, u2, b.upstream("app2"))
	after := b.Status()
	require.Equal(t, before.Streams, after.Streams)
	for i := range before.Upstreams {
		require.Equal(t, before.Upstreams[i].State, after.Upstreams[i].State)
		require.Equal(t, before.Upstreams[i].Streams, after.Upstreams[i].Streams)
		require.Equal(t, before.Upstreams[i].Requests, after.Upstreams[i].Requests)
		require.Equal(t, before.Upstreams[i].Addresses, after.Upstreams[i].Addresses)
	}
}

func TestBalancer_UpdateReaddsUpstreamForgottenDuringProbe(t *testing.T) {
	// Update probes the added upstreams outside the lock. If the drain tick forgets a removed
	// upstream in that window and the update re-adds it, it must still end up in the config.
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	probing, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() {
			close(probing)
		})
		<-release
	}))
	t.Cleanup(slow.Close)
	app3 := &testUpstream{name: "app3", server: slow}
	b := newTestBalancer(t, app1, app2)
	conf := newTestConfig(app1)
	conf.DrainDuration = 0
	b.Update(conf)
	updated := make(chan struct{})
	go func() {
		defer close(updated)
		b.Update(newTestConfig(app1, app2, app3))
	}()
	<-probing
	b.drainTick(time.Now())
	require.Nil(t, b.upstream("app2"))
	close(release)
	<-updated
	status := b.Status()
	require.Len(t, status.Upstreams, 3)
	require.Equal(t, "app2", status.Upstreams[1].Name)
	require.False(t, status.Upstreams[1].Removed)
	require.Equal(t, StateHealthy, status.Upstreams[1].State)
	keyPreferring(b, "app2")
}

func TestBalancer_StartAndStop(t *testing.T) {
	// Start probes once before returning; canceling ctx stops both loops
	app1 := newTestUpstream(t, "app1")
	app1.ready.Store(false)
	b := New(newTestConfig(app1))
	before := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	b.Start(ctx)
	require.Equal(t, StateUnhealthy, b.Status().Upstreams[0].State)
	require.NotNil(t, b.Status().Upstreams[0].LastCheck)
	require.GreaterOrEqual(t, runtime.NumGoroutine(), before+2)
	cancel()
	// Polled by hand: require.Eventually runs the condition in goroutines of its own
	deadline := time.Now().Add(3 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	require.LessOrEqual(t, runtime.NumGoroutine(), before)
}

func TestBalancer_ConcurrencyStress(t *testing.T) {
	// Sessions open, attach and close while drains, undrains, updates, health rounds, drain ticks
	// and status snapshots run. Run with -race. Nothing may deadlock, and every count ends at zero.
	app1, app2, app3 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2"), newTestUpstream(t, "app3")
	app1b := newTestUpstream(t, "app1") // Same name, other address
	confs := []*config.Config{
		newTestConfig(app1, app2, app3),
		newTestConfig(app1, app2),
		newTestConfig(app2, app3),
		newTestConfig(app1b, app2, app3),
	}
	confs[1].DrainDuration = 50 * time.Millisecond
	confs[2].DrainDuration = 0
	b := newTestBalancer(t, app1, app2, app3)
	stop := make(chan struct{})
	var seenMu sync.Mutex
	seen := make(map[*Upstream]bool)
	var wg sync.WaitGroup
	loop := func(f func(i int)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				f(i)
			}
		}()
	}

	// Clients
	for c := 0; c < 16; c++ {
		loop(func(i int) {
			var canceled atomic.Bool
			s := b.NewSession(func(cause error) {
				canceled.Store(true)
			})
			for j, u := range b.Pick("s", keyFor(c*100000+i)) {
				seenMu.Lock()
				seen[u] = true
				seenMu.Unlock()
				addr := b.Reserve(s, u)
				if (i+j)%7 == 0 {
					b.RecordAddressFailure(u, addr) // Retry on the next one
					continue
				}
				b.Attach(s, u, addr)
				break
			}
			if i%3 == 0 {
				runtime.Gosched()
			}
			b.Close(s)
			if i%5 == 0 {
				b.Close(s)
			}
		})
	}

	// Operators
	names := []string{"app1", "app2", "app3", "nope"}
	loop(func(i int) {
		_ = b.DrainUpstream(names[i%len(names)], time.Duration(i%3)*10*time.Millisecond)
		time.Sleep(time.Millisecond)
	})
	loop(func(i int) {
		_ = b.UndrainUpstream(names[(i+1)%len(names)])
		time.Sleep(time.Millisecond)
	})
	loop(func(i int) {
		if i%2 == 0 {
			b.Drain(time.Duration(i%4) * 10 * time.Millisecond)
		} else {
			b.Undrain()
		}
		time.Sleep(2 * time.Millisecond)
	})
	loop(func(i int) {
		b.Update(confs[i%len(confs)]) // Reloads are serialized by the server, as here
		time.Sleep(3 * time.Millisecond)
	})
	loop(func(i int) {
		b.drainTick(time.Now().Add(time.Duration(i%100) * time.Millisecond))
	})
	loop(func(i int) {
		b.checkHealth(context.Background())
		time.Sleep(5 * time.Millisecond)
	})
	loop(func(i int) {
		status := b.Status()
		require.GreaterOrEqual(t, status.Streams, 0)
		for _, u := range status.Upstreams {
			require.GreaterOrEqual(t, u.Streams, 0)
		}
		b.Healthy()
		b.Draining()
		b.Drained()
	})

	// Run for a bit, then stop everyone; a deadlock fails instead of hanging
	time.Sleep(500 * time.Millisecond)
	close(stop)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		wg.Wait()
	}()
	select {
	case <-finished:
	case <-time.After(10 * time.Second):
		t.Fatal("deadlock: goroutines did not stop")
	}
	require.Equal(t, 0, b.Status().Streams)
	for _, u := range b.Status().Upstreams {
		require.Equal(t, 0, u.Streams, u.Name)
	}
	require.NotEmpty(t, seen)
	for u := range seen {
		require.Equal(t, 0, u.streamCount(), u.Name())
		u.mu.Lock()
		require.Equal(t, 0, sumCounts(u.addrStreams), u.Name())
		require.Equal(t, 0, sumCounts(u.addrPending), u.Name())
		u.mu.Unlock()
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, b.WaitIdle(ctx))
}

func TestBalancer_ReserveSpreadsInFlightAttempts(t *testing.T) {
	// Attempts still connecting count, so a reconnect storm spreads over the addresses instead
	// of piling onto the one with the fewest attached streams
	b := New(mustParse(t, "sites: [{name: s, listen-http: ':0', upstream: {servers: [{name: app1, addresses: ['10.0.0.1:1', '10.0.0.1:2', '10.0.0.1:3']}]}}]"))
	u := b.upstream("app1")
	var sessions []*Session
	counts := make(map[string]int)
	for i := 0; i < 6; i++ {
		s := b.NewSession(func(error) {})
		sessions = append(sessions, s)
		counts[b.Reserve(s, u)]++
	}
	require.Equal(t, map[string]int{"10.0.0.1:1": 2, "10.0.0.1:2": 2, "10.0.0.1:3": 2}, counts)
	require.Equal(t, 0, b.Status().Upstreams[0].Streams) // Reserved is not attached

	// Attach turns the reservation into a stream; the address stays as loaded as it was
	addr := b.Reserve(sessions[0], u) // A retry: releases the first reservation
	b.Attach(sessions[0], u, addr)
	require.Equal(t, 1, b.Status().Upstreams[0].Streams)
	u.mu.Lock()
	require.Equal(t, 5, sumCounts(u.addrPending))
	u.mu.Unlock()

	// Closing releases reservations as well as streams
	for _, s := range sessions {
		b.Close(s)
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	require.Equal(t, 0, sumCounts(u.addrPending))
	require.Equal(t, 0, sumCounts(u.addrStreams))
}

func TestBalancer_ReserveRetryOnAnotherUpstream(t *testing.T) {
	// A retry on the next upstream releases the reservation on the first one
	b := New(mustParse(t, "sites: [{name: s, listen-http: ':0', upstream: {servers: [{name: app1, addresses: ['10.0.0.1:1']}, {name: app2, addresses: ['10.0.0.2:1']}]}}]"))
	u1, u2 := b.upstream("app1"), b.upstream("app2")
	s := b.NewSession(func(error) {})
	b.Reserve(s, u1)
	addr := b.Reserve(s, u2)
	b.Attach(s, u2, addr)
	u1.mu.Lock()
	require.Equal(t, 0, sumCounts(u1.addrPending))
	u1.mu.Unlock()
	u2.mu.Lock()
	require.Equal(t, 0, sumCounts(u2.addrPending))
	require.Equal(t, 1, sumCounts(u2.addrStreams))
	u2.mu.Unlock()

	// A closed session reserves nothing, so nothing leaks
	b.Close(s)
	b.Reserve(s, u1)
	u1.mu.Lock()
	defer u1.mu.Unlock()
	require.Equal(t, 0, sumCounts(u1.addrPending))
}

func TestBalancer_AddressBackoff(t *testing.T) {
	// A failed address is skipped for a while, even with zero streams, since the health check
	// only probes the first address
	b := New(mustParse(t, "sites: [{name: s, listen-http: ':0', upstream: {servers: [{name: app1, addresses: ['10.0.0.1:1', '10.0.0.1:2']}]}}]"))
	u := b.upstream("app1")
	for i := 0; i < 3; i++ {
		s := b.NewSession(func(error) {})
		b.Attach(s, u, "10.0.0.1:1")
	}
	require.Equal(t, "10.0.0.1:2", u.NextAddress())
	b.RecordAddressFailure(u, "10.0.0.1:2")
	require.Equal(t, uint64(1), b.Status().Upstreams[0].Failures)
	require.Equal(t, "10.0.0.1:1", u.NextAddress())
	s := b.NewSession(func(error) {})
	require.Equal(t, "10.0.0.1:1", b.Reserve(s, u))

	// Consecutive failures back off longer, up to a cap
	u.mu.Lock()
	first := u.addrRetry["10.0.0.1:2"]
	u.mu.Unlock()
	b.RecordAddressFailure(u, "10.0.0.1:2")
	u.mu.Lock()
	second := u.addrRetry["10.0.0.1:2"]
	u.mu.Unlock()
	require.Greater(t, second.Sub(first), addressBackoffMin/2)
	for i := 0; i < 20; i++ {
		b.RecordAddressFailure(u, "10.0.0.1:2")
	}
	u.mu.Lock()
	require.LessOrEqual(t, time.Until(u.addrRetry["10.0.0.1:2"]), addressBackoffMax)
	u.mu.Unlock()

	// Once the backoff is over it is the least loaded again
	now := time.Now()
	u.recordAddressFailureNoLock("10.0.0.1:2", now.Add(-time.Hour))
	require.Equal(t, "10.0.0.1:2", u.NextAddress())

	// A stream that attaches to it clears its failures
	u.recordAddressFailureNoLock("10.0.0.1:2", now)
	b.Attach(s, u, "10.0.0.1:2")
	u.mu.Lock()
	require.Zero(t, u.addrFails["10.0.0.1:2"])
	require.True(t, u.addrRetry["10.0.0.1:2"].IsZero())
	u.mu.Unlock()

	// If every address is backing off, the least loaded one is used anyway
	b.RecordAddressFailure(u, "10.0.0.1:1")
	b.RecordAddressFailure(u, "10.0.0.1:2")
	require.Equal(t, "10.0.0.1:2", u.NextAddress())
}

func sumCounts(m map[string]int) int {
	n := 0
	for _, c := range m {
		n += c
	}
	return n
}

func TestBalancer_UpdateAddressChangeResetsHealth(t *testing.T) {
	// New addresses are probed before the upstream takes traffic on them, like a new upstream,
	// and the first probe decides, whatever the old addresses' health was
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	b := newTestBalancer(t, app1, app2)
	u2 := b.upstream("app2")
	s := newTestSession(b, "app2")
	app2down := newTestUpstream(t, "app2")
	app2down.ready.Store(false)
	b.Update(newTestConfig(app1, app2down))
	status := b.Status().Upstreams[1]
	require.Equal(t, []string{app2down.addr()}, status.Addresses)
	require.Equal(t, StateUnhealthy, status.State)
	require.Contains(t, status.LastError, ": unexpected status 503")
	require.Equal(t, 1, status.Streams) // Existing streams stay where they are
	require.False(t, s.canceled())
	require.Same(t, u2, b.upstream("app2"))
	for i := 0; i < 100; i++ {
		require.Equal(t, []string{"app1"}, names(b.Pick("s", keyFor(i))))
	}

	// And back to a healthy address: in rotation right away, no passes needed
	app2up := newTestUpstream(t, "app2")
	b.Update(newTestConfig(app1, app2up))
	status = b.Status().Upstreams[1]
	require.Equal(t, StateHealthy, status.State)
	require.Empty(t, status.LastError)
	keyPreferring(b, "app2")

	// The hysteresis starts over for the new address: a single failure does not take it out
	app2up.ready.Store(false)
	b.checkHealth(context.Background())
	require.Equal(t, StateHealthy, b.Status().Upstreams[1].State)
}

func TestBalancer_Sites(t *testing.T) {
	// Two sites with an upstream of the same name: each site picks its own
	a1, b1, b2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	b := New(mustParse(t, fmt.Sprintf(`
sites:
  - {name: a, listen-http: ':0', hosts: [a.com], upstream: {health-check: {interval: 1h, timeout: 1s}, servers: [{name: app1, addresses: [%q]}]}}
  - {name: b, listen-http: ':0', upstream: {health-check: {interval: 1h, timeout: 1s}, servers: [{name: app1, addresses: [%q]}, {name: app2, addresses: [%q]}]}}
`, a1.addr(), b1.addr(), b2.addr())))
	b.checkHealth(context.Background())
	key := []byte{192, 0, 2, 1}
	require.Equal(t, []string{a1.addr()}, addressesOf(b.Pick("a", key)))
	require.ElementsMatch(t, []string{b1.addr(), b2.addr()}, addressesOf(b.Pick("b", key)))
	require.Empty(t, b.Pick("c", key))

	// The status names the site of each upstream
	status := b.Status()
	require.Len(t, status.Upstreams, 3)
	require.Equal(t, "a", status.Upstreams[0].Site)
	require.Equal(t, "app1", status.Upstreams[0].Name)
	require.Equal(t, "b", status.Upstreams[1].Site)

	// Drains name the upstream as site/name, or by name alone if only one site has it
	require.ErrorIs(t, b.DrainUpstream("app1", time.Hour), ErrUpstreamAmbiguous)
	require.ErrorIs(t, b.DrainUpstream("c/app1", time.Hour), ErrUpstreamNotFound)
	require.ErrorIs(t, b.DrainUpstream("a/app2", time.Hour), ErrUpstreamNotFound)
	require.NoError(t, b.DrainUpstream("b/app1", time.Hour))
	require.NoError(t, b.DrainUpstream("app2", time.Hour))
	require.Empty(t, b.Pick("b", key))
	require.Len(t, b.Pick("a", key), 1)
	require.ErrorIs(t, b.UndrainUpstream("app1"), ErrUpstreamAmbiguous)
	require.NoError(t, b.UndrainUpstream("b/app1"))
	require.Equal(t, []string{b1.addr()}, addressesOf(b.Pick("b", key)))

	// A site that leaves the config drains its upstreams; the other site's app1 is untouched
	conf := mustParse(t, fmt.Sprintf("sites: [{name: a, listen-http: ':0', upstream: {health-check: {interval: 1h, timeout: 1s}, servers: [{name: app1, addresses: [%q]}]}}]", a1.addr()))
	b.Update(conf)
	status = b.Status()
	require.Len(t, status.Upstreams, 3)
	require.False(t, status.Upstreams[0].Removed)
	require.True(t, status.Upstreams[1].Removed)
	require.True(t, status.Upstreams[2].Removed)
	require.Empty(t, b.Pick("b", key))
	require.NoError(t, b.DrainUpstream("app1", time.Hour)) // Only a's is left
}

func addressesOf(upstreams []*Upstream) []string {
	var addrs []string
	for _, u := range upstreams {
		addrs = append(addrs, u.NextAddress())
	}
	return addrs
}

// NextAddress returns (for tests) the upstream address with the fewest streams and reserved attempts,
// skipping addresses that failed recently (unless all did). It reserves nothing, see Reserve.
func (u *Upstream) NextAddress() string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.nextAddressNoLock(time.Now())
}

// upstream returns an upstream by ID or unique name (see findNoLock), removed or not, or nil
func (b *Balancer) upstream(name string) *Upstream {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, u := range b.upstreams {
		if u.id == name || u.name == name {
			return u
		}
	}
	return nil
}
