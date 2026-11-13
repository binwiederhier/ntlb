package balancer

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// drainUpstreamSecondByCloseOrder drains app1 with n streams over d seconds, ticking once a
// second, and returns the second each stream was closed at, in session (age) order, and the
// second app1 became drained
func drainUpstreamSecondByCloseOrder(t *testing.T, n, d int) (closedAt []int, finishedAt int) {
	app1 := newTestUpstream(t, "app1")
	b := newTestBalancer(t, app1)
	var sessions []*testSession
	for i := 0; i < n; i++ {
		sessions = append(sessions, newTestSession(b, "app1"))
	}
	require.NoError(t, b.DrainUpstream("app1", time.Duration(d)*time.Second))
	start := b.Status().Upstreams[0].DrainDeadline.Add(-time.Duration(d) * time.Second)
	closedAt = make([]int, n)
	for sec := 1; sec <= d; sec++ {
		b.drainTick(start.Add(time.Duration(sec) * time.Second))
		for i, s := range sessions {
			if closedAt[i] == 0 && s.canceled() {
				closedAt[i] = sec
				b.Close(s.Session) // Like the proxy does when the canceled stream ends
			}
		}
		if finishedAt == 0 && b.Status().Upstreams[0].State == StateDrained {
			finishedAt = sec
		}
	}
	return closedAt, finishedAt
}

func TestBalancer_DrainPacingEven(t *testing.T) {
	// N streams over D seconds close at about (k - 0.5) * D / N, oldest first, and all of them by
	// the deadline, whatever the ratio of streams to ticks
	tests := []struct {
		n, d int
	}{
		{1, 10},
		{3, 5},
		{4, 100},
		{7, 30},
		{10, 10},
		{59, 60},
		{60, 60},
		{61, 60},
		{100, 10},
		{1000, 60},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d streams over %ds", tt.n, tt.d), func(t *testing.T) {
			closedAt, finishedAt := drainUpstreamSecondByCloseOrder(t, tt.n, tt.d)
			for k, sec := range closedAt {
				require.NotZero(t, sec, "stream %d never closed", k)
				if k > 0 {
					require.GreaterOrEqual(t, sec, closedAt[k-1], "stream %d closed before an older one", k)
				}
				ideal := (float64(k) + 0.5) * float64(tt.d) / float64(tt.n)
				require.InDelta(t, ideal, float64(sec), 1, "stream %d", k)
			}

			// No tick closes much more than its fair share (no burst at the start or the end)
			perTick := make(map[int]int)
			for _, sec := range closedAt {
				perTick[sec]++
			}
			fair := int(math.Ceil(float64(tt.n) / float64(tt.d)))
			for sec, count := range perTick {
				require.LessOrEqual(t, count, fair+1, "second %d", sec)
			}
			require.Equal(t, tt.d, finishedAt)
		})
	}
}

func TestBalancer_DrainPacingMidpoints(t *testing.T) {
	// Few streams close in the middle of their slots: one stream over 100s at 50s, not early.
	// The credit is earned on the work not yet earned (open streams minus credit), so it grows
	// linearly instead of hyperbolically.
	tests := []struct {
		n    int
		want []int
	}{
		{1, []int{50}},
		{2, []int{25, 75}},
		{4, []int{13, 38, 63, 88}},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d streams", tt.n), func(t *testing.T) {
			closedAt, finishedAt := drainUpstreamSecondByCloseOrder(t, tt.n, 100)
			require.Len(t, closedAt, len(tt.want))
			for k := range tt.want {
				require.InDelta(t, tt.want[k], closedAt[k], 1, "stream %d closed at %v", k, closedAt)
			}
			require.Equal(t, 100, finishedAt)
		})
	}
}

func TestDrainState_Pick(t *testing.T) {
	start := time.Now()
	d := newDrainState(start, 10*time.Second, false)
	sessions := []*Session{{id: 3}, {id: 1}, {id: 2}}

	// No time passed, or time going backwards: nothing to close
	require.Empty(t, d.pick(start, sessions))
	require.Empty(t, d.pick(start.Add(-time.Second), sessions))
	require.False(t, d.finished)

	// No sessions: nothing, but the tick counts
	d = newDrainState(start, 10*time.Second, false)
	require.Empty(t, d.pick(start.Add(time.Second), nil))
	require.Equal(t, start.Add(time.Second), d.last)

	// Oldest first, whatever order they come in
	d = newDrainState(start, 10*time.Second, false)
	picked := d.pick(start.Add(5*time.Second), []*Session{{id: 3}, {id: 1}, {id: 4}, {id: 2}})
	require.Len(t, picked, 2)
	require.Equal(t, uint64(1), picked[0].id)
	require.Equal(t, uint64(2), picked[1].id)
	require.InDelta(t, 0, d.credit, 1e-9)

	// The rounding remainder carries over: 3 streams over 10s earn 0.3 per second, the first one
	// at 2s (0.6 rounds up), and the credit goes negative
	d = newDrainState(start, 10*time.Second, false)
	require.Empty(t, d.pick(start.Add(time.Second), sessions))
	require.InDelta(t, 0.3, d.credit, 1e-9)
	require.Len(t, d.pick(start.Add(2*time.Second), sessions), 1)
	require.InDelta(t, -0.4, d.credit, 1e-9)
	require.Empty(t, d.pick(start.Add(3*time.Second), sessions[:2]))
	require.InDelta(t, -0.1, d.credit, 1e-9)

	// At the deadline everything is picked and the drain is finished, also with nothing left
	d = newDrainState(start, 10*time.Second, false)
	require.Len(t, d.pick(start.Add(10*time.Second), sessions), 3)
	require.True(t, d.finished)
	d = newDrainState(start, 10*time.Second, false)
	require.Empty(t, d.pick(start.Add(time.Minute), nil))
	require.True(t, d.finished)
}

func TestBalancer_DrainZeroClosesAllOnNextTick(t *testing.T) {
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	b := newTestBalancer(t, app1, app2)
	var sessions []*testSession
	for i := 0; i < 5; i++ {
		sessions = append(sessions, newTestSession(b, "app1"), newTestSession(b, "app2"), newTestSession(b, ""))
	}
	b.Drain(0)
	require.True(t, b.Draining())
	require.False(t, b.Drained())
	require.Equal(t, 0, countCanceled(sessions))
	b.drainTick(time.Now().Add(time.Millisecond))
	require.Equal(t, 15, countCanceled(sessions))
	require.True(t, b.Drained())
	require.True(t, b.Status().Drained)
}

func TestBalancer_DrainUpstreamZeroClosesAllOnNextTick(t *testing.T) {
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	b := newTestBalancer(t, app1, app2)
	var sessions1, sessions2 []*testSession
	for i := 0; i < 5; i++ {
		sessions1 = append(sessions1, newTestSession(b, "app1"))
		sessions2 = append(sessions2, newTestSession(b, "app2"))
	}
	require.NoError(t, b.DrainUpstream("app1", 0))
	b.drainTick(time.Now().Add(time.Millisecond))
	require.Equal(t, 5, countCanceled(sessions1))
	require.Equal(t, 0, countCanceled(sessions2))

	// Drained only once the closed streams are gone too
	require.Equal(t, StateDraining, b.Status().Upstreams[0].State)
	for _, s := range sessions1 {
		b.Close(s.Session)
	}
	require.Equal(t, StateDrained, b.Status().Upstreams[0].State)
}

func TestBalancer_UndrainMidDrainKeepsRemainingStreams(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	b := newTestBalancer(t, app1)
	var sessions []*testSession
	for i := 0; i < 10; i++ {
		sessions = append(sessions, newTestSession(b, "app1"))
	}
	start := time.Now()
	b.Drain(10 * time.Second)
	b.drainTick(start.Add(5 * time.Second))
	closed := countCanceled(sessions)
	require.InDelta(t, 5, closed, 1)
	b.Undrain()
	for sec := 6; sec <= 30; sec++ {
		b.drainTick(start.Add(time.Duration(sec) * time.Second))
	}
	require.Equal(t, closed, countCanceled(sessions))
	require.True(t, b.Status().Ready)
	require.False(t, b.Draining())
}

func TestBalancer_UndrainUpstreamMidDrainKeepsRemainingStreams(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	b := newTestBalancer(t, app1)
	var sessions []*testSession
	for i := 0; i < 10; i++ {
		sessions = append(sessions, newTestSession(b, "app1"))
	}
	start := time.Now()
	require.NoError(t, b.DrainUpstream("app1", 10*time.Second))
	b.drainTick(start.Add(5 * time.Second))
	closed := countCanceled(sessions)
	require.InDelta(t, 5, closed, 1)
	require.NoError(t, b.UndrainUpstream("app1"))
	for sec := 6; sec <= 30; sec++ {
		b.drainTick(start.Add(time.Duration(sec) * time.Second))
	}
	require.Equal(t, closed, countCanceled(sessions))
	status := b.Status().Upstreams[0]
	require.Equal(t, StateHealthy, status.State)
	require.Nil(t, status.DrainDeadline)
}

func TestBalancer_RedrainWithDifferentDuration(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	b := newTestBalancer(t, app1)
	var sessions []*testSession
	for i := 0; i < 10; i++ {
		sessions = append(sessions, newTestSession(b, "app1"))
	}

	// A long drain, replaced by a short one: the new deadline counts, from the new start
	b.Drain(100 * time.Second)
	first := *b.Status().DrainDeadline
	b.drainTick(time.Now().Add(10 * time.Second))
	require.Equal(t, 1, countCanceled(sessions))
	b.Drain(5 * time.Second)
	second := *b.Status().DrainDeadline
	require.True(t, second.Before(first))
	require.False(t, b.Drained())
	b.drainTick(second.Add(-2500 * time.Millisecond))
	require.InDelta(t, 6, countCanceled(sessions), 1) // 1, plus half of the other 9
	require.Less(t, countCanceled(sessions), 10)
	require.False(t, b.Drained())
	b.drainTick(second)
	require.Equal(t, 10, countCanceled(sessions))
	require.True(t, b.Drained())

	// Draining again after it drained starts over
	b.Drain(time.Minute)
	require.True(t, b.Draining())
	require.False(t, b.Drained())

	// Same for an upstream
	b.Undrain()
	s := newTestSession(b, "app1")
	require.NoError(t, b.DrainUpstream("app1", 100*time.Second))
	require.NoError(t, b.DrainUpstream("app1", time.Second))
	deadline := *b.Status().Upstreams[0].DrainDeadline
	b.drainTick(deadline)
	require.True(t, s.canceled())
}

func TestBalancer_UpstreamDrainWaitsForLoadBalancerDrain(t *testing.T) {
	// While the load balancer drains, it closes all streams at its own pace; the upstream drains
	// wait, and catch up once it is undrained
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	b := newTestBalancer(t, app1, app2)
	var sessions1, sessions2 []*testSession
	for i := 0; i < 10; i++ {
		sessions1 = append(sessions1, newTestSession(b, "app1"))
		sessions2 = append(sessions2, newTestSession(b, "app2"))
	}
	start := time.Now()
	b.Drain(100 * time.Second)
	require.NoError(t, b.DrainUpstream("app1", 10*time.Second))
	for sec := 1; sec <= 20; sec++ {
		b.drainTick(start.Add(time.Duration(sec) * time.Second))
	}

	// 20 streams over 100s: 4 by 20s, oldest first across both upstreams
	require.Equal(t, 2, countCanceled(sessions1))
	require.Equal(t, 2, countCanceled(sessions2))
	require.Equal(t, StateDraining, b.Status().Upstreams[0].State)

	// app1's drain is past its deadline: the next tick closes the rest of it
	b.Undrain()
	b.drainTick(start.Add(21 * time.Second))
	require.Equal(t, 10, countCanceled(sessions1))
	require.Equal(t, 2, countCanceled(sessions2))
	for _, s := range sessions1 {
		b.Close(s.Session)
	}
	require.Equal(t, StateDrained, b.Status().Upstreams[0].State)
}

func TestBalancer_DrainSkipsCanceledSessions(t *testing.T) {
	// A canceled session that has not closed yet is canceled once, and does not count as open
	app1 := newTestUpstream(t, "app1")
	b := newTestBalancer(t, app1)
	var calls atomic.Int32
	s := b.NewSession(func(error) {
		calls.Add(1)
	})
	u := b.upstream("app1")
	b.Attach(s, u, u.NextAddress())
	b.Drain(0)
	for i := 1; i <= 5; i++ {
		b.drainTick(time.Now().Add(time.Duration(i) * time.Second))
	}
	require.Equal(t, int32(1), calls.Load())
}

func TestBalancer_AttachWhileUpstreamDraining(t *testing.T) {
	// Pick never returns a draining upstream, but a request picked just before the drain attaches
	// after it started. Such late streams are closed too, after the older ones, by the deadline.
	app1 := newTestUpstream(t, "app1")
	b := newTestBalancer(t, app1)
	var old, late []*testSession
	for i := 0; i < 4; i++ {
		old = append(old, newTestSession(b, "app1"))
	}
	start := time.Now()
	require.NoError(t, b.DrainUpstream("app1", 10*time.Second))
	b.drainTick(start.Add(2 * time.Second))
	for i := 0; i < 4; i++ {
		late = append(late, newTestSession(b, "app1"))
	}
	for sec := 3; sec <= 9; sec++ {
		b.drainTick(start.Add(time.Duration(sec) * time.Second))
		if countCanceled(late) > 0 {
			require.Equal(t, 4, countCanceled(old), "a late stream closed before an old one")
		}
	}
	b.drainTick(start.Add(10 * time.Second))
	require.Equal(t, 4, countCanceled(old))
	require.Equal(t, 4, countCanceled(late))

	// And after the deadline, a stream attaching to the drained upstream is closed right away: no
	// tick scans the sessions for a drain that is done
	b.drainTick(start.Add(11 * time.Second)) // Past the deadline (DrainUpstream started a bit after start)
	s := newTestSession(b, "app1")
	require.True(t, s.canceled())
}

func TestBalancer_FinishedDrainDoesNotScan(t *testing.T) {
	// A drained upstream that stays drained (until undrain) costs the drain tick nothing, also with
	// many streams on the other upstreams: at 85k sessions a scan holds the lock for milliseconds
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	b := newTestBalancer(t, app1, app2)
	for range 1000 {
		newTestSession(b, "app1")
	}
	start := time.Now()
	idle := testing.AllocsPerRun(10, func() {
		b.drainTickVictims(start)
	})
	require.NoError(t, b.DrainUpstream("app2", 0))
	b.drainTick(start) // The deadline tick: finished, nothing to close
	require.Zero(t, testing.AllocsPerRun(10, func() {
		b.drainTickVictims(start.Add(time.Second))
	})-idle)
}

func TestBalancer_AttachToRemovedUpstream(t *testing.T) {
	// A stream attaching to an upstream that was removed but is still draining is closed by the
	// drain, and the upstream is forgotten after that
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	b := newTestBalancer(t, app1, app2)
	u2 := b.upstream("app2") // Picked before the reload
	conf := newTestConfig(app1)
	conf.DrainDuration = 10 * time.Second
	start := time.Now()
	b.Update(conf)
	s := &testSession{}
	s.Session = b.NewSession(func(cause error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.cause = cause
	})
	b.Attach(s.Session, u2, u2.NextAddress())
	require.Equal(t, 1, b.Status().Upstreams[1].Streams)
	b.drainTick(start.Add(10 * time.Second))
	require.True(t, s.canceled())
	require.Len(t, b.Status().Upstreams, 2)
	b.Close(s.Session)
	b.drainTick(start.Add(11 * time.Second))
	require.Len(t, b.Status().Upstreams, 1)
}

func TestBalancer_AttachToForgottenUpstream(t *testing.T) {
	// A request picked an upstream, which was then removed, drained and forgotten before the
	// request attached. Its stream must still be closed, or it would run on forever, invisible:
	// right away, as no tick scans the streams while nothing drains.
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	b := newTestBalancer(t, app1, app2)
	u2 := b.upstream("app2") // Picked before the reload
	conf := newTestConfig(app1)
	conf.DrainDuration = 0
	b.Update(conf)
	now := time.Now()
	b.drainTick(now)
	require.Len(t, b.Status().Upstreams, 1)
	s := &testSession{}
	s.Session = b.NewSession(func(cause error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.cause = cause
	})
	b.Attach(s.Session, u2, u2.NextAddress())
	require.True(t, s.canceled())
	require.ErrorIs(t, s.cause, ErrDrained)

	// Same if it was re-added in between: the stream belongs to the forgotten upstream
	b.Update(newTestConfig(app1, app2))
	s2 := &testSession{}
	s2.Session = b.NewSession(func(cause error) {
		s2.mu.Lock()
		defer s2.mu.Unlock()
		s2.cause = cause
	})
	b.Attach(s2.Session, u2, u2.NextAddress())
	fresh := newTestSession(b, "app2")
	require.True(t, s2.canceled())
	require.False(t, fresh.canceled())
}

func TestBalancer_RemovedUpstreamForgottenOnlyAfterDeadline(t *testing.T) {
	// A removed upstream without streams stays listed (draining) until its deadline
	app1, app2 := newTestUpstream(t, "app1"), newTestUpstream(t, "app2")
	b := newTestBalancer(t, app1, app2)
	conf := newTestConfig(app1)
	conf.DrainDuration = 10 * time.Second
	b.Update(conf)
	start := b.Status().Upstreams[1].DrainDeadline.Add(-10 * time.Second)
	b.drainTick(start.Add(5 * time.Second))
	require.Len(t, b.Status().Upstreams, 2)
	require.Equal(t, StateDraining, b.Status().Upstreams[1].State)
	b.drainTick(start.Add(10 * time.Second))
	require.Len(t, b.Status().Upstreams, 1)
	require.Nil(t, b.upstream("app2"))
}

func TestBalancer_DrainLoop(t *testing.T) {
	// The loop ticks with the real clock, and returns when done is closed
	app1 := newTestUpstream(t, "app1")
	b := newTestBalancer(t, app1)
	s := newTestSession(b, "app1")
	b.Drain(0)
	done, exited := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(exited)
		b.drainLoop(done)
	}()
	require.Eventually(t, s.canceled, 3*time.Second, 10*time.Millisecond)
	close(done)
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("drain loop did not stop")
	}
}

func TestBalancer_DrainLoopStopsOnContextCancel(t *testing.T) {
	app1 := newTestUpstream(t, "app1")
	b := newTestBalancer(t, app1)
	ctx, cancel := context.WithCancel(context.Background())
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		b.drainLoop(ctx.Done())
	}()
	cancel()
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("drain loop did not stop")
	}
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
		require.Equal(t, []string{"app2"}, names(b.Pick("s", "", keyFor(i))))
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
