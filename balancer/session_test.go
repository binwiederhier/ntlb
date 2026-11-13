package balancer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

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

func TestBalancer_ReserveSpreadsInFlightAttempts(t *testing.T) {
	// Attempts still connecting count, so a reconnect storm spreads over the addresses instead
	// of piling onto the one with the fewest attached streams
	b := New(mustParse(t, "sites: [{name: s, listen-http: ':0', upstreams: {default: {servers: [{name: app1, addresses: ['10.0.0.1:1', '10.0.0.1:2', '10.0.0.1:3']}]}}}]"))
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
	b := New(mustParse(t, "sites: [{name: s, listen-http: ':0', upstreams: {default: {servers: [{name: app1, addresses: ['10.0.0.1:1']}, {name: app2, addresses: ['10.0.0.2:1']}]}}}]"))
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
