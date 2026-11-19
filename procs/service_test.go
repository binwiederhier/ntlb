package procs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAdapter_LowersOnlyAfterIdleHeld(t *testing.T) {
	// Starting at all CPUs, a mostly idle process keeps them until the low use held for
	// lowerAfter samples, then drops to what its use needs at targetUse per P
	a := newAdapter(12, 12)
	for range lowerAfter - 1 {
		require.Equal(t, 12, a.next(0.7))
	}
	require.Equal(t, 4, a.next(0.7)) // ceil(0.7/0.2)
}

func TestAdapter_RaisesRightAway(t *testing.T) {
	// More use than raiseAbove per P adds Ps at once; a saturated process (use = its Ps)
	// gets more on every sample until it is not saturated any more
	a := newAdapter(3, 12)
	require.Equal(t, 5, a.next(1.0))  // 0.33 per P: ceil(1/0.2)
	require.Equal(t, 10, a.next(2.0)) // Still more demand
	require.Equal(t, 10, a.next(2.0)) // 0.2 per P: stays
	require.Equal(t, 12, a.next(9.0)) // Capped at all CPUs
}

func TestAdapter_LowSampleResetsByBusyOne(t *testing.T) {
	// A busy sample in between starts the idle count over
	a := newAdapter(8, 12)
	for range lowerAfter - 1 {
		require.Equal(t, 8, a.next(0.8))
	}
	require.Equal(t, 8, a.next(2.0)) // 0.25 per P: neither raise nor low
	for range lowerAfter - 1 {
		require.Equal(t, 8, a.next(0.8))
	}
	require.Equal(t, 4, a.next(0.8)) // ceil(0.8/0.2)
}

func TestAdapter_LeavesIdlePsForTheGC(t *testing.T) {
	// 1.8 cores of use must get 9 Ps (20% each), not stay at 3 or 5: in the lab at 350k streams,
	// 1.7 cores at 5 Ps put 8% of requests over 5ms, since a GC cycle of a 5 GB heap had too few
	// idle Ps to mark with (97% under 5ms at 7 Ps and at 16)
	a := newAdapter(3, 16)
	require.Equal(t, 9, a.next(1.8))
}

func TestAdapter_NeverBelowMinimum(t *testing.T) {
	a := newAdapter(4, 12)
	for range lowerAfter {
		a.next(0)
	}
	require.Equal(t, 3, a.procs)
}

func TestEnabled(t *testing.T) {
	// GOMAXPROCS set by the operator wins; with few CPUs there is nothing to adapt
	t.Setenv("GOMAXPROCS", "")
	require.True(t, enabled(8))
	require.False(t, enabled(3))
	t.Setenv("GOMAXPROCS", "4")
	require.False(t, enabled(8))
}
