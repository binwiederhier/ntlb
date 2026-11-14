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
	require.Equal(t, 3, a.next(0.7)) // ceil(0.7/0.4) = 2, but never below 3
}

func TestAdapter_RaisesRightAway(t *testing.T) {
	// More use than raiseAbove per P adds Ps at once; a saturated process (use = its Ps)
	// gets more on every sample until it is not saturated any more
	a := newAdapter(3, 12)
	require.Equal(t, 5, a.next(2.0))  // 0.67 per P: ceil(2/0.4)
	require.Equal(t, 10, a.next(4.0)) // Still more demand
	require.Equal(t, 10, a.next(4.0)) // 0.4 per P: stays
	require.Equal(t, 12, a.next(9.0)) // Capped at all CPUs
}

func TestAdapter_LowSampleResetsByBusyOne(t *testing.T) {
	// A busy sample in between starts the idle count over
	a := newAdapter(8, 12)
	for range lowerAfter - 1 {
		require.Equal(t, 8, a.next(1.0))
	}
	require.Equal(t, 8, a.next(3.0)) // 0.375 per P: neither raise nor low
	for range lowerAfter - 1 {
		require.Equal(t, 8, a.next(1.0))
	}
	require.Equal(t, 3, a.next(1.0)) // ceil(1/0.4)
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
