package server

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAttemptBody(t *testing.T) {
	// Each attempt reads the shared body through its own handle; a retired handle reads nothing,
	// so a late read by a failed attempt cannot eat what the next attempt needs
	shared := &trackingBody{rc: io.NopCloser(strings.NewReader("hello"))}
	first := shared.attempt()
	require.False(t, first.retire()) // Untouched: safe to retry
	n, err := first.Read(make([]byte, 5))
	require.Zero(t, n)
	require.ErrorIs(t, err, errBodyRetired)
	second := shared.attempt()
	buf := make([]byte, 2)
	n, err = second.Read(buf)
	require.NoError(t, err)
	require.Equal(t, "he", string(buf[:n]))
	require.True(t, second.retire()) // Touched: never retry
	require.True(t, touched(shared))
}
