package server

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSafely(t *testing.T) {
	// A panic in a goroutine of our own (detached stream, cancel callback) is logged and
	// contained: it must not take down the process and every stream with it
	ran := false
	require.NotPanics(t, func() {
		safely("test", func() {
			ran = true
			panic("boom")
		})
	})
	require.True(t, ran)
	require.NotPanics(t, func() {
		safely("test", func() {
			panic(http.ErrAbortHandler) // Our own way to cut a stream: not an error
		})
	})
}
