package server

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetrics_ProcessWithoutOpenFDs(t *testing.T) {
	// The process metrics, but not the open fds: counting them lists /proc/self/fd, ~110 ms per
	// scrape at 170k fds (ntlb_connections counts the client side)
	app1 := newTestUpstream(t, "app1")
	s, _ := newTestServer(t, "", app1)
	_, body := adminDo(t, s, http.MethodGet, "/metrics", "")
	for _, want := range []string{"process_cpu_seconds_total ", "process_resident_memory_bytes ", "process_virtual_memory_bytes ", "process_max_fds ", "process_start_time_seconds "} {
		require.Contains(t, body, want)
	}
	require.NotContains(t, body, "process_open_fds")
}
