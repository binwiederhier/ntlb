//go:build linux

package server

import (
	"context"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// userTimeout reads a connection's TCP_USER_TIMEOUT
func userTimeout(t testing.TB, c net.Conn) int {
	raw, err := c.(syscall.Conn).SyscallConn()
	require.NoError(t, err)
	var ms int
	var serr error
	require.NoError(t, raw.Control(func(fd uintptr) {
		ms, serr = unix.GetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_USER_TIMEOUT)
	}))
	require.NoError(t, serr)
	return ms
}

func TestTCP_UserTimeout(t *testing.T) {
	// Keepalive probes only run while nothing is in flight: a stream whose writes (ntfy's 45s
	// keepalives) are never acked would otherwise wait for TCP's retransmissions to give up
	// (~15 min). TCP_USER_TIMEOUT ends it after the keepalive's 2m, on both sides of the LB.
	want := int(tcpKeepAlive.Idle+tcpKeepAlive.Interval*4) / 1e6
	ln, err := newListenConfig().Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	dialed, err := newDialer(time.Second).Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer dialed.Close()
	accepted, err := ln.Accept()
	require.NoError(t, err)
	defer accepted.Close()
	require.Equal(t, want, userTimeout(t, accepted), "client side")
	require.Equal(t, want, userTimeout(t, dialed), "upstream side")
}
