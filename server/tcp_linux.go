//go:build linux

package server

import (
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// setUserTimeout sets TCP_USER_TIMEOUT to the keepalive's give-up time, as a socket's Control:
// keepalive probes only run while nothing is in flight, so a peer that vanished while ntlb had
// unacked data out (a stream's keepalive message) is otherwise only noticed when TCP's
// retransmissions give up, after ~15 minutes. An accepted connection inherits it from the listener.
func setUserTimeout(_, _ string, c syscall.RawConn) error {
	ms := int((tcpKeepAlive.Idle + tcpKeepAlive.Interval*time.Duration(tcpKeepAlive.Count)).Milliseconds())
	var serr error
	if err := c.Control(func(fd uintptr) {
		serr = unix.SetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_USER_TIMEOUT, ms)
	}); err != nil {
		return err
	}
	return serr
}
