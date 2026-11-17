//go:build !linux

package server

import (
	"syscall"
)

// setUserTimeout does nothing outside Linux, which has TCP_USER_TIMEOUT (see tcp_linux.go)
func setUserTimeout(_, _ string, _ syscall.RawConn) error {
	return nil
}
