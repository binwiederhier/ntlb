package server

import (
	"net"
	"time"
)

var (
	// tcpKeepAlive probes idle connections to clients and upstreams, so that a peer that vanished
	// (a phone that lost its network, a node that crashed) is noticed after idle + interval * count
	// (2m), and not only by the next write
	tcpKeepAlive = net.KeepAliveConfig{
		Enable:   true,
		Idle:     time.Minute,
		Interval: 15 * time.Second,
		Count:    4,
	}
)

// newListenConfig binds the public listeners: TCP keepalives and the user timeout towards the clients
func newListenConfig() *net.ListenConfig {
	return &net.ListenConfig{
		KeepAliveConfig: tcpKeepAlive,
		Control:         setUserTimeout,
	}
}

// newDialer dials the upstreams: TCP keepalives and the user timeout towards the servers
func newDialer(connectTimeout time.Duration) *net.Dialer {
	return &net.Dialer{
		Timeout:         connectTimeout,
		KeepAliveConfig: tcpKeepAlive,
		Control:         setUserTimeout,
	}
}
