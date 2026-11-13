package server

import (
	"bufio"
	"errors"
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

// WebSockets take the stream path (see stream.go): instead of the reverse proxy, which holds four
// goroutines and four buffers per WebSocket for its whole life, the client connection is
// hijacked, the handshake sent to the upstream on a raw connection, and after its 101 both
// connections are copied both ways by two goroutines that hold no buffer while they wait.

// activity is when bytes last moved on a connection, either way if both directions share it: the
// read timeout counts from there, like nginx's proxy_read_timeout on an upgraded connection
type activity struct {
	last    atomic.Int64 // Unix nanoseconds
	timeout time.Duration
}

func newActivity(timeout time.Duration) *activity {
	a := &activity{
		timeout: timeout,
	}
	a.touch()
	return a
}

func (a *activity) touch() {
	a.last.Store(time.Now().UnixNano())
}

func (a *activity) deadline() time.Time {
	return time.Unix(0, a.last.Load()).Add(a.timeout)
}

// movedSince reports whether err is a read deadline that passed while bytes moved the other way,
// so the wait goes on
func (a *activity) movedSince(err error) bool {
	return errors.Is(err, os.ErrDeadlineExceeded) && time.Now().Before(a.deadline())
}

// copyUpgraded writes the 101 to a hijacked client connection and copies both connections both
// ways, pending (the client's bytes that came with the handshake) first, until either side ends
// or nothing moved either way for the read timeout; then it ends both, and the request
func copyUpgraded(client net.Conn, pending []byte, us *upstreamStream, head []byte, pr *proxyRequest) {
	defer pr.finish()
	defer client.Close()
	client.SetDeadline(time.Time{})
	act := newActivity(pr.readTimeout)
	toClient := func(b []byte) error {
		client.SetWriteDeadline(time.Now().Add(pr.readTimeout)) // A client that takes nothing for this long is gone
		n, err := client.Write(b)
		pr.bytes += int64(n)
		return err
	}
	toUpstream := func(b []byte) error {
		us.conn.SetWriteDeadline(time.Now().Add(pr.readTimeout))
		_, err := us.conn.Write(b)
		return err
	}
	client.SetWriteDeadline(time.Now().Add(pr.readTimeout))
	if _, err := client.Write(head); err != nil { // Not counted, like the head of any response
		return
	}
	if len(us.buffered) > 0 {
		if err := toClient(us.buffered); err != nil {
			return
		}
		us.buffered = nil
	}
	if len(pending) > 0 {
		if err := toUpstream(pending); err != nil {
			return
		}
	}

	// One copy per direction; the first to end (EOF, error, timeout) ends the other
	var upErr error
	up := make(chan struct{})
	go safely("websocket copy", func() {
		defer close(up)
		upErr = copyStream(client, act, toUpstream)
		us.conn.Close()
	})
	downErr := copyStream(us.conn, act, toClient)
	client.Close()
	us.conn.Close()
	<-up
	if errors.Is(downErr, os.ErrDeadlineExceeded) || errors.Is(upErr, os.ErrDeadlineExceeded) {
		pr.cancel(errUpstreamReadTimeout) // For the metrics, which the finishers record
	}
}

// takeBuffered returns a copy of the bytes r read ahead, the hijacked client's first bytes after
// the handshake
func takeBuffered(r *bufio.Reader) []byte {
	if r == nil || r.Buffered() == 0 {
		return nil
	}
	buffered, _ := r.Peek(r.Buffered())
	return append([]byte(nil), buffered...)
}

// upgradeType returns the protocol a request upgrades to, or "" if it is no upgrade: an Upgrade
// header counts only with "Upgrade" in Connection, like the reverse proxy has it
func upgradeType(h http.Header) string {
	for _, field := range h.Values(headerConnection) {
		for _, token := range strings.Split(field, ",") {
			if strings.EqualFold(strings.TrimSpace(token), headerUpgrade) {
				return h.Get(headerUpgrade)
			}
		}
	}
	return ""
}
