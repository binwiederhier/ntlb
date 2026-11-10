package balancer

import (
	"context"
	"errors"
	"time"
)

// State is an upstream's routing state
type State string

// Upstream states, in the order they win: a draining upstream is draining whether healthy or not
const (
	StateHealthy   State = "healthy"   // In rotation
	StateUnhealthy State = "unhealthy" // Failed its health checks, out of rotation; open streams keep running
	StateDraining  State = "draining"  // Out of rotation; open streams are being closed until the deadline
	StateDrained   State = "drained"   // Out of rotation, every stream closed
)

var (
	// ErrDrained is the cause a stream is canceled with when its upstream or the load balancer drains
	ErrDrained = errors.New("connection drained")

	// ErrUpstreamNotFound is returned for an unknown (or removed) upstream name
	ErrUpstreamNotFound = errors.New("upstream not found")

	// ErrUpstreamAmbiguous is returned for an upstream name that several sites have
	ErrUpstreamAmbiguous = errors.New("several sites have an upstream of this name, use site/name")
)

// Status is a snapshot of the load balancer's state, as served by the admin API
type Status struct {
	Healthy       bool              `json:"healthy"`
	Ready         bool              `json:"ready"`
	Draining      bool              `json:"draining"`
	Drained       bool              `json:"drained"`
	DrainDeadline *time.Time        `json:"drain_deadline,omitempty"`
	Streams       int               `json:"streams"`
	Upstreams     []*UpstreamStatus `json:"upstreams"`
}

// UpstreamStatus is a snapshot of one upstream
type UpstreamStatus struct {
	Site          string     `json:"site"`
	Name          string     `json:"name"`
	State         State      `json:"state"`
	Healthy       bool       `json:"healthy"`
	Removed       bool       `json:"removed,omitempty"`
	Addresses     []string   `json:"addresses"`
	Streams       int        `json:"streams"`
	Requests      uint64     `json:"requests"`
	Failures      uint64     `json:"failures"`
	DrainDeadline *time.Time `json:"drain_deadline,omitempty"`
	LastCheck     *time.Time `json:"last_check,omitempty"`
	LastError     string     `json:"last_error,omitempty"`
}

// Session is one proxied request, typically a long-lived subscription stream. It is attached to
// an upstream once the upstream answered, and canceled to drain it.
type Session struct {
	id           uint64
	cancel       context.CancelCauseFunc
	upstream     *Upstream // Protected by Balancer.mu
	addr         string    // Protected by Balancer.mu
	reserved     *Upstream // Upstream of the attempt in flight (see Reserve), protected by Balancer.mu
	reservedAddr string    // Protected by Balancer.mu
	canceled     bool      // Protected by Balancer.mu
}

// drainState is a drain in progress (or done) for an upstream or the load balancer itself. The
// streams are closed oldest first, paced so that the last one closes before the deadline: each
// tick earns the share of the streams not earned yet (open streams minus credit) that matches the
// share of the remaining time that passed since the last tick, and closes the whole streams of it.
// The fraction carries over, so that 4 streams over 100s close at about 12.5s, 37.5s, 62.5s and
// 87.5s, not one per tick upfront (and 1 stream at 50s).
// Spreading the closes keeps tens of thousands of clients from reconnecting all at once.
type drainState struct {
	deadline time.Time
	last     time.Time // Last tick, or the start
	removal  bool      // Drained because the upstream left the config, not by command
	finished bool      // The tick at or after the deadline ran, so every stream was closed
	credit   float64   // Streams earned but not closed yet, carried between ticks
}
