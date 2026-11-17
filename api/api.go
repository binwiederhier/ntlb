// Package api is the admin API's wire format: its paths, and the requests and responses that the
// server (package server) and the client (package client, the CLI) exchange. It has no logic, so
// the client does not pull in the server.
//
// Served on the admin listener only (keep it on localhost or a private address). The server
// refuses requests with an Origin header, a Host other than an IP address or localhost, or a
// client outside loopback and private networks, so neither a web page nor the internet reaches it:
//
//	GET    /v1/health                    200 if at least one upstream is healthy, else 503
//	GET    /v1/ready                     200 if healthy and neither withdrawing nor draining, else 503
//	GET    /v1/status                    the full Status
//	POST   /v1/reload                    re-read the config file (like SIGHUP)
//	POST   /v1/upstreams/{name}/drain    drain an upstream ("site%2Fname", or the name if only one
//	                                     site has it), body {"duration":"5m"} (optional)
//	DELETE /v1/upstreams/{name}/drain    undrain it
//	GET    /metrics                      Prometheus metrics
//	GET    /debug/pprof/                 Go profiles (heap, goroutine, profile, ...)
package api

import (
	"net/url"
	"strings"
	"time"
)

// Paths of the admin API
const (
	HealthPath        = "/v1/health"
	ReadyPath         = "/v1/ready"
	StatusPath        = "/v1/status"
	ReloadPath        = "/v1/reload"
	UpstreamDrainPath = "/v1/upstreams/{name}/drain"
	MetricsPath       = "/metrics"
	PprofPath         = "/debug/pprof/"

	upstreamNameParam = "{name}"
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

// Status is a snapshot of the load balancer's state, the response to /v1/status
type Status struct {
	Healthy       bool              `json:"healthy"`
	Ready         bool              `json:"ready"`
	Withdrawing   bool              `json:"withdrawing"`
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
	Pool          string     `json:"pool"` // The name of the site's upstream block it is in
	State         State      `json:"state"`
	Healthy       bool       `json:"healthy"`
	Removed       bool       `json:"removed,omitempty"`
	Weight        int        `json:"weight"`
	Backup        bool       `json:"backup,omitempty"`
	Addresses     []string   `json:"addresses"`
	Streams       int        `json:"streams"`
	Requests      uint64     `json:"requests"`
	Failures      uint64     `json:"failures"`
	DrainDeadline *time.Time `json:"drain_deadline,omitempty"`
	LastCheck     *time.Time `json:"last_check,omitempty"`
	LastError     string     `json:"last_error,omitempty"`
}

// HealthResponse is the response to /v1/health
type HealthResponse struct {
	Healthy bool `json:"healthy"`
}

// ReloadResponse is the response to POST /v1/reload: the settings that changed but only apply
// after a restart, if any
type ReloadResponse struct {
	Success         bool     `json:"success"`
	RestartRequired []string `json:"restart_required,omitempty"`
}

// ReadyResponse is the response to /v1/ready
type ReadyResponse struct {
	Ready       bool `json:"ready"`
	Draining    bool `json:"draining"`
	Withdrawing bool `json:"withdrawing"`
}

// DrainRequest is the optional body of the drain calls; without a duration, the config's
// drain-duration applies
type DrainRequest struct {
	Duration string `json:"duration,omitempty"`
}

// SuccessResponse is the response to a successful call that returns nothing else
type SuccessResponse struct {
	Success bool `json:"success"`
}

// ErrorResponse is the response to a failed call
type ErrorResponse struct {
	Error string `json:"error"`
}

// UpstreamDrainURL is the drain path of an upstream name ("site/name" or "name"), escaped
func UpstreamDrainURL(name string) string {
	return strings.Replace(UpstreamDrainPath, upstreamNameParam, url.PathEscape(name), 1)
}
