package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"heckel.io/ntlb/balancer"
)

// Admin API, served on the admin listener only (keep it on localhost or a private address):
//
//	GET    /v1/health                    200 if at least one upstream is healthy, else 503
//	GET    /v1/ready                     200 if healthy and neither withdrawing nor draining, else 503
//	GET    /v1/status                    the full balancer.Status
//	POST   /v1/reload                    re-read the config file (like SIGHUP)
//	POST   /v1/upstreams/{name}/drain    drain an upstream ("site%2Fname", or the name if only one
//	                                     site has it), body {"duration":"5m"} (optional)
//	DELETE /v1/upstreams/{name}/drain    undrain it
//	GET    /metrics                      Prometheus metrics
//	GET    /debug/pprof/                 Go profiles (heap, goroutine, profile, ...)
const (
	apiHealthPath          = "/v1/health"
	apiReadyPath           = "/v1/ready"
	apiStatusPath          = "/v1/status"
	apiReloadPath          = "/v1/reload"
	apiUpstreamDrainPath   = "/v1/upstreams/{name}/drain"
	apiMetricsPath         = "/metrics"
	apiPprofPath           = "/debug/pprof/"
	apiMaxRequestBodyBytes = 4096
)

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

type adminError struct {
	status int
	err    error
}

func (e *adminError) Error() string {
	return e.err.Error()
}

func (s *Server) adminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+apiHealthPath, s.handleAdmin(s.handleHealth))
	mux.HandleFunc("GET "+apiReadyPath, s.handleAdmin(s.handleReady))
	mux.HandleFunc("GET "+apiStatusPath, s.handleAdmin(s.handleStatus))
	mux.HandleFunc("POST "+apiReloadPath, s.handleAdmin(s.handleReload))
	mux.HandleFunc("POST "+apiUpstreamDrainPath, s.handleAdmin(s.handleUpstreamDrain))
	mux.HandleFunc("DELETE "+apiUpstreamDrainPath, s.handleAdmin(s.handleUpstreamUndrain))
	mux.Handle("GET "+apiMetricsPath, promhttp.HandlerFor(s.metrics.registry, promhttp.HandlerOpts{}))
	handlePprof(mux)
	return mux
}

// handleAdmin turns a handler's error into a JSON error response
func (s *Server) handleAdmin(next func(w http.ResponseWriter, r *http.Request) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := next(w, r); err != nil {
			status := http.StatusInternalServerError
			var adminErr *adminError
			if errors.As(err, &adminErr) {
				status = adminErr.status
			}
			writeJSON(w, status, &ErrorResponse{Error: err.Error()})
		}
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) error {
	healthy := s.balancer.Healthy()
	return writeJSON(w, statusFor(healthy), &HealthResponse{Healthy: healthy})
}

func (s *Server) handleReady(w http.ResponseWriter, _ *http.Request) error {
	status := s.balancer.Status()
	return writeJSON(w, statusFor(status.Ready), &ReadyResponse{
		Ready:       status.Ready,
		Draining:    status.Draining,
		Withdrawing: status.Withdrawing,
	})
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) error {
	return writeJSON(w, http.StatusOK, s.balancer.Status())
}

func (s *Server) handleReload(w http.ResponseWriter, _ *http.Request) error {
	restart, err := s.Reload()
	if err != nil {
		return &adminError{status: http.StatusBadRequest, err: err}
	}
	return writeJSON(w, http.StatusOK, &ReloadResponse{Success: true, RestartRequired: restart})
}

func (s *Server) handleUpstreamDrain(w http.ResponseWriter, r *http.Request) error {
	d, err := s.readDrainDuration(r)
	if err != nil {
		return err
	}
	if err := s.balancer.DrainUpstream(r.PathValue("name"), d); err != nil {
		return upstreamError(err)
	}
	return writeJSON(w, http.StatusOK, &SuccessResponse{Success: true})
}

func (s *Server) handleUpstreamUndrain(w http.ResponseWriter, r *http.Request) error {
	if err := s.balancer.UndrainUpstream(r.PathValue("name")); err != nil {
		return upstreamError(err)
	}
	return writeJSON(w, http.StatusOK, &SuccessResponse{Success: true})
}

func (s *Server) readDrainDuration(r *http.Request) (time.Duration, error) {
	req := &DrainRequest{}
	body, err := io.ReadAll(io.LimitReader(r.Body, apiMaxRequestBodyBytes))
	if err != nil {
		return 0, err
	}
	if len(strings.TrimSpace(string(body))) > 0 {
		if err := json.Unmarshal(body, req); err != nil {
			return 0, &adminError{status: http.StatusBadRequest, err: fmt.Errorf("invalid request: %w", err)}
		}
	}
	if req.Duration == "" {
		return s.config.Load().DrainDuration, nil
	}
	d, err := time.ParseDuration(req.Duration)
	if err != nil || d < 0 {
		return 0, &adminError{status: http.StatusBadRequest, err: fmt.Errorf("invalid duration %q", req.Duration)}
	}
	return d, nil
}

func upstreamError(err error) error {
	if errors.Is(err, balancer.ErrUpstreamNotFound) {
		return &adminError{status: http.StatusNotFound, err: err}
	}
	if errors.Is(err, balancer.ErrUpstreamAmbiguous) {
		return &adminError{status: http.StatusBadRequest, err: err}
	}
	return err
}

func writeJSON(w http.ResponseWriter, status int, v any) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	return json.NewEncoder(w).Encode(v)
}

func statusFor(ok bool) int {
	if ok {
		return http.StatusOK
	}
	return http.StatusServiceUnavailable
}
