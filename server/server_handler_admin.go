package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/pprof"
	"strings"
	"time"

	"heckel.io/ntfy-lb/balancer"
)

// Admin API, served on the admin listener only (keep it on localhost or a private address):
//
//	GET    /v1/health                    200 if at least one upstream is healthy, else 503
//	GET    /v1/ready                     200 if healthy and not draining, else 503
//	GET    /v1/status                    the full balancer.Status
//	POST   /v1/reload                    re-read the config file (like SIGHUP)
//	POST   /v1/drain                     drain the load balancer, body {"duration":"5m"} (optional)
//	DELETE /v1/drain                     undrain it
//	POST   /v1/upstreams/{name}/drain    drain an upstream, body {"duration":"5m"} (optional)
//	DELETE /v1/upstreams/{name}/drain    undrain it
//	GET    /metrics                      Prometheus metrics
//	GET    /debug/pprof/                 Go profiles (heap, goroutine, profile, ...)
const (
	apiHealthPath          = "/v1/health"
	apiReadyPath           = "/v1/ready"
	apiStatusPath          = "/v1/status"
	apiReloadPath          = "/v1/reload"
	apiDrainPath           = "/v1/drain"
	apiUpstreamDrainPath   = "/v1/upstreams/{name}/drain"
	apiMetricsPath         = "/metrics"
	apiPprofPath           = "/debug/pprof/"
	apiMaxRequestBodyBytes = 4096
)

// HealthResponse is the response to /v1/health
type HealthResponse struct {
	Healthy bool `json:"healthy"`
}

// ReadyResponse is the response to /v1/ready
type ReadyResponse struct {
	Ready    bool `json:"ready"`
	Draining bool `json:"draining"`
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
	mux.HandleFunc("POST "+apiDrainPath, s.handleAdmin(s.handleDrain))
	mux.HandleFunc("DELETE "+apiDrainPath, s.handleAdmin(s.handleUndrain))
	mux.HandleFunc("POST "+apiUpstreamDrainPath, s.handleAdmin(s.handleUpstreamDrain))
	mux.HandleFunc("DELETE "+apiUpstreamDrainPath, s.handleAdmin(s.handleUpstreamUndrain))
	mux.HandleFunc("GET "+apiMetricsPath, s.handleMetrics)
	mux.HandleFunc("GET "+apiPprofPath, pprof.Index)
	mux.HandleFunc("GET "+apiPprofPath+"profile", pprof.Profile)
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
	return writeJSON(w, statusFor(status.Ready), &ReadyResponse{Ready: status.Ready, Draining: status.Draining})
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) error {
	return writeJSON(w, http.StatusOK, s.balancer.Status())
}

func (s *Server) handleReload(w http.ResponseWriter, _ *http.Request) error {
	if err := s.Reload(); err != nil {
		return &adminError{status: http.StatusBadRequest, err: err}
	}
	return writeJSON(w, http.StatusOK, &SuccessResponse{Success: true})
}

func (s *Server) handleDrain(w http.ResponseWriter, r *http.Request) error {
	d, err := s.readDrainDuration(r)
	if err != nil {
		return err
	}
	s.balancer.Drain(d)
	return writeJSON(w, http.StatusOK, &SuccessResponse{Success: true})
}

func (s *Server) handleUndrain(w http.ResponseWriter, _ *http.Request) error {
	s.balancer.Undrain()
	return writeJSON(w, http.StatusOK, &SuccessResponse{Success: true})
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

// handleMetrics writes the status as Prometheus metrics, in the text format
func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	status := s.balancer.Status()
	var b strings.Builder
	metric := func(name, help, kind string) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, kind)
	}
	metric("ntfy_lb_ready", "Whether the load balancer is ready (healthy, not draining)", "gauge")
	fmt.Fprintf(&b, "ntfy_lb_ready %d\n", boolInt(status.Ready))
	metric("ntfy_lb_draining", "Whether the load balancer is draining", "gauge")
	fmt.Fprintf(&b, "ntfy_lb_draining %d\n", boolInt(status.Draining))
	metric("ntfy_lb_streams", "Open proxied requests", "gauge")
	fmt.Fprintf(&b, "ntfy_lb_streams %d\n", status.Streams)
	upstreamMetric := func(name, help, kind string, value func(u *balancer.UpstreamStatus) any) {
		metric(name, help, kind)
		for _, u := range status.Upstreams {
			fmt.Fprintf(&b, "%s{upstream=%q} %v\n", name, u.Name, value(u))
		}
	}
	upstreamMetric("ntfy_lb_upstream_healthy", "Whether the upstream passes its health checks", "gauge", func(u *balancer.UpstreamStatus) any {
		return boolInt(u.Healthy)
	})
	upstreamMetric("ntfy_lb_upstream_draining", "Whether the upstream is draining or drained", "gauge", func(u *balancer.UpstreamStatus) any {
		return boolInt(u.State == balancer.StateDraining || u.State == balancer.StateDrained)
	})
	upstreamMetric("ntfy_lb_upstream_streams", "Open proxied requests per upstream", "gauge", func(u *balancer.UpstreamStatus) any {
		return u.Streams
	})
	upstreamMetric("ntfy_lb_upstream_requests_total", "Requests the upstream answered", "counter", func(u *balancer.UpstreamStatus) any {
		return u.Requests
	})
	upstreamMetric("ntfy_lb_upstream_failures_total", "Requests the upstream failed (connection errors, timeouts)", "counter", func(u *balancer.UpstreamStatus) any {
		return u.Failures
	})
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	io.WriteString(w, b.String())
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

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
