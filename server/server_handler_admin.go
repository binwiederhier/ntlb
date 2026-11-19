package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"heckel.io/ntlb/api"
	"heckel.io/ntlb/balancer"
)

const (
	apiMaxRequestBodyBytes = 4096
)

var (
	sharedAddressSpace = netip.MustParsePrefix("100.64.0.0/10") // RFC 6598, not routed on the internet
)

type adminError struct {
	status int
	err    error
}

func (e *adminError) Error() string {
	return e.err.Error()
}

func (s *Server) adminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+api.HealthPath, s.handleAdmin(s.handleHealth))
	mux.HandleFunc("GET "+api.ReadyPath, s.handleAdmin(s.handleReady))
	mux.HandleFunc("GET "+api.StatusPath, s.handleAdmin(s.handleStatus))
	mux.HandleFunc("POST "+api.ReloadPath, s.handleAdmin(s.handleReload))
	mux.HandleFunc("POST "+api.UpstreamDrainPath, s.handleAdmin(s.handleUpstreamDrain))
	mux.HandleFunc("DELETE "+api.UpstreamDrainPath, s.handleAdmin(s.handleUpstreamUndrain))
	mux.Handle("GET "+api.MetricsPath, promhttp.HandlerFor(s.metrics.registry, promhttp.HandlerOpts{}))
	handlePprof(mux)
	return localOnly(mux)
}

// localOnly refuses requests from web pages: cross-origin ones (CSRF), which browsers mark with
// an Origin header, or, for a GET from an <img> and the like that has none, with Sec-Fetch-Site
// ("none" is a URL typed into the address bar), and DNS rebinding, where a page's own hostname resolves to this listener. It
// also refuses clients outside loopback and private networks: nothing here authenticates, so a
// listener bound to a public address by mistake must not hand out reloads, drains and pprof.
func localOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fromPage := r.Header.Get(headerOrigin) != "" || r.Header.Get(headerSecFetchSite) != "" && r.Header.Get(headerSecFetchSite) != "none"
		if fromPage || !localHost(r.Host) || !privateClient(r.RemoteAddr) {
			http.Error(w, http.StatusText(http.StatusForbidden), http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// localHost reports whether a Host header names an IP address or localhost, as tools do, rather
// than a domain a browser could have resolved here
func localHost(host string) bool {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.TrimSuffix(strings.TrimPrefix(host, "["), "]")
	return host == "localhost" || net.ParseIP(host) != nil
}

// privateClient reports whether a remote address is on loopback or a private network (RFC 1918,
// IPv6 ULA, and the shared address space 100.64.0.0/10 of Tailscale and carriers' inner side)
func privateClient(remoteAddr string) bool {
	ap, err := netip.ParseAddrPort(remoteAddr)
	if err != nil {
		return false
	}
	addr := ap.Addr().Unmap()
	return addr.IsLoopback() || addr.IsPrivate() || sharedAddressSpace.Contains(addr)
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
			writeJSON(w, status, &api.ErrorResponse{Error: err.Error()})
		}
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) error {
	healthy := s.balancer.Healthy()
	return writeJSON(w, statusFor(healthy), &api.HealthResponse{Healthy: healthy})
}

func (s *Server) handleReady(w http.ResponseWriter, _ *http.Request) error {
	status := s.balancer.Status()
	return writeJSON(w, statusFor(status.Ready), &api.ReadyResponse{
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
	return writeJSON(w, http.StatusOK, &api.ReloadResponse{Success: true, RestartRequired: restart})
}

func (s *Server) handleUpstreamDrain(w http.ResponseWriter, r *http.Request) error {
	d, err := s.readDrainDuration(r)
	if err != nil {
		return err
	}
	if err := s.balancer.DrainUpstream(r.PathValue("name"), d); err != nil {
		return upstreamError(err)
	}
	return writeJSON(w, http.StatusOK, &api.SuccessResponse{Success: true})
}

func (s *Server) handleUpstreamUndrain(w http.ResponseWriter, r *http.Request) error {
	if err := s.balancer.UndrainUpstream(r.PathValue("name")); err != nil {
		return upstreamError(err)
	}
	return writeJSON(w, http.StatusOK, &api.SuccessResponse{Success: true})
}

func (s *Server) readDrainDuration(r *http.Request) (time.Duration, error) {
	req := &api.DrainRequest{}
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
