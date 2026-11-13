package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"sync/atomic"

	"heckel.io/ntlb/config"
)

// publicServer is the proxying server of one listen address
type publicServer struct {
	srv     *http.Server
	ln      net.Listener
	tls     bool
	retired atomic.Bool // Its listener was closed on purpose
}

// HTTPAddr returns the address the first HTTP listener is bound to, or "" if there is none
func (s *Server) HTTPAddr() string {
	return s.firstListenerAddr(false)
}

// HTTPSAddr returns the address the first HTTPS listener is bound to, or "" if there is none
func (s *Server) HTTPSAddr() string {
	return s.firstListenerAddr(true)
}

// ListenerAddr returns the address a listener (by its configured address) is bound to, or ""
func (s *Server) ListenerAddr(listen string) string {
	return s.listenerAddr(listen)
}

// MetricsAddr returns the address the metrics listener is bound to, or "" if there is none
func (s *Server) MetricsAddr() string {
	return s.listenerAddr(listenerMetrics)
}

// HealthAddr returns the address the health listener is bound to, or "" if there is none
func (s *Server) HealthAddr() string {
	return s.listenerAddr(listenerHealth)
}

// ProfilerAddr returns the address the profiler listener is bound to, or "" if there is none
func (s *Server) ProfilerAddr() string {
	return s.listenerAddr(listenerProfiler)
}

// AdminAddr returns the address the admin listener is bound to
func (s *Server) AdminAddr() string {
	return s.listenerAddr(listenerAdmin)
}

func (s *Server) listen(conf *config.Config) error {
	// Public listeners, one per address of the sites
	public, err := s.bindPublic(conf, conf.Listeners)
	if err != nil {
		return err
	}
	for addr, ps := range public {
		s.public[addr] = ps
		s.listeners[addr] = ps.ln
	}

	// Admin listener
	ln, err := net.Listen("tcp", conf.ListenAdmin)
	if err != nil {
		return err
	}
	s.listeners[listenerAdmin] = ln
	s.admin = newHTTPServer(listenerAdmin, s.adminHandler())

	// Metrics listener
	if conf.ListenMetrics != "" {
		ln, err := net.Listen("tcp", conf.ListenMetrics)
		if err != nil {
			return err
		}
		s.listeners[listenerMetrics] = ln
		s.metricsSrv = newHTTPServer(listenerMetrics, s.metricsHandler())
	}

	// Health listener
	if conf.ListenHealth != "" {
		ln, err := net.Listen("tcp", conf.ListenHealth)
		if err != nil {
			return err
		}
		s.listeners[listenerHealth] = ln
		s.healthSrv = newHTTPServer(listenerHealth, s.healthHandler())
	}

	// Profiler listener
	if conf.ListenProfiler != "" {
		ln, err := net.Listen("tcp", conf.ListenProfiler)
		if err != nil {
			return err
		}
		s.listeners[listenerProfiler] = ln
		s.profilerSrv = newHTTPServer(listenerProfiler, profilerHandler())
	}
	return nil
}

// bindPublic binds the proxying servers of listeners, with the configured TCP keepalives towards
// the clients; on an error it closes the ones it bound
func (s *Server) bindPublic(conf *config.Config, listeners []*config.Listener) (map[string]*publicServer, error) {
	ka := conf.ClientKeepalive.KeepAliveConfig()
	lc := &net.ListenConfig{
		KeepAliveConfig: ka,
	}
	if !ka.Enable {
		lc.KeepAlive = -1
	}
	public := make(map[string]*publicServer)
	for _, l := range listeners {
		ln, err := lc.Listen(context.Background(), "tcp", l.Addr)
		if err != nil {
			closePublic(public)
			return nil, err
		}
		srv := newHTTPServer(l.Addr, s.observe(l.Addr, s.proxy))
		tracker := &connTracker{
			onBad: s.badRequest,
		}
		srv.ConnContext = tracker.connContext
		srv.ConnState = tracker.connState
		if l.TLS {
			srv.TLSConfig = &tls.Config{
				MinVersion:     tls.VersionTLS12,
				CipherSuites:   tls12CipherSuites,
				GetCertificate: s.certs.getter(l.Addr),
			}
		}
		public[l.Addr] = &publicServer{
			srv: srv,
			ln: &countingListener{
				Listener: ln,
				metrics:  s.metrics,
			},
			tls: l.TLS,
		}
	}
	return public, nil
}

// bindAdded binds the listen addresses that conf adds. An address that switches between HTTP and
// HTTPS cannot be bound twice, so it needs a restart.
func (s *Server) bindAdded(conf *config.Config) (map[string]*publicServer, error) {
	s.mu.Lock()
	var added []*config.Listener
	for _, l := range conf.Listeners {
		if ps, ok := s.public[l.Addr]; !ok {
			added = append(added, l)
		} else if ps.tls != l.TLS {
			s.mu.Unlock()
			return nil, fmt.Errorf("listener %s switches between listen-http and listen-https: restart ntlb for that", l.Addr)
		}
	}
	s.mu.Unlock()
	return s.bindPublic(conf, added)
}

// swapPublic starts the added servers and retires the ones conf no longer has: their listener is
// closed, their connections run on until they end or Shutdown
func (s *Server) swapPublic(conf *config.Config, added map[string]*publicServer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for addr, ps := range added {
		s.public[addr] = ps
		s.listeners[addr] = ps.ln
		if s.serving {
			go s.servePublic(ps)
		}
	}
	for addr, ps := range s.public {
		if conf.Listener(addr) == nil {
			slog.Info("Listener removed, closing it", "listener", addr)
			ps.retired.Store(true)
			ps.ln.Close()
			s.retired = append(s.retired, ps)
			delete(s.public, addr)
			delete(s.listeners, addr)
			go s.forgetRetired(ps)
		}
	}
}

// forgetRetired shuts a removed listener's server down gracefully (closes its idle connections,
// sends HTTP/2 GOAWAY, waits for the requests in flight), then forgets it. The streams it hijacked
// are not its own any more and run on.
func (s *Server) forgetRetired(ps *publicServer) {
	ps.srv.Shutdown(context.Background()) // Shutdown cuts it short with Close, if it runs that long
	s.mu.Lock()
	defer s.mu.Unlock()
	s.retired = slices.DeleteFunc(s.retired, func(p *publicServer) bool {
		return p == ps
	})
}

func (s *Server) firstListenerAddr(tls bool) string {
	for _, l := range s.config.Load().Listeners {
		if l.TLS == tls {
			return s.listenerAddr(l.Addr)
		}
	}
	return ""
}

func closePublic(public map[string]*publicServer) {
	for _, ps := range public {
		ps.ln.Close()
	}
}

func (s *Server) closeListeners() {
	for _, ln := range s.listeners {
		ln.Close()
	}
}

func (s *Server) listenerAddr(name string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ln, ok := s.listeners[name]; ok {
		return ln.Addr().String()
	}
	return ""
}

// newHTTPServer creates a server without read or write timeouts beyond the request header: its
// requests are long-lived streams in both directions
func newHTTPServer(name string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              name,
		Handler:           handler,
		ReadHeaderTimeout: readHeaderTimeout,
		IdleTimeout:       idleTimeout,
		MaxHeaderBytes:    maxHeaderBytes,
		ErrorLog:          slog.NewLogLogger(slog.Default().Handler(), slog.LevelDebug), // TLS handshake errors and the like
	}
}
