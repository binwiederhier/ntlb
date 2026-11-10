// Package server runs the load balancer's listeners: the HTTP/HTTPS listeners that proxy to the
// upstreams, and the admin listener with /v1/health, /v1/ready, the admin API and the metrics.
package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"heckel.io/ntlb/balancer"
	"heckel.io/ntlb/config"
	"heckel.io/ntlb/match"
	"heckel.io/ntlb/rules"
)

const (
	readHeaderTimeout = 30 * time.Second
	idleTimeout       = 65 * time.Second // Like nginx's keepalive_timeout
	maxHeaderBytes    = 64 * 1024
	shutdownGrace     = 5 * time.Second // After the drain: how long the last responses get to finish
	drainTickSlack    = 2 * time.Second // The drain closes the last streams on its first tick after the deadline
)

// Server is the load balancer process: listeners, proxy and admin API around a balancer.Balancer
type Server struct {
	configFile string
	config     atomic.Pointer[config.Config]
	balancer   *balancer.Balancer
	proxy      *proxy
	rules      *rules.Engine
	certs      *certStore
	files      *logFiles // Access logs and the ban file
	metrics    *metrics
	servers    []*http.Server // Proxying servers (HTTP, HTTPS)
	admin      *http.Server
	metricsSrv *http.Server // listen-metrics, if set
	listeners  map[string]net.Listener
	reloadMu   sync.Mutex // Serializes reloads
	stopOnce   sync.Once
	stopped    chan struct{}
}

// New loads the config file and binds all listeners, so that a bad address fails right away.
// Call Serve to start serving.
func New(configFile string) (*Server, error) {
	conf, err := config.Load(configFile)
	if err != nil {
		return nil, err
	}
	s := &Server{
		configFile: configFile,
		balancer:   balancer.New(conf),
		files:      newLogFiles(),
		listeners:  make(map[string]net.Listener),
		stopped:    make(chan struct{}),
	}
	s.config.Store(conf)
	s.rules = rules.New(conf, s.writeBan)
	s.metrics = newMetrics(s.balancer)
	s.proxy = newProxy(s.balancer, s.metrics, conf)
	if conf.ListenHTTPS != "" {
		certs, err := loadCerts(conf)
		if err != nil {
			return nil, err
		}
		s.certs = &certStore{}
		s.certs.store(certs)
	}
	files, err := openLogFiles(logPaths(conf))
	if err != nil {
		return nil, err
	}
	s.files.swap(files)
	if err := s.listen(conf); err != nil {
		s.closeListeners()
		s.files.close()
		return nil, err
	}
	return s, nil
}

// Serve probes the upstreams once, then serves until Shutdown is done. ctx bounds the balancer's
// background loops.
func (s *Server) Serve(ctx context.Context) error {
	s.balancer.Start(ctx)
	errChan := make(chan error, len(s.listeners))
	serve := func(name string, srv *http.Server, ln net.Listener, tls bool) {
		slog.Info("Listening", "listener", name, "addr", ln.Addr().String())
		var err error
		if tls {
			err = srv.ServeTLS(ln, "", "")
		} else {
			err = srv.Serve(ln)
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errChan <- fmt.Errorf("%s listener: %w", name, err)
		}
	}
	for _, srv := range s.servers {
		go serve(srv.Addr, srv, s.listeners[srv.Addr], srv.TLSConfig != nil)
	}
	go serve(listenerAdmin, s.admin, s.listeners[listenerAdmin], false)
	if s.metricsSrv != nil {
		go serve(listenerMetrics, s.metricsSrv, s.listeners[listenerMetrics], false)
	}
	select {
	case err := <-errChan:
		s.Shutdown(0)
		return err
	case <-s.stopped:
		return nil
	}
}

// Reload re-reads the config file and the site files it includes, and applies them: sites,
// upstreams, health checks, timeouts, drain durations, streams, the client key, rules, bans, the
// certificates and the log files. A broken config (or a certificate or log file that cannot be
// opened) is rejected as a whole. It returns the changed
// settings that only a restart applies: listener addresses and client keepalives.
func (s *Server) Reload() ([]string, error) {
	s.reloadMu.Lock()
	defer s.reloadMu.Unlock()
	conf, err := config.Load(s.configFile)
	if err != nil {
		return nil, err
	}
	old := s.config.Load()
	restart := old.RestartRequired(conf)
	if len(restart) > 0 {
		slog.Warn("Some settings changed that only a restart applies", "settings", restart)
	}

	// Open everything that can fail first, then apply
	var certs []*tls.Certificate
	if s.certs != nil {
		if certs, err = loadCerts(conf); err != nil {
			return nil, err
		}
	}
	files, err := openLogFiles(logPaths(conf))
	if err != nil {
		return nil, err
	}
	if s.certs != nil {
		s.certs.store(certs)
	}
	s.files.swap(files)
	s.rules.Update(conf)
	s.balancer.Update(conf)
	s.proxy.update(conf)
	s.config.Store(conf)
	slog.Info("Config reloaded", "sites", len(conf.Sites), "upstreams", conf.UpstreamCount())
	return restart, nil
}

// Shutdown drains the load balancer over d (not ready right away, streams closed oldest first),
// then closes the listeners and returns once every response finished or the grace period passed
func (s *Server) Shutdown(d time.Duration) {
	s.stopOnce.Do(func() {
		slog.Info("Shutting down", "drain", d)
		s.balancer.Drain(d)
		ctx, cancel := context.WithTimeout(context.Background(), d+drainTickSlack)
		defer cancel()
		if err := s.balancer.WaitIdle(ctx); err != nil {
			slog.Warn("Streams still open after the drain, closing them", "error", err)
		}

		// End whatever the drain did not (e.g. its loop stopped), and wait for the finishers
		graceCtx, graceCancel := context.WithTimeout(context.Background(), shutdownGrace)
		defer graceCancel()
		if err := s.proxy.closeAll(graceCtx); err != nil {
			slog.Warn("Requests still open after closing them", "error", err)
		}
		var wg sync.WaitGroup
		others := []*http.Server{s.admin}
		if s.metricsSrv != nil {
			others = append(others, s.metricsSrv)
		}
		for _, srv := range append(s.servers, others...) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := srv.Shutdown(graceCtx); err != nil {
					srv.Close()
				}
			}()
		}
		wg.Wait()
		s.files.close()
		close(s.stopped)
	})
}

// observe wraps the proxy to see every request's outcome, for the access logs, the response rules
// and the metrics, and answers the requests that a rule decides itself. The outcome is recorded
// by a finisher, when the request ends, which for a detached stream is after the handler returned.
func (s *Server) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		conf := s.config.Load()
		env := match.NewEnv(r)
		if site := conf.SiteFor(r.Host); site != nil {
			env.Site = site.Name
		}
		respEnv := match.NewResponseEnv(env) // Strings only, so the request can be let go
		var entry *accessLogEntry
		if len(conf.AccessLogs) > 0 {
			entry = newAccessLogEntry(r, start)
		}
		pr := &proxyRequest{
			env:  env,
			site: env.Site,
		}
		sw := &statusWriter{ResponseWriter: w}
		rule := s.rules.Check(r, env)
		if rule != nil {
			pr.rule, pr.kind = rule.ID, kindRequest
			s.metrics.ruleMatches.WithLabelValues(rule.ID).Inc()
		}
		pr.onFinish(func() {
			status := pr.finalStatus() // Must not use sw: it references the server's connection state
			respEnv.Status, respEnv.UpstreamStatus, respEnv.Rule = status, pr.upstreamStatus, pr.rule
			if entry != nil {
				s.writeAccessLog(entry, pr, respEnv)
			}
			s.rules.CheckResponse(respEnv)
			s.metrics.observe(pr, status, start)
		})
		defer func() { // Deferred: a stream cut short ends in a http.ErrAbortHandler panic
			if !pr.detached {
				if pr.status == 0 {
					pr.status, pr.bytes = sw.status, sw.bytes
				}
				pr.finish()
			}
		}()
		if rule != nil {
			s.answer(sw, r, env, rule)
			return
		}
		next.ServeHTTP(sw, r.WithContext(withProxyRequest(r.Context(), pr)))
	})
}

// writeAccessLog writes the request to the first access log whose match is true
func (s *Server) writeAccessLog(entry *accessLogEntry, pr *proxyRequest, env *match.ResponseEnv) {
	for _, l := range s.config.Load().AccessLogs {
		if l.Program == nil || l.Program.Match(env) {
			if log := s.files.get(l.File); log != nil {
				log.write(entry, pr, env.Status)
			}
			return
		}
	}
}

// writeBan writes a ban line to the ban file
func (s *Server) writeBan(line string) {
	s.metrics.bans.Inc()
	if bans := s.config.Load().Bans; bans != nil {
		if log := s.files.get(bans.File); log != nil {
			log.writeLine(line + "\n")
		}
	}
}

// HTTPAddr returns the address the HTTP listener is bound to, or "" if there is none
func (s *Server) HTTPAddr() string {
	return s.listenerAddr(listenerHTTP)
}

// HTTPSAddr returns the address the HTTPS listener is bound to, or "" if there is none
func (s *Server) HTTPSAddr() string {
	return s.listenerAddr(listenerHTTPS)
}

// MetricsAddr returns the address the metrics listener is bound to, or "" if there is none
func (s *Server) MetricsAddr() string {
	return s.listenerAddr(listenerMetrics)
}

// AdminAddr returns the address the admin listener is bound to
func (s *Server) AdminAddr() string {
	return s.listenerAddr(listenerAdmin)
}

// Listener names, also the http.Server.Addr of each server
const (
	listenerHTTP    = "http"
	listenerHTTPS   = "https"
	listenerAdmin   = "admin"
	listenerMetrics = "metrics"
)

func (s *Server) listen(conf *config.Config) error {
	// Proxying listeners, with the configured TCP keepalives towards the clients
	ka := conf.ClientKeepalive.KeepAliveConfig()
	lc := &net.ListenConfig{
		KeepAliveConfig: ka,
	}
	if !ka.Enable {
		lc.KeepAlive = -1
	}
	handler := s.observe(s.proxy)
	if conf.ListenHTTP != "" {
		ln, err := lc.Listen(context.Background(), "tcp", conf.ListenHTTP)
		if err != nil {
			return err
		}
		s.listeners[listenerHTTP] = ln
		s.servers = append(s.servers, newHTTPServer(listenerHTTP, handler))
	}
	if conf.ListenHTTPS != "" {
		ln, err := lc.Listen(context.Background(), "tcp", conf.ListenHTTPS)
		if err != nil {
			return err
		}
		s.listeners[listenerHTTPS] = ln
		srv := newHTTPServer(listenerHTTPS, handler)
		srv.TLSConfig = &tls.Config{
			MinVersion:     tls.VersionTLS12,
			GetCertificate: s.certs.get,
		}
		s.servers = append(s.servers, srv)
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
	return nil
}

func (s *Server) closeListeners() {
	for _, ln := range s.listeners {
		ln.Close()
	}
}

func (s *Server) listenerAddr(name string) string {
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
