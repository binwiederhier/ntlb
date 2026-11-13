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
	"strings"
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

// Names of the admin, metrics and profiler listeners; the public ones are named by their address
const (
	listenerAdmin    = "admin"
	listenerMetrics  = "metrics"
	listenerProfiler = "profiler"
	listenerHealth   = "health"
)

var (
	// tls12CipherSuites are Mozilla's "intermediate" TLS 1.2 suites, as in the nginx config: AEAD
	// only (TLS 1.3 suites are not configurable)
	tls12CipherSuites = []uint16{
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
		tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
	}
	errShuttingDown = errors.New("shutting down, not reloading")
)

// Server is the load balancer process: listeners, proxy and admin API around a balancer.Balancer
type Server struct {
	configFile  string
	config      atomic.Pointer[config.Config]
	balancer    *balancer.Balancer
	proxy       *proxy
	rules       *rules.Engine
	certs       *certStore
	files       *logFiles // Access logs and the ban file
	metrics     *metrics
	public      map[string]*publicServer // Proxying servers by listen address
	retired     []*publicServer          // Removed by a reload: no new connections, open ones run on
	admin       *http.Server
	metricsSrv  *http.Server // listen-metrics, if set
	profilerSrv *http.Server // listen-profiler, if set
	healthSrv   *http.Server // listen-health, if set
	listeners   map[string]net.Listener
	serving     bool       // Serve started the servers; ones added later start right away
	errChan     chan error // A listener that failed; Serve shuts down
	stopped     chan struct{}
	stopping    bool // Shutdown began: no more reloads, protected by reloadMu
	stopOnce    sync.Once
	reloadMu    sync.Mutex // Serializes reloads, and keeps them out of Shutdown
	mu          sync.Mutex // Protects public, retired, listeners, serving
}

// statusWriter records the status and body size of a response. Unwrap lets
// http.ResponseController (used by the reverse proxy) reach Flush and Hijack.
type statusWriter struct {
	http.ResponseWriter
	status int
	bytes  int64
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
		public:     make(map[string]*publicServer),
		listeners:  make(map[string]net.Listener),
		errChan:    make(chan error, 1),
		stopped:    make(chan struct{}),
	}
	s.config.Store(conf)
	s.rules = rules.New(conf, s.writeBan)
	s.metrics = newMetrics(s.balancer)
	s.proxy = newProxy(s.balancer, s.metrics, conf)
	certs, err := loadCerts(conf)
	if err != nil {
		return nil, err
	}
	s.certs = &certStore{}
	s.certs.store(certs)
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
	s.mu.Lock()
	s.serving = true
	for _, ps := range s.public {
		go s.servePublic(ps)
	}
	go s.serve(listenerAdmin, s.admin, s.listeners[listenerAdmin], false)
	if s.metricsSrv != nil {
		go s.serve(listenerMetrics, s.metricsSrv, s.listeners[listenerMetrics], false)
	}
	if s.profilerSrv != nil {
		go s.serve(listenerProfiler, s.profilerSrv, s.listeners[listenerProfiler], false)
	}
	if s.healthSrv != nil {
		go s.serve(listenerHealth, s.healthSrv, s.listeners[listenerHealth], false)
	}
	s.mu.Unlock()
	select {
	case err := <-s.errChan:
		s.Shutdown(0, 0)
		return err
	case <-s.stopped:
		return nil
	}
}

// Reload re-reads the config file and the files it includes, and applies them: sites and their
// listen addresses, upstreams, health checks, timeouts, drain durations, streams, the client
// key, rules, bans, the certificates and the log files. A new listen address is bound before
// anything changes; one that no site uses any more stops taking connections, while the ones it
// has run on. A broken config (or a certificate, log file or address that cannot be opened) is
// rejected as a whole. It returns the changed settings that only a restart applies.
func (s *Server) Reload() ([]string, error) {
	s.reloadMu.Lock()
	defer s.reloadMu.Unlock()
	if s.stopping {
		return nil, errShuttingDown
	}
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
	certs, err := loadCerts(conf)
	if err != nil {
		return nil, err
	}
	added, err := s.bindAdded(conf)
	if err != nil {
		return nil, err
	}
	files, err := openLogFiles(logPaths(conf))
	if err != nil {
		closePublic(added)
		return nil, err
	}
	s.certs.store(certs)
	s.files.swap(files)
	s.rules.Update(conf)
	s.balancer.Update(conf)
	s.proxy.update(conf)
	s.config.Store(conf)
	s.swapPublic(conf, added)
	slog.Info("Config reloaded", "sites", len(conf.Sites), "upstreams", conf.UpstreamCount())
	return restart, nil
}

// Shutdown drains the load balancer over d (not ready right away, streams closed oldest first),
// then closes the listeners and returns once every response finished or the grace period passed
func (s *Server) Shutdown(delay, d time.Duration) {
	s.stopOnce.Do(func() {
		slog.Info("Shutting down", "delay", delay, "drain", d)
		s.reloadMu.Lock() // Waits for a reload in progress
		s.stopping = true
		s.reloadMu.Unlock()
		if delay > 0 {
			// Withdrawing, like ntfy's drain delay: not ready, so the health checks in front take
			// the load balancer out, but still serving everything to clients that have its address
			s.balancer.Withdraw()
			time.Sleep(delay)
		}
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
		servers := []*http.Server{s.admin}
		if s.metricsSrv != nil {
			servers = append(servers, s.metricsSrv)
		}
		if s.profilerSrv != nil {
			servers = append(servers, s.profilerSrv)
		}
		if s.healthSrv != nil {
			servers = append(servers, s.healthSrv)
		}
		s.mu.Lock()
		for _, ps := range s.public {
			servers = append(servers, ps.srv)
		}
		for _, ps := range s.retired {
			servers = append(servers, ps.srv)
		}
		s.mu.Unlock()
		for _, srv := range servers {
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

func (s *Server) servePublic(ps *publicServer) {
	err := s.serveUntilClosed(ps.srv.Addr, ps.srv, ps.ln, ps.tls)
	if err != nil && !ps.retired.Load() {
		s.fail(err)
	}
}

func (s *Server) serve(name string, srv *http.Server, ln net.Listener, tls bool) {
	if err := s.serveUntilClosed(name, srv, ln, tls); err != nil {
		s.fail(err)
	}
}

// serveUntilClosed serves until the server or its listener is closed; it returns an unexpected error
func (s *Server) serveUntilClosed(name string, srv *http.Server, ln net.Listener, tls bool) error {
	slog.Info("Listening", "listener", name, "addr", ln.Addr().String())
	var err error
	if tls {
		err = srv.ServeTLS(ln, "", "")
	} else {
		err = srv.Serve(ln)
	}
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("%s listener: %w", name, err)
	}
	return nil
}

// fail hands a listener's error to Serve, which shuts down
func (s *Server) fail(err error) {
	select {
	case s.errChan <- err:
	default: // Already failing
	}
}

// observe wraps the proxy to see every request's outcome, for the access logs, the response rules
// and the metrics, and answers the requests that a rule decides itself. The outcome is recorded
// by a finisher, when the request ends, which for a detached stream is after the handler returned.
// listen is the listener's configured address, which decides the sites the request can go to.
func (s *Server) observe(listen string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		markHandled(r.Context())
		conf := s.config.Load()
		if conf.Listener(listen) == nil {
			// A connection to a listener a reload removed: send the client to reconnect (HTTP/2:
			// GOAWAY), which lands it on an address that is still there
			s.metrics.refused.Inc()
			w.Header().Set(headerConnection, connectionClose)
			writeError(w, conf.ErrorPagesFor(nil), httpErrListenerClosed)
			return
		}
		env := match.NewEnv(r)
		site := conf.SiteFor(listen, r.Host)
		if site != nil {
			env.Site = site.Name
		}
		respEnv := match.NewResponseEnv(env) // Strings only, so the request can be let go
		var entry *accessLogEntry
		if len(conf.AccessLogsFor(site)) > 0 {
			entry = newAccessLogEntry(r, start)
		}
		pr := &proxyRequest{
			env:        env,
			site:       env.Site,
			siteConf:   site,
			errorPages: conf.ErrorPagesFor(site),
		}
		sw := &statusWriter{ResponseWriter: w}
		valid := validRequest(r)
		var rule *config.Rule
		if valid {
			rule = s.rules.Check(r, env)
		}
		if rule != nil {
			pr.rule, pr.kind = rule.ID, kindRequest
			s.metrics.ruleMatches.WithLabelValues(rule.ID).Inc()
		}
		if rule != nil && rule.Action == config.ActionProxy {
			pr.pool = rule.Upstream
			rule = nil // Proxied like any other request, only to that upstream
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
		if !valid {
			writeError(sw, pr.errorPages, httpErrBadRequest)
			return
		}
		var out http.ResponseWriter = sw
		if rule != nil {
			out = boundAnswer(sw, r, site)
		}
		w = newGzipWriter(out, r, conf.GzipFor(site), pr)
		if gw, ok := w.(*gzipWriter); ok {
			defer gw.close() // Runs before the deferred finish above, which reads sw
		}
		if rule != nil {
			s.answer(w, r, env, rule, pr.errorPages)
			return
		}
		next.ServeHTTP(w, r.WithContext(withProxyRequest(r.Context(), pr)))
	})
}

// boundAnswer prepares the answer of a rule like a proxied response: every write to the client is
// bounded by the site's read timeout, so a client that stops reading (say, a large static file)
// is cut instead of holding the handler, the file and a compressor; and a request body is not
// waited for: the connection closes after the answer instead of reading the rest of the body.
func boundAnswer(sw *statusWriter, r *http.Request, site *config.Site) http.ResponseWriter {
	timeout := config.DefaultUpstreamReadTimeout
	if site != nil {
		timeout = site.DefaultUpstream().Timeouts.Read
	}
	if r.ContentLength != 0 {
		sw.Header().Set(headerConnection, connectionClose)
	}
	return &deadlineWriter{
		ResponseWriter: sw,
		conn: &clientConn{
			ctrl: http.NewResponseController(sw),
		},
		timeout: timeout,
	}
}

// writeAccessLog writes the request to the first access log of its site whose match is true. The
// logs come from the current config, not the request's: a stream can outlive reloads that move
// its log, whose old file is closed then.
func (s *Server) writeAccessLog(entry *accessLogEntry, pr *proxyRequest, env *match.ResponseEnv) {
	conf := s.config.Load()
	for _, l := range conf.AccessLogsFor(conf.Site(pr.site)) {
		if l.Program == nil || l.Program.Match(env) {
			if log := s.files.get(l.File); log != nil {
				log.write(entry, pr, env.Status)
			}
			return
		}
	}
}

// writeBan writes a ban line to a ban file (the global one, or a site's)
func (s *Server) writeBan(file, line string) {
	s.metrics.bans.Inc()
	if log := s.files.get(file); log != nil {
		log.writeLine([]byte(line + "\n"))
	}
}

// WriteHeader records the first final status: an informational one (100 Continue, 103 Early
// Hints) is followed by the real one, except 101, which ends the response as an upgrade
func (w *statusWriter) WriteHeader(status int) {
	if w.status == 0 && (status >= 200 || status == http.StatusSwitchingProtocols) {
		w.status = status
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(p)
	w.bytes += int64(n)
	return n, err
}

func (w *statusWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

// validRequest checks what Go's HTTP/1 parser checks but HTTP/2 lets through as :path and
// :authority: an origin-form target (or "*") without spaces or control bytes (quotes are fine:
// scripts send them, and the access log escapes them), and a host of name or IP characters with
// an optional numeric port
func validRequest(r *http.Request) bool {
	uri := r.RequestURI
	if uri != "*" && !strings.HasPrefix(uri, "/") && !strings.HasPrefix(uri, "http://") && !strings.HasPrefix(uri, "https://") {
		return false // Not origin-form, absolute-form (HTTP/1.1 proxies) or asterisk-form
	}
	for i := 0; i < len(uri); i++ {
		if c := uri[i]; c <= ' ' || c == 0x7f {
			return false
		}
	}
	host, port := r.Host, ""
	if strings.Contains(r.Host, ":") { // Only then: SplitHostPort's error for a host without a port allocates
		if h, p, err := net.SplitHostPort(r.Host); err == nil {
			host, port = h, p
		} else if strings.Contains(strings.TrimSuffix(strings.TrimPrefix(r.Host, "["), "]"), ":") && !strings.HasPrefix(r.Host, "[") {
			return false // A colon, but no valid port
		}
	}
	for i := 0; i < len(port); i++ {
		if port[i] < '0' || port[i] > '9' {
			return false
		}
	}
	for i := 0; i < len(host); i++ {
		if !isHostChar(host[i]) {
			return false
		}
	}
	return true
}

// isHostChar reports whether c may appear in a host name or IP literal
func isHostChar(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		return true
	}
	return strings.IndexByte(".-_:[]", c) >= 0
}
