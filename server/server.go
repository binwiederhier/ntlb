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
	"slices"
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
	listeners   map[string]net.Listener
	serving     bool       // Serve started the servers; ones added later start right away
	errChan     chan error // A listener that failed; Serve shuts down
	stopped     chan struct{}
	stopping    bool // Shutdown began: no more reloads, protected by reloadMu
	stopOnce    sync.Once
	reloadMu    sync.Mutex // Serializes reloads, and keeps them out of Shutdown
	mu          sync.Mutex // Protects public, retired, listeners, serving
}

// publicServer is the proxying server of one listen address
type publicServer struct {
	srv     *http.Server
	ln      net.Listener
	tls     bool
	retired atomic.Bool // Its listener was closed on purpose
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
	s.mu.Unlock()
	select {
	case err := <-s.errChan:
		s.Shutdown(0)
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
func (s *Server) Shutdown(d time.Duration) {
	s.stopOnce.Do(func() {
		slog.Info("Shutting down", "drain", d)
		s.reloadMu.Lock() // Waits for a reload in progress
		s.stopping = true
		s.reloadMu.Unlock()
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

// ProfilerAddr returns the address the profiler listener is bound to, or "" if there is none
func (s *Server) ProfilerAddr() string {
	return s.listenerAddr(listenerProfiler)
}

// AdminAddr returns the address the admin listener is bound to
func (s *Server) AdminAddr() string {
	return s.listenerAddr(listenerAdmin)
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
			http.Error(w, "listener closed, reconnect", http.StatusServiceUnavailable)
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
			env:      env,
			site:     env.Site,
			siteConf: site,
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
			http.Error(sw, "Bad Request", http.StatusBadRequest)
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
			s.answer(w, r, env, rule)
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
		log.writeLine(line + "\n")
	}
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
	if h, p, err := net.SplitHostPort(r.Host); err == nil {
		host, port = h, p
	} else if strings.Contains(strings.TrimSuffix(strings.TrimPrefix(r.Host, "["), "]"), ":") && !strings.HasPrefix(r.Host, "[") {
		return false // A colon, but no valid port
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
