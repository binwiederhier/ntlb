package server

import (
	"bufio"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	accessLogFlushInterval = time.Second
	accessLogTimeFormat    = "02/Jan/2006:15:04:05 -0700" // nginx's $time_local
)

// accessLog writes one line per request in the format of the nginx log_format "combined_ssl" on
// ntfy.sh (combined, then key=value fields), so the existing log parsers keep working. It is
// reopened on reload, for logrotate.
type accessLog struct {
	filename string
	file     *os.File
	w        *bufio.Writer
	done     chan struct{}
	mu       sync.Mutex // Protects file and w
}

// statusWriter records the status and body size of a response. Unwrap lets
// http.ResponseController (used by the reverse proxy) reach Flush and Hijack.
type statusWriter struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (w *statusWriter) WriteHeader(status int) {
	if w.status == 0 {
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

func newAccessLog(filename string) (*accessLog, error) {
	l := &accessLog{
		filename: filename,
		done:     make(chan struct{}),
	}
	if err := l.reopen(); err != nil {
		return nil, err
	}
	go l.flushLoop()
	return l, nil
}

// accessLogEntry holds what a log line needs from the request, taken upfront so that a stream
// that outlives its handler does not keep the whole request alive
type accessLogEntry struct {
	start      time.Time
	remoteAddr string
	request    string // "GET /path HTTP/1.1", unquoted
	referer    string
	userAgent  string
	host       string
}

// handler logs every request served by next, when it ends
func (l *accessLog) handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteAddr, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			remoteAddr = r.RemoteAddr
		}
		entry := &accessLogEntry{
			start:      time.Now(),
			remoteAddr: remoteAddr,
			request:    r.Method + " " + escape(r.RequestURI) + " " + r.Proto,
			referer:    r.Referer(),
			userAgent:  r.UserAgent(),
			host:       r.Host,
		}
		pr := &proxyRequest{}
		sw := &statusWriter{ResponseWriter: w}
		pr.onFinish(func() {
			l.write(entry, pr) // Must not hold sw: it references the server's connection state
		})
		defer func() { // Deferred: a stream cut short ends in a http.ErrAbortHandler panic
			if !pr.detached {
				if pr.status == 0 {
					pr.status, pr.bytes = sw.status, sw.bytes
				}
				pr.finish()
			}
		}()
		next.ServeHTTP(sw, r.WithContext(withProxyRequest(r.Context(), pr)))
	})
}

func (l *accessLog) write(e *accessLogEntry, pr *proxyRequest) {
	status, bytes := pr.status, pr.bytes
	if status == 0 && pr.upstreamStatus == http.StatusSwitchingProtocols {
		status = http.StatusSwitchingProtocols // Hijacked: the 101 went out on the raw connection
	}
	upstream, upstreamStatus := "-", "-"
	if pr.upstream != "" {
		upstream, upstreamStatus = pr.upstream, fmt.Sprint(pr.upstreamStatus)
	}
	line := fmt.Sprintf("%s - - [%s] \"%s\" %d %d \"%s\" \"%s\" http_host=%s request_time=%.3f upstream=%s upstream_status=%s\n",
		e.remoteAddr, e.start.Format(accessLogTimeFormat), e.request, status, bytes,
		escape(e.referer), escape(e.userAgent), escape(e.host), time.Since(e.start).Seconds(), upstream, upstreamStatus)
	l.mu.Lock()
	defer l.mu.Unlock()
	l.w.WriteString(line)
}

// reopen (re)opens the log file, e.g. after logrotate moved it
func (l *accessLog) reopen() error {
	file, err := os.OpenFile(l.filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file != nil {
		l.w.Flush()
		l.file.Close()
	}
	l.file, l.w = file, bufio.NewWriterSize(file, 64*1024)
	return nil
}

func (l *accessLog) close() {
	close(l.done)
	l.mu.Lock()
	defer l.mu.Unlock()
	l.w.Flush()
	l.file.Close()
}

func (l *accessLog) flushLoop() {
	ticker := time.NewTicker(accessLogFlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-l.done:
			return
		case <-ticker.C:
			l.mu.Lock()
			if err := l.w.Flush(); err != nil {
				slog.Warn("Cannot write access log", "error", err)
			}
			l.mu.Unlock()
		}
	}
}

// escape makes a value safe to put between quotes, like nginx's \x22 escaping, and "-" if empty
func escape(s string) string {
	if s == "" {
		return "-"
	}
	return strings.NewReplacer(`"`, `\x22`, `\`, `\x5C`, "\n", `\x0A`).Replace(s)
}
