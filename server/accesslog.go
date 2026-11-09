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
	"sync/atomic"
	"time"
)

const (
	accessLogFlushInterval = time.Second
	accessLogTimeFormat    = "02/Jan/2006:15:04:05 -0700" // nginx's $time_local
)

// accessLog writes one line per request in the format of the nginx log_format "combined_ssl" on
// ntfy.sh (combined, then key=value fields), so the existing log parsers keep working. A reload
// reopens it (for logrotate), moves it, or turns it on or off; with no file, nothing is logged.
type accessLog struct {
	enabled atomic.Bool
	file    *os.File
	w       *bufio.Writer
	done    chan struct{}
	mu      sync.Mutex // Protects file and w
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
		done: make(chan struct{}),
	}
	file, err := openAccessLog(filename)
	if err != nil {
		return nil, err
	}
	l.swap(file)
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

// entry takes what a log line needs from the request, or returns nil if logging is off
func (l *accessLog) entry(r *http.Request, start time.Time) *accessLogEntry {
	if !l.enabled.Load() {
		return nil
	}
	remoteAddr, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		remoteAddr = r.RemoteAddr
	}
	return &accessLogEntry{
		start:      start,
		remoteAddr: remoteAddr,
		request:    r.Method + " " + escape(r.RequestURI) + " " + r.Proto,
		referer:    r.Referer(),
		userAgent:  r.UserAgent(),
		host:       r.Host,
	}
}

func (l *accessLog) write(e *accessLogEntry, pr *proxyRequest, status int) {
	bytes := pr.bytes
	upstream, upstreamStatus := "-", "-"
	if pr.upstream != "" {
		upstream, upstreamStatus = pr.upstream, fmt.Sprint(pr.upstreamStatus)
	}
	line := fmt.Sprintf("%s - - [%s] \"%s\" %d %d \"%s\" \"%s\" http_host=%s request_time=%.3f upstream=%s upstream_status=%s\n",
		e.remoteAddr, e.start.Format(accessLogTimeFormat), e.request, status, bytes,
		escape(e.referer), escape(e.userAgent), escape(e.host), time.Since(e.start).Seconds(), upstream, upstreamStatus)
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.w != nil { // Turned off while the request ran
		l.w.WriteString(line)
	}
}

// openAccessLog opens (or creates) the log file for appending; nil if filename is empty
func openAccessLog(filename string) (*os.File, error) {
	if filename == "" {
		return nil, nil
	}
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("cannot open access log: %w", err)
	}
	return file, nil
}

// swap makes file the log file (nil turns logging off), flushing and closing the old one. It
// takes an opened file, so a reload can fail before it changes anything.
func (l *accessLog) swap(file *os.File) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file != nil {
		l.w.Flush()
		l.file.Close()
	}
	l.file, l.w = file, nil
	if file != nil {
		l.w = bufio.NewWriterSize(file, 64*1024)
	}
	l.enabled.Store(file != nil)
}

func (l *accessLog) close() {
	close(l.done)
	l.swap(nil)
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
			if l.w != nil {
				if err := l.w.Flush(); err != nil {
					slog.Warn("Cannot write access log", "error", err)
				}
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
