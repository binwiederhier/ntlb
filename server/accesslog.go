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
	maxLogField            = 4096                         // Bytes per escaped field, so one request cannot write a huge line
)

// accessLog is a buffered log file that a reload can swap for a reopened one (for logrotate). As
// an access log it writes one line per request in the format of the nginx log_format
// "combined_ssl" on ntfy.sh (combined, then key=value fields), so the existing parsers keep working.
type accessLog struct {
	file *os.File
	w    *bufio.Writer
	done chan struct{}
	mu   sync.Mutex // Protects file and w
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

// newAccessLogFile creates a log that writes to an opened file (nil: off)
func newAccessLogFile(file *os.File) *accessLog {
	l := &accessLog{
		done: make(chan struct{}),
	}
	l.swap(file)
	go l.flushLoop()
	return l
}

// newAccessLogEntry takes what a log line needs from the request
func newAccessLogEntry(r *http.Request, start time.Time) *accessLogEntry {
	remoteAddr, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		remoteAddr = r.RemoteAddr
	}
	return &accessLogEntry{
		start:      start,
		remoteAddr: remoteAddr,
		request:    escapeUnquoted(r.Method) + " " + escapeUnquoted(redactAuth(r.RequestURI)) + " " + escapeUnquoted(r.Proto),
		referer:    escape(r.Referer()),
		userAgent:  escape(r.UserAgent()),
		host:       escapeUnquoted(r.Host),
	}
}

func (l *accessLog) write(e *accessLogEntry, pr *proxyRequest, status int) {
	bytes := pr.bytes
	site, upstream, upstreamStatus := "-", "-", "-"
	if pr.site != "" {
		site = pr.site
	}
	if pr.upstream != "" {
		upstream, upstreamStatus = pr.upstream, fmt.Sprint(pr.upstreamStatus)
	}
	line := fmt.Sprintf("%s - - [%s] \"%s\" %d %d \"%s\" \"%s\" http_host=%s site=%s request_time=%.3f upstream=%s upstream_status=%s",
		e.remoteAddr, e.start.Format(accessLogTimeFormat), e.request, status, bytes,
		orDash(e.referer), orDash(e.userAgent), orDash(e.host), site, time.Since(e.start).Seconds(), upstream, upstreamStatus)
	if pr.rule != "" {
		line += " rule=" + pr.rule
	}
	line += "\n"
	l.writeLine(line)
}

// writeLine writes a line that ends in a newline
func (l *accessLog) writeLine(line string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.w != nil { // Closed while the request ran
		l.w.WriteString(line)
	}
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

// escape escapes a log field the way nginx does: quotes, backslashes, and the bytes that are not
// printable ASCII become \xHH, so a field cannot end its quotes or the line. Long fields are cut.
func escape(s string) string {
	return escapeField(s, false)
}

// escapeUnquoted is escape for a field outside quotes (the request line's parts, http_host), where
// a space would start another field
func escapeUnquoted(s string) string {
	return escapeField(s, true)
}

func escapeField(s string, spaces bool) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < 0x20 || c > 0x7e || c == '"' || c == '\\' || (spaces && c == ' ') {
			fmt.Fprintf(&b, "\\x%02X", c)
		} else {
			b.WriteByte(c)
		}
		if b.Len() >= maxLogField {
			return b.String() + "..."
		}
	}
	return b.String()
}

// redactAuth hides the values of auth query parameters (the web app's credentials)
func redactAuth(uri string) string {
	path, query, ok := strings.Cut(uri, "?")
	if !ok {
		return uri
	}
	params := strings.Split(query, "&")
	for i, p := range params {
		if key, _, ok := strings.Cut(p, "="); ok && strings.EqualFold(key, "auth") {
			params[i] = key + "=REDACTED"
		}
	}
	return path + "?" + strings.Join(params, "&")
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
