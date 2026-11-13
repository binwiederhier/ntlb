package server

import (
	"bufio"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	accessLogFlushInterval = time.Second
	accessLogLineSize      = 320                          // Room for a typical line, built in one allocation
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
	// Appended field by field, like fmt's "%s - - [%s] \"%s\" %d %d \"%s\" \"%s\" http_host=%s
	// site=%s request_time=%.3f upstream=%s upstream_status=%s", without boxing every argument
	line := make([]byte, 0, accessLogLineSize)
	line = append(line, e.remoteAddr...)
	line = append(line, " - - ["...)
	line = e.start.AppendFormat(line, accessLogTimeFormat)
	line = append(line, "] \""...)
	line = append(line, e.request...)
	line = append(line, "\" "...)
	line = strconv.AppendInt(line, int64(status), 10)
	line = append(line, ' ')
	line = strconv.AppendInt(line, pr.bytes, 10)
	line = append(line, " \""...)
	line = append(line, orDash(e.referer)...)
	line = append(line, "\" \""...)
	line = append(line, orDash(e.userAgent)...)
	line = append(line, "\" http_host="...)
	line = append(line, orDash(e.host)...)
	line = append(line, " site="...)
	line = append(line, orDash(pr.site)...)
	line = append(line, " request_time="...)
	line = strconv.AppendFloat(line, time.Since(e.start).Seconds(), 'f', 3, 64)
	line = append(line, " upstream="...)
	if pr.upstream != "" {
		line = append(line, pr.upstream...)
		line = append(line, " upstream_status="...)
		line = strconv.AppendInt(line, int64(pr.upstreamStatus), 10)
	} else {
		line = append(line, "- upstream_status=-"...)
	}
	if pr.rule != "" {
		line = append(line, " rule="...)
		line = append(line, pr.rule...)
	}
	l.writeLine(append(line, '\n'))
}

// writeLine writes a line that ends in a newline
func (l *accessLog) writeLine(line []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.w != nil { // Closed while the request ran
		l.w.Write(line)
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
	if len(s) < maxLogField && !needsEscape(s, spaces) {
		return s // The usual case: nothing to escape or cut
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if escaped(c, spaces) {
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

func needsEscape(s string, spaces bool) bool {
	for i := 0; i < len(s); i++ {
		if escaped(s[i], spaces) {
			return true
		}
	}
	return false
}

// escaped reports whether a byte is written as \xHH (see escape)
func escaped(c byte, spaces bool) bool {
	return c < 0x20 || c > 0x7e || c == '"' || c == '\\' || (spaces && c == ' ')
}

// redactAuth hides the values of auth query parameters (the web app's credentials). A URI without
// one is returned as it is.
func redactAuth(uri string) string {
	path, query, ok := strings.Cut(uri, "?")
	if !ok || !hasAuthParam(query) {
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

// hasAuthParam reports whether a query has an auth=... parameter, any case
func hasAuthParam(query string) bool {
	for query != "" {
		var p string
		p, query, _ = strings.Cut(query, "&")
		if key, _, ok := strings.Cut(p, "="); ok && strings.EqualFold(key, "auth") {
			return true
		}
	}
	return false
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
