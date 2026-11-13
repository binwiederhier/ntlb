package server

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"html/template"
	"log/slog"
	"net/http"
	"sync"

	"heckel.io/ntlb/config"
)

// The answers ntlb gives itself. A code names the reason, so that a client can tell a block from a
// limit or a drain. Shaped like ntfy's (status * 100 + n), but ntfy's count up from 01 and ntlb's
// down from 99, so they do not collide; the first three digits are the default status, which a
// block rule can change.
var (
	httpErrBadRequest        = &httpError{Code: 40099, HTTP: http.StatusBadRequest, Error: "bad request"}
	httpErrBlocked           = &httpError{Code: 40399, HTTP: http.StatusForbidden, Error: "blocked"}
	httpErrNoSite            = &httpError{Code: 40499, HTTP: http.StatusNotFound, Error: "no site for this host"}
	httpErrNotFound          = &httpError{Code: 40498, HTTP: http.StatusNotFound, Error: "not found"}
	httpErrRateLimited       = &httpError{Code: 42999, HTTP: http.StatusTooManyRequests, Error: "rate limited"}
	httpErrNoLocation        = &httpError{Code: 50099, HTTP: http.StatusInternalServerError, Error: "redirect has no location"}
	httpErrUpstreamFailed    = &httpError{Code: 50299, HTTP: http.StatusBadGateway, Error: "upstream failed"}
	httpErrUpstreamMissing   = &httpError{Code: 50298, HTTP: http.StatusBadGateway, Error: "upstream not configured"}
	httpErrDraining          = &httpError{Code: 50399, HTTP: http.StatusServiceUnavailable, Error: "load balancer is draining"}
	httpErrListenerClosed    = &httpError{Code: 50398, HTTP: http.StatusServiceUnavailable, Error: "listener closed, reconnect"}
	httpErrNoHealthyUpstream = &httpError{Code: 50397, HTTP: http.StatusServiceUnavailable, Error: "no upstream available"}
	httpErrDrained           = &httpError{Code: 50396, HTTP: http.StatusServiceUnavailable, Error: "drained, reconnect"}
	httpErrUpstreamTimeout   = &httpError{Code: 50499, HTTP: http.StatusGatewayTimeout, Error: "upstream timed out"}
)

const (
	errorOrigin         = "ntlb"
	maxCachedErrorPages = 1000 // Formats times codes times the statuses rules choose: far fewer
)

var (
	errorPages   = make(map[errorPageKey]*errorPage) // Rendered pages of the built-in formats
	errorPagesMu sync.RWMutex                        // Protects errorPages
	nosniff      = []string{"nosniff"}               // X-Content-Type-Options, shared
)

var (
	//go:embed errorpage.html
	errorPageHTML     string
	errorPageTemplate = template.Must(template.New("errorpage").Parse(errorPageHTML))
)

// errorPageKey identifies a page of a built-in format: rules choose the status of a block
type errorPageKey struct {
	format string
	code   int
	status int
}

// errorPage is a rendered error page
type errorPage struct {
	contentType []string // The Content-Type header's value
	body        []byte
}

// httpError is one of ntlb's own answers; also what error page templates get
type httpError struct {
	Code   int    `json:"code"`
	HTTP   int    `json:"http"`
	Status string `json:"-"` // The status text, "Forbidden"; set when written
	Error  string `json:"error"`
	Origin string `json:"origin"` // Always "ntlb": not an upstream's error
}

// withStatus returns the error with another status, a rule's
func (e *httpError) withStatus(status int) *httpError {
	return &httpError{
		Code:  e.Code,
		HTTP:  status,
		Error: e.Error,
	}
}

// writeError answers with one of ntlb's errors, in the format of the site's error pages. A
// template that fails falls back to text. The built-in formats are rendered once per error and
// status, so a flood of blocked or limited requests does not render a page for each.
func writeError(w http.ResponseWriter, pages *config.ErrorPages, e *httpError) {
	page := cachedErrorPage(pages, e)
	if page == nil {
		page = newErrorPage(pages, e)
	}
	h := w.Header()
	h.Del("Content-Length")
	h["Content-Type"] = page.contentType // Shared, never changed: Header.Set would allocate every time
	h["X-Content-Type-Options"] = nosniff
	w.WriteHeader(e.HTTP)
	w.Write(page.body)
}

// cachedErrorPage returns the rendered page of a built-in format, rendering it the first time;
// nil for a template of the config's own
func cachedErrorPage(pages *config.ErrorPages, e *httpError) *errorPage {
	if pages.Executor != nil {
		return nil
	}
	key := errorPageKey{
		format: pages.Format,
		code:   e.Code,
		status: e.HTTP,
	}
	errorPagesMu.RLock()
	page, ok := errorPages[key]
	errorPagesMu.RUnlock()
	if ok {
		return page
	}
	page = newErrorPage(pages, e)
	errorPagesMu.Lock()
	if len(errorPages) < maxCachedErrorPages {
		errorPages[key] = page
	}
	errorPagesMu.Unlock()
	return page
}

// newErrorPage renders an error page
func newErrorPage(pages *config.ErrorPages, e *httpError) *errorPage {
	data := &httpError{
		Code:   e.Code,
		HTTP:   e.HTTP,
		Status: http.StatusText(e.HTTP),
		Error:  e.Error,
		Origin: errorOrigin,
	}
	var b bytes.Buffer
	contentType, err := renderError(&b, pages, data)
	if err != nil {
		slog.Warn("Error page template failed, answering with text", "error", err)
		b.Reset()
		contentType = renderTextError(&b, data)
	}
	return &errorPage{
		contentType: []string{contentType},
		body:        b.Bytes(),
	}
}

// renderError writes the error page and returns its content type
func renderError(b *bytes.Buffer, pages *config.ErrorPages, data *httpError) (string, error) {
	switch {
	case pages.Executor != nil:
		return pages.ContentType, pages.Executor.Execute(b, data)
	case pages.Format == config.ErrorFormatJSON:
		return "application/json", json.NewEncoder(b).Encode(data)
	case pages.Format == config.ErrorFormatText:
		return renderTextError(b, data), nil
	default:
		return "text/html; charset=utf-8", errorPageTemplate.Execute(b, data)
	}
}

func renderTextError(b *bytes.Buffer, data *httpError) string {
	fmt.Fprintf(b, "Error %d: %s\n", data.Code, data.Error)
	return "text/plain; charset=utf-8"
}
