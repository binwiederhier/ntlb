package server

import (
	"compress/gzip"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"

	"heckel.io/ntlb/config"
)

const (
	// gzipLevel and gzipMinLength are nginx's gzip_comp_level and gzip_min_length on ntfy.sh
	gzipLevel     = 6
	gzipMinLength = 1100
)

var (
	gzipWriters = sync.Pool{
		New: func() any {
			zw, _ := gzip.NewWriterLevel(nil, gzipLevel) // The level is valid
			return zw
		},
	}
)

// gzipWriter compresses a response if it qualifies, like nginx's gzip with gzip_proxied any: a
// listed media type, status 200, 403 or 404, no Content-Encoding yet, and at least gzipMinLength
// bytes. Without a Content-Length, it holds back the first bytes until there are enough or the
// response is flushed (then compressed), or it ends short (then not, e.g. an error page).
// Subscriptions are never compressed.
type gzipWriter struct {
	http.ResponseWriter
	pr      *proxyRequest
	types   []string
	head    bool   // A HEAD request: nothing to compress
	status  int    // Held back until the decision, 0 before WriteHeader
	pending []byte // Bytes held back until the decision
	decided bool
	zw      *gzip.Writer // Set if the response is compressed
}

// newGzipWriter wraps w if the client accepts gzip (HTTP/1.1 and later, like nginx's
// gzip_http_version), else returns w
func newGzipWriter(w http.ResponseWriter, r *http.Request, gz *config.Gzip, pr *proxyRequest) http.ResponseWriter {
	if gz == nil || !r.ProtoAtLeast(1, 1) || !acceptsGzip(r.Header.Values(headerAcceptEncoding)) {
		return w
	}
	return &gzipWriter{
		ResponseWriter: w,
		pr:             pr,
		types:          gz.Types,
		head:           r.Method == http.MethodHead,
	}
}

func (w *gzipWriter) WriteHeader(status int) {
	if w.decided || w.status != 0 {
		return // Like a ResponseWriter: a second call does nothing
	} else if status >= 100 && status < 200 {
		w.ResponseWriter.WriteHeader(status) // Informational: 100 Continue, 103
		return
	}
	w.status = status
	switch length := w.contentLength(); {
	case !w.qualifies():
		w.decide(false)
	case length >= 0:
		w.decide(length >= gzipMinLength)
	}
}

func (w *gzipWriter) Write(p []byte) (int, error) {
	if w.status == 0 && !w.decided {
		w.WriteHeader(http.StatusOK)
	}
	if !w.decided {
		w.pending = append(w.pending, p...)
		if len(w.pending) >= gzipMinLength {
			if err := w.decide(true); err != nil {
				return 0, err
			}
		}
		return len(p), nil
	} else if w.zw != nil {
		return w.zw.Write(p)
	}
	return w.ResponseWriter.Write(p)
}

// Flush sends what is held back and what the compressor holds (a sync flush), so a streamed
// response reaches the client as it goes. A flushed response of unknown length is compressed,
// like nginx does for any of them: the reverse proxy flushes after every write.
func (w *gzipWriter) Flush() {
	w.FlushError()
}

// FlushError is Flush, returning the error of the flush to the client (e.g. a reset HTTP/2
// stream), for http.ResponseController
func (w *gzipWriter) FlushError() error {
	if !w.decided {
		if w.status == 0 {
			w.status = http.StatusOK // Headers out before any body: as is, they cannot change later
			w.decide(false)
		} else {
			w.decide(true)
		}
	}
	if w.zw != nil {
		if err := w.zw.Flush(); err != nil {
			return err
		}
	}
	return http.NewResponseController(w.ResponseWriter).Flush()
}

// Unwrap lets http.ResponseController hijack the connection, for upgrades and detached streams,
// which are never compressed
func (w *gzipWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

// close ends the response once the handler returned: it sends what is held back, or the gzip trailer
func (w *gzipWriter) close() {
	if !w.decided && w.status != 0 {
		w.decide(false)
	}
	if w.zw != nil {
		w.zw.Close()
		w.zw.Reset(nil)
		gzipWriters.Put(w.zw)
		w.zw = nil
	}
}

// qualifies reports whether the response may be compressed, by everything but its length
func (w *gzipWriter) qualifies() bool {
	h := w.Header()
	if w.head || w.pr.kind != kindRequest || h.Get(headerContentEncoding) != "" {
		return false
	} else if w.status != http.StatusOK && w.status != http.StatusForbidden && w.status != http.StatusNotFound {
		return false
	}
	mediaType, _, _ := strings.Cut(h.Get(headerContentType), ";")
	return slices.Contains(w.types, strings.ToLower(strings.TrimSpace(mediaType)))
}

// contentLength returns the response's Content-Length, or -1 if it has none
func (w *gzipWriter) contentLength() int64 {
	length, err := strconv.ParseInt(w.Header().Get(headerContentLength), 10, 64)
	if err != nil {
		return -1
	}
	return length
}

// decide writes the header, compressed or not, and what was held back
func (w *gzipWriter) decide(compress bool) error {
	w.decided = true
	if compress {
		h := w.Header()
		h.Del(headerContentLength)
		h.Del(headerAcceptRanges)
		h.Set(headerContentEncoding, encodingGzip)
		h.Add(headerVary, headerAcceptEncoding)
		if etag := h.Get(headerETag); etag != "" && !strings.HasPrefix(etag, "W/") {
			h.Set(headerETag, "W/"+etag) // The bytes differ, like nginx's weakened ETag
		}
		w.zw = gzipWriters.Get().(*gzip.Writer)
		w.zw.Reset(w.ResponseWriter)
	}
	w.ResponseWriter.WriteHeader(w.status)
	pending := w.pending
	w.pending = nil
	if len(pending) == 0 {
		return nil
	} else if w.zw != nil {
		_, err := w.zw.Write(pending)
		return err
	}
	_, err := w.ResponseWriter.Write(pending)
	return err
}

// acceptsGzip reports whether the Accept-Encoding values list gzip, with a q-value other than 0
func acceptsGzip(values []string) bool {
	for _, value := range values {
		for coding := range strings.SplitSeq(value, ",") {
			name, params, _ := strings.Cut(coding, ";")
			if !strings.EqualFold(strings.TrimSpace(name), encodingGzip) {
				continue
			}
			q, found := strings.CutPrefix(strings.ReplaceAll(strings.ToLower(params), " ", ""), "q=")
			if !found {
				return true
			} else if v, err := strconv.ParseFloat(q, 64); err == nil && v > 0 {
				return true
			}
		}
	}
	return false
}

// unwrapGzip returns the ResponseWriter under a gzipWriter, or w: for the stream path, which never
// compresses, so a subscription does not go through the wrapper for its whole life
func unwrapGzip(w http.ResponseWriter) http.ResponseWriter {
	if gw, ok := w.(*gzipWriter); ok {
		return gw.ResponseWriter
	}
	return w
}
