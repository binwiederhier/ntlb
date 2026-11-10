package server

import (
	"net/http"
	"net/http/pprof"
)

// profilerHandler serves only Go's profiles, for listen-profiler: CPU (profile), execution trace,
// and the named ones (heap, allocs, goroutine, block, mutex, threadcreate) through the index
func profilerHandler() http.Handler {
	mux := http.NewServeMux()
	handlePprof(mux)
	return mux
}

// handlePprof registers the profiles, for the profiler and the admin listener
func handlePprof(mux *http.ServeMux) {
	mux.HandleFunc("GET "+apiPprofPath, pprof.Index)
	mux.HandleFunc("GET "+apiPprofPath+"profile", pprof.Profile)
	mux.HandleFunc("GET "+apiPprofPath+"trace", pprof.Trace)
	mux.HandleFunc("GET "+apiPprofPath+"symbol", pprof.Symbol)
}
