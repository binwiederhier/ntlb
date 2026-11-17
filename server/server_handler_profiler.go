package server

import (
	"net/http"
	"net/http/pprof"

	"heckel.io/ntlb/api"
)

// profilerHandler serves only Go's profiles, for listen-profiler: CPU (profile), execution trace,
// and the named ones (heap, allocs, goroutine, block, mutex, threadcreate) through the index
func profilerHandler() http.Handler {
	mux := http.NewServeMux()
	handlePprof(mux)
	return localOnly(mux)
}

// handlePprof registers the profiles, for the profiler and the admin listener
func handlePprof(mux *http.ServeMux) {
	mux.HandleFunc("GET "+api.PprofPath, pprof.Index)
	mux.HandleFunc("GET "+api.PprofPath+"profile", pprof.Profile)
	mux.HandleFunc("GET "+api.PprofPath+"trace", pprof.Trace)
	mux.HandleFunc("GET "+api.PprofPath+"symbol", pprof.Symbol)
}
