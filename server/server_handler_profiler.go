package server

import (
	"net/http"
	"net/http/pprof"
	"runtime"
	"sync"

	"heckel.io/ntlb/api"
)

var profileRatesOnce sync.Once

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

// setProfileRates turns on the mutex profile, which is off (empty) by default, so it is ready
// before a problem shows; the rate is the process's, so once
func setProfileRates() {
	profileRatesOnce.Do(func() {
		runtime.SetMutexProfileFraction(mutexProfileFraction)
	})
}
