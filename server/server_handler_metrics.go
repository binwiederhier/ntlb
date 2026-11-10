package server

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// metricsHandler serves only /metrics, for listen-metrics: unlike the admin listener, it can sit
// on a private network, because nothing on it changes or reveals more than the metrics
func (s *Server) metricsHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET "+apiMetricsPath, promhttp.HandlerFor(s.metrics.registry, promhttp.HandlerOpts{}))
	return mux
}
