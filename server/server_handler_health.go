package server

import (
	"net/http"
)

// healthHandler serves only /v1/ready, for listen-health: a port of its own that an outside health
// check (Route53, a load balancer in front) can reach, without shadowing a path of the sites
func (s *Server) healthHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+apiReadyPath, s.handleAdmin(s.handleReady))
	return mux
}
