package server

import (
	"net/http"

	"heckel.io/ntlb/api"
)

// healthHandler serves only /v1/ready, for listen-health: a port of its own that an outside health
// check (Route53, a load balancer in front) can reach, without shadowing a path of the sites
func (s *Server) healthHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+api.ReadyPath, s.handleAdmin(s.handleReady))
	return mux
}
