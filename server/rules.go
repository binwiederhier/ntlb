package server

import (
	"log/slog"
	"net/http"
	"os"
	"path"
	"path/filepath"

	"heckel.io/ntfy-lb/config"
	"heckel.io/ntfy-lb/match"
)

// answer answers a request that a rule decided: blocked, limited, redirected or served from disk
func (s *Server) answer(w http.ResponseWriter, r *http.Request, env *match.Env, rule *config.Rule) {
	switch rule.Action {
	case config.ActionRedirect:
		location := rule.LocationProgram.String(env)
		if location == "" {
			slog.Warn("Redirect rule has no location for a request", "rule", rule.Name, "uri", env.URI)
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Location", location)
		w.WriteHeader(rule.Status)
	case config.ActionStatic:
		serveStatic(w, r, rule.Root, rule.Fallback)
	default:
		http.Error(w, http.StatusText(rule.Status), rule.Status)
	}
}

// serveStatic serves the file at the request path under root, like nginx's try_files $uri
// <fallback>: a path that is no regular file (missing, a directory) gets the fallback, or a 404
// without one. The path is cleaned first, so it cannot leave root.
func serveStatic(w http.ResponseWriter, r *http.Request, root, fallback string) {
	for _, p := range []string{r.URL.Path, fallback} {
		if p == "" {
			continue
		}
		file, info := openStatic(root, p)
		if file == nil {
			continue
		}
		defer file.Close()
		http.ServeContent(w, r, info.Name(), info.ModTime(), file)
		return
	}
	http.NotFound(w, r)
}

func openStatic(root, p string) (*os.File, os.FileInfo) {
	file, err := os.Open(filepath.Join(root, filepath.FromSlash(path.Clean("/"+p))))
	if err != nil {
		return nil, nil
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		file.Close()
		return nil, nil
	}
	return file, info
}
