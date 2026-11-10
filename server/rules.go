package server

import (
	"log/slog"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"

	"heckel.io/ntlb/config"
	"heckel.io/ntlb/match"
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
	if !cleanStaticPath(r.URL.Path) {
		http.NotFound(w, r)
		return
	}
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
	file, err := os.OpenInRoot(root, filepath.FromSlash(path.Clean("/" + p)[1:])) // Also no symlink out of root
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

// cleanStaticPath reports whether a path is what it names: no dot segments or empty segments
// (rules match the path as sent, so "/s/../x" must not serve "/x"), and no dotfiles
func cleanStaticPath(p string) bool {
	clean := path.Clean(p)
	if strings.HasSuffix(p, "/") && clean != "/" {
		clean += "/"
	}
	return clean == p && !strings.Contains(p, "/.")
}
