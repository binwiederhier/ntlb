package server

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogFiles_ReopenedOnReload(t *testing.T) {
	// logrotate moves the file away, then reloads: the log is reopened at its path
	app1 := newTestUpstream(t, "app1")
	logFile := filepath.Join(t.TempDir(), "access.log")
	s, _ := newTestServer(t, "access-logs: [{file: "+logFile+"}]", app1)
	get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/name")
	before := s.files.get(logFile)
	waitFor(t, func() bool {
		b, _ := os.ReadFile(logFile)
		return strings.Count(string(b), "\n") == 1
	})
	require.NoError(t, os.Rename(logFile, logFile+".1"))
	requireReload(t, s)
	require.Same(t, before, s.files.get(logFile)) // Same log, new file
	get(t, http.DefaultClient, "http://"+s.HTTPAddr()+"/name")
	waitFor(t, func() bool {
		b, _ := os.ReadFile(logFile)
		return strings.Count(string(b), "\n") == 1
	})
	b, err := os.ReadFile(logFile + ".1")
	require.NoError(t, err)
	require.Equal(t, 1, strings.Count(string(b), "\n"))
}

func TestLogFiles_SharedPath(t *testing.T) {
	// The ban file and an access log may even be the same file: one writer
	app1 := newTestUpstream(t, "app1")
	logFile := filepath.Join(t.TempDir(), "all.log")
	s, _ := newTestServer(t, "access-logs: [{file: "+logFile+"}, {file: "+logFile+"}]\nbans: {file: "+logFile+", threshold: 1}", app1)
	require.Len(t, *s.files.logs.Load(), 1)
}

func TestOpenLogFiles_ClosesOnFailure(t *testing.T) {
	dir := t.TempDir()
	opened := openFDs(t)
	_, err := openLogFiles([]string{filepath.Join(dir, "a.log"), filepath.Join(dir, "a.log"), filepath.Join(dir, "missing", "c.log")})
	require.ErrorContains(t, err, "missing")
	require.Equal(t, opened, openFDs(t))
}

// openFDs counts the open file descriptors of this process
func openFDs(t testing.TB) int {
	entries, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	return len(entries)
}

func TestOpenLogFiles_NotWorldReadable(t *testing.T) {
	// Access logs hold every client address and topic name, and ntfy's topic names are secrets
	// for open topics: like nginx's logs (0640, group adm), others cannot read them
	defer syscall.Umask(syscall.Umask(0o022))
	path := filepath.Join(t.TempDir(), "access.log")
	files, err := openLogFiles([]string{path})
	require.NoError(t, err)
	files[path].Close()
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o640), info.Mode().Perm())
}
