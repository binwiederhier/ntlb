package accesslog

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpen_ClosesOnFailure(t *testing.T) {
	dir := t.TempDir()
	opened := openFDs(t)
	_, err := Open([]string{filepath.Join(dir, "a.log"), filepath.Join(dir, "a.log"), filepath.Join(dir, "missing", "c.log")})
	require.ErrorContains(t, err, "missing")
	require.Equal(t, opened, openFDs(t))
}

// openFDs counts the open file descriptors of this process
func openFDs(t testing.TB) int {
	entries, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	return len(entries)
}

func TestOpen_NotWorldReadable(t *testing.T) {
	// Access logs hold every client address and topic name, and ntfy's topic names are secrets
	// for open topics: like nginx's logs (0640, group adm), others cannot read them
	defer syscall.Umask(syscall.Umask(0o022))
	path := filepath.Join(t.TempDir(), "access.log")
	files, err := Open([]string{path})
	require.NoError(t, err)
	files[path].Close()
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o640), info.Mode().Perm())
}

func TestFiles_SharedPath(t *testing.T) {
	// Access logs and ban files that name the same path share one open file and one buffer
	path := filepath.Join(t.TempDir(), "all.log")
	files, err := Open([]string{path, path, path})
	require.NoError(t, err)
	require.Len(t, files, 1)
	l := NewFiles()
	defer l.Close()
	l.Swap(files)
	require.NotNil(t, l.Get(path))
}
