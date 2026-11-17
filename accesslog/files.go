package accesslog

import (
	"fmt"
	"os"
	"sync/atomic"

	"heckel.io/ntlb/config"
)

// Files are the open log files (access logs, the ban file) by path; uses of one path share it
type Files struct {
	logs atomic.Pointer[map[string]*Log]
}

func NewFiles() *Files {
	l := &Files{}
	l.logs.Store(&map[string]*Log{})
	return l
}

// Paths are the files a config writes to
func Paths(conf *config.Config) []string {
	var paths []string
	for _, l := range conf.AccessLogs {
		paths = append(paths, l.File)
	}
	for _, site := range conf.Sites {
		for _, l := range site.AccessLogs {
			paths = append(paths, l.File)
		}
		if site.Bans != nil {
			paths = append(paths, site.Bans.File)
		}
	}
	if conf.Bans != nil {
		paths = append(paths, conf.Bans.File)
	}
	return paths
}

// Open opens every path once, so that New and Reload can fail before they change anything
func Open(paths []string) (map[string]*os.File, error) {
	files := make(map[string]*os.File)
	for _, path := range paths {
		if files[path] != nil {
			continue
		}
		// Like nginx's logs: they hold client addresses and topic names, which only the log
		// readers' group (adm, via the directory) may read
		file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0640)
		if err != nil {
			for _, f := range files {
				f.Close()
			}
			return nil, fmt.Errorf("cannot open log file: %w", err)
		}
		files[path] = file
	}
	return files, nil
}

// swap makes files the log files. A path that stays keeps its log, reopened (for logrotate); the
// logs of paths that are gone are closed.
func (l *Files) Swap(files map[string]*os.File) {
	old := *l.logs.Load()
	next := make(map[string]*Log, len(files))
	for path, file := range files {
		if log, ok := old[path]; ok {
			log.swap(file)
			next[path] = log
		} else {
			next[path] = newLog(file)
		}
	}
	for path, log := range old {
		if _, ok := next[path]; !ok {
			log.close()
		}
	}
	l.logs.Store(&next)
}

// get returns the log of a path, or nil if a reload just closed it
func (l *Files) Get(path string) *Log {
	return (*l.logs.Load())[path]
}

func (l *Files) Close() {
	l.Swap(nil)
}
