package server

import (
	"fmt"
	"os"
	"sync/atomic"

	"heckel.io/ntlb/config"
)

// logFiles are the open log files (access logs, the ban file) by path; uses of one path share it
type logFiles struct {
	logs atomic.Pointer[map[string]*accessLog]
}

func newLogFiles() *logFiles {
	l := &logFiles{}
	l.logs.Store(&map[string]*accessLog{})
	return l
}

// logPaths are the files a config writes to
func logPaths(conf *config.Config) []string {
	var paths []string
	for _, l := range conf.AccessLogs {
		paths = append(paths, l.File)
	}
	for _, site := range conf.Sites {
		for _, l := range site.AccessLogs {
			paths = append(paths, l.File)
		}
	}
	if conf.Bans != nil {
		paths = append(paths, conf.Bans.File)
	}
	return paths
}

// openLogFiles opens every path once, so that New and Reload can fail before they change anything
func openLogFiles(paths []string) (map[string]*os.File, error) {
	files := make(map[string]*os.File)
	for _, path := range paths {
		if files[path] != nil {
			continue
		}
		file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
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
func (l *logFiles) swap(files map[string]*os.File) {
	old := *l.logs.Load()
	next := make(map[string]*accessLog, len(files))
	for path, file := range files {
		if log, ok := old[path]; ok {
			log.swap(file)
			next[path] = log
		} else {
			next[path] = newAccessLogFile(file)
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
func (l *logFiles) get(path string) *accessLog {
	return (*l.logs.Load())[path]
}

func (l *logFiles) close() {
	l.swap(nil)
}
