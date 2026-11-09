package server

import (
	"fmt"
	"os"
	"sync/atomic"

	"heckel.io/ntfy-lb/config"
)

// ruleLogs are the open log files of the rules, by path; rules that share a path share the file
type ruleLogs struct {
	logs atomic.Pointer[map[string]*accessLog]
}

func newRuleLogs() *ruleLogs {
	l := &ruleLogs{}
	l.logs.Store(&map[string]*accessLog{})
	return l
}

// openRuleLogFiles opens the log file of every rule, once per path, so that New and Reload can
// fail before they change anything
func openRuleLogFiles(conf *config.Config) (map[string]*os.File, error) {
	files := make(map[string]*os.File)
	for _, r := range conf.Rules {
		if r.Log == "" || files[r.Log] != nil {
			continue
		}
		file, err := openAccessLog(r.Log)
		if err != nil {
			for _, f := range files {
				f.Close()
			}
			return nil, fmt.Errorf("rule %s: %w", r.Name, err)
		}
		files[r.Log] = file
	}
	return files, nil
}

// swap makes files the rule logs. A path that stays keeps its log, reopened (for logrotate); the
// logs of paths that are gone are closed.
func (l *ruleLogs) swap(files map[string]*os.File) {
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
func (l *ruleLogs) get(path string) *accessLog {
	return (*l.logs.Load())[path]
}

func (l *ruleLogs) close() {
	l.swap(nil)
}
