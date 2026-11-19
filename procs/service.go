// Package procs adapts GOMAXPROCS to the load. A load balancer is mostly idle goroutines woken
// for a few microseconds each (a message on a stream, a request), and every wakeup with an idle P
// wakes an OS thread to spin for work and sleep again: at a fraction of a core of use over all
// of a machine's CPUs, that futex and scheduler churn costs as much CPU as the proxying itself.
// Fewer Ps make it go away (in the lab: half the CPU of HTTP/2 streams at one P instead of
// twelve), but a fixed small number would cap a reconnect storm, which takes several cores for a
// minute. So the Ps follow the load: raised right away once they get busy, lowered once the load
// stayed low for a while.
package procs

import (
	"context"
	"log/slog"
	"math"
	"os"
	"runtime"
	"syscall"
	"time"
)

const (
	interval   = 250 * time.Millisecond // How often the use is sampled
	minProcs   = 3                      // Never fewer: at 2, a fan-out to thousands of streams at once took twice as long
	targetUse  = 0.2                    // Use per P after a change: room for bursts, and idle Ps for the GC (docs/design.md, Scale test)
	raiseAbove = 0.3                    // Use per P above which Ps are added right away
	lowerBelow = 0.12                   // Use per P below which Ps are taken away, once it held for lowerAfter samples
	lowerAfter = 8                      // Samples (2s)
)

// Adapter decides the number of Ps from samples of the CPU use
type Adapter struct {
	procs int // The current number of Ps
	max   int // All CPUs, as the runtime saw them at the start
	low   int // Consecutive samples below lowerBelow
}

// Run adapts GOMAXPROCS until ctx is done, unless GOMAXPROCS is set in the environment (the
// operator's choice wins) or there are too few CPUs to adapt
func Run(ctx context.Context) {
	max := runtime.GOMAXPROCS(0)
	if !enabled(max) {
		return
	}
	a := newAdapter(max, max)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	lastCPU, lastTime := cpuTime(), time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			cpu := cpuTime()
			used := (cpu - lastCPU).Seconds() / now.Sub(lastTime).Seconds()
			lastCPU, lastTime = cpu, now
			if n := a.next(used); n != runtime.GOMAXPROCS(0) {
				slog.Debug("Adapting GOMAXPROCS", "procs", n, "cores_used", math.Round(used*100)/100)
				runtime.GOMAXPROCS(n)
			}
		}
	}
}

func newAdapter(procs, max int) *Adapter {
	return &Adapter{
		procs: procs,
		max:   max,
	}
}

// next returns the number of Ps after a sample of used cores
func (a *Adapter) next(used float64) int {
	perProc := used / float64(a.procs)
	switch {
	case perProc > raiseAbove:
		a.low = 0
		a.procs = a.fit(used)
	case perProc < lowerBelow:
		a.low++
		if a.low >= lowerAfter {
			a.low = 0
			a.procs = a.fit(used)
		}
	default:
		a.low = 0
	}
	return a.procs
}

// fit returns the number of Ps that used cores keep at targetUse each
func (a *Adapter) fit(used float64) int {
	return min(max(int(math.Ceil(used/targetUse)), minProcs), a.max)
}

func enabled(max int) bool {
	return os.Getenv("GOMAXPROCS") == "" && max > minProcs
}

// cpuTime is the CPU time the process used so far, user and system
func cpuTime() time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}
