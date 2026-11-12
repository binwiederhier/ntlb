package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"heckel.io/ntlb/balancer"
	"heckel.io/ntlb/config"
	"heckel.io/ntlb/server"
)

// startSignalHandler runs handleSignals on an unbuffered channel, so that a send returns only
// once the previous signal was handled. The returned channel receives every exit call.
func startSignalHandler(t *testing.T, s *server.Server, filename string) (chan<- os.Signal, <-chan struct{}) {
	signals := make(chan os.Signal)
	exits := make(chan struct{}, 10)
	done := make(chan struct{})
	go func() {
		handleSignals(s, filename, signals, func() {
			exits <- struct{}{}
		})
		close(done)
	}()
	t.Cleanup(func() {
		close(signals)
		<-done
	})
	return signals, exits
}

// reload sends SIGHUP twice and so returns once the first reload is done: the unbuffered channel
// only takes the second one when the handler is back at receiving
func reload(signals chan<- os.Signal) {
	signals <- syscall.SIGHUP
	signals <- syscall.SIGHUP
}

// rewriteListenAdmin replaces the listen-admin of a config file from writeTestConfig
func rewriteListenAdmin(t *testing.T, filename, addr string) string {
	b, err := os.ReadFile(filename)
	require.NoError(t, err)
	conf := strings.Replace(string(b), "listen-admin: '127.0.0.1:0'", fmt.Sprintf("listen-admin: %q", addr), 1)
	require.NoError(t, os.WriteFile(filename, []byte(conf), 0600))
	return filename
}

// startStreamingServer serves a config with extra settings, in front of an upstream that keeps
// every request open until the client goes away or the returned func is called. Serve's result
// goes to the returned channel.
func startStreamingServer(t *testing.T, extra string) (*server.Server, string, <-chan error, func()) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	streaming := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
		case <-release:
		}
	})
	closeStreams := func() {
		releaseOnce.Do(func() {
			close(release)
		})
	}
	filename := writeTestConfig(t, extra, streaming, "app1")
	t.Cleanup(closeStreams) // Runs before the upstream's Close, which waits for its handlers
	s, err := server.New(filename)
	require.NoError(t, err)
	served := make(chan error, 1)
	go func() {
		served <- s.Serve(context.Background())
	}()
	waitReady(t, s.AdminAddr())
	return s, filename, served, closeStreams
}

// openStream opens a subscription through the load balancer and waits until it counts
func openStream(t *testing.T, s *server.Server) *http.Response {
	resp, err := http.Get("http://" + s.HTTPAddr() + "/mytopic/json")
	require.NoError(t, err)
	t.Cleanup(func() {
		resp.Body.Close()
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Eventually(t, func() bool {
		return fetchStatus(t, s).Streams == 1
	}, 5*time.Second, 10*time.Millisecond)
	return resp
}

// waitDraining waits until the load balancer drains and returns its drain deadline
func waitDraining(t *testing.T, s *server.Server) time.Time {
	var deadline time.Time
	require.Eventually(t, func() bool {
		status := fetchStatus(t, s)
		if !status.Draining || status.DrainDeadline == nil {
			return false
		}
		require.False(t, status.Ready)
		deadline = *status.DrainDeadline
		return true
	}, 5*time.Second, 10*time.Millisecond)
	return deadline
}

// waitServed waits until Serve returned without an error
func waitServed(t *testing.T, served <-chan error) {
	select {
	case err := <-served:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return after the stop signal")
	}
}

func fetchStatus(t *testing.T, s *server.Server) *balancer.Status {
	out, err := run(t, s, "status", "--json")
	require.NoError(t, err)
	status := &balancer.Status{}
	require.NoError(t, json.Unmarshal([]byte(out), status))
	return status
}

func upstreamNames(status *balancer.Status) []string {
	var names []string
	for _, u := range status.Upstreams {
		names = append(names, u.Name)
	}
	return names
}

// freeAddr returns a 127.0.0.1 address that was free a moment ago
func freeAddr(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

func TestHandleSignals_Reload(t *testing.T) {
	s, filename := newTestServer(t, "app1", "app2")
	signals, exits := startSignalHandler(t, s, filename)
	good, err := os.ReadFile(filename)
	require.NoError(t, err)

	// SIGHUP applies a good config
	added := string(good) + "        - name: app3\n          addresses: ['127.0.0.1:1']\n"
	require.NoError(t, os.WriteFile(filename, []byte(added), 0600))
	reload(signals)
	require.Equal(t, []string{"app1", "app2", "app3"}, upstreamNames(fetchStatus(t, s)))

	// A broken config is rejected, the current one stays
	require.NoError(t, os.WriteFile(filename, []byte("listen-http: ["), 0600))
	reload(signals)
	require.Equal(t, []string{"app1", "app2", "app3"}, upstreamNames(fetchStatus(t, s)))
	require.NoError(t, os.WriteFile(filename, []byte("drain-duration: 1m"), 0600)) // No upstreams
	reload(signals)
	status := fetchStatus(t, s)
	require.Equal(t, []string{"app1", "app2", "app3"}, upstreamNames(status))
	require.False(t, status.Draining)

	// Still serving, and reloads keep working
	require.NoError(t, os.WriteFile(filename, good, 0600))
	reload(signals)
	status = fetchStatus(t, s)
	require.False(t, status.Draining)
	require.True(t, status.Ready)
	require.Len(t, exits, 0)
}

func TestHandleSignals_Shutdown(t *testing.T) {
	for _, sig := range []os.Signal{syscall.SIGTERM, syscall.SIGINT} {
		t.Run(sig.String(), func(t *testing.T) {
			s, filename, served, _ := startStreamingServer(t, "shutdown-drain-duration: 2s\ndrain-duration: 1h\n")
			signals, exits := startSignalHandler(t, s, filename)
			resp := openStream(t, s) // Keeps the drain going until its deadline

			// The stop signal drains over shutdown-drain-duration, not drain-duration or the default
			start := time.Now()
			signals <- sig
			deadline := waitDraining(t, s)
			require.WithinDuration(t, start.Add(2*time.Second), deadline, time.Second)

			// The drain closes the stream, then Serve returns
			io.Copy(io.Discard, resp.Body)
			waitServed(t, served)
			require.Less(t, time.Since(start), 8*time.Second)
			require.Len(t, exits, 0)
		})
	}
}

func TestHandleSignals_ShutdownDrainDurationReadAtStop(t *testing.T) {
	// The duration comes from the config file as it is when the signal arrives
	s, filename, served, closeStreams := startStreamingServer(t, "shutdown-drain-duration: 1h\n")
	signals, _ := startSignalHandler(t, s, filename)
	openStream(t, s)
	b, err := os.ReadFile(filename)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filename, []byte(strings.Replace(string(b), "shutdown-drain-duration: 1h", "shutdown-drain-duration: 3m", 1)), 0600))
	start := time.Now()
	signals <- syscall.SIGTERM
	require.WithinDuration(t, start.Add(3*time.Minute), waitDraining(t, s), 5*time.Second)
	closeStreams() // The last stream is gone, so the shutdown ends early
	waitServed(t, served)
}

func TestHandleSignals_ShutdownDefaultDuration(t *testing.T) {
	// If the config file became unreadable, the default shutdown-drain-duration applies
	s, filename, served, closeStreams := startStreamingServer(t, "shutdown-drain-duration: 1h\n")
	signals, _ := startSignalHandler(t, s, filename)
	openStream(t, s)
	require.NoError(t, os.Remove(filename))
	start := time.Now()
	signals <- syscall.SIGTERM
	require.WithinDuration(t, start.Add(config.DefaultShutdownDrainDuration), waitDraining(t, s), 5*time.Second)
	closeStreams()
	waitServed(t, served)
}

func TestHandleSignals_SecondStopExits(t *testing.T) {
	s, filename, served, closeStreams := startStreamingServer(t, "shutdown-drain-duration: 1h\n")
	signals, exits := startSignalHandler(t, s, filename)
	openStream(t, s)
	signals <- syscall.SIGTERM
	waitDraining(t, s)
	require.Len(t, exits, 0)
	signals <- syscall.SIGINT
	require.Eventually(t, func() bool {
		return len(exits) == 1
	}, 5*time.Second, 10*time.Millisecond)

	// A reload still works while stopping
	reload(signals)
	signals <- syscall.SIGTERM
	require.Eventually(t, func() bool {
		return len(exits) == 2
	}, 5*time.Second, 10*time.Millisecond)
	closeStreams()
	waitServed(t, served)
}

func TestServe_CLI(t *testing.T) {
	// Capture the signal channel instead of subscribing the test process to real signals
	var notifiedSignals []os.Signal
	notified := make(chan chan<- os.Signal, 1)
	exited := make(chan int, 1)
	origNotifySignals, origOSExit := notifySignals, osExit
	notifySignals = func(c chan<- os.Signal, sig ...os.Signal) {
		notifiedSignals = sig
		notified <- c
	}
	osExit = func(code int) {
		exited <- code
	}
	t.Cleanup(func() {
		notifySignals, osExit = origNotifySignals, origOSExit
	})
	adminAddr := freeAddr(t)
	filename := writeTestConfig(t, "shutdown-drain-duration: 10ms\n", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}), "app1", "app2")
	filename = rewriteListenAdmin(t, filename, adminAddr)

	// Serve until the stop signal
	served := make(chan error, 1)
	go func() {
		_, err := runApp("--config", filename, "serve")
		served <- err
	}()
	var signals chan<- os.Signal
	select {
	case signals = <-notified:
	case err := <-served:
		t.Fatalf("serve returned early: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not subscribe to signals")
	}
	require.Equal(t, []os.Signal{syscall.SIGHUP, syscall.SIGTERM, syscall.SIGINT}, notifiedSignals)
	waitReady(t, adminAddr)

	// The other commands find the admin API through the same config file
	out, err := runApp("--config", filename, "status")
	require.NoError(t, err)
	require.Contains(t, out, "Load balancer: ready, 0 stream(s)")
	require.Regexp(t, `app1\s+healthy`, out)
	require.Regexp(t, `app2\s+healthy`, out)

	// SIGHUP reloads, SIGTERM stops, a second one exits right away
	signals <- syscall.SIGHUP
	signals <- syscall.SIGTERM
	select {
	case err := <-served:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not return after SIGTERM")
	}
	signals <- syscall.SIGINT
	select {
	case code := <-exited:
		require.Equal(t, 1, code)
	case <-time.After(5 * time.Second):
		t.Fatal("second stop signal did not exit")
	}
	close(signals)
	_, err = http.Get("http://" + adminAddr + "/v1/ready")
	require.Error(t, err)
}

func TestServe_Errors(t *testing.T) {
	// Missing config file
	_, err := runApp("--config", filepath.Join(t.TempDir(), "missing.yml"), "serve")
	require.True(t, errors.Is(err, fs.ErrNotExist))

	// Invalid config
	filename := filepath.Join(t.TempDir(), "config.yml")
	require.NoError(t, os.WriteFile(filename, []byte("drain-duration: 1m"), 0600))
	_, err = runApp("--config", filename, "serve")
	require.ErrorContains(t, err, "at least one site")

	// Listen address taken
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	filename = writeTestConfig(t, "", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}), "app1")
	filename = rewriteListenAdmin(t, filename, ln.Addr().String())
	_, err = runApp("--config", filename, "serve")
	require.ErrorContains(t, err, "address already in use")
}

func TestCheckConfig_Output(t *testing.T) {
	filename := writeTestConfig(t, "", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}), "app1", "app2", "app3")
	out, err := runApp("check-config", "--config", filename)
	require.NoError(t, err)
	require.Equal(t, "Config OK, 1 site(s), 3 upstream(s)\n", out)

	// From the env var
	t.Setenv("NTLB_CONFIG", filename)
	out, err = runApp("check-config")
	require.NoError(t, err)
	require.Equal(t, "Config OK, 1 site(s), 3 upstream(s)\n", out)
}

func TestCheckConfig_Errors(t *testing.T) {
	dir := t.TempDir()
	_, err := runApp("check-config", "-c", filepath.Join(dir, "missing.yml"))
	require.True(t, errors.Is(err, fs.ErrNotExist))
	filename := filepath.Join(dir, "config.yml")
	require.NoError(t, os.WriteFile(filename, []byte("listen-http: ["), 0600))
	out, err := runApp("check-config", "-c", filename)
	require.ErrorContains(t, err, "invalid config")
	require.Empty(t, out)
	require.NoError(t, os.WriteFile(filename, []byte("sites: [{name: s, listen-http: ':80', upstreams: {default: {servers: [{name: a, addresses: ['nope']}]}}}]"), 0600))
	_, err = runApp("check-config", "-c", filename)
	require.ErrorContains(t, err, `invalid address "nope"`)
}
