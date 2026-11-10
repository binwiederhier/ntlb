package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/urfave/cli/v3"
	"heckel.io/ntlb/config"
	"heckel.io/ntlb/server"
)

var (
	// notifySignals and osExit are replaced in tests, so that no real signal reaches the test process
	notifySignals = signal.Notify
	osExit        = os.Exit
)

func newServeCommand() *cli.Command {
	return &cli.Command{
		Name:  "serve",
		Usage: "Run the load balancer",
		Description: `Runs the load balancer with the config file (--config).

Signals:
  SIGHUP           reload the config file (same as "ntlb reload")
  SIGTERM, SIGINT  drain over shutdown-drain-duration, then exit; a second one exits right away`,
		Action: execServe,
	}
}

func newCheckConfigCommand() *cli.Command {
	return &cli.Command{
		Name:   "check-config",
		Usage:  "Validate a config file",
		Action: execCheckConfig,
	}
}

func execServe(ctx context.Context, cmd *cli.Command) error {
	s, err := server.New(cmd.String(flagConfig))
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	signals := make(chan os.Signal, 2)
	notifySignals(signals, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGINT)
	go handleSignals(s, cmd.String(flagConfig), signals, func() {
		osExit(1)
	})
	return s.Serve(ctx)
}

func execCheckConfig(ctx context.Context, cmd *cli.Command) error {
	conf, err := config.Load(cmd.String(flagConfig))
	if err != nil {
		return err
	}
	fmt.Fprintf(cmd.Root().Writer, "Config OK, %d site(s), %d upstream(s)\n", len(conf.Sites), conf.UpstreamCount())
	return nil
}

// handleSignals reloads on SIGHUP and shuts down on SIGTERM or SIGINT; a second stop signal calls
// exit. It returns when signals is closed.
func handleSignals(s *server.Server, configFile string, signals <-chan os.Signal, exit func()) {
	stopping := false
	for sig := range signals {
		switch {
		case sig == syscall.SIGHUP:
			if _, err := s.Reload(); err != nil {
				slog.Error("Reload failed, keeping the current config", "error", err)
			}
		case stopping:
			slog.Warn("Second stop signal, exiting right away")
			exit()
		default:
			stopping = true
			d := config.DefaultShutdownDrainDuration
			if conf, err := config.Load(configFile); err == nil {
				d = conf.ShutdownDrainDuration
			}
			go s.Shutdown(d)
		}
	}
}
