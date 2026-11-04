package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/urfave/cli/v3"
	"heckel.io/ntfy-lb/config"
	"heckel.io/ntfy-lb/server"
)

var cmdServe = &cli.Command{
	Name:  "serve",
	Usage: "Run the load balancer",
	Description: `Runs the load balancer with the config file (--config).

Signals:
  SIGHUP           reload the config file (same as "ntfy-lb reload")
  SIGTERM, SIGINT  drain over shutdown-drain-duration, then exit; a second one exits right away`,
	Action: execServe,
}

var cmdCheckConfig = &cli.Command{
	Name:   "check-config",
	Usage:  "Validate a config file",
	Action: execCheckConfig,
}

func execServe(ctx context.Context, cmd *cli.Command) error {
	s, err := server.New(cmd.String(flagConfig))
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go handleSignals(s, cmd.String(flagConfig))
	return s.Serve(ctx)
}

func execCheckConfig(ctx context.Context, cmd *cli.Command) error {
	conf, err := config.Load(cmd.String(flagConfig))
	if err != nil {
		return err
	}
	fmt.Fprintf(cmd.Root().Writer, "Config OK, %d upstream(s)\n", len(conf.Upstreams))
	return nil
}

func handleSignals(s *server.Server, configFile string) {
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGINT)
	stopping := false
	for sig := range signals {
		switch {
		case sig == syscall.SIGHUP:
			if err := s.Reload(); err != nil {
				slog.Error("Reload failed, keeping the current config", "error", err)
			}
		case stopping:
			slog.Warn("Second stop signal, exiting right away")
			os.Exit(1)
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
