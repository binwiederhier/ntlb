// Package cmd implements the ntlb command line
package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"

	"github.com/urfave/cli/v3"
	"heckel.io/ntlb/client"
	"heckel.io/ntlb/config"
)

const (
	flagConfig   = "config"
	flagAdminURL = "admin-url"
	flagDuration = "duration"
	flagJSON     = "json"
	flagLogLevel = "log-level"
)

// New returns the ntlb CLI app. Its flags work anywhere on the command line, also after a
// subcommand and its arguments. Commands and flags are created fresh for every app, because
// urfave/cli keeps parse state in them, e.g. a flag set once ignores its env var from then on.
func New() *cli.Command {
	return &cli.Command{
		Name:                   "ntlb",
		Usage:                  "HTTP load balancer, built for ntfy",
		UseShortOptionHandling: true,
		Reader:                 os.Stdin,
		Writer:                 os.Stdout,
		ErrWriter:              os.Stderr,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    flagConfig,
				Aliases: []string{"c"},
				Sources: cli.EnvVars("NTLB_CONFIG"),
				Value:   config.DefaultFile,
				Usage:   "config file",
			},
			&cli.StringFlag{
				Name:    flagAdminURL,
				Aliases: []string{"a"},
				Sources: cli.EnvVars("NTLB_ADMIN_URL"),
				Usage:   "admin API URL (default: listen-admin from the config file, or " + config.DefaultListenAdmin + ")",
			},
			&cli.StringFlag{
				Name:    flagLogLevel,
				Sources: cli.EnvVars("NTLB_LOG_LEVEL"),
				Value:   "info",
				Usage:   "log level: debug, info, warn or error",
			},
		},
		Before: func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
			var level slog.Level
			if err := level.UnmarshalText([]byte(cmd.String(flagLogLevel))); err != nil {
				return ctx, fmt.Errorf("invalid log level %q", cmd.String(flagLogLevel))
			}
			slog.SetLogLoggerLevel(level)
			return ctx, nil
		},
		Commands: []*cli.Command{
			newServeCommand(),
			newCheckConfigCommand(),
			newStatusCommand(),
			newReloadCommand(),
			newUpstreamCommand(),
		},
	}
}

// newClient returns an admin API client: --admin-url, else the config file's listen-admin
func newClient(cmd *cli.Command) *client.Client {
	if adminURL := cmd.String(flagAdminURL); adminURL != "" {
		return client.New(adminURL)
	}
	listenAdmin := config.DefaultListenAdmin
	if conf, err := config.Load(cmd.String(flagConfig)); err == nil {
		listenAdmin = conf.ListenAdmin
	}
	return client.New("http://" + dialAddr(listenAdmin))
}

// newDurationFlag returns the --duration flag of the upstream drain command
func newDurationFlag() *cli.StringFlag {
	return &cli.StringFlag{
		Name:    flagDuration,
		Aliases: []string{"d"},
		Usage:   "close all streams over this long, e.g. 10m (default: drain-duration from the config)",
	}
}

// dialAddr turns a listen address into one to connect to: a wildcard host (":2590",
// "0.0.0.0:2590", "[::]:2590") becomes 127.0.0.1, which a Go wildcard listener also accepts on
func dialAddr(listenAddr string) string {
	host, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return listenAddr
	}
	if ip := net.ParseIP(host); host == "" || (ip != nil && ip.IsUnspecified()) {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port)
}
