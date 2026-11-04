// Package cmd implements the ntfy-lb command line
package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/urfave/cli/v3"
	"heckel.io/ntfy-lb/client"
	"heckel.io/ntfy-lb/config"
)

const (
	flagConfig   = "config"
	flagAdminURL = "admin-url"
	flagDuration = "duration"
	flagJSON     = "json"
	flagLogLevel = "log-level"
)

var (
	configFlag = &cli.StringFlag{
		Name:    flagConfig,
		Aliases: []string{"c"},
		Sources: cli.EnvVars("NTFY_LB_CONFIG"),
		Value:   config.DefaultFile,
		Usage:   "config file",
	}
	durationFlag = &cli.StringFlag{
		Name:    flagDuration,
		Aliases: []string{"d"},
		Usage:   "close all streams over this long, e.g. 10m (default: drain-duration from the config)",
	}
)

// New returns the ntfy-lb CLI app. Its flags work anywhere on the command line, also after a
// subcommand and its arguments.
func New() *cli.Command {
	return &cli.Command{
		Name:                   "ntfy-lb",
		Usage:                  "load balancer for ntfy",
		UseShortOptionHandling: true,
		Reader:                 os.Stdin,
		Writer:                 os.Stdout,
		ErrWriter:              os.Stderr,
		Flags: []cli.Flag{
			configFlag,
			&cli.StringFlag{
				Name:    flagAdminURL,
				Aliases: []string{"a"},
				Sources: cli.EnvVars("NTFY_LB_ADMIN_URL"),
				Usage:   "admin API URL (default: listen-admin from the config file, or " + config.DefaultListenAdmin + ")",
			},
			&cli.StringFlag{
				Name:    flagLogLevel,
				Sources: cli.EnvVars("NTFY_LB_LOG_LEVEL"),
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
			cmdServe,
			cmdCheckConfig,
			cmdStatus,
			cmdReload,
			cmdDrain,
			cmdUndrain,
			cmdUpstream,
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
	if strings.HasPrefix(listenAdmin, ":") {
		listenAdmin = "127.0.0.1" + listenAdmin
	}
	return client.New("http://" + listenAdmin)
}
