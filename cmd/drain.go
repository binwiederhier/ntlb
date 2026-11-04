package cmd

import (
	"context"
	"errors"
	"fmt"

	"github.com/urfave/cli/v3"
)

var cmdReload = &cli.Command{
	Name:   "reload",
	Usage:  "Re-read the config file (upstreams, health checks, timeouts, certificate)",
	Action: execReload,
}

var cmdDrain = &cli.Command{
	Name:  "drain",
	Usage: "Drain the load balancer: turn unready and close all streams, oldest first",
	Description: `Makes /v1/ready fail right away, then closes the open streams a few at a time, so that
the last one closes after --duration. New requests are still served until then, and turned away
with 503 after. "ntfy-lb undrain" ends it.`,
	Flags:  []cli.Flag{durationFlag},
	Action: execDrain,
}

var cmdUndrain = &cli.Command{
	Name:   "undrain",
	Usage:  "End a drain of the load balancer",
	Action: execUndrain,
}

var cmdUpstream = &cli.Command{
	Name:  "upstream",
	Usage: "Drain or undrain an upstream",
	Commands: []*cli.Command{
		{
			Name:      "drain",
			Usage:     "Take an upstream out of rotation and close its streams, oldest first",
			ArgsUsage: "NAME",
			Description: `Sends no new clients to the upstream, and closes its open streams a few at a time, so that
the last one closes after --duration. The clients reconnect and land on the next upstream in their
hash order. Lasts until "ntfy-lb upstream undrain", also across reloads.`,
			Flags:  []cli.Flag{durationFlag},
			Action: execUpstreamDrain,
		},
		{
			Name:      "undrain",
			Usage:     "Put a drained upstream back into rotation",
			ArgsUsage: "NAME",
			Action:    execUpstreamUndrain,
		},
	},
}

func execReload(ctx context.Context, cmd *cli.Command) error {
	if err := newClient(cmd).Reload(ctx); err != nil {
		return err
	}
	fmt.Fprintln(cmd.Root().Writer, "Config reloaded")
	return nil
}

func execDrain(ctx context.Context, cmd *cli.Command) error {
	if err := newClient(cmd).Drain(ctx, cmd.String(flagDuration)); err != nil {
		return err
	}
	fmt.Fprintln(cmd.Root().Writer, "Draining the load balancer")
	return nil
}

func execUndrain(ctx context.Context, cmd *cli.Command) error {
	if err := newClient(cmd).Undrain(ctx); err != nil {
		return err
	}
	fmt.Fprintln(cmd.Root().Writer, "Load balancer undrained")
	return nil
}

func execUpstreamDrain(ctx context.Context, cmd *cli.Command) error {
	name, err := upstreamArg(cmd)
	if err != nil {
		return err
	}
	if err := newClient(cmd).DrainUpstream(ctx, name, cmd.String(flagDuration)); err != nil {
		return err
	}
	fmt.Fprintf(cmd.Root().Writer, "Draining upstream %s\n", name)
	return nil
}

func execUpstreamUndrain(ctx context.Context, cmd *cli.Command) error {
	name, err := upstreamArg(cmd)
	if err != nil {
		return err
	}
	if err := newClient(cmd).UndrainUpstream(ctx, name); err != nil {
		return err
	}
	fmt.Fprintf(cmd.Root().Writer, "Upstream %s undrained\n", name)
	return nil
}

func upstreamArg(cmd *cli.Command) (string, error) {
	if cmd.NArg() != 1 {
		return "", errors.New("expected exactly one upstream name")
	}
	return cmd.Args().First(), nil
}
