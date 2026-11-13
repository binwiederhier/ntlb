package cmd

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/urfave/cli/v3"
)

func newReloadCommand() *cli.Command {
	return &cli.Command{
		Name:   "reload",
		Usage:  "Re-read the config file and its site files (sites, upstreams, rules, certificates, ...)",
		Action: execReload,
	}
}

func newUpstreamCommand() *cli.Command {
	return &cli.Command{
		Name:  "upstream",
		Usage: "Drain or undrain an upstream",
		Commands: []*cli.Command{
			{
				Name:      "drain",
				Usage:     "Take an upstream out of rotation and close its streams, oldest first",
				ArgsUsage: "[SITE/]NAME",
				Description: `Sends no new clients to the upstream, and closes its open streams a few at a time, so that
the last one closes after --duration. The clients reconnect and land on the next upstream in their
hash order. Lasts until "ntlb upstream undrain", also across reloads. NAME is the upstream's name,
or SITE/NAME if several sites have an upstream of that name.`,
				Flags:  []cli.Flag{newDurationFlag()},
				Action: execUpstreamDrain,
			},
			{
				Name:      "undrain",
				Usage:     "Put a drained upstream back into rotation",
				ArgsUsage: "[SITE/]NAME",
				Action:    execUpstreamUndrain,
			},
		},
	}
}

func execReload(ctx context.Context, cmd *cli.Command) error {
	restart, err := newClient(cmd).Reload(ctx)
	if err != nil {
		return err
	} else if len(restart) > 0 {
		fmt.Fprintf(cmd.Root().Writer, "Config reloaded, but these settings need a restart: %s\n", strings.Join(restart, ", "))
		return nil
	}
	fmt.Fprintln(cmd.Root().Writer, "Config reloaded")
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
