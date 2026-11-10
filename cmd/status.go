package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/urfave/cli/v3"
	"heckel.io/ntlb/balancer"
)

func newStatusCommand() *cli.Command {
	return &cli.Command{
		Name:  "status",
		Usage: "Show the load balancer's and the upstreams' status",
		Flags: []cli.Flag{
			&cli.BoolFlag{Name: flagJSON, Aliases: []string{"j"}, Usage: "print the raw JSON"},
		},
		Action: execStatus,
	}
}

func execStatus(ctx context.Context, cmd *cli.Command) error {
	status, err := newClient(cmd).Status(ctx)
	if err != nil {
		return err
	}
	if cmd.Bool(flagJSON) {
		enc := json.NewEncoder(cmd.Root().Writer)
		enc.SetIndent("", "  ")
		return enc.Encode(status)
	}
	fmt.Fprintf(cmd.Root().Writer, "Load balancer: %s, %d stream(s)\n\n", lbState(status), status.Streams)
	w := tabwriter.NewWriter(cmd.Root().Writer, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "SITE\tUPSTREAM\tSTATE\tSTREAMS\tREQUESTS\tFAILURES\tADDRESSES\tNOTE")
	for _, u := range status.Upstreams {
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%d\t%d\t%s\t%s\n", u.Site, u.Name, u.State, u.Streams, u.Requests, u.Failures, strings.Join(u.Addresses, ","), upstreamNote(u))
	}
	return w.Flush()
}

func lbState(s *balancer.Status) string {
	switch {
	case s.Drained:
		return "drained (turning requests away)"
	case s.Draining:
		return "draining, done " + until(*s.DrainDeadline)
	case s.Ready:
		return "ready"
	default:
		return "unhealthy (no healthy upstream, sending to all)"
	}
}

func upstreamNote(u *balancer.UpstreamStatus) string {
	var notes []string
	if u.Removed {
		notes = append(notes, "removed from config")
	}
	if u.State == balancer.StateDraining && u.DrainDeadline != nil {
		notes = append(notes, "drained "+until(*u.DrainDeadline))
	}
	if (u.State == balancer.StateDraining || u.State == balancer.StateDrained) && !u.Healthy {
		notes = append(notes, "unhealthy")
	}
	if u.LastError != "" {
		notes = append(notes, "last check: "+u.LastError)
	}
	return strings.Join(notes, "; ")
}

func until(t time.Time) string {
	return fmt.Sprintf("in %s (%s)", time.Until(t).Round(time.Second), t.Local().Format("15:04:05"))
}
