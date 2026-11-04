// ntfy-lb is a small load balancer for ntfy: sticky (consistent-hash) routing by client IP that
// agrees across load balancer instances, active health checks, long-lived streams, and paced
// draining of upstreams and of the load balancer itself. See README.md.
package main

import (
	"context"
	"fmt"
	"os"

	"heckel.io/ntfy-lb/cmd"
)

func main() {
	if err := cmd.New().Run(context.Background(), os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}
