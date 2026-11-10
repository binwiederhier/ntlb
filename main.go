// ntlb is a small HTTP load balancer built for ntfy: balancing per site (round robin, least
// connections, random two, or consistent hashing of the client IP that agrees across load
// balancer instances), active health checks, long-lived streams, and paced draining of upstreams
// and of the load balancer itself. See README.md.
package main

import (
	"context"
	"fmt"
	"os"

	"heckel.io/ntlb/cmd"
)

func main() {
	if err := cmd.New().Run(context.Background(), os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}
