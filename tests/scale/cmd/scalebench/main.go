/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Command scalebench drives scale tests of the ComputeDomain driver against
// fake (KWOK) nodes: it creates the nodes, publishes the ResourceSlices their
// kubelet plugins would, simulates the IMEX daemons on them, runs workload
// and static IMEX daemon scenarios, and reports scheduler, controller and API
// server measurements. See tests/scale/README.md.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/urfave/cli/v2"
)

func main() {
	if err := newApp().Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func newApp() *cli.App {
	return &cli.App{
		Name:  "scalebench",
		Usage: "Scale tests of the ComputeDomain driver on KWOK nodes",
		Flags: globalFlags(),
		Commands: []*cli.Command{
			preflightCommand(),
			nodesCommand(),
			agentCommand(),
			admissionCommand(),
			staticCommand(),
			compareCommand(),
			summaryCommand(),
		},
	}
}

// signalContext is canceled on SIGINT or SIGTERM, so that scenarios clean up.
func signalContext(parent context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
}
