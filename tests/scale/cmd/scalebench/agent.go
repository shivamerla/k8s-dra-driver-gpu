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

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/urfave/cli/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/klog/v2"

	nvinformers "sigs.k8s.io/dra-driver-nvidia-gpu/pkg/nvidia.com/informers/externalversions"
	"sigs.k8s.io/dra-driver-nvidia-gpu/tests/scale/fakeslices"
)

// agent stands in for what runs on each fake node: the kubelet plugins'
// ResourceSlices and the IMEX daemons' API writes.
type agent struct {
	mode      driverMode
	publisher *slicePublisher
	daemons   *daemonSimulator
}

type agentOptions struct {
	gpusPerNode      int
	simulateDaemons  bool
	publisherWorkers int
	daemonWorkers    int
	daemonReadyDelay time.Duration
}

func agentFlags() []cli.Flag {
	return []cli.Flag{
		&cli.IntFlag{Name: "gpus-per-node", Value: 4, Usage: "fake GPUs each node publishes, 0 for none"},
		&cli.BoolFlag{Name: "simulate-daemons", Value: true, Usage: "register IMEX daemon pods on fake nodes in their ComputeDomainCliques"},
		&cli.DurationFlag{Name: "daemon-ready-delay", Usage: "time a simulated IMEX daemon takes from its pod running to Ready, to model the IMEX startup measured on real nodes"},
	}
}

func agentOptionsFrom(c *cli.Context) agentOptions {
	return agentOptions{
		gpusPerNode:      c.Int("gpus-per-node"),
		simulateDaemons:  c.Bool("simulate-daemons"),
		publisherWorkers: 32,
		daemonWorkers:    32,
		daemonReadyDelay: c.Duration("daemon-ready-delay"),
	}
}

func agentCommand() *cli.Command {
	return &cli.Command{
		Name:  "agent",
		Usage: "Publish fake ResourceSlices and simulate IMEX daemons until interrupted",
		Flags: agentFlags(),
		Action: func(c *cli.Context) error {
			ctx, cancel := signalContext(c.Context)
			defer cancel()
			cs, err := newClients(c)
			if err != nil {
				return err
			}
			a, err := startAgent(ctx, cs, agentOptionsFrom(c))
			if err != nil {
				return err
			}
			ticker := time.NewTicker(30 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return nil
				case <-ticker.C:
					a.logStats()
				}
			}
		},
	}
}

func fakeNodeSelector() labels.Selector {
	return labels.SelectorFromSet(labels.Set{fakeNodeLabelKey: "true"})
}

// startAgent starts publishing slices and simulating daemons in the
// background, and returns once all fake nodes have their slices.
func startAgent(ctx context.Context, cs *clients, opts agentOptions) (*agent, error) {
	mode, err := detectDriverMode(ctx, cs)
	if err != nil {
		return nil, err
	}
	klog.Infof("Driver mode %s (daemonLifecycle=%s, reservations=%v, controller QPS %s)", mode.Name(), mode.Lifecycle, mode.Reservations, mode.KubeAPIQPS)

	nodeFactory := informers.NewSharedInformerFactoryWithOptions(cs.core, 0, informers.WithTweakListOptions(func(o *metav1.ListOptions) {
		o.LabelSelector = fakeNodeSelector().String()
	}))
	sliceFactory := informers.NewSharedInformerFactoryWithOptions(cs.core, 0, informers.WithTweakListOptions(func(o *metav1.ListOptions) {
		o.LabelSelector = fmt.Sprintf("%s=%s", fakeslices.ManagedByLabelKey, fakeslices.ManagedByLabelValue)
	}))
	driverFactory := informers.NewSharedInformerFactoryWithOptions(cs.core, 0, informers.WithNamespace(cs.driverNamespace))
	cliqueFactory := nvinformers.NewSharedInformerFactoryWithOptions(cs.nvidia, 0, nvinformers.WithNamespace(cs.driverNamespace))

	a := &agent{mode: mode}
	a.publisher, err = newSlicePublisher(cs, mode, opts.gpusPerNode, nodeFactory, sliceFactory)
	if err != nil {
		return nil, err
	}
	if opts.simulateDaemons {
		a.daemons, err = newDaemonSimulator(cs, nodeFactory, driverFactory, cliqueFactory, opts.daemonReadyDelay)
		if err != nil {
			return nil, err
		}
	}
	for _, f := range []informers.SharedInformerFactory{nodeFactory, sliceFactory, driverFactory} {
		f.Start(ctx.Done())
	}
	cliqueFactory.Start(ctx.Done())
	for _, f := range []informers.SharedInformerFactory{nodeFactory, sliceFactory, driverFactory} {
		for typ, synced := range f.WaitForCacheSync(ctx.Done()) {
			if !synced {
				return nil, fmt.Errorf("informer cache for %v did not sync", typ)
			}
		}
	}
	for typ, synced := range cliqueFactory.WaitForCacheSync(ctx.Done()) {
		if !synced {
			return nil, fmt.Errorf("informer cache for %v did not sync", typ)
		}
	}
	go a.publisher.run(ctx, opts.publisherWorkers)
	if a.daemons != nil {
		go a.daemons.run(ctx, opts.daemonWorkers)
	}
	if err := a.publisher.waitPublished(ctx); err != nil {
		return nil, err
	}
	return a, nil
}

func (a *agent) logStats() {
	klog.Infof("ResourceSlice writes: %d", a.publisher.writes.Load())
	if a.daemons != nil {
		st := a.daemons.stats()
		klog.Infof("IMEX daemon simulation: %d registered, %d reregistered, %d writes, %d conflicts", st.Registered, st.Reregistered, st.Writes, st.Conflicts)
	}
}

func (a *agent) stats() agentStats {
	st := agentStats{SliceWrites: a.publisher.writes.Load()}
	if a.daemons != nil {
		ds := a.daemons.stats()
		st.Daemons = &ds
	}
	return st
}

type agentStats struct {
	SliceWrites int64           `json:"sliceWrites"`
	Daemons     *daemonSimStats `json:"daemons,omitempty"`
}
