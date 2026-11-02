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
	"golang.org/x/sync/errgroup"
	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	"sigs.k8s.io/dra-driver-nvidia-gpu/tests/scale/fakeslices"
)

const (
	// fakeNodeLabelKey marks the fake nodes scalebench manages.
	fakeNodeLabelKey = "scale.nvidia.com/node"
	// fakeClusterUUID prefixes clique IDs, as on GB200 NVL72 systems.
	fakeClusterUUID = "5ca1ab1e-0000-4000-8000-00000000c11c"
	// createWorkers bounds concurrent object creation.
	createWorkers = 64
)

func nodesCommand() *cli.Command {
	return &cli.Command{
		Name:  "nodes",
		Usage: "Create or delete fake GB200 nodes",
		Subcommands: []*cli.Command{
			{
				Name:  "up",
				Usage: "Create cliques of fake nodes, idempotently",
				Flags: []cli.Flag{
					&cli.IntFlag{Name: "cliques", Value: 8, Usage: "number of NVLink cliques"},
					&cli.IntFlag{Name: "clique-size", Value: 18, Usage: "nodes per clique, 18 for NVL72"},
					&cli.IntFlag{Name: "first-clique", Value: 0, Usage: "index of the first clique, to add cliques later"},
				},
				Action: func(c *cli.Context) error {
					ctx, cancel := signalContext(c.Context)
					defer cancel()
					cs, err := newClients(c)
					if err != nil {
						return err
					}
					if err := ensureGPUDeviceClass(ctx, cs); err != nil {
						return err
					}
					return createNodes(ctx, cs, c.Int("first-clique"), c.Int("cliques"), c.Int("clique-size"))
				},
			},
			{
				Name:  "down",
				Usage: "Delete all fake nodes and their ResourceSlices",
				Action: func(c *cli.Context) error {
					ctx, cancel := signalContext(c.Context)
					defer cancel()
					cs, err := newClients(c)
					if err != nil {
						return err
					}
					return deleteNodes(ctx, cs)
				},
			},
		},
	}
}

func cliqueID(clique int) string {
	return fmt.Sprintf("%s.%d", fakeClusterUUID, clique)
}

func nodeName(clique, index int) string {
	return fmt.Sprintf("kwok-c%04d-n%02d", clique, index)
}

func fakeNode(name, clique string) *corev1.Node {
	capacity := corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("144"),
		corev1.ResourceMemory: resource.MustParse("1700Gi"),
		corev1.ResourcePods:   resource.MustParse("110"),
	}
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				corev1.LabelHostname:     name,
				nodeTypeLabelKey:         nodeTypeLabelValue,
				fakeNodeLabelKey:         "true",
				gpuCliqueLabelKey:        clique,
				"nvidia.com/gpu.product": "NVIDIA-GB200",
			},
			Annotations: map[string]string{
				kwokNodeAnnotationKey: fakeNodeValue,
			},
		},
		Spec: corev1.NodeSpec{
			Taints: []corev1.Taint{{Key: kwokNodeTaintKey, Value: fakeNodeValue, Effect: corev1.TaintEffectNoSchedule}},
		},
		Status: corev1.NodeStatus{
			Capacity:    capacity,
			Allocatable: capacity,
		},
	}
}

func createNodes(ctx context.Context, cs *clients, first, cliques, size int) error {
	start := time.Now()
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(createWorkers)
	for clique := first; clique < first+cliques; clique++ {
		for i := range size {
			node := fakeNode(nodeName(clique, i), cliqueID(clique))
			g.Go(func() error {
				_, err := cs.core.CoreV1().Nodes().Create(gctx, node, metav1.CreateOptions{})
				if err != nil && !apierrors.IsAlreadyExists(err) {
					return fmt.Errorf("error creating Node %s: %w", node.Name, err)
				}
				return nil
			})
		}
	}
	if err := g.Wait(); err != nil {
		return err
	}
	klog.Infof("Created %d fake nodes in %d cliques in %v", cliques*size, cliques, time.Since(start).Round(time.Millisecond))
	return waitNodesReady(ctx, cs, cliques*size+first*size)
}

// waitNodesReady waits for KWOK to mark at least n fake nodes Ready.
func waitNodesReady(ctx context.Context, cs *clients, n int) error {
	return poll(ctx, 2*time.Second, 10*time.Minute, func() (bool, error) {
		nodes, err := cs.core.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: fakeNodeLabelKey})
		if err != nil {
			return false, err
		}
		ready := 0
		for i := range nodes.Items {
			if nodeReady(&nodes.Items[i]) {
				ready++
			}
		}
		klog.V(1).Infof("%d/%d fake nodes Ready", ready, n)
		if ready < n {
			return false, nil
		}
		klog.Infof("%d fake nodes Ready", ready)
		return true, nil
	})
}

func nodeReady(node *corev1.Node) bool {
	for _, c := range node.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func deleteNodes(ctx context.Context, cs *clients) error {
	nodes, err := cs.core.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: fakeNodeLabelKey})
	if err != nil {
		return fmt.Errorf("error listing fake nodes: %w", err)
	}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(createWorkers)
	for i := range nodes.Items {
		name := nodes.Items[i].Name
		g.Go(func() error {
			err := cs.core.CoreV1().Nodes().Delete(gctx, name, metav1.DeleteOptions{})
			if err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("error deleting Node %s: %w", name, err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	err = cs.core.ResourceV1().ResourceSlices().DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s", fakeslices.ManagedByLabelKey, fakeslices.ManagedByLabelValue),
	})
	if err != nil {
		return fmt.Errorf("error deleting fake ResourceSlices: %w", err)
	}
	klog.Infof("Deleted %d fake nodes and their ResourceSlices", len(nodes.Items))
	return nil
}

func ensureGPUDeviceClass(ctx context.Context, cs *clients) error {
	class := &resourceapi.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{Name: fakeslices.GPUDeviceClassName},
		Spec: resourceapi.DeviceClassSpec{
			Selectors: []resourceapi.DeviceSelector{{
				CEL: &resourceapi.CELDeviceSelector{
					Expression: fmt.Sprintf("device.driver == %q && device.attributes[%q].type == 'gpu'", fakeslices.GPUDriverName, fakeslices.GPUDriverName),
				},
			}},
		},
	}
	_, err := cs.core.ResourceV1().DeviceClasses().Create(ctx, class, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("error creating DeviceClass %s: %w", class.Name, err)
	}
	return nil
}

// poll calls check every interval until it returns true, it fails, or timeout
// passes.
func poll(ctx context.Context, interval, timeout time.Duration, check func() (bool, error)) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	start := time.Now()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		done, err := check()
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		select {
		case <-ctx.Done():
			// The caller's deadline may be the earlier one.
			return fmt.Errorf("timed out after %v: %w", time.Since(start).Round(time.Second), ctx.Err())
		case <-ticker.C:
		}
	}
}
