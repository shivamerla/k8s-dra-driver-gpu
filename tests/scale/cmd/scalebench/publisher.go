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
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	"sigs.k8s.io/dra-driver-nvidia-gpu/tests/scale/fakeslices"
)

// slicePublisher publishes the ResourceSlices of the kubelet plugins of fake
// nodes, and publishes them again when a node label they reflect changes:
// its clique, and its ComputeDomain owner under per-ComputeDomain IMEX
// daemons. It is the only writer of those slices.
type slicePublisher struct {
	cs          *clients
	mode        driverMode
	gpusPerNode int

	nodes  corelisters.NodeLister
	slices cache.Indexer
	queue  workqueue.TypedRateLimitingInterface[string]

	// writes counts ResourceSlice creates and updates.
	writes atomic.Int64
}

func newSlicePublisher(cs *clients, mode driverMode, gpusPerNode int, nodeFactory, sliceFactory informers.SharedInformerFactory) (*slicePublisher, error) {
	p := &slicePublisher{
		cs:          cs,
		mode:        mode,
		gpusPerNode: gpusPerNode,
		nodes:       nodeFactory.Core().V1().Nodes().Lister(),
		slices:      sliceFactory.Resource().V1().ResourceSlices().Informer().GetIndexer(),
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "scalebench-slices"},
		),
	}
	enqueueNode := func(obj any) {
		if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
			obj = tombstone.Obj
		}
		if node, ok := obj.(*corev1.Node); ok {
			p.queue.Add(node.Name)
		}
	}
	_, err := nodeFactory.Core().V1().Nodes().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    enqueueNode,
		UpdateFunc: func(_, obj any) { enqueueNode(obj) },
		DeleteFunc: enqueueNode,
	})
	if err != nil {
		return nil, fmt.Errorf("error adding Node event handler: %w", err)
	}
	enqueueSlice := func(obj any) {
		if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
			obj = tombstone.Obj
		}
		if slice, ok := obj.(*resourceapi.ResourceSlice); ok && slice.Spec.NodeName != nil {
			p.queue.Add(*slice.Spec.NodeName)
		}
	}
	_, err = sliceFactory.Resource().V1().ResourceSlices().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: enqueueSlice,
	})
	if err != nil {
		return nil, fmt.Errorf("error adding ResourceSlice event handler: %w", err)
	}
	return p, nil
}

func (p *slicePublisher) run(ctx context.Context, workers int) {
	defer p.queue.ShutDown()
	for range workers {
		go func() {
			for p.processNext(ctx) {
			}
		}()
	}
	<-ctx.Done()
}

func (p *slicePublisher) processNext(ctx context.Context) bool {
	name, shutdown := p.queue.Get()
	if shutdown {
		return false
	}
	defer p.queue.Done(name)
	if err := p.sync(ctx, name); err != nil {
		klog.Warningf("Error publishing ResourceSlices of node %s: %v", name, err)
		p.queue.AddRateLimited(name)
		return true
	}
	p.queue.Forget(name)
	return true
}

func (p *slicePublisher) desired(node *corev1.Node) []*resourceapi.ResourceSlice {
	config := p.mode.config()
	cdDevices := fakeslices.ComputeDomainDevices(fakeslices.ComputeDomainOptions{
		Reservations:      p.mode.Reservations,
		SharedIMEXDaemons: config.SharedIMEXDaemons(),
		CliqueID:          node.Labels[gpuCliqueLabelKey],
		Owner:             node.Labels[computeDomainLabelKey],
	})
	desired := []*resourceapi.ResourceSlice{fakeslices.Slice(node.Name, fakeslices.ComputeDomainDriverName, 0, cdDevices)}
	if p.gpusPerNode > 0 {
		desired = append(desired, fakeslices.Slice(node.Name, fakeslices.GPUDriverName, 0, fakeslices.GPUDevices(node.Name, p.gpusPerNode)))
	}
	return desired
}

func (p *slicePublisher) sync(ctx context.Context, name string) error {
	node, err := p.nodes.Get(name)
	if apierrors.IsNotFound(err) || (err == nil && node.Labels[fakeNodeLabelKey] == "") {
		return p.deleteSlices(ctx, name)
	}
	if err != nil {
		return err
	}
	for _, want := range p.desired(node) {
		obj, exists, err := p.slices.GetByKey(want.Name)
		if err != nil {
			return err
		}
		if !exists {
			if _, err := p.cs.core.ResourceV1().ResourceSlices().Create(ctx, want, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
				return fmt.Errorf("error creating ResourceSlice %s: %w", want.Name, err)
			}
			p.writes.Add(1)
			continue
		}
		have, ok := obj.(*resourceapi.ResourceSlice)
		if !ok {
			return fmt.Errorf("unexpected object %T in ResourceSlice informer", obj)
		}
		if equality.Semantic.DeepEqual(have.Spec.Devices, want.Spec.Devices) {
			continue
		}
		// A new pool generation makes the scheduler drop the old devices, as
		// the kubelet plugin's ResourceSlice controller does.
		update := have.DeepCopy()
		update.Spec.Devices = want.Spec.Devices
		update.Spec.Pool.Generation = have.Spec.Pool.Generation + 1
		if _, err := p.cs.core.ResourceV1().ResourceSlices().Update(ctx, update, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("error updating ResourceSlice %s: %w", want.Name, err)
		}
		p.writes.Add(1)
	}
	return nil
}

func (p *slicePublisher) deleteSlices(ctx context.Context, node string) error {
	for _, driver := range []string{fakeslices.ComputeDomainDriverName, fakeslices.GPUDriverName} {
		name := fakeslices.SliceName(node, driver)
		if _, exists, _ := p.slices.GetByKey(name); !exists {
			continue
		}
		err := p.cs.core.ResourceV1().ResourceSlices().Delete(ctx, name, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("error deleting ResourceSlice %s: %w", name, err)
		}
	}
	return nil
}

// waitPublished waits until every Ready fake node has its slices.
func (p *slicePublisher) waitPublished(ctx context.Context) error {
	return poll(ctx, time.Second, 10*time.Minute, func() (bool, error) {
		nodes, err := p.nodes.List(fakeNodeSelector())
		if err != nil {
			return false, err
		}
		missing := 0
		for _, node := range nodes {
			for _, want := range p.desired(node) {
				if _, exists, _ := p.slices.GetByKey(want.Name); !exists {
					missing++
				}
			}
		}
		if missing > 0 {
			klog.V(1).Infof("Waiting for %d ResourceSlices of %d fake nodes", missing, len(nodes))
			return false, nil
		}
		klog.Infof("Published ResourceSlices of %d fake nodes (%s)", len(nodes), p.mode.Name())
		return true, nil
	})
}
