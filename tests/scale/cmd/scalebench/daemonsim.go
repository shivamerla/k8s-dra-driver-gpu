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
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
	nvinformers "sigs.k8s.io/dra-driver-nvidia-gpu/pkg/nvidia.com/informers/externalversions"
)

// daemonSimulator does what compute-domain-daemon does against the API server
// once its IMEX daemon runs, for daemon pods on fake nodes: it labels its pod
// with the clique it joined, registers in the ComputeDomainClique (NotReady,
// then Ready), registers again when its entry or the clique goes away, and
// unregisters when its pod terminates. The controller and the
// static IMEX daemon rollout read these, and the concurrent writers to one
// clique object are part of what scale tests measure.
type daemonSimulator struct {
	cs    *clients
	nodes corelisters.NodeLister
	pods  corelisters.PodNamespaceLister
	queue workqueue.TypedRateLimitingInterface[string]
	// readyDelay is how long a daemon takes to become Ready once its pod
	// runs.
	readyDelay time.Duration

	mu sync.Mutex
	// registered maps a pod key to the clique it registered in, to
	// unregister it.
	registered map[string]registration
	// owners maps a clique entry (clique and node) to the pod that last
	// registered it. KWOK replaces a daemon pod at once, so a replaced pod
	// may unregister after its successor registered; it must not remove the
	// successor's entry.
	owners map[registration]string
	// rejoining holds the pods queued by reregisterMissing, whose
	// registration latency is not that of a new daemon.
	rejoining map[string]bool

	Writes    atomic.Int64
	Conflicts atomic.Int64
	// Reregistered counts daemons that registered again after their entry
	// or clique was removed.
	Reregistered atomic.Int64
	// registerLatency records, per daemon, the time from its pod running to
	// its Ready registration.
	latencyMu       sync.Mutex
	registerLatency []time.Duration
}

type registration struct {
	clique string
	node   string
}

func newDaemonSimulator(cs *clients, nodeFactory, driverFactory informers.SharedInformerFactory, cliqueFactory nvinformers.SharedInformerFactory, readyDelay time.Duration) (*daemonSimulator, error) {
	s := &daemonSimulator{
		cs:         cs,
		readyDelay: readyDelay,
		nodes:      nodeFactory.Core().V1().Nodes().Lister(),
		pods:       driverFactory.Core().V1().Pods().Lister().Pods(cs.driverNamespace),
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "scalebench-daemons"},
		),
		registered: make(map[string]registration),
		owners:     make(map[registration]string),
		rejoining:  make(map[string]bool),
	}
	enqueue := func(obj any) {
		if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
			obj = tombstone.Obj
		}
		pod, ok := obj.(*corev1.Pod)
		if !ok || daemonDomain(pod) == "" {
			return
		}
		s.queue.Add(pod.Name)
	}
	_, err := driverFactory.Core().V1().Pods().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    enqueue,
		UpdateFunc: func(_, obj any) { enqueue(obj) },
		DeleteFunc: enqueue,
	})
	if err != nil {
		return nil, fmt.Errorf("error adding Pod event handler: %w", err)
	}
	// The daemon puts its entry back on every change of its clique, and
	// the clique itself at start. Garbage collection deletes a clique whose
	// daemon pods were all replaced, and the controller removes entries it
	// takes for stale, which can race with a new daemon registering.
	_, err = cliqueFactory.Resource().V1beta1().ComputeDomainCliques().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { s.reregisterMissing(obj, false) },
		UpdateFunc: func(_, obj any) { s.reregisterMissing(obj, false) },
		DeleteFunc: func(obj any) { s.reregisterMissing(obj, true) },
	})
	if err != nil {
		return nil, fmt.Errorf("error adding ComputeDomainClique event handler: %w", err)
	}
	return s, nil
}

// reregisterMissing queues the daemons registered in a clique whose entry is
// gone, or all of them when the clique was deleted, to register again.
func (s *daemonSimulator) reregisterMissing(obj any, deleted bool) {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	clique, ok := obj.(*nvapi.ComputeDomainClique)
	if !ok {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for reg, name := range s.owners {
		if reg.clique != clique.Name {
			continue
		}
		if !deleted && slices.ContainsFunc(clique.Daemons, func(d *nvapi.ComputeDomainDaemonInfo) bool { return d.NodeName == reg.node }) {
			continue
		}
		delete(s.registered, name)
		delete(s.owners, reg)
		s.rejoining[name] = true
		s.Reregistered.Add(1)
		s.queue.Add(name)
	}
}

// daemonDomain returns the label key and UID of the domain an IMEX daemon pod
// serves: the ComputeDomainDaemonSet of a static daemon, or the
// ComputeDomain of a per-ComputeDomain daemon.
func daemonDomain(pod *corev1.Pod) string {
	if uid := pod.Labels[computeDomainDaemonSetLabelKey]; uid != "" {
		return uid
	}
	return pod.Labels[computeDomainLabelKey]
}

func daemonDomainLabelKey(pod *corev1.Pod) string {
	if pod.Labels[computeDomainDaemonSetLabelKey] != "" {
		return computeDomainDaemonSetLabelKey
	}
	return computeDomainLabelKey
}

func (s *daemonSimulator) run(ctx context.Context, workers int) {
	defer s.queue.ShutDown()
	for range workers {
		go func() {
			for s.processNext(ctx) {
			}
		}()
	}
	<-ctx.Done()
}

func (s *daemonSimulator) processNext(ctx context.Context) bool {
	name, shutdown := s.queue.Get()
	if shutdown {
		return false
	}
	defer s.queue.Done(name)
	if err := s.sync(ctx, name); err != nil {
		klog.V(1).Infof("Error simulating IMEX daemon %s: %v", name, err)
		s.queue.AddRateLimited(name)
		return true
	}
	s.queue.Forget(name)
	return true
}

func (s *daemonSimulator) sync(ctx context.Context, name string) error {
	pod, err := s.pods.Get(name)
	if apierrors.IsNotFound(err) || (err == nil && pod.DeletionTimestamp != nil) {
		return s.unregister(ctx, name)
	}
	if err != nil {
		return err
	}
	if pod.Spec.NodeName == "" || !podReady(pod) {
		return nil
	}
	node, err := s.nodes.Get(pod.Spec.NodeName)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	clique := node.Labels[gpuCliqueLabelKey]
	if node.Labels[fakeNodeLabelKey] == "" || clique == "" {
		return nil
	}
	if _, joined := pod.Labels[computeDomainCliqueLabelKey]; !joined {
		if err := s.labelPod(ctx, pod, clique); err != nil {
			return err
		}
	} else if pod.Labels[computeDomainCliqueLabelKey] != clique {
		// The real daemon keeps the clique it joined until it restarts,
		// which is what lets the controller realign it.
		clique = pod.Labels[computeDomainCliqueLabelKey]
	}
	cliqueName := fmt.Sprintf("%s.%s", daemonDomain(pod), clique)
	s.mu.Lock()
	_, done := s.registered[name]
	s.mu.Unlock()
	if done {
		return nil
	}
	if since, ok := podReadyTime(pod); ok && s.readyDelay > 0 {
		if wait := s.readyDelay - time.Since(since); wait > 0 {
			s.queue.AddAfter(name, wait)
			return nil
		}
	}
	if err := s.register(ctx, pod, cliqueName, clique); err != nil {
		return err
	}
	reg := registration{clique: cliqueName, node: pod.Spec.NodeName}
	s.mu.Lock()
	s.registered[name] = reg
	s.owners[reg] = name
	rejoined := s.rejoining[name]
	delete(s.rejoining, name)
	s.mu.Unlock()
	if since, ok := podReadyTime(pod); ok && !rejoined {
		s.latencyMu.Lock()
		s.registerLatency = append(s.registerLatency, time.Since(since))
		s.latencyMu.Unlock()
	}
	return nil
}

func (s *daemonSimulator) labelPod(ctx context.Context, pod *corev1.Pod, clique string) error {
	patch, err := json.Marshal(map[string]any{"metadata": map[string]any{"labels": map[string]string{computeDomainCliqueLabelKey: clique}}})
	if err != nil {
		return err
	}
	_, err = s.cs.core.CoreV1().Pods(pod.Namespace).Patch(ctx, pod.Name, types.MergePatchType, patch, metav1.PatchOptions{})
	s.Writes.Add(1)
	if err != nil {
		return fmt.Errorf("error labeling pod %s: %w", pod.Name, err)
	}
	return nil
}

// register adds the daemon to its clique NotReady, then marks it Ready, in
// two writes as the daemon does.
func (s *daemonSimulator) register(ctx context.Context, pod *corev1.Pod, cliqueName, clique string) error {
	cliques := s.cs.nvidia.ResourceV1beta1().ComputeDomainCliques(s.cs.driverNamespace)
	_, err := cliques.Create(ctx, &nvapi.ComputeDomainClique{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cliqueName,
			Namespace: s.cs.driverNamespace,
			Labels: map[string]string{
				daemonDomainLabelKey(pod):   daemonDomain(pod),
				computeDomainCliqueLabelKey: clique,
			},
			OwnerReferences: []metav1.OwnerReference{podOwnerRef(pod)},
		},
	}, metav1.CreateOptions{})
	s.Writes.Add(1)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("error creating ComputeDomainClique %s: %w", cliqueName, err)
	}
	for _, status := range []string{nvapi.ComputeDomainStatusNotReady, nvapi.ComputeDomainStatusReady} {
		err := s.updateClique(ctx, cliqueName, func(c *nvapi.ComputeDomainClique) (bool, error) {
			i := slices.IndexFunc(c.Daemons, func(d *nvapi.ComputeDomainDaemonInfo) bool { return d.NodeName == pod.Spec.NodeName })
			if i < 0 {
				index, err := nextIndex(c.Daemons)
				if err != nil {
					return false, err
				}
				c.Daemons = append(c.Daemons, &nvapi.ComputeDomainDaemonInfo{NodeName: pod.Spec.NodeName, CliqueID: clique, Index: index})
				i = len(c.Daemons) - 1
			}
			d := c.Daemons[i]
			if d.IPAddress == pod.Status.PodIP && d.Status == status {
				return false, nil
			}
			d.IPAddress = pod.Status.PodIP
			d.Status = status
			if !slices.ContainsFunc(c.OwnerReferences, func(r metav1.OwnerReference) bool { return r.UID == pod.UID }) {
				c.OwnerReferences = append(c.OwnerReferences, podOwnerRef(pod))
			}
			return true, nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *daemonSimulator) unregister(ctx context.Context, name string) error {
	s.mu.Lock()
	reg, ok := s.registered[name]
	replaced := ok && s.owners[reg] != name
	if replaced {
		delete(s.registered, name)
	}
	s.mu.Unlock()
	if !ok || replaced {
		return nil
	}
	// A daemon whose node is gone cannot unregister, which leaves its entry
	// to the controller.
	if _, err := s.nodes.Get(reg.node); apierrors.IsNotFound(err) {
		s.mu.Lock()
		delete(s.registered, name)
		if s.owners[reg] == name {
			delete(s.owners, reg)
		}
		s.mu.Unlock()
		return nil
	}
	err := s.updateClique(ctx, reg.clique, func(c *nvapi.ComputeDomainClique) (bool, error) {
		n := len(c.Daemons)
		c.Daemons = slices.DeleteFunc(c.Daemons, func(d *nvapi.ComputeDomainDaemonInfo) bool { return d.NodeName == reg.node })
		return len(c.Daemons) != n, nil
	})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	s.mu.Lock()
	delete(s.registered, name)
	if s.owners[reg] == name {
		delete(s.owners, reg)
	}
	s.mu.Unlock()
	return nil
}

// updateClique applies mutate to the latest clique and writes it, retrying on
// conflicts with other daemons of the clique.
func (s *daemonSimulator) updateClique(ctx context.Context, name string, mutate func(*nvapi.ComputeDomainClique) (bool, error)) error {
	cliques := s.cs.nvidia.ResourceV1beta1().ComputeDomainCliques(s.cs.driverNamespace)
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		clique, err := cliques.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		changed, err := mutate(clique)
		if err != nil || !changed {
			return err
		}
		_, err = cliques.Update(ctx, clique, metav1.UpdateOptions{})
		s.Writes.Add(1)
		if apierrors.IsConflict(err) {
			s.Conflicts.Add(1)
		}
		return err
	})
}

// nextIndex fills the lowest free index, as the daemon does, so that DNS
// names stay stable.
func nextIndex(daemons []*nvapi.ComputeDomainDaemonInfo) (int, error) {
	used := make(map[int]bool, len(daemons))
	for _, d := range daemons {
		used[d.Index] = true
	}
	for i := range 1024 {
		if !used[i] {
			return i, nil
		}
	}
	return 0, fmt.Errorf("no free daemon index")
}

func podOwnerRef(pod *corev1.Pod) metav1.OwnerReference {
	return metav1.OwnerReference{APIVersion: "v1", Kind: "Pod", Name: pod.Name, UID: pod.UID}
}

func podReady(pod *corev1.Pod) bool {
	_, ready := podReadyTime(pod)
	return ready
}

func podReadyTime(pod *corev1.Pod) (time.Time, bool) {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return c.LastTransitionTime.Time, true
		}
	}
	return time.Time{}, false
}

// stats returns what the simulator wrote and how long registration took.
func (s *daemonSimulator) stats() daemonSimStats {
	s.latencyMu.Lock()
	latencies := slices.Clone(s.registerLatency)
	s.latencyMu.Unlock()
	return daemonSimStats{
		Writes:          s.Writes.Load(),
		Conflicts:       s.Conflicts.Load(),
		Reregistered:    s.Reregistered.Load(),
		Registered:      len(latencies),
		RegisterLatency: summarize(latencies),
	}
}

type daemonSimStats struct {
	Writes          int64   `json:"writes"`
	Conflicts       int64   `json:"conflicts"`
	Reregistered    int64   `json:"reregistered"`
	Registered      int     `json:"registered"`
	RegisterLatency Summary `json:"registerLatency"`
}
