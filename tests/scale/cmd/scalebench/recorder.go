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
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/imex"
	nvinformers "sigs.k8s.io/dra-driver-nvidia-gpu/pkg/nvidia.com/informers/externalversions"
	nvlisters "sigs.k8s.io/dra-driver-nvidia-gpu/pkg/nvidia.com/listers/resource/v1beta1"
	"sigs.k8s.io/dra-driver-nvidia-gpu/tests/scale/fakeslices"
)

// podTimeline holds when scalebench observed each step of a workload pod.
// Watch events arrive within milliseconds on one host, which is finer than
// the one second resolution of API timestamps, and the same for every run.
type podTimeline struct {
	created time.Time
	bound   time.Time
	ready   time.Time
	deleted bool
	node    string
	// namespace and computeDomain name the ComputeDomain of the pod.
	namespace     string
	computeDomain string
	channel       channelTimeline
	gpusSeen      time.Time
}

// channelTimeline follows the channel claim of a pod.
type channelTimeline struct {
	allocated time.Time
	admitted  time.Time
	// rejections counts ChannelRejected decisions, after which the
	// scheduler allocates the claim again.
	rejections int
	reasons    map[string]int
	// wasRejected tracks the last rejection, to count each once.
	wasRejected bool
	// waitsForAdmission is true when the allocation carries binding
	// conditions, false when the device has none (gate off, or a shared
	// IMEX domain).
	waitsForAdmission bool
	// needsChannel is true for the shared channel device, whose channel the
	// controller assigns: without binding conditions, Prepare waits for it.
	needsChannel bool
}

// recorder watches the pods and claims of a run.
type recorder struct {
	runID string
	mu    sync.Mutex
	pods  map[types.UID]*podTimeline
	// claimOwner maps a claim to its pod, from its owner reference.
	claimOwner map[types.UID]types.UID
	// early holds claim events that arrived before their pod.
	early map[types.UID][]*resourceapi.ResourceClaim

	// staticDomain is the ComputeDomainDaemonSet UID whose daemons serve
	// every pod with static IMEX daemons; otherwise each pod is served by
	// the daemons of its ComputeDomain.
	staticDomain   string
	computeDomains nvlisters.ComputeDomainLister
	// daemonReady maps a domain UID and node to when the node's IMEX daemon
	// was first seen Ready in its ComputeDomainClique, which is what the
	// kubelet plugin's Prepare waits for.
	daemonReady map[string]time.Time
}

func newRecorder(runID string) *recorder {
	return &recorder{
		runID:       runID,
		pods:        make(map[types.UID]*podTimeline),
		claimOwner:  make(map[types.UID]types.UID),
		early:       make(map[types.UID][]*resourceapi.ResourceClaim),
		daemonReady: make(map[string]time.Time),
	}
}

func daemonKey(domain, node string) string {
	return domain + "/" + node
}

func (r *recorder) inRun(namespace string) bool {
	return strings.HasPrefix(namespace, runNamespacePrefix(r.runID))
}

// start watches pods and claims of the run namespaces. Factories watch all
// namespaces once, rather than one watch per namespace.
func (r *recorder) start(ctx context.Context, cs *clients) error {
	podFactory := informers.NewSharedInformerFactoryWithOptions(cs.core, 0, informers.WithTweakListOptions(func(o *metav1.ListOptions) {
		o.LabelSelector = fmt.Sprintf("%s=%s", scaleLabelKey, r.runID)
	}))
	claimFactory := informers.NewSharedInformerFactory(cs.core, 0)
	_, err := podFactory.Core().V1().Pods().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { r.onPod(obj) },
		UpdateFunc: func(_, obj any) { r.onPod(obj) },
		DeleteFunc: func(obj any) { r.onPodDelete(obj) },
	})
	if err != nil {
		return err
	}
	_, err = claimFactory.Resource().V1().ResourceClaims().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { r.onClaim(obj) },
		UpdateFunc: func(_, obj any) { r.onClaim(obj) },
	})
	if err != nil {
		return err
	}
	cdFactory := nvinformers.NewSharedInformerFactoryWithOptions(cs.nvidia, 0, nvinformers.WithTweakListOptions(func(o *metav1.ListOptions) {
		o.LabelSelector = fmt.Sprintf("%s=%s", scaleLabelKey, r.runID)
	}))
	r.computeDomains = cdFactory.Resource().V1beta1().ComputeDomains().Lister()
	cliqueFactory := nvinformers.NewSharedInformerFactoryWithOptions(cs.nvidia, 0, nvinformers.WithNamespace(cs.driverNamespace))
	_, err = cliqueFactory.Resource().V1beta1().ComputeDomainCliques().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { r.onClique(obj) },
		UpdateFunc: func(_, obj any) { r.onClique(obj) },
	})
	if err != nil {
		return err
	}
	podFactory.Start(ctx.Done())
	claimFactory.Start(ctx.Done())
	cdFactory.Start(ctx.Done())
	cliqueFactory.Start(ctx.Done())
	for _, f := range []informers.SharedInformerFactory{podFactory, claimFactory} {
		for typ, synced := range f.WaitForCacheSync(ctx.Done()) {
			if !synced {
				return fmt.Errorf("informer cache for %v did not sync", typ)
			}
		}
	}
	for _, f := range []nvinformers.SharedInformerFactory{cdFactory, cliqueFactory} {
		for typ, synced := range f.WaitForCacheSync(ctx.Done()) {
			if !synced {
				return fmt.Errorf("informer cache for %v did not sync", typ)
			}
		}
	}
	return nil
}

// onClique records when each daemon first registered Ready. A clique is
// named after the domain it belongs to: <domain UID>.<clique ID>.
func (r *recorder) onClique(obj any) {
	clique, ok := obj.(*nvapi.ComputeDomainClique)
	if !ok {
		return
	}
	domain, _, _ := strings.Cut(clique.Name, ".")
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, d := range clique.Daemons {
		key := daemonKey(domain, d.NodeName)
		if _, seen := r.daemonReady[key]; !seen && d.Status == nvapi.ComputeDomainStatusReady {
			r.daemonReady[key] = now
		}
	}
}

// domainOf returns the UID of the domain whose IMEX daemons serve a pod.
func (r *recorder) domainOf(t *podTimeline) string {
	if r.staticDomain != "" {
		return r.staticDomain
	}
	if r.computeDomains == nil || t.computeDomain == "" {
		return ""
	}
	cd, err := r.computeDomains.ComputeDomains(t.namespace).Get(t.computeDomain)
	if err != nil {
		return ""
	}
	return string(cd.UID)
}

func (r *recorder) onPod(obj any) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return
	}
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	t := r.pods[pod.UID]
	if t == nil {
		t = &podTimeline{created: now, namespace: pod.Namespace, computeDomain: pod.Labels[cdNameLabelKey]}
		r.pods[pod.UID] = t
		for _, claim := range r.early[pod.UID] {
			r.observeClaimLocked(t, claim, now)
		}
		delete(r.early, pod.UID)
	}
	if t.bound.IsZero() && pod.Spec.NodeName != "" {
		t.bound = now
		t.node = pod.Spec.NodeName
	}
	if t.ready.IsZero() && podReady(pod) {
		t.ready = now
	}
}

func (r *recorder) onPodDelete(obj any) {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if t := r.pods[pod.UID]; t != nil {
		t.deleted = true
	}
}

func (r *recorder) onClaim(obj any) {
	claim, ok := obj.(*resourceapi.ResourceClaim)
	if !ok || !r.inRun(claim.Namespace) {
		return
	}
	owner := metav1.GetControllerOf(claim)
	if owner == nil || owner.Kind != "Pod" {
		return
	}
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.claimOwner[claim.UID] = owner.UID
	t := r.pods[owner.UID]
	if t == nil {
		r.early[owner.UID] = append(r.early[owner.UID], claim.DeepCopy())
		return
	}
	r.observeClaimLocked(t, claim, now)
}

func (r *recorder) observeClaimLocked(t *podTimeline, claim *resourceapi.ResourceClaim, now time.Time) {
	if claim.Status.Allocation == nil {
		return
	}
	for _, result := range claim.Status.Allocation.Devices.Results {
		switch {
		case result.Driver == fakeslices.GPUDriverName:
			if t.gpusSeen.IsZero() {
				t.gpusSeen = now
			}
		case result.Driver == fakeslices.ComputeDomainDriverName && strings.HasPrefix(result.Device, "channel"):
			r.observeChannelLocked(&t.channel, claim, &result, now)
		}
	}
}

func (r *recorder) observeChannelLocked(c *channelTimeline, claim *resourceapi.ResourceClaim, result *resourceapi.DeviceRequestAllocationResult, now time.Time) {
	if c.allocated.IsZero() {
		c.allocated = now
	}
	c.waitsForAdmission = slices.Contains(result.BindingConditions, imex.ChannelAdmittedCondition)
	c.needsChannel = result.Device == imex.ChannelDeviceName
	for i := range claim.Status.Devices {
		status := &claim.Status.Devices[i]
		if status.Driver != result.Driver || status.Pool != result.Pool || status.Device != result.Device {
			continue
		}
		if c.admitted.IsZero() && meta.IsStatusConditionTrue(status.Conditions, imex.ChannelAdmittedCondition) {
			c.admitted = now
		}
		rejected := meta.FindStatusCondition(status.Conditions, imex.ChannelRejectedCondition)
		if rejected != nil && rejected.Status == metav1.ConditionTrue {
			if !c.wasRejected {
				c.rejections++
				if c.reasons == nil {
					c.reasons = make(map[string]int)
				}
				c.reasons[rejected.Reason]++
			}
			c.wasRejected = true
		} else {
			c.wasRejected = false
		}
	}
}

// prepareReady returns when the kubelet plugin's Prepare of a pod's channel
// claim could first succeed: once the IMEX daemon on its node is Ready, and,
// without binding conditions, the controller assigned its channel.
func (r *recorder) prepareReady(t *podTimeline) (time.Time, bool) {
	readyAt, ok := r.daemonReady[daemonKey(r.domainOf(t), t.node)]
	if !ok {
		return time.Time{}, false
	}
	if c := t.channel; c.needsChannel && !c.waitsForAdmission {
		if c.admitted.IsZero() {
			return time.Time{}, false
		}
		if c.admitted.After(readyAt) {
			readyAt = c.admitted
		}
	}
	return readyAt, true
}

// Summary describes a distribution of durations, in seconds.
type Summary struct {
	Count int     `json:"count"`
	Mean  float64 `json:"mean"`
	P50   float64 `json:"p50"`
	P90   float64 `json:"p90"`
	P99   float64 `json:"p99"`
	Max   float64 `json:"max"`
}

func summarize(durations []time.Duration) Summary {
	if len(durations) == 0 {
		return Summary{}
	}
	sorted := slices.Clone(durations)
	slices.Sort(sorted)
	var total time.Duration
	for _, d := range sorted {
		total += d
	}
	at := func(q float64) float64 {
		i := int(math.Ceil(q*float64(len(sorted)))) - 1
		return sorted[max(i, 0)].Seconds()
	}
	return Summary{
		Count: len(sorted),
		Mean:  (total / time.Duration(len(sorted))).Seconds(),
		P50:   at(0.5),
		P90:   at(0.9),
		P99:   at(0.99),
		Max:   sorted[len(sorted)-1].Seconds(),
	}
}

// PodLatencies breaks down where the time from creating a pod to binding it
// went.
type PodLatencies struct {
	// CreateToBound is what users see: from creating a pod to it being
	// bound to a node.
	CreateToBound Summary `json:"createToBound"`
	// CreateToAllocated is scheduling until PreBind writes the allocation,
	// including time in the scheduling queue.
	CreateToAllocated Summary `json:"createToAllocated"`
	// AllocatedToAdmitted is the controller's admission. Without binding
	// conditions it runs while the pod binds.
	AllocatedToAdmitted Summary `json:"allocatedToAdmitted"`
	// AdmittedToBound is the scheduler noticing the condition and binding.
	AdmittedToBound Summary `json:"admittedToBound"`
	// AllocatedToBound is the whole PreBind wait plus binding.
	AllocatedToBound Summary `json:"allocatedToBound"`
	// CreateToPrepareReady is a workload's bootstrap: until its pod is bound,
	// the IMEX daemon on its node is Ready and, without binding conditions,
	// its channel is assigned, so that the kubelet plugin's Prepare can
	// succeed.
	CreateToPrepareReady Summary `json:"createToPrepareReady,omitzero"`
	// BoundToPrepareReady is how long a bound pod waited for that daemon and
	// channel; 0 when both were ready before the pod was bound.
	BoundToPrepareReady Summary `json:"boundToPrepareReady,omitzero"`
}

// Outcome counts what happened to the pods of a run.
type Outcome struct {
	Pods  int `json:"pods"`
	Bound int `json:"bound"`
	Ready int `json:"ready"`
	// PrepareReady counts bound pods whose Prepare could succeed.
	PrepareReady      int            `json:"prepareReady"`
	WaitedOnAdmission int            `json:"waitedOnAdmission"`
	Rejections        int            `json:"rejections"`
	RejectionReasons  map[string]int `json:"rejectionReasons,omitempty"`
	// Throughput is bound pods per second, from the first pod created to
	// the last pod bound.
	Throughput float64 `json:"throughputPodsPerSecond"`
	// Window is that span, in seconds.
	Window float64 `json:"windowSeconds"`
}

func (r *recorder) counts() (created, bound int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, t := range r.pods {
		created++
		if !t.bound.IsZero() {
			bound++
		}
	}
	return created, bound
}

// unboundPods returns how many pods that exist are not bound yet.
func (r *recorder) unboundPods() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, t := range r.pods {
		if t.bound.IsZero() && !t.deleted {
			n++
		}
	}
	return n
}

func (r *recorder) analyze() (PodLatencies, Outcome) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var createToBound, createToAllocated, allocatedToAdmitted, admittedToBound, allocatedToBound []time.Duration
	var createToPrepareReady, boundToPrepareReady []time.Duration
	out := Outcome{RejectionReasons: make(map[string]int)}
	var first, last time.Time
	for _, t := range r.pods {
		out.Pods++
		if first.IsZero() || t.created.Before(first) {
			first = t.created
		}
		out.Rejections += t.channel.rejections
		for reason, n := range t.channel.reasons {
			out.RejectionReasons[reason] += n
		}
		if !t.ready.IsZero() {
			out.Ready++
		}
		if t.bound.IsZero() {
			continue
		}
		out.Bound++
		if t.bound.After(last) {
			last = t.bound
		}
		createToBound = append(createToBound, t.bound.Sub(t.created))
		c := t.channel
		if readyAt, ok := r.prepareReady(t); ok {
			out.PrepareReady++
			wait := max(readyAt.Sub(t.bound), 0)
			boundToPrepareReady = append(boundToPrepareReady, wait)
			createToPrepareReady = append(createToPrepareReady, t.bound.Sub(t.created)+wait)
		}
		if c.allocated.IsZero() {
			continue
		}
		createToAllocated = append(createToAllocated, c.allocated.Sub(t.created))
		allocatedToBound = append(allocatedToBound, t.bound.Sub(c.allocated))
		if !c.admitted.IsZero() {
			allocatedToAdmitted = append(allocatedToAdmitted, c.admitted.Sub(c.allocated))
		}
		if c.waitsForAdmission {
			out.WaitedOnAdmission++
			if !c.admitted.IsZero() {
				admittedToBound = append(admittedToBound, t.bound.Sub(c.admitted))
			}
		}
	}
	if out.Bound > 0 && last.After(first) {
		out.Window = last.Sub(first).Seconds()
		out.Throughput = float64(out.Bound) / out.Window
	}
	return PodLatencies{
		CreateToBound:       summarize(createToBound),
		CreateToAllocated:   summarize(createToAllocated),
		AllocatedToAdmitted: summarize(allocatedToAdmitted),
		AdmittedToBound:     summarize(admittedToBound),
		AllocatedToBound:    summarize(allocatedToBound),

		CreateToPrepareReady: summarize(createToPrepareReady),
		BoundToPrepareReady:  summarize(boundToPrepareReady),
	}, out
}
