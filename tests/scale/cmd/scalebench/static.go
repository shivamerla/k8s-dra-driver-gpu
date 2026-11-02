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
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/urfave/cli/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/informers"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/imex"
	nvinformers "sigs.k8s.io/dra-driver-nvidia-gpu/pkg/nvidia.com/informers/externalversions"
	nvlisters "sigs.k8s.io/dra-driver-nvidia-gpu/pkg/nvidia.com/listers/resource/v1beta1"
)

const templateHashKey = "resource.nvidia.com/templateHash"

var staticPhases = []string{"rollout", "update", "repartition", "node-churn"}

type staticParams struct {
	Phases                []string `json:"phases"`
	MaxUnavailableCliques string   `json:"maxUnavailableCliques"`
	MinReadySeconds       int32    `json:"minReadySeconds"`
	MaxNodesPerClique     int      `json:"maxNodesPerClique"`
	RepartitionNodes      int      `json:"repartitionNodes"`
	PhaseTimeout          string   `json:"phaseTimeout"`
}

func staticCommand() *cli.Command {
	return &cli.Command{
		Name:  "static",
		Usage: "Measure static IMEX daemons: initial rollout, rolling update, NVLink repartition and node churn",
		Flags: append(scenarioFlags(),
			&cli.StringSliceFlag{Name: "phases", Value: cli.NewStringSlice(staticPhases...), Usage: "phases to run, in order"},
			&cli.StringFlag{Name: "max-unavailable-cliques", Value: "25%", Usage: "rollingUpdate.maxUnavailableCliques, a number or percentage"},
			&cli.IntFlag{Name: "min-ready-seconds", Value: 0, Usage: "rollingUpdate.minReadySeconds"},
			&cli.IntFlag{Name: "max-nodes-per-clique", Value: 18},
			&cli.IntFlag{Name: "repartition-nodes", Value: 4, Usage: "nodes moved to a new NVLink clique by the repartition phase"},
			&cli.DurationFlag{Name: "phase-timeout", Value: 20 * time.Minute},
			&cli.BoolFlag{Name: "recreate", Usage: "delete an existing ComputeDomainDaemonSet first, to measure the initial rollout"},
		),
		Action: runStatic,
	}
}

// staticRun watches the static IMEX daemons through informers, so that
// sampling them does not load the API server it measures.
type staticRun struct {
	cs     *clients
	params staticParams
	pods   corelisters.PodNamespaceLister
	nodes  corelisters.NodeLister
	clique nvlisters.ComputeDomainCliqueNamespaceLister
	// simulated is true when daemons register in cliques, so registration
	// can be checked.
	simulated bool
	result    *Result
}

// PhaseResult reports one phase of the static scenario.
type PhaseResult struct {
	Name     string             `json:"name"`
	Duration float64            `json:"durationSeconds"`
	Marks    map[string]float64 `json:"marksSeconds,omitempty"`
	// MaxDisruptedCliques is the most cliques without a ready daemon on
	// every node at once, during a rolling update.
	MaxDisruptedCliques int     `json:"maxDisruptedCliques,omitempty"`
	CliqueRestart       Summary `json:"cliqueRestart,omitzero"`
	Error               string  `json:"error,omitempty"`
}

func runStatic(c *cli.Context) error {
	ctx, cancel := signalContext(c.Context)
	defer cancel()
	cs, err := newClients(c)
	if err != nil {
		return err
	}
	mode, err := detectDriverMode(ctx, cs)
	if err != nil {
		return err
	}
	if mode.Lifecycle != imex.DaemonLifecycleStatic {
		return fmt.Errorf("the driver runs %s, install it with scripts/install-driver.sh static", mode.Name())
	}
	params := staticParams{
		Phases:                c.StringSlice("phases"),
		MaxUnavailableCliques: c.String("max-unavailable-cliques"),
		MinReadySeconds:       int32(c.Int("min-ready-seconds")),
		MaxNodesPerClique:     c.Int("max-nodes-per-clique"),
		RepartitionNodes:      c.Int("repartition-nodes"),
		PhaseTimeout:          c.Duration("phase-timeout").String(),
	}
	for _, phase := range params.Phases {
		if !slices.Contains(staticPhases, phase) {
			return fmt.Errorf("unknown phase %q, must be one of %s", phase, strings.Join(staticPhases, ", "))
		}
	}
	var a *agent
	if c.Bool("agent") {
		if a, err = startAgent(ctx, cs, agentOptionsFrom(c)); err != nil {
			return err
		}
	}
	run, err := newStaticRun(ctx, cs, params, c.Bool("agent") && c.Bool("simulate-daemons"))
	if err != nil {
		return err
	}
	run.result = &Result{Scenario: "static", Label: c.String("label"), RunID: newRunID(), Mode: mode, Params: params, Start: time.Now()}
	cliqueNodes := run.cliqueNodes()
	run.result.Nodes = len(cliqueNodes)
	cliques := make(map[string]bool)
	for _, id := range cliqueNodes {
		cliques[id] = true
	}
	run.result.Cliques = len(cliques)

	sc := &scraper{cs: cs}
	before := sc.scrapeAll(ctx, sourceAPIServer, sourceScheduler, sourceController)
	sampler := &gaugeSampler{scraper: sc}
	samplerCtx, stopSampler := context.WithCancel(ctx)
	defer stopSampler()
	go sampler.run(samplerCtx, 5*time.Second)

	var runErr error
	for _, phase := range params.Phases {
		phaseCtx, cancelPhase := context.WithTimeout(ctx, c.Duration("phase-timeout"))
		start := time.Now()
		var pr PhaseResult
		var err error
		switch phase {
		case "rollout":
			pr, err = run.rollout(phaseCtx, c.Bool("recreate"))
		case "update":
			pr, err = run.update(phaseCtx)
		case "repartition":
			pr, err = run.repartition(phaseCtx)
		case "node-churn":
			pr, err = run.nodeChurn(phaseCtx)
		}
		cancelPhase()
		pr.Name = phase
		pr.Duration = time.Since(start).Seconds()
		if err != nil {
			pr.Error = err.Error()
			runErr = errors.Join(runErr, fmt.Errorf("phase %s: %w", phase, err))
		}
		run.result.addCheck(fmt.Sprintf("phase %s settles", phase), err == nil, "")
		klog.Infof("Phase %s took %.1fs", phase, pr.Duration)
		run.result.Phases = append(run.result.Phases, pr)
		// The other phases change a rolled out daemon set, so that they
		// still run after one of them fails, from where it left off.
		if err != nil && phase == "rollout" {
			break
		}
		if err != nil {
			klog.Warningf("Phase %s failed, running the next phase from where it left off: %v", phase, err)
		}
	}
	stopSampler()
	run.result.End = time.Now()
	run.result.Metrics = diffMetrics(before, sc.scrapeAll(context.WithoutCancel(ctx), sourceAPIServer, sourceScheduler, sourceController))
	run.result.Gauges = sampler.results()
	if a != nil {
		st := a.stats()
		run.result.Agent = &st
	}
	if runErr != nil {
		run.result.Error = runErr.Error()
	}
	if !c.Bool("keep") && runErr == nil && slices.Contains(params.Phases, "rollout") {
		run.deleteCDDS(context.WithoutCancel(ctx))
	}
	if err := run.result.write(c.String("output-dir")); err != nil {
		return err
	}
	if failed := run.result.failedChecks(); len(failed) > 0 && runErr == nil {
		runErr = fmt.Errorf("failed checks: %s", strings.Join(failed, ", "))
	}
	return runErr
}

func newStaticRun(ctx context.Context, cs *clients, params staticParams, simulated bool) (*staticRun, error) {
	driverFactory := informers.NewSharedInformerFactoryWithOptions(cs.core, 0, informers.WithNamespace(cs.driverNamespace), informers.WithTweakListOptions(func(o *metav1.ListOptions) {
		o.LabelSelector = computeDomainDaemonSetLabelKey
	}))
	nodeFactory := informers.NewSharedInformerFactoryWithOptions(cs.core, 0, informers.WithTweakListOptions(func(o *metav1.ListOptions) {
		o.LabelSelector = fakeNodeSelector().String()
	}))
	cliqueFactory := nvinformers.NewSharedInformerFactoryWithOptions(cs.nvidia, 0, nvinformers.WithNamespace(cs.driverNamespace))
	run := &staticRun{
		cs:        cs,
		params:    params,
		pods:      driverFactory.Core().V1().Pods().Lister().Pods(cs.driverNamespace),
		nodes:     nodeFactory.Core().V1().Nodes().Lister(),
		clique:    cliqueFactory.Resource().V1beta1().ComputeDomainCliques().Lister().ComputeDomainCliques(cs.driverNamespace),
		simulated: simulated,
	}
	driverFactory.Start(ctx.Done())
	nodeFactory.Start(ctx.Done())
	cliqueFactory.Start(ctx.Done())
	for typ, ok := range driverFactory.WaitForCacheSync(ctx.Done()) {
		if !ok {
			return nil, fmt.Errorf("informer cache for %v did not sync", typ)
		}
	}
	for typ, ok := range nodeFactory.WaitForCacheSync(ctx.Done()) {
		if !ok {
			return nil, fmt.Errorf("informer cache for %v did not sync", typ)
		}
	}
	for typ, ok := range cliqueFactory.WaitForCacheSync(ctx.Done()) {
		if !ok {
			return nil, fmt.Errorf("informer cache for %v did not sync", typ)
		}
	}
	return run, nil
}

// cliqueNodes maps the fake nodes in a clique to it.
func (r *staticRun) cliqueNodes() map[string]string {
	nodes, err := r.nodes.List(fakeNodeSelector())
	if err != nil {
		return nil
	}
	m := make(map[string]string, len(nodes))
	for _, n := range nodes {
		if id := n.Labels[gpuCliqueLabelKey]; id != "" {
			m[n.Name] = id
		}
	}
	return m
}

// staticSnapshot is the state of the static IMEX daemons at one moment.
type staticSnapshot struct {
	nodes int
	// ready counts nodes running a ready daemon, updated those that run
	// one of wantHash.
	ready, updated int
	// registered counts nodes Ready in the clique of their node, stale the
	// clique entries of nodes that left it.
	registered, stale int
	// disrupted lists cliques with a node missing a ready daemon.
	disrupted []string
}

func (r *staticRun) snapshot(cddsUID types.UID, wantHash string) staticSnapshot {
	cliqueOf := r.cliqueNodes()
	s := staticSnapshot{nodes: len(cliqueOf)}
	pods, _ := r.pods.List(labels.SelectorFromSet(labels.Set{computeDomainDaemonSetLabelKey: string(cddsUID)}))
	readyOn := make(map[string]*corev1.Pod)
	for _, pod := range pods {
		if pod.DeletionTimestamp == nil && podReady(pod) {
			readyOn[pod.Spec.NodeName] = pod
		}
	}
	disrupted := make(map[string]bool)
	for node, clique := range cliqueOf {
		pod, ok := readyOn[node]
		if !ok {
			disrupted[clique] = true
			continue
		}
		s.ready++
		if wantHash == "" || pod.Annotations[templateHashKey] == wantHash {
			s.updated++
		}
	}
	for clique := range disrupted {
		s.disrupted = append(s.disrupted, clique)
	}
	sort.Strings(s.disrupted)
	cliques, _ := r.clique.List(labels.SelectorFromSet(labels.Set{computeDomainDaemonSetLabelKey: string(cddsUID)}))
	for _, c := range cliques {
		id := c.Labels[computeDomainCliqueLabelKey]
		for _, d := range c.Daemons {
			switch {
			case cliqueOf[d.NodeName] != id:
				s.stale++
			case d.Status == nvapi.ComputeDomainStatusReady:
				s.registered++
			}
		}
	}
	return s
}

// settled reports whether every node runs a ready daemon of the wanted
// revision, registered in its clique with no stale entries.
func (r *staticRun) settled(s staticSnapshot) bool {
	if s.ready != s.nodes || s.updated != s.nodes {
		return false
	}
	return !r.simulated || (s.registered == s.nodes && s.stale == 0)
}

func (r *staticRun) getCDDS(ctx context.Context) (*nvapi.ComputeDomainDaemonSet, error) {
	cdds, err := r.cs.nvidia.ResourceV1beta1().ComputeDomainDaemonSets().Get(ctx, nvapi.ComputeDomainDaemonSetName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	return cdds, err
}

func (r *staticRun) rollout(ctx context.Context, recreate bool) (PhaseResult, error) {
	pr := PhaseResult{Marks: make(map[string]float64)}
	existing, err := r.getCDDS(ctx)
	if err != nil {
		return pr, err
	}
	if existing != nil {
		if !recreate {
			return pr, fmt.Errorf("ComputeDomainDaemonSet %s exists, pass --recreate to measure the rollout from scratch", existing.Name)
		}
		r.deleteCDDS(ctx)
	}
	maxUnavailable := intstr.Parse(r.params.MaxUnavailableCliques)
	cdds := &nvapi.ComputeDomainDaemonSet{
		ObjectMeta: metav1.ObjectMeta{Name: nvapi.ComputeDomainDaemonSetName},
		Spec: nvapi.ComputeDomainDaemonSetSpec{
			MaxNodesPerClique: r.params.MaxNodesPerClique,
			LogVerbosity:      1,
			UpdateStrategy: nvapi.ComputeDomainDaemonSetUpdateStrategy{
				Type: nvapi.ComputeDomainDaemonSetUpdateStrategyRollingUpdate,
				RollingUpdate: &nvapi.ComputeDomainDaemonSetRollingUpdate{
					MaxUnavailableCliques: &maxUnavailable,
					MinReadySeconds:       ptr.To(r.params.MinReadySeconds),
				},
			},
		},
	}
	start := time.Now()
	created, err := r.cs.nvidia.ResourceV1beta1().ComputeDomainDaemonSets().Create(ctx, cdds, metav1.CreateOptions{})
	if err != nil {
		return pr, fmt.Errorf("error creating ComputeDomainDaemonSet: %w", err)
	}
	err = r.waitSettled(ctx, created.UID, "", start, pr.Marks, true)
	return pr, err
}

// waitSettled samples the daemons until they settle and the
// ComputeDomainDaemonSet reports it, recording when each milestone passed.
func (r *staticRun) waitSettled(ctx context.Context, uid types.UID, wantHash string, start time.Time, marks map[string]float64, needReady bool) error {
	mark := func(name string) {
		if _, ok := marks[name]; !ok {
			marks[name] = time.Since(start).Seconds()
		}
	}
	lastLog := time.Now()
	return poll(ctx, 500*time.Millisecond, 24*time.Hour, func() (bool, error) {
		s := r.snapshot(uid, wantHash)
		if s.ready == s.nodes {
			mark("allDaemonsReady")
		}
		if s.updated == s.nodes {
			mark("allDaemonsUpdated")
		}
		if r.simulated && s.registered == s.nodes && s.stale == 0 {
			mark("allRegistered")
		}
		cdds, err := r.getCDDS(ctx)
		if err != nil {
			return false, err
		}
		statusDone := cdds != nil && cdds.Status.ObservedGeneration == cdds.Generation &&
			int(cdds.Status.NumberReady) == s.nodes && int(cdds.Status.UpdatedNumberScheduled) == s.nodes &&
			!meta.IsStatusConditionTrue(cdds.Status.Conditions, nvapi.ComputeDomainDaemonSetConditionRolloutInProgress)
		if needReady && cdds != nil {
			statusDone = statusDone && meta.IsStatusConditionTrue(cdds.Status.Conditions, nvapi.ComputeDomainDaemonSetConditionReady)
		}
		if statusDone {
			mark("statusSettled")
		}
		if time.Since(lastLog) > 10*time.Second {
			lastLog = time.Now()
			klog.Infof("%v: %d/%d nodes ready, %d updated, %d registered, %d stale entries, %d cliques disrupted",
				time.Since(start).Round(time.Second), s.ready, s.nodes, s.updated, s.registered, s.stale, len(s.disrupted))
		}
		return r.settled(s) && statusDone, nil
	})
}

func (r *staticRun) update(ctx context.Context) (PhaseResult, error) {
	pr := PhaseResult{Marks: make(map[string]float64)}
	cdds, err := r.getCDDS(ctx)
	if err != nil || cdds == nil {
		return pr, fmt.Errorf("no ComputeDomainDaemonSet to update, run the rollout phase first: %w", err)
	}
	oldHash, err := r.daemonSetHash(ctx, cdds.UID)
	if err != nil {
		return pr, err
	}
	verbosity := 1
	if cdds.Spec.LogVerbosity == 1 {
		verbosity = 2
	}
	patch, err := json.Marshal(map[string]any{"spec": map[string]any{"logVerbosity": verbosity}})
	if err != nil {
		return pr, err
	}
	start := time.Now()
	if _, err := r.cs.nvidia.ResourceV1beta1().ComputeDomainDaemonSets().Patch(ctx, cdds.Name, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
		return pr, fmt.Errorf("error patching ComputeDomainDaemonSet: %w", err)
	}
	var newHash string
	err = poll(ctx, 200*time.Millisecond, 2*time.Minute, func() (bool, error) {
		h, err := r.daemonSetHash(ctx, cdds.UID)
		newHash = h
		return err == nil && h != "" && h != oldHash, err
	})
	if err != nil {
		return pr, fmt.Errorf("waiting for the static IMEX DaemonSet to render the update: %w", err)
	}
	pr.Marks["templateRendered"] = time.Since(start).Seconds()

	// Track each clique's restart, to check the batch size and time it.
	disruptedSince := make(map[string]time.Time)
	var restarts []time.Duration
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
			}
			s := r.snapshot(cdds.UID, newHash)
			now := time.Now()
			pr.MaxDisruptedCliques = max(pr.MaxDisruptedCliques, len(s.disrupted))
			for _, c := range s.disrupted {
				if _, ok := disruptedSince[c]; !ok {
					disruptedSince[c] = now
				}
			}
			for c, since := range disruptedSince {
				if !slices.Contains(s.disrupted, c) {
					restarts = append(restarts, now.Sub(since))
					delete(disruptedSince, c)
				}
			}
		}
	}()
	err = r.waitSettled(ctx, cdds.UID, newHash, start, pr.Marks, false)
	close(done)
	<-finished
	pr.CliqueRestart = summarize(restarts)
	limit := r.maxUnavailable(len(r.cliqueSet()))
	r.result.addCheck(fmt.Sprintf("update restarts at most %d cliques at a time", limit), pr.MaxDisruptedCliques <= limit,
		fmt.Sprintf("%d cliques disrupted at once", pr.MaxDisruptedCliques))
	return pr, err
}

func (r *staticRun) daemonSetHash(ctx context.Context, uid types.UID) (string, error) {
	list, err := r.cs.core.AppsV1().DaemonSets(r.cs.driverNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s", computeDomainDaemonSetLabelKey, uid),
	})
	if err != nil {
		return "", err
	}
	if len(list.Items) != 1 {
		return "", nil
	}
	// The controller annotates the pod template, which its pods inherit.
	return list.Items[0].Spec.Template.Annotations[templateHashKey], nil
}

func (r *staticRun) cliqueSet() map[string]bool {
	set := make(map[string]bool)
	for _, c := range r.cliqueNodes() {
		set[c] = true
	}
	return set
}

func (r *staticRun) maxUnavailable(cliques int) int {
	v := intstr.Parse(r.params.MaxUnavailableCliques)
	n, err := intstr.GetScaledValueFromIntOrPercent(&v, cliques, false)
	if err != nil {
		return 1
	}
	return max(n, 1)
}

// repartition moves nodes of the last clique to a new one, as an NVLink
// repartition does, and back. The controller must restart their daemons so
// that they join the new clique, and drop their old entries.
func (r *staticRun) repartition(ctx context.Context) (PhaseResult, error) {
	pr := PhaseResult{Marks: make(map[string]float64)}
	cdds, err := r.getCDDS(ctx)
	if err != nil || cdds == nil {
		return pr, fmt.Errorf("no ComputeDomainDaemonSet, run the rollout phase first: %w", err)
	}
	cliqueOf := r.cliqueNodes()
	var nodes []string
	for n := range cliqueOf {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)
	last := cliqueOf[nodes[len(nodes)-1]]
	var moved []string
	for _, n := range nodes {
		if cliqueOf[n] == last && len(moved) < r.params.RepartitionNodes {
			moved = append(moved, n)
		}
	}
	target := fmt.Sprintf("%s.repartition", fakeClusterUUID)
	for _, step := range []struct {
		name, clique string
	}{{"moved", target}, {"restored", last}} {
		start := time.Now()
		if err := r.relabel(ctx, moved, step.clique); err != nil {
			return pr, err
		}
		marks := make(map[string]float64)
		hash, _ := r.daemonSetHash(ctx, cdds.UID)
		if err := r.waitSettled(ctx, cdds.UID, hash, start, marks, false); err != nil {
			return pr, fmt.Errorf("%d nodes %s to clique %s: %w", len(moved), step.name, step.clique, err)
		}
		for k, v := range marks {
			pr.Marks[step.name+"."+k] = v
		}
	}
	return pr, nil
}

func (r *staticRun) relabel(ctx context.Context, nodes []string, clique string) error {
	patch, err := json.Marshal(map[string]any{"metadata": map[string]any{"labels": map[string]string{gpuCliqueLabelKey: clique}}})
	if err != nil {
		return err
	}
	for _, n := range nodes {
		if _, err := r.cs.core.CoreV1().Nodes().Patch(ctx, n, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
			return fmt.Errorf("error relabeling Node %s: %w", n, err)
		}
	}
	return nil
}

// nodeChurn deletes the nodes of the first clique, as when they are
// replaced, and adds them back. Their daemons cannot unregister, so the
// controller must remove their stale clique entries.
func (r *staticRun) nodeChurn(ctx context.Context) (PhaseResult, error) {
	pr := PhaseResult{Marks: make(map[string]float64)}
	cdds, err := r.getCDDS(ctx)
	if err != nil || cdds == nil {
		return pr, fmt.Errorf("no ComputeDomainDaemonSet, run the rollout phase first: %w", err)
	}
	cliqueOf := r.cliqueNodes()
	first := ""
	for _, c := range cliqueOf {
		if first == "" || c < first {
			first = c
		}
	}
	var victims []*corev1.Node
	for n, c := range cliqueOf {
		if c != first {
			continue
		}
		node, err := r.nodes.Get(n)
		if err != nil {
			return pr, err
		}
		victims = append(victims, node.DeepCopy())
	}
	hash, _ := r.daemonSetHash(ctx, cdds.UID)
	start := time.Now()
	for _, n := range victims {
		if err := r.cs.core.CoreV1().Nodes().Delete(ctx, n.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return pr, fmt.Errorf("error deleting Node %s: %w", n.Name, err)
		}
	}
	marks := make(map[string]float64)
	if err := r.waitSettled(ctx, cdds.UID, hash, start, marks, false); err != nil {
		return pr, fmt.Errorf("after deleting %d nodes of clique %s: %w", len(victims), first, err)
	}
	for k, v := range marks {
		pr.Marks["deleted."+k] = v
	}
	start = time.Now()
	for _, n := range victims {
		fresh := fakeNode(n.Name, first)
		if _, err := r.cs.core.CoreV1().Nodes().Create(ctx, fresh, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return pr, fmt.Errorf("error recreating Node %s: %w", n.Name, err)
		}
	}
	marks = make(map[string]float64)
	if err := r.waitSettled(ctx, cdds.UID, hash, start, marks, false); err != nil {
		return pr, fmt.Errorf("after recreating %d nodes of clique %s: %w", len(victims), first, err)
	}
	for k, v := range marks {
		pr.Marks["recreated."+k] = v
	}
	return pr, nil
}

func (r *staticRun) deleteCDDS(ctx context.Context) {
	err := r.cs.nvidia.ResourceV1beta1().ComputeDomainDaemonSets().Delete(ctx, nvapi.ComputeDomainDaemonSetName, metav1.DeleteOptions{PropagationPolicy: ptr.To(metav1.DeletePropagationForeground)})
	if err != nil && !apierrors.IsNotFound(err) {
		klog.Warningf("Error deleting ComputeDomainDaemonSet: %v", err)
		return
	}
	err = poll(ctx, time.Second, 10*time.Minute, func() (bool, error) {
		cdds, err := r.getCDDS(ctx)
		if err != nil {
			return false, err
		}
		pods, err := r.pods.List(labels.Everything())
		return cdds == nil && len(pods) == 0, err
	})
	if err != nil {
		klog.Warningf("ComputeDomainDaemonSet or its daemons are not gone: %v", err)
	}
}
