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
	"math/rand/v2"
	"sync"
	"time"

	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/imex"
	"sigs.k8s.io/dra-driver-nvidia-gpu/tests/scale/fakeslices"
)

const (
	gpuClaimTemplateName = "scale-gpus"
	cdNameLabelKey       = "scale.nvidia.com/compute-domain"
	pauseImage           = "registry.k8s.io/pause:3.10"
)

type admissionParams struct {
	ComputeDomains int     `json:"computeDomains"`
	PodsPerCD      int     `json:"podsPerComputeDomain"`
	ChannelsPerCD  int     `json:"channelsPerComputeDomain"`
	GPUsPerPod     int     `json:"gpusPerPod"`
	Namespaces     int     `json:"namespaces"`
	CreateQPS      float64 `json:"createQPS"`
	SameClique     bool    `json:"sameClique"`
	Churn          string  `json:"churn,omitempty"`
	ChurnInterval  string  `json:"churnInterval,omitempty"`
}

func admissionCommand() *cli.Command {
	return &cli.Command{
		Name:  "admission",
		Usage: "Create ComputeDomain workloads and measure scheduling, allocation and channel admission",
		Flags: append(scenarioFlags(),
			&cli.IntFlag{Name: "compute-domains", Value: 50},
			&cli.IntFlag{Name: "pods-per-cd", Value: 4, Usage: "workload pods of each ComputeDomain"},
			&cli.IntFlag{Name: "channels-per-cd", Value: 1, Usage: "workload ResourceClaimTemplates (channels) per ComputeDomain; more than 1 requires channel reservations"},
			&cli.IntFlag{Name: "gpus-per-pod", Value: 4, Usage: "fake GPUs each pod claims, 0 for none; 4 makes pods use whole nodes, as in production"},
			&cli.IntFlag{Name: "namespaces", Value: 10},
			&cli.Float64Flag{Name: "create-qps", Value: 100, Usage: "pod creation rate; keep it above the scheduling rate to measure throughput"},
			&cli.BoolFlag{Name: "same-clique", Usage: "keep the pods of a ComputeDomain in one NVLink clique with pod affinity"},
			&cli.DurationFlag{Name: "churn", Usage: "after the initial burst, replace ComputeDomains for this long"},
			&cli.DurationFlag{Name: "churn-interval", Value: 2 * time.Second, Usage: "how often churn replaces a ComputeDomain"},
		),
		Action: runAdmission,
	}
}

func scenarioFlags() []cli.Flag {
	return append(agentFlags(),
		&cli.BoolFlag{Name: "agent", Value: true, Usage: "run the agent in this process; disable it while scalebench agent runs separately"},
		&cli.StringFlag{Name: "label", Usage: "free-form name of this run in results, for example gate-on"},
		&cli.StringFlag{Name: "output-dir", Value: "results"},
		&cli.DurationFlag{Name: "timeout", Value: 30 * time.Minute},
		&cli.DurationFlag{Name: "cleanup-timeout", Value: 10 * time.Minute, Usage: "how long to wait for the namespaces of the run to go"},
		&cli.BoolFlag{Name: "keep", Usage: "keep the objects of the run"},
	)
}

type admissionRun struct {
	cs     *clients
	runID  string
	params admissionParams
	rec    *recorder

	limiter *rate.Limiter
	mu      sync.Mutex
	next    int
	live    []cdRef
}

type cdRef struct {
	namespace, name string
	templates       []string
}

func runNamespacePrefix(runID string) string {
	return fmt.Sprintf("scale-%s-", runID)
}

func newRunID() string {
	return time.Now().UTC().Format("0102-150405")
}

func runAdmission(c *cli.Context) error {
	ctx, cancel := signalContext(c.Context)
	defer cancel()
	cs, err := newClients(c)
	if err != nil {
		return err
	}
	p := admissionParams{
		ComputeDomains: c.Int("compute-domains"),
		PodsPerCD:      c.Int("pods-per-cd"),
		ChannelsPerCD:  c.Int("channels-per-cd"),
		GPUsPerPod:     c.Int("gpus-per-pod"),
		Namespaces:     max(c.Int("namespaces"), 1),
		CreateQPS:      c.Float64("create-qps"),
		SameClique:     c.Bool("same-clique"),
	}
	if c.Duration("churn") > 0 {
		p.Churn = c.Duration("churn").String()
		p.ChurnInterval = c.Duration("churn-interval").String()
	}
	if p.ChannelsPerCD < 1 || p.PodsPerCD < 1 || p.ComputeDomains < 1 {
		return fmt.Errorf("compute-domains, pods-per-cd and channels-per-cd must be at least 1")
	}

	mode, err := detectDriverMode(ctx, cs)
	if err != nil {
		return err
	}
	if p.ChannelsPerCD > 1 && !mode.Reservations {
		return fmt.Errorf("channels-per-cd=%d requires the ComputeDomainChannelReservations feature gate, the driver runs %s", p.ChannelsPerCD, mode.Name())
	}
	var a *agent
	if c.Bool("agent") {
		a, err = startAgent(ctx, cs, agentOptionsFrom(c))
		if err != nil {
			return err
		}
	}
	nodes, err := checkCapacity(ctx, cs, p, c.Int("gpus-per-node"))
	if err != nil {
		return err
	}

	run := &admissionRun{
		cs:      cs,
		runID:   newRunID(),
		params:  p,
		limiter: rate.NewLimiter(rate.Limit(p.CreateQPS), max(int(p.CreateQPS), 1)),
	}
	run.rec = newRecorder(run.runID)
	result := &Result{
		Scenario: "admission",
		Label:    c.String("label"),
		RunID:    run.runID,
		Mode:     mode,
		Nodes:    nodes,
		Params:   p,
	}
	klog.Infof("Run %s: %d ComputeDomains x %d pods on %d fake nodes, mode %s", run.runID, p.ComputeDomains, p.PodsPerCD, nodes, mode.Name())

	// Runs that end early still clean up; the others measure their teardown
	// before writing the result.
	cleaned := c.Bool("keep")
	defer func() {
		if !cleaned {
			run.cleanup(context.WithoutCancel(ctx), c.Duration("cleanup-timeout"))
		}
	}()
	if err := run.createNamespaces(ctx); err != nil {
		return err
	}
	if mode.Lifecycle == imex.DaemonLifecycleStatic {
		cdds, err := cs.nvidia.ResourceV1beta1().ComputeDomainDaemonSets().Get(ctx, nvapi.ComputeDomainDaemonSetName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("error getting the ComputeDomainDaemonSet, run the static scenario first: %w", err)
		}
		run.rec.staticDomain = string(cdds.UID)
	}
	if err := run.rec.start(ctx, cs); err != nil {
		return err
	}

	sc := &scraper{cs: cs}
	before := sc.scrapeAll(ctx, sourceAPIServer, sourceScheduler, sourceController)
	sampler := &gaugeSampler{scraper: sc}
	samplerCtx, stopSampler := context.WithCancel(ctx)
	defer stopSampler()
	go sampler.run(samplerCtx, 5*time.Second)
	result.Start = time.Now()

	timeoutCtx, cancelTimeout := context.WithTimeout(ctx, c.Duration("timeout"))
	defer cancelTimeout()
	var templateLatency []time.Duration
	err = run.burst(timeoutCtx, &templateLatency)
	if err == nil {
		err = run.waitBound(timeoutCtx)
	}
	if err == nil && c.Duration("churn") > 0 {
		err = run.churn(timeoutCtx, c.Duration("churn"), c.Duration("churn-interval"), &templateLatency)
		if err == nil {
			err = run.waitBound(timeoutCtx)
		}
	}
	result.End = time.Now()
	stopSampler()

	after := sc.scrapeAll(context.WithoutCancel(ctx), sourceAPIServer, sourceScheduler, sourceController)
	result.Metrics = diffMetrics(before, after)
	result.Gauges = sampler.results()
	result.Latencies, result.Outcome = run.rec.analyze()
	result.TemplateLatency = summarize(templateLatency)
	if a != nil {
		st := a.stats()
		result.Agent = &st
	}
	if err != nil {
		result.Error = err.Error()
	}
	if !cleaned {
		cleaned = true
		result.Teardown = run.cleanup(context.WithoutCancel(ctx), c.Duration("cleanup-timeout"))
	}
	if werr := result.write(c.String("output-dir")); werr != nil {
		return werr
	}
	return err
}

// checkCapacity fails early when the fake nodes cannot fit all pods, which
// would otherwise leave pods pending until the timeout.
func checkCapacity(ctx context.Context, cs *clients, p admissionParams, gpusPerNode int) (int, error) {
	nodes, err := cs.core.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: fakeNodeLabelKey})
	if err != nil {
		return 0, err
	}
	ready := 0
	for i := range nodes.Items {
		if nodeReady(&nodes.Items[i]) {
			ready++
		}
	}
	if ready == 0 {
		return 0, fmt.Errorf("no Ready fake nodes, run `scalebench nodes up` first")
	}
	pods := p.ComputeDomains * p.PodsPerCD
	if p.GPUsPerPod > 0 {
		if gpusPerNode < p.GPUsPerPod {
			return 0, fmt.Errorf("pods claim %d GPUs but nodes have %d", p.GPUsPerPod, gpusPerNode)
		}
		if fit := ready * (gpusPerNode / p.GPUsPerPod); fit < pods {
			return 0, fmt.Errorf("%d pods of %d GPUs do not fit on %d nodes of %d GPUs, add nodes or lower --compute-domains", pods, p.GPUsPerPod, ready, gpusPerNode)
		}
	}
	return ready, nil
}

func (r *admissionRun) namespace(i int) string {
	return fmt.Sprintf("%s%d", runNamespacePrefix(r.runID), i%r.params.Namespaces)
}

func (r *admissionRun) createNamespaces(ctx context.Context) error {
	for i := range r.params.Namespaces {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: r.namespace(i), Labels: map[string]string{scaleLabelKey: r.runID}}}
		if _, err := r.cs.core.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("error creating Namespace %s: %w", ns.Name, err)
		}
		if r.params.GPUsPerPod == 0 {
			continue
		}
		rct := &resourceapi.ResourceClaimTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: gpuClaimTemplateName, Namespace: ns.Name},
			Spec: resourceapi.ResourceClaimTemplateSpec{Spec: resourceapi.ResourceClaimSpec{Devices: resourceapi.DeviceClaim{
				Requests: []resourceapi.DeviceRequest{{
					Name: "gpus",
					Exactly: &resourceapi.ExactDeviceRequest{
						DeviceClassName: fakeslices.GPUDeviceClassName,
						AllocationMode:  resourceapi.DeviceAllocationModeExactCount,
						Count:           int64(r.params.GPUsPerPod),
					},
				}},
			}}},
		}
		if _, err := r.cs.core.ResourceV1().ResourceClaimTemplates(ns.Name).Create(ctx, rct, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("error creating ResourceClaimTemplate %s/%s: %w", ns.Name, rct.Name, err)
		}
	}
	return nil
}

// burst creates all ComputeDomains, then their pods at the create rate.
func (r *admissionRun) burst(ctx context.Context, templateLatency *[]time.Duration) error {
	cds := make([]cdRef, r.params.ComputeDomains)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(createWorkers)
	var mu sync.Mutex
	for i := range cds {
		g.Go(func() error {
			cd, latency, err := r.createComputeDomain(gctx)
			if err != nil {
				return err
			}
			mu.Lock()
			cds[i] = cd
			*templateLatency = append(*templateLatency, latency)
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	klog.Infof("Created %d ComputeDomains and their workload ResourceClaimTemplates", len(cds))
	g, gctx = errgroup.WithContext(ctx)
	g.SetLimit(createWorkers)
	for _, cd := range cds {
		for j := range r.params.PodsPerCD {
			g.Go(func() error { return r.createPod(gctx, cd, j) })
		}
	}
	return g.Wait()
}

// createComputeDomain creates a ComputeDomain and waits for the controller to
// create its workload ResourceClaimTemplates, returning how long that took.
func (r *admissionRun) createComputeDomain(ctx context.Context) (cdRef, time.Duration, error) {
	r.mu.Lock()
	i := r.next
	r.next++
	r.mu.Unlock()
	cd := cdRef{namespace: r.namespace(i), name: fmt.Sprintf("cd-%d", i)}
	spec := nvapi.ComputeDomainSpec{}
	for ch := range r.params.ChannelsPerCD {
		cd.templates = append(cd.templates, fmt.Sprintf("%s-ch%d", cd.name, ch))
	}
	if len(cd.templates) == 1 {
		spec.Channel = &nvapi.ComputeDomainChannelSpec{ResourceClaimTemplate: nvapi.ComputeDomainResourceClaimTemplate{Name: cd.templates[0]}}
	} else {
		for _, t := range cd.templates {
			spec.Channels = append(spec.Channels, nvapi.ComputeDomainReservedChannelSpec{ResourceClaimTemplate: nvapi.ComputeDomainResourceClaimTemplate{Name: t}})
		}
	}
	obj := &nvapi.ComputeDomain{
		ObjectMeta: metav1.ObjectMeta{Name: cd.name, Namespace: cd.namespace, Labels: map[string]string{scaleLabelKey: r.runID}},
		Spec:       spec,
	}
	start := time.Now()
	if _, err := r.cs.nvidia.ResourceV1beta1().ComputeDomains(cd.namespace).Create(ctx, obj, metav1.CreateOptions{}); err != nil {
		return cd, 0, fmt.Errorf("error creating ComputeDomain %s/%s: %w", cd.namespace, cd.name, err)
	}
	err := poll(ctx, 200*time.Millisecond, 5*time.Minute, func() (bool, error) {
		for _, t := range cd.templates {
			_, err := r.cs.core.ResourceV1().ResourceClaimTemplates(cd.namespace).Get(ctx, t, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
		}
		return true, nil
	})
	if err != nil {
		return cd, 0, fmt.Errorf("waiting for the workload ResourceClaimTemplates of ComputeDomain %s/%s: %w", cd.namespace, cd.name, err)
	}
	r.mu.Lock()
	r.live = append(r.live, cd)
	r.mu.Unlock()
	return cd, time.Since(start), nil
}

func (r *admissionRun) createPod(ctx context.Context, cd cdRef, j int) error {
	if err := r.limiter.Wait(ctx); err != nil {
		return err
	}
	template := cd.templates[j%len(cd.templates)]
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-p%d", cd.name, j),
			Namespace: cd.namespace,
			Labels:    map[string]string{scaleLabelKey: r.runID, cdNameLabelKey: cd.name},
		},
		Spec: corev1.PodSpec{
			NodeSelector:                  map[string]string{nodeTypeLabelKey: nodeTypeLabelValue},
			Tolerations:                   []corev1.Toleration{{Key: kwokNodeTaintKey, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}},
			TerminationGracePeriodSeconds: ptr.To[int64](0),
			Containers: []corev1.Container{{
				Name:      "workload",
				Image:     pauseImage,
				Resources: corev1.ResourceRequirements{Claims: []corev1.ResourceClaim{{Name: "channel"}}},
			}},
			ResourceClaims: []corev1.PodResourceClaim{{Name: "channel", ResourceClaimTemplateName: ptr.To(template)}},
		},
	}
	if r.params.GPUsPerPod > 0 {
		pod.Spec.Containers[0].Resources.Claims = append(pod.Spec.Containers[0].Resources.Claims, corev1.ResourceClaim{Name: "gpus"})
		pod.Spec.ResourceClaims = append(pod.Spec.ResourceClaims, corev1.PodResourceClaim{Name: "gpus", ResourceClaimTemplateName: ptr.To(gpuClaimTemplateName)})
	}
	if r.params.SameClique {
		pod.Spec.Affinity = &corev1.Affinity{PodAffinity: &corev1.PodAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
				LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{cdNameLabelKey: cd.name}},
				TopologyKey:   gpuCliqueLabelKey,
			}},
		}}
	}
	if _, err := r.cs.core.CoreV1().Pods(cd.namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("error creating Pod %s/%s: %w", cd.namespace, pod.Name, err)
	}
	return nil
}

func (r *admissionRun) waitBound(ctx context.Context) error {
	start := time.Now()
	return poll(ctx, 5*time.Second, 24*time.Hour, func() (bool, error) {
		created, bound := r.rec.counts()
		unbound := r.rec.unboundPods()
		_, outcome := r.rec.analyze()
		klog.Infof("%v: %d/%d pods bound, %d channel rejections", time.Since(start).Round(time.Second), bound, created, outcome.Rejections)
		return unbound == 0 && created > 0, nil
	})
}

// churn replaces a random ComputeDomain and its pods every interval, so that
// channels are released and reused while pods schedule.
func (r *admissionRun) churn(ctx context.Context, duration, interval time.Duration, templateLatency *[]time.Duration) error {
	klog.Infof("Churning ComputeDomains every %v for %v", interval, duration)
	deadline := time.Now().Add(duration)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(createWorkers)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		r.mu.Lock()
		if len(r.live) == 0 {
			r.mu.Unlock()
			continue
		}
		victim := rand.IntN(len(r.live))
		old := r.live[victim]
		r.live = append(r.live[:victim], r.live[victim+1:]...)
		r.mu.Unlock()
		g.Go(func() error {
			if err := r.deleteComputeDomain(gctx, old); err != nil {
				return err
			}
			cd, latency, err := r.createComputeDomain(gctx)
			if err != nil {
				return err
			}
			r.mu.Lock()
			*templateLatency = append(*templateLatency, latency)
			r.mu.Unlock()
			for j := range r.params.PodsPerCD {
				if err := r.createPod(gctx, cd, j); err != nil {
					return err
				}
			}
			return nil
		})
	}
	return g.Wait()
}

func (r *admissionRun) deleteComputeDomain(ctx context.Context, cd cdRef) error {
	err := r.cs.core.CoreV1().Pods(cd.namespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{LabelSelector: fmt.Sprintf("%s=%s", cdNameLabelKey, cd.name)})
	if err != nil {
		return fmt.Errorf("error deleting pods of ComputeDomain %s/%s: %w", cd.namespace, cd.name, err)
	}
	err = r.cs.nvidia.ResourceV1beta1().ComputeDomains(cd.namespace).Delete(ctx, cd.name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("error deleting ComputeDomain %s/%s: %w", cd.namespace, cd.name, err)
	}
	return nil
}

// cleanup deletes the namespaces of the run and waits for them to go, so
// that the next run starts from an idle cluster, and reports how long that
// took.
func (r *admissionRun) cleanup(ctx context.Context, timeout time.Duration) *Teardown {
	klog.Infof("Deleting the namespaces of run %s", r.runID)
	for i := range r.params.Namespaces {
		err := r.cs.core.CoreV1().Namespaces().Delete(ctx, r.namespace(i), metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			klog.Warningf("Error deleting Namespace %s: %v", r.namespace(i), err)
		}
	}
	start := time.Now()
	err := poll(ctx, 2*time.Second, timeout, func() (bool, error) {
		list, err := r.cs.core.CoreV1().Namespaces().List(ctx, metav1.ListOptions{LabelSelector: fmt.Sprintf("%s=%s", scaleLabelKey, r.runID)})
		if err != nil {
			return false, err
		}
		return len(list.Items) == 0, nil
	})
	t := &Teardown{Seconds: time.Since(start).Seconds(), Complete: err == nil}
	if err != nil {
		klog.Warningf("Namespaces of run %s are not gone: %v", r.runID, err)
	} else {
		klog.Infof("Namespaces of run %s are gone after %.0fs", r.runID, t.Seconds)
	}
	return t
}
