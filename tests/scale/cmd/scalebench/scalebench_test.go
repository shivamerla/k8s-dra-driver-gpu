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
	"strings"
	"testing"
	"time"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/ptr"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/imex"
	nvfake "sigs.k8s.io/dra-driver-nvidia-gpu/pkg/nvidia.com/clientset/versioned/fake"
	"sigs.k8s.io/dra-driver-nvidia-gpu/tests/scale/fakeslices"
)

func controllerDeployment(env ...corev1.EnvVar) *appsv1.Deployment {
	return &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
		Containers: []corev1.Container{{Name: "compute-domain", Image: "driver:dev", Env: env}},
	}}}}
}

func TestModeFromDeployment(t *testing.T) {
	tests := map[string]struct {
		env  []corev1.EnvVar
		want string
	}{
		"default": {want: "per-cd"},
		"per-cd with reservations": {
			env:  []corev1.EnvVar{{Name: "FEATURE_GATES", Value: "ComputeDomainChannelReservations=true,"}},
			want: "per-cd-reservations",
		},
		"static": {
			env: []corev1.EnvVar{
				{Name: "IMEX_DAEMON_LIFECYCLE", Value: "static"},
				{Name: "FEATURE_GATES", Value: "ComputeDomainDaemonSet=true,ComputeDomainChannelReservations=true,"},
			},
			want: "static",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			mode, err := modeFromDeployment(controllerDeployment(tc.env...))
			require.NoError(t, err)
			assert.Equal(t, tc.want, mode.Name())
			assert.Equal(t, "driver:dev", mode.Image)
		})
	}

	_, err := modeFromDeployment(controllerDeployment(corev1.EnvVar{Name: "FEATURE_GATES", Value: "Broken"}))
	assert.Error(t, err)
	_, err = modeFromDeployment(controllerDeployment(corev1.EnvVar{Name: "IMEX_DAEMON_LIFECYCLE", Value: "hostManaged"}))
	assert.Error(t, err, "the scale tests do not cover host-managed IMEX daemons")
}

func TestHistogramQuantile(t *testing.T) {
	// 100 observations: 50 at or below 0.1s, 40 more by 1s, 10 more by 10s.
	h := &histogram{count: 100, buckets: map[float64]float64{0.1: 50, 1: 90, 10: 100}}
	assert.InDelta(t, 0.1, h.quantile(0.5), 1e-9)
	assert.InDelta(t, 1, h.quantile(0.9), 1e-9)
	assert.InDelta(t, 5.5, h.quantile(0.95), 1e-9)
}

func TestHistogramMinus(t *testing.T) {
	before := &histogram{count: 10, sum: 1, buckets: map[float64]float64{1: 10}}
	after := &histogram{count: 30, sum: 5, buckets: map[float64]float64{1: 25}}
	d := after.minus(before)
	assert.InDelta(t, 20, d.count, 1e-9)
	assert.InDelta(t, 4, d.sum, 1e-9)
	assert.InDelta(t, 15, d.buckets[1], 1e-9)

	// A restart reset the histogram, so after is the change.
	reset := (&histogram{count: 5, buckets: map[float64]float64{1: 5}}).minus(before)
	assert.InDelta(t, 5, reset.count, 1e-9)
}

const schedulerMetrics = `# TYPE scheduler_schedule_attempts_total counter
scheduler_schedule_attempts_total{profile="default-scheduler",result="scheduled"} %d
scheduler_schedule_attempts_total{profile="default-scheduler",result="unschedulable"} 3
# TYPE scheduler_framework_extension_point_duration_seconds histogram
scheduler_framework_extension_point_duration_seconds_bucket{extension_point="PreBind",profile="default-scheduler",status="Success",le="0.1"} %d
scheduler_framework_extension_point_duration_seconds_bucket{extension_point="PreBind",profile="default-scheduler",status="Success",le="+Inf"} %d
scheduler_framework_extension_point_duration_seconds_sum{extension_point="PreBind",profile="default-scheduler",status="Success"} 1
scheduler_framework_extension_point_duration_seconds_count{extension_point="PreBind",profile="default-scheduler",status="Success"} %d
`

func parseScrape(t *testing.T, scheduled, prebind int) scrape {
	t.Helper()
	text := fmt.Sprintf(schedulerMetrics, scheduled, prebind, prebind, prebind)
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(strings.NewReader(text))
	require.NoError(t, err)
	return scrape{sourceScheduler: families}
}

func TestDiffMetrics(t *testing.T) {
	results := diffMetrics(parseScrape(t, 10, 10), parseScrape(t, 110, 60))
	byKey := make(map[string]MetricResult)
	for _, r := range results {
		byKey[r.Key()] = r
	}
	scheduled := byKey["scheduler_schedule_attempts_total,result=scheduled"]
	require.NotNil(t, scheduled.Delta)
	assert.InDelta(t, 100, *scheduled.Delta, 1e-9)
	// Unchanged series are left out.
	assert.NotContains(t, byKey, "scheduler_schedule_attempts_total,result=unschedulable")
	prebind := byKey["scheduler_framework_extension_point_duration_seconds,extension_point=PreBind"]
	require.NotNil(t, prebind.Count)
	assert.InDelta(t, 50, *prebind.Count, 1e-9)
}

func TestSummarize(t *testing.T) {
	var ds []time.Duration
	for i := 1; i <= 100; i++ {
		ds = append(ds, time.Duration(i)*time.Millisecond)
	}
	s := summarize(ds)
	assert.Equal(t, 100, s.Count)
	assert.InDelta(t, 0.050, s.P50, 1e-9)
	assert.InDelta(t, 0.099, s.P99, 1e-9)
	assert.InDelta(t, 0.100, s.Max, 1e-9)
	assert.Equal(t, Summary{}, summarize(nil))
}

func TestRecorderBreaksDownBindingConditions(t *testing.T) {
	rec := newRecorder("run")
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "scale-run-0", UID: "pod-uid"}}
	rec.onPod(pod)

	claim := &resourceapi.ResourceClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "c", Namespace: "scale-run-0", UID: "claim-uid",
		OwnerReferences: []metav1.OwnerReference{{Kind: "Pod", UID: types.UID("pod-uid"), Controller: ptr.To(true)}},
	}}
	result := resourceapi.DeviceRequestAllocationResult{
		Request: "channel", Driver: fakeslices.ComputeDomainDriverName, Pool: "node", Device: imex.ChannelDeviceName,
		BindingConditions: []string{imex.ChannelAdmittedCondition},
	}
	claim.Status.Allocation = &resourceapi.AllocationResult{Devices: resourceapi.DeviceAllocationResult{Results: []resourceapi.DeviceRequestAllocationResult{result}}}
	rec.onClaim(claim)

	// Rejected once, then admitted after the scheduler allocates again.
	rejected := claim.DeepCopy()
	rejected.Status.Devices = []resourceapi.AllocatedDeviceStatus{{
		Driver: result.Driver, Pool: result.Pool, Device: result.Device,
		Conditions: []metav1.Condition{{Type: imex.ChannelRejectedCondition, Status: metav1.ConditionTrue, Reason: "NoFreeChannel"}},
	}}
	rec.onClaim(rejected)
	rec.onClaim(rejected)
	admitted := claim.DeepCopy()
	admitted.Status.Devices = []resourceapi.AllocatedDeviceStatus{{
		Driver: result.Driver, Pool: result.Pool, Device: result.Device,
		Conditions: []metav1.Condition{{Type: imex.ChannelAdmittedCondition, Status: metav1.ConditionTrue}},
	}}
	rec.onClaim(admitted)

	bound := pod.DeepCopy()
	bound.Spec.NodeName = "node"
	rec.onPod(bound)

	latencies, outcome := rec.analyze()
	assert.Equal(t, 1, outcome.Bound)
	assert.Equal(t, 1, outcome.WaitedOnAdmission)
	assert.Equal(t, 1, outcome.Rejections)
	assert.Equal(t, map[string]int{"NoFreeChannel": 1}, outcome.RejectionReasons)
	assert.Equal(t, 1, latencies.AllocatedToAdmitted.Count)
	assert.Equal(t, 1, latencies.AdmittedToBound.Count)

	// Claims of other runs are ignored.
	other := claim.DeepCopy()
	other.Namespace = "scale-other-0"
	other.UID = "other"
	rec.onClaim(other)
	assert.NotContains(t, rec.claimOwner, types.UID("other"))
}

func TestSummaryRows(t *testing.T) {
	start := time.Unix(0, 0)
	r := &Result{
		Scenario: "admission",
		Nodes:    1008,
		Start:    start,
		End:      start.Add(10 * time.Second),
		Outcome:  Outcome{Pods: 500, Bound: 500, Throughput: 50},
		Metrics: []MetricResult{
			{Source: sourceController, Name: "process_cpu_seconds_total", Delta: ptrTo(5)},
			{Source: sourceScheduler, Name: "scheduler_schedule_attempts_total", Labels: map[string]string{"result": "unschedulable"}, Delta: ptrTo(2)},
		},
		Gauges: []MetricResult{
			{Source: sourceController, Name: "process_resident_memory_bytes", Max: ptrTo(64 << 20)},
		},
	}
	admission, _ := summaryRows([]*Result{r})
	require.Len(t, admission.rows, 1)
	row := make(map[string]string)
	for i, h := range admission.header {
		row[h] = admission.rows[0][i]
	}
	assert.Equal(t, "1008", row["nodes"])
	assert.Equal(t, "0.50", row["controller cores"])
	assert.Equal(t, "64", row["controller MiB"])
	assert.Equal(t, "2", row["unschedulable attempts"])
	// Unschedulable attempts fail the run.
	assert.Equal(t, "FAIL", row["ok"])
}

func TestDaemonSetHash(t *testing.T) {
	ds := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "imex",
			Namespace:   "driver",
			Labels:      map[string]string{computeDomainDaemonSetLabelKey: "uid"},
			Annotations: map[string]string{"resource.nvidia.com/image": "pinned"},
		},
		Spec: appsv1.DaemonSetSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{templateHashKey: "abc"}},
			},
		},
	}
	r := &staticRun{cs: &clients{core: fake.NewClientset(ds), driverNamespace: "driver"}}
	hash, err := r.daemonSetHash(context.Background(), "uid")
	require.NoError(t, err)
	assert.Equal(t, "abc", hash)
}

func TestScalingBottleneck(t *testing.T) {
	run := func(nodes int, admissionP99 float64) *Result {
		return &Result{
			Scenario: "admission",
			Label:    "ppcd4-fill50",
			Mode:     driverMode{Lifecycle: imex.DaemonLifecycleStatic, Reservations: true, KubeAPIQPS: "100"},
			Nodes:    nodes,
			Outcome:  Outcome{Pods: nodes / 2, Bound: nodes / 2, Throughput: 90},
			Latencies: PodLatencies{
				CreateToBound:       Summary{Count: 1, P50: 2, P99: 1 + admissionP99},
				CreateToAllocated:   Summary{Count: 1, P50: 0.2, P99: 0.5},
				AllocatedToAdmitted: Summary{Count: 1, P50: 1.5, P99: admissionP99},
				AdmittedToBound:     Summary{Count: 1, P50: 0.1, P99: 0.2},
			},
		}
	}
	small, large := run(1008, 1), run(5040, 6)

	assert.Equal(t, "admission (75%)", slowestStage(large.Latencies))
	assert.Contains(t, bottleneckFlags(large), "controller client QPS (90 pods/s at QPS 100)")

	line := growth(small, large)
	assert.Contains(t, line, "admission x6.0")
	assert.Contains(t, line, "admission grows at least as fast as the cluster")

	var b strings.Builder
	writeScaling(&b, []*Result{large, small})
	assert.Contains(t, b.String(), "### static ppcd4-fill50")
}

func TestBootstrap(t *testing.T) {
	start := time.Unix(1000, 0)
	r := newRecorder("run")
	r.staticDomain = "cdds"
	// The daemon on node-a was Ready before the pod was bound, the one on
	// node-b one second after.
	r.daemonReady[daemonKey("cdds", "node-a")] = start
	r.daemonReady[daemonKey("cdds", "node-b")] = start.Add(3 * time.Second)
	r.pods["a"] = &podTimeline{created: start, bound: start.Add(2 * time.Second), node: "node-a"}
	r.pods["b"] = &podTimeline{created: start, bound: start.Add(2 * time.Second), node: "node-b"}
	r.pods["c"] = &podTimeline{created: start, bound: start.Add(2 * time.Second), node: "node-c"}

	l, out := r.analyze()
	assert.Equal(t, 2, out.PrepareReady)
	assert.InDelta(t, 3, l.CreateToPrepareReady.Max, 1e-9)
	assert.InDelta(t, 1, l.BoundToPrepareReady.Max, 1e-9)

	// Without binding conditions, a pod binds before its channel is
	// assigned, and Prepare also waits for the channel.
	shared := func() *podTimeline {
		return &podTimeline{created: start, bound: start.Add(2 * time.Second), node: "node-a", channel: channelTimeline{
			allocated: start.Add(time.Second), needsChannel: true,
		}}
	}
	r.pods["d"] = shared()
	r.pods["d"].channel.admitted = start.Add(4 * time.Second)
	r.pods["e"] = shared()
	l, out = r.analyze()
	assert.Equal(t, 3, out.PrepareReady, "a pod without a channel cannot prepare")
	assert.InDelta(t, 4, l.CreateToPrepareReady.Max, 1e-9)
	assert.InDelta(t, 2, l.BoundToPrepareReady.Max, 1e-9)
	assert.Equal(t, 0, out.WaitedOnAdmission)
	assert.Equal(t, 1, l.AllocatedToAdmitted.Count, "the admission is measured without binding conditions")
	assert.Equal(t, 0, l.AdmittedToBound.Count)

	result := func(mode driverMode, p50 float64) *Result {
		return &Result{
			Scenario: "admission", Label: "ppcd4-fill95", Nodes: 1008, Mode: mode,
			Outcome:   Outcome{Pods: 957, PrepareReady: 957},
			Latencies: PodLatencies{CreateToPrepareReady: Summary{Count: 957, P50: p50, P99: p50 * 2}},
		}
	}
	var b strings.Builder
	writeBootstrap(&b, []*Result{
		result(driverMode{Reservations: true}, 4),
		result(driverMode{Lifecycle: imex.DaemonLifecycleStatic, Reservations: true}, 1),
	})
	assert.Contains(t, b.String(), "| ppcd4-fill95 | 1008 | 957/957 | 4.000 | 8.000 |")
	assert.Contains(t, b.String(), "| +3.000 | +6.000 |")
}

func TestSingleComputeDomain(t *testing.T) {
	result := func(mode driverMode, nodes, cds int, teardown *Teardown) *Result {
		return &Result{
			Scenario: "admission", Nodes: nodes, Mode: mode, Teardown: teardown,
			Params:    map[string]any{"computeDomains": float64(cds)},
			Outcome:   Outcome{Pods: nodes, Bound: nodes},
			Latencies: PodLatencies{CreateToPrepareReady: Summary{Count: nodes, Max: 9}},
		}
	}
	var b strings.Builder
	writeSingleComputeDomain(&b, []*Result{
		result(driverMode{Reservations: true}, 10080, 1, &Teardown{Seconds: 1800, Complete: false}),
		result(driverMode{Lifecycle: imex.DaemonLifecycleStatic, Reservations: true}, 1008, 1, &Teardown{Seconds: 12.4, Complete: true}),
		result(driverMode{Reservations: true}, 1008, 50, nil),
	})
	out := b.String()
	assert.Contains(t, out, "| per-cd-reservations | 10080 | 10080/10080 |")
	assert.Contains(t, out, "| >1800 |", "an unfinished teardown is a lower bound")
	assert.Contains(t, out, "| static | 1008 | 1008/1008 |")
	assert.Contains(t, out, "| 12 |")
	rows := strings.Count(out, "\n| per-cd-reservations |") + strings.Count(out, "\n| static |")
	assert.Equal(t, 2, rows, "runs of many ComputeDomains are left out")

	b.Reset()
	writeSingleComputeDomain(&b, []*Result{result(driverMode{}, 1008, 50, nil)})
	assert.Empty(t, b.String())
}

// A replaced daemon pod that unregisters after its successor registered on
// the same node must leave the successor's entry.
func TestDaemonSimulatorReplacedPod(t *testing.T) {
	ctx := context.Background()
	nodes := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	require.NoError(t, nodes.Add(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}}))
	s := &daemonSimulator{
		cs:         &clients{nvidia: nvfake.NewSimpleClientset(), driverNamespace: "driver"},
		nodes:      corelisters.NewNodeLister(nodes),
		registered: make(map[string]registration),
		owners:     make(map[registration]string),
	}
	daemon := func(name, ip string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name), Labels: map[string]string{computeDomainDaemonSetLabelKey: "cdds"}},
			Spec:       corev1.PodSpec{NodeName: "node-a"},
			Status:     corev1.PodStatus{PodIP: ip},
		}
	}
	reg := registration{clique: "cdds.c0", node: "node-a"}
	for _, pod := range []*corev1.Pod{daemon("old", "10.0.0.1"), daemon("new", "10.0.0.2")} {
		require.NoError(t, s.register(ctx, pod, reg.clique, "c0"))
		s.registered[pod.Name] = reg
		s.owners[reg] = pod.Name
	}

	require.NoError(t, s.unregister(ctx, "old"))
	clique, err := s.cs.nvidia.ResourceV1beta1().ComputeDomainCliques("driver").Get(ctx, reg.clique, metav1.GetOptions{})
	require.NoError(t, err)
	require.Len(t, clique.Daemons, 1)
	assert.Equal(t, "10.0.0.2", clique.Daemons[0].IPAddress)
	assert.Equal(t, nvapi.ComputeDomainStatusReady, clique.Daemons[0].Status)

	require.NoError(t, s.unregister(ctx, "new"))
	clique, err = s.cs.nvidia.ResourceV1beta1().ComputeDomainCliques("driver").Get(ctx, reg.clique, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Empty(t, clique.Daemons)
}

func TestDaemonSimulatorReregistersMissing(t *testing.T) {
	s := &daemonSimulator{
		queue:      workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]()),
		registered: make(map[string]registration),
		owners:     make(map[registration]string),
		rejoining:  make(map[string]bool),
	}
	defer s.queue.ShutDown()
	a := registration{clique: "cdds.c0", node: "node-a"}
	b := registration{clique: "cdds.c0", node: "node-b"}
	other := registration{clique: "cdds.c1", node: "node-c"}
	for name, reg := range map[string]registration{"pod-a": a, "pod-b": b, "pod-c": other} {
		s.registered[name] = reg
		s.owners[reg] = name
	}
	clique := &nvapi.ComputeDomainClique{
		ObjectMeta: metav1.ObjectMeta{Name: "cdds.c0"},
		Daemons:    []*nvapi.ComputeDomainDaemonInfo{{NodeName: "node-b"}},
	}

	s.reregisterMissing(clique, false)
	assert.Equal(t, 1, s.queue.Len(), "only the daemon whose entry is gone registers again")
	assert.NotContains(t, s.registered, "pod-a")
	assert.Contains(t, s.registered, "pod-b")
	assert.True(t, s.rejoining["pod-a"])

	s.reregisterMissing(cache.DeletedFinalStateUnknown{Obj: clique}, true)
	assert.Equal(t, 2, s.queue.Len(), "all daemons of a deleted clique register again")
	assert.NotContains(t, s.registered, "pod-b")
	assert.Contains(t, s.registered, "pod-c", "other cliques are left alone")
	assert.Equal(t, int64(2), s.Reregistered.Load())
}
