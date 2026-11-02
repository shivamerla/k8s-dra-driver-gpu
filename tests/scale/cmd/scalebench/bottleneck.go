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
	"cmp"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
)

// stage is one part of a pod's way from creation to binding.
type stage struct {
	name     string
	p50, p99 float64
}

// stages splits create -> bound into scheduling, the controller's admission
// (binding conditions only) and binding, then the wait for Prepare.
func stages(l PodLatencies) []stage {
	s := []stage{{"scheduling", l.CreateToAllocated.P50, l.CreateToAllocated.P99}}
	if l.AdmittedToBound.Count > 0 {
		s = append(s,
			stage{"admission", l.AllocatedToAdmitted.P50, l.AllocatedToAdmitted.P99},
			stage{"binding", l.AdmittedToBound.P50, l.AdmittedToBound.P99})
	} else {
		s = append(s, stage{"binding", l.AllocatedToBound.P50, l.AllocatedToBound.P99})
	}
	if l.BoundToPrepareReady.Count > 0 {
		s = append(s, stage{"prepare wait", l.BoundToPrepareReady.P50, l.BoundToPrepareReady.P99})
	}
	return s
}

// slowestStage names the stage that takes most of a typical pod's time.
func slowestStage(l PodLatencies) string {
	total := l.CreateToBound.P50
	if l.CreateToPrepareReady.Count > 0 {
		total = l.CreateToPrepareReady.P50
	}
	if total <= 0 {
		return ""
	}
	slowest := slices.MaxFunc(stages(l), func(a, b stage) int { return cmp.Compare(a.p50, b.p50) })
	return fmt.Sprintf("%s (%.0f%%)", slowest.name, 100*min(slowest.p50/total, 1))
}

// bottleneckFlags names what limited a run, by thresholds that separate a
// saturated component from one with room.
func bottleneckFlags(r *Result) []string {
	var flags []string
	o, l := r.Outcome, r.Latencies
	if o.Bound < o.Pods {
		flags = append(flags, fmt.Sprintf("%d pods not bound", o.Pods-o.Bound))
	}
	if o.Rejections > 0 {
		var reasons []string
		for _, reason := range sortedKeys(o.RejectionReasons) {
			reasons = append(reasons, fmt.Sprintf("%s=%d", reason, o.RejectionReasons[reason]))
		}
		flags = append(flags, "channel rejections "+strings.Join(reasons, ","))
	}
	if n := metricDelta(r.Metrics, sourceScheduler, "scheduler_schedule_attempts_total", map[string]string{"result": "unschedulable"}); n > 0 {
		flags = append(flags, fmt.Sprintf("%.0f unschedulable attempts", n))
	}
	// Admission writes one claim status each, at the controller's client QPS.
	if qps, err := strconv.ParseFloat(r.Mode.KubeAPIQPS, 64); err == nil && qps > 0 && l.AllocatedToAdmitted.Count > 0 && o.Throughput >= 0.8*qps {
		flags = append(flags, fmt.Sprintf("controller client QPS (%.0f pods/s at QPS %.0f)", o.Throughput, qps))
	}
	if depth := gaugePeak(r.Gauges, sourceController, "workqueue_depth", map[string]string{"name": "compute-domain-claim-admission"}); depth >= 100 {
		flags = append(flags, fmt.Sprintf("admission queue backlog %.0f", depth))
	}
	if l.CreateToAllocated.P50 >= 1 {
		flags = append(flags, fmt.Sprintf("scheduler backlog (pending peak %.0f)", gaugePeak(r.Gauges, sourceScheduler, "scheduler_pending_pods", nil)))
	}
	apfRejected := metricDelta(r.Metrics, sourceAPIServer, "apiserver_flowcontrol_rejected_requests_total", nil)
	apfWait := metricP99(r.Metrics, sourceAPIServer, "apiserver_flowcontrol_request_wait_duration_seconds", nil)
	if apfRejected > 0 || apfWait >= 0.1 {
		flags = append(flags, fmt.Sprintf("API server throttling (%.0f rejected, wait p99 %.2fs)", apfRejected, apfWait))
	}
	if p99 := metricP99(r.Metrics, sourceAPIServer, "etcd_request_duration_seconds", nil); p99 >= 0.1 {
		flags = append(flags, fmt.Sprintf("etcd p99 %.2fs", p99))
	}
	return flags
}

// writeScaling shows, for each workload of each mode, how its measurements
// change with the number of nodes, and which stage grows the most.
func writeScaling(w io.Writer, results []*Result) {
	groups := make(map[string][]*Result)
	for _, r := range results {
		if r.Scenario != "admission" {
			continue
		}
		key := r.Mode.Name() + " " + r.Label
		groups[key] = append(groups[key], r)
	}
	var keys []string
	for key, runs := range groups {
		if len(runs) > 1 {
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return
	}
	slices.Sort(keys)
	fmt.Fprintf(w, "\n## Scaling by workload\n\nHow each workload behaves as nodes grow. p99 in seconds.\n")
	for _, key := range keys {
		runs := groups[key]
		slices.SortFunc(runs, func(a, b *Result) int { return cmp.Compare(a.Nodes, b.Nodes) })
		t := table{header: []string{"nodes", "pods", "pods/s", "create->bound"}}
		for _, s := range stages(runs[0].Latencies) {
			t.header = append(t.header, s.name)
		}
		t.header = append(t.header, "controller cores", "apiserver cores", "slowest stage", "flags")
		for _, r := range runs {
			row := []string{strconv.Itoa(r.Nodes), strconv.Itoa(r.Outcome.Pods), fmt.Sprintf("%.1f", r.Outcome.Throughput), secs(r.Latencies.CreateToBound.P99)}
			for _, s := range stages(r.Latencies) {
				// Stages are only listed when measured.
				row = append(row, fmt.Sprintf("%.3f", s.p99))
			}
			window := r.End.Sub(r.Start).Seconds()
			cores := func(source string) string {
				if window <= 0 {
					return ""
				}
				return fmt.Sprintf("%.2f", metricDelta(r.Metrics, source, "process_cpu_seconds_total", nil)/window)
			}
			row = append(row, cores(sourceController), cores(sourceAPIServer), slowestStage(r.Latencies), strings.Join(bottleneckFlags(r), "; "))
			t.rows = append(t.rows, row)
		}
		fmt.Fprintf(w, "\n### %s\n\n", key)
		fmt.Fprintf(w, "| %s |\n|%s\n", strings.Join(t.header, " | "), strings.Repeat(" --- |", len(t.header)))
		for _, row := range t.rows {
			fmt.Fprintf(w, "| %s |\n", strings.Join(row, " | "))
		}
		fmt.Fprintf(w, "\n%s\n", growth(runs[0], runs[len(runs)-1]))
	}
}

// growth compares the smallest and the largest run of a workload: the
// stage whose p99 grew the most is where scaling costs show first.
func growth(first, last *Result) string {
	if first.Nodes == last.Nodes {
		return ""
	}
	nodes := float64(last.Nodes) / float64(first.Nodes)
	var parts []string
	worst, worstRatio := "", 0.0
	firstStages, lastStages := stages(first.Latencies), stages(last.Latencies)
	for i := range min(len(firstStages), len(lastStages)) {
		a, b := firstStages[i].p99, lastStages[i].p99
		if a <= 0 {
			continue
		}
		ratio := b / a
		parts = append(parts, fmt.Sprintf("%s x%.1f", firstStages[i].name, ratio))
		if ratio > worstRatio {
			worst, worstRatio = firstStages[i].name, ratio
		}
	}
	line := fmt.Sprintf("From %d to %d nodes (x%.1f): p99 %s.", first.Nodes, last.Nodes, nodes, strings.Join(parts, ", "))
	if first.Outcome.Throughput > 0 {
		line += fmt.Sprintf(" Throughput x%.1f.", last.Outcome.Throughput/first.Outcome.Throughput)
	}
	switch {
	case worst == "":
	case worstRatio >= nodes:
		line += fmt.Sprintf(" **%s grows at least as fast as the cluster: the scaling bottleneck.**", worst)
	case worstRatio >= 1.5:
		line += fmt.Sprintf(" %s grows the most.", worst)
	default:
		line += " No stage grows by more than x1.5."
	}
	return line
}

// writeBootstrap compares the workload bootstrap of each mode side by side,
// for runs of the same workload on the same number of nodes: how long pods
// take from creation until they are bound and the IMEX daemon on their node
// is Ready, which Prepare waits for.
func writeBootstrap(w io.Writer, results []*Result) {
	type key struct {
		nodes int
		label string
	}
	byKey := make(map[key]map[string]*Result)
	modeSet := make(map[string]bool)
	for _, r := range results {
		if r.Scenario != "admission" || r.Latencies.CreateToPrepareReady.Count == 0 {
			continue
		}
		k := key{r.Nodes, r.Label}
		if byKey[k] == nil {
			byKey[k] = make(map[string]*Result)
		}
		byKey[k][r.Mode.Name()] = r
		modeSet[r.Mode.Name()] = true
	}
	if len(modeSet) < 2 {
		return
	}
	modes := sortedKeys(modeSet)
	keys := make([]key, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b key) int { return cmp.Or(cmp.Compare(a.label, b.label), cmp.Compare(a.nodes, b.nodes)) })

	t := table{header: []string{"workload", "nodes"}}
	for _, m := range modes {
		t.header = append(t.header, m+" pods", m+" bootstrap p50", m+" bootstrap p99", m+" prepare wait p99")
	}
	if len(modes) == 2 {
		t.header = append(t.header, fmt.Sprintf("p50 %s - %s", modes[0], modes[1]), fmt.Sprintf("p99 %s - %s", modes[0], modes[1]))
	}
	for _, k := range keys {
		row := []string{k.label, strconv.Itoa(k.nodes)}
		for _, m := range modes {
			r, ok := byKey[k][m]
			if !ok {
				row = append(row, "", "", "", "")
				continue
			}
			l := r.Latencies
			row = append(row, fmt.Sprintf("%d/%d", r.Outcome.PrepareReady, r.Outcome.Pods), secs(l.CreateToPrepareReady.P50), secs(l.CreateToPrepareReady.P99), measuredSecs(l.BoundToPrepareReady, l.BoundToPrepareReady.P99))
		}
		if len(modes) == 2 {
			a, aok := byKey[k][modes[0]]
			b, bok := byKey[k][modes[1]]
			if aok && bok {
				row = append(row,
					fmt.Sprintf("%+.3f", a.Latencies.CreateToPrepareReady.P50-b.Latencies.CreateToPrepareReady.P50),
					fmt.Sprintf("%+.3f", a.Latencies.CreateToPrepareReady.P99-b.Latencies.CreateToPrepareReady.P99))
			} else {
				row = append(row, "", "")
			}
		}
		t.rows = append(t.rows, row)
	}
	writeMarkdown(w, "Workload bootstrap by mode", t)
	fmt.Fprintln(w, "\nBootstrap is from creating a pod until it is bound and the IMEX daemon on its node is Ready, which the kubelet plugin's Prepare waits for. Prepare wait is the part after binding.")
}

// writeSingleComputeDomain tabulates runs of one ComputeDomain over the whole
// cluster, the most nodes and pods a single domain spans, by mode and size.
func writeSingleComputeDomain(w io.Writer, results []*Result) {
	var runs []*Result
	for _, r := range results {
		if r.Scenario == "admission" && computeDomains(r) == 1 {
			runs = append(runs, r)
		}
	}
	if len(runs) == 0 {
		return
	}
	slices.SortFunc(runs, func(a, b *Result) int {
		return cmp.Or(cmp.Compare(a.Mode.Name(), b.Mode.Name()), cmp.Compare(a.Nodes, b.Nodes))
	})
	t := table{header: []string{
		"mode", "nodes", "pods bound", "pods/s", "create->bound p50", "create->bound p99", "create->bound max",
		"bootstrap max", "prepare wait p99", "allocated->admitted p99", "controller cores", "controller MiB",
		"scheduler cores", "apiserver cores", "teardown s",
	}}
	for _, r := range runs {
		o, l := r.Outcome, r.Latencies
		window := r.End.Sub(r.Start).Seconds()
		cores := func(source string) string {
			cpu := metricDelta(r.Metrics, source, "process_cpu_seconds_total", nil)
			if cpu == 0 || window <= 0 {
				return ""
			}
			return fmt.Sprintf("%.2f", cpu/window)
		}
		mib := ""
		if v := gaugePeak(r.Gauges, sourceController, "process_resident_memory_bytes", nil); v > 0 {
			mib = fmt.Sprintf("%.0f", v/(1<<20))
		}
		t.rows = append(t.rows, []string{
			r.Mode.Name(), strconv.Itoa(r.Nodes), fmt.Sprintf("%d/%d", o.Bound, o.Pods), fmt.Sprintf("%.1f", o.Throughput),
			secs(l.CreateToBound.P50), secs(l.CreateToBound.P99), secs(l.CreateToBound.Max),
			secs(l.CreateToPrepareReady.Max), measuredSecs(l.BoundToPrepareReady, l.BoundToPrepareReady.P99),
			secs(l.AllocatedToAdmitted.P99), cores(sourceController), mib, cores(sourceScheduler), cores(sourceAPIServer),
			teardownSecs(r.Teardown),
		})
	}
	writeMarkdown(w, "Single ComputeDomain", t)
	fmt.Fprintln(w, "\nOne ComputeDomain whose pods fill the cluster. Bootstrap max is when its last pod could be prepared: the whole domain is up.")
}

// computeDomains returns how many ComputeDomains an admission run created.
func computeDomains(r *Result) int {
	switch p := r.Params.(type) {
	case admissionParams:
		return p.ComputeDomains
	case map[string]any:
		if v, ok := p["computeDomains"].(float64); ok {
			return int(v)
		}
	}
	return 0
}
