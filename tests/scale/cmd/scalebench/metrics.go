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
	"bytes"
	"context"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

const (
	sourceAPIServer  = "apiserver"
	sourceScheduler  = "scheduler"
	sourceController = "controller"
)

// metricSpec selects the series of one metric to report, split by groupBy.
// A match value may list alternatives separated by "|".
type metricSpec struct {
	source  string
	name    string
	match   map[string]string
	groupBy []string
}

// reportedMetrics are what tells whether binding conditions cost the
// scheduler, and where the time of a claim goes.
var reportedMetrics = []metricSpec{
	{source: sourceScheduler, name: "scheduler_schedule_attempts_total", groupBy: []string{"result"}},
	{source: sourceScheduler, name: "scheduler_scheduling_attempt_duration_seconds", groupBy: []string{"result"}},
	{source: sourceScheduler, name: "scheduler_pod_scheduling_sli_duration_seconds"},
	{source: sourceScheduler, name: "scheduler_pod_scheduling_attempts"},
	{source: sourceScheduler, name: "scheduler_framework_extension_point_duration_seconds", match: map[string]string{"extension_point": "PreFilter|Filter|Reserve|PreBind|Bind"}, groupBy: []string{"extension_point"}},
	{source: sourceScheduler, name: "scheduler_plugin_execution_duration_seconds", match: map[string]string{"plugin": "DynamicResources"}, groupBy: []string{"extension_point"}},
	{source: sourceScheduler, name: "scheduler_dra_bindingconditions_allocations_total", groupBy: []string{"status"}},
	{source: sourceScheduler, name: "scheduler_dra_bindingconditions_wait_duration_seconds", groupBy: []string{"status"}},
	{source: sourceController, name: "workqueue_adds_total", match: map[string]string{"name": "compute-domain-claim-admission|compute-domain-daemonset"}, groupBy: []string{"name"}},
	{source: sourceController, name: "workqueue_retries_total", match: map[string]string{"name": "compute-domain-claim-admission|compute-domain-daemonset"}, groupBy: []string{"name"}},
	{source: sourceController, name: "workqueue_queue_duration_seconds", match: map[string]string{"name": "compute-domain-claim-admission|compute-domain-daemonset"}, groupBy: []string{"name"}},
	{source: sourceController, name: "workqueue_work_duration_seconds", match: map[string]string{"name": "compute-domain-claim-admission|compute-domain-daemonset"}, groupBy: []string{"name"}},
	{source: sourceAPIServer, name: "apiserver_request_total", match: map[string]string{"resource": "resourceclaims|resourceslices|computedomaincliques|nodes"}, groupBy: []string{"resource", "subresource", "verb", "code"}},
	{source: sourceAPIServer, name: "apiserver_request_duration_seconds", match: map[string]string{"resource": "resourceclaims|pods", "subresource": "status|binding", "verb": "PUT|PATCH|POST"}, groupBy: []string{"resource", "subresource", "verb"}},
	// Requests that API Priority and Fairness rejected or queued: the API
	// server itself throttling the run.
	{source: sourceAPIServer, name: "apiserver_flowcontrol_rejected_requests_total", groupBy: []string{"priority_level", "reason"}},
	{source: sourceAPIServer, name: "apiserver_flowcontrol_request_wait_duration_seconds", match: map[string]string{"execute": "true"}, groupBy: []string{"priority_level"}},
	{source: sourceAPIServer, name: "etcd_request_duration_seconds", match: map[string]string{"type": "resourceclaims.resource.k8s.io|computedomaincliques.resource.nvidia.com|pods"}, groupBy: []string{"type", "operation"}},
	// CPU seconds each component used; divided by the run's duration, the
	// cores it kept busy.
	{source: sourceAPIServer, name: "process_cpu_seconds_total"},
	{source: sourceScheduler, name: "process_cpu_seconds_total"},
	{source: sourceController, name: "process_cpu_seconds_total"},
}

// sampledGauges are sampled during a run, as only their peak matters.
var sampledGauges = []metricSpec{
	{source: sourceScheduler, name: "scheduler_pending_pods", groupBy: []string{"queue"}},
	{source: sourceController, name: "workqueue_depth", match: map[string]string{"name": "compute-domain-claim-admission|compute-domain-daemonset"}, groupBy: []string{"name"}},
	{source: sourceAPIServer, name: "apiserver_flowcontrol_current_inqueue_requests"},
	{source: sourceAPIServer, name: "apiserver_storage_objects", match: map[string]string{"resource": "nodes|pods|resourceclaims.resource.k8s.io|resourceslices.resource.k8s.io|computedomaincliques.resource.nvidia.com"}, groupBy: []string{"resource"}},
	{source: sourceAPIServer, name: "process_resident_memory_bytes"},
	{source: sourceScheduler, name: "process_resident_memory_bytes"},
	{source: sourceController, name: "process_resident_memory_bytes"},
	{source: sourceController, name: "go_goroutines"},
}

type scrape map[string]map[string]*dto.MetricFamily

type scraper struct {
	cs *clients
}

// scrapeAll fetches the metrics of all sources. A source that cannot be
// reached is logged and skipped, so that a run still reports the others.
func (s *scraper) scrapeAll(ctx context.Context, sources ...string) scrape {
	result := make(scrape)
	for _, source := range sources {
		raw, err := s.fetch(ctx, source)
		if err != nil {
			klog.Warningf("Skipping %s metrics: %v", source, err)
			continue
		}
		parser := expfmt.NewTextParser(model.UTF8Validation)
		families, err := parser.TextToMetricFamilies(bytes.NewReader(raw))
		if err != nil {
			klog.Warningf("Skipping %s metrics: %v", source, err)
			continue
		}
		result[source] = families
	}
	return result
}

func (s *scraper) fetch(ctx context.Context, source string) ([]byte, error) {
	switch source {
	case sourceAPIServer:
		return s.cs.core.CoreV1().RESTClient().Get().AbsPath("/metrics").DoRaw(ctx)
	case sourceScheduler:
		pods, err := s.cs.core.CoreV1().Pods("kube-system").List(ctx, metav1.ListOptions{LabelSelector: "component=kube-scheduler"})
		if err != nil {
			return nil, err
		}
		if len(pods.Items) == 0 {
			return nil, fmt.Errorf("no kube-scheduler pod")
		}
		return s.cs.core.CoreV1().Pods("kube-system").ProxyGet("https", pods.Items[0].Name, "10259", "metrics", nil).DoRaw(ctx)
	case sourceController:
		pods, err := s.cs.core.CoreV1().Pods(s.cs.driverNamespace).List(ctx, metav1.ListOptions{LabelSelector: controllerComponentLabel})
		if err != nil {
			return nil, err
		}
		for i := range pods.Items {
			pod := &pods.Items[i]
			// A replaced pod stays Running while it shuts down its server.
			if pod.Status.Phase != "Running" || pod.DeletionTimestamp != nil {
				continue
			}
			port, path := "8080", "metrics"
			for _, c := range pod.Spec.Containers {
				for _, env := range c.Env {
					switch env.Name {
					case "HTTP_ENDPOINT":
						if _, p, ok := strings.Cut(env.Value, ":"); ok && p != "" {
							port = p
						}
					case "METRICS_PATH":
						path = strings.TrimPrefix(env.Value, "/")
					}
				}
			}
			return s.cs.core.CoreV1().Pods(s.cs.driverNamespace).ProxyGet("http", pod.Name, port, path, nil).DoRaw(ctx)
		}
		return nil, fmt.Errorf("no running controller pod")
	}
	return nil, fmt.Errorf("unknown source %q", source)
}

// MetricResult is the change of one series over a run. Counters report
// Delta; histograms report Count, Sum and quantiles in seconds.
type MetricResult struct {
	Source string            `json:"source"`
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels,omitempty"`
	Delta  *float64          `json:"delta,omitempty"`
	Count  *float64          `json:"count,omitempty"`
	Mean   *float64          `json:"mean,omitempty"`
	P50    *float64          `json:"p50,omitempty"`
	P90    *float64          `json:"p90,omitempty"`
	P99    *float64          `json:"p99,omitempty"`
	Max    *float64          `json:"max,omitempty"`
	// Samples of a sampled gauge over the run.
	Samples []Sample `json:"samples,omitempty"`
}

// Sample is the value of a gauge At seconds into a run.
type Sample struct {
	At    float64 `json:"t"`
	Value float64 `json:"v"`
}

// Key identifies a series across runs. Metrics that every source exposes
// are prefixed with their source.
func (r MetricResult) Key() string {
	keys := make([]string, 0, len(r.Labels))
	for k := range r.Labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	if strings.HasPrefix(r.Name, "process_") || strings.HasPrefix(r.Name, "go_") {
		b.WriteString(r.Source + ":")
	}
	b.WriteString(r.Name)
	for _, k := range keys {
		fmt.Fprintf(&b, ",%s=%s", k, r.Labels[k])
	}
	return b.String()
}

type histogram struct {
	count, sum float64
	// buckets maps upper bounds to cumulative counts.
	buckets map[float64]float64
}

func labelsOf(m *dto.Metric) map[string]string {
	l := make(map[string]string, len(m.GetLabel()))
	for _, p := range m.GetLabel() {
		l[p.GetName()] = p.GetValue()
	}
	return l
}

func (spec metricSpec) matches(l map[string]string) bool {
	for k, alternatives := range spec.match {
		if !slices.Contains(strings.Split(alternatives, "|"), l[k]) {
			return false
		}
	}
	return true
}

func (spec metricSpec) group(l map[string]string) map[string]string {
	g := make(map[string]string, len(spec.groupBy))
	for _, k := range spec.groupBy {
		g[k] = l[k]
	}
	return g
}

func groupKey(g map[string]string) string {
	return MetricResult{Labels: g}.Key()
}

// aggregate sums the matching series of a metric by group.
func (spec metricSpec) aggregate(s scrape) (map[string]float64, map[string]*histogram, map[string]map[string]string) {
	values := make(map[string]float64)
	hists := make(map[string]*histogram)
	groups := make(map[string]map[string]string)
	family := s[spec.source][spec.name]
	if family == nil {
		return values, hists, groups
	}
	for _, m := range family.GetMetric() {
		l := labelsOf(m)
		if !spec.matches(l) {
			continue
		}
		g := spec.group(l)
		key := groupKey(g)
		groups[key] = g
		switch family.GetType() {
		case dto.MetricType_COUNTER:
			values[key] += m.GetCounter().GetValue()
		case dto.MetricType_GAUGE:
			values[key] += m.GetGauge().GetValue()
		case dto.MetricType_HISTOGRAM:
			h := hists[key]
			if h == nil {
				h = &histogram{buckets: make(map[float64]float64)}
				hists[key] = h
			}
			h.count += float64(m.GetHistogram().GetSampleCount())
			h.sum += m.GetHistogram().GetSampleSum()
			for _, b := range m.GetHistogram().GetBucket() {
				h.buckets[b.GetUpperBound()] += float64(b.GetCumulativeCount())
			}
		default:
		}
	}
	return values, hists, groups
}

// diffMetrics reports how the selected metrics changed between two scrapes.
// A counter that went down was reset by a restart, so its after value is the
// change.
func diffMetrics(before, after scrape) []MetricResult {
	var results []MetricResult
	for _, spec := range reportedMetrics {
		bv, bh, _ := spec.aggregate(before)
		av, ah, groups := spec.aggregate(after)
		keys := make([]string, 0, len(groups))
		for k := range groups {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, key := range keys {
			r := MetricResult{Source: spec.source, Name: spec.name, Labels: groups[key]}
			if h, ok := ah[key]; ok {
				d := h.minus(bh[key])
				if d.count == 0 {
					continue
				}
				r.Count = ptrTo(d.count)
				r.Mean = ptrTo(d.sum / d.count)
				r.P50 = ptrTo(d.quantile(0.5))
				r.P90 = ptrTo(d.quantile(0.9))
				r.P99 = ptrTo(d.quantile(0.99))
			} else {
				delta := av[key] - bv[key]
				if delta < 0 {
					delta = av[key]
				}
				if delta == 0 {
					continue
				}
				r.Delta = ptrTo(delta)
			}
			results = append(results, r)
		}
	}
	return results
}

func (h *histogram) minus(before *histogram) *histogram {
	d := &histogram{count: h.count, sum: h.sum, buckets: make(map[float64]float64, len(h.buckets))}
	for b, c := range h.buckets {
		d.buckets[b] = c
	}
	if before == nil || before.count > h.count {
		return d
	}
	d.count -= before.count
	d.sum -= before.sum
	for b, c := range before.buckets {
		d.buckets[b] -= c
	}
	return d
}

// quantile interpolates linearly within buckets, as histogram_quantile does.
func (h *histogram) quantile(q float64) float64 {
	bounds := make([]float64, 0, len(h.buckets))
	for b := range h.buckets {
		bounds = append(bounds, b)
	}
	sort.Float64s(bounds)
	rank := q * h.count
	prevBound, prevCount := 0.0, 0.0
	for _, b := range bounds {
		c := h.buckets[b]
		if c >= rank {
			if math.IsInf(b, 1) {
				return prevBound
			}
			if c == prevCount {
				return b
			}
			return prevBound + (b-prevBound)*(rank-prevCount)/(c-prevCount)
		}
		prevBound, prevCount = b, c
	}
	return prevBound
}

// gaugeSampler records sampledGauges during a run, with their peaks.
type gaugeSampler struct {
	scraper *scraper
	mu      sync.Mutex
	start   time.Time
	peaks   map[string]MetricResult
	samples map[string][]Sample
}

func (g *gaugeSampler) run(ctx context.Context, interval time.Duration) {
	g.mu.Lock()
	g.start = time.Now()
	g.mu.Unlock()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			g.sample(ctx)
		}
	}
}

func (g *gaugeSampler) sample(ctx context.Context) {
	s := g.scraper.scrapeAll(ctx, sourceAPIServer, sourceScheduler, sourceController)
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.peaks == nil {
		g.peaks = make(map[string]MetricResult)
		g.samples = make(map[string][]Sample)
	}
	at := time.Since(g.start).Round(time.Millisecond).Seconds()
	for _, spec := range sampledGauges {
		values, _, groups := spec.aggregate(s)
		for key, v := range values {
			r := MetricResult{Source: spec.source, Name: spec.name, Labels: groups[key], Max: ptrTo(v)}
			g.samples[r.Key()] = append(g.samples[r.Key()], Sample{At: at, Value: v})
			if old, ok := g.peaks[r.Key()]; ok && *old.Max >= v {
				continue
			}
			g.peaks[r.Key()] = r
		}
	}
}

func (g *gaugeSampler) results() []MetricResult {
	g.mu.Lock()
	defer g.mu.Unlock()
	results := make([]MetricResult, 0, len(g.peaks))
	for key, r := range g.peaks {
		r.Samples = g.samples[key]
		results = append(results, r)
	}
	slices.SortFunc(results, func(a, b MetricResult) int { return strings.Compare(a.Key(), b.Key()) })
	return results
}

func ptrTo(f float64) *float64 { return &f }
