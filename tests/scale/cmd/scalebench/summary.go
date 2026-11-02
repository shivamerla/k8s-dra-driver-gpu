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
	"encoding/csv"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/urfave/cli/v2"
)

func summaryCommand() *cli.Command {
	return &cli.Command{
		Name:      "summary",
		Usage:     "Tabulate the headline measurements of many runs, for example a node count sweep",
		ArgsUsage: "RESULT.json|DIR...",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "csv-dir", Usage: "also write admission.csv and static.csv here"},
		},
		Action: func(c *cli.Context) error {
			if c.NArg() == 0 {
				return fmt.Errorf("want result files or directories")
			}
			results, err := readResults(c.Args().Slice())
			if err != nil {
				return err
			}
			admission, static := summaryRows(results)
			writeMarkdown(os.Stdout, "Channel admission", admission)
			writeMarkdown(os.Stdout, "Static IMEX daemons", static)
			writeBootstrap(os.Stdout, results)
			writeSingleComputeDomain(os.Stdout, results)
			writeScaling(os.Stdout, results)
			if dir := c.String("csv-dir"); dir != "" {
				if err := writeCSV(filepath.Join(dir, "admission.csv"), admission); err != nil {
					return err
				}
				if err := writeCSV(filepath.Join(dir, "static.csv"), static); err != nil {
					return err
				}
			}
			return nil
		},
	}
}

func readResults(paths []string) ([]*Result, error) {
	var results []*Result
	for _, root := range paths {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".json") {
				return nil
			}
			r, err := readResult(path)
			if err != nil {
				return err
			}
			if r.Scenario == "admission" || r.Scenario == "static" {
				results = append(results, r)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	slices.SortFunc(results, func(a, b *Result) int {
		return cmp.Or(
			cmp.Compare(a.Scenario, b.Scenario),
			cmp.Compare(a.Mode.Name(), b.Mode.Name()),
			cmp.Compare(a.Nodes, b.Nodes),
			a.Start.Compare(b.Start),
		)
	})
	return results, nil
}

// table is a header and rows of formatted cells.
type table struct {
	header []string
	rows   [][]string
}

func summaryRows(results []*Result) (admission, static table) {
	admission.header = []string{
		"nodes", "mode", "label", "pods bound", "pods/s",
		"create->bound p50", "create->bound p99", "bootstrap p50", "bootstrap p99", "prepare wait p99",
		"create->allocated p99", "allocated->admitted p50", "allocated->admitted p99",
		"sched attempt p99", "unschedulable attempts", "channel rejections",
		"admission queue peak", "pending pods peak", "APF rejected",
		"controller cores", "controller MiB", "scheduler cores", "apiserver cores", "apiserver MiB",
		"teardown s", "slowest stage", "flags", "ok",
	}
	static.header = []string{
		"nodes", "cliques", "rollout s", "update s", "update s per clique", "max cliques disrupted",
		"clique restart p50", "clique restart max", "repartition s", "node-churn s",
		"checks failed", "controller cores", "controller MiB", "apiserver cores",
	}
	for _, r := range results {
		window := r.End.Sub(r.Start).Seconds()
		cores := func(source string) string {
			cpu := metricDelta(r.Metrics, source, "process_cpu_seconds_total", nil)
			if cpu == 0 || window <= 0 {
				return ""
			}
			return fmt.Sprintf("%.2f", cpu/window)
		}
		mib := func(source string) string {
			if v := gaugePeak(r.Gauges, source, "process_resident_memory_bytes", nil); v > 0 {
				return fmt.Sprintf("%.0f", v/(1<<20))
			}
			return ""
		}
		switch r.Scenario {
		case "admission":
			o, l := r.Outcome, r.Latencies
			unschedulable := metricDelta(r.Metrics, sourceScheduler, "scheduler_schedule_attempts_total", map[string]string{"result": "unschedulable"})
			ok := r.Error == "" && o.Bound == o.Pods && o.Rejections == 0 && unschedulable == 0
			admission.rows = append(admission.rows, []string{
				strconv.Itoa(r.Nodes), r.Mode.Name(), r.Label,
				fmt.Sprintf("%d/%d", o.Bound, o.Pods), fmt.Sprintf("%.1f", o.Throughput),
				secs(l.CreateToBound.P50), secs(l.CreateToBound.P99),
				secs(l.CreateToPrepareReady.P50), secs(l.CreateToPrepareReady.P99), measuredSecs(l.BoundToPrepareReady, l.BoundToPrepareReady.P99),
				secs(l.CreateToAllocated.P99), secs(l.AllocatedToAdmitted.P50), secs(l.AllocatedToAdmitted.P99),
				secs(metricP99(r.Metrics, sourceScheduler, "scheduler_scheduling_attempt_duration_seconds", map[string]string{"result": "scheduled"})),
				fmt.Sprintf("%.0f", unschedulable), strconv.Itoa(o.Rejections),
				fmt.Sprintf("%.0f", gaugePeak(r.Gauges, sourceController, "workqueue_depth", map[string]string{"name": "compute-domain-claim-admission"})),
				fmt.Sprintf("%.0f", gaugePeak(r.Gauges, sourceScheduler, "scheduler_pending_pods", nil)),
				fmt.Sprintf("%.0f", metricDelta(r.Metrics, sourceAPIServer, "apiserver_flowcontrol_rejected_requests_total", nil)),
				cores(sourceController), mib(sourceController), cores(sourceScheduler), cores(sourceAPIServer), mib(sourceAPIServer),
				teardownSecs(r.Teardown), slowestStage(l), strings.Join(bottleneckFlags(r), "; "), passFail(ok),
			})
		case "static":
			phases := make(map[string]PhaseResult, len(r.Phases))
			for _, p := range r.Phases {
				phases[p.Name] = p
			}
			duration := func(name string) string {
				p, ok := phases[name]
				if !ok {
					return ""
				}
				if p.Error != "" {
					return "FAIL"
				}
				return fmt.Sprintf("%.1f", p.Duration)
			}
			update := phases["update"]
			static.rows = append(static.rows, []string{
				strconv.Itoa(r.Nodes), strconv.Itoa(r.Cliques), duration("rollout"), duration("update"), perClique(update, r.Cliques),
				strconv.Itoa(update.MaxDisruptedCliques), secs(update.CliqueRestart.P50), secs(update.CliqueRestart.Max),
				duration("repartition"), duration("node-churn"),
				strconv.Itoa(len(r.failedChecks())), cores(sourceController), mib(sourceController), cores(sourceAPIServer),
			})
		}
	}
	return admission, static
}

func perClique(p PhaseResult, cliques int) string {
	if p.Duration == 0 || p.Error != "" || cliques == 0 {
		return ""
	}
	return fmt.Sprintf("%.1f", p.Duration/float64(cliques))
}

func secs(v float64) string {
	if v == 0 {
		return ""
	}
	return fmt.Sprintf("%.3f", v)
}

// measuredSecs formats a latency of s, which is zero rather than empty when
// it was measured: a static IMEX daemon is ready before the workload binds.
func measuredSecs(s Summary, v float64) string {
	if s.Count == 0 {
		return ""
	}
	return fmt.Sprintf("%.3f", v)
}

// teardownSecs formats how long a run's teardown took, as a lower bound when
// it did not finish.
func teardownSecs(t *Teardown) string {
	switch {
	case t == nil:
		return ""
	case t.Complete:
		return fmt.Sprintf("%.0f", t.Seconds)
	default:
		return fmt.Sprintf(">%.0f", t.Seconds)
	}
}

func passFail(ok bool) string {
	if ok {
		return "PASS"
	}
	return "FAIL"
}

func matchesLabels(m MetricResult, source, name string, labels map[string]string) bool {
	if m.Source != source || m.Name != name {
		return false
	}
	for k, v := range labels {
		if m.Labels[k] != v {
			return false
		}
	}
	return true
}

// metricDelta sums the counter deltas of the matching series.
func metricDelta(ms []MetricResult, source, name string, labels map[string]string) float64 {
	var sum float64
	for _, m := range ms {
		if matchesLabels(m, source, name, labels) {
			sum += deref(m.Delta)
		}
	}
	return sum
}

// metricP99 is the largest p99 of the matching histograms.
func metricP99(ms []MetricResult, source, name string, labels map[string]string) float64 {
	var p99 float64
	for _, m := range ms {
		if matchesLabels(m, source, name, labels) {
			p99 = max(p99, deref(m.P99))
		}
	}
	return p99
}

// gaugePeak is the largest peak of the matching gauges.
func gaugePeak(ms []MetricResult, source, name string, labels map[string]string) float64 {
	var peak float64
	for _, m := range ms {
		if matchesLabels(m, source, name, labels) {
			peak = max(peak, deref(m.Max))
		}
	}
	return peak
}

func writeMarkdown(w io.Writer, title string, t table) {
	if len(t.rows) == 0 {
		return
	}
	fmt.Fprintf(w, "\n## %s\n\n", title)
	fmt.Fprintf(w, "| %s |\n", strings.Join(t.header, " | "))
	fmt.Fprintf(w, "|%s\n", strings.Repeat(" --- |", len(t.header)))
	for _, row := range t.rows {
		fmt.Fprintf(w, "| %s |\n", strings.Join(row, " | "))
	}
}

func writeCSV(path string, t table) error {
	if len(t.rows) == 0 {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	_ = w.Write(t.header)
	_ = w.WriteAll(t.rows)
	if err := w.Error(); err != nil {
		f.Close()
		return fmt.Errorf("error writing %s: %w", path, err)
	}
	return f.Close()
}
