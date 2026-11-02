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
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/urfave/cli/v2"
	"k8s.io/klog/v2"
)

// Result is what a scenario run writes to <output-dir>/<scenario>-<mode>-<label>-<run>.json.
type Result struct {
	Scenario string     `json:"scenario"`
	Label    string     `json:"label,omitempty"`
	RunID    string     `json:"runID"`
	Mode     driverMode `json:"mode"`
	Nodes    int        `json:"nodes"`
	Cliques  int        `json:"cliques,omitempty"`
	Params   any        `json:"params"`
	Start    time.Time  `json:"start"`
	End      time.Time  `json:"end"`
	Error    string     `json:"error,omitempty"`

	// Admission scenario.
	Latencies       PodLatencies `json:"latencies,omitzero"`
	Outcome         Outcome      `json:"outcome,omitzero"`
	TemplateLatency Summary      `json:"computeDomainToTemplates,omitzero"`
	Teardown        *Teardown    `json:"teardown,omitempty"`

	// Static scenario.
	Phases []PhaseResult `json:"phases,omitempty"`
	Checks []Check       `json:"checks,omitempty"`

	Metrics []MetricResult `json:"metrics,omitempty"`
	Gauges  []MetricResult `json:"gaugePeaks,omitempty"`
	Agent   *agentStats    `json:"agent,omitempty"`
}

// Teardown is how long deleting the namespaces of a run took. An incomplete
// teardown leaves work for the next run, which then starts on a busy
// controller.
type Teardown struct {
	Seconds  float64 `json:"seconds"`
	Complete bool    `json:"complete"`
}

// Check is a pass/fail assertion of a run.
type Check struct {
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
	Detail string `json:"detail,omitempty"`
}

func (r *Result) addCheck(name string, passed bool, detail string) {
	r.Checks = append(r.Checks, Check{Name: name, Passed: passed, Detail: detail})
}

func (r *Result) failedChecks() []string {
	var failed []string
	for _, c := range r.Checks {
		if !c.Passed {
			failed = append(failed, c.Name)
		}
	}
	return failed
}

func (r *Result) fileName() string {
	parts := []string{r.Scenario, r.Mode.Name()}
	if r.Label != "" {
		parts = append(parts, r.Label)
	}
	parts = append(parts, r.RunID)
	return strings.Join(parts, "-") + ".json"
}

func (r *Result) write(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("error creating %s: %w", dir, err)
	}
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(dir, r.fileName())
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("error writing %s: %w", path, err)
	}
	r.print(os.Stdout)
	klog.Infof("Wrote %s", path)
	return nil
}

func (r *Result) print(w io.Writer) {
	fmt.Fprintf(w, "\n%s run %s, mode %s", r.Scenario, r.RunID, r.Mode.Name())
	if r.Label != "" {
		fmt.Fprintf(w, " (%s)", r.Label)
	}
	fmt.Fprintf(w, ", %d nodes, %v\n", r.Nodes, r.End.Sub(r.Start).Round(time.Second))
	if r.Error != "" {
		fmt.Fprintf(w, "ERROR: %s\n", r.Error)
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if r.Scenario == "admission" {
		o := r.Outcome
		fmt.Fprintf(tw, "pods bound\t%d/%d\n", o.Bound, o.Pods)
		fmt.Fprintf(tw, "throughput\t%.1f pods/s over %.1fs\n", o.Throughput, o.Window)
		fmt.Fprintf(tw, "waited on admission\t%d\n", o.WaitedOnAdmission)
		fmt.Fprintf(tw, "ready to prepare\t%d/%d bound pods\n", o.PrepareReady, o.Bound)
		fmt.Fprintf(tw, "channel rejections\t%d %v\n", o.Rejections, o.RejectionReasons)
		fmt.Fprintln(tw, "\nseconds\tp50\tp90\tp99\tmax")
		for _, row := range []struct {
			name string
			s    Summary
		}{
			{"create -> bound", r.Latencies.CreateToBound},
			{"create -> allocated", r.Latencies.CreateToAllocated},
			{"allocated -> admitted", r.Latencies.AllocatedToAdmitted},
			{"admitted -> bound", r.Latencies.AdmittedToBound},
			{"allocated -> bound", r.Latencies.AllocatedToBound},
			{"bound -> ready to prepare", r.Latencies.BoundToPrepareReady},
			{"bootstrap (create -> ready to prepare)", r.Latencies.CreateToPrepareReady},
			{"CD -> templates", r.TemplateLatency},
		} {
			if row.s.Count == 0 {
				continue
			}
			fmt.Fprintf(tw, "%s\t%.3f\t%.3f\t%.3f\t%.3f\n", row.name, row.s.P50, row.s.P90, row.s.P99, row.s.Max)
		}
	}
	for _, p := range r.Phases {
		fmt.Fprintf(tw, "phase %s\t%.1fs", p.Name, p.Duration)
		if p.MaxDisruptedCliques > 0 {
			fmt.Fprintf(tw, ", at most %d cliques disrupted, clique restart p50 %.1fs max %.1fs", p.MaxDisruptedCliques, p.CliqueRestart.P50, p.CliqueRestart.Max)
		}
		if p.Error != "" {
			fmt.Fprintf(tw, ", ERROR: %s", p.Error)
		}
		fmt.Fprintln(tw)
		for _, k := range sortedKeys(p.Marks) {
			fmt.Fprintf(tw, "  %s\t%.1fs\n", k, p.Marks[k])
		}
	}
	for _, c := range r.Checks {
		status := "PASS"
		if !c.Passed {
			status = "FAIL"
		}
		fmt.Fprintf(tw, "%s\t%s %s\n", status, c.Name, c.Detail)
	}
	if r.Agent != nil && r.Agent.Daemons != nil {
		d := r.Agent.Daemons
		fmt.Fprintf(tw, "IMEX daemon clique writes\t%d, %d conflicts, %d reregistered\n", d.Writes, d.Conflicts, d.Reregistered)
	}
	fmt.Fprintln(tw, "\nmetric\tcount/delta\tp50\tp99")
	for _, m := range r.Metrics {
		fmt.Fprintf(tw, "%s\t%s\n", m.Key(), m.cells())
	}
	for _, m := range r.Gauges {
		fmt.Fprintf(tw, "%s (peak)\t%.0f\n", m.Key(), *m.Max)
	}
	tw.Flush()
}

func (m MetricResult) cells() string {
	if m.Delta != nil {
		return fmt.Sprintf("%.0f", *m.Delta)
	}
	if m.Count != nil {
		return fmt.Sprintf("%.0f\t%.4f\t%.4f", *m.Count, *m.P50, *m.P99)
	}
	return ""
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

func compareCommand() *cli.Command {
	return &cli.Command{
		Name:      "compare",
		Usage:     "Compare two admission results, for example binding conditions off and on",
		ArgsUsage: "BASELINE.json CANDIDATE.json",
		Action: func(c *cli.Context) error {
			if c.NArg() != 2 {
				return fmt.Errorf("want 2 result files, got %d", c.NArg())
			}
			a, err := readResult(c.Args().Get(0))
			if err != nil {
				return err
			}
			b, err := readResult(c.Args().Get(1))
			if err != nil {
				return err
			}
			compare(os.Stdout, a, b)
			return nil
		},
	}
}

func readResult(path string) (*Result, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var r Result
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("error parsing %s: %w", path, err)
	}
	return &r, nil
}

func name(r *Result) string {
	if r.Label != "" {
		return r.Mode.Name() + "/" + r.Label
	}
	return r.Mode.Name()
}

func compare(w io.Writer, a, b *Result) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintf(tw, "\t%s\t%s\tchange\t\n", name(a), name(b))
	row := func(label string, x, y float64, unit string) {
		change := "n/a"
		if x != 0 {
			change = fmt.Sprintf("%+.1f%%", 100*(y-x)/x)
		}
		fmt.Fprintf(tw, "%s\t%.3f%s\t%.3f%s\t%s\t\n", label, x, unit, y, unit, change)
	}
	fmt.Fprintf(tw, "nodes\t%d\t%d\t\t\n", a.Nodes, b.Nodes)
	fmt.Fprintf(tw, "pods bound\t%d/%d\t%d/%d\t\t\n", a.Outcome.Bound, a.Outcome.Pods, b.Outcome.Bound, b.Outcome.Pods)
	row("throughput (pods/s)", a.Outcome.Throughput, b.Outcome.Throughput, "")
	for _, s := range []struct {
		label string
		x, y  Summary
	}{
		{"create->bound", a.Latencies.CreateToBound, b.Latencies.CreateToBound},
		{"create->allocated", a.Latencies.CreateToAllocated, b.Latencies.CreateToAllocated},
		{"allocated->bound", a.Latencies.AllocatedToBound, b.Latencies.AllocatedToBound},
		{"allocated->admitted", a.Latencies.AllocatedToAdmitted, b.Latencies.AllocatedToAdmitted},
		{"bound->ready to prepare", a.Latencies.BoundToPrepareReady, b.Latencies.BoundToPrepareReady},
		{"bootstrap", a.Latencies.CreateToPrepareReady, b.Latencies.CreateToPrepareReady},
	} {
		row(s.label+" p50", s.x.P50, s.y.P50, "s")
		row(s.label+" p99", s.x.P99, s.y.P99, "s")
	}
	fmt.Fprintf(tw, "channel rejections\t%d\t%d\t\t\n", a.Outcome.Rejections, b.Outcome.Rejections)
	am, bm := indexMetrics(a.Metrics), indexMetrics(b.Metrics)
	for _, key := range unionKeys(am, bm) {
		x, y := am[key], bm[key]
		switch {
		case x.Count != nil || y.Count != nil:
			row(key+" count", deref(x.Count), deref(y.Count), "")
			row(key+" p99", deref(x.P99), deref(y.P99), "s")
		default:
			row(key, deref(x.Delta), deref(y.Delta), "")
		}
	}
	tw.Flush()
}

func indexMetrics(ms []MetricResult) map[string]MetricResult {
	m := make(map[string]MetricResult, len(ms))
	for _, r := range ms {
		m[r.Key()] = r
	}
	return m
}

func unionKeys(a, b map[string]MetricResult) []string {
	set := make(map[string]bool)
	for k := range a {
		set[k] = true
	}
	for k := range b {
		set[k] = true
	}
	return sortedKeys(set)
}

func deref(f *float64) float64 {
	if f == nil {
		return 0
	}
	return *f
}
