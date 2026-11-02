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
	"context"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/urfave/cli/v2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/dra-driver-nvidia-gpu/tests/scale/fakeslices"
)

// minKubernetes serves binding conditions and multiple allocations by
// default (beta).
var minKubernetes = version.MustParseGeneric("1.36.0")

func preflightCommand() *cli.Command {
	return &cli.Command{
		Name:  "preflight",
		Usage: "Check that the cluster can run the scale tests",
		Action: func(c *cli.Context) error {
			ctx, cancel := signalContext(c.Context)
			defer cancel()
			cs, err := newClients(c)
			if err != nil {
				return err
			}
			return preflight(ctx, cs)
		},
	}
}

type preflightCheck struct {
	name string
	run  func(context.Context, *clients) (string, error)
}

func preflight(ctx context.Context, cs *clients) error {
	checks := []preflightCheck{
		{"Kubernetes version", checkVersion},
		{"DRA fields served", checkDRAFields},
		{"KWOK controller", checkKWOK},
		{"driver controller", checkDriver},
		{"scheduler metrics", checkSource(sourceScheduler)},
		{"controller metrics", checkSource(sourceController)},
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	failed := 0
	for _, check := range checks {
		detail, err := check.run(ctx, cs)
		status := "ok"
		if err != nil {
			status, detail = "FAIL", err.Error()
			failed++
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\n", check.name, status, detail)
	}
	tw.Flush()
	if failed > 0 {
		return fmt.Errorf("%d preflight checks failed", failed)
	}
	return nil
}

func checkVersion(_ context.Context, cs *clients) (string, error) {
	info, err := cs.core.Discovery().ServerVersion()
	if err != nil {
		return "", err
	}
	v, err := version.ParseGeneric(info.GitVersion)
	if err != nil {
		return "", err
	}
	if v.LessThan(minKubernetes) {
		return "", fmt.Errorf("%s is older than %s, which serves binding conditions by default", info.GitVersion, minKubernetes)
	}
	return info.GitVersion, nil
}

// checkDRAFields creates a ResourceSlice in dry-run mode: the API server
// drops fields of disabled feature gates instead of failing.
func checkDRAFields(ctx context.Context, cs *clients) (string, error) {
	devices := fakeslices.ComputeDomainDevices(fakeslices.ComputeDomainOptions{Reservations: true, CliqueID: "preflight", Owner: "preflight"})
	slice := fakeslices.Slice("preflight", fakeslices.ComputeDomainDriverName, 0, devices)
	slice.Spec.NodeName = nil
	slice.Spec.AllNodes = ptr.To(true)
	created, err := cs.core.ResourceV1().ResourceSlices().Create(ctx, slice, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
	if err != nil {
		return "", err
	}
	for _, d := range created.Spec.Devices {
		if d.Name != "channel" {
			continue
		}
		var missing []string
		if len(d.BindingConditions) == 0 {
			missing = append(missing, "bindingConditions (DRADeviceBindingConditions)")
		}
		if d.AllowMultipleAllocations == nil {
			missing = append(missing, "allowMultipleAllocations (DRAConsumableCapacity)")
		}
		if len(d.Taints) == 0 {
			missing = append(missing, "taints (DRADeviceTaints)")
		}
		if len(missing) > 0 {
			return "", fmt.Errorf("the API server drops %v, enable the feature gates", missing)
		}
		return "binding conditions, multiple allocations, device taints", nil
	}
	return "", fmt.Errorf("dry-run ResourceSlice lost its channel device")
}

func checkKWOK(ctx context.Context, cs *clients) (string, error) {
	d, err := cs.core.AppsV1().Deployments("kube-system").Get(ctx, "kwok-controller", metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return "", fmt.Errorf("kwok-controller is not installed in kube-system, run scripts/create-cluster.sh")
	}
	if err != nil {
		return "", err
	}
	if d.Status.ReadyReplicas == 0 {
		return "", fmt.Errorf("kwok-controller is not ready")
	}
	nodes, err := cs.core.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: fakeNodeLabelKey})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%d fake nodes", len(nodes.Items)), nil
}

func checkDriver(ctx context.Context, cs *clients) (string, error) {
	mode, err := detectDriverMode(ctx, cs)
	if err != nil {
		return "", err
	}
	classes := []string{"compute-domain-default-channel.nvidia.com"}
	if mode.Reservations {
		classes = append(classes, "compute-domain-channel.nvidia.com")
	}
	for _, name := range classes {
		if _, err := cs.core.ResourceV1().DeviceClasses().Get(ctx, name, metav1.GetOptions{}); err != nil {
			return "", fmt.Errorf("DeviceClass %s: %w", name, err)
		}
	}
	if _, err := cs.core.ResourceV1().DeviceClasses().Get(ctx, fakeslices.GPUDeviceClassName, metav1.GetOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return "", err
	}
	return fmt.Sprintf("mode %s, gates %q, controller QPS %s, image %s", mode.Name(), mode.FeatureGates, mode.KubeAPIQPS, mode.Image), nil
}

func checkSource(source string) func(context.Context, *clients) (string, error) {
	return func(ctx context.Context, cs *clients) (string, error) {
		// A just installed controller may not serve metrics yet.
		var raw []byte
		var fetchErr error
		err := wait.PollUntilContextTimeout(ctx, 2*time.Second, time.Minute, true, func(ctx context.Context) (bool, error) {
			raw, fetchErr = (&scraper{cs: cs}).fetch(ctx, source)
			if fetchErr != nil {
				klog.V(2).Infof("Waiting for %s metrics: %v", source, fetchErr)
				return false, nil
			}
			return true, nil
		})
		if err != nil {
			return "", cmp.Or(fetchErr, err)
		}
		return fmt.Sprintf("%d bytes", len(raw)), nil
	}
}
