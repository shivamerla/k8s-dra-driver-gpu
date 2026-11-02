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
	"strconv"
	"strings"

	"github.com/urfave/cli/v2"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/imex"
	nvclientset "sigs.k8s.io/dra-driver-nvidia-gpu/pkg/nvidia.com/clientset/versioned"
)

const (
	// Labels and taints of the fake nodes. KWOK manages nodes with the
	// annotation, and the taint keeps real pods off them.
	kwokNodeAnnotationKey = "kwok.x-k8s.io/node"
	kwokNodeTaintKey      = "kwok.x-k8s.io/node"
	fakeNodeValue         = "fake"
	nodeTypeLabelKey      = "type"
	nodeTypeLabelValue    = "kwok"
	gpuCliqueLabelKey     = "nvidia.com/gpu.clique"
	// scaleLabelKey marks every object scalebench creates, with the run.
	scaleLabelKey = "scale.nvidia.com/run"

	// Keys the driver uses, copied here as the driver packages are commands.
	computeDomainLabelKey          = "resource.nvidia.com/computeDomain"
	computeDomainDaemonSetLabelKey = "resource.nvidia.com/computeDomainDaemonSet"
	computeDomainCliqueLabelKey    = "resource.nvidia.com/computeDomain.cliqueID"
	controllerComponentLabel       = "dra-driver-nvidia-gpu-component=controller"
)

func globalFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{Name: "kubeconfig", EnvVars: []string{"KUBECONFIG"}, Usage: "kubeconfig of the kind cluster"},
		&cli.Float64Flag{Name: "kube-api-qps", Value: 500, Usage: "client QPS of scalebench, kept above what it measures"},
		&cli.IntFlag{Name: "kube-api-burst", Value: 1000},
		&cli.StringFlag{Name: "driver-namespace", Value: "dra-driver-nvidia-gpu", EnvVars: []string{"DRIVER_NAMESPACE"}},
	}
}

type clients struct {
	core   kubernetes.Interface
	nvidia nvclientset.Interface
	// driverNamespace runs the controller and the IMEX daemons.
	driverNamespace string
}

func newClients(c *cli.Context) (*clients, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if path := c.String("kubeconfig"); path != "" {
		rules.ExplicitPath = path
	}
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("error loading kubeconfig: %w", err)
	}
	config.QPS = float32(c.Float64("kube-api-qps"))
	config.Burst = c.Int("kube-api-burst")
	config.UserAgent = "scalebench"
	core, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("error creating clientset: %w", err)
	}
	nvidia, err := nvclientset.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("error creating nvidia clientset: %w", err)
	}
	return &clients{core: core, nvidia: nvidia, driverNamespace: c.String("driver-namespace")}, nil
}

// driverMode is how the installed driver runs, read from its controller, so
// that fake nodes publish what its kubelet plugins would.
type driverMode struct {
	Lifecycle    imex.DaemonLifecycle `json:"lifecycle"`
	Reservations bool                 `json:"reservations"`
	FeatureGates string               `json:"featureGates"`
	KubeAPIQPS   string               `json:"controllerKubeAPIQPS"`
	Image        string               `json:"image"`
}

func (m driverMode) config() imex.Config {
	return imex.Config{DaemonLifecycle: m.Lifecycle}
}

// Name names the mode in results, as tests/bats does.
func (m driverMode) Name() string {
	switch {
	case m.Lifecycle == imex.DaemonLifecycleStatic:
		return "static"
	case m.Reservations:
		return "per-cd-reservations"
	default:
		return "per-cd"
	}
}

func detectDriverMode(ctx context.Context, cs *clients) (driverMode, error) {
	// The chart sets the component label only on the controller's pods.
	selector, err := labels.Parse(controllerComponentLabel)
	if err != nil {
		return driverMode{}, err
	}
	deployments, err := cs.core.AppsV1().Deployments(cs.driverNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return driverMode{}, fmt.Errorf("error listing controller Deployments: %w", err)
	}
	var controllers []*appsv1.Deployment
	for i := range deployments.Items {
		if selector.Matches(labels.Set(deployments.Items[i].Spec.Template.Labels)) {
			controllers = append(controllers, &deployments.Items[i])
		}
	}
	if len(controllers) != 1 {
		return driverMode{}, fmt.Errorf("found %d controller Deployments in namespace %s, want 1: is the driver installed (scripts/install-driver.sh)?", len(controllers), cs.driverNamespace)
	}
	return modeFromDeployment(controllers[0])
}

func modeFromDeployment(d *appsv1.Deployment) (driverMode, error) {
	mode := driverMode{Lifecycle: imex.DaemonLifecyclePerComputeDomain}
	for _, container := range d.Spec.Template.Spec.Containers {
		if container.Name != "compute-domain" {
			continue
		}
		mode.Image = container.Image
		for _, env := range container.Env {
			switch env.Name {
			case "IMEX_DAEMON_LIFECYCLE":
				if env.Value != "" {
					mode.Lifecycle = imex.DaemonLifecycle(env.Value)
				}
			case "FEATURE_GATES":
				mode.FeatureGates = strings.TrimSuffix(env.Value, ",")
			case "KUBE_API_QPS":
				mode.KubeAPIQPS = env.Value
			}
		}
	}
	if mode.Lifecycle == imex.DaemonLifecycleHostManaged {
		return driverMode{}, fmt.Errorf("the scale tests do not cover host-managed IMEX daemons, install the driver with scripts/install-driver.sh")
	}
	gates, err := parseGates(mode.FeatureGates)
	if err != nil {
		return driverMode{}, err
	}
	mode.Reservations = gates["ComputeDomainChannelReservations"]
	return mode, nil
}

func parseGates(s string) (map[string]bool, error) {
	gates := make(map[string]bool)
	for kv := range strings.SplitSeq(s, ",") {
		if kv == "" {
			continue
		}
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			return nil, fmt.Errorf("invalid feature gate %q", kv)
		}
		enabled, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("invalid feature gate %q: %w", kv, err)
		}
		gates[k] = enabled
	}
	return gates, nil
}
