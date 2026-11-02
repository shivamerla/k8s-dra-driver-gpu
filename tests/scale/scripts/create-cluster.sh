#!/usr/bin/env bash
# Copyright The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Creates a one-node kind cluster sized for thousands of fake nodes, exposes
# kube-scheduler metrics, and installs KWOK.

source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"

if kind get clusters 2> /dev/null | grep -qx "${KIND_CLUSTER_NAME}"; then
  log "kind cluster ${KIND_CLUSTER_NAME} exists, reusing it"
  kind export kubeconfig --name "${KIND_CLUSTER_NAME}" --kubeconfig "${KUBECONFIG}"
else
  # The scheduler reads its client rate limits from a config file that the
  # node mounts from the host, so it stays for the life of the cluster.
  work="${SCALE_STATE_DIR}/${KIND_CLUSTER_NAME}"
  mkdir -p "${work}/scheduler"
  cat > "${work}/scheduler/config.yaml" << EOF
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
parallelism: ${SCHEDULER_PARALLELISM}
clientConnection:
  kubeconfig: /etc/kubernetes/scheduler.conf
  qps: ${SCHEDULER_QPS}
  burst: ${SCHEDULER_BURST}
leaderElection:
  leaderElect: true
EOF

  gates_arg() {
    if [[ -n "${SCALE_FEATURE_GATES}" ]]; then
      printf '      - name: feature-gates\n        value: "%s"\n' "${SCALE_FEATURE_GATES}"
    fi
  }

  # kind merges these patches into its kubeadm config. Lists are replaced,
  # so each extraArgs list repeats the feature gates.
  cat > "${work}/kind.yaml" << EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
networking:
  podSubnet: "${POD_SUBNET}"
nodes:
- role: control-plane
  extraMounts:
  - hostPath: ${work}/scheduler
    containerPath: /etc/kubernetes/scale
    readOnly: true
  kubeadmConfigPatches:
  - |
    apiVersion: kubeadm.k8s.io/v1beta4
    kind: ClusterConfiguration
    apiServer:
      extraArgs:
$(gates_arg)
      - name: max-requests-inflight
        value: "2000"
      - name: max-mutating-requests-inflight
        value: "1000"
    controllerManager:
      extraArgs:
$(gates_arg)
      - name: enable-hostpath-provisioner
        value: "true"
      - name: kube-api-qps
        value: "500"
      - name: kube-api-burst
        value: "1000"
    scheduler:
      extraArgs:
$(gates_arg)
      - name: config
        value: /etc/kubernetes/scale/config.yaml
      - name: bind-address
        value: "0.0.0.0"
      - name: authorization-always-allow-paths
        value: /healthz,/readyz,/livez,/metrics
      extraVolumes:
      - name: scale-config
        hostPath: /etc/kubernetes/scale
        mountPath: /etc/kubernetes/scale
        readOnly: true
        pathType: Directory
EOF

  log "creating kind cluster ${KIND_CLUSTER_NAME} (${KIND_IMAGE})"
  kind create cluster --name "${KIND_CLUSTER_NAME}" --image "${KIND_IMAGE}" \
      --config "${work}/kind.yaml" --kubeconfig "${KUBECONFIG}" --wait 5m
fi

log "installing KWOK ${KWOK_VERSION}"
kwok_url="https://github.com/kubernetes-sigs/kwok/releases/download/${KWOK_VERSION}"
kubectl apply -f "${kwok_url}/kwok.yaml"
kubectl apply -f "${kwok_url}/stage-fast.yaml"
kubectl -n kube-system rollout status deployment/kwok-controller --timeout=5m

log "cluster ready, KUBECONFIG=${KUBECONFIG}"
