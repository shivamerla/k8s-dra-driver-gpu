# shellcheck shell=bash
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

# Settings of the scale test scripts. Override any of them in the
# environment.

set -euo pipefail

SCALE_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." &> /dev/null && pwd)"
PROJECT_DIR="$(cd -- "${SCALE_DIR}/../.." &> /dev/null && pwd)"

# Kubernetes 1.36+ serves binding conditions and multiple allocations by
# default; the kind config below uses the kubeadm v1beta4 format of 1.36+.
: "${KIND_CLUSTER_NAME:=dra-scale}"
: "${K8S_VERSION:=v1.37.0}"
: "${KIND_IMAGE:=kindest/node:${K8S_VERSION}}"
: "${KWOK_VERSION:=v0.8.0}"
# Extra Kubernetes feature gates, for example "DRADeviceBindingConditions=true"
# on clusters where they are off by default.
: "${SCALE_FEATURE_GATES:=}"
# kube-scheduler client rate limits, raised so that the scheduler's own client
# does not cap what the tests measure.
: "${SCHEDULER_QPS:=500}"
: "${SCHEDULER_BURST:=1000}"
# Goroutines the scheduler filters and scores nodes with, raised from the
# upstream 16 as large clusters tune it, so that the scheduler's cores do not
# cap what the tests measure.
: "${SCHEDULER_PARALLELISM:=48}"
# Pod CIDR large enough for one /24 per fake node (16k nodes).
: "${POD_SUBNET:=100.64.0.0/10}"

: "${DRIVER_NAMESPACE:=dra-driver-nvidia-gpu}"
: "${DRIVER_RELEASE:=dra-driver-nvidia-gpu}"
# The chart names the controller Deployment after the chart.
: "${CONTROLLER_DEPLOYMENT:=dra-driver-nvidia-gpu-controller}"
: "${DRIVER_IMAGE_REPO:=localhost/dra-driver-nvidia-gpu}"
: "${DRIVER_IMAGE_TAG:=scale-$(git -C "${PROJECT_DIR}" rev-parse --short HEAD)}"
: "${DRIVER_IMAGE:=${DRIVER_IMAGE_REPO}:${DRIVER_IMAGE_TAG}}"
# The controller's client rate limits; they bound how fast claims are
# admitted, so keep them at the chart default unless that is what you test.
: "${CONTROLLER_QPS:=100}"
: "${CONTROLLER_BURST:=200}"

: "${SCALE_STATE_DIR:=${SCALE_DIR}/.state}"
: "${KUBECONFIG:=${SCALE_STATE_DIR}/kubeconfig}"
export KUBECONFIG

SCALEBENCH="${SCALE_DIR}/bin/scalebench"

log() { echo "[$(date +%H:%M:%S)] $*" >&2; }

build_scalebench() {
  (cd "${PROJECT_DIR}" && go build -o "${SCALEBENCH}" ./tests/scale/cmd/scalebench)
}

# How long ensure_idle waits for the workloads of earlier runs to go, before
# and after restarting the controller.
: "${IDLE_TIMEOUT:=15m}"

# leftovers lists what earlier runs left behind: their namespaces and the
# daemon DaemonSets of their ComputeDomains.
leftovers() {
  kubectl get namespaces -l scale.nvidia.com/run -o name
  kubectl -n "${DRIVER_NAMESPACE}" get daemonsets -l resource.nvidia.com/computeDomain -o name
}

# seconds converts a duration such as 90s, 15m or 1h to seconds.
seconds() {
  case "$1" in
    *h) echo $((${1%h} * 3600)) ;;
    *m) echo $((${1%m} * 60)) ;;
    *s) echo "${1%s}" ;;
    *) echo "$1" ;;
  esac
}

wait_for_no_leftovers() {
  local deadline=$((SECONDS + $(seconds "$1")))
  while [[ -n "$(leftovers 2> /dev/null)" ]]; do
    ((SECONDS < deadline)) || return 1
    sleep 5
  done
}

# ensure_idle waits until the workloads of earlier runs are gone, so that a
# slow teardown cannot fail or slow down the next run. If they are still
# there after IDLE_TIMEOUT, it restarts the compute-domain-controller, whose
# work queue then holds a backlog of their deletions, and waits again.
# Finally it removes node owners no ComputeDomain uses anymore.
ensure_idle() {
  if [[ -n "$(leftovers 2> /dev/null)" ]]; then
    log "waiting up to ${IDLE_TIMEOUT} for the workloads of earlier runs to go"
    if ! wait_for_no_leftovers "${IDLE_TIMEOUT}"; then
      log "still there: $(leftovers 2> /dev/null | tr '\n' ' ')"
      log "restarting the compute-domain-controller to clear its backlog"
      kubectl -n "${DRIVER_NAMESPACE}" rollout restart deployment "${CONTROLLER_DEPLOYMENT}"
      kubectl -n "${DRIVER_NAMESPACE}" rollout status deployment "${CONTROLLER_DEPLOYMENT}" --timeout 5m
      if ! wait_for_no_leftovers "${IDLE_TIMEOUT}"; then
        log "ERROR: earlier runs are not cleaned up: $(leftovers 2> /dev/null | tr '\n' ' ')"
        return 1
      fi
    fi
  fi
  kubectl label nodes -l scale.nvidia.com/node resource.nvidia.com/computeDomain- > /dev/null
}
