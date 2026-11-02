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

# Usage: install-driver.sh MODE
#
# (Re)installs the driver from this checkout in one of the modes the scale
# tests compare:
#   per-cd               per-ComputeDomain IMEX daemons, no channel reservations
#   per-cd-reservations  per-ComputeDomain IMEX daemons, binding conditions
#   static               static IMEX daemons (ComputeDomainDaemonSet)
#
# Switching modes uninstalls the driver first, after removing what the
# previous mode deployed, as switching IMEX daemon lifecycles requires.
# Set BUILD=true to rebuild the driver image from this checkout.

source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"

MODE="${1:-}"
reservations=false cdds=false lifecycle=perComputeDomain
case "${MODE}" in
  per-cd) ;;
  per-cd-reservations) reservations=true ;;
  static) lifecycle=static cdds=true reservations=true ;;
  *)
    echo "usage: $0 per-cd|per-cd-reservations|static" >&2
    exit 2
    ;;
esac

image_present() {
  docker image inspect "${DRIVER_IMAGE}" > /dev/null 2>&1
}

if [[ "${BUILD:-false}" == true ]] || ! image_present; then
  log "building ${DRIVER_IMAGE}"
  make -C "${PROJECT_DIR}" -f deployments/container/Makefile build \
    IMAGE_NAME="${DRIVER_IMAGE_REPO}" IMAGE_TAG="${DRIVER_IMAGE_TAG}"
fi
log "loading ${DRIVER_IMAGE} into kind"
kind load docker-image --name "${KIND_CLUSTER_NAME}" "${DRIVER_IMAGE}"

# Remove what the previous mode deployed: workloads of earlier runs, then the
# ComputeDomainDaemonSet with its static daemons.
# The chart sets the component label only on the controller's pods.
current_lifecycle=$(kubectl -n "${DRIVER_NAMESPACE}" get pods \
  -l dra-driver-nvidia-gpu-component=controller \
  -o jsonpath='{.items[0].spec.containers[?(@.name=="compute-domain")].env[?(@.name=="IMEX_DAEMON_LIFECYCLE")].value}' 2> /dev/null || true)
log "deleting workloads of earlier runs"
kubectl delete namespaces -l scale.nvidia.com/run --wait=true --timeout=10m
# Node owners of per-ComputeDomain modes outlive a switch to a mode that does
# not release them.
kubectl label nodes -l scale.nvidia.com/node resource.nvidia.com/computeDomain- > /dev/null
if [[ -n "${current_lifecycle}" && "${current_lifecycle}" != "${lifecycle}" ]]; then
  if kubectl get crd computedomaindaemonsets.resource.nvidia.com > /dev/null 2>&1; then
    kubectl delete computedomaindaemonsets --all --wait=true --timeout=10m
  fi
  log "uninstalling the driver (${current_lifecycle}) to switch to ${lifecycle}"
  helm uninstall -n "${DRIVER_NAMESPACE}" "${DRIVER_RELEASE}" --wait --timeout 5m
fi

log "installing the driver in mode ${MODE}"
helm upgrade --install "${DRIVER_RELEASE}" "${PROJECT_DIR}/deployments/helm/dra-driver-nvidia-gpu" \
  --namespace "${DRIVER_NAMESPACE}" --create-namespace \
  --set image.repository="${DRIVER_IMAGE_REPO}" \
  --set image.tag="${DRIVER_IMAGE_TAG}" \
  --set image.pullPolicy=IfNotPresent \
  --set resources.gpus.enabled=false \
  --set logVerbosity=2 \
  --set controller.kubeAPI.qps="${CONTROLLER_QPS}" \
  --set controller.kubeAPI.burst="${CONTROLLER_BURST}" \
  --set featureGates.ComputeDomainChannelReservations="${reservations}" \
  --set featureGates.ComputeDomainDaemonSet="${cdds}" \
  --set resources.computeDomains.imex.daemonLifecycle="${lifecycle}" \
  --wait --timeout 5m

build_scalebench
"${SCALEBENCH}" preflight
