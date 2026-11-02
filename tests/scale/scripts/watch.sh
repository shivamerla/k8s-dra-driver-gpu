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

# Usage: watch.sh [interval seconds]
#
# Prints the state of a scale run every interval: fake nodes and cliques,
# the ComputeDomainDaemonSet, IMEX daemon pods and their clique
# registrations, channel claims by admission state, workload pods, and the
# scheduler's and controller's queues. Run it in a second terminal.

source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"

INTERVAL="${1:-5}"
: "${CONTROLLER_METRICS_PORT:=8080}"

counts() { sort | uniq -c | awk '{printf "  %-28s %s\n", $2, $1}'; }

metric() {
  # metric RAW_PATH NAME [LABEL_FILTER]: sums the samples of a metric.
  kubectl get --raw "$1" 2>/dev/null |
    awk -v name="$2" -v filter="${3:-}" '$1 ~ "^"name"({|$)" && index($1, filter) {sum += $2} END {printf "%d", sum}'
}

snapshot() {
  echo "=== $(date +%H:%M:%S)  cluster ${KIND_CLUSTER_NAME}"

  local nodes cliques
  nodes=$(kubectl get nodes -l scale.nvidia.com/node -L nvidia.com/gpu.clique --no-headers 2>/dev/null)
  cliques=$(awk '{print $NF}' <<<"${nodes}" | sort -u | grep -vc '^$' || true)
  echo "fake nodes: $(grep -c . <<<"${nodes}" || true), ready: $(grep -c ' Ready ' <<<"${nodes}" || true), cliques: ${cliques}"

  echo
  echo "ComputeDomainDaemonSet:"
  kubectl get computedomaindaemonsets 2>/dev/null | sed 's/^/  /' || echo "  none"
  kubectl get computedomaindaemonsets -o jsonpath='{range .items[*].status.cliques[?(@.stalledReason)]}  stalled {.id}: {.stalledReason}{"\n"}{end}' 2>/dev/null

  echo
  echo "IMEX daemon pods (${DRIVER_NAMESPACE}):"
  kubectl get pods -n "${DRIVER_NAMESPACE}" -l resource.nvidia.com/computeDomainDaemonSet --no-headers 2>/dev/null |
    awk '{print $3 "/" $2}' | counts

  echo
  echo "Clique registrations (ComputeDomainCliques: $(kubectl get computedomaincliques -A --no-headers 2>/dev/null | grep -c . || true)):"
  kubectl get computedomaincliques -A -o jsonpath='{range .items[*]}{range .daemons[*]}{.status}{"\n"}{end}{end}' 2>/dev/null | counts

  echo
  echo "ComputeDomains: $(kubectl get computedomains -A --no-headers 2>/dev/null | grep -c . || true)"
  echo "Channel claims:"
  # driver|true condition types per claim; a claim without allocation is pending.
  kubectl get resourceclaims -A -o jsonpath='{range .items[*]}{.status.allocation.devices.results[*].driver}{"|"}{.status.devices[*].conditions[?(@.status=="True")].type}{"\n"}{end}' 2>/dev/null |
    awk -F'|' '$1 == "" {print "pending"; next}
      $1 !~ /compute-domain.nvidia.com/ {next}
      $2 ~ /ChannelRejected/ {print "rejected"; next}
      $2 ~ /ChannelAdmitted/ {print "admitted"; next}
      {print "allocated(awaiting-admission)"}' | counts

  echo
  echo "Workload pods:"
  kubectl get pods -A --no-headers 2>/dev/null | awk '$1 ~ /^scale-/ {print $4}' | counts

  echo
  local scheduler controller
  scheduler=$(kubectl get pods -n kube-system -l component=kube-scheduler -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
  controller=$(kubectl get pods -n "${DRIVER_NAMESPACE}" -l dra-driver-nvidia-gpu-component=controller --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
  if [[ -n "${scheduler}" ]]; then
    local s="/api/v1/namespaces/kube-system/pods/https:${scheduler}:10259/proxy/metrics"
    echo "scheduler pending pods: active $(metric "$s" scheduler_pending_pods 'queue="active"'), backoff $(metric "$s" scheduler_pending_pods 'queue="backoff"'), unschedulable $(metric "$s" scheduler_pending_pods 'queue="unschedulable"'), gated $(metric "$s" scheduler_pending_pods 'queue="gated"')"
    echo "scheduler attempts: scheduled $(metric "$s" scheduler_schedule_attempts_total 'result="scheduled"'), unschedulable $(metric "$s" scheduler_schedule_attempts_total 'result="unschedulable"'), error $(metric "$s" scheduler_schedule_attempts_total 'result="error"')"
  fi
  if [[ -n "${controller}" ]]; then
    local c="/api/v1/namespaces/${DRIVER_NAMESPACE}/pods/http:${controller}:${CONTROLLER_METRICS_PORT}/proxy/metrics"
    echo "controller queue depth: claim-admission $(metric "$c" workqueue_depth 'name="compute-domain-claim-admission"'), daemonset $(metric "$c" workqueue_depth 'name="compute-domain-daemonset"')"
  fi
}

while true; do
  out=$(snapshot)
  clear
  echo "${out}"
  sleep "${INTERVAL}"
done
