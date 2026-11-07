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

# Usage: demo.sh [all|static|channels|health|upgrade|board|cleanup]
#
# A three minute walk through static IMEX daemons and ComputeDomain channel
# reservations on two GB200 nodes of one NVLink clique:
#
#   static    IMEX daemons run on every node before any workload
#   channels  two ComputeDomains share a node, each template gets its channel
#   health    a tainted GPU evicts one pod, which moves to the other node
#   upgrade   the daemons roll out by clique while workloads keep running
#
# The driver must run with imex.daemonLifecycle=static, the
# ComputeDomainDaemonSet and ComputeDomainChannelReservations feature gates,
# and the default ComputeDomainDaemonSet. The health scene needs the
# DRADeviceTaints and DRADeviceTaintRules gates of Kubernetes. See README.md.

set -euo pipefail

: "${DEMO_NS:=imex-demo}"
: "${DEMO_DRIVER_NS:=dra-driver-nvidia-gpu}"
: "${DEMO_IMAGE:=ubuntu:24.04}"
# Two nodes of one clique, for example "gb200-a gb200-b". Detected when empty.
: "${DEMO_NODES:=}"
# Set to 1 to run without waiting for Enter between steps.
: "${DEMO_AUTO:=0}"
# Set to 1 to recreate the ComputeDomainDaemonSet in the static scene.
: "${DEMO_FRESH_DAEMONS:=0}"
# How long the upgrade scene watches the rollout.
: "${DEMO_ROLLOUT_WATCH:=90}"
: "${DEMO_TIMEOUT:=180}"

DEMO_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
CDDS_SPEC="${DEMO_DIR}/../computedomaindaemonset.yaml"
DAEMON_SELECTOR="resource.nvidia.com/computeDomainDaemonSet"

R=$'\e[0m' B=$'\e[1m' D=$'\e[2m'
RED=$'\e[31m' GRN=$'\e[32m' YLW=$'\e[33m' BLU=$'\e[34m' MAG=$'\e[35m' CYN=$'\e[36m'
# Cell colors of the board, per ComputeDomain.
ALPHA_BG=$'\e[1;97;44m' BETA_BG=$'\e[1;97;45m' FREE_BG=$'\e[2;37;100m' TAINT_BG=$'\e[1;97;41m'

say() { printf '%s\n' "$*"; }
step() { printf '\n%s▸ %s%s\n' "${B}${CYN}" "$*" "${R}"; }
note() { printf '  %s%s%s\n' "${D}" "$*" "${R}"; }
ok() { printf '  %s✔ %s%s\n' "${GRN}" "$*" "${R}"; }
fail() { printf '  %s✘ %s%s\n' "${RED}" "$*" "${R}" >&2; exit 1; }

pause() {
  [[ "${DEMO_AUTO}" == 1 ]] && return 0
  printf '%s  ⏎%s' "${D}" "${R}"
  read -rs _
  printf '\r    \r'
}

# show_apply prints the manifest on stdin, then applies it.
show_apply() {
  local manifest
  manifest=$(cat)
  printf '%s%s%s\n' "${D}" "$(sed 's/^/    /' <<< "${manifest}")" "${R}"
  kubectl apply -f - <<< "${manifest}" > /dev/null
}

# run prints a command, then runs it.
run() {
  printf '%s$ %s%s\n' "${YLW}" "$*" "${R}"
  eval "$@"
}

banner() {
  local title="$1" sub="$2" line
  line=$(printf '━%.0s' $(seq 1 72))
  printf '\n%s%s\n  %s\n  %s%s%s\n%s%s\n' "${B}${MAG}" "${line}" "${title}" "${R}${D}" "${sub}" "${R}${B}${MAG}" "${line}" "${R}"
}

need() {
  command -v "$1" > /dev/null || fail "$1 is required"
}

detect_nodes() {
  if [[ -n "${DEMO_NODES}" ]]; then
    read -r NODE1 NODE2 <<< "${DEMO_NODES}"
  else
    read -r NODE1 NODE2 < <(kubectl get nodes -l nvidia.com/gpu.clique -o json | jq -r '
      [.items[] | {n: .metadata.name, c: .metadata.labels["nvidia.com/gpu.clique"]}]
      | group_by(.c) | map(select(length >= 2)) | first // [] | map(.n) | sort | .[:2] | join(" ")')
  fi
  [[ -n "${NODE1:-}" && -n "${NODE2:-}" ]] || fail "need two nodes of one clique, set DEMO_NODES"
  CLIQUE=$(kubectl get node "${NODE1}" -o jsonpath='{.metadata.labels.nvidia\.com/gpu\.clique}')
}

preflight() {
  need kubectl
  need jq
  detect_nodes
  kubectl get crd computedomaindaemonsets.resource.nvidia.com > /dev/null 2>&1 ||
    fail "ComputeDomainDaemonSet CRD not found: install the driver with imex.daemonLifecycle=static"
  kubectl get resourceslices -o json | jq -e '[.items[] | select(.spec.driver == "compute-domain.nvidia.com")
    | .spec.devices[]? | select(.allowMultipleAllocations == true)] | length > 0' > /dev/null ||
    fail "no shared channel device: enable featureGates.ComputeDomainChannelReservations"
  [[ "${DEMO_FRESH_DAEMONS}" == 1 ]] || kubectl get computedomaindaemonset default > /dev/null 2>&1 ||
    fail "no ComputeDomainDaemonSet: kubectl apply -f ${CDDS_SPEC}, or set DEMO_FRESH_DAEMONS=1"
}

# ---------------------------------------------------------------- the board

# board draws each demo node with its IMEX daemon and one cell per GPU: the
# ComputeDomain and channel template using it, and the IMEX channel injected.
board() {
  local pods claims slices daemons rules
  pods=$(kubectl get pods -n "${DEMO_NS}" -o json 2> /dev/null || echo '{"items":[]}')
  claims=$(kubectl get resourceclaims -n "${DEMO_NS}" -o json 2> /dev/null || echo '{"items":[]}')
  slices=$(kubectl get resourceslices -o json)
  daemons=$(kubectl get pods -n "${DEMO_DRIVER_NS}" -l "${DAEMON_SELECTOR}" -o json)
  rules=$(taint_rules_json)

  local node
  for node in "${NODE1}" "${NODE2}"; do
    local daemon
    daemon=$(jq -r --arg n "${node}" '[.items[] | select(.spec.nodeName == $n and .metadata.deletionTimestamp == null)][0]
      | if . == null then "none"
        elif ([.status.containerStatuses[]?.ready] | all) and (.status.containerStatuses | length > 0) then "ready \(.metadata.name)"
        else "starting \(.metadata.name)" end' <<< "${daemons}")
    local state="${daemon%% *}" dcolor="${YLW}"
    [[ "${state}" == ready ]] && dcolor="${GRN}"
    [[ "${state}" == none ]] && dcolor="${RED}"
    printf '\n  %s%-24s%s %sclique %s%s   IMEX daemon %s● %s%s\n' "${B}" "${node}" "${R}" "${D}" "${CLIQUE##*.}" "${R}" "${dcolor}" "${state}" "${R}"

    # One row per GPU: name, cd, template, channel, pod, tainted.
    local rows
    rows=$(jq -r -n --arg n "${node}" --argjson pods "${pods}" --argjson claims "${claims}" \
      --argjson slices "${slices}" --argjson rules "${rules}" '
      ($claims.items | map({key: .metadata.name, value: .}) | from_entries) as $c
      | [$pods.items[] | select(.spec.nodeName == $n and .metadata.deletionTimestamp == null)
         | . as $p
         | [.status.resourceClaimStatuses[]? | $c[.resourceClaimName] // empty] as $pc
         | ([$pc[].status.devices[]? | select(.device == "channel") | .data.channel] | first // "?") as $ch
         | $pc[].status.allocation.devices.results[]? | select(.driver == "gpu.nvidia.com")
         | {gpu: .device, cd: $p.metadata.labels.cd, tmpl: $p.metadata.labels.template, ch: $ch,
            pod: $p.metadata.labels.app, phase: $p.status.phase}] as $used
      | [$rules[] | select(.pool == $n) | .device] as $tainted
      | [$slices.items[] | select(.spec.driver == "gpu.nvidia.com" and .spec.pool.name == $n)
         | .spec.devices[].name | select(test("^gpu-[0-9]+$"))] | unique | sort_by(ltrimstr("gpu-") | tonumber)[]
      | . as $g | ($used | map(select(.gpu == $g)) | first) as $u
      | [$g, ($u.cd // "-"), ($u.tmpl // "-"), ($u.ch // "-" | tostring), ($u.pod // "-"), ($u.phase // "-"),
         (if ($tainted | index($g)) then "x" else "-" end)] | @tsv')

    # Four text lines per cell, built side by side.
    local o1="  " o2="  " o3="  " o4="  " t1 t2 t3 t4
    local gpu cd tmpl ch pod phase tainted bg
    while IFS=$'\t' read -r gpu cd tmpl ch pod phase tainted; do
      [[ -z "${gpu}" ]] && continue
      t1="${gpu}" t2="" t3="" t4=""
      if [[ "${cd}" == - ]]; then
        bg="${FREE_BG}" t2="free"
        [[ "${tainted}" == x ]] && bg="${TAINT_BG}" t1="${gpu}  XID 79" t2="unhealthy"
      else
        bg="${ALPHA_BG}"
        [[ "${cd}" == beta ]] && bg="${BETA_BG}"
        [[ "${tainted}" == x ]] && bg="${TAINT_BG}" t1="${gpu}  XID 79"
        t2="${cd} / ${tmpl}" t3="channel ${ch}" t4="${pod}"
      fi
      o1+=$(printf '%s %-17.17s %s ' "${bg}" "${t1}" "${R}")
      o2+=$(printf '%s %-17.17s %s ' "${bg}" "${t2}" "${R}")
      o3+=$(printf '%s %-17.17s %s ' "${bg}" "${t3}" "${R}")
      o4+=$(printf '%s %-17.17s %s ' "${bg}" "${t4}" "${R}")
    done <<< "${rows}"
    printf '%s\n%s\n%s\n%s\n' "${o1}" "${o2}" "${o3}" "${o4}"
  done
  printf '  %s alpha %s %s beta %s %s free %s %s tainted %s   %spods of one template share its channel; templates never do%s\n' \
    "${ALPHA_BG}" "${R}" "${BETA_BG}" "${R}" "${FREE_BG}" "${R}" "${TAINT_BG}" "${R}" "${D}" "${R}"
}

# ---------------------------------------------------------------- workloads

# workload prints a Deployment of one pod of ComputeDomain $1 using channel
# template $2 and $3 GPUs, allowed on nodes $4.
workload() {
  local cd="$1" tmpl="$2" gpus="$3" nodes="$4" name="$5"
  cat << EOF
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${name}
  namespace: ${DEMO_NS}
  labels: {demo: imex, cd: ${cd}, template: ${tmpl}}
spec:
  replicas: 1
  strategy: {type: Recreate}
  selector: {matchLabels: {app: ${name}}}
  template:
    metadata:
      labels: {demo: imex, app: ${name}, cd: ${cd}, template: ${tmpl}}
    spec:
      terminationGracePeriodSeconds: 2
      tolerations: [{key: nvidia.com/gpu, operator: Exists}]
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
            - matchExpressions: [{key: kubernetes.io/hostname, operator: In, values: [${nodes}]}]
      containers:
      - name: app
        image: ${DEMO_IMAGE}
        command: ["bash", "-c"]
        args:
        - |
          echo "IMEX channels: \$(ls /dev/nvidia-caps-imex-channels)"
          nvidia-smi -L 2> /dev/null || true
          trap 'exit 0' TERM; sleep infinity & wait
        resources:
          claims: [{name: channel}, {name: gpus}]
      resourceClaims:
      - {name: channel, resourceClaimTemplateName: ${cd}-${tmpl}}
      - {name: gpus, resourceClaimTemplateName: gpus-${gpus}}
EOF
}

gpu_templates() {
  local n
  for n in 1 2; do
    cat << EOF
---
apiVersion: resource.k8s.io/v1
kind: ResourceClaimTemplate
metadata: {name: gpus-${n}, namespace: ${DEMO_NS}}
spec:
  spec:
    devices:
      requests:
      - name: gpu
        exactly: {deviceClassName: gpu.nvidia.com, allocationMode: ExactCount, count: ${n}}
EOF
  done
}

wait_available() {
  kubectl wait deploy -n "${DEMO_NS}" "$@" --for=condition=Available --timeout="${DEMO_TIMEOUT}s" > /dev/null
}

# ---------------------------------------------------------------- scenes

scene_static() {
  banner "1 · Static IMEX daemons" "One IMEX daemon per node, shared by every ComputeDomain, up before any workload"
  if [[ "${DEMO_FRESH_DAEMONS}" == 1 ]]; then
    step "The cluster admin creates the ComputeDomainDaemonSet once"
    kubectl delete computedomaindaemonset default --ignore-not-found --wait=true > /dev/null
    run kubectl apply -f "${CDDS_SPEC}"
    run kubectl wait computedomaindaemonset default --for=condition=Ready --timeout=600s
  fi
  step "No workloads yet, but every node of the clique already runs its IMEX daemon"
  run kubectl get computedomaindaemonset
  board
  ok "a workload never waits for an IMEX daemon to start"
  pause
}

scene_channels() {
  banner "2 · Channel reservations" "Several ComputeDomains on one node, every channel template isolated"
  kubectl create namespace "${DEMO_NS}" --dry-run=client -o yaml | kubectl apply -f - > /dev/null
  gpu_templates | kubectl apply -f - > /dev/null

  step "Two ComputeDomains: alpha with two channel templates, beta with one"
  show_apply << EOF
apiVersion: resource.nvidia.com/v1beta1
kind: ComputeDomain
metadata: {name: alpha, namespace: ${DEMO_NS}}
spec:
  channels:
  - resourceClaimTemplate: {name: alpha-train}
  - resourceClaimTemplate: {name: alpha-eval}
---
apiVersion: resource.nvidia.com/v1beta1
kind: ComputeDomain
metadata: {name: beta, namespace: ${DEMO_NS}}
spec:
  channels:
  - resourceClaimTemplate: {name: beta-train}
EOF
  pause

  step "Pods with 1 or 2 GPUs each, packed onto ${NODE1}, plus one alpha/train pod on ${NODE2}"
  {
    workload alpha train 1 "${NODE1}" alpha-train-a
    workload alpha eval 1 "${NODE1}" alpha-eval-a
    workload alpha train 1 "${NODE2}" alpha-train-b
  } | kubectl apply -f - > /dev/null
  wait_available -l cd=alpha
  # beta may also run on NODE2, where it moves in the health scene. Cordoning
  # NODE2 while it starts makes it start next to alpha on NODE1.
  kubectl cordon "${NODE2}" > /dev/null
  workload beta train 2 "${NODE1},${NODE2}" beta-train | kubectl apply -f - > /dev/null
  wait_available -l cd=beta || { kubectl uncordon "${NODE2}" > /dev/null; fail "beta did not start"; }
  kubectl uncordon "${NODE2}" > /dev/null
  board
  pause

  step "Inside the pods: each sees only its own channel"
  local d
  for d in alpha-train-a alpha-eval-a beta-train alpha-train-b; do
    printf '  %s%-14s%s %s\n' "${B}" "${d}" "${R}" "$(kubectl exec -n "${DEMO_NS}" "deploy/${d}" -- ls /dev/nvidia-caps-imex-channels | tr '\n' ' ')"
  done
  ok "3 isolated channels on one node, 2 ComputeDomains, 4 GPUs fully used"
  ok "alpha/train has the same channel on both nodes, so its pods talk over NVLink"
  pause
}

taint_api() {
  kubectl api-resources --api-group=resource.k8s.io --no-headers 2> /dev/null |
    awk '$NF == "DeviceTaintRule" {print $(NF-2)}'
}

taint_rules_json() {
  kubectl get devicetaintrules -l demo=imex -o json 2> /dev/null |
    jq -c '[.items[].spec.deviceSelector | {pool, device}]' 2> /dev/null || echo '[]'
}

scene_health() {
  banner "3 · GPU health" "A failed GPU evicts only the pod using it; the pod moves, everyone else keeps running"
  local api
  api=$(taint_api)
  [[ -n "${api}" ]] || fail "DeviceTaintRule API not found: enable the DRADeviceTaints and DRADeviceTaintRules gates"

  local claim pool device
  claim=$(kubectl get pods -n "${DEMO_NS}" -l app=beta-train -o json |
    jq -r '.items[0].status.resourceClaimStatuses[] | select(.name == "gpus") | .resourceClaimName')
  read -r pool device < <(kubectl get resourceclaim -n "${DEMO_NS}" "${claim}" -o json |
    jq -r '.status.allocation.devices.results[0] | "\(.pool) \(.device)"')
  local before
  before=$(kubectl get pods -n "${DEMO_NS}" -l cd=alpha -o jsonpath='{range .items[*]}{.metadata.uid}{"\n"}{end}' | sort)

  step "${device} on ${pool} reports a fatal XID: taint it NoExecute"
  show_apply << EOF
apiVersion: ${api}
kind: DeviceTaintRule
metadata: {name: xid-${pool}-${device}, labels: {demo: imex}}
spec:
  deviceSelector: {driver: gpu.nvidia.com, pool: ${pool}, device: ${device}}
  taint: {key: gpu.nvidia.com/xid, value: "79", effect: NoExecute}
EOF

  # Redraw until beta runs again, on the node with two healthy GPUs free.
  local deadline=$((SECONDS + DEMO_TIMEOUT)) node=""
  while ((SECONDS < deadline)); do
    node=$(kubectl get pods -n "${DEMO_NS}" -l app=beta-train -o json | jq -r '[.items[]
      | select(.metadata.deletionTimestamp == null and .status.phase == "Running"
               and ([.status.containerStatuses[]?.ready] | all))][0].spec.nodeName // ""')
    printf '\e[2J\e[H'
    banner "3 · GPU health" "${device} on ${pool} tainted NoExecute"
    board
    [[ -n "${node}" && "${node}" != "${pool}" ]] && break
    sleep 2
  done
  [[ -n "${node}" && "${node}" != "${pool}" ]] || fail "beta did not move off ${pool}"
  ok "beta moved to ${node} with 2 healthy GPUs, and got its channel at once from the running daemon"
  if [[ "$(kubectl get pods -n "${DEMO_NS}" -l cd=alpha -o jsonpath='{range .items[*]}{.metadata.uid}{"\n"}{end}' | sort)" == "${before}" ]]; then
    ok "every alpha pod kept running untouched"
  fi
  note "before reservations a pod took all 4 GPUs of a node, so one bad GPU took the whole node"
  pause
}

scene_upgrade() {
  banner "4 · Daemon upgrades" "The admin decides when and how fast daemons change: by clique, never node by node"
  step "Upgrade knobs of the ComputeDomainDaemonSet"
  kubectl get computedomaindaemonset default -o json | jq '{image: (.spec.image // "<pinned at creation>"), updateStrategy: .spec.updateStrategy}'
  note "OnDelete never restarts daemons · RollingUpdate restarts maxUnavailableCliques whole cliques at a time"
  note "minReadySeconds soaks each batch · paused holds the rollout · topologyKey goes rack block by rack block"
  pause

  local verbosity
  verbosity=$(kubectl get computedomaindaemonset default -o json | jq -r 'if .spec.logVerbosity == 2 then 1 else 2 end')
  step "Change the daemons (here, their log verbosity) and roll out one clique at a time"
  run kubectl patch computedomaindaemonset default --type=merge -p "'{\"spec\": {\"logVerbosity\": ${verbosity}, \"updateStrategy\": {\"type\": \"RollingUpdate\", \"rollingUpdate\": {\"maxUnavailableCliques\": 1, \"minReadySeconds\": 10}}}}'"
  pause

  local before
  before=$(kubectl get pods -n "${DEMO_NS}" -o jsonpath='{range .items[*]}{.metadata.uid}{"\n"}{end}' | sort)
  local deadline=$((SECONDS + DEMO_ROLLOUT_WATCH)) started=$SECONDS
  while ((SECONDS < deadline)); do
    printf '\e[2J\e[H'
    banner "4 · Daemon upgrades" "rolling out, $((SECONDS - started))s"
    kubectl get computedomaindaemonset default
    kubectl get computedomaindaemonset default -o json | jq -r '
      "\n  clique                                    nodes ready updated",
      (.status.cliques[]? | "  \(.id | .[0:40] | . + (" " * (40 - length)))  \(.nodes)     \(.readyNodes)     \(.updatedNodes)\(if .stalledReason then "  stalled: " + .stalledReason else "" end)")'
    printf '\n  %sworkloads%s\n' "${B}" "${R}"
    kubectl get pods -n "${DEMO_NS}" -o custom-columns='  POD:.metadata.labels.app,NODE:.spec.nodeName,STATUS:.status.phase,RESTARTS:.status.containerStatuses[0].restartCount' --no-headers |
      sed 's/^/  /'
    if kubectl get computedomaindaemonset default -o json | jq -e '.status.observedGeneration == .metadata.generation
        and .status.updatedNumberScheduled == .status.desiredNumberScheduled
        and .status.numberReady == .status.desiredNumberScheduled
        and ([.status.conditions[]? | select(.type == "RolloutInProgress" and .status == "True")] | length == 0)' > /dev/null; then
      ok "every clique runs the new daemons"
      if [[ "$(kubectl get pods -n "${DEMO_NS}" -o jsonpath='{range .items[*]}{.metadata.uid}{"\n"}{end}' | sort)" == "${before}" ]]; then
        ok "and every workload pod kept running through it"
      fi
      break
    fi
    sleep 3
  done
  pause
}

cleanup() {
  banner "Cleanup" "Removing the demo workloads"
  kubectl delete devicetaintrules -l demo=imex --ignore-not-found > /dev/null 2>&1 || true
  [[ -n "${NODE2:-}" ]] && kubectl uncordon "${NODE2}" > /dev/null 2>&1 || true
  kubectl delete namespace "${DEMO_NS}" --ignore-not-found --timeout="${DEMO_TIMEOUT}s"
  ok "done"
}

main() {
  local what="${1:-all}"
  preflight
  case "${what}" in
    all)
      banner "ComputeDomains on GB200" "static IMEX daemons + channel reservations · nodes ${NODE1} ${NODE2}"
      pause
      scene_static
      scene_channels
      scene_health
      scene_upgrade
      ;;
    static) scene_static ;;
    channels) scene_channels ;;
    health) scene_health ;;
    upgrade) scene_upgrade ;;
    board) board ;;
    cleanup) cleanup ;;
    *) sed -n '16,30p' "$0"; exit 1 ;;
  esac
}

main "$@"
