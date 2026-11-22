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

# Usage: kwok-demo.sh setup|run|tmux|day0|workloads|upgrade|board|reset|teardown
#
# The backup demo: static IMEX daemons on 8 fake GB200 NVL72 racks (18 KWOK
# nodes each) in a kind cluster, drawn as one row of 18 nodes per rack.
#
#   setup      creates the cluster with tests/scale, installs the driver with
#              static daemons, and starts the agent that simulates them
#   run        day0, workloads and upgrade, waiting for Enter between steps
#   tmux       run in a split screen with the last applied YAML, the racks and
#              the driver pods always in view (see panes.sh)
#   day0       the admin creates the ComputeDomainDaemonSet: 144 daemons
#   workloads  two ComputeDomains start on racks that already run daemons
#   upgrade    the workloads are drained, as restarting the daemons of a
#              rack disrupts its jobs; then a new daemon image rolls out two
#              racks at a time, one domain of racks after the other, pauses,
#              stalls on a broken node, and finishes once the admin cordons it
#   reset      removes the workloads and daemons, to run again
#   teardown   stops the agent; add --cluster to delete the cluster

set -euo pipefail

DEMO_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../../../../tests/scale/scripts/common.sh
source "${DEMO_DIR}/../../../../tests/scale/scripts/common.sh"
# The rack board and the objects need more rows than the panes default to.
: "${DEMO_BOARD_ROWS:=24}"
# shellcheck source=panes.sh
source "${DEMO_DIR}/panes.sh"

: "${DEMO_NS:=kwok-demo}"
: "${DEMO_AUTO:=0}"
# Seconds each step stays on screen with DEMO_AUTO=1.
: "${DEMO_HOLD:=15}"
: "${DEMO_RACKS:=8}"
: "${DEMO_TIMEOUT:=300}"
# The node whose new daemon never becomes ready in the upgrade.
: "${DEMO_BROKEN_NODE:=kwok-c0006-n04}"
: "${DEMO_NEW_IMAGE:=${DRIVER_IMAGE_REPO}:v-next}"

RACK_SIZE=18
UUID="5ca1ab1e-0000-4000-8000-00000000c11c"
DOMAIN_KEY="demo.nvidia.com/domain"
HASH_KEY="resource.nvidia.com/templateHash"
AGENT_PID="${SCALE_STATE_DIR}/kwok-demo-agent.pid"
AGENT_LOG="${SCALE_STATE_DIR}/kwok-demo-agent.log"

R=$'\e[0m' B=$'\e[1m' D=$'\e[2m' GRN=$'\e[32m' YLW=$'\e[33m' CYN=$'\e[36m' MAG=$'\e[35m' RED=$'\e[31m'

STEP_TITLE=""
step() {
  STEP_TITLE="$*"
  printf '\n%s▸ %s%s\n' "${B}${CYN}" "$*" "${R}"
}
note() { printf '  %s%s%s\n' "${D}" "$*" "${R}"; }
ok() { printf '  %s✔ %s%s\n' "${GRN}" "$*" "${R}"; }
fail() { printf '  %s✘ %s%s\n' "${RED}" "$*" "${R}" >&2; exit 1; }

# pause waits for Enter, or with DEMO_AUTO=1 holds the screen for DEMO_HOLD
# seconds, so that the audience can read it.
pause() {
  if [[ "${DEMO_AUTO}" == 1 ]]; then
    sleep "${DEMO_HOLD}"
    return 0
  fi
  printf '%s  ⏎%s' "${D}" "${R}"
  read -rs _
  printf '\r    \r'
}

# run prints a command, then runs it. Commands that change the cluster also
# go to the applied pane.
run() {
  printf '%s$ %s%s\n' "${YLW}" "$*" "${R}"
  case "$2" in
    get | wait) ;;
    *) record "${STEP_TITLE}" "\$ $*" ;;
  esac
  eval "$@"
}

# show_board draws the board, unless a pane of its own does.
show_board() {
  panes_on || board
}

banner() {
  local line
  line=$(printf '━%.0s' $(seq 1 78))
  printf '%s%s\n  %s\n  %s%s%s\n%s%s\n' "${B}${MAG}" "${line}" "$1" "${R}${D}" "$2" "${R}${B}${MAG}" "${line}" "${R}"
}

show_apply() {
  local manifest
  manifest=$(cat)
  record "${STEP_TITLE}" "${manifest}"
  panes_on || printf '%s%s%s\n' "${D}" "$(sed 's/^/    /' <<< "${manifest}")" "${R}"
  kubectl apply -f - <<< "${manifest}" > /dev/null
}

# ---------------------------------------------------------------- the board

# board draws one row per rack and one cell per node: the state of its IMEX
# daemon as the background, and a mark for the ComputeDomain running on it.
board() {
  local nodes daemons dss cdds work
  nodes=$(kubectl get nodes -l scale.nvidia.com/node -o json)
  daemons=$(kubectl get pods -n "${DRIVER_NAMESPACE}" -l resource.nvidia.com/computeDomainDaemonSet -o json)
  dss=$(kubectl get daemonsets -n "${DRIVER_NAMESPACE}" -o json)
  cdds=$(kubectl get computedomaindaemonset default -o json 2> /dev/null || echo '{}')
  work=$(kubectl get pods -n "${DEMO_NS}" -o json 2> /dev/null || echo '{"items":[]}')
  jq -r -n --argjson nodes "${nodes}" --argjson daemons "${daemons}" --argjson dss "${dss}" \
    --argjson cdds "${cdds}" --argjson work "${work}" '
    def esc($c): "\u001b[" + $c + "m";
    def ready: any(.status.conditions[]?; .type == "Ready" and .status == "True");
    ([$dss.items[].spec.template.metadata.annotations["'"${HASH_KEY}"'"] // empty] | first // "") as $hash
    | ([$daemons.items[] | select(.spec.nodeName != null)] | group_by(.spec.nodeName)
       | map({key: .[0].spec.nodeName, value: .}) | from_entries) as $dp
    | ([$work.items[] | select(.spec.nodeName != null and .metadata.deletionTimestamp == null)]
       | group_by(.spec.nodeName) | map({key: .[0].spec.nodeName, value: .[0].metadata.labels.cd}) | from_entries) as $wl
    | (($cdds.status.cliques // []) | map({key: .id, value: .}) | from_entries) as $cs
    | ($cdds.status.updatingTopology // "") as $updating
    | [$nodes.items[] | {name: .metadata.name, clique: .metadata.labels["nvidia.com/gpu.clique"],
         domain: (.metadata.labels["'"${DOMAIN_KEY}"'"] // "-"), cordoned: (.spec.unschedulable // false)}]
    | group_by(.clique) | sort_by(.[0].name)[]
    | . as $rack | ($cs[$rack[0].clique] // {}) as $st | (($st.stalledReason // "") != "") as $stalled
    | [$rack | sort_by(.name)[] | . as $n
        | ([$dp[$n.name][]? | select(.metadata.deletionTimestamp == null and ready)]) as $up
        | (if ($up | length) == 0 then (if ($dp[$n.name] // [] | length) == 0 then "none" else "wait" end)
           elif any($up[]; .metadata.annotations["'"${HASH_KEY}"'"] == $hash) then "new" else "old" end) as $s
        | (if $n.cordoned then "38;5;244;48;5;236" elif $s == "new" then "38;5;255;48;5;29"
           elif $s == "old" then "38;5;255;48;5;24" elif $s == "wait" and $stalled then "38;5;255;48;5;124"
           elif $s == "wait" then "38;5;232;48;5;179" else "38;5;255;48;5;245" end) as $c
        | ({"vision": "◆", "speech": "●"}[$wl[$n.name] // ""] // (if $n.cordoned then "░" else " " end)) as $g
        | esc($c) + " " + $g + " " + esc("0")] as $cells
    | (if $stalled then esc("1;31") + "  STALLED" + esc("0")
       elif ($st.updatedNodes // 0) == ($st.nodes // -1) and ($st.readyNodes // 0) == ($st.nodes // -1) then esc("32") + "  ✔ \($st.updatedNodes)/\($st.nodes)" + esc("0")
       elif $updating != "" and $rack[0].domain == $updating then esc("33") + "  ◀ updating \($st.updatedNodes // 0)/\($st.nodes // 0)" + esc("0")
       else esc("2") + "  \($st.updatedNodes // 0)/\($st.nodes // 0)" + esc("0") end) as $tail
    | "  " + esc("1") + "rack \($rack[0].clique | split(".") | last)" + esc("0") + esc("2") + "  domain \($rack[0].domain)  " + esc("0")
      + ($cells | join("")) + $tail'
  printf '\n  %s   %s new daemon  %s   %s old daemon  %s   %s restarting  %s   %s stalled  %s ░ %s cordoned   ◆ vision  ● speech\n' \
    $'\e[48;5;29m' "${R}" $'\e[48;5;24m' "${R}" $'\e[48;5;179m' "${R}" $'\e[48;5;124m' "${R}" $'\e[48;5;236m' "${R}"
  jq -r '.status.conditions[]? | select(.type == "RolloutInProgress" and .status == "True")
    | "\n  \u001b[1m\(.reason)\u001b[0m  \(.message | .[0:220])"' <<< "${cdds}"
}

# watch_board redraws the board under title $1 until command $2 succeeds, or
# for DEMO_TIMEOUT seconds.
watch_board() {
  local title="$1" until="$2" deadline=$((SECONDS + DEMO_TIMEOUT)) started=$SECONDS
  while ((SECONDS < deadline)); do
    if panes_on; then
      printf '\r  %s%s, %ds%s ' "${D}" "${title}" "$((SECONDS - started))" "${R}"
    else
      printf '\e[2J\e[H'
      banner "${title}" "$((SECONDS - started))s"
      kubectl get computedomaindaemonset 2> /dev/null | sed 's/^/  /' || true
      board
    fi
    if eval "${until}"; then
      panes_on && printf '\n'
      return 0
    fi
    sleep 2
  done
  return 1
}

cdds_done() {
  kubectl get computedomaindaemonset default -o json | jq -e '.status.observedGeneration == .metadata.generation
    and .status.desiredNumberScheduled > 0
    and .status.numberReady == .status.desiredNumberScheduled
    and .status.updatedNumberScheduled == .status.desiredNumberScheduled' > /dev/null
}

# cdds_rolled is true once no daemon runs an outdated revision.
cdds_rolled() {
  kubectl get computedomaindaemonset default -o json | jq -e '.metadata.generation as $g
    | .status.observedGeneration == $g
    and any(.status.conditions[]?; .type == "RolloutInProgress" and .status == "False" and .observedGeneration == $g)' > /dev/null
}

cdds_stalled() {
  kubectl get computedomaindaemonset default -o json | jq -e '[.status.cliques[]? | select(.stalledReason)] | length > 0' > /dev/null
}

cdds_updating_domain() {
  kubectl get computedomaindaemonset default -o json | jq -e --arg b "$1" '.status.updatingTopology == $b' > /dev/null
}

# ---------------------------------------------------------------- KWOK

# break_node keeps KWOK from making new pods on $1 ready, as if their
# container could not start there.
break_node() {
  kubectl patch stage pod-ready --type=json -p "[{\"op\": \"add\", \"path\": \"/spec/selector/matchExpressions/-\",
    \"value\": {\"key\": \".spec.nodeName\", \"operator\": \"NotIn\", \"values\": [\"$1\"]}}]" > /dev/null
}

fix_nodes() {
  local i
  i=$(kubectl get stage pod-ready -o json | jq '[.spec.selector.matchExpressions | to_entries[] | select(.value.key == ".spec.nodeName") | .key] | first // empty')
  while [[ -n "${i}" ]]; do
    kubectl patch stage pod-ready --type=json -p "[{\"op\": \"remove\", \"path\": \"/spec/selector/matchExpressions/${i}\"}]" > /dev/null
    i=$(kubectl get stage pod-ready -o json | jq '[.spec.selector.matchExpressions | to_entries[] | select(.value.key == ".spec.nodeName") | .key] | first // empty')
  done
}

agent_running() {
  [[ -f "${AGENT_PID}" ]] && kill -0 "$(cat "${AGENT_PID}")" 2> /dev/null
}

start_agent() {
  agent_running && return 0
  log "starting the agent that publishes fake GPUs and simulates IMEX daemons, log in ${AGENT_LOG}"
  nohup "${SCALEBENCH}" agent --gpus-per-node 4 > "${AGENT_LOG}" 2>&1 &
  echo $! > "${AGENT_PID}"
}

stop_agent() {
  if agent_running; then
    kill "$(cat "${AGENT_PID}")"
  fi
  rm -f "${AGENT_PID}"
}

# ---------------------------------------------------------------- commands

setup() {
  if ! kind get clusters 2> /dev/null | grep -qx "${KIND_CLUSTER_NAME}"; then
    "${SCALE_DIR}/scripts/create-cluster.sh"
  fi
  build_scalebench
  "${SCALEBENCH}" nodes up --cliques "${DEMO_RACKS}" --clique-size "${RACK_SIZE}"
  local c
  for ((c = 0; c < DEMO_RACKS; c++)); do
    kubectl label nodes -l "nvidia.com/gpu.clique=${UUID}.${c}" "${DOMAIN_KEY}=$((c < DEMO_RACKS / 2 ? 1 : 2))" --overwrite > /dev/null
  done
  "${SCALE_DIR}/scripts/install-driver.sh" static
  reset
  start_agent
  ok "ready: ${DEMO_RACKS} racks of ${RACK_SIZE} nodes, no IMEX daemons yet. Run: $0 run"
}

reset() {
  kubectl delete namespace "${DEMO_NS}" --ignore-not-found --wait=true --timeout=5m > /dev/null
  kubectl delete computedomaindaemonsets --all --ignore-not-found --wait=true --timeout=5m > /dev/null
  fix_nodes
  kubectl uncordon -l scale.nvidia.com/node > /dev/null
  ok "reset: no daemons, no workloads"
}

day0() {
  banner "1 · Day 0" "${DEMO_RACKS} racks of ${RACK_SIZE} GB200 nodes, no daemons yet"
  agent_running || start_agent
  show_board
  pause
  step "The admin creates the daemons for the whole cluster"
  show_apply << EOF
apiVersion: resource.nvidia.com/v1beta1
kind: ComputeDomainDaemonSet
metadata: {name: default}
spec:
  maxNodesPerClique: ${RACK_SIZE}
  updateStrategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailableCliques: 2        # two racks at a time
      minReadySeconds: 5              # soak each batch
      progressDeadlineSeconds: 20     # report a rack that does not come back
      topologyKey: ${DOMAIN_KEY}  # domain 1 first, then domain 2
EOF
  pause
  watch_board "1 · Day 0: daemons starting" cdds_done || fail "the daemons did not become ready"
  ok "$((DEMO_RACKS * RACK_SIZE)) daemons ready before the first job"
  pause
}

# workload prints ComputeDomain $1 with $2 pods of $3 GPUs on the nodes whose
# label $4 has one of the values $5.
workload() {
  local cd="$1" pods="$2" gpus="$3" key="$4" values="$5" i
  cat << EOF
---
apiVersion: resource.nvidia.com/v1beta1
kind: ComputeDomain
metadata: {name: ${cd}, namespace: ${DEMO_NS}}
spec:
  channels: [{resourceClaimTemplate: {name: ${cd}}}]
---
apiVersion: resource.k8s.io/v1
kind: ResourceClaimTemplate
metadata: {name: ${cd}-gpus, namespace: ${DEMO_NS}}
spec:
  spec:
    devices:
      requests:
      - {name: gpu, exactly: {deviceClassName: scale-gpu.nvidia.com, allocationMode: ExactCount, count: ${gpus}}}
EOF
  for ((i = 0; i < pods; i++)); do
    cat << EOF
---
apiVersion: v1
kind: Pod
metadata: {name: ${cd}-${i}, namespace: ${DEMO_NS}, labels: {cd: ${cd}}}
spec:
  terminationGracePeriodSeconds: 0
  tolerations: [{key: kwok.x-k8s.io/node, operator: Exists, effect: NoSchedule}]
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
        - matchExpressions: [{key: ${key}, operator: In, values: [${values}]}]
  containers:
  - name: app
    image: registry.k8s.io/pause:3.10
    resources: {claims: [{name: channel}, {name: gpus}]}
  resourceClaims:
  - {name: channel, resourceClaimTemplateName: ${cd}}
  - {name: gpus, resourceClaimTemplateName: ${cd}-gpus}
EOF
  done
}

workloads_running() {
  kubectl get pods -n "${DEMO_NS}" -o json | jq -e '.items | length > 0 and all(.status.phase == "Running")' > /dev/null
}

workloads() {
  banner "2 · Jobs" "No daemon to wait for"
  kubectl create namespace "${DEMO_NS}" --dry-run=client -o yaml | kubectl apply -f - > /dev/null
  step "vision on racks 1 and 2, speech two pods per node on rack 5"
  {
    workload vision 36 4 nvidia.com/gpu.clique "${UUID}.1, ${UUID}.2"
    workload speech 8 2 kubernetes.io/hostname "kwok-c0005-n00, kwok-c0005-n01, kwok-c0005-n02, kwok-c0005-n03"
  } | kubectl apply -f - > /dev/null
  watch_board "2 · Jobs starting" workloads_running || fail "the workloads did not start"
  ok "44 pods running, channels ready at once"
  pause
}

upgrade() {
  banner "3 · Daemon upgrades" "Rack by rack, when the admin decides"
  # Restarting the IMEX daemons of a clique breaks the multi-node jobs on it,
  # so admins move workloads off before the daemons change.
  step "Drain jobs first: a daemon restart disrupts its rack"
  run kubectl delete pods -n "${DEMO_NS}" --all
  ok "Racks drained"
  pause
  step "Simulate a node in rack 6 that cannot start the new daemon"
  break_node "${DEMO_BROKEN_NODE}"
  step "Roll out a new daemon image"
  run kubectl patch computedomaindaemonset default --type=merge -p "'{\"spec\": {\"image\": \"${DEMO_NEW_IMAGE}\"}}'"
  pause

  watch_board "3 · Domain 1, two racks at a time" "cdds_updating_domain 2" ||
    fail "the rollout did not reach domain 2"
  step "Pause before domain 2"
  run kubectl patch computedomaindaemonset default --type=merge -p "'{\"spec\": {\"updateStrategy\": {\"rollingUpdate\": {\"paused\": true}}}}'"
  sleep 3
  if ! panes_on; then
    printf '\e[2J\e[H'
    banner "3 · Paused" "Racks in progress finish, no new ones start"
    board
  fi
  pause
  run kubectl patch computedomaindaemonset default --type=merge -p "'{\"spec\": {\"updateStrategy\": {\"rollingUpdate\": {\"paused\": false}}}}'"

  watch_board "3 · Domain 2" "cdds_stalled || cdds_rolled" || fail "the rollout did not finish"
  if cdds_stalled; then
    printf '\n'
    ok "Rack 6 stalled, and the rollout holds"
    pause
    step "The admin cordons the broken node"
    run kubectl cordon "${DEMO_BROKEN_NODE}"
    watch_board "3 · Rollout resumes" cdds_rolled || fail "the rollout did not finish"
  fi
  ok "All racks updated"
  pause
}

teardown() {
  stop_agent
  fix_nodes 2> /dev/null || true
  if [[ "${1:-}" == --cluster ]]; then
    "${SCALE_DIR}/scripts/delete-cluster.sh"
  fi
  ok "agent stopped"
}

# driver_pods lists the driver pods, then the IMEX daemon pods, newest first
# so that the ones a rollout replaces are on top.
driver_pods() {
  kubectl get daemonsets,deployments -n "${DRIVER_NAMESPACE}"
  printf '\n'
  kubectl get pods -n "${DRIVER_NAMESPACE}" -l '!resource.nvidia.com/computeDomainDaemonSet' -o wide
  printf '\n'
  kubectl get pods -n "${DRIVER_NAMESPACE}" -l resource.nvidia.com/computeDomainDaemonSet -o wide \
    --sort-by=.metadata.creationTimestamp | awk 'NR == 1 {print; next} {l[n++] = $0} END {while (n) print l[--n]}'
}

# objects prints the ComputeDomain objects, and the pods of each workload.
objects() {
  show_objects "${DRIVER_NAMESPACE}" "${DEMO_NS}"
  printf '\n\e[1;35mPods\e[0m\n'
  kubectl get pods -n "${DEMO_NS}" -o json 2> /dev/null | jq -r '
    ["COMPUTEDOMAIN", "PODS", "RUNNING"], (.items | group_by(.metadata.labels.cd)[]
      | [.[0].metadata.labels.cd, length, ([.[] | select(.status.phase == "Running")] | length)]) | @tsv' |
    column -t -s $'\t'
}

main() {
  need() { command -v "$1" > /dev/null || fail "$1 is required"; }
  need kubectl
  need jq
  case "${1:-}" in
    tmux) panes_tmux "$0" run ;;
    pane-applied) follow_applied ;;
    pane-board) refresh board ;;
    pane-objects) refresh objects ;;
    pane-pods) refresh driver_pods ;;
    setup) setup ;;
    run)
      agent_running || start_agent
      day0
      workloads
      upgrade
      ;;
    day0) day0 ;;
    workloads) workloads ;;
    upgrade) upgrade ;;
    board) board ;;
    reset) reset ;;
    teardown) teardown "${2:-}" ;;
    *) sed -n '16,35p' "$0"; exit 1 ;;
  esac
}

main "$@"
