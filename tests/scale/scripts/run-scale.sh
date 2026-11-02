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

# Usage: run-scale.sh MODE [admission flags...]
#
# Scales one driver mode from SCALE_CLIQUES[0] to the last size and, at each
# size, runs a grid of admission workloads: every PODS_PER_CD_LIST value at
# every FILL_LIST share of the nodes (four GPUs per pod put one pod on a
# node). In static mode, each size first runs the static IMEX daemon phases.
#
# The cluster is created if it does not exist, and the fake nodes are reset
# to the first size, so that each mode starts from the same state. Results,
# with summary.md that points out the slowest stage of each run and what
# grows with cluster size, go to results/<mode>-<timestamp>/.
#
# scale-static.sh, scale-per-cd.sh and scale-per-cd-reservations.sh run this
# for one mode each.

source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"

MODE="${1:?usage: $0 MODE [admission flags...]}"
shift
ADMISSION_ARGS=("$@")

# 1008, 2016, 3024, 4032 and 5040 nodes of 18-node cliques.
: "${SCALE_CLIQUES:=56 112 168 224 280}"
: "${CLIQUE_SIZE:=18}"
# ComputeDomain sizes: 4 pods for small jobs, 18 for a full rack, or all for
# one ComputeDomain whose pods fill the share of the nodes.
: "${PODS_PER_CD_LIST:=4 18}"
# Share of the nodes the workload fills.
: "${FILL_LIST:=0.25 0.5 0.9}"
: "${STATIC_PHASES:=rollout,update,repartition,node-churn}"
: "${ADMISSION_TIMEOUT:=15m}"
# How long each run waits for its namespaces to go before the next one.
: "${CLEANUP_TIMEOUT:=10m}"
# Cliques the update phase restarts at a time, a number or percentage.
: "${MAX_UNAVAILABLE_CLIQUES:=25%}"
# Time a simulated IMEX daemon takes to become Ready once its pod runs. 0
# measures only the control plane; set it to the IMEX startup measured on
# real nodes to include that in the workload bootstrap.
: "${DAEMON_READY_DELAY:=0s}"
: "${REPEAT:=1}"

runs=$(($(wc -w <<<"${SCALE_CLIQUES}") * $(wc -w <<<"${PODS_PER_CD_LIST}") * $(wc -w <<<"${FILL_LIST}") * REPEAT))
: "${SCALE_OUT:=${SCALE_DIR}/results/${MODE}-$(date +%Y%m%d-%H%M%S)}"
OUT="${SCALE_OUT}"
mkdir -p "${OUT}"
log "mode ${MODE}: ${runs} admission runs, cliques ${SCALE_CLIQUES}, pods per ComputeDomain ${PODS_PER_CD_LIST}, fill ${FILL_LIST}"
log "results in ${OUT}"

if ! kind get clusters 2> /dev/null | grep -qx "${KIND_CLUSTER_NAME}"; then
  "${SCALE_DIR}/scripts/create-cluster.sh"
fi
build_scalebench

first_cliques=${SCALE_CLIQUES%% *}
log "resetting fake nodes to $((first_cliques * CLIQUE_SIZE))"
"${SCALEBENCH}" nodes down
"${SCALEBENCH}" nodes up --cliques "${first_cliques}" --clique-size "${CLIQUE_SIZE}"
"${SCALE_DIR}/scripts/install-driver.sh" "${MODE}" 2>&1 | tee -a "${OUT}/driver.log"

for cliques in ${SCALE_CLIQUES}; do
  nodes=$((cliques * CLIQUE_SIZE))
  log "=== ${nodes} nodes in ${cliques} cliques"
  "${SCALEBENCH}" nodes up --cliques "${cliques}" --clique-size "${CLIQUE_SIZE}"

  if [[ "${MODE}" == static ]]; then
    ensure_idle
    log "static IMEX daemons: ${STATIC_PHASES}"
    "${SCALEBENCH}" static --output-dir "${OUT}" --label "n${nodes}" --phases "${STATIC_PHASES}" --recreate --keep \
      --max-unavailable-cliques "${MAX_UNAVAILABLE_CLIQUES}" \
      --daemon-ready-delay "${DAEMON_READY_DELAY}" 2>&1 |
      tee -a "${OUT}/static.log" || log "static failed at ${nodes} nodes, see ${OUT}/static.log"
  fi

  for pods_per_cd in ${PODS_PER_CD_LIST}; do
    for fill in ${FILL_LIST}; do
      pct=$(awk -v f="${fill}" 'BEGIN {printf "%d", f * 100}')
      if [[ "${pods_per_cd}" == all ]]; then
        cds=1
        ppcd=$(awk -v n="${nodes}" -v f="${fill}" 'BEGIN {p = int(n * f); print (p < 1 ? 1 : p)}')
        name="single-cd"
      else
        cds=$(awk -v n="${nodes}" -v f="${fill}" -v p="${pods_per_cd}" 'BEGIN {c = int(n * f / p); print (c < 1 ? 1 : c)}')
        ppcd=${pods_per_cd}
        name="ppcd${pods_per_cd}"
      fi
      for i in $(seq 1 "${REPEAT}"); do
        label="${name}-fill${pct}"
        [[ "${REPEAT}" -gt 1 ]] && label="${label}-r${i}"
        ensure_idle
        log "admission ${label}: ${cds} ComputeDomains x ${ppcd} pods on ${nodes} nodes"
        "${SCALEBENCH}" admission --output-dir "${OUT}" --label "${label}" --timeout "${ADMISSION_TIMEOUT}" \
          --cleanup-timeout "${CLEANUP_TIMEOUT}" --daemon-ready-delay "${DAEMON_READY_DELAY}" \
          --compute-domains "${cds}" --pods-per-cd "${ppcd}" ${ADMISSION_ARGS[@]+"${ADMISSION_ARGS[@]}"} 2>&1 |
          tee -a "${OUT}/admission.log" || log "admission ${label} failed at ${nodes} nodes, see ${OUT}/admission.log"
      done
    done
  done
done

"${SCALEBENCH}" summary --csv-dir "${OUT}" "${OUT}" | tee "${OUT}/summary.md"
log "results in ${OUT}"
