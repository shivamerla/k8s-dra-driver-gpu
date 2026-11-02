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

# Usage: run-sweep.sh [admission flags...]
#
# Grows the cluster through SWEEP_CLIQUES and, at each size, runs every mode
# of SWEEP_MODES:
#   - static runs the static IMEX daemon phases first;
#   - each mode then runs an admission workload that puts one pod on
#     SWEEP_FILL of the nodes, REPEAT times.
# Results go to results/sweep-<timestamp>/, with summary.md, admission.csv
# and static.csv tabulating them by node count.

source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"

# 144, 504, 1008, 2016 and 4032 nodes of 18-node cliques.
: "${SWEEP_CLIQUES:=8 28 56 112 224}"
: "${CLIQUE_SIZE:=18}"
: "${SWEEP_MODES:=per-cd-reservations static}"
: "${SWEEP_FILL:=0.5}"
: "${PODS_PER_CD:=4}"
: "${STATIC_PHASES:=rollout,update,repartition,node-churn}"
: "${REPEAT:=1}"
ADMISSION_ARGS=("$@")

OUT="${SCALE_DIR}/results/sweep-$(date +%Y%m%d-%H%M%S)"
mkdir -p "${OUT}"
build_scalebench

for cliques in ${SWEEP_CLIQUES}; do
  nodes=$((cliques * CLIQUE_SIZE))
  # Four GPUs per pod put one pod on each node.
  cds=$(awk -v n="${nodes}" -v f="${SWEEP_FILL}" -v p="${PODS_PER_CD}" 'BEGIN {c = int(n * f / p); print (c < 1 ? 1 : c)}')
  log "${nodes} nodes in ${cliques} cliques: ${cds} ComputeDomains of ${PODS_PER_CD} pods"
  "${SCALEBENCH}" nodes up --cliques "${cliques}" --clique-size "${CLIQUE_SIZE}"

  for mode in ${SWEEP_MODES}; do
    "${SCALE_DIR}/scripts/install-driver.sh" "${mode}"
    if [[ "${mode}" == static ]]; then
      log "static: ${STATIC_PHASES}"
      "${SCALEBENCH}" static --output-dir "${OUT}" --label "n${nodes}" --phases "${STATIC_PHASES}" --recreate --keep 2>&1 |
        tee -a "${OUT}/static.log" || log "static failed at ${nodes} nodes, see ${OUT}/static.log"
    fi
    for i in $(seq 1 "${REPEAT}"); do
      ensure_idle
      log "admission: ${mode}, ${nodes} nodes, repeat ${i}"
      "${SCALEBENCH}" admission --output-dir "${OUT}" --label "n${nodes}-r${i}" \
        --compute-domains "${cds}" --pods-per-cd "${PODS_PER_CD}" ${ADMISSION_ARGS[@]+"${ADMISSION_ARGS[@]}"} 2>&1 |
        tee -a "${OUT}/${mode}.log" || log "admission failed: ${mode}, ${nodes} nodes, see ${OUT}/${mode}.log"
    done
  done
done

"${SCALEBENCH}" summary --csv-dir "${OUT}" "${OUT}" | tee "${OUT}/summary.md"
log "results in ${OUT}"
