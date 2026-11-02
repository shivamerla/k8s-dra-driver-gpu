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

# Usage: scale-single-cd.sh [admission flags...]
#
# Runs one ComputeDomain across the cluster, from 1008 to 10080 nodes, with
# per-ComputeDomain and static IMEX daemons: at each size, one ComputeDomain
# puts one pod on SINGLE_CD_FILL of the nodes. This is the most nodes and
# pods a single domain spans, so it finds the limits of the controller and
# the kubelet plugin for one domain rather than for many small ones.
#
# Static mode first rolls out its daemons at each size (STATIC_PHASES). Runs
# get longer timeouts, as the scheduler places up to about 19000 pods
# (workload and daemon pods) at the largest size.
#
# Results of both modes go to results/single-cd-<timestamp>/, with a Single
# ComputeDomain table and the bootstrap of both modes side by side in
# summary.md. See run-scale.sh for the settings.

source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"

# 1008, 2016, 3024, 4032, 5040 and 10080 nodes of 18-node cliques.
: "${SINGLE_CD_CLIQUES:=56 112 168 224 280 560}"
: "${SINGLE_CD_FILL:=0.95}"
: "${SINGLE_CD_MODES:=per-cd-reservations static}"

OUT="${SCALE_DIR}/results/single-cd-$(date +%Y%m%d-%H%M%S)"
mkdir -p "${OUT}"
for mode in ${SINGLE_CD_MODES}; do
  SCALE_OUT="${OUT}/${mode}" SCALE_CLIQUES="${SINGLE_CD_CLIQUES}" FILL_LIST="${SINGLE_CD_FILL}" PODS_PER_CD_LIST=all \
    STATIC_PHASES="${STATIC_PHASES:-rollout}" ADMISSION_TIMEOUT="${ADMISSION_TIMEOUT:-60m}" CLEANUP_TIMEOUT="${CLEANUP_TIMEOUT:-30m}" \
    "${SCALE_DIR}/scripts/run-scale.sh" "${mode}" "$@" || log "mode ${mode} failed, see ${OUT}/${mode}"
done

"${SCALEBENCH}" summary --csv-dir "${OUT}" "${OUT}" | tee "${OUT}/summary.md"
log "results in ${OUT}"
