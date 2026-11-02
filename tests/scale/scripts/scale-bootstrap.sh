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

# Usage: scale-bootstrap.sh [admission flags...]
#
# Compares how long workloads take to bootstrap with per-ComputeDomain IMEX
# daemons (per-cd-reservations) and with static IMEX daemons (static), from
# about 1000 to 5000 pods: each mode runs the same workloads at each cluster
# size, filling BOOTSTRAP_FILL of the nodes with one pod each.
#
# Bootstrap is from creating a pod until it is bound and the IMEX daemon on
# its node is Ready in its ComputeDomainClique, which the kubelet plugin's
# Prepare waits for. With per-ComputeDomain daemons, a daemon starts for each
# new ComputeDomain on each of its nodes; static daemons are already Ready.
#
# Results of both modes go to results/bootstrap-<timestamp>/, with
# summary.md comparing them side by side. See run-scale.sh for the settings.

source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"

# 95% of 1008 to 5040 nodes: about 960 to 4790 pods.
: "${BOOTSTRAP_FILL:=0.95}"
: "${BOOTSTRAP_MODES:=per-cd-reservations static}"

OUT="${SCALE_DIR}/results/bootstrap-$(date +%Y%m%d-%H%M%S)"
mkdir -p "${OUT}"
for mode in ${BOOTSTRAP_MODES}; do
  SCALE_OUT="${OUT}/${mode}" FILL_LIST="${BOOTSTRAP_FILL}" "${SCALE_DIR}/scripts/run-scale.sh" "${mode}" "$@" ||
    log "mode ${mode} failed, see ${OUT}/${mode}"
done

"${SCALEBENCH}" summary --csv-dir "${OUT}" "${OUT}" | tee "${OUT}/summary.md"
log "results in ${OUT}"
