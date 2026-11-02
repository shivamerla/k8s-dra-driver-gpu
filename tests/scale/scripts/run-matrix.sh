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

# Usage: run-matrix.sh [per-cd|static|all] [admission flags...]
#
# Runs the same admission workload with binding conditions off and on, and
# compares them:
#   per-cd   per-cd vs per-cd-reservations: adds node ownership
#   static   static IMEX daemons: daemon rollout phases, then the admission
#            workload with the daemons running
# Each pair runs REPEAT times, alternating modes, so that drift of the
# cluster does not favor one. Results go to results/<timestamp>/.

source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"

: "${REPEAT:=3}"
SUITE="${1:-all}"
shift || true
ADMISSION_ARGS=("$@")

OUT="${SCALE_DIR}/results/$(date +%Y%m%d-%H%M%S)"
mkdir -p "${OUT}"
build_scalebench

admission() {
  local mode=$1 label=$2
  ensure_idle
  log "admission: ${mode} (${label})"
  "${SCALEBENCH}" admission --output-dir "${OUT}" --label "${label}" ${ADMISSION_ARGS[@]+"${ADMISSION_ARGS[@]}"} 2>&1 |
    tee -a "${OUT}/${mode}.log"
}

pair() {
  local off=$1 on=$2
  for i in $(seq 1 "${REPEAT}"); do
    for mode in "${off}" "${on}"; do
      "${SCALE_DIR}/scripts/install-driver.sh" "${mode}"
      admission "${mode}" "r${i}"
    done
  done
  for i in $(seq 1 "${REPEAT}"); do
    a=$(ls "${OUT}"/admission-"${off}"-r"${i}"-*.json)
    b=$(ls "${OUT}"/admission-"${on}"-r"${i}"-*.json)
    echo "== ${off} vs ${on}, repeat ${i}" | tee -a "${OUT}/compare.txt"
    "${SCALEBENCH}" compare "${a}" "${b}" | tee -a "${OUT}/compare.txt"
  done
}

static_suite() {
  "${SCALE_DIR}/scripts/install-driver.sh" static
  log "static IMEX daemons: rollout, update, repartition, node churn"
  "${SCALEBENCH}" static --output-dir "${OUT}" --recreate --keep 2>&1 | tee -a "${OUT}/static.log"
  for i in $(seq 1 "${REPEAT}"); do
    admission static "r${i}"
  done
}

case "${SUITE}" in
  per-cd) pair per-cd per-cd-reservations ;;
  static) static_suite ;;
  all)
    pair per-cd per-cd-reservations
    static_suite
    ;;
  *)
    echo "usage: $0 [per-cd|static|all] [admission flags...]" >&2
    exit 2
    ;;
esac
log "results in ${OUT}"
