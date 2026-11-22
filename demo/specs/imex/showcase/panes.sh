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

# Split screen for demo.sh and kwok-demo.sh. In tmux, the script runs its
# commands in one pane while the others show what it applied last, the
# cluster, its ComputeDomain objects and the driver pods, all the time:
#
#   +----------------------+---------------------+
#   | commands             | last applied YAML   |
#   +----------------------+---------------------+
#   | cluster board        | objects             |
#   +----------------------+---------------------+
#   | driver and IMEX daemon pods                |
#   +--------------------------------------------+

# Set by the tmux session: the script only runs its commands, other panes draw.
: "${DEMO_PANES:=0}"
: "${DEMO_STATE:=${TMPDIR:-/tmp}/imex-showcase-${USER:-demo}}"
# Heights of the board row and of the driver pods pane in the split screen;
# the commands and the applied YAML get the rest.
: "${DEMO_BOARD_ROWS:=20}"
: "${DEMO_POD_ROWS:=10}"
APPLIED_FILE="${DEMO_STATE}/applied"

panes_on() { [[ "${DEMO_PANES}" == 1 ]]; }

# record shows $2, a manifest or a command, under the title $1 in the applied
# pane.
record() {
  mkdir -p "${DEMO_STATE}"
  printf '# %s\n%s\n' "$1" "$2" > "${APPLIED_FILE}.tmp"
  mv "${APPLIED_FILE}.tmp" "${APPLIED_FILE}"
}

pane_rows() {
  stty size < /dev/tty 2> /dev/null | cut -d' ' -f1 || echo 40
}

# refresh redraws the output of command $1 every two seconds. The output is
# built before the screen is cleared, so that it does not flicker, and a
# failing command, such as a kubectl call the API server times out, does not
# end the pane.
refresh() {
  local out
  printf '\e[?25l'
  while true; do
    out=$(eval "$1" 2>&1 | head -n "$(($(pane_rows) - 1))") || true
    printf '\e[H\e[2J%s' "${out}"
    sleep 2
  done
}

# follow_applied shows the last recorded manifest, with comments, keys and
# commands in their own colors.
follow_applied() {
  local cur last=""
  printf '\e[?25l'
  while true; do
    cur=$(cat "${APPLIED_FILE}" 2> /dev/null || echo "# nothing applied yet")
    if [[ "${cur}" != "${last}" ]]; then
      printf '\e[H\e[2J\e[1;35m  applied\e[0m\n\n'
      sed -E $'s/^(# .*)$/\e[1;36m\\1\e[0m/; s/^(\\$ .*)$/\e[33m\\1\e[0m/; s/^( *-? *)([A-Za-z][A-Za-z0-9]*):/\\1\e[32m\\2\e[0m:/; s/^/  /' <<< "${cur}"
      last="${cur}"
    fi
    sleep 0.5
  done
}

# show_objects prints the ComputeDomainDaemonSet, the ComputeDomainCliques in
# driver namespace $1, and the ComputeDomains in namespace $2.
show_objects() {
  local h=$'\e[1;35m' r=$'\e[0m'
  printf '%sComputeDomainDaemonSet%s\n' "${h}" "${r}"
  kubectl get computedomaindaemonset 2> /dev/null || echo "none"
  printf '\n%sComputeDomainCliques%s\n' "${h}" "${r}"
  kubectl get computedomaincliques -n "$1" -o json 2> /dev/null | jq -r '
    ["NAME", "DAEMONS", "READY"], (.items[] | [(.metadata.name | if length > 44 then "…" + .[-43:] else . end),
      (.daemons // [] | length), ([.daemons[]? | select(.status == "Ready")] | length)]) | @tsv' | column -t -s $'\t'
  printf '\n%sComputeDomains%s\n' "${h}" "${r}"
  kubectl get computedomains -n "$2" -o json 2> /dev/null | jq -r '
    ["NAME", "STATUS", "CHANNEL TEMPLATES"], (.items[] | [.metadata.name, (.status.status // "-"),
      ([(.spec.channels // [])[].resourceClaimTemplate.name, .spec.channel.resourceClaimTemplate.name // empty] | join(", "))]) | @tsv' |
    column -t -s $'\t'
}

# panes_tmux runs script $1 with arguments $2... in a tmux session of five
# panes.
panes_tmux() {
  local script="$1" session="imex-showcase" envs v
  shift
  command -v tmux > /dev/null || { echo "tmux is required" >&2; return 1; }
  # A tmux server started earlier does not see this shell's variables.
  envs="env"
  for v in KUBECONFIG KIND_CLUSTER_NAME $(compgen -v | grep '^DEMO_' | grep -vx DEMO_PANES || true); do
    [[ -n "${!v:-}" ]] && envs+=" $(printf '%q' "${v}=${!v}")"
  done
  rm -f "${APPLIED_FILE}"
  local rows cols middle
  read -r rows cols < <(stty size < /dev/tty 2> /dev/null || echo "50 200")
  # Small terminals keep at least 8 rows for the commands.
  middle=$((DEMO_BOARD_ROWS + DEMO_POD_ROWS + 1))
  ((middle > rows - 8)) && middle=$((rows - 8))
  tmux kill-session -t "${session}" 2> /dev/null || true
  tmux new-session -d -s "${session}" -x "${cols}" -y "${rows}" \
    "${envs} DEMO_PANES=1 $(printf '%q ' "${script}" "$@"); echo; read -rp 'demo done, Enter closes the session'; tmux kill-session -t ${session}"
  local cmd board
  cmd="${envs} $(printf '%q' "${script}")"
  tmux select-pane -t "${session}:0.0" -T commands
  tmux split-window -h -l 45% -t "${session}:0.0" "${cmd} pane-applied"
  tmux select-pane -T "applied YAML"
  board=$(tmux split-window -v -f -l "${middle}" -t "${session}:0" -P -F '#{pane_id}' "${cmd} pane-board")
  tmux select-pane -t "${board}" -T cluster
  tmux split-window -v -l $((middle * DEMO_POD_ROWS / (DEMO_BOARD_ROWS + DEMO_POD_ROWS + 1))) -t "${board}" "${cmd} pane-pods"
  tmux select-pane -T "driver and IMEX daemon pods"
  tmux split-window -h -l 45% -t "${board}" "${cmd} pane-objects"
  tmux select-pane -T objects
  tmux set -t "${session}" pane-border-status top > /dev/null
  tmux set -t "${session}" pane-border-format ' #{pane_title} ' > /dev/null
  tmux select-pane -t "${session}:0.0"
  if [[ -n "${TMUX:-}" ]]; then
    tmux switch-client -t "${session}"
  else
    tmux attach -t "${session}"
  fi
}
