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

# Scale tests on KWOK nodes in kind. See README.md.

SHELL := /bin/bash
SCRIPTS := $(CURDIR)/scripts
STATE := $(CURDIR)/.state
export KUBECONFIG ?= $(STATE)/kubeconfig
SCALEBENCH := $(CURDIR)/bin/scalebench

# Fake GB200 NVL72 racks: CLIQUES x 18 nodes.
CLIQUES ?= 8
CLIQUE_SIZE ?= 18
# per-cd | per-cd-reservations | static
MODE ?= static
# Extra flags for scalebench admission / static, e.g. ARGS="--compute-domains 500"
ARGS ?=

.PHONY: all scalebench cluster nodes nodes-down driver preflight admission static matrix sweep scale-static scale-per-cd scale-per-cd-reservations scale-bootstrap scale-single-cd scale-all watch summary compare delete-cluster

all: cluster nodes driver preflight

scalebench:
	cd ../.. && go build -o $(SCALEBENCH) ./tests/scale/cmd/scalebench

cluster:
	$(SCRIPTS)/create-cluster.sh

nodes: scalebench
	$(SCALEBENCH) nodes up --cliques $(CLIQUES) --clique-size $(CLIQUE_SIZE)

nodes-down: scalebench
	$(SCALEBENCH) nodes down

driver:
	$(SCRIPTS)/install-driver.sh $(MODE)

preflight: scalebench
	$(SCALEBENCH) preflight

admission: scalebench
	$(SCALEBENCH) admission $(ARGS)

static: scalebench
	$(SCALEBENCH) static $(ARGS)

# SUITE=per-cd|static|all, REPEAT=3
matrix:
	REPEAT=$(or $(REPEAT),3) $(SCRIPTS)/run-matrix.sh $(or $(SUITE),all) $(ARGS)

# Grow the cluster and run each mode at every size; see scripts/run-sweep.sh.
sweep:
	$(SCRIPTS)/run-sweep.sh $(ARGS)

# Scale one mode from 1008 to 5040 nodes with a grid of workloads; see
# scripts/run-scale.sh.
scale-static:
	$(SCRIPTS)/scale-static.sh $(ARGS)

scale-per-cd:
	$(SCRIPTS)/scale-per-cd.sh $(ARGS)

scale-per-cd-reservations:
	$(SCRIPTS)/scale-per-cd-reservations.sh $(ARGS)

# Compare workload bootstrap with per-ComputeDomain and static IMEX daemons.
scale-bootstrap:
	$(SCRIPTS)/scale-bootstrap.sh $(ARGS)

# One ComputeDomain across 1008 to 10080 nodes, per-ComputeDomain and static.
scale-single-cd:
	$(SCRIPTS)/scale-single-cd.sh $(ARGS)

# The bootstrap comparison, then the single ComputeDomain sweep, even if the
# first fails.
scale-all:
	-$(SCRIPTS)/scale-bootstrap.sh $(ARGS)
	$(SCRIPTS)/scale-single-cd.sh $(ARGS)

# Print the state of a run every 5s, from a second terminal.
watch:
	$(SCRIPTS)/watch.sh

# make summary DIR=results/sweep-...
summary: scalebench
	$(SCALEBENCH) summary --csv-dir $(firstword $(DIR)) $(DIR)

# make compare A=results/.../a.json B=results/.../b.json
compare: scalebench
	$(SCALEBENCH) compare $(A) $(B)

delete-cluster:
	$(SCRIPTS)/delete-cluster.sh
