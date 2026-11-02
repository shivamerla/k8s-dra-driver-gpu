# ComputeDomain scale tests

These tests run the real compute-domain-controller and kube-scheduler against
thousands of fake GB200 nodes ([KWOK](https://kwok.sigs.k8s.io/)) in a kind
cluster. They measure:

- **Channel admission.** How much channel admission costs the scheduler and
  how long it delays a workload: its binding with per-ComputeDomain IMEX
  daemons, which wait for it through binding conditions, or its Prepare with
  shared IMEX daemons. The same workload runs with
  `ComputeDomainChannelReservations` off and on, and the two runs are
  compared.
- **Static IMEX daemons.** How the `ComputeDomainDaemonSet` rollout, a rolling
  update, an NVLink repartition and node replacement behave at scale.

## What is real and what is faked

| Real | Faked |
| --- | --- |
| kube-apiserver, kube-scheduler (DRA allocation, binding conditions, device taints) | Nodes and pods (KWOK marks bound pods Running and Ready) |
| compute-domain-controller: templates, claim admission, node ownership, `ComputeDomainDaemonSet` rollout | Kubelet plugin ResourceSlices, published by `scalebench` |
| DaemonSet controller, pod GC, garbage collection | IMEX daemons' API writes, simulated by `scalebench` |

**Matching the kubelet plugin.** The fake ComputeDomain devices come from
[fakeslices](fakeslices/devices.go). The test
`TestScaleFakeSlicesMatchPublishedDevices` in
`cmd/compute-domain-kubelet-plugin` fails if they drift from what the kubelet
plugin publishes.

**The IMEX daemon simulator.** For each daemon pod on a fake node,
`scalebench` does what `compute-domain-daemon` does against the API server:
- labels the pod with its clique;
- registers in the ComputeDomainClique, NotReady and then Ready;
- unregisters when the pod terminates.

A daemon whose node was deleted does not unregister, so the controller has to
clean up its entry.

**Not covered:**
- Prepare and Unprepare, since there is no kubelet;
- real IMEX and CDI;
- GPU kubelet plugin logic. Fake GPUs exist only so that allocation costs what
  it would on real nodes.

## Prerequisites

The tests need a Linux host with Docker, `kind` v0.33+, `kubectl`, `helm`,
`make` and Go 1.26. On Ubuntu 24.04, `scripts/install-tools.sh` installs them
and raises the inotify and open file limits that kind needs.

**Which host to use.** One GB200 node is plenty: the control plane runs in one
kind container, and fake nodes cost only API objects. Don't use a node that's
in your real GPU cluster, since kind and Docker would share it with its
kubelet. Cordon and drain it first, or use a separate machine.

**Kubernetes version.** Kubernetes 1.36+ is required, and the default is
1.37.0. Binding conditions, multiple allocations and device taints are on by
default there, and `scalebench preflight` checks that the API server serves
them. For other versions, set `K8S_VERSION`, and `SCALE_FEATURE_GATES` if the
gates are off by default.

## Quick start

Run from `tests/scale`. `KUBECONFIG` defaults to `.state/kubeconfig`.

1. Create the cluster, the fake nodes and the driver. The default is 144 nodes
   in static mode:

   ```bash
   make cluster nodes driver MODE=static CLIQUES=8
   ```

   This creates the kind cluster with KWOK, adds 8 cliques of 18 fake nodes,
   then builds the driver image from this checkout, loads it into kind,
   installs the chart and runs preflight.

2. Run the static IMEX daemon phases:

   ```bash
   make static ARGS="--recreate --keep"
   ```

3. Run one admission workload of 50 ComputeDomains with 4 pods each:

   ```bash
   make admission ARGS="--compute-domains 50 --pods-per-cd 4"
   ```

4. Compare channel reservations off and on, 3 runs each:

   ```bash
   make matrix SUITE=per-cd ARGS="--compute-domains 400 --pods-per-cd 4"
   ```

To scale up, add cliques. Nodes are created idempotently, so `make nodes`
adds the missing ones:

```bash
make nodes CLIQUES=112
```

## Watching a run

In a second terminal, from `tests/scale`:

```bash
make watch
```

Every 5 seconds this prints:
- the fake nodes and cliques;
- the `ComputeDomainDaemonSet` columns, and any clique that stalled;
- the IMEX daemon pods by phase;
- the clique registrations by status;
- the channel claims as pending, awaiting admission, admitted or rejected;
- the workload pods by phase;
- the scheduler's pending pods and attempts, and the controller's queue depths.

To look at one thing directly (`kubectl` uses `.state/kubeconfig`):

```bash
kubectl get cdds                                     # Ready/Desired/Updated/Cliques
kubectl get cdds default -o yaml                     # per-clique status, stalledReason
kubectl get computedomaincliques -A                  # one per ComputeDomain and clique
kubectl get pods -n dra-driver-nvidia-gpu -l resource.nvidia.com/computeDomainDaemonSet -o wide
kubectl get computedomains -A
kubectl get resourceclaims -A                        # STATE column: pending, allocated, reserved
kubectl get pods -A -l scale.nvidia.com/compute-domain --field-selector=status.phase=Pending
kubectl logs -n dra-driver-nvidia-gpu -l dra-driver-nvidia-gpu-component=controller -f
```

The admission condition of one claim:

```bash
kubectl get resourceclaim -n NAMESPACE NAME \
  -o jsonpath='{range .status.devices[*].conditions[*]}{.type}={.status} {.reason}{"\n"}{end}'
```

Pass `--keep` to a scenario to keep its objects after the run. Otherwise an
admission run deletes its namespaces and waits up to `--cleanup-timeout`
(default 10m) for them to go, and records how long that took.

**A slow teardown cannot spill into the next run.** Before each measured run,
the scripts wait up to `IDLE_TIMEOUT` (default 15m) for the namespaces and
per-ComputeDomain daemon DaemonSets of earlier runs to go. If they are still
there, the compute-domain-controller's work queue is backed up with their
deletions, which would keep the next run's ComputeDomains waiting for their
claim templates. The scripts then restart the controller, which starts with
an empty queue, and wait again; if that does not help, they stop the mode
with an error rather than measure a busy cluster. They also remove node
owner labels left behind.

## Showing that it scales

`make sweep` grows the cluster through `SWEEP_CLIQUES`. The default is 144,
504, 1008, 2016 and 4032 nodes. At each size it does this for every mode of
`SWEEP_MODES` (default `per-cd-reservations static`):
1. In `static` mode only, runs the static IMEX daemon phases.
2. Runs an admission workload that puts one pod on `SWEEP_FILL` (default
   half) of the nodes.

Then it writes `summary.md`, `admission.csv` and `static.csv` to
`results/sweep-<timestamp>/`. For a quick first pass:

```bash
make sweep SWEEP_CLIQUES="8 56" SWEEP_MODES="static"
make summary DIR=results/sweep-...                   # re-tabulate any results
```

**What makes the case.** These are the measurements, in `summary.md`, that
show channel admission and static IMEX daemons don't hold up scheduling as
nodes grow:

| Claim | Measurement | Expected |
| --- | --- | --- |
| Channel admission stays off the scheduler's path | `allocated->admitted` p99 in `static` | tens of milliseconds, well below create to bound |
| Admission does not grow with cluster size | `allocated->admitted` p50/p99 across sizes | flat |
| Nothing fails or retries | `unschedulable attempts`, `channel rejections`, `APF rejected` | 0, `ok` is PASS |
| The controller keeps up | `admission queue peak`, controller `workqueue_queue_duration_seconds` | bounded, not growing with size |
| The control plane has room | `controller/scheduler/apiserver cores`, `controller MiB`, `apiserver MiB` | grows at most linearly with objects |
| Runs clean up before the next one | `teardown s` | a number, not `>N`: `>N` means the namespaces were still going after `--cleanup-timeout`, and the next run starts on a busy controller |
| Static daemons roll out by clique | `rollout s`, `update s`, `max cliques disrupted` | update disrupts at most `--max-unavailable-cliques` |
| Recovery is per clique | `clique restart p50/max`, `node-churn s` | independent of the number of cliques |

Each result JSON also keeps every sampled gauge as a time series (`samples`
under `gaugePeaks`), for plots of queue depth, pending pods and memory over a
run.

## Finding scaling bottlenecks, one mode at a time

Each mode has its own script, which you can run on its own:

```bash
make scale-static
make scale-per-cd
make scale-per-cd-reservations
```

Each script:
1. Creates the cluster if needed, resets the fake nodes to the first size and
   installs the driver in its mode.
2. For every size of `SCALE_CLIQUES` (default 1008, 2016, 3024, 4032 and 5040
   nodes):
   - in static mode, runs the static IMEX daemon phases;
   - runs one admission workload for each ComputeDomain size in
     `PODS_PER_CD_LIST` (default 4 and 18 pods) at each share of the nodes in
     `FILL_LIST` (default 25%, 50% and 90%).
3. Writes `results/<mode>-<timestamp>/summary.md` and the CSVs.

That is 30 admission runs per mode with the defaults; expect a few hours for
each. For a short first pass:

```bash
make scale-static SCALE_CLIQUES="56 112" PODS_PER_CD_LIST=4 FILL_LIST=0.5
```

**Reading `summary.md`:**
- The admission table names each run's **slowest stage** (scheduling,
  admission or binding, as a share of `create -> bound` p50), and **flags**
  what limited it:

  | Flag | Means |
  | --- | --- |
  | `controller client QPS` | admission runs at the controller's client QPS (`CONTROLLER_QPS`), one claim status write each |
  | `admission queue backlog` | claims wait in the controller's admission queue |
  | `scheduler backlog` | pods wait a second or more to be allocated |
  | `API server throttling` | API Priority and Fairness rejected or queued requests |
  | `etcd p99` | etcd writes are slow, usually the host's disk |
  | `channel rejections` | admission refused channels, by reason; `NodeUsedByOtherComputeDomain` is node ownership limiting packing |
  | `unschedulable attempts`, `pods not bound` | the workload did not fit |

- **Scaling by workload** shows each workload at every size, with each stage's
  p99. Under each one, a line compares the smallest and largest cluster. A
  stage whose p99 grows at least as fast as the node count is called out as
  the scaling bottleneck.
- The static table adds `update s per clique`: if it stays constant, the
  rolling update scales with the number of cliques, as designed.

### Workload bootstrap: per-ComputeDomain vs static IMEX daemons

```bash
make scale-bootstrap
```

This runs `per-cd-reservations` and then `static` over the same cluster sizes
and workloads, with one pod on 95% of the nodes (`BOOTSTRAP_FILL`): about 960
to 4790 pods. It writes both to `results/bootstrap-<timestamp>/`, and
`summary.md` starts with **Workload bootstrap by mode**: the two modes side
by side for each workload and size, and the difference.

**What bootstrap measures.** The kubelet plugin's Prepare succeeds only once
the IMEX daemon on the pod's node is registered Ready in the
ComputeDomainClique, and, with shared IMEX daemons, the controller assigned
the claim's channel. Bootstrap is from creating a pod until it is bound
**and** Prepare could succeed. `prepare wait` is the part after binding.
- With per-ComputeDomain daemons, each new ComputeDomain starts a daemon on
  each of its nodes: a DaemonSet pod is placed and the daemon registers.
- Static daemons are Ready before the workload arrives, and the controller
  assigns the channel while the pod binds, so `prepare wait` should stay near
  0 at every size.

**What it leaves out.** There is no kubelet, so:
- The real IMEX daemon's startup time is not included. By default, a
  simulated daemon is Ready as soon as its pod runs, so only the control plane
  cost is measured. Time how long a real daemon takes from its pod running to
  Ready on GB200 nodes, and pass it to include it:
  ```bash
  make scale-bootstrap DAEMON_READY_DELAY=15s
  ```
- The kubelet's retries of a failed Prepare would add to the real wait.
  Bootstrap is the earliest Prepare could succeed.

### Everything at once

```bash
make scale-all
```

This runs `make scale-bootstrap` and then `make scale-single-cd`, the second
even if the first fails. Run it in `tmux`, as it takes many hours.

### One ComputeDomain across the cluster

```bash
make scale-single-cd
```

This runs one ComputeDomain whose pods cover 95% of the nodes
(`SINGLE_CD_FILL`), from 1008 to 10080 nodes (`SINGLE_CD_CLIQUES`), with
`per-cd-reservations` and then `static`. It is the most nodes and pods one
domain spans, so it shows the limits of the controller and the kubelet
plugin for a single domain:
- Per-ComputeDomain mode runs one DaemonSet with a daemon on every node, the
  controller takes ownership of every node, and 280 to 560 cliques of the one
  domain register.
- Static mode shares one channel per clique across all the pods.

Static mode only rolls out its daemons at each size (`STATIC_PHASES=rollout`).
Each run may take up to 60 minutes (`ADMISSION_TIMEOUT`) and 30 minutes to
tear down (`CLEANUP_TIMEOUT`). Results go to
`results/single-cd-<timestamp>/`, and `summary.md` has a **Single
ComputeDomain** table: create to bound, `bootstrap max` (when the last pod
could be prepared, so the whole domain is up), admission, control plane
load and teardown time, by mode and size.

At 10080 nodes the kind control plane needs roughly twice the memory of
5040 nodes, and etcd's default 2 GB quota can run out; watch for `mvcc:
database space exceeded`. For a first pass, stop at 5040 nodes:

```bash
make scale-single-cd SINGLE_CD_CLIQUES="56 280"
```

To compare modes, put their result directories in one summary:

```bash
make summary DIR="results/static-... results/per-cd-..."
```

## Modes

`scripts/install-driver.sh MODE` reinstalls the driver in a different mode,
removing what the previous mode deployed when the IMEX daemon lifecycle
changes.

| Mode | IMEX daemons | Binding conditions |
| --- | --- | --- |
| `per-cd` | per ComputeDomain | no |
| `per-cd-reservations` | per ComputeDomain | yes, plus node ownership |
| `static` | `ComputeDomainDaemonSet` (shared) | no, Prepare waits for the channel |

**The `per-cd` pair is not like for like.** With reservations, the controller
labels a node with its ComputeDomain at admission, which places a
per-ComputeDomain daemon pod there. Without reservations, the kubelet plugin
sets that label during Prepare, which never runs on fake nodes. So only
`per-cd-reservations` runs daemon pods.

Static mode requires channel reservations, so it has no "off" run. Its
admission cost shows directly as `allocated->admitted`: shared daemons,
channels unique per clique, no node ownership.

Host-managed IMEX daemons are not covered: scalebench refuses a driver
installed with `daemonLifecycle=hostManaged`.

## Scenarios

### `scalebench admission`

**What it does:**
1. Creates `--compute-domains` ComputeDomains spread over `--namespaces`.
2. Waits for the controller to create their workload ResourceClaimTemplates.
3. Creates `--pods-per-cd` pods for each ComputeDomain at `--create-qps`. Each
   pod has a channel claim and `--gpus-per-pod` fake GPUs.
4. With `--churn`, keeps replacing ComputeDomains for that long, so that
   channels are released and reused.

Four GPUs per pod (the default) puts one pod on each node, as in production.
`--same-clique` keeps the pods of each ComputeDomain in one clique.

**What it reports per pod.** Times are taken from watch events (millisecond
resolution, the same for every run):

| Measure | What it covers |
| --- | --- |
| `create -> bound` | what users see |
| `create -> allocated` | the scheduler, including queueing, until PreBind writes the allocation |
| `allocated -> admitted` | the controller's admission; without binding conditions it runs while the pod binds |
| `admitted -> bound` | the scheduler noticing the condition and binding (binding conditions only) |
| `allocated -> bound` | the whole PreBind wait |

**Other results:**
- Throughput: pods bound per second.
- Channel rejections, by reason.
- Changes in scheduler, controller and API server metrics, and gauges sampled
  every 5s, with their peaks.
- CPU seconds and peak memory of the API server, scheduler and controller;
  API Priority and Fairness rejections and queueing; etcd latency of claims,
  pods and cliques.

### `scalebench static`

The phases run in order (`--phases`). Each phase waits until:
- every clique-labeled node runs a ready daemon of the current revision;
- every daemon is registered Ready in its clique, with no stale entries;
- the `ComputeDomainDaemonSet` status agrees.

| Phase | What it does | What it checks |
| --- | --- | --- |
| `rollout` | Creates the `ComputeDomainDaemonSet` (`--recreate` replaces an existing one) | time until all daemons are ready, registered, and reported |
| `update` | Changes `spec.logVerbosity` to roll out a new revision | that no more than `--max-unavailable-cliques` cliques are disrupted at once; per-clique restart time |
| `repartition` | Moves `--repartition-nodes` nodes to a new clique label, then back | that their daemons restart into the new clique and old entries go |
| `node-churn` | Deletes all nodes of one clique, then recreates them | that stale clique entries are removed and daemons come back |

A run fails on a phase that does not settle in `--phase-timeout`, or on a
failed check.

### `scalebench agent`

Runs the slice publisher and daemon simulator on their own, for manual
experiments. Pass `--agent=false` to scenarios while it runs.

## Reading the results

Each run writes `results/<scenario>-<mode>-<label>-<run>.json` and prints a
summary. To compare two runs:

```bash
scalebench compare BASELINE.json CANDIDATE.json
```

**What the comparison should show.** With per-ComputeDomain IMEX daemons,
pods wait for their admission through binding conditions in PreBind, in the
scheduler's binding cycle, not in the scheduling cycle. Shared IMEX daemons
publish no binding conditions: pods bind at once, and the kubelet plugin's
Prepare waits for the channel the controller assigns meanwhile. So:

- **Throughput** and `scheduler_scheduling_attempt_duration_seconds` should
  match between the off and on runs.
- **`create -> bound`** should not grow in shared modes. In
  `per-cd-reservations` it grows by at least `allocated -> admitted`, and by
  up to 5s more: the scheduler checks binding conditions once, then every 5s.
- **`scheduler_schedule_attempts_total{result="unschedulable"}`** and channel
  rejections should stay at 0 in shared modes. In `per-cd-reservations`,
  `NodeUsedByOtherComputeDomain` rejections show how often the owner taint
  arrived too late.
- **If `allocated -> admitted` grows with load**, look at
  `workqueue_queue_duration_seconds{name="compute-domain-claim-admission"}`.
  The controller's client QPS (`CONTROLLER_QPS`, default 100) limits how fast
  claims are admitted, at one status write each.
- **`scheduler_plugin_execution_duration_seconds`** samples about 10% of
  attempts. `scheduler_framework_extension_point_duration_seconds` covers all
  of them.

## Settings

The scripts read these from the environment; see
[scripts/common.sh](scripts/common.sh):

| Variable | Default | |
| --- | --- | --- |
| `K8S_VERSION` | `v1.37.0` | kind node image |
| `KWOK_VERSION` | `v0.8.0` | |
| `SCALE_FEATURE_GATES` | | extra Kubernetes feature gates |
| `SCHEDULER_QPS` / `SCHEDULER_BURST` | `500` / `1000` | kube-scheduler client |
| `SCHEDULER_PARALLELISM` | `48` | kube-scheduler `parallelism` (upstream default 16); set when the cluster is created |
| `CONTROLLER_QPS` / `CONTROLLER_BURST` | `100` / `200` | compute-domain-controller client (chart default) |
| `DRIVER_IMAGE_REPO` / `DRIVER_IMAGE_TAG` | `localhost/dra-driver-nvidia-gpu` / `scale-<commit>` | |
| `BUILD` | `false` | rebuild the driver image even if it exists |
| `REPEAT` | `3` | runs per mode in `make matrix`; `make sweep` defaults to 1 |
| `SCALE_CLIQUES` | `56 112 168 224 280` | sizes of `make scale-<mode>`, in cliques |
| `PODS_PER_CD_LIST` | `4 18` | ComputeDomain sizes of `make scale-<mode>`; `all` runs one ComputeDomain over the filled nodes |
| `FILL_LIST` | `0.25 0.5 0.9` | shares of the nodes `make scale-<mode>` fills |
| `MAX_UNAVAILABLE_CLIQUES` | `25%` | cliques the static update phase restarts at a time, a number or percentage |
| `DAEMON_READY_DELAY` | `0s` | simulated IMEX daemon startup, from pod running to Ready |
| `BOOTSTRAP_FILL` | `0.95` | share of the nodes `make scale-bootstrap` fills |
| `ADMISSION_TIMEOUT` | `15m` | limit of each admission run in `make scale-<mode>`; `60m` in `make scale-single-cd` |
| `IDLE_TIMEOUT` | `15m` | how long the scripts wait for earlier runs to clean up, before and after restarting the controller |
| `CLEANUP_TIMEOUT` | `10m` | how long each run waits for its namespaces to go; `30m` in `make scale-single-cd` |
| `SINGLE_CD_CLIQUES` | `56 112 168 224 280 560` | sizes of `make scale-single-cd`, in cliques |
| `SINGLE_CD_FILL` | `0.95` | share of the nodes the single ComputeDomain fills |
| `SINGLE_CD_MODES` | `per-cd-reservations static` | modes `make scale-single-cd` runs |
| `SWEEP_CLIQUES` | `8 28 56 112 224` | cluster sizes of `make sweep`, in cliques |
| `SWEEP_MODES` | `per-cd-reservations static` | modes `make sweep` runs at each size |
| `SWEEP_FILL` | `0.5` | share of the nodes the sweep's admission workload uses |
| `CONTROLLER_METRICS_PORT` | `8080` | controller metrics port read by `make watch` |
