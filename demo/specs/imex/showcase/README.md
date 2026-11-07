# ComputeDomain showcase

A three minute live demo of static IMEX daemons and ComputeDomain channel
reservations on two GB200 nodes of one NVLink clique. `demo.sh` draws each
node as four GPU cells, colored by the ComputeDomain that uses them and
labeled with the IMEX channel the pod got.

## Before the demo

The driver runs with static IMEX daemons and channel reservations:

```bash
helm upgrade -i dra-driver-nvidia-gpu deployments/helm/dra-driver-nvidia-gpu \
  -n dra-driver-nvidia-gpu \
  --set resources.computeDomains.imex.daemonLifecycle=static \
  --set featureGates.ComputeDomainDaemonSet=true \
  --set featureGates.ComputeDomainChannelReservations=true
kubectl apply -f demo/specs/imex/computedomaindaemonset.yaml
kubectl wait computedomaindaemonset default --for=condition=Ready --timeout=10m
```

The health scene taints a GPU with a `DeviceTaintRule`, which needs the
`DRADeviceTaints` and `DRADeviceTaintRules` Kubernetes feature gates on the
kube-apiserver, kube-scheduler and kube-controller-manager.

Pull `ubuntu:24.04` on both nodes, then rehearse once and clean up:

```bash
DEMO_AUTO=1 demo/specs/imex/showcase/demo.sh all
demo/specs/imex/showcase/demo.sh cleanup
```

The nodes are the first two of a clique, or `DEMO_NODES="node-a node-b"`.
Each scene also runs on its own: `demo.sh static|channels|health|upgrade`,
and `demo.sh board` redraws the nodes. Enter moves to the next step.

## Talk track

| Time | Scene | Say |
| --- | --- | --- |
| 0:00 | 1 · Static IMEX daemons | No workload runs, yet every node already has its IMEX daemon. The admin owns it, not each job, so a job never waits for a daemon to start. |
| 0:30 | 2 · Channel reservations | Two ComputeDomains share one node: four GPUs, three pods, three channels. Each template gets its own channel, isolated from the others. alpha/train has the same channel on both nodes, so its pods share memory over NVLink. |
| 1:30 | 3 · GPU health | One GPU reports a fatal XID. Only beta, which used it, is evicted. It needs two healthy GPUs, so it moves to the other node and gets its channel at once. alpha never notices. Before reservations a pod held all four GPUs of a node, so one bad GPU took the whole node down. |
| 2:15 | 4 · Daemon upgrades | Daemons change only when the admin says so: OnDelete, or RollingUpdate by whole cliques with a soak time, a pause switch and rack block ordering. The workloads keep running through the rollout. |

## How it works

- Every pod is a one-replica Deployment that claims a channel template of its
  ComputeDomain and one or two GPUs. The pods print the channels they see.
- beta starts on the first node because the second node is cordoned while it
  is scheduled. Nothing else in the demo is pinned.
- The health scene creates a `DeviceTaintRule` with `effect: NoExecute` on one
  GPU of beta. The driver itself only taints unhealthy GPUs `NoSchedule`; an
  admin rule like this one is how a cluster evicts their pods.
- The upgrade scene changes the daemons' log verbosity, which restarts them
  clique by clique.

## Backup: 8 racks on KWOK

`kwok-demo.sh` needs no GPUs. It runs the driver in a kind cluster with 8 fake
GB200 NVL72 racks of 18 KWOK nodes, built with the [scale test](../../../../tests/scale/README.md)
tooling, and draws one row of 18 nodes per rack, colored by the state of
each node's IMEX daemon.

```bash
demo/specs/imex/showcase/kwok-demo.sh setup   # ~10 minutes, once: cluster, driver, fake racks
demo/specs/imex/showcase/kwok-demo.sh run     # the demo
demo/specs/imex/showcase/kwok-demo.sh reset   # before running it again
demo/specs/imex/showcase/kwok-demo.sh teardown --cluster
```

`setup` starts `scalebench agent` in the background. It publishes the fake
GPUs and plays the IMEX daemons, registering each ready daemon in its
ComputeDomainClique.

| Time | Scene | Say |
| --- | --- | --- |
| 0:00 | 1 · Day 0 | 144 nodes, no daemons. The admin creates one ComputeDomainDaemonSet, and every rack fills in before any job runs. The update strategy is set now: two racks at a time, a 5s soak, block by block. |
| 0:45 | 2 · Workloads | A 36-node training job on racks 1–2 and an inference job packed two pods per node on rack 5 start at once: their daemons are already up. |
| 1:15 | 3 · Upgrade | A new daemon image rolls through block 1 two racks at a time while the jobs keep running. The admin pauses before block 2, then resumes. |
| 2:15 | 3 · Stalled rack | One node in rack 6 cannot start the new daemon. The rack turns red with the reason, and the rollout holds instead of spreading a bad revision. The admin cordons the node, and the rollout finishes. |

The broken node is simulated by keeping KWOK from making new pods on it ready
(an extra selector on KWOK's `pod-ready` Stage); `reset` and `teardown`
remove it.
