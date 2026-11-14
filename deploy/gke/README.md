# GopherGrid on GKE (Terraform), with KEDA and the cluster autoscaler

[deploy/k8s/gophergrid.yaml](../k8s/gophergrid.yaml) had only been validated with `kubeconform`.
This directory runs it on a real cluster: Google Kubernetes Engine, built with Terraform, measured,
then destroyed.

| File | What it does |
|---|---|
| [main.tf](main.tf), [variables.tf](variables.tf), [versions.tf](versions.tf), [outputs.tf](outputs.tf) | Enables the GKE and Artifact Registry APIs, creates a Docker repo, a zonal GKE Standard cluster and a spot `e2-standard-2` node pool (2 nodes, autoscaling to 4). Logging and monitoring are cut to system components, so nodes are the only real cost |
| [kustomization.yaml](kustomization.yaml) | Overlay on `../k8s/gophergrid.yaml`: registry images, GPU worker removed, workers sized to 0.5 vCPU, and the KEDA trigger switched from Prometheus to the scheduler's own `GET /v1/autoscale` (`metrics-api`, 1 worker per 100 queued jobs, max 6) |
| [ggbench-job.yaml](ggbench-job.yaml) | The repo's `ggbench` as a Kubernetes Job. It submits N jobs and exits non-zero unless every job has exactly one result |
| [run.sh](run.sh) | `apply`, `push`, `deploy` (KEDA 2.21.0 + GopherGrid), `bench` (with chaos), `destroy` (terraform destroy + project delete) |
| [tools.Dockerfile](tools.Dockerfile) | gcloud + kubectl + gke-gcloud-auth-plugin + terraform 1.17.2, so the laptop only needs Docker |

## What ran (2026-11-14)

A throwaway project with billing, created for this run and deleted after it. Kubernetes
v1.36.4-gke, KEDA 2.21.0, GopherGrid images built from `main` and pushed to Artifact Registry.

The benchmark: **3,000 jobs of 500 ms** (100 mCPU each), submitted in one burst. One worker pod
was deleted 60 s in, and the **scheduler leader pod** 30 s after that.

| | run 1: 2 fixed nodes | run 3: node autoscaling 2 → 4 |
|---|---:|---:|
| results / duplicates / failed | **3,000 / 0 / 0** | **3,000 / 0 / 0** |
| wall time | 174 s | **127 s** |
| throughput | 17.2 jobs/s | **23.6 jobs/s** |
| workers KEDA asked for → running | 6 → **2** (4 Pending all run) | 6 → **6** |
| queue wait p50 / p95 | 91.0 / 165.5 s | 88.1 / 123.7 s |

(Run 2 is left out: I didn't reset the jobs table between runs, so `ggbench` counted run 1's
3,000 results too and reported "6000 results for 3000 jobs". That was my bug, not GopherGrid's;
duplicates were still 0. `-reset` fixes it.)

Logs: [bench/out/gke/](../../bench/out/gke/).

## What it showed

1. **The manifest works on a real cluster, apart from sizing.** Postgres, etcd, 2 schedulers
   spread across nodes by anti-affinity, workers, and KEDA all came up from the same YAML that
   had only been schema-checked. The one change needed was the worker size: the manifest's
   2-vCPU workers can't fit an `e2-standard-2`.
2. **Exactly-once survives losing the leader on Kubernetes.** Deleting the scheduler pod that
   held the etcd lease moved leadership to the other replica, and both runs still ended with
   exactly one result per job. A Kubernetes pod delete is graceful: SIGTERM, then the worker's 40 s
   drain, the scheduler resigning its lease. So this is the polite failure. The SIGKILL case was
   tested on the homelab in `docs/homelab.md`.
3. **Pod autoscaling without node autoscaling doesn't add capacity.** KEDA scaled the worker
   Deployment to 6 within 30 s, but in run 1 only 2 fit. The other 4 sat `Pending` for the whole
   run, the cluster autoscaler was off (`NotTriggerScaleUp`), and throughput stayed at 2
   workers' worth.
4. **With the node pool autoscaling, the two layers chain up.** The pool had already shrunk back
   to 2 nodes when run 3 started. Pending workers triggered a scale-up (`TriggeredScaleUp`), and
   two new spot nodes registered about 60–75 s later. All 6 workers were running about 90 s after
   the burst, and the run took 27% less time.
5. **For short bursts, scale-up time dominates.** A 2-minute burst spends about 1.5 minutes
   waiting for nodes. Keeping a warm buffer node (`min_node_count`), or a smaller KEDA target so
   scaling starts earlier, would matter more than a faster scheduler.

## Cost and teardown

About 45 minutes of cluster time: 2–4 spot `e2-standard-2` nodes (about $0.02/hour each), small
standard persistent disks, and a few hundred MB in Artifact Registry. The free tier covers the
management fee of one zonal cluster. Estimated total: **under $0.25**, within the $2 the user
approved.

Teardown, also run:

```bash
sh run.sh <project> destroy     # terraform destroy (5 resources), then gcloud projects delete
```

`terraform destroy` reported `Resources: 5 destroyed`, and the project went to
`DELETE_REQUESTED` (Google keeps it recoverable for 30 days, then purges it).

## Run it yourself

```bash
docker build -t gophergrid/gke-tools:1 -f tools.Dockerfile .
docker run --rm -it -v gcloud-config:/root/.config/gcloud gophergrid/gke-tools:1 gcloud auth login --no-launch-browser --update-adc
# create a project + link billing, then:
T="docker run --rm -v gcloud-config:/root/.config/gcloud -v kubeconfig:/root/.kube -v $PWD/..:/deploy -w /deploy/gke gophergrid/gke-tools:1"
$T sh run.sh <project> apply
TAG=gke1 ../../scripts/build.sh scheduler worker ggbench   # then docker push to the registry output
$T sh run.sh <project> deploy
N=3000 MS=500 $T sh run.sh <project> bench
$T sh run.sh <project> destroy
```
