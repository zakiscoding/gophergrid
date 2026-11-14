# Kubernetes

[gophergrid.yaml](gophergrid.yaml) runs the same system on Kubernetes. It passes
`kubeconform -strict` against Kubernetes 1.33 and the KEDA CRD schema. It has been run on a
live GKE cluster with KEDA and the cluster autoscaler: see [../gke/README.md](../gke/README.md).

```bash
kubectl create namespace gophergrid
kubectl -n gophergrid create secret generic gophergrid-db --from-literal=password="$(openssl rand -hex 24)"
# Images: scripts/build.sh, then `kind load docker-image gophergrid/scheduler:dev gophergrid/worker:dev`
# (or push them to a registry and change the image fields).
kubectl -n gophergrid apply -f deploy/k8s/gophergrid.yaml
```

| Object | Notes |
|---|---|
| `postgres`, `etcd` | Single replicas for a lab. Production wants a replicated Postgres and 3 etcd members. |
| `scheduler` | 2 replicas, spread across nodes. Each advertises its pod IP, so a follower's redirect points workers at the leader pod. SIGTERM resigns leadership at once. |
| `worker` | CPU workers. `CPU_MILLIS` and `MEMORY_MB` match the pod limits, so placement and cgroups agree. 40 s grace for draining. |
| `worker-gpu` | `GPU_MODE=nvidia`, requests `nvidia.com/gpu: 1`. Needs the NVIDIA device plugin, as on the homelab k3s node. Starts at 0 replicas. |
| `ScaledObject` | KEDA scales `worker` on the leader's queue depth: one worker per 100 queued jobs, 1 to 20. |
