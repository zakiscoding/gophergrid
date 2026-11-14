# Running GopherGrid across the homelab

**Status: running since 2026-11-13** on three machines of the
[homelab](https://github.com/zakiscoding/homelab) tailnet. The compose files are in
[`deploy/homelab/`](../deploy/homelab); the measurements are [below](#measured-on-the-homelab).

## Layout

Workers only make outbound gRPC calls to a scheduler. Nothing connects to them, so any machine
on the tailnet can join. Every port binds to `127.0.0.1` and is shared on the tailnet only with
`tailscale serve --tcp`, the same pattern the GPU PC already uses for vLLM. Nothing is reachable
from the LAN.

```mermaid
flowchart LR
    subgraph T["Tailscale tailnet (tailscale serve, tailnet only)"]
        subgraph I["mos-imac (M-series, Colima)"]
            PG[("Postgres :5433")]
            ET[("etcd :2379")]
            S1["scheduler-1<br/>gRPC :9095 · REST :8090"]
        end
        subgraph L["hp-laptop (Docker Desktop)"]
            S2["scheduler-2<br/>gRPC :9095 · REST :8091"]
            WL["worker hp-laptop<br/>4 cores, CPU only"]
        end
        subgraph G["GPU PC, Windows boot (zakar, WSL2)"]
            GW["worker gpu-pc<br/>8 cores + RTX 5070<br/>GPU_MODE=nvidia"]
            V["vllm-chat (vLLM)<br/>Qwen2.5-1.5B"]
        end
    end
    S1 & S2 --- PG & ET
    WL & GW -- "gRPC heartbeats" --> S1
    WL & GW -. "redirect when S2 leads" .-> S2
    GW -- "llm jobs" --> V
```

| Machine | Compose file | Runs | Notes |
|---|---|---|---|
| `mos-imac` | [`imac.yml`](../deploy/homelab/imac.yml) | Postgres, etcd, `scheduler-1` | arm64. Ports 9090/3000 are the homelab Prometheus/Grafana, so GopherGrid uses 9095/8090 |
| `hp-laptop` | [`laptop.yml`](../deploy/homelab/laptop.yml) | `scheduler-2`, worker `hp-laptop` | A second scheduler on another machine, so the cluster survives losing the iMac's scheduler |
| GPU PC (`zakar`, WSL2) | [`gpu-pc.yml`](../deploy/homelab/gpu-pc.yml) | worker `gpu-pc`: Go by default, or Rust with `WORKER_IMAGE=gophergrid/worker-rs:dev` | Real RTX 5070 from `nvidia-smi`; runs `container` and `benchmark` jobs with `docker run --gpus device=<uuid>`, and `llm` jobs against `vllm-chat` over the serving network. `llm` and `benchmark` exist only in the Go worker, so since 2026-11-14 the GPU PC runs Go; the Rust worker ran here before that (see [worker-rs](../worker-rs)) |

The PC is in one boot at a time. On the Ubuntu boot (`zak-gpu-node`) the same `gpu-pc.yml`
works with the same `WORKER_ID`.

## Deploying it

```bash
# laptop: one shared secret, kept out of Git (deploy/homelab/.env is ignored)
printf 'POSTGRES_PASSWORD=%s\n' "$(openssl rand -hex 24)" > deploy/homelab/.env

# iMac (arm64). Its Docker has no BuildKit, so it can't run the Dockerfile itself. The build
# stage cross-compiles, so the laptop builds arm64 images natively and streams them over SSH.
tar -c --exclude=.git . | docker buildx build --platform linux/arm64 --target scheduler \
  -t gophergrid/scheduler:dev -o type=docker,dest=- - | ssh imac-ts docker load
ssh imac-ts 'cat > ~/1tb-drive/gophergrid/deploy/homelab/.env' < deploy/homelab/.env
ssh imac-ts 'cd ~/1tb-drive/gophergrid && docker-compose -p gophergrid \
  --env-file deploy/homelab/.env -f deploy/homelab/imac.yml up -d'
ssh imac-ts 'for p in 5433 2379 9095 8090; do tailscale serve --bg --tcp $p tcp://127.0.0.1:$p; done'

# GPU PC (amd64 worker image from the laptop)
docker save gophergrid/worker:dev | gzip | ssh zak-ts "wsl -d Ubuntu -- bash -c 'gunzip | docker load'"
ssh zak-ts "wsl -d Ubuntu -- bash -c 'cd ~/gophergrid && docker compose -f gpu-pc.yml up -d'"

# laptop
tailscale serve --bg --tcp 9095 tcp://127.0.0.1:9095
tailscale serve --bg --tcp 8091 tcp://127.0.0.1:8091
docker compose -p gophergrid-homelab --env-file deploy/homelab/.env -f deploy/homelab/laptop.yml up -d
```

(Over `ssh imac-ts`, Docker needs `DOCKER_HOST` pointing at the Colima socket on the external
drive, and Tailscale's CLI is `/Applications/Tailscale.app/Contents/MacOS/Tailscale`.)

Check it from any tailnet machine:

```bash
curl -s 100.91.113.98:8090/v1/leader
curl -s 100.91.113.98:8090/v1/workers
scripts/homelab-bench.sh -n 2000 -ms 0 -name my-run -reset   # ggbench against the iMac
```

`GPU_RESERVED_MB=11300`: vLLM keeps ~11.2 GB of the card's 12 GB, so the worker offers only
the remaining ~0.9 GB to jobs. Without it, placement would treat the whole card as free.

## Measured on the homelab

2026-11-13, raw JSON in [`bench/out/homelab-*`](../bench/out). Tailnet round trip 6–7 ms
between nodes; slots below are at 250 millicores per job (16 on the laptop, 32 on the GPU PC).

| Test | Result |
|---|---|
| [`llm` jobs on the live vLLM](../bench/out/homelab-05-llm.json): 300 jobs × 8 prompts × 64 tokens, Qwen2.5-1.5B-Instruct on the RTX 5070 | **12.2 jobs/s, 5,792 completion tok/s**, request p50/p95 518/567 ms; 300/300, 0 retried. Same jobs again with the `gpu-pc` worker **SIGKILLed while running 6 of them**, back 5 s later: 300/300, 8 retried, 9.3 jobs/s, **0 duplicates** (600 jobs, 600 result rows) |
| [`benchmark` job, `matmul` suite](../bench/out/homelab-06-benchmark-matmul.json): gpu-systems-lab's `benchmarks/smoke_test.py`, 8192², 1 GPU and 900 MB requested, vLLM still serving | **63.6 TFLOPS fp16, 64.0 bf16**, 32.6 tf32, 22.9 fp32; 800 MiB peak, inside the 900 MB the job asked for; 15.5 s in the container |
| [`benchmark` jobs, `vllm-sweep` suite](../bench/out/homelab-07-inference-sweeps.json): FP16, AWQ and FP8 vLLM servers on the RTX 5070, each started, swept over concurrency 1–64 and stopped by the job ([submitter](../bench/submit_inference_sweeps.py)) | 3 configurations in 28 min of a chat-down window, ~6 min each; at 64 concurrent requests **FP8 5,735 tok/s vs FP16 4,211**. Results and charts are in [gpu-systems-lab](https://github.com/zakiscoding/gpu-systems-lab/blob/main/docs/RESULTS.md). AWQ needed 3 attempts: twice `gpu-pc` missed heartbeats for >3 s mid-run, was declared dead and its job requeued (see below) |
| Real GPU job: a `container` job running a PyTorch fp16 matmul in the vLLM image, needing 1 GPU and 600 MB | Placed by the iMac leader on `gpu-pc`'s RTX 5070 next to the live vLLM server: **41.4 TFLOPS fp16**, 9.4 s submit to result (mostly container start) |
| [Throughput](../bench/out/homelab-01-baseline.json): 2,000 zero-length jobs at 100 millicores | **1,620 jobs/s**, queue p50 567 ms; 1,321 ran on the GPU PC, 679 on the laptop; 0 duplicates |
| [Control](../bench/out/homelab-02-control.json): 3,000 × 200 ms jobs | 181 jobs/s, 0 retried, 0 duplicates |
| [Worker kills](../bench/out/homelab-03-chaos.json): same jobs, SIGKILL the laptop worker every 4 s, back after 6 s | 5 kills, 16 jobs retried, 3,000/3,000 succeeded, **0 duplicates**; 130 jobs/s, as the laptop's third of the slots was down most of the run |
| [Leader failover across machines](../bench/out/homelab-04-leader-failover.json): 600 × 1 s jobs, SIGKILL `scheduler-1` on the iMac with 360 queued | `scheduler-2` on the laptop elected **4.5 s** after the kill, first assignment 4.5 s; 600/600, 0 retried, **0 duplicates**, no worker declared dead |

The homelab throughput is higher than the single-laptop 1,269 jobs/s in
[RESULTS.md](RESULTS.md): Postgres and the leader run on the iMac's faster cores, and the
laptop no longer hosts every container.

## Real work: `llm` and `benchmark` jobs

Two job types do real work on the GPU PC instead of simulating it. Only workers configured for
them advertise them, so placement sends them to `gpu-pc` and nowhere else.

| Type | Payload | What the worker does |
|---|---|---|
| `llm` | `{"prompts": [...], "max_tokens": 64}` | Chat completions against the live vLLM (`vllm-chat`, joined over its compose network), 8 in flight per job, greedy and seeded. The API key comes from the serving stack's `.env` and is never logged or returned. HTTP 4xx fails the job; 5xx, 429 and network errors retry |
| `benchmark` | `{"suite": "vllm-sweep", "label": "awq", "model": "Qwen/...", "quantization": "awq_marlin"}` or `{"suite": "matmul"}` | Runs a [gpu-systems-lab](https://github.com/zakiscoding/gpu-systems-lab) suite in the vLLM image on the assigned GPU. The payload names a suite and checked parameters, never a command |

**What "exactly once" means for real work.** A killed worker's `llm` requests *are* sent again,
because the work isn't idempotent at the server, and in the CI test the fake server saw 272
requests for 240 prompts. What GopherGrid guarantees is that each job records **one** result:
the lease-fenced attempt that won, with `outputs_digest` naming its text. It does not
guarantee that a retry writes the same text. With temperature 0 and a fixed seed, only 1,240
of 2,400 prompts (52%) gave identical completions across two runs on the live vLLM, because
batched kernels' numerics depend on what else is in the batch.

**Benchmarks and the GPU.** Each attempt writes to its own directory
(`BENCH_OUT_DIR/<job>/a<attempt>`), so a retry never mixes files with an earlier attempt, and
the recorded result names the directory of the winning attempt. Before starting, the worker
removes any container still labelled with the job, so an earlier attempt that lost its lease
can't keep holding the card. If the lease is lost mid-run, the worker removes the container
itself (killing the `docker` CLI alone would leave it running). A `vllm-sweep` needs the whole
card, so it only fits once `vllm-chat` is stopped and the worker restarts with
`GPU_RESERVED_MB=0`; until then placement leaves it queued.

Drivers: [`bench/llm_homelab.py`](../bench/llm_homelab.py) (the kill test above); benchmark
jobs are plain `POST /v1/jobs`.

## Things to know

- **Leases and timeouts use the Postgres clock**, so clock skew between machines doesn't
  matter for correctness.
- **The laptop sleeps.** If `scheduler-2` leads when it does, the iMac's scheduler takes over in
  ~5 s; its worker is declared dead after 3 s and its jobs move to the GPU PC.
- **A worker declared dead can still be running.** During the AWQ sweep, `gpu-pc` missed
  heartbeats twice (cause not found; its logs went with a container recreate), so the leader
  requeued the benchmark and handed attempt 2 straight back to the same, re-registered worker.
  The old attempt still held the GPU, so the worker declined it, and the decline counted as a
  failed attempt. Now a worker reports a decline as `declined`, which requeues the job without
  using up an attempt (up to 3 times; `TestDeclineDoesNotUseAnAttempt`). The lost run still
  costs the GPU time it had used; reattaching a "dead" worker's live attempt would avoid that.
- **No retry backoff.** A failed attempt is retried at once: a job whose `llm` URL didn't
  resolve used all 5 attempts in 0.3 s. Backoff would be the next fix.
- **The iMac is a single point of failure** for Postgres and etcd, as noted in DESIGN.md.
- **Monitoring:** the homelab's central Prometheus on the iMac scrapes both schedulers and both
  workers (worker metrics are shared on the tailnet as laptop `:9101` and GPU PC `:9102`), and
  [the dashboard](../deploy/grafana/dashboards/gophergrid.json) is public and read-only at
  https://grafana-proxy.vllm-gateway.workers.dev.
