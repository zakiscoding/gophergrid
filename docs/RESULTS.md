# Results

All measured on 2026-11-13 on one laptop: Intel i3-1215U (6 cores, 8 threads), Docker Desktop
4.91 on WSL2. The stack is 2 schedulers, Postgres 17, etcd 3.6.6, 3 CPU workers (2 cores each)
and 2 simulated GPU workers (4 cores each), all in Docker on that one machine. The numbers
measure GopherGrid's own overhead, not a real cluster's compute.

Throughput and latency come from [`ggbench`](../cmd/ggbench/main.go), which reads Postgres
timestamps:
- **Queue latency** is `assigned_at − created_at`.
- **End-to-end latency** is `finished_at − created_at`.
- **Throughput** is jobs ÷ (last finish − first submit).

Every run checks that the result count equals the job count and that no job has two results.
Raw JSON is in [`bench/out/`](../bench/out).

## Fault tolerance (integration tests)

`docker compose --profile test run --rm tester`. Each test resets the queue, does the damage
through the Docker API, then checks Postgres.

| Test | What happens | Result |
|---|---|---|
| `TestWorkerCrashReassignsWithoutDuplicates` | 100 image jobs (640×640, blur 4, same seed); SIGKILL `worker-2` while it holds jobs | Jobs reassigned 2.0–2.3 s after the kill; all 100 succeeded, 100 results, 1 distinct checksum |
| `TestLeaderFailover` | 600 one-second jobs; SIGKILL the leader with ~490 still queued | New leader 3.9–4.9 s after the kill (read from the `leadership` row), first new assignment 4–5 s; 600/600, 0 duplicates, 0 workers declared dead |
| `TestFrozenWorkerCannotRecordStaleResults` | `docker pause` `worker-3` holding 4 jobs until declared dead and its jobs finished elsewhere, then unpause | 0 results from its stale attempts; on thaw, heartbeats cancelled them before they could report |
| `TestGPUWorkerCrashDuringBatch` | 20-shard inference batch; SIGKILL the 2-GPU worker holding 2 shards | 20/20 shards, 2 retried on the 80 GB GPU, 0 duplicates, 0 GPU double-bookings |
| `TestRetriesThenSucceeds` | A job that fails its first 2 attempts | Succeeds on attempt 3 with one result |
| `TestIdempotentSubmit` | The same `Idempotency-Key` twice | Same job id, one row |

Store tests against Postgres (`internal/store`): stale attempt rejected after reassignment; 20
racing completions → exactly 1 accepted; deposed leader fenced out of assign, reap and
dead-marking; heartbeat renews only current attempts; aging query matches the full sort.

### Chaos under load

3,000 jobs × 200 ms, 250 millicores each (56 slots, so at most 280 jobs/s).

| Run | Kills | Throughput | Retried | Duplicates |
|---|---|---|---|---|
| [Control](../bench/out/09-no-chaos-control.json) | none | 220 jobs/s | 0 | 0 |
| [Workers down 6 s](../bench/out/08-chaos-slow-restart.json) (past the 3 s heartbeat timeout) | 2 | 205 jobs/s (−7%) | 8 | 0 |
| [Workers back in 2 s](../bench/out/07-chaos-fast-restart.json) (inside the timeout) | 3 | 198 jobs/s | 0 | 0 |

When a worker restarts inside the heartbeat timeout under the same `WORKER_ID`, it is never
declared dead. Its first heartbeat holds nothing, so the leader redelivers its current
attempts and it re-runs them under the same attempt number. That's correct: the old process is
dead, and if it weren't, the fence would still allow only one report.

## Throughput and latency

5,000 (then 20,000) zero-length `sleep` jobs at 100 millicores: 140 slots across 5 workers.

| Run | Change | Jobs | Throughput | Submit rate | Queue p50 / p95 / p99 |
|---|---|---|---|---|---|
| [01](../bench/out/01-baseline.json) | baseline: 500 ms heartbeat | 5k | 276/s | 721/s | 5,611 / 10,294 / 10,548 ms |
| [02](../bench/out/02-long-poll.json) | + long-poll 400 ms | 5k | 199/s | 762/s | 9,283 / 17,346 / 17,891 ms |
| [03](../bench/out/03-eager-no-long-poll.json) | eager sync, no long-poll | 5k | 343/s | — | 3,405 / 7,142 / 7,483 ms |
| [04](../bench/out/04-eager-long-poll.json) | eager sync + long-poll | 5k | **660/s** | 720/s | **27 / 52 / 64 ms** |
| [05](../bench/out/05-batch-insert.json) | + one-statement batch insert | 20k | 768/s | **28,083/s** | (20k backlog) |
| [06](../bench/out/06-skip-scan-queue.json) | + skip-scan aging query | 20k | **1,269/s** | 23,103/s | (20k backlog) |

How each step was found:

- **01:** 140 slots, refilled once per 500 ms heartbeat, gives a ceiling of about 280 jobs/s.
  Measured: 276.
- **02:** long polling alone lowered throughput. `gophergrid_assignment_delivery_seconds`
  showed 4,820 of 5,000 assignments waited 256–512 ms. A worker that got work ran it in
  microseconds and then slept until its next tick, so it was never waiting at the server when
  the next assignment came.
- **03 and 04:** the worker re-polls right after receiving work and after each completion.
  Without long polling that only helps a little (343/s): the slot frees before the leader has
  assigned it. With long polling the heartbeat is already waiting when the assignment commits.
  At 04 throughput equals the submit rate, so submission became the limit.
- **05:** the batch endpoint did one `INSERT` per job. One `INSERT … SELECT FROM unnest(…)`
  per batch submits 39× faster. With the backlog now in the queue, 236 scheduling passes
  averaged 82 ms and filled 19 of the 26 s run.
- **06:** `EXPLAIN ANALYZE` on the queue query:

  | Queued jobs | Full sort on aging score | Skip scan + per-level index |
  |---|---|---|
  | 20,000 | 24.8 ms (seq scan + sort) | 10.9 ms |
  | 100,000 | 140.4 ms | 9.8 ms |

  Average pass time fell to 48 ms and throughput rose 65%.

Long polling and eager sync are the defaults now (`HEARTBEAT_LONG_POLL=400ms`, `EAGER_SYNC=true`).
With them on, high-priority jobs behind 300 low-priority ones waited p95 0.07 s (was 0.32 s).

### Placement microbenchmark

`BenchmarkPlace`: 256 queued jobs (1 in 8 needs a GPU) × 100 workers with one GPU each.

| | ns/op | B/op | allocs/op |
|---|---|---|---|
| Before | 1,450,000 | 515,993 | 13,962 |
| After (allocation-free fit check) | 1,100,000 | 86,165 | 538 |

### REST load test (k6)

[`bench/k6/submit.js`](../bench/k6/submit.js): single-job `POST /v1/jobs` at a constant
500 req/s for 30 s, while the cluster runs the 50 ms jobs.
[Result](../bench/out/k6-submit.json): 14,978 requests, 497 req/s, p50 8.7 ms, p95 43 ms,
max 163 ms, 0 failures. All 14,978 jobs succeeded with 0 duplicate results.

## GPU placement and priorities

| Test | Result |
|---|---|
| `TestInferenceBatchFansOutAcrossGPUs`: 2,048 prompts → 32 shards needing 10 GB | All 3 GPUs used, 262,144 tokens, ~5 s, 0 overlapping attempts on any GPU |
| `TestGPUMemoryPlacement`: shards needing 40 GB | All on the 80 GB GPU |
| `TestPriorityJumpsTheQueue`: 300 low (−10) then 10 high (90) | High p95 wait 0.07 s; low p50 3.89 s; 254 earlier low jobs started after the last high one |
| `TestUnschedulableJobWaits`: a 3-GPU job | Stays queued, not placed |

## Autoscaling

`TestAutoscalerAddsAndRemovesWorkers` (Compose, `MAX_WORKERS=4`, cooldown 10 s): 1,500 jobs
of 400 ms at one core each. The pool grew within 1.8 s, peaked at 4 extra workers, drained the
queue in 32 s, and was back to 0 extra workers 9 s later, each worker draining before exit.

## Not tested

- Real GPUs: `GPU_MODE=nvidia` and the `container` runner's `--gpus` path haven't run on
  the RTX 5070 yet (see [homelab.md](homelab.md)).
- The Kubernetes manifests on a live cluster (they pass `kubeconform -strict`).
- Losing Postgres or etcd. Postgres is a single point of failure, and losing etcd stops
  elections but not the current leader's work until its session expires.
- Multi-machine latency: workers and schedulers shared one host, so every network hop was local.
