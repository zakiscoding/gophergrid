# Design

Why GopherGrid is built the way it is. The guarantees first, then the decisions behind them.

## Guarantees

1. **A job records at most one result.** `results.job_id` is the primary key.
2. **Only the current attempt can record it.** `Complete` locks the job row and accepts a
   report only if the job is `running` that exact `(attempt, worker)`. Every assignment bumps
   `attempt`, so it works as a fencing token: a worker that lost its lease (crash, partition,
   GC pause) can finish its work, but its report is rejected.
3. **A job isn't lost while attempts remain.** A running job always has a lease. Leases expire
   when heartbeats stop, when the worker is declared dead, or when the job runs past its
   timeout plus a grace period. The reaper puts expired jobs back on the queue.
4. **Only one leader writes.** Leader-only writes check the `leadership` epoch in the same
   transaction.

Execution is *at least once*; recording is *exactly once*. A job can run twice (its worker
froze, and the job was reassigned and run again), so job side effects should be idempotent or
keyed by job id. Results can't be recorded twice.

## Decisions

### Postgres is the source of truth, not the leader's memory

Every queue, lease and assignment change is a Postgres transaction. The leader keeps no state
that a new leader needs. Delivery is driven by the database too: a heartbeat answers "what is
assigned to you that you don't hold yet?" from `jobs`. So a new leader delivers assignments
the old one committed and never sent.

- Alternatives: an in-memory queue replicated over Raft, or etcd as the queue. Faster, but
  far more code, and etcd isn't built for millions of job rows.
- Cost: every assignment and completion is a write. Measured ceiling on this laptop: ~1,270
  jobs/s for zero-length jobs, enough for jobs that run for seconds. Faster work needs batching
  completions into heartbeats (not done).

### Workers pull over one heartbeat RPC

The worker calls `Heartbeat(running attempts, free capacity)` and gets back
`(new assignments, attempts to cancel)`. One unary RPC carries liveness, lease renewal,
delivery and reconciliation.

- Workers only make outbound connections, so a worker on the homelab GPU node behind NAT, on
  Tailscale or in a pod needs nothing opened toward it.
- A unary call is simple to retry and redirect. A bidirectional stream would deliver work
  faster, but it fails over in more complicated ways.
- The latency cost of polling is solved with **long polling** plus **eager re-sync**: an idle
  worker's heartbeat waits at the leader (up to 400 ms) and returns as soon as it is assigned
  something, and a worker heartbeats again right after it receives work or finishes a task.
  Long polling on its own made things *worse* (see RESULTS: workers were idle between ticks,
  not waiting at the server). The two together took queue latency p50 from 5.6 s to 27 ms.

### Leases renewed by heartbeat, plus a hard timeout

A lease is 10 s and every heartbeat (500 ms) renews it, so a live worker never loses one. A
worker silent for 3 s is declared dead and its leases are expired immediately: detection
takes ~2–3 s rather than the lease length. Separately, an attempt that runs past
`timeout + grace` is taken back even if heartbeats keep renewing it, which catches a stuck
worker that's still alive.

After a leader change, the new leader waits one heartbeat timeout before declaring anything
dead or reaping leases, because workers are still finding it. The failover test checks that
no worker is wrongly declared dead.

### Leader election in etcd, fencing in Postgres

Schedulers campaign with `concurrency.Election` (session TTL 5 s). Etcd alone can't stop a
deposed leader that hasn't noticed yet (a long GC pause, a partition) from writing to Postgres.
So the winner writes its election revision, which only grows, into a one-row `leadership`
table. Every leader transaction first reads it `FOR SHARE` and aborts if it changed. Taking
over updates the row, which waits for the old leader's in-flight transactions to finish;
after that, every write the old leader starts fails. `TestDeposedLeaderIsFencedOut` covers this.

Followers serve the REST API and accept completions (fenced by attempt, so safe anywhere), but
answer heartbeats with `FAILED_PRECONDITION` and the leader's address in a trailer. Workers
follow the redirect.

### Placement: greedy, with three rules

For each queued job, in queue order:

- **Fit** CPU, memory, GPU count and per-GPU memory, and a worker that runs the job type.
- **GPU jobs: best fit.** Prefer the node that has the fewest free GPUs left afterwards, and
  the smallest GPUs that are big enough. A 10 GB shard takes a 12 GB GPU rather than an 80 GB
  one, keeping the large GPU for jobs that need it (`TestGPUBestFitAndMemory`).
- **CPU jobs: least loaded** (max of CPU and memory reservation), which spreads load. With
  `KEEP_GPU_NODES_FREE`, GPU workers only take CPU jobs when no CPU-only worker has room.
- **Backfill:** a job that fits nowhere is skipped, and smaller jobs behind it still run. A job
  no worker could run even when idle is counted in `gophergrid_unschedulable_jobs`.

Greedy per batch is O(jobs × workers) and has no preemption. 256 jobs × 100 workers takes
1.1 ms with 538 allocations.

### Priority with aging, without sorting the queue

Order is `priority + wait / PRIORITY_AGING`: a job gains a point per minute waited, so a
stream of high-priority work can't starve the rest. That expression can't use an index, and
sorting the whole queue each pass cost 25 ms at 20k queued and 140 ms at 100k. But within one
priority level the oldest job scores highest, so the global top N is among the N oldest jobs
of each priority level present. The query finds the levels with a recursive skip scan, takes
N per level from the `(priority DESC, created_at)` index and sorts only those: ~10 ms, flat in
queue depth. `TestAgingQueueMatchesNaiveOrder` checks it returns the same order as the full sort.

### Distributed inference as a batch of shards

`POST /v1/inference/batches` splits prompts into shards. It writes the batch row and all shard
jobs in one transaction, with `COPY`. Shards are ordinary jobs, so retries, fencing,
GPU placement and priority all apply unchanged. `GET /v1/batches/{id}` aggregates progress,
tokens and the GPUs used. Each attempt records its GPU ids, so a test can prove from the
`attempts` table that no GPU ran two shards at once.

### Autoscaling on queue depth

Both autoscalers use the same signal: queued jobs per worker (100). On Compose,
`cmd/autoscaler` creates worker containers with the Docker API, cloning a template worker's
environment. On Kubernetes, KEDA scales the worker Deployment on the leader's
`gophergrid_queue_depth`. It scales up at once and down only after a cooldown, and removal is
a SIGTERM: the worker stops taking work, finishes what it holds and reports before exiting.

## What would change at larger scale

- **Completions batched into heartbeats.** Saves a transaction per job; the next bottleneck
  after the queue query.
- **Partitioned queues** (by priority band or tenant), so the scheduling pass doesn't
  serialize on one table.
- **Preemption** for high-priority GPU jobs, with checkpoint hooks, instead of only queue jumping.
- **A replicated Postgres** (or a managed one) and 3 etcd members, so no single node can take
  the control plane down.
