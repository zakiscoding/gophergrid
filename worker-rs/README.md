# worker-rs

A GopherGrid worker in Rust. It speaks the same gRPC protocol as the Go worker
([`proto/gophergrid/v1/worker.proto`](../proto/gophergrid/v1/worker.proto)), reads the same
environment variables, and runs the same job types. The two can be swapped service by service,
or mixed in one cluster.

The scheduler's guarantees don't depend on the worker's language: the attempt number fences
stale reports, and leases expire when heartbeats stop. So a Rust worker gets exactly-once
recording for free, as long as it follows the protocol. The integration tests check this by
running unchanged against Rust workers.

## Layout

| File | What |
|---|---|
| [`src/agent.rs`](src/agent.rs) | Heartbeat loop, capacity checks, task lifecycle, reporting with retries, drain on SIGTERM |
| [`src/transport.rs`](src/transport.rs) | tonic client: follows the leader redirect trailer, fails over between schedulers, re-registers on `NOT_FOUND` |
| [`src/runners.rs`](src/runners.rs) | `sleep`, `image`, `hash`, `inference` (simulated) and `container` (`docker run`, with `--gpus device=…`) |
| [`src/gpu.rs`](src/gpu.rs) | GPU discovery: `none`, `sim`, or `nvidia` from `nvidia-smi` |
| [`tests/agent.rs`](tests/agent.rs) | The real agent against a fake two-scheduler cluster |

It follows the Go agent ([`internal/worker/agent.go`](../internal/worker/agent.go)) closely, with
Rust's tools in place of Go's: a `CancellationToken` per attempt instead of a context, a
`TaskTracker` instead of a `WaitGroup`, `Notify` instead of a one-slot channel. CPU-bound jobs
run on Tokio's blocking pool and poll their token, so a cancel or timeout stops them within one
row of pixels.

## The same checksum in both languages

A retry can land on a Go or a Rust worker, so an image job must give the same checksum on
both. The Go worker used to fill its pixels from `math/rand`, whose seeding goes through a
607-entry internal table. Both now use SplitMix64, which is a few lines in any language. The
test vectors are pinned in both test suites, and were computed a third time by an independent
Python implementation:

| Input | Expected |
|---|---|
| SplitMix64, seed 1, first 10 bytes | `c15c0289ec2d0a9167ec` |
| Image job 64×48, blur radius 2, seed 3 | checksum `1682d6bdc6025b5b` |

## Go vs. Rust

Measured 2026-11-13 on the laptop, **on battery** (the CPU held at 1.2 GHz) and with another
project's containers running, so only the Go/Rust ratios mean anything, not the absolute
numbers. Each comparison ran under the same conditions, alternating where it was repeated.

| | Go | Rust | |
|---|---|---|---|
| Image job, 512×512 blur radius 3, median of 5 ([raw](../bench/out/go-vs-rust-microbench.txt)) | 339 ms | 161 ms | **2.1× faster** |
| Hash job, 1M SHA-256 rounds, median of 5 | 381 ms | 218 ms | 1.75× faster |
| 300 image jobs (640×640 r4, 1 core each) on a 5-worker fleet | 4.4 and 3.2 jobs/s | 8.4 jobs/s | ~2× the throughput |
| 5,000 zero-length jobs on a 5-worker fleet | 212 and 110 jobs/s | 206 jobs/s | the same: the scheduler is the limit |
| Worker memory, idle | 7.0–9.2 MiB | 1.5 MiB | ~5× less |
| Worker memory, peak during the image run | 17.5–20.4 MiB (CPU), 26–27 MiB (GPU sim) | 7.8–8.2 MiB, 14 MiB | ~2× less |
| Container start to registered | 1.3–2.9 s | 1.2–1.7 s | similar |
| Binary / image | 13.8 MB / 211 MB | 3.8 MB / 197 MB | the image is mostly Debian and the Docker CLI |

Both run the same algorithm. Go's version reads pixels through `image.RGBA.RGBAAt`, which
computes an offset and bounds-checks each call; the Rust loops index a byte slice. The fleet
runs are in [`bench/out/fleet-*`](../bench/out).

## Tested

| Test | Result |
|---|---|
| `cargo test`: 13 unit tests and the fake-cluster test | Pass. The fake cluster checks the follower redirect, re-registering after `NOT_FOUND`, the image checksum, rejecting over-capacity work, a timeout, and that a cancelled attempt never reports. `clippy -D warnings` and `rustfmt --check` are clean |
| All 19 store and integration tests, every worker in Rust (`compose.rust.yml`) | Pass: worker SIGKILL, leader failover (new leader in 3.2 s), frozen worker (0 stale results), GPU worker killed mid-batch, priorities, retries |
| The same tests on a mixed fleet (`compose.mixed.yml`) | Pass. The killed Rust `worker-2`'s image jobs re-ran on Go workers, and all 100 results had one checksum; the killed Rust `gpu-worker-1`'s shards finished on a Go GPU worker |
| Homelab: Rust worker on the GPU PC with `GPU_MODE=nvidia` | Found the RTX 5070 through `nvidia-smi`, ran a PyTorch `container` job on it with `--gpus device=<uuid>` (63.5 TFLOPS fp16); 24 identical image jobs split between the laptop's Go worker and the GPU PC's Rust worker gave one checksum |

**Upgrade Go workers together with this change.** The image job's checksum changed when the
pixel source moved to SplitMix64, so an old Go worker and a new one disagree. This happened on
the homelab: the laptop's worker container was still on the old image and produced a second
checksum until it was recreated.

**Not explained:** in one all-Rust run, the worker-crash test reported reassignment 200 ms after
the kill. The heartbeat timeout is 3 s, so that should take ~2 s. Repeating the kill by hand
gave 1.5 s, and the mixed-fleet run gave 2.5 s; heartbeat gaps under load stayed under 0.6 s
for both languages, so a live worker wasn't being declared dead.

## Build and test

Only Docker is needed. Cargo runs in a container ([`scripts/cargo.sh`](../scripts/cargo.sh)),
and `protoc` is vendored by the build script.

```bash
scripts/cargo.sh test                                  # unit tests + the fake-cluster test
scripts/build.sh worker-rs                             # image gophergrid/worker-rs:dev
docker compose -f docker-compose.yml -f compose.rust.yml up -d     # every worker in Rust
docker compose -f docker-compose.yml -f compose.mixed.yml up -d    # worker-2 and gpu-worker-1 in Rust
docker compose -f docker-compose.yml -f compose.rust.yml --profile test run --rm tester
```
