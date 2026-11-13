//go:build integration

package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var allWorkers = []string{"scheduler-1", "scheduler-2", "worker-1", "worker-2", "worker-3", "gpu-worker-1", "gpu-worker-2"}

type batch struct {
	ID       string         `json:"id"`
	Shards   int            `json:"shards"`
	ByStatus map[string]int `json:"by_status"`
	Done     bool           `json:"done"`
	Tokens   int64          `json:"generated_tokens"`
	Workers  []string       `json:"workers"`
	GPUs     []string       `json:"gpus"`
	Retried  int            `json:"retried_shards"`
	WallMS   int64          `json:"wall_ms"`
}

func submitInference(t *testing.T, req map[string]any) string {
	t.Helper()
	var resp struct {
		BatchID string `json:"batch_id"`
		Shards  int    `json:"shards"`
	}
	if code := api(t, "POST", "/v1/inference/batches", req, &resp); code != 201 {
		t.Fatalf("submit inference batch: HTTP %d", code)
	}
	return resp.BatchID
}

func waitBatch(t *testing.T, id string, timeout time.Duration) batch {
	t.Helper()
	var b batch
	waitFor(t, timeout, "batch "+id, func() bool {
		api(t, "GET", "/v1/batches/"+id, nil, &b)
		return b.Done
	})
	return b
}

// doubleBooked counts pairs of attempts that held the same GPU at overlapping times.
func doubleBooked(t *testing.T, pool *pgxpool.Pool) int {
	var n int
	must(t, pool.QueryRow(context.Background(), `
		SELECT count(*) FROM attempts a JOIN attempts b
		  ON a.worker_id = b.worker_id AND (a.job_id, a.attempt) < (b.job_id, b.attempt)
		 AND a.gpu_ids && b.gpu_ids
		 AND a.started_at < coalesce(b.ended_at, now()) AND b.started_at < coalesce(a.ended_at, now())`).Scan(&n))
	return n
}

// TestInferenceBatchFansOutAcrossGPUs splits 2048 prompts into 32 shards that each need one
// GPU with 10 GB. All three simulated GPUs qualify; none may ever run two shards at once.
func TestInferenceBatchFansOutAcrossGPUs(t *testing.T) {
	pool := db(t)
	reset(t, pool)
	ensureRunning(t, allWorkers...)
	waitWorkersAlive(t, 5)

	id := submitInference(t, map[string]any{"prompts": 2048, "shard_size": 64, "tokens": 128,
		"ms_per_token": 0.05, "gpu_memory_mb": 10_000})
	b := waitBatch(t, id, 2*time.Minute)
	if b.ByStatus["succeeded"] != 32 || b.Tokens != 2048*128 {
		t.Fatalf("want 32 shards and %d tokens, got %+v", 2048*128, b)
	}
	for _, w := range b.Workers {
		if !strings.HasPrefix(w, "gpu-worker-") {
			t.Fatalf("a shard ran on CPU-only worker %s", w)
		}
	}
	if len(b.GPUs) != 3 {
		t.Errorf("want shards spread over all 3 GPUs, used %v", b.GPUs)
	}
	if n := doubleBooked(t, pool); n != 0 {
		t.Fatalf("%d pairs of attempts shared a GPU at the same time", n)
	}
	t.Logf("32 shards on %v in %d ms, %d tokens", b.GPUs, b.WallMS, b.Tokens)
}

// TestGPUMemoryPlacement sends shards that need 40 GB: only the simulated H100 has that.
func TestGPUMemoryPlacement(t *testing.T) {
	pool := db(t)
	reset(t, pool)
	ensureRunning(t, allWorkers...)
	waitWorkersAlive(t, 5)
	id := submitInference(t, map[string]any{"prompts": 256, "shard_size": 64, "tokens": 64,
		"ms_per_token": 0.05, "gpu_memory_mb": 40_000})
	b := waitBatch(t, id, time.Minute)
	if b.ByStatus["succeeded"] != 4 || len(b.Workers) != 1 || b.Workers[0] != "gpu-worker-2" {
		t.Fatalf("40 GB shards must all run on gpu-worker-2: %+v", b)
	}
}

// TestGPUWorkerCrashDuringBatch kills the two-GPU worker mid-batch. Its shards must move
// to the remaining GPU worker, and the batch must finish with no duplicate results.
func TestGPUWorkerCrashDuringBatch(t *testing.T) {
	pool := db(t)
	reset(t, pool)
	ensureRunning(t, allWorkers...)
	waitWorkersAlive(t, 5)
	id := submitInference(t, map[string]any{"prompts": 1280, "shard_size": 64, "tokens": 256,
		"ms_per_token": 0.05, "gpu_memory_mb": 8_000}) // 20 shards of ~820 ms each
	var held []string
	waitFor(t, 30*time.Second, "gpu-worker-1 to hold shards", func() bool {
		held = heldBy(t, pool, "gpu-worker-1")
		return len(held) == 2
	})
	kill(t, "gpu-worker-1")
	b := waitBatch(t, id, 2*time.Minute)
	if b.ByStatus["succeeded"] != 20 || b.Tokens != 1280*256 {
		t.Fatalf("batch incomplete after GPU worker crash: %+v", b)
	}
	assertExactlyOnce(t, pool, 20)
	if b.Retried < len(held) {
		t.Errorf("want >= %d retried shards, got %d", len(held), b.Retried)
	}
	if n := doubleBooked(t, pool); n != 0 {
		t.Fatalf("%d GPU double bookings", n)
	}
	t.Logf("gpu-worker-1 killed holding %d shards; batch finished in %d ms on %v (%d shards retried)",
		len(held), b.WallMS, b.GPUs, b.Retried)
}

// TestPriorityJumpsTheQueue fills every slot with low-priority work, then submits
// high-priority jobs. They must start before low-priority jobs submitted earlier.
func TestPriorityJumpsTheQueue(t *testing.T) {
	pool := db(t)
	reset(t, pool)
	ensureRunning(t, allWorkers...)
	waitWorkersAlive(t, 5)
	ctx := context.Background()
	low := make([]map[string]any, 300)
	for i := range low {
		low[i] = map[string]any{"type": "sleep", "priority": -10, "payload": map[string]any{"ms": 300},
			"resources": map[string]any{"cpu_millis": 1000, "memory_mb": 32}}
	}
	submitBatch(t, low)
	time.Sleep(time.Second)
	high := make([]map[string]any, 10)
	for i := range high {
		high[i] = map[string]any{"type": "sleep", "priority": 90, "payload": map[string]any{"ms": 300},
			"resources": map[string]any{"cpu_millis": 1000, "memory_mb": 32}}
	}
	submitBatch(t, high)
	waitAllDone(t, pool, 3*time.Minute)

	var highP95, lowP50 float64
	var lowAfterHigh int
	must(t, pool.QueryRow(ctx, `SELECT
		(SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM assigned_at - created_at)) FROM jobs WHERE priority = 90),
		(SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM assigned_at - created_at)) FROM jobs WHERE priority = -10),
		(SELECT count(*) FROM jobs WHERE priority = -10 AND assigned_at > (SELECT max(assigned_at) FROM jobs WHERE priority = 90))`).
		Scan(&highP95, &lowP50, &lowAfterHigh))
	t.Logf("queue wait: high-priority p95 %.2fs, low-priority p50 %.2fs; %d earlier low-priority jobs started after the last high-priority one",
		highP95, lowP50, lowAfterHigh)
	if highP95 > 2 || lowAfterHigh < 100 {
		t.Fatalf("high-priority jobs didn't jump the queue")
	}
}

// TestUnschedulableJobWaits submits a job needing 3 GPUs; no worker has 3, so it must stay
// queued rather than be placed on a worker that can't run it.
func TestUnschedulableJobWaits(t *testing.T) {
	pool := db(t)
	reset(t, pool)
	ensureRunning(t, allWorkers...)
	var j job
	api(t, "POST", "/v1/jobs", map[string]any{"type": "inference", "resources": map[string]any{"gpus": 3}}, &j)
	time.Sleep(3 * time.Second)
	var got job
	api(t, "GET", "/v1/jobs/"+j.ID, nil, &got)
	if got.Status != "queued" || got.Attempt != 0 {
		t.Fatalf("a 3-GPU job was placed: %+v", got)
	}
}
