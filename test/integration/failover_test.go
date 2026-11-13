//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type job struct {
	ID       string  `json:"id"`
	Status   string  `json:"status"`
	Attempt  int     `json:"attempt"`
	WorkerID *string `json:"worker_id"`
}

func submitBatch(t *testing.T, specs []map[string]any) []job {
	t.Helper()
	var resp struct{ Jobs []job }
	for i := 0; i < len(specs); i += 1000 {
		part := specs[i:min(i+1000, len(specs))]
		var r struct{ Jobs []job }
		if code := api(t, "POST", "/v1/jobs/batch", map[string]any{"jobs": part}, &r); code != 201 {
			t.Fatalf("batch submit: HTTP %d", code)
		}
		resp.Jobs = append(resp.Jobs, r.Jobs...)
	}
	return resp.Jobs
}

func heldBy(t *testing.T, pool *pgxpool.Pool, worker string) []string {
	rows, err := pool.Query(context.Background(), `SELECT id FROM jobs WHERE status = 'running' AND worker_id = $1`, worker)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		_ = rows.Scan(&id)
		ids = append(ids, id)
	}
	return ids
}

func waitAllDone(t *testing.T, pool *pgxpool.Pool, timeout time.Duration) {
	t.Helper()
	waitFor(t, timeout, "all jobs to finish", func() bool {
		var open int
		_ = pool.QueryRow(context.Background(), `SELECT count(*) FROM jobs WHERE status IN ('queued', 'running')`).Scan(&open)
		return open == 0
	})
}

// assertExactlyOnce checks that all n jobs succeeded with exactly one result each.
func assertExactlyOnce(t *testing.T, pool *pgxpool.Pool, n int) {
	t.Helper()
	var succeeded, results, distinct int
	must(t, pool.QueryRow(context.Background(), `SELECT
		(SELECT count(*) FROM jobs WHERE status = 'succeeded'),
		(SELECT count(*) FROM results),
		(SELECT count(DISTINCT job_id) FROM results)`).Scan(&succeeded, &results, &distinct))
	if succeeded != n || results != n || distinct != n {
		t.Fatalf("want %d succeeded jobs with one result each; got succeeded=%d results=%d distinct=%d",
			n, succeeded, results, distinct)
	}
}

// TestWorkerCrashReassignsWithoutDuplicates is the spec's example: submit 100
// image-processing jobs, kill worker-2 while it holds some, and check every job finishes
// exactly once. Detection is by missed heartbeats, not lease timeout: the jobs keep the
// default 60 s timeout, so a fast recovery can only come from heartbeat detection.
func TestWorkerCrashReassignsWithoutDuplicates(t *testing.T) {
	pool := db(t)
	reset(t, pool)
	ensureRunning(t, "scheduler-1", "scheduler-2", "worker-1", "worker-2", "worker-3")
	waitWorkersAlive(t, 3)
	ctx := context.Background()

	const n = 100
	specs := make([]map[string]any, n)
	for i := range specs {
		specs[i] = map[string]any{
			"type":      "image",
			"payload":   map[string]any{"width": 640, "height": 640, "blur_radius": 4, "seed": 7},
			"resources": map[string]any{"cpu_millis": 1000, "memory_mb": 128},
		}
	}
	submitBatch(t, specs)
	start := time.Now()

	var held []string
	waitFor(t, 30*time.Second, "worker-2 to hold a job", func() bool {
		held = heldBy(t, pool, "worker-2")
		return len(held) > 0
	})
	kill(t, "worker-2")
	killedAt := time.Now()
	t.Logf("worker-2 held %d running jobs when killed", len(held))

	// Time until every job worker-2 held is back on the queue or running elsewhere.
	waitFor(t, 30*time.Second, "worker-2's jobs to be reassigned", func() bool {
		var stuck int
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE id = ANY($1) AND worker_id = 'worker-2'`, held).Scan(&stuck)
		return stuck == 0
	})
	t.Logf("detected and reassigned %s after the kill (heartbeat timeout 3s)", time.Since(killedAt).Round(100*time.Millisecond))

	waitAllDone(t, pool, 3*time.Minute)
	t.Logf("all %d jobs finished in %s", n, time.Since(start).Round(time.Millisecond))
	assertExactlyOnce(t, pool, n)

	var checksums int
	must(t, pool.QueryRow(ctx, `SELECT count(DISTINCT output->>'checksum') FROM results`).Scan(&checksums))
	if checksums != 1 {
		t.Fatalf("identical image jobs produced %d different checksums", checksums)
	}
	for _, id := range held {
		var attempt int
		var resultWorker string
		must(t, pool.QueryRow(ctx, `SELECT j.attempt, r.worker_id FROM jobs j JOIN results r ON r.job_id = j.id
			WHERE j.id = $1`, id).Scan(&attempt, &resultWorker))
		if attempt < 2 || resultWorker == "worker-2" {
			t.Errorf("job %s held by dead worker-2: attempt=%d result from %s", id, attempt, resultWorker)
		}
	}
}

// TestLeaderFailover kills the leader scheduler mid-run. The other instance must take over
// (etcd session TTL 5 s), workers must follow it, and every job must finish exactly once.
func TestLeaderFailover(t *testing.T) {
	pool := db(t)
	reset(t, pool)
	ensureRunning(t, "scheduler-1", "scheduler-2", "worker-1", "worker-2", "worker-3")
	waitWorkersAlive(t, 3)
	ctx := context.Background()

	// Enough work that plenty is still queued when the leader dies, whatever the worker count.
	const n = 600
	specs := make([]map[string]any, n)
	for i := range specs {
		specs[i] = map[string]any{"type": "sleep", "payload": map[string]any{"ms": 1000},
			"resources": map[string]any{"cpu_millis": 250, "memory_mb": 32}}
	}
	submitBatch(t, specs)
	waitFor(t, 30*time.Second, "some progress", func() bool {
		var done int
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE status = 'succeeded'`).Scan(&done)
		return done >= 20
	})

	var deadBefore int
	must(t, pool.QueryRow(ctx, `SELECT count(*) FROM workers WHERE state = 'dead'`).Scan(&deadBefore))
	old := leaderService(t)
	kill(t, old)
	killedAt := time.Now()
	var queued int
	must(t, pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE status = 'queued'`).Scan(&queued))
	if queued < 50 {
		t.Fatalf("only %d jobs still queued at the kill; the test needs more work", queued)
	}
	var newLeader string
	waitFor(t, 30*time.Second, "a new leader", func() bool {
		newLeader = leaderService(t)
		return newLeader != old
	})
	// Measure from the leadership row the new leader wrote, not from this test's polling,
	// which stalls on DNS lookups for the dead container.
	var epoch int64
	var holder string
	var electedAt time.Time
	must(t, pool.QueryRow(ctx, `SELECT epoch, holder, elected_at FROM leadership`).Scan(&epoch, &holder, &electedAt))
	elected := electedAt.Sub(killedAt)
	t.Logf("%s killed with %d jobs queued; %s elected after %s (epoch %d)", old, queued, newLeader,
		elected.Round(100*time.Millisecond), epoch)

	// The new leader assigns work again.
	var firstAssign time.Time
	waitFor(t, 30*time.Second, "an assignment by the new leader", func() bool {
		var ts *time.Time
		_ = pool.QueryRow(ctx, `SELECT min(started_at) FROM attempts WHERE started_at > $1`, killedAt).Scan(&ts)
		if ts != nil {
			firstAssign = *ts
		}
		return ts != nil
	})
	t.Logf("first assignment by the new leader %s after the kill", firstAssign.Sub(killedAt).Round(100*time.Millisecond))

	waitAllDone(t, pool, 3*time.Minute)
	assertExactlyOnce(t, pool, n)
	// Only workers that died during this test count; earlier tests leave dead rows behind.
	var deadAfter int
	must(t, pool.QueryRow(ctx, `SELECT count(*) FROM workers WHERE state = 'dead'`).Scan(&deadAfter))
	if deadAfter != deadBefore {
		t.Errorf("%d workers were declared dead during a scheduler failover", deadAfter-deadBefore)
	}
}

// TestFrozenWorkerCannotRecordStaleResults freezes worker-3 (docker pause, like a long GC
// pause or a network partition) while it runs jobs. The leader declares it dead and gives
// its jobs to others. When it thaws, its tasks finish and it tries to report; the attempt
// fence must reject every one of those reports.
func TestFrozenWorkerCannotRecordStaleResults(t *testing.T) {
	pool := db(t)
	reset(t, pool)
	ensureRunning(t, "scheduler-1", "scheduler-2", "worker-1", "worker-2", "worker-3")
	waitWorkersAlive(t, 3)
	ctx := context.Background()

	const n = 60
	specs := make([]map[string]any, n)
	for i := range specs {
		specs[i] = map[string]any{"type": "sleep", "payload": map[string]any{"ms": 2500},
			"resources": map[string]any{"cpu_millis": 500, "memory_mb": 32}}
	}
	submitBatch(t, specs)
	var held []string
	waitFor(t, 30*time.Second, "worker-3 to hold jobs", func() bool {
		held = heldBy(t, pool, "worker-3")
		return len(held) >= 2
	})
	pause(t, "worker-3")
	waitFor(t, 30*time.Second, "worker-3 to be declared dead", func() bool {
		var state string
		_ = pool.QueryRow(ctx, `SELECT state FROM workers WHERE id = 'worker-3'`).Scan(&state)
		return state == "dead"
	})
	// Let the reassigned copies finish, then thaw the zombie.
	waitFor(t, 60*time.Second, "worker-3's jobs to finish elsewhere", func() bool {
		var done int
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE id = ANY($1) AND status = 'succeeded'`, held).Scan(&done)
		return done == len(held)
	})
	unpause(t, "worker-3")
	waitAllDone(t, pool, 2*time.Minute)
	// Give the thawed worker time to report and rejoin.
	waitWorkersAlive(t, 3)

	assertExactlyOnce(t, pool, n)
	var fromZombie, rejected int
	must(t, pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM results WHERE job_id = ANY($1) AND worker_id = 'worker-3'),
		(SELECT count(*) FROM attempts WHERE job_id = ANY($1) AND worker_id = 'worker-3' AND outcome = 'rejected')`,
		held).Scan(&fromZombie, &rejected))
	if fromZombie != 0 {
		t.Fatalf("%d results recorded from the frozen worker's stale attempts", fromZombie)
	}
	t.Logf("worker-3 held %d jobs when frozen; %d stale reports rejected after it thawed (the rest were cancelled by heartbeat)",
		len(held), rejected)
}

// TestIdempotentSubmit checks that a client retry with the same key doesn't create a second job.
func TestIdempotentSubmit(t *testing.T) {
	pool := db(t)
	reset(t, pool)
	key := fmt.Sprintf("retry-%d", time.Now().UnixNano())
	spec := map[string]any{"type": "sleep", "payload": map[string]any{"ms": 10}}
	var a, b job
	if code := api(t, "POST", "/v1/jobs", spec, &a, "Idempotency-Key", key); code != 201 {
		t.Fatalf("first submit: %d", code)
	}
	if code := api(t, "POST", "/v1/jobs", spec, &b, "Idempotency-Key", key); code != 200 {
		t.Fatalf("replayed submit: want 200, got %d", code)
	}
	if a.ID != b.ID {
		t.Fatalf("replay created a new job: %s vs %s", a.ID, b.ID)
	}
	var count int
	must(t, pool.QueryRow(context.Background(), `SELECT count(*) FROM jobs`).Scan(&count))
	if count != 1 {
		t.Fatalf("want 1 job, got %d", count)
	}
}

// TestRetriesThenSucceeds checks a job that fails its first two attempts is retried and
// recorded once.
func TestRetriesThenSucceeds(t *testing.T) {
	pool := db(t)
	reset(t, pool)
	ensureRunning(t, "scheduler-1", "scheduler-2", "worker-1", "worker-2", "worker-3")
	var j job
	api(t, "POST", "/v1/jobs", map[string]any{"type": "sleep", "max_attempts": 4,
		"payload": map[string]any{"ms": 20, "fail_attempts": 2}}, &j)
	waitFor(t, 60*time.Second, "job to finish", func() bool {
		var got job
		api(t, "GET", "/v1/jobs/"+j.ID, nil, &got)
		j = got
		return got.Status == "succeeded" || got.Status == "failed"
	})
	if j.Status != "succeeded" || j.Attempt != 3 {
		t.Fatalf("want succeeded on attempt 3, got %s on attempt %d", j.Status, j.Attempt)
	}
	var results int
	must(t, pool.QueryRow(context.Background(), `SELECT count(*) FROM results WHERE job_id = $1`, j.ID).Scan(&results))
	if results != 1 {
		t.Fatalf("want 1 result, got %d", results)
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
