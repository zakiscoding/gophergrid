//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"
)

type job struct {
	ID       string  `json:"id"`
	Status   string  `json:"status"`
	Attempt  int     `json:"attempt"`
	WorkerID *string `json:"worker_id"`
}

// TestWorkerCrashReassignsWithoutDuplicates is the spec's example: submit 100
// image-processing jobs, kill worker-2 while it holds some of them, and check that every
// job finishes exactly once with no duplicate results.
func TestWorkerCrashReassignsWithoutDuplicates(t *testing.T) {
	pool := db(t)
	reset(t, pool)
	ensureRunning(t, "worker-1", "worker-2", "worker-3")
	ctx := context.Background()

	const n = 100
	specs := make([]map[string]any, n)
	for i := range specs {
		specs[i] = map[string]any{
			"type": "image",
			// Same seed for all, so every correct result has the same checksum.
			"payload":   map[string]any{"width": 640, "height": 640, "blur_radius": 4, "seed": 7},
			"resources": map[string]any{"cpu_millis": 1000, "memory_mb": 128},
			// Phase 1 has no heartbeats: a dead worker's job comes back when its lease
			// (timeout + 5 s grace) runs out, so keep the timeout short.
			"timeout_ms": 3000,
		}
	}
	var resp struct{ Jobs []job }
	if code := api(t, "POST", "/v1/jobs/batch", map[string]any{"jobs": specs}, &resp); code != 201 || len(resp.Jobs) != n {
		t.Fatalf("batch submit: %d, %d jobs", code, len(resp.Jobs))
	}
	start := time.Now()

	// Kill worker-2 as soon as it is running something.
	var held []string
	waitFor(t, 30*time.Second, "worker-2 to hold a job", func() bool {
		rows, err := pool.Query(ctx, `SELECT id FROM jobs WHERE status = 'running' AND worker_id = 'worker-2'`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		held = held[:0]
		for rows.Next() {
			var id string
			_ = rows.Scan(&id)
			held = append(held, id)
		}
		return len(held) > 0
	})
	kill(t, "worker-2")
	t.Logf("worker-2 held %d running jobs when killed", len(held))

	waitFor(t, 3*time.Minute, "all jobs to finish", func() bool {
		var open int
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE status IN ('queued', 'running')`).Scan(&open)
		return open == 0
	})
	t.Logf("all %d jobs finished in %s", n, time.Since(start).Round(time.Millisecond))

	var succeeded, results, distinctResults, checksums, multiResult int
	must(t, pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM jobs WHERE status = 'succeeded'),
		(SELECT count(*) FROM results),
		(SELECT count(DISTINCT job_id) FROM results),
		(SELECT count(DISTINCT output->>'checksum') FROM results),
		(SELECT count(*) FROM (SELECT job_id FROM results GROUP BY job_id HAVING count(*) > 1) d)`).
		Scan(&succeeded, &results, &distinctResults, &checksums, &multiResult))
	if succeeded != n || results != n || distinctResults != n || multiResult != 0 {
		t.Fatalf("want %d succeeded jobs with one result each; got succeeded=%d results=%d distinct=%d dup=%d",
			n, succeeded, results, distinctResults, multiResult)
	}
	if checksums != 1 {
		t.Fatalf("identical image jobs produced %d different checksums", checksums)
	}

	// Every job worker-2 held was retried and finished somewhere else.
	for _, id := range held {
		var attempt int
		var resultWorker string
		must(t, pool.QueryRow(ctx, `SELECT j.attempt, r.worker_id FROM jobs j JOIN results r ON r.job_id = j.id
			WHERE j.id = $1`, id).Scan(&attempt, &resultWorker))
		if attempt < 2 || resultWorker == "worker-2" {
			t.Errorf("job %s held by dead worker-2: attempt=%d result from %s", id, attempt, resultWorker)
		}
	}
	var expired int
	must(t, pool.QueryRow(ctx, `SELECT count(*) FROM attempts WHERE outcome = 'lease_expired' AND worker_id = 'worker-2'`).Scan(&expired))
	if expired < len(held) {
		t.Errorf("want >= %d lease_expired attempts on worker-2, got %d", len(held), expired)
	}
	t.Logf("reassigned %d jobs from worker-2; 0 duplicate results", expired)
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
	var j job
	api(t, "POST", "/v1/jobs", map[string]any{"type": "sleep", "max_attempts": 4,
		"payload": map[string]any{"ms": 20, "fail_attempts": 2}}, &j)
	waitFor(t, 30*time.Second, "job to finish", func() bool {
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
