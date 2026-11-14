//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"
)

// TestLLMWorkerCrashExactlyOnce runs `llm` jobs (chat completions against the fake
// OpenAI server, which stands in for the homelab's vLLM) and SIGKILLs a worker while it has
// requests in flight. The dead worker's jobs are sent again by another worker, so the server
// sees more requests than there are prompts, but each job records exactly one result, and
// (the fake server being deterministic) a retried job's output equals the first attempt's.
func TestLLMWorkerCrashExactlyOnce(t *testing.T) {
	pool := db(t)
	reset(t, pool)
	ensureRunning(t, "scheduler-1", "scheduler-2", "worker-1", "worker-2", "worker-3", "fakellm")
	waitWorkersAlive(t, 3)
	ctx := context.Background()
	before := fakeRequests(t)

	const n, prompts = 60, 4
	specs := make([]map[string]any, n)
	for i := range specs {
		ps := make([]string, prompts)
		for j := range ps {
			ps[j] = fmt.Sprintf("question %d of job %d", j, i%10) // 10 distinct jobs, 6 copies each
		}
		specs[i] = map[string]any{
			"type":       "llm",
			"payload":    map[string]any{"prompts": ps, "max_tokens": 40, "seed": 7}, // ~2.1 s per request
			"timeout_ms": 30000,
			"resources":  map[string]any{"cpu_millis": 250, "memory_mb": 32},
		}
	}
	submitBatch(t, specs)

	var held []string
	waitFor(t, 30*time.Second, "worker-2 to hold llm jobs", func() bool {
		held = heldBy(t, pool, "worker-2")
		return len(held) > 0
	})
	kill(t, "worker-2")
	t.Logf("killed worker-2 holding %d llm jobs mid-request", len(held))

	waitAllDone(t, pool, 3*time.Minute)
	assertExactlyOnce(t, pool, n)

	// Same prompts, same seed: one output digest per distinct job, whichever attempt won.
	var digests int
	must(t, pool.QueryRow(ctx, `SELECT count(DISTINCT output->>'outputs_digest') FROM results`).Scan(&digests))
	if digests != 10 {
		t.Fatalf("10 distinct llm jobs produced %d distinct output digests", digests)
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
	sent := fakeRequests(t) - before
	t.Logf("server saw %d requests for %d prompts (%d re-sent by retries); %d results recorded",
		sent, n*prompts, sent-n*prompts, n)
	if sent < n*prompts {
		t.Fatalf("server saw %d requests, fewer than the %d prompts", sent, n*prompts)
	}
}

func fakeRequests(t *testing.T) int {
	t.Helper()
	resp, err := http.Get(env("FAKELLM_URL", "http://fakellm:8000") + "/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var s struct{ Requests int }
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		t.Fatal(err)
	}
	return s.Requests
}
