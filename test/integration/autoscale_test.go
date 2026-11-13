//go:build integration

package integration

import (
	"encoding/json"
	"net/url"
	"testing"
	"time"
)

func autoscaled(t *testing.T) int {
	f, _ := json.Marshal(map[string][]string{"label": {"gophergrid.autoscaled=true"}, "status": {"running"}})
	var cs []struct{ ID string `json:"Id"` }
	docker(t, "GET", "/containers/json?filters="+url.QueryEscape(string(f)), &cs)
	return len(cs)
}

// TestAutoscalerAddsAndRemovesWorkers runs only with the autoscale profile:
//
//	AUTOSCALE_COOLDOWN=10s docker compose --profile autoscale up -d autoscaler
func TestAutoscalerAddsAndRemovesWorkers(t *testing.T) {
	if _, state := containerMaybe(t, "autoscaler"); state != "running" {
		t.Skip("autoscaler not running (docker compose --profile autoscale up -d autoscaler)")
	}
	pool := db(t)
	reset(t, pool)
	ensureRunning(t, allWorkers...)
	waitWorkersAlive(t, 5)
	before := autoscaled(t)

	const n = 1500
	specs := make([]map[string]any, n)
	for i := range specs {
		specs[i] = map[string]any{"type": "sleep", "payload": map[string]any{"ms": 400},
			"resources": map[string]any{"cpu_millis": 1000, "memory_mb": 32}}
	}
	start := time.Now()
	submitBatch(t, specs)
	peak := 0
	waitFor(t, 60*time.Second, "the autoscaler to add workers", func() bool {
		peak = max(peak, autoscaled(t))
		return peak >= 2
	})
	t.Logf("autoscaler: %d -> %d workers %s after a %d-job burst", before, peak, time.Since(start).Round(100*time.Millisecond), n)
	waitFor(t, 3*time.Minute, "all jobs to finish", func() bool {
		peak = max(peak, autoscaled(t))
		var open int
		_ = pool.QueryRow(t.Context(), `SELECT count(*) FROM jobs WHERE status IN ('queued', 'running')`).Scan(&open)
		return open == 0
	})
	done := time.Now()
	t.Logf("drained in %s with up to %d autoscaled workers", done.Sub(start).Round(100*time.Millisecond), peak)
	assertExactlyOnce(t, pool, n)
	waitFor(t, 3*time.Minute, "scale down to zero", func() bool { return autoscaled(t) == 0 })
	t.Logf("scaled back to 0 %s after the queue drained", time.Since(done).Round(time.Second))
}
