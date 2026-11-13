// Command ggbench measures GopherGrid: it submits a burst of jobs over REST, waits for them
// to finish, and reads throughput and latency percentiles from Postgres timestamps (one
// clock, no client skew). Optionally it kills workers on a schedule while the run is going.
//
//	ggbench -n 5000 -ms 0 -name baseline -reset
//	ggbench -n 3000 -ms 200 -chaos 10s -chaos-targets worker-1,worker-2,worker-3 -name chaos
//
// Results are written as JSON to -out (default bench/out/<name>.json).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type result struct {
	Name          string            `json:"name"`
	Jobs          int               `json:"jobs"`
	JobType       string            `json:"job_type"`
	JobMS         int               `json:"job_ms"`
	CPUMillis     int               `json:"cpu_millis"`
	Workers       int               `json:"workers"`
	Note          string            `json:"note,omitempty"`
	SubmitSeconds float64           `json:"submit_seconds"`
	SubmitRate    float64           `json:"submit_jobs_per_sec"`
	WallSeconds   float64           `json:"wall_seconds"` // first created to last finished
	Throughput    float64           `json:"throughput_jobs_per_sec"`
	QueueMS       map[string]float64 `json:"queue_latency_ms"` // created -> first assigned
	EndToEndMS    map[string]float64 `json:"end_to_end_ms"`    // created -> finished
	Retried       int               `json:"retried_jobs"`
	Failed        int               `json:"failed_jobs"`
	Results       int               `json:"results"`
	Duplicates    int               `json:"duplicate_results"`
	Kills         []string          `json:"kills,omitempty"`
	Config        map[string]string `json:"config,omitempty"`
	At            time.Time         `json:"at"`
}

func main() {
	n := flag.Int("n", 2000, "jobs")
	typ := flag.String("type", "sleep", "job type")
	ms := flag.Int("ms", 0, "sleep job duration")
	payloadFlag := flag.String("payload", "", "JSON payload (overrides -ms)")
	cpu := flag.Int("cpu", 100, "CPU millicores per job")
	batch := flag.Int("batch", 500, "jobs per submit request")
	submitters := flag.Int("submitters", 4, "concurrent submit requests")
	name := flag.String("name", "run", "result name")
	out := flag.String("out", "", "output file (default bench/out/<name>.json)")
	reset := flag.Bool("reset", false, "TRUNCATE jobs before the run (local benchmarks only)")
	chaos := flag.Duration("chaos", 0, "kill one random target every interval")
	targets := flag.String("chaos-targets", "", "compose services to kill (restarted after the run)")
	restartAfter := flag.Duration("chaos-restart", 0, "restart a killed target after this long (default: chaos/2)")
	project := flag.String("project", "gophergrid", "compose project for -chaos")
	note := flag.String("note", "", "free-text config note stored in the result")
	timeout := flag.Duration("timeout", 10*time.Minute, "give up after")
	flag.Parse()

	base := env("GOPHERGRID_URL", "http://scheduler-1:8080")
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	check(err)
	defer pool.Close()
	if *reset {
		_, err = pool.Exec(ctx, `TRUNCATE batches, jobs CASCADE`)
		check(err)
	}
	var workers int
	check(pool.QueryRow(ctx, `SELECT count(*) FROM workers WHERE state = 'alive' AND last_heartbeat > now() - interval '3 seconds'`).Scan(&workers))

	payload := json.RawMessage(fmt.Sprintf(`{"ms":%d}`, *ms))
	if *payloadFlag != "" {
		payload = json.RawMessage(*payloadFlag)
	}
	spec := map[string]any{"type": *typ, "payload": payload, "resources": map[string]any{"cpu_millis": *cpu, "memory_mb": 16}}

	// Chaos: kill a random target every interval until the run ends.
	var kills []string
	chaosDone := make(chan struct{})
	stopChaos := make(chan struct{})
	if *chaos > 0 && *targets != "" {
		go func() {
			defer close(chaosDone)
			ts := strings.Split(*targets, ",")
			tick := time.NewTicker(*chaos)
			defer tick.Stop()
			for {
				select {
				case <-stopChaos:
					return
				case <-tick.C:
					victim := ts[rand.Intn(len(ts))]
					if err := dockerAction(*project, victim, "kill?signal=SIGKILL"); err == nil {
						kills = append(kills, fmt.Sprintf("%s@%s", victim, time.Now().Format("15:04:05.000")))
						fmt.Println("chaos: killed", victim)
						go func(v string) { // bring it back so capacity recovers
							d := *restartAfter
							if d == 0 {
								d = *chaos / 2
							}
							time.Sleep(d)
							_ = dockerAction(*project, v, "start")
						}(victim)
					}
				}
			}
		}()
	} else {
		close(chaosDone)
	}

	// Submit.
	start := time.Now()
	var wg sync.WaitGroup
	sem := make(chan struct{}, *submitters)
	var submitErr error
	var mu sync.Mutex
	for off := 0; off < *n; off += *batch {
		size := min(*batch, *n-off)
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			specs := make([]map[string]any, size)
			for i := range specs {
				specs[i] = spec
			}
			if err := post(ctx, base+"/v1/jobs/batch", map[string]any{"jobs": specs}); err != nil {
				mu.Lock()
				submitErr = err
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	check(submitErr)
	submitted := time.Since(start)
	fmt.Printf("submitted %d jobs in %s (%.0f/s)\n", *n, submitted.Round(time.Millisecond), float64(*n)/submitted.Seconds())

	// Wait.
	for {
		var open int
		check(pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE status IN ('queued', 'running')`).Scan(&open))
		if open == 0 {
			break
		}
		select {
		case <-ctx.Done():
			check(fmt.Errorf("timed out with %d jobs open", open))
		case <-time.After(200 * time.Millisecond):
		}
	}
	close(stopChaos)
	<-chaosDone
	if *chaos > 0 {
		for _, t := range strings.Split(*targets, ",") {
			_ = dockerAction(*project, t, "start")
		}
	}

	r := result{Name: *name, Jobs: *n, JobType: *typ, JobMS: *ms, CPUMillis: *cpu, Workers: workers,
		SubmitSeconds: submitted.Seconds(), SubmitRate: float64(*n) / submitted.Seconds(), Kills: kills,
		Note: *note, At: time.Now().UTC(), Config: map[string]string{}}
	var wall float64
	q := `SELECT
		extract(epoch FROM max(finished_at) - min(created_at)),
		percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM assigned_at - created_at)) * 1000,
		percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM assigned_at - created_at)) * 1000,
		percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM assigned_at - created_at)) * 1000,
		percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM finished_at - created_at)) * 1000,
		percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM finished_at - created_at)) * 1000,
		percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM finished_at - created_at)) * 1000,
		count(*) FILTER (WHERE attempt > 1), count(*) FILTER (WHERE status = 'failed'),
		(SELECT count(*) FROM results),
		(SELECT count(*) FROM (SELECT job_id FROM results GROUP BY job_id HAVING count(*) > 1) d)
		FROM jobs`
	var q50, q95, q99, e50, e95, e99 float64
	check(pool.QueryRow(ctx, q).Scan(&wall, &q50, &q95, &q99, &e50, &e95, &e99, &r.Retried, &r.Failed, &r.Results, &r.Duplicates))
	r.WallSeconds = wall
	r.Throughput = float64(*n) / wall
	r.QueueMS = map[string]float64{"p50": round(q50), "p95": round(q95), "p99": round(q99)}
	r.EndToEndMS = map[string]float64{"p50": round(e50), "p95": round(e95), "p99": round(e99)}
	for _, k := range []string{"HEARTBEAT_LONG_POLL", "EAGER_SYNC", "SCHED_INTERVAL", "SCHED_BATCH", "HEARTBEAT_INTERVAL"} {
		if v := os.Getenv(k); v != "" {
			r.Config[k] = v
		}
	}

	b, _ := json.MarshalIndent(r, "", "  ")
	fmt.Println(string(b))
	path := *out
	if path == "" {
		path = filepath.Join("bench", "out", *name+".json")
	}
	_ = os.MkdirAll(filepath.Dir(path), 0o755)
	check(os.WriteFile(path, append(b, '\n'), 0o644))
	if r.Duplicates != 0 || r.Results != *n-r.Failed {
		fmt.Fprintf(os.Stderr, "INVARIANT BROKEN: %d results for %d jobs (%d failed), %d duplicates\n", r.Results, *n, r.Failed, r.Duplicates)
		os.Exit(1)
	}
}

func round(f float64) float64 { return float64(int(f*10+0.5)) / 10 }

func post(ctx context.Context, u string, body any) error {
	b, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, "POST", u, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 300))
		return fmt.Errorf("POST %s: %s: %s", u, resp.Status, msg)
	}
	return nil
}

var docker = &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, "unix", "/var/run/docker.sock")
}}}

func dockerAction(project, service, action string) error {
	f, _ := json.Marshal(map[string][]string{"label": {"com.docker.compose.project=" + project, "com.docker.compose.service=" + service}})
	resp, err := docker.Get("http://docker/containers/json?all=1&filters=" + url.QueryEscape(string(f)))
	if err != nil {
		return err
	}
	var cs []struct{ ID string `json:"Id"` }
	_ = json.NewDecoder(resp.Body).Decode(&cs)
	resp.Body.Close()
	if len(cs) == 0 {
		return fmt.Errorf("no container for %s", service)
	}
	resp, err = docker.Post("http://docker/containers/"+cs[0].ID+"/"+action, "", nil)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "ggbench:", err)
		os.Exit(1)
	}
}
