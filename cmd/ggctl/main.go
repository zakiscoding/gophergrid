// Command ggctl is a small GopherGrid client: submit jobs, check status, and wait for a
// batch to finish.
//
//	ggctl submit -type image -n 100 -payload '{"width":512,"height":512}'
//	ggctl get <job-id>
//	ggctl stats
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	base := env("GOPHERGRID_URL", "http://localhost:8080")
	switch os.Args[1] {
	case "submit":
		fs := flag.NewFlagSet("submit", flag.ExitOnError)
		typ := fs.String("type", "sleep", "job type")
		n := fs.Int("n", 1, "number of jobs")
		payload := fs.String("payload", "{}", "JSON payload")
		prio := fs.Int("priority", 0, "priority, -100..100")
		cpu := fs.Int64("cpu", 100, "CPU millicores")
		mem := fs.Int64("mem", 64, "memory MB")
		gpus := fs.Int("gpus", 0, "GPUs")
		gpuMem := fs.Int64("gpu-mem", 0, "GPU memory MB per GPU")
		wait := fs.Bool("wait", false, "wait until all jobs finish")
		_ = fs.Parse(os.Args[2:])
		var specs []map[string]any
		for i := 0; i < *n; i++ {
			specs = append(specs, map[string]any{"type": *typ, "payload": json.RawMessage(*payload), "priority": *prio,
				"resources": map[string]any{"cpu_millis": *cpu, "memory_mb": *mem, "gpus": *gpus, "gpu_memory_mb": *gpuMem}})
		}
		var resp struct {
			Jobs []struct {
				ID string `json:"id"`
			} `json:"jobs"`
		}
		start := time.Now()
		must(call(base, "POST", "/v1/jobs/batch", map[string]any{"jobs": specs}, &resp))
		fmt.Printf("submitted %d jobs in %s\n", len(resp.Jobs), time.Since(start).Round(time.Millisecond))
		if *wait {
			waitAll(base, len(resp.Jobs), start)
		}
	case "get":
		if len(os.Args) < 3 {
			usage()
		}
		var j json.RawMessage
		must(call(base, "GET", "/v1/jobs/"+os.Args[2], nil, &j))
		pretty(j)
	case "stats":
		var s json.RawMessage
		must(call(base, "GET", "/v1/stats", nil, &s))
		pretty(s)
	case "workers":
		var s json.RawMessage
		must(call(base, "GET", "/v1/workers", nil, &s))
		pretty(s)
	default:
		usage()
	}
}

func waitAll(base string, n int, start time.Time) {
	for {
		var st struct {
			ByStatus map[string]int `json:"by_status"`
		}
		must(call(base, "GET", "/v1/stats", nil, &st))
		if st.ByStatus["queued"]+st.ByStatus["running"] == 0 {
			fmt.Printf("all done in %s: %v\n", time.Since(start).Round(time.Millisecond), st.ByStatus)
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func call(base, method, path string, body, out any) error {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, base+path, r)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s: %s: %s", method, path, resp.Status, bytes.TrimSpace(b))
	}
	return json.Unmarshal(b, out)
}

func pretty(b json.RawMessage) {
	var buf bytes.Buffer
	_ = json.Indent(&buf, b, "", "  ")
	fmt.Println(buf.String())
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: ggctl submit|get <id>|stats|workers   (GOPHERGRID_URL, default http://localhost:8080)")
	os.Exit(2)
}
