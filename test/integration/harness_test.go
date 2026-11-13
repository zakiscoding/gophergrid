//go:build integration

// Package integration drives a running docker compose stack. Run it with
//
//	docker compose --profile test run --rm tester
//
// The tester container is on the compose network and has the Docker socket, so tests can
// kill and restart workers and schedulers.
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	baseURL = env("GOPHERGRID_URL", "http://scheduler:8080")
	project = env("COMPOSE_PROJECT", "gophergrid")
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// db connects to the scheduler's database, which the tests use as ground truth.
func db(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// reset deletes all jobs so each test starts from an empty queue.
func reset(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `TRUNCATE jobs CASCADE`); err != nil {
		t.Fatal(err)
	}
}

func api(t *testing.T, method, path string, body, out any, hdr ...string) int {
	t.Helper()
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, baseURL+path, r)
	req.Header.Set("Content-Type", "application/json")
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if out != nil && resp.StatusCode < 300 {
		if err := json.Unmarshal(b, out); err != nil {
			t.Fatalf("%s %s: decode %q: %v", method, path, b, err)
		}
	}
	return resp.StatusCode
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// --- Docker Engine API over the mounted socket -------------------------------------------

var dockerClient = &http.Client{Transport: &http.Transport{
	DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", "/var/run/docker.sock")
	},
}}

func docker(t *testing.T, method, path string, out any) {
	t.Helper()
	req, _ := http.NewRequest(method, "http://docker"+path, nil)
	resp, err := dockerClient.Do(req)
	if err != nil {
		t.Fatalf("docker %s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 && resp.StatusCode != http.StatusNotModified {
		t.Fatalf("docker %s %s: %s: %s", method, path, resp.Status, b)
	}
	if out != nil {
		_ = json.Unmarshal(b, out)
	}
}

// container returns the id and state of a compose service's container.
func container(t *testing.T, service string) (id, state string) {
	t.Helper()
	f, _ := json.Marshal(map[string][]string{"label": {
		"com.docker.compose.project=" + project, "com.docker.compose.service=" + service}})
	var cs []struct {
		ID    string `json:"Id"`
		State string `json:"State"`
	}
	docker(t, "GET", "/containers/json?all=1&filters="+url.QueryEscape(string(f)), &cs)
	if len(cs) == 0 {
		t.Fatalf("no container for service %s", service)
	}
	return cs[0].ID, cs[0].State
}

func kill(t *testing.T, service string) {
	id, _ := container(t, service)
	docker(t, "POST", "/containers/"+id+"/kill?signal=SIGKILL", nil)
	t.Logf("killed %s", service)
}

func start(t *testing.T, service string) {
	id, state := container(t, service)
	if state != "running" {
		docker(t, "POST", "/containers/"+id+"/start", nil)
		t.Logf("started %s", service)
	}
}

func ensureRunning(t *testing.T, services ...string) {
	for _, s := range services {
		start(t, s)
	}
	t.Cleanup(func() {
		for _, s := range services {
			start(t, s)
		}
	})
}

func fmtCounts(m map[string]int) string { return fmt.Sprint(m) }
