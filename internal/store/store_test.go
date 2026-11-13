package store

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/zakiscoding/gophergrid/internal/jobs"
)

// These tests need Postgres. The compose tester sets TEST_DATABASE_URL; without it they skip.
func testStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	s, err := Open(ctx, dsn, 10, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pool.Exec(ctx, `TRUNCATE jobs CASCADE`); err != nil {
		t.Fatal(err)
	}
	return s
}

func submit(t *testing.T, s *Store, spec jobs.Spec) jobs.Job {
	t.Helper()
	if err := spec.Normalize(); err != nil {
		t.Fatal(err)
	}
	j, _, err := s.Submit(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

var big = jobs.Resources{CPUMillis: 1 << 30, MemoryMB: 1 << 30, GPUs: 64}

// expire forces a job's lease into the past, as if its worker stopped heartbeating.
func expire(t *testing.T, s *Store, id string) {
	t.Helper()
	if _, err := s.Pool.Exec(context.Background(), `UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
}

func TestStaleAttemptCannotRecordResult(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	j := submit(t, s, jobs.Spec{Type: "sleep"})

	first, err := s.Claim(ctx, "w1", big, time.Second)
	if err != nil || first.ID != j.ID || first.Attempt != 1 {
		t.Fatalf("claim 1: %+v %v", first, err)
	}
	expire(t, s, j.ID)
	if r, err := s.ReapExpired(ctx); err != nil || r.Requeued != 1 {
		t.Fatalf("reap: %+v %v", r, err)
	}
	second, err := s.Claim(ctx, "w2", big, time.Second)
	if err != nil || second.Attempt != 2 {
		t.Fatalf("claim 2: %+v %v", second, err)
	}

	// The first worker wakes up and reports. It must be rejected.
	out, err := s.Complete(ctx, Completion{JobID: j.ID, Attempt: 1, WorkerID: "w1", Success: true, Output: json.RawMessage(`{"from":"w1"}`)})
	if err != nil || out.Accepted {
		t.Fatalf("stale completion accepted: %+v %v", out, err)
	}
	out, err = s.Complete(ctx, Completion{JobID: j.ID, Attempt: 2, WorkerID: "w2", Success: true, Output: json.RawMessage(`{"from":"w2"}`)})
	if err != nil || !out.Accepted || out.NewStatus != jobs.StatusSucceeded {
		t.Fatalf("current completion: %+v %v", out, err)
	}
	// A duplicate report of the winning attempt (e.g. a retried RPC) is also rejected.
	out, err = s.Complete(ctx, Completion{JobID: j.ID, Attempt: 2, WorkerID: "w2", Success: true, Output: json.RawMessage(`{"again":true}`)})
	if err != nil || out.Accepted {
		t.Fatalf("duplicate completion accepted: %+v %v", out, err)
	}
	got, err := s.Get(ctx, j.ID)
	if err != nil || string(got.Result) != `{"from": "w2"}` {
		t.Fatalf("result: %s %v", got.Result, err)
	}
	var outcomes []string
	rows, _ := s.Pool.Query(ctx, `SELECT outcome FROM attempts WHERE job_id = $1 ORDER BY attempt`, j.ID)
	for rows.Next() {
		var o string
		_ = rows.Scan(&o)
		outcomes = append(outcomes, o)
	}
	rows.Close()
	if len(outcomes) != 2 || outcomes[0] != "lease_expired" || outcomes[1] != "succeeded" {
		t.Fatalf("attempt outcomes: %v", outcomes)
	}
}

func TestConcurrentCompletionsRecordOneResult(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	j := submit(t, s, jobs.Spec{Type: "sleep"})
	if _, err := s.Claim(ctx, "w1", big, time.Second); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	accepted := make(chan bool, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := s.Complete(ctx, Completion{JobID: j.ID, Attempt: 1, WorkerID: "w1", Success: true, Output: json.RawMessage(`{}`)})
			if err != nil {
				t.Error(err)
			}
			accepted <- out.Accepted
		}()
	}
	wg.Wait()
	close(accepted)
	n := 0
	for a := range accepted {
		if a {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("want exactly 1 accepted completion, got %d", n)
	}
}

func TestRetryThenFailAfterMaxAttempts(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	j := submit(t, s, jobs.Spec{Type: "sleep", MaxAttempts: 2})
	for attempt := 1; attempt <= 2; attempt++ {
		c, err := s.Claim(ctx, "w1", big, time.Second)
		if err != nil || c.Attempt != attempt {
			t.Fatalf("claim %d: %+v %v", attempt, c, err)
		}
		out, err := s.Complete(ctx, Completion{JobID: j.ID, Attempt: attempt, WorkerID: "w1", Error: "boom"})
		if err != nil || !out.Accepted {
			t.Fatalf("complete %d: %+v %v", attempt, out, err)
		}
		want := jobs.StatusQueued
		if attempt == 2 {
			want = jobs.StatusFailed
		}
		if out.NewStatus != want {
			t.Fatalf("attempt %d: want %s, got %s", attempt, want, out.NewStatus)
		}
	}
}

func TestPermanentFailureIsNotRetried(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	j := submit(t, s, jobs.Spec{Type: "sleep", MaxAttempts: 5})
	if _, err := s.Claim(ctx, "w1", big, time.Second); err != nil {
		t.Fatal(err)
	}
	out, err := s.Complete(ctx, Completion{JobID: j.ID, Attempt: 1, WorkerID: "w1", Permanent: true, Error: "bad payload"})
	if err != nil || out.NewStatus != jobs.StatusFailed {
		t.Fatalf("%+v %v", out, err)
	}
}

func TestClaimOrderAndFit(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	low := submit(t, s, jobs.Spec{Type: "sleep", Priority: -10})
	bigJob := submit(t, s, jobs.Spec{Type: "sleep", Priority: 50, Resources: jobs.Resources{CPUMillis: 4000}})
	high := submit(t, s, jobs.Spec{Type: "sleep", Priority: 10})

	// 1000 millicores free: the priority-50 job doesn't fit, so priority 10 comes first.
	free := jobs.Resources{CPUMillis: 1000, MemoryMB: 1024}
	for _, want := range []string{high.ID, low.ID} {
		c, err := s.Claim(ctx, "w1", free, time.Second)
		if err != nil || c.ID != want {
			t.Fatalf("want %s, got %s (%v)", want, c.ID, err)
		}
	}
	if _, err := s.Claim(ctx, "w1", free, time.Second); err != ErrNotFound {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	c, err := s.Claim(ctx, "w2", big, time.Second)
	if err != nil || c.ID != bigJob.ID {
		t.Fatalf("big worker: %v %v", c.ID, err)
	}
}

func TestIdempotencyKey(t *testing.T) {
	s := testStore(t)
	spec := jobs.Spec{Type: "sleep", IdempotencyKey: "k1"}
	if err := spec.Normalize(); err != nil {
		t.Fatal(err)
	}
	a, created, err := s.Submit(context.Background(), spec)
	if err != nil || !created {
		t.Fatal(created, err)
	}
	b, created, err := s.Submit(context.Background(), spec)
	if err != nil || created || b.ID != a.ID {
		t.Fatalf("replay: created=%v id=%s err=%v", created, b.ID, err)
	}
}

func TestDeposedLeaderIsFencedOut(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	if _, err := s.Pool.Exec(ctx, `DELETE FROM leadership`); err != nil {
		t.Fatal(err)
	}
	submit(t, s, jobs.Spec{Type: "sleep"})
	if err := s.UpsertWorker(ctx, Worker{ID: "w1", CPUMillis: 1000, MemoryMB: 1000, JobTypes: []string{"sleep"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.TakeLeadership(ctx, 10, "old"); err != nil {
		t.Fatal(err)
	}
	if err := s.TakeLeadership(ctx, 20, "new"); err != nil {
		t.Fatal(err)
	}
	if err := s.TakeLeadership(ctx, 15, "stale"); err != ErrNotLeader {
		t.Fatalf("an older epoch took leadership back: %v", err)
	}
	placeAll := func(q []jobs.Job, _ map[string]Usage) []Assignment {
		var out []Assignment
		for _, j := range q {
			out = append(out, Assignment{JobID: j.ID, WorkerID: "w1"})
		}
		return out
	}
	order := "j.priority DESC, j.created_at"
	if _, err := s.ScheduleBatch(ctx, 10, 10, time.Second, order, placeAll); err != ErrNotLeader {
		t.Fatalf("deposed leader assigned work: %v", err)
	}
	if _, err := s.ReapExpiredFenced(ctx, 10, time.Second); err != ErrNotLeader {
		t.Fatalf("deposed leader reaped: %v", err)
	}
	if _, _, err := s.MarkDeadWorkers(ctx, 10, time.Second); err != ErrNotLeader {
		t.Fatalf("deposed leader declared workers dead: %v", err)
	}
	got, err := s.ScheduleBatch(ctx, 20, 10, time.Second, order, placeAll)
	if err != nil || len(got) != 1 || got[0].Attempt != 1 {
		t.Fatalf("current leader: %+v %v", got, err)
	}
}

func TestHeartbeatRenewsOnlyCurrentAttempts(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	j := submit(t, s, jobs.Spec{Type: "sleep"})
	c, err := s.Claim(ctx, "w1", big, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	n, err := s.RenewLeases(ctx, "w1", []Held{{c.ID, 1}, {c.ID, 99}}, time.Minute)
	if err != nil || n != 1 {
		t.Fatalf("renewed %d (want 1): %v", n, err)
	}
	if n, _ := s.RenewLeases(ctx, "w2", []Held{{j.ID, 1}}, time.Minute); n != 0 {
		t.Fatal("another worker renewed a lease it doesn't hold")
	}
}
