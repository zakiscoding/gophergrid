package worker

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/zakiscoding/gophergrid/internal/jobs"
)

// Cross-language test vectors, also computed by a separate Python implementation.
const (
	splitMixSeed1      = "c15c0289ec2d0a9167ec"
	imageChecksum64x48 = "1682d6bdc6025b5b"
)

func TestImageIsDeterministic(t *testing.T) {
	task := Task{JobID: "j", Attempt: 1, Type: jobs.TypeImage, Payload: json.RawMessage(`{"width":64,"height":48,"blur_radius":2,"seed":3}`)}
	a, err := runImage(context.Background(), task)
	if err != nil {
		t.Fatal(err)
	}
	task.Attempt = 2
	b, _ := runImage(context.Background(), task)
	if string(a) != string(b) {
		t.Fatalf("two attempts differ: %s vs %s", a, b)
	}
	task.Payload = json.RawMessage(`{"width":64,"height":48,"blur_radius":2,"seed":4}`)
	c, _ := runImage(context.Background(), task)
	if string(a) == string(c) {
		t.Fatal("different seeds produced the same checksum")
	}
}

func TestPayloadErrorsArePermanent(t *testing.T) {
	cases := []Task{
		{Type: jobs.TypeImage, Payload: json.RawMessage(`{"width":-1}`)},
		{Type: jobs.TypeSleep, Payload: json.RawMessage(`{"ms":"soon"}`)},
		{Type: jobs.TypeInference, Payload: json.RawMessage(`{}`)}, // no GPU assigned
	}
	runners := Runners(false, "sim")
	for _, c := range cases {
		_, err := runners[c.Type](context.Background(), c)
		var p PermanentError
		if !errors.As(err, &p) {
			t.Errorf("%s %s: want permanent error, got %v", c.Type, c.Payload, err)
		}
	}
}

func TestSleepFailAttempts(t *testing.T) {
	task := Task{Type: jobs.TypeSleep, Payload: json.RawMessage(`{"ms":1,"fail_attempts":1}`), Attempt: 1}
	if _, err := runSleep(context.Background(), task); err == nil {
		t.Fatal("attempt 1 should fail")
	}
	task.Attempt = 2
	if _, err := runSleep(context.Background(), task); err != nil {
		t.Fatal(err)
	}
}

func TestInferenceScalesWithGPUs(t *testing.T) {
	p := json.RawMessage(`{"prompts":10,"tokens":10,"ms_per_token":2}`) // 200 ms of work
	t0 := time.Now()
	if _, err := runInferenceSim(context.Background(), Task{Payload: p, GPUIDs: []string{"a", "b", "c", "d"}}); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(t0); d > 150*time.Millisecond {
		t.Fatalf("4 GPUs should take ~50ms, took %s", d)
	}
}

func TestParseNvidiaSMI(t *testing.T) {
	gpus, err := parseNvidiaSMI("GPU-abc, NVIDIA GeForce RTX 5070, 12227\nGPU-def, NVIDIA H100 80GB HBM3, 81559\n")
	if err != nil || len(gpus) != 2 || gpus[0].ID != "GPU-abc" || gpus[1].MemoryMB != 81559 {
		t.Fatalf("%+v %v", gpus, err)
	}
	if _, err := parseNvidiaSMI("garbage"); err == nil {
		t.Fatal("want error")
	}
}

// fakeTransport hands out a fixed list of tasks once and records results.
type fakeTransport struct {
	mu      sync.Mutex
	pending []Task
	states  []State
	results []Result
	done    chan struct{}
	want    int
}

func (f *fakeTransport) Register(context.Context, Info) error { return nil }
func (f *fakeTransport) Sync(_ context.Context, st State) (SyncResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.states = append(f.states, st)
	tasks := f.pending
	f.pending = nil
	return SyncResult{Tasks: tasks}, nil
}
func (f *fakeTransport) Complete(_ context.Context, r Result) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.results = append(f.results, r)
	if len(f.results) == f.want {
		close(f.done)
	}
	return true, nil
}

func TestAgentEnforcesCapacity(t *testing.T) {
	sleep := json.RawMessage(`{"ms":50}`)
	ft := &fakeTransport{done: make(chan struct{}), want: 3, pending: []Task{
		{JobID: "a", Attempt: 1, Type: "sleep", Payload: sleep, Resources: jobs.Resources{CPUMillis: 600, MemoryMB: 10}, Timeout: time.Second},
		{JobID: "b", Attempt: 1, Type: "sleep", Payload: sleep, Resources: jobs.Resources{CPUMillis: 600, MemoryMB: 10}, Timeout: time.Second},
		{JobID: "g", Attempt: 1, Type: "sleep", Payload: sleep, Resources: jobs.Resources{CPUMillis: 1, MemoryMB: 1}, GPUIDs: []string{"nope"}, Timeout: time.Second},
	}}
	a := &Agent{
		Info:      Info{ID: "w", Capacity: jobs.Resources{CPUMillis: 1000, MemoryMB: 100}, GPUList: []GPU{{ID: "sim-0"}}},
		Transport: ft, Runners: Runners(false, "sim"), Interval: 10 * time.Millisecond,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = a.Run(ctx, time.Second) }()
	select {
	case <-ft.done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out")
	}
	cancel()
	ft.mu.Lock()
	defer ft.mu.Unlock()
	ok, rejected := 0, 0
	for _, r := range ft.results {
		switch {
		case r.Success:
			ok++
		case r.Error == "worker over capacity":
			rejected++
		}
	}
	// a fits; b would exceed 1000 millicores; g asks for a GPU the worker doesn't have.
	if ok != 1 || rejected != 2 {
		t.Fatalf("want 1 ok and 2 rejected, got %+v", ft.results)
	}
}

func TestAgentDrainFinishesRunningWork(t *testing.T) {
	ft := &fakeTransport{done: make(chan struct{}), want: 1, pending: []Task{
		{JobID: "a", Attempt: 1, Type: "sleep", Payload: json.RawMessage(`{"ms":200}`), Resources: jobs.Resources{CPUMillis: 1, MemoryMB: 1}, Timeout: time.Second},
	}}
	a := &Agent{Info: Info{ID: "w", Capacity: jobs.Resources{CPUMillis: 1000, MemoryMB: 100}}, Transport: ft,
		Runners: Runners(false, "none"), Interval: 10 * time.Millisecond, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- a.Run(ctx, 2*time.Second) }()
	time.Sleep(50 * time.Millisecond)
	cancel() // SIGTERM while the task is running
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	ft.mu.Lock()
	defer ft.mu.Unlock()
	if len(ft.results) != 1 || !ft.results[0].Success {
		t.Fatalf("drain should let the task finish: %+v", ft.results)
	}
	if last := ft.states[len(ft.states)-1]; !last.Draining || last.Free.CPUMillis != 0 {
		t.Fatalf("last sync should advertise draining with no capacity: %+v", last)
	}
}

// The same vectors are in worker-rs/src/runners.rs: Go and Rust workers must agree.
func TestImageChecksumMatchesSpec(t *testing.T) {
	b := make([]byte, 10)
	fillSplitMix64(b, 1)
	if got := hex.EncodeToString(b); got != splitMixSeed1 {
		t.Fatalf("splitmix64(seed 1) = %s, want %s", got, splitMixSeed1)
	}
	out, err := runImage(context.Background(), Task{Type: jobs.TypeImage,
		Payload: json.RawMessage(`{"width":64,"height":48,"blur_radius":2,"seed":3}`)})
	if err != nil {
		t.Fatal(err)
	}
	var r struct{ Checksum string }
	_ = json.Unmarshal(out, &r)
	if r.Checksum != imageChecksum64x48 {
		t.Fatalf("checksum = %s, want %s", r.Checksum, imageChecksum64x48)
	}
}

// BenchmarkImageJob matches bench_image_job in worker-rs (cargo test --release -- --ignored).
func BenchmarkImageJob(b *testing.B) {
	task := Task{Type: jobs.TypeImage, Payload: json.RawMessage(`{"width":512,"height":512,"blur_radius":3,"seed":7}`)}
	for b.Loop() {
		if _, err := runImage(context.Background(), task); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkHashJob matches bench_hash_job in worker-rs.
func BenchmarkHashJob(b *testing.B) {
	task := Task{JobID: "bench", Type: jobs.TypeHash, Payload: json.RawMessage(`{"rounds":1000000}`)}
	for b.Loop() {
		if _, err := runHash(context.Background(), task); err != nil {
			b.Fatal(err)
		}
	}
}
