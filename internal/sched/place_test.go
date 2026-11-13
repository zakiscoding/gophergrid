package sched

import (
	"fmt"
	"testing"

	"github.com/zakiscoding/gophergrid/internal/jobs"
	"github.com/zakiscoding/gophergrid/internal/store"
)

var allTypes = []string{jobs.TypeSleep, jobs.TypeImage, jobs.TypeHash, jobs.TypeInference}

func worker(id string, cpu, mem int64, gpus ...store.GPUInfo) store.Worker {
	return store.Worker{ID: id, CPUMillis: cpu, MemoryMB: mem, GPUs: gpus, JobTypes: allTypes, State: "alive"}
}

func gpu(id string, mem int64) store.GPUInfo { return store.GPUInfo{ID: id, MemoryMB: mem} }

func job(id string, cpu, mem int64, gpus int, gpuMem int64) jobs.Job {
	return jobs.Job{ID: id, Type: jobs.TypeSleep,
		Resources: jobs.Resources{CPUMillis: cpu, MemoryMB: mem, GPUs: gpus, GPUMemoryMB: gpuMem}}
}

func nodes(ws ...store.Worker) []*Node {
	out := make([]*Node, len(ws))
	for i, w := range ws {
		out[i] = NewNode(w, store.Usage{})
	}
	return out
}

func where(as []store.Assignment) map[string]string {
	m := map[string]string{}
	for _, a := range as {
		m[a.JobID] = a.WorkerID
	}
	return m
}

func TestSpreadsByLoad(t *testing.T) {
	ns := nodes(worker("a", 2000, 2048), worker("b", 2000, 2048), worker("c", 2000, 2048))
	var q []jobs.Job
	for i := 0; i < 6; i++ {
		q = append(q, job(fmt.Sprint(i), 500, 64, 0, 0))
	}
	per := map[string]int{}
	for _, w := range where(Place(q, ns, Policy{})) {
		per[w]++
	}
	if per["a"] != 2 || per["b"] != 2 || per["c"] != 2 {
		t.Fatalf("want 2 jobs per worker, got %v", per)
	}
}

func TestRespectsCapacityAndBackfills(t *testing.T) {
	ns := nodes(worker("a", 1000, 1024))
	q := []jobs.Job{job("big", 1500, 64, 0, 0), job("fits", 800, 64, 0, 0), job("tiny", 200, 64, 0, 0), job("over", 100, 64, 0, 0)}
	got := where(Place(q, ns, Policy{}))
	if _, ok := got["big"]; ok {
		t.Fatal("placed a job bigger than any worker")
	}
	if got["fits"] != "a" || got["tiny"] != "a" {
		t.Fatalf("smaller jobs should backfill behind the big one: %v", got)
	}
	if _, ok := got["over"]; ok {
		t.Fatal("over-committed CPU")
	}
}

func TestMemoryIsAConstraintToo(t *testing.T) {
	ns := nodes(worker("a", 8000, 512))
	got := where(Place([]jobs.Job{job("x", 100, 400, 0, 0), job("y", 100, 400, 0, 0)}, ns, Policy{}))
	if len(got) != 1 {
		t.Fatalf("want one job placed in 512 MB, got %v", got)
	}
}

func TestGPUBestFitAndMemory(t *testing.T) {
	// "small" has one 12 GB GPU, "big" has two 80 GB GPUs.
	ns := nodes(worker("small", 8000, 8192, gpu("s0", 12_000)), worker("big", 8000, 8192, gpu("b0", 80_000), gpu("b1", 80_000)))
	q := []jobs.Job{
		job("needs-40g", 100, 64, 1, 40_000), // only fits on big
		job("needs-8g", 100, 64, 1, 8_000),   // fits anywhere: best fit keeps big's GPU free
		job("needs-2x", 100, 64, 2, 1_000),   // needs two GPUs: only big had two, one is taken
	}
	as := Place(q, ns, Policy{})
	got := where(as)
	if got["needs-40g"] != "big" || got["needs-8g"] != "small" {
		t.Fatalf("bad GPU placement: %v", got)
	}
	if _, ok := got["needs-2x"]; ok {
		t.Fatal("placed a 2-GPU job with one GPU free")
	}
	for _, a := range as {
		if a.JobID == "needs-40g" && (len(a.GPUIDs) != 1 || a.GPUIDs[0] == "s0") {
			t.Fatalf("40 GB job got GPUs %v", a.GPUIDs)
		}
	}
}

func TestGPUsAreNotDoubleBooked(t *testing.T) {
	w := worker("g", 8000, 8192, gpu("g0", 12_000), gpu("g1", 12_000))
	n := NewNode(w, store.Usage{GPUIDs: []string{"g0"}, Resources: jobs.Resources{GPUs: 1}})
	as := Place([]jobs.Job{job("a", 100, 64, 1, 0), job("b", 100, 64, 1, 0)}, []*Node{n}, Policy{})
	if len(as) != 1 || as[0].GPUIDs[0] != "g1" {
		t.Fatalf("want only g1 handed out, got %+v", as)
	}
}

func TestKeepGPUNodesFree(t *testing.T) {
	ns := nodes(worker("cpu", 1000, 1024), worker("gpu", 8000, 8192, gpu("g0", 12_000)))
	q := []jobs.Job{job("c1", 500, 64, 0, 0), job("c2", 500, 64, 0, 0), job("c3", 500, 64, 0, 0)}
	got := where(Place(q, ns, Policy{KeepGPUNodesFree: true}))
	// The CPU node takes two; the third spills over to the GPU node rather than waiting.
	if got["c1"] != "cpu" || got["c2"] != "cpu" || got["c3"] != "gpu" {
		t.Fatalf("got %v", got)
	}
	got = where(Place(q, nodes(worker("cpu", 1000, 1024), worker("gpu", 8000, 8192, gpu("g0", 12_000))), Policy{}))
	if got["c1"] != "gpu" {
		t.Fatalf("without the policy the emptier GPU node should win: %v", got)
	}
}

func TestOnlyWorkersThatRunTheType(t *testing.T) {
	w := worker("a", 1000, 1024)
	w.JobTypes = []string{jobs.TypeSleep}
	j := job("x", 100, 64, 0, 0)
	j.Type = jobs.TypeImage
	if as := Place([]jobs.Job{j}, nodes(w), Policy{}); len(as) != 0 {
		t.Fatal("placed an image job on a sleep-only worker")
	}
}

func BenchmarkPlace(b *testing.B) {
	var ws []store.Worker
	for i := 0; i < 100; i++ {
		ws = append(ws, worker(fmt.Sprint("w", i), 16_000, 65_536, gpu(fmt.Sprint("g", i), 24_000)))
	}
	var q []jobs.Job
	for i := 0; i < 256; i++ {
		q = append(q, job(fmt.Sprint(i), 250, 128, i%8/7, 0))
	}
	b.ReportAllocs()
	for b.Loop() {
		Place(q, nodes(ws...), Policy{KeepGPUNodesFree: true})
	}
}
