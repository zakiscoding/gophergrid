// Package worker is the GopherGrid worker: it gets tasks from the scheduler, runs them
// within its resource capacity, and reports results.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/zakiscoding/gophergrid/internal/jobs"
)

// GPU is one device a worker offers. In sim mode these are made up; in nvidia mode they
// come from nvidia-smi.
type GPU struct {
	ID       string `json:"id"`
	Model    string `json:"model"`
	MemoryMB int64  `json:"memory_mb"`
}

// Info describes a worker to the scheduler.
type Info struct {
	ID       string
	Hostname string
	Capacity jobs.Resources // GPUs/GPUMemoryMB are derived from GPUList
	GPUList  []GPU
	GPUMode  string // "none", "sim" or "nvidia"
	Types    []string
}

// Running identifies one attempt the worker is running.
type Running struct {
	JobID   string
	Attempt int
}

// State is what the worker reports on every sync.
type State struct {
	Free     jobs.Resources
	FreeGPUs []string
	Running  []Running // running, or finished and waiting for the scheduler to acknowledge
	Draining bool
	CPUUtil  float64 // measured process CPU over the last interval, as a fraction of capacity
}

// SyncResult is what the scheduler sends back.
type SyncResult struct {
	Tasks  []Task
	Cancel []Running // attempts the worker must stop (lease lost, job reassigned)
}

// Result is a finished attempt.
type Result struct {
	JobID     string
	Attempt   int
	Success   bool
	Permanent bool
	Output    json.RawMessage
	Error     string
	Duration  time.Duration
}

// Transport is how the worker talks to the scheduler.
type Transport interface {
	Register(ctx context.Context, info Info) error
	Sync(ctx context.Context, st State) (SyncResult, error)
	Complete(ctx context.Context, r Result) (accepted bool, err error)
}

// Agent runs tasks.
type Agent struct {
	Info      Info
	Transport Transport
	Runners   map[string]Runner
	Interval  time.Duration // how often to sync (phase 2: the heartbeat interval)
	Log       *slog.Logger

	mu       sync.Mutex
	cpu      cpuMeter
	used     jobs.Resources
	gpuBusy  map[string]bool
	running  map[Running]context.CancelFunc
	wg       sync.WaitGroup
	draining bool
}

// Run syncs with the scheduler until ctx is cancelled, then stops taking work and waits up
// to drain for running tasks to finish and report.
func (a *Agent) Run(ctx context.Context, drain time.Duration) error {
	a.gpuBusy = map[string]bool{}
	a.running = map[Running]context.CancelFunc{}
	if a.Interval <= 0 {
		a.Interval = time.Second
	}
	if err := retry(ctx, a.Log, "register", func() error { return a.Transport.Register(ctx, a.Info) }); err != nil {
		return err
	}
	a.Log.Info("worker registered", "id", a.Info.ID, "cpu_millis", a.Info.Capacity.CPUMillis,
		"memory_mb", a.Info.Capacity.MemoryMB, "gpus", len(a.Info.GPUList), "gpu_mode", a.Info.GPUMode)

	// Tasks run under runCtx, which outlives ctx so in-flight work can finish while draining.
	runCtx, stopTasks := context.WithCancel(context.Background())
	defer stopTasks()

	t := time.NewTicker(a.Interval)
	defer t.Stop()
	for {
		a.sync(ctx, runCtx)
		select {
		case <-ctx.Done():
			return a.drain(runCtx, stopTasks, drain)
		case <-t.C:
		}
	}
}

func (a *Agent) sync(ctx, runCtx context.Context) {
	res, err := a.Transport.Sync(ctx, a.state())
	if err != nil {
		if ctx.Err() == nil {
			a.Log.Warn("sync failed", "err", err)
		}
		return
	}
	for _, c := range res.Cancel {
		a.mu.Lock()
		if cancel, ok := a.running[c]; ok {
			a.Log.Warn("scheduler cancelled attempt", "job", c.JobID, "attempt", c.Attempt)
			cancel()
		}
		a.mu.Unlock()
	}
	for _, task := range res.Tasks {
		a.start(runCtx, task)
	}
}

func (a *Agent) state() State {
	a.mu.Lock()
	defer a.mu.Unlock()
	st := State{Draining: a.draining, CPUUtil: a.cpu.sample(a.Info.Capacity.CPUMillis)}
	st.Free = jobs.Resources{
		CPUMillis: a.Info.Capacity.CPUMillis - a.used.CPUMillis,
		MemoryMB:  a.Info.Capacity.MemoryMB - a.used.MemoryMB,
	}
	for _, g := range a.Info.GPUList {
		if !a.gpuBusy[g.ID] {
			st.FreeGPUs = append(st.FreeGPUs, g.ID)
		}
	}
	st.Free.GPUs = len(st.FreeGPUs)
	if a.draining {
		st.Free = jobs.Resources{}
		st.FreeGPUs = nil
	}
	for r := range a.running {
		st.Running = append(st.Running, r)
	}
	return st
}

// start reserves the task's resources and runs it. The scheduler only sends tasks that fit,
// but the worker re-checks, because a resource limit is only real if both sides enforce it.
func (a *Agent) start(runCtx context.Context, task Task) {
	key := Running{task.JobID, task.Attempt}
	a.mu.Lock()
	if _, dup := a.running[key]; dup {
		a.mu.Unlock()
		return
	}
	fits := a.used.CPUMillis+task.Resources.CPUMillis <= a.Info.Capacity.CPUMillis &&
		a.used.MemoryMB+task.Resources.MemoryMB <= a.Info.Capacity.MemoryMB
	for _, id := range task.GPUIDs {
		if a.gpuBusy[id] || !a.hasGPU(id) {
			fits = false
		}
	}
	if !fits {
		a.mu.Unlock()
		a.Log.Warn("rejecting task that doesn't fit", "job", task.JobID)
		go a.report(Result{JobID: task.JobID, Attempt: task.Attempt, Error: "worker over capacity"})
		return
	}
	a.used.CPUMillis += task.Resources.CPUMillis
	a.used.MemoryMB += task.Resources.MemoryMB
	for _, id := range task.GPUIDs {
		a.gpuBusy[id] = true
	}
	ctx, cancel := context.WithTimeout(runCtx, task.Timeout)
	a.running[key] = cancel
	a.wg.Add(1)
	a.mu.Unlock()

	go func() {
		defer a.wg.Done()
		start := time.Now()
		res := a.execute(ctx, task)
		res.Duration = time.Since(start)
		cancelled := ctx.Err() == context.Canceled
		cancel()
		a.mu.Lock()
		a.used.CPUMillis -= task.Resources.CPUMillis
		a.used.MemoryMB -= task.Resources.MemoryMB
		for _, id := range task.GPUIDs {
			delete(a.gpuBusy, id)
		}
		a.mu.Unlock()
		// The attempt stays in a.running until the report is acknowledged, so heartbeats
		// keep listing it and a new leader doesn't hand it out again in the meantime.
		if !cancelled || res.Success {
			a.report(res)
		} // else: the scheduler took the attempt back; nothing to report
		a.mu.Lock()
		delete(a.running, key)
		a.mu.Unlock()
	}()
}

func (a *Agent) hasGPU(id string) bool {
	for _, g := range a.Info.GPUList {
		if g.ID == id {
			return true
		}
	}
	return false
}

func (a *Agent) execute(ctx context.Context, task Task) (res Result) {
	res = Result{JobID: task.JobID, Attempt: task.Attempt}
	defer func() {
		if p := recover(); p != nil {
			res.Success, res.Error = false, fmt.Sprintf("panic: %v", p)
		}
	}()
	run, ok := a.Runners[task.Type]
	if !ok {
		res.Permanent, res.Error = true, "worker can't run type "+task.Type
		return res
	}
	out, err := run(ctx, task)
	switch {
	case err == nil:
		res.Success, res.Output = true, out
	case errors.Is(err, context.DeadlineExceeded):
		res.Error = fmt.Sprintf("timed out after %s", task.Timeout)
	default:
		res.Error = err.Error()
		var p PermanentError
		res.Permanent = errors.As(err, &p)
	}
	return res
}

// report sends a result, retrying on transport errors. A rejection (accepted=false) is
// final: the attempt is stale and its result is dropped by design.
func (a *Agent) report(r Result) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var accepted bool
	err := retry(ctx, a.Log, "complete", func() error {
		var err error
		accepted, err = a.Transport.Complete(ctx, r)
		return err
	})
	switch {
	case err != nil:
		a.Log.Error("could not report result; the lease will expire and the job will be retried",
			"job", r.JobID, "attempt", r.Attempt, "err", err)
	case !accepted:
		a.Log.Warn("result rejected as stale", "job", r.JobID, "attempt", r.Attempt)
	default:
		a.Log.Debug("result accepted", "job", r.JobID, "attempt", r.Attempt, "ok", r.Success)
	}
}

func (a *Agent) drain(runCtx context.Context, stopTasks context.CancelFunc, d time.Duration) error {
	a.mu.Lock()
	a.draining = true
	n := len(a.running)
	a.mu.Unlock()
	a.Log.Info("draining", "running", n, "timeout", d)
	// Tell the scheduler we're draining so it stops sending work.
	sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	_, _ = a.Transport.Sync(sctx, a.state())
	cancel()
	done := make(chan struct{})
	go func() { a.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-time.After(d):
		stopTasks()
		<-done
		return fmt.Errorf("drain timed out; unfinished jobs will be retried elsewhere")
	}
}

func retry(ctx context.Context, log *slog.Logger, op string, f func() error) error {
	backoff := 100 * time.Millisecond
	for {
		err := f()
		if err == nil {
			return nil
		}
		var p PermanentError
		if errors.As(err, &p) {
			return err
		}
		log.Debug("retrying", "op", op, "err", err, "backoff", backoff)
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: %w (last error: %v)", op, ctx.Err(), err)
		case <-time.After(backoff):
		}
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
}
