package sched

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/zakiscoding/gophergrid/internal/jobs"
	"github.com/zakiscoding/gophergrid/internal/metrics"
	"github.com/zakiscoding/gophergrid/internal/store"
)

// Config tunes the leader.
type Config struct {
	ID                string        // this instance, for the leadership row
	SchedInterval     time.Duration // how often to assign queued jobs
	BatchSize         int           // jobs considered per pass
	LeaseTTL          time.Duration // lease length; heartbeats renew it
	HeartbeatTimeout  time.Duration // silence after which a worker is dead
	TimeoutGrace      time.Duration // added to a job's timeout before the reaper takes it back
	MaintenanceEvery  time.Duration // reaper and dead-worker check
	Policy            Policy
	QueueOrder        string // SQL ORDER BY for the queue scan
}

// Defaults fills unset fields.
func (c *Config) Defaults() {
	if c.SchedInterval <= 0 {
		c.SchedInterval = 50 * time.Millisecond
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 256
	}
	if c.LeaseTTL <= 0 {
		c.LeaseTTL = 10 * time.Second
	}
	if c.HeartbeatTimeout <= 0 {
		c.HeartbeatTimeout = 3 * time.Second
	}
	if c.TimeoutGrace <= 0 {
		c.TimeoutGrace = 5 * time.Second
	}
	if c.MaintenanceEvery <= 0 {
		c.MaintenanceEvery = 250 * time.Millisecond
	}
	if c.QueueOrder == "" {
		c.QueueOrder = "j.priority DESC, j.created_at"
	}
}

// QueueOrder returns the queue scan order. With aging > 0 a job gains one priority point
// per aging period it waits, so a steady stream of high-priority work can't starve
// low-priority jobs forever. Zero disables aging and lets Postgres use the queue index.
func QueueOrder(aging time.Duration) string {
	if aging <= 0 {
		return "j.priority DESC, j.created_at"
	}
	return fmt.Sprintf("(j.priority + extract(epoch FROM now() - j.created_at) / %f) DESC, j.created_at", aging.Seconds())
}

// Scheduler is the leader's work loop.
type Scheduler struct {
	Store  *store.Store
	Config Config
	Log    *slog.Logger
	// Notify, if set, is called with each worker that just got assignments, so a waiting
	// heartbeat can return early.
	Notify func(workerID string)

	mu      sync.Mutex
	workers []store.Worker
	loaded  time.Time
}

// Lead runs until ctx is cancelled or leadership is lost.
func (s *Scheduler) Lead(ctx context.Context, epoch int64) {
	if err := s.Store.TakeLeadership(ctx, epoch, s.Config.ID); err != nil {
		s.Log.Error("could not record leadership", "epoch", epoch, "err", err)
		return
	}
	metrics.IsLeader.Set(1)
	defer metrics.IsLeader.Set(0)
	takeover := time.Now()
	s.Log.Info("leading", "epoch", epoch)

	sched := time.NewTicker(s.Config.SchedInterval)
	defer sched.Stop()
	maint := time.NewTicker(s.Config.MaintenanceEvery)
	defer maint.Stop()
	gauges := time.NewTicker(2 * time.Second)
	defer gauges.Stop()

	for {
		var err error
		select {
		case <-ctx.Done():
			return
		case <-sched.C:
			// Keep going while full batches are being assigned.
			for n := s.Config.BatchSize; n == s.Config.BatchSize && err == nil && ctx.Err() == nil; {
				n, err = s.schedule(ctx, epoch)
			}
		case <-maint.C:
			// After a takeover, give workers one heartbeat timeout to find the new leader
			// before treating their silence (and their unrenewed leases) as failure.
			if time.Since(takeover) > s.Config.HeartbeatTimeout {
				err = s.maintain(ctx, epoch)
			}
		case <-gauges.C:
			s.gauges(ctx)
		}
		if errors.Is(err, store.ErrNotLeader) {
			s.Log.Warn("fenced out by a newer leader", "epoch", epoch)
			return
		}
		if err != nil && ctx.Err() == nil {
			s.Log.Error("leader loop", "err", err)
		}
	}
}

func (s *Scheduler) liveWorkers(ctx context.Context) ([]store.Worker, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if time.Since(s.loaded) < 200*time.Millisecond {
		return s.workers, nil
	}
	all, err := s.Store.ListWorkers(ctx)
	if err != nil {
		return nil, err
	}
	s.workers = s.workers[:0]
	for _, w := range all {
		if w.State == "alive" {
			s.workers = append(s.workers, w)
		}
	}
	s.loaded = time.Now()
	return s.workers, nil
}

func (s *Scheduler) schedule(ctx context.Context, epoch int64) (int, error) {
	start := time.Now()
	workers, err := s.liveWorkers(ctx)
	if err != nil || len(workers) == 0 {
		return 0, err
	}
	assigned, err := s.Store.ScheduleBatch(ctx, epoch, s.Config.BatchSize, s.Config.LeaseTTL, s.Config.QueueOrder,
		func(queued []jobs.Job, usage map[string]store.Usage) []store.Assignment {
			nodes := make([]*Node, len(workers))
			for i, w := range workers {
				nodes[i] = NewNode(w, usage[w.ID])
			}
			metrics.Unschedulable.Set(float64(countUnschedulable(queued, workers)))
			return Place(queued, nodes, s.Config.Policy)
		})
	if err != nil {
		return 0, err
	}
	if len(assigned) == 0 {
		return 0, nil
	}
	metrics.SchedulingPass.Observe(time.Since(start).Seconds())
	notified := map[string]bool{}
	for _, a := range assigned {
		metrics.Assignments.Inc()
		if a.Attempt == 1 {
			metrics.QueueLatency.Observe(a.QueueLatency.Seconds())
		}
		if s.Notify != nil && !notified[a.WorkerID] {
			notified[a.WorkerID] = true
			s.Notify(a.WorkerID)
		}
	}
	return len(assigned), nil
}

func (s *Scheduler) maintain(ctx context.Context, epoch int64) error {
	dead, lost, err := s.Store.MarkDeadWorkers(ctx, epoch, s.Config.HeartbeatTimeout)
	if err != nil {
		return err
	}
	if len(dead) > 0 {
		s.Log.Warn("workers missed heartbeats; declared dead", "workers", dead, "jobs_to_retry", lost)
		metrics.DeadWorkers.Add(float64(len(dead)))
		s.mu.Lock()
		s.loaded = time.Time{} // reload the worker list now
		s.mu.Unlock()
	}
	r, err := s.Store.ReapExpiredFenced(ctx, epoch, s.Config.TimeoutGrace)
	if err != nil {
		return err
	}
	if r.Requeued+r.Failed > 0 {
		s.Log.Info("reaped expired leases", "requeued", r.Requeued, "failed", r.Failed)
		metrics.TaskFailures.WithLabelValues("lease_expired").Add(float64(r.Requeued + r.Failed))
		metrics.Requeues.WithLabelValues("lease_expired").Add(float64(r.Requeued))
		metrics.JobsFinished.WithLabelValues("unknown", "failed").Add(float64(r.Failed))
	}
	return nil
}

func (s *Scheduler) gauges(ctx context.Context) {
	var high, normal, low, running int
	err := s.Store.Pool.QueryRow(ctx, `SELECT
		count(*) FILTER (WHERE status = 'queued' AND priority >= 50),
		count(*) FILTER (WHERE status = 'queued' AND priority >= 0 AND priority < 50),
		count(*) FILTER (WHERE status = 'queued' AND priority < 0),
		count(*) FILTER (WHERE status = 'running')
		FROM jobs WHERE status IN ('queued', 'running')`).Scan(&high, &normal, &low, &running)
	if err == nil {
		metrics.QueueDepth.WithLabelValues("high").Set(float64(high))
		metrics.QueueDepth.WithLabelValues("normal").Set(float64(normal))
		metrics.QueueDepth.WithLabelValues("low").Set(float64(low))
		metrics.RunningJobs.Set(float64(running))
	}
	workers, err := s.Store.ListWorkers(ctx)
	if err != nil {
		return
	}
	alive := 0
	metrics.WorkerUtilization.Reset()
	for _, w := range workers {
		if w.State == "dead" {
			continue
		}
		alive++
		metrics.WorkerUtilization.WithLabelValues(w.ID, "cpu_reserved").Set(1 - float64(w.FreeCPUMillis)/float64(max(w.CPUMillis, 1)))
		metrics.WorkerUtilization.WithLabelValues(w.ID, "memory_reserved").Set(1 - float64(w.FreeMemoryMB)/float64(max(w.MemoryMB, 1)))
		metrics.WorkerUtilization.WithLabelValues(w.ID, "cpu_used").Set(w.CPUUtil)
	}
	metrics.WorkersAlive.Set(float64(alive))
}

// countUnschedulable counts jobs that wouldn't fit on any live worker even if it were
// empty: they will wait until a bigger worker joins.
func countUnschedulable(queued []jobs.Job, workers []store.Worker) int {
	n := 0
	for _, j := range queued {
		fits := false
		for _, w := range workers {
			if _, ok := NewNode(w, store.Usage{}).fits(j); ok {
				fits = true
				break
			}
		}
		if !fits {
			n++
		}
	}
	return n
}
