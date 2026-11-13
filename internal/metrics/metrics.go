// Package metrics holds GopherGrid's Prometheus metrics. Names are prefixed gophergrid_.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// JobsSubmitted counts new jobs (not idempotent replays) by type.
	JobsSubmitted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gophergrid_jobs_submitted_total", Help: "Jobs submitted, by type.",
	}, []string{"type"})

	// JobsFinished counts terminal outcomes; throughput is rate() of this.
	JobsFinished = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gophergrid_jobs_finished_total", Help: "Jobs reaching a terminal state, by type and status.",
	}, []string{"type", "status"})

	// TaskFailures counts failed attempts, including ones that were retried.
	TaskFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gophergrid_task_failures_total", Help: "Failed attempts, by reason (error, lease_expired, worker_dead).",
	}, []string{"reason"})

	// Requeues counts attempts put back on the queue.
	Requeues = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gophergrid_requeues_total", Help: "Jobs put back on the queue, by reason.",
	}, []string{"reason"})

	// StaleCompletions counts reports rejected by the attempt fence (would-be duplicates).
	StaleCompletions = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gophergrid_stale_completions_total", Help: "Completions rejected because the attempt was no longer current.",
	})

	// QueueLatency is time from submission to first assignment.
	QueueLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "gophergrid_queue_latency_seconds", Help: "Submission to first assignment.",
		Buckets: prometheus.ExponentialBuckets(0.005, 2, 16),
	})

	// RunTime is a successful attempt's run time as reported by the worker.
	RunTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "gophergrid_job_run_seconds", Help: "Run time of successful attempts, by type.",
		Buckets: prometheus.ExponentialBuckets(0.005, 2, 16),
	}, []string{"type"})

	// QueueDepth is queued jobs, by priority band.
	QueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gophergrid_queue_depth", Help: "Queued jobs, by priority band (high >= 50, normal, low < 0).",
	}, []string{"band"})

	// RunningJobs is jobs currently running.
	RunningJobs = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gophergrid_running_jobs", Help: "Jobs currently running.",
	})
)

var (
	// IsLeader is 1 on the scheduler instance that currently leads.
	IsLeader = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gophergrid_is_leader", Help: "1 if this scheduler instance is the leader.",
	})

	// WorkersAlive is workers heartbeating (alive or draining).
	WorkersAlive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gophergrid_workers_alive", Help: "Workers that are heartbeating.",
	})

	// DeadWorkers counts workers declared dead after missing heartbeats.
	DeadWorkers = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gophergrid_dead_workers_total", Help: "Workers declared dead after missing heartbeats.",
	})

	// WorkerUtilization is per-worker utilization: reserved CPU and memory (by assigned jobs)
	// and measured CPU use, each 0..1.
	WorkerUtilization = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gophergrid_worker_utilization", Help: "Per-worker utilization, 0..1, by resource.",
	}, []string{"worker", "resource"})

	// Heartbeats counts heartbeats handled by the leader.
	Heartbeats = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gophergrid_heartbeats_total", Help: "Heartbeats handled by the leader.",
	})

	// Assignments counts job attempts assigned to workers.
	Assignments = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gophergrid_assignments_total", Help: "Job attempts assigned to workers.",
	})

	// SchedulingPass is the duration of a scheduling pass that assigned work.
	SchedulingPass = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "gophergrid_scheduling_pass_seconds", Help: "Duration of scheduling passes that assigned work.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14),
	})

	// AssignToStart is time from assignment to the worker receiving it.
	AssignToStart = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "gophergrid_assignment_delivery_seconds", Help: "Assignment commit to delivery in a heartbeat response.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	})
)

// Unschedulable is queued jobs (in the last scheduling pass) that no live worker could run
// even when idle: too much CPU, memory, GPU or GPU memory, or a job type nobody runs.
var Unschedulable = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "gophergrid_unschedulable_jobs", Help: "Queued jobs that no live worker can run even when idle.",
})
