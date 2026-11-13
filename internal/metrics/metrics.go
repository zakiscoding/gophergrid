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
