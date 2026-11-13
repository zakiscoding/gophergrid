package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/zakiscoding/gophergrid/internal/jobs"
)

// ErrNotLeader means a newer leader has taken over; the caller must stop leader work.
var ErrNotLeader = errors.New("leadership lost: a newer epoch holds the lease table")

// TakeLeadership records epoch as current. It fails with ErrNotLeader if a newer epoch is
// already recorded. Because leader transactions hold the row FOR SHARE, this waits for the
// old leader's in-flight writes to finish, and every write it starts afterwards fails.
func (s *Store) TakeLeadership(ctx context.Context, epoch int64, holder string) error {
	tag, err := s.Pool.Exec(ctx, `
		INSERT INTO leadership (id, epoch, holder) VALUES (1, $1, $2)
		ON CONFLICT (id) DO UPDATE SET epoch = EXCLUDED.epoch, holder = EXCLUDED.holder, elected_at = now()
		WHERE leadership.epoch < EXCLUDED.epoch`, epoch, holder)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotLeader
	}
	return nil
}

// leaderTx runs f in a transaction that first checks epoch is still current.
func (s *Store) leaderTx(ctx context.Context, epoch int64, f func(pgx.Tx) error) error {
	return pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		var current int64
		err := tx.QueryRow(ctx, `SELECT epoch FROM leadership WHERE id = 1 FOR SHARE`).Scan(&current)
		if errors.Is(err, pgx.ErrNoRows) || (err == nil && current != epoch) {
			return ErrNotLeader
		}
		if err != nil {
			return err
		}
		return f(tx)
	})
}

// GPUInfo is a GPU as stored in the worker registry.
type GPUInfo struct {
	ID       string `json:"id"`
	Model    string `json:"model"`
	MemoryMB int64  `json:"memory_mb"`
}

// Worker is a row of the worker registry.
type Worker struct {
	ID            string    `json:"id"`
	Hostname      string    `json:"hostname"`
	CPUMillis     int64     `json:"cpu_millis"`
	MemoryMB      int64     `json:"memory_mb"`
	GPUs          []GPUInfo `json:"gpus"`
	GPUMode       string    `json:"gpu_mode"`
	JobTypes      []string  `json:"job_types"`
	State         string    `json:"state"`
	RegisteredAt  time.Time `json:"registered_at"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	FreeCPUMillis int64     `json:"free_cpu_millis"`
	FreeMemoryMB  int64     `json:"free_memory_mb"`
	Running       int       `json:"running"`
	CPUUtil       float64   `json:"cpu_utilization"`
}

// UpsertWorker registers a worker, or re-registers it (alive again) with new capacity.
func (s *Store) UpsertWorker(ctx context.Context, w Worker) error {
	gpus, _ := json.Marshal(w.GPUs)
	if w.GPUs == nil {
		gpus = []byte("[]")
	}
	_, err := s.Pool.Exec(ctx, `
		INSERT INTO workers (id, hostname, cpu_millis, memory_mb, gpus, gpu_mode, job_types,
		                     free_cpu_millis, free_memory_mb)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $3, $4)
		ON CONFLICT (id) DO UPDATE SET hostname = EXCLUDED.hostname, cpu_millis = EXCLUDED.cpu_millis,
			memory_mb = EXCLUDED.memory_mb, gpus = EXCLUDED.gpus, gpu_mode = EXCLUDED.gpu_mode,
			job_types = EXCLUDED.job_types, state = 'alive', last_heartbeat = now(), registered_at = now()`,
		w.ID, w.Hostname, w.CPUMillis, w.MemoryMB, gpus, w.GPUMode, w.JobTypes)
	return err
}

// TouchWorker records a heartbeat. It returns false if the worker is unknown or was
// declared dead; the worker must register again.
func (s *Store) TouchWorker(ctx context.Context, id string, free jobs.Resources, running int, util float64, draining bool) (bool, error) {
	state := "alive"
	if draining {
		state = "draining"
	}
	tag, err := s.Pool.Exec(ctx, `UPDATE workers SET last_heartbeat = now(), free_cpu_millis = $2,
		free_memory_mb = $3, running = $4, cpu_utilization = $5, state = $6
		WHERE id = $1 AND state <> 'dead'`, id, free.CPUMillis, free.MemoryMB, running, util, state)
	return tag.RowsAffected() == 1, err
}

// ListWorkers returns the registry, live workers first.
func (s *Store) ListWorkers(ctx context.Context) ([]Worker, error) {
	rows, err := s.Pool.Query(ctx, `SELECT id, hostname, cpu_millis, memory_mb, gpus, gpu_mode, job_types,
		state, registered_at, last_heartbeat, free_cpu_millis, free_memory_mb, running, cpu_utilization
		FROM workers ORDER BY state = 'dead', id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Worker
	for rows.Next() {
		var w Worker
		var gpus []byte
		if err := rows.Scan(&w.ID, &w.Hostname, &w.CPUMillis, &w.MemoryMB, &gpus, &w.GPUMode, &w.JobTypes,
			&w.State, &w.RegisteredAt, &w.LastHeartbeat, &w.FreeCPUMillis, &w.FreeMemoryMB, &w.Running, &w.CPUUtil); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(gpus, &w.GPUs)
		out = append(out, w)
	}
	return out, rows.Err()
}

// MarkDeadWorkers declares workers dead after timeout without a heartbeat and expires
// their leases at once, so the reaper requeues their jobs on its next pass instead of
// waiting out the lease TTL.
func (s *Store) MarkDeadWorkers(ctx context.Context, epoch int64, timeout time.Duration) (dead []string, jobsLost int, err error) {
	err = s.leaderTx(ctx, epoch, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `UPDATE workers SET state = 'dead'
			WHERE state <> 'dead' AND last_heartbeat < now() - make_interval(secs => $1::float8) RETURNING id`, timeout.Seconds())
		if err != nil {
			return err
		}
		dead, err = pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return err
		}
		// Forget workers that have been gone for an hour (autoscaled and drained, or retired).
		if _, err := tx.Exec(ctx, `DELETE FROM workers WHERE state = 'dead' AND last_heartbeat < now() - interval '1 hour'`); err != nil {
			return err
		}
		if len(dead) == 0 {
			return nil
		}
		tag, err := tx.Exec(ctx, `UPDATE jobs SET lease_expires_at = now() - interval '1 millisecond'
			WHERE status = 'running' AND worker_id = ANY($1)`, dead)
		jobsLost = int(tag.RowsAffected())
		return err
	})
	return dead, jobsLost, err
}

// Held is an attempt a worker reports holding.
type Held struct {
	JobID   string
	Attempt int
}

// RenewLeases extends the leases of the attempts a worker reports, if they're still the
// current attempt on that worker. It returns how many were renewed.
func (s *Store) RenewLeases(ctx context.Context, workerID string, held []Held, ttl time.Duration) (int, error) {
	if len(held) == 0 {
		return 0, nil
	}
	ids := make([]string, len(held))
	attempts := make([]int32, len(held))
	for i, h := range held {
		ids[i], attempts[i] = h.JobID, int32(h.Attempt)
	}
	tag, err := s.Pool.Exec(ctx, `UPDATE jobs j SET lease_expires_at = now() + make_interval(secs => $4::float8)
		FROM unnest($2::uuid[], $3::int[]) AS h(id, attempt)
		WHERE j.id = h.id AND j.attempt = h.attempt AND j.worker_id = $1 AND j.status = 'running'`,
		workerID, ids, attempts, ttl.Seconds())
	return int(tag.RowsAffected()), err
}

// HeldBy returns the jobs currently assigned to a worker.
func (s *Store) HeldBy(ctx context.Context, workerID string) ([]jobs.Job, error) {
	rows, err := s.Pool.Query(ctx, `SELECT `+jobColumns+` FROM jobs j WHERE j.status = 'running' AND j.worker_id = $1`, workerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []jobs.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// Usage is what running jobs hold on one worker.
type Usage struct {
	jobs.Resources
	GPUIDs []string
}

// Assignment places one job on one worker.
type Assignment struct {
	JobID    string
	WorkerID string
	GPUIDs   []string
}

// Assigned is a committed assignment.
type Assigned struct {
	Assignment
	Attempt      int
	QueueLatency time.Duration // only for first attempts
	Type         string
}

// PlaceFunc decides placements for a batch of queued jobs, given current usage per worker.
type PlaceFunc func(queued []jobs.Job, usage map[string]Usage) []Assignment

// QueueSQL selects the next $1 queued jobs in queue order.
//
// Without aging the order is (priority DESC, created_at), served straight from
// jobs_queued_idx. With aging, a job's score is priority + wait/aging, which no index can
// order directly, and sorting the whole queue every pass took 25 ms at 20k queued jobs.
// But within one priority level the oldest job always scores highest, so the global top N
// is always among the N oldest jobs of each priority level present. The query finds those
// levels with a skip scan, takes N per level from the index, and sorts only that small set:
// the same result as the full sort, at index cost. TestAgingQueueMatchesNaiveOrder checks
// that the two orders match.
func QueueSQL(aging time.Duration, lock bool) string {
	suffix := ""
	if lock {
		suffix = " FOR UPDATE OF j SKIP LOCKED"
	}
	if aging <= 0 {
		return `SELECT ` + jobColumns + ` FROM jobs j WHERE j.status = 'queued'
			ORDER BY j.priority DESC, j.created_at LIMIT $1` + suffix
	}
	score := fmt.Sprintf("(j.priority + extract(epoch FROM now() - j.created_at) / %f)", aging.Seconds())
	return `WITH RECURSIVE levels(p) AS (
			(SELECT priority FROM jobs WHERE status = 'queued' ORDER BY priority DESC LIMIT 1)
			UNION ALL
			SELECT (SELECT priority FROM jobs WHERE status = 'queued' AND priority < l.p ORDER BY priority DESC LIMIT 1)
			FROM levels l WHERE l.p IS NOT NULL
		), top AS (
			SELECT c.id FROM levels l CROSS JOIN LATERAL (
				SELECT id, priority, created_at FROM jobs
				WHERE status = 'queued' AND priority = l.p ORDER BY created_at LIMIT $1) c
			WHERE l.p IS NOT NULL
			ORDER BY ` + strings.ReplaceAll(score, "j.", "c.") + ` DESC, c.created_at LIMIT $1
		)
		SELECT ` + jobColumns + ` FROM jobs j WHERE j.id = ANY(ARRAY(SELECT id FROM top)) AND j.status = 'queued'
		ORDER BY ` + score + ` DESC, j.created_at` + suffix
}

// ScheduleBatch takes up to limit queued jobs, asks place where they go, and commits the
// assignments with fresh leases, all in one fenced transaction.
func (s *Store) ScheduleBatch(ctx context.Context, epoch int64, limit int, leaseTTL, aging time.Duration, place PlaceFunc) ([]Assigned, error) {
	var out []Assigned
	err := s.leaderTx(ctx, epoch, func(tx pgx.Tx) error {
		out = out[:0]
		rows, err := tx.Query(ctx, QueueSQL(aging, true), limit)
		if err != nil {
			return err
		}
		var queued []jobs.Job
		for rows.Next() {
			j, err := scanJob(rows)
			if err != nil {
				rows.Close()
				return err
			}
			queued = append(queued, j)
		}
		rows.Close()
		if err := rows.Err(); err != nil || len(queued) == 0 {
			return err
		}
		usage, err := usageTx(ctx, tx)
		if err != nil {
			return err
		}
		as := place(queued, usage)
		if len(as) == 0 {
			return nil
		}
		ids := make([]string, len(as))
		workers := make([]string, len(as))
		gpuIDs := make([]string, len(as)) // comma-joined; arrays of arrays don't unnest cleanly
		for i, a := range as {
			ids[i], workers[i], gpuIDs[i] = a.JobID, a.WorkerID, joinIDs(a.GPUIDs)
		}
		rows, err = tx.Query(ctx, `
			UPDATE jobs j SET status = 'running', attempt = j.attempt + 1, worker_id = a.worker,
				gpu_ids = CASE WHEN a.gpus = '' THEN NULL ELSE string_to_array(a.gpus, ',') END,
				lease_expires_at = now() + make_interval(secs => $4::float8),
				assigned_at = coalesce(j.assigned_at, now()), started_at = now()
			FROM unnest($1::uuid[], $2::text[], $3::text[]) AS a(id, worker, gpus)
			WHERE j.id = a.id AND j.status = 'queued'
			RETURNING j.id, j.worker_id, j.attempt, j.type, coalesce(j.gpu_ids, '{}'),
				CASE WHEN j.attempt = 1 THEN extract(epoch FROM j.assigned_at - j.created_at) ELSE 0 END`,
			ids, workers, gpuIDs, leaseTTL.Seconds())
		if err != nil {
			return err
		}
		for rows.Next() {
			var a Assigned
			var lat float64
			if err := rows.Scan(&a.JobID, &a.WorkerID, &a.Attempt, &a.Type, &a.GPUIDs, &lat); err != nil {
				rows.Close()
				return err
			}
			a.QueueLatency = time.Duration(lat * float64(time.Second))
			out = append(out, a)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		aIDs := make([]string, len(out))
		aAttempts := make([]int32, len(out))
		aWorkers := make([]string, len(out))
		aGPUs := make([]string, len(out))
		for i, a := range out {
			aIDs[i], aAttempts[i], aWorkers[i], aGPUs[i] = a.JobID, int32(a.Attempt), a.WorkerID, joinIDs(a.GPUIDs)
		}
		_, err = tx.Exec(ctx, `INSERT INTO attempts (job_id, attempt, worker_id, gpu_ids)
			SELECT id, attempt, worker, CASE WHEN gpus = '' THEN NULL ELSE string_to_array(gpus, ',') END
			FROM unnest($1::uuid[], $2::int[], $3::text[], $4::text[]) AS a(id, attempt, worker, gpus)`,
			aIDs, aAttempts, aWorkers, aGPUs)
		return err
	})
	return out, err
}

func usageTx(ctx context.Context, tx pgx.Tx) (map[string]Usage, error) {
	rows, err := tx.Query(ctx, `SELECT j.worker_id, sum(j.cpu_millis)::bigint, sum(j.memory_mb)::bigint, sum(j.gpus)::bigint,
		coalesce((SELECT array_agg(g) FROM jobs j2, unnest(j2.gpu_ids) AS g
		          WHERE j2.status = 'running' AND j2.worker_id = j.worker_id), '{}')
		FROM jobs j WHERE j.status = 'running' AND j.worker_id IS NOT NULL GROUP BY j.worker_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	usage := map[string]Usage{}
	for rows.Next() {
		var w string
		var u Usage
		var gpus int64
		if err := rows.Scan(&w, &u.CPUMillis, &u.MemoryMB, &gpus, &u.GPUIDs); err != nil {
			return nil, err
		}
		u.GPUs = int(gpus)
		usage[w] = u
	}
	return usage, rows.Err()
}

func joinIDs(ids []string) string {
	out := ""
	for i, id := range ids {
		if i > 0 {
			out += ","
		}
		out += id
	}
	return out
}

// ReapExpiredFenced is ReapExpired as the leader: it also times out attempts that ran past
// their job timeout plus grace even though heartbeats kept renewing them (a stuck worker).
func (s *Store) ReapExpiredFenced(ctx context.Context, epoch int64, grace time.Duration) (Reaped, error) {
	var r Reaped
	err := s.leaderTx(ctx, epoch, func(tx pgx.Tx) error {
		r = Reaped{}
		rows, err := tx.Query(ctx, reapSQL(`lease_expires_at < now()
			OR started_at + make_interval(secs => timeout_ms / 1000.0::float8 + $1::float8) < now()`), grace.Seconds())
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var status string
			if err := rows.Scan(&status); err != nil {
				return err
			}
			if status == string(jobs.StatusFailed) {
				r.Failed++
			} else {
				r.Requeued++
			}
		}
		return rows.Err()
	})
	return r, err
}
