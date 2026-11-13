// Package store is GopherGrid's PostgreSQL persistence: the job queue, leases, attempts
// and results.
package store

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/zakiscoding/gophergrid/internal/jobs"
)

//go:embed migrations/*.sql
var migrations embed.FS

// ErrNotFound is returned when a job doesn't exist.
var ErrNotFound = errors.New("not found")

// Store wraps a pgx connection pool.
type Store struct {
	Pool *pgxpool.Pool
}

// Open connects to Postgres, retrying for up to wait while the database starts.
func Open(ctx context.Context, dsn string, maxConns int32, wait time.Duration) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	deadline := time.Now().Add(wait)
	for {
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err == nil {
			if err = pool.Ping(ctx); err == nil {
				return &Store{Pool: pool}, nil
			}
			pool.Close()
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("connect to postgres: %w", err)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// Close closes the pool.
func (s *Store) Close() { s.Pool.Close() }

// Migrate applies embedded migrations in order. Several schedulers may start at once, so
// it holds a Postgres advisory lock while it runs.
func (s *Store) Migrate(ctx context.Context) error {
	conn, err := s.Pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	const lockID = 0x6770_6772_6964 // "gpgrid"
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, lockID); err != nil {
		return err
	}
	defer conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, lockID) //nolint:errcheck

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	names, err := fs.Glob(migrations, "migrations/*.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)
	for _, name := range names {
		version := strings.TrimSuffix(strings.TrimPrefix(name, "migrations/"), ".sql")
		var done bool
		if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`, version).Scan(&done); err != nil {
			return err
		}
		if done {
			continue
		}
		sql, err := migrations.ReadFile(name)
		if err != nil {
			return err
		}
		err = pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, string(sql)); err != nil {
				return fmt.Errorf("migration %s: %w", version, err)
			}
			_, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, version)
			return err
		})
		if err != nil {
			return err
		}
	}
	return nil
}

const jobColumns = `j.id, j.idempotency_key, j.type, j.payload, j.priority, j.cpu_millis, j.memory_mb,
	j.gpus, j.gpu_memory_mb, j.timeout_ms, j.max_attempts, j.status, j.attempt, j.worker_id,
	j.lease_expires_at, j.last_error, j.created_at, j.assigned_at, j.started_at, j.finished_at, j.gpu_ids, j.batch_id`

func scanJob(row pgx.Row, extra ...any) (jobs.Job, error) {
	var j jobs.Job
	var status string
	dest := []any{&j.ID, &j.IdempotencyKey, &j.Type, &j.Payload, &j.Priority, &j.Resources.CPUMillis,
		&j.Resources.MemoryMB, &j.Resources.GPUs, &j.Resources.GPUMemoryMB, &j.TimeoutMS, &j.MaxAttempts,
		&status, &j.Attempt, &j.WorkerID, &j.LeaseExpiresAt, &j.LastError, &j.CreatedAt, &j.AssignedAt,
		&j.StartedAt, &j.FinishedAt, &j.GPUIDs, &j.BatchID}
	err := row.Scan(append(dest, extra...)...)
	if errors.Is(err, pgx.ErrNoRows) {
		return j, ErrNotFound
	}
	j.Status = jobs.Status(status)
	return j, err
}

// Submit stores a new queued job. With an idempotency key that already exists, it returns
// the existing job and created=false.
func (s *Store) Submit(ctx context.Context, spec jobs.Spec) (jobs.Job, bool, error) {
	var key *string
	if spec.IdempotencyKey != "" {
		key = &spec.IdempotencyKey
	}
	row := s.Pool.QueryRow(ctx, `
		INSERT INTO jobs AS j (id, idempotency_key, type, payload, priority, cpu_millis, memory_mb,
		                       gpus, gpu_memory_mb, timeout_ms, max_attempts)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING `+jobColumns,
		uuid.NewString(), key, spec.Type, spec.Payload, spec.Priority, spec.Resources.CPUMillis,
		spec.Resources.MemoryMB, spec.Resources.GPUs, spec.Resources.GPUMemoryMB, spec.TimeoutMS, spec.MaxAttempts)
	j, err := scanJob(row)
	if err == nil {
		return j, true, nil
	}
	if !errors.Is(err, ErrNotFound) || key == nil {
		return j, false, err
	}
	// Conflict on the idempotency key: return the original.
	j, err = scanJob(s.Pool.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs j WHERE j.idempotency_key = $1`, *key))
	return j, false, err
}

// Get returns a job with its result, if any.
func (s *Store) Get(ctx context.Context, id string) (jobs.Job, error) {
	if _, err := uuid.Parse(id); err != nil {
		return jobs.Job{}, ErrNotFound
	}
	var result []byte
	j, err := scanJob(s.Pool.QueryRow(ctx, `SELECT `+jobColumns+`, r.output
		FROM jobs j LEFT JOIN results r ON r.job_id = j.id WHERE j.id = $1`, id), &result)
	if len(result) > 0 {
		j.Result = result
	}
	return j, err
}

// List returns up to limit jobs, newest first, optionally filtered by status.
func (s *Store) List(ctx context.Context, status string, limit int) ([]jobs.Job, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := s.Pool.Query(ctx, `SELECT `+jobColumns+` FROM jobs j
		WHERE ($1 = '' OR j.status = $1) ORDER BY j.created_at DESC LIMIT $2`, status, limit)
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

// LeaseGrace is added to a job's timeout to get its lease length when no heartbeats renew it.
const LeaseGrace = 5 * time.Second

// Claim atomically takes the highest-priority queued job that fits in free, for workerID.
// It returns ErrNotFound when nothing fits. SKIP LOCKED lets many workers claim at once
// without blocking on each other's rows.
func (s *Store) Claim(ctx context.Context, workerID string, free jobs.Resources, lease time.Duration) (jobs.Job, error) {
	var j jobs.Job
	err := pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		var err error
		j, err = scanJob(tx.QueryRow(ctx, `
			WITH next AS (
				SELECT id FROM jobs
				WHERE status = 'queued' AND cpu_millis <= $2 AND memory_mb <= $3 AND gpus <= $4
				ORDER BY priority DESC, created_at
				LIMIT 1
				FOR UPDATE SKIP LOCKED
			)
			UPDATE jobs j SET status = 'running', attempt = j.attempt + 1, worker_id = $1,
				lease_expires_at = now() + make_interval(secs => j.timeout_ms / 1000.0::float8 + $5::float8),
				assigned_at = coalesce(j.assigned_at, now()), started_at = now()
			FROM next WHERE j.id = next.id
			RETURNING `+jobColumns,
			workerID, free.CPUMillis, free.MemoryMB, free.GPUs, lease.Seconds()))
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO attempts (job_id, attempt, worker_id) VALUES ($1, $2, $3)`,
			j.ID, j.Attempt, workerID)
		return err
	})
	return j, err
}

// Completion is a worker's report for one attempt.
type Completion struct {
	JobID     string
	Attempt   int
	WorkerID  string
	Success   bool
	Permanent bool // failure that retrying won't fix
	Output    json.RawMessage
	Error     string
}

// Outcome says what Complete did.
type Outcome struct {
	Accepted  bool
	Reason    string      // why it was rejected
	NewStatus jobs.Status // status after the call, when accepted
	Requeued  bool
	Type      string // the job's type, for metrics
}

// Complete records an attempt's result. It is idempotent and fenced:
//   - The job row is locked, and the report is accepted only if the job is still running
//     the same attempt on the same worker. A worker whose lease expired and whose job was
//     reassigned gets Accepted=false and its output is dropped.
//   - On success the result goes into results, whose primary key is the job id, so a job
//     can never have two results even if two reports race.
func (s *Store) Complete(ctx context.Context, c Completion) (Outcome, error) {
	var out Outcome
	err := pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		var status string
		var attempt, maxAttempts int
		var worker *string
		err := tx.QueryRow(ctx, `SELECT status, attempt, worker_id, max_attempts, type FROM jobs WHERE id = $1 FOR UPDATE`,
			c.JobID).Scan(&status, &attempt, &worker, &maxAttempts, &out.Type)
		if errors.Is(err, pgx.ErrNoRows) {
			out.Reason = "unknown job"
			return nil
		}
		if err != nil {
			return err
		}
		if status != string(jobs.StatusRunning) || attempt != c.Attempt || worker == nil || *worker != c.WorkerID {
			out.Reason = fmt.Sprintf("stale attempt: job is %s, attempt %d", status, attempt)
			_, err := tx.Exec(ctx, `UPDATE attempts SET ended_at = coalesce(ended_at, now()),
				outcome = coalesce(outcome, 'rejected') WHERE job_id = $1 AND attempt = $2`, c.JobID, c.Attempt)
			return err
		}
		out.Accepted = true
		if c.Success {
			output := c.Output
			if len(output) == 0 || !json.Valid(output) {
				output = json.RawMessage(`null`)
			}
			tag, err := tx.Exec(ctx, `INSERT INTO results (job_id, attempt, worker_id, output) VALUES ($1, $2, $3, $4)
				ON CONFLICT (job_id) DO NOTHING`, c.JobID, c.Attempt, c.WorkerID, output)
			if err != nil {
				return err
			}
			if tag.RowsAffected() == 0 {
				// Can't happen while the fencing check above holds; kept as a backstop.
				out.Accepted, out.Reason = false, "result already recorded"
				return nil
			}
			out.NewStatus = jobs.StatusSucceeded
		} else if c.Permanent || attempt >= maxAttempts {
			out.NewStatus = jobs.StatusFailed
		} else {
			out.NewStatus, out.Requeued = jobs.StatusQueued, true
		}
		outcome := "succeeded"
		if !c.Success {
			outcome = "failed"
		}
		if _, err := tx.Exec(ctx, `UPDATE attempts SET ended_at = now(), outcome = $3, error = nullif($4, '')
			WHERE job_id = $1 AND attempt = $2`, c.JobID, c.Attempt, outcome, c.Error); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE jobs SET status = $2,
				worker_id = CASE WHEN $3 THEN NULL ELSE worker_id END,
				gpu_ids = CASE WHEN $3 THEN NULL ELSE gpu_ids END,
				lease_expires_at = NULL, last_error = nullif($4, ''),
				finished_at = CASE WHEN $3 THEN NULL ELSE now() END
			WHERE id = $1`, c.JobID, string(out.NewStatus), out.Requeued, c.Error)
		return err
	})
	return out, err
}

// Reaped counts what ReapExpired did.
type Reaped struct{ Requeued, Failed int }

// ReapExpired requeues running jobs whose lease has expired, or fails them once they're
// out of attempts. The worker that held the lease may still be running the job; if it
// reports later, Complete rejects it as a stale attempt.
func (s *Store) ReapExpired(ctx context.Context) (Reaped, error) {
	var r Reaped
	rows, err := s.Pool.Query(ctx, reapSQL(`lease_expires_at < now()`))
	if err != nil {
		return r, err
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		if err := rows.Scan(&status); err != nil {
			return r, err
		}
		if status == string(jobs.StatusFailed) {
			r.Failed++
		} else {
			r.Requeued++
		}
	}
	return r, rows.Err()
}

// reapSQL requeues (or fails, when out of attempts) running jobs matching cond, marks
// their attempts lease_expired, and returns each job's new status.
func reapSQL(cond string) string {
	return `
		WITH expired AS (
			SELECT id FROM jobs WHERE status = 'running' AND (` + cond + `)
			FOR UPDATE SKIP LOCKED
		), updated AS (
			UPDATE jobs j SET
				status = CASE WHEN j.attempt >= j.max_attempts THEN 'failed' ELSE 'queued' END,
				finished_at = CASE WHEN j.attempt >= j.max_attempts THEN now() ELSE NULL END,
				last_error = 'lease expired on worker ' || coalesce(j.worker_id, '?'),
				worker_id = NULL, lease_expires_at = NULL, gpu_ids = NULL
			FROM expired WHERE j.id = expired.id
			RETURNING j.id, j.attempt, j.status
		), marked AS (
			UPDATE attempts a SET ended_at = now(), outcome = 'lease_expired'
			FROM updated u WHERE a.job_id = u.id AND a.attempt = u.attempt AND a.outcome IS NULL
		)
		SELECT status FROM updated`
}

// Stats is a snapshot of the queue.
type Stats struct {
	ByStatus          map[string]int `json:"by_status"`
	Results           int            `json:"results"`
	Reassigned        int            `json:"reassigned_jobs"` // jobs that needed more than one attempt
	RejectedAttempts  int            `json:"rejected_attempts"`
	AvgQueueLatencyMS float64        `json:"avg_queue_latency_ms"`
}

// Stats summarizes the queue.
func (s *Store) Stats(ctx context.Context) (Stats, error) {
	st := Stats{ByStatus: map[string]int{}}
	rows, err := s.Pool.Query(ctx, `SELECT status, count(*) FROM jobs GROUP BY status`)
	if err != nil {
		return st, err
	}
	for rows.Next() {
		var k string
		var n int
		if err := rows.Scan(&k, &n); err != nil {
			rows.Close()
			return st, err
		}
		st.ByStatus[k] = n
	}
	rows.Close()
	err = s.Pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM results),
		(SELECT count(*) FROM jobs WHERE attempt > 1),
		(SELECT count(*) FROM attempts WHERE outcome = 'rejected'),
		(SELECT coalesce(avg(extract(epoch FROM assigned_at - created_at)) * 1000, 0) FROM jobs WHERE assigned_at IS NOT NULL)`).
		Scan(&st.Results, &st.Reassigned, &st.RejectedAttempts, &st.AvgQueueLatencyMS)
	return st, err
}

// IsUniqueViolation reports whether err is a Postgres unique-constraint error.
func IsUniqueViolation(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && pg.Code == "23505"
}
