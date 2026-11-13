package store

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/zakiscoding/gophergrid/internal/jobs"
)

// Batch is a fan-out of shard jobs, e.g. a large inference request split across GPUs.
type Batch struct {
	ID         string          `json:"id"`
	Kind       string          `json:"kind"`
	Spec       json.RawMessage `json:"spec"`
	Shards     int             `json:"shards"`
	CreatedAt  time.Time       `json:"created_at"`
	ByStatus   map[string]int  `json:"by_status"`
	Done       bool            `json:"done"`
	Tokens     int64           `json:"generated_tokens"`
	Workers    []string        `json:"workers"`    // workers that produced results
	GPUs       []string        `json:"gpus"`       // GPUs that produced results
	Retried    int             `json:"retried_shards"`
	FinishedAt *time.Time      `json:"finished_at,omitempty"`
	WallMS     int64           `json:"wall_ms,omitempty"` // created to last shard finished
}

// CreateBatch stores a batch and its shard jobs in one transaction. A repeated idempotency
// key returns the existing batch and created=false.
func (s *Store) CreateBatch(ctx context.Context, kind string, spec any, key string, shards []jobs.Spec) (string, bool, error) {
	specJSON, err := json.Marshal(spec)
	if err != nil {
		return "", false, err
	}
	var keyPtr *string
	if key != "" {
		keyPtr = &key
	}
	id := uuid.NewString()
	created := true
	err = pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `INSERT INTO batches (id, idempotency_key, kind, spec, shards) VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (idempotency_key) DO NOTHING`, id, keyPtr, kind, specJSON, len(shards))
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			created = false
			return tx.QueryRow(ctx, `SELECT id FROM batches WHERE idempotency_key = $1`, key).Scan(&id)
		}
		rows := make([][]any, len(shards))
		for i, sp := range shards {
			var k *string
			if key != "" {
				kk := key + "/" + strconv.Itoa(i)
				k = &kk
			}
			rows[i] = []any{uuid.NewString(), k, sp.Type, []byte(sp.Payload), sp.Priority, sp.Resources.CPUMillis,
				sp.Resources.MemoryMB, sp.Resources.GPUs, sp.Resources.GPUMemoryMB, sp.TimeoutMS, sp.MaxAttempts, id}
		}
		_, err = tx.CopyFrom(ctx, pgx.Identifier{"jobs"}, []string{"id", "idempotency_key", "type", "payload", "priority",
			"cpu_millis", "memory_mb", "gpus", "gpu_memory_mb", "timeout_ms", "max_attempts", "batch_id"}, pgx.CopyFromRows(rows))
		return err
	})
	return id, created, err
}

// GetBatch returns a batch with its progress.
func (s *Store) GetBatch(ctx context.Context, id string) (Batch, error) {
	var b Batch
	if _, err := uuid.Parse(id); err != nil {
		return b, ErrNotFound
	}
	err := s.Pool.QueryRow(ctx, `SELECT id, kind, spec, shards, created_at FROM batches WHERE id = $1`, id).
		Scan(&b.ID, &b.Kind, &b.Spec, &b.Shards, &b.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return b, ErrNotFound
	}
	if err != nil {
		return b, err
	}
	b.ByStatus = map[string]int{}
	rows, err := s.Pool.Query(ctx, `SELECT status, count(*) FROM jobs WHERE batch_id = $1 GROUP BY status`, id)
	if err != nil {
		return b, err
	}
	for rows.Next() {
		var st string
		var n int
		if err := rows.Scan(&st, &n); err != nil {
			rows.Close()
			return b, err
		}
		b.ByStatus[st] = n
	}
	rows.Close()
	err = s.Pool.QueryRow(ctx, `SELECT
		coalesce(sum((r.output->>'generated_tokens')::bigint), 0),
		coalesce(array_agg(DISTINCT r.worker_id) FILTER (WHERE r.worker_id IS NOT NULL), '{}'),
		coalesce((SELECT array_agg(DISTINCT a.worker_id || '/' || g)
		          FROM jobs j2 JOIN attempts a ON a.job_id = j2.id CROSS JOIN LATERAL unnest(a.gpu_ids) AS g
		          WHERE j2.batch_id = $1 AND a.outcome = 'succeeded'), '{}'),
		(SELECT count(*) FROM jobs WHERE batch_id = $1 AND attempt > 1),
		max(j.finished_at)
		FROM jobs j LEFT JOIN results r ON r.job_id = j.id WHERE j.batch_id = $1`, id).
		Scan(&b.Tokens, &b.Workers, &b.GPUs, &b.Retried, &b.FinishedAt)
	if err != nil {
		return b, err
	}
	b.Done = b.ByStatus["queued"]+b.ByStatus["running"] == 0
	if b.Done && b.FinishedAt != nil {
		b.WallMS = b.FinishedAt.Sub(b.CreatedAt).Milliseconds()
	} else {
		b.FinishedAt = nil
	}
	return b, nil
}
