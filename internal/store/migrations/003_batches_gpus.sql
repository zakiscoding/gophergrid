-- Phase 3: batches (distributed inference fan-out) and per-attempt GPU records.

CREATE TABLE IF NOT EXISTS batches (
    id              UUID PRIMARY KEY,
    idempotency_key TEXT UNIQUE,
    kind            TEXT        NOT NULL,   -- inference
    spec            JSONB       NOT NULL,
    shards          INT         NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS batch_id UUID REFERENCES batches (id) ON DELETE CASCADE;
CREATE INDEX IF NOT EXISTS jobs_batch_idx ON jobs (batch_id) WHERE batch_id IS NOT NULL;

-- Which GPUs each attempt held, so tests and debugging can prove no GPU was double-booked.
ALTER TABLE attempts ADD COLUMN IF NOT EXISTS gpu_ids TEXT[];
