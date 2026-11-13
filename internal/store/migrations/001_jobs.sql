-- Phase 1: persistent job queue and exactly-once results.

CREATE TABLE IF NOT EXISTS jobs (
    id               UUID PRIMARY KEY,
    -- A retried submission with the same key returns the original job instead of a copy.
    idempotency_key  TEXT UNIQUE,
    type             TEXT        NOT NULL,
    payload          JSONB       NOT NULL DEFAULT '{}',
    priority         INT         NOT NULL DEFAULT 0,
    cpu_millis       BIGINT      NOT NULL,
    memory_mb        BIGINT      NOT NULL,
    gpus             INT         NOT NULL DEFAULT 0,
    gpu_memory_mb    BIGINT      NOT NULL DEFAULT 0,
    timeout_ms       BIGINT      NOT NULL,
    max_attempts     INT         NOT NULL,
    status           TEXT        NOT NULL DEFAULT 'queued'
                     CHECK (status IN ('queued', 'running', 'succeeded', 'failed')),
    -- Bumped on every assignment. A worker must present the attempt it was given when it
    -- completes the job; a stale attempt (lease lost, job reassigned) is rejected. This is
    -- the fencing token that keeps a slow or partitioned worker from recording a duplicate.
    attempt          INT         NOT NULL DEFAULT 0,
    worker_id        TEXT,
    lease_expires_at TIMESTAMPTZ,
    last_error       TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    assigned_at      TIMESTAMPTZ,   -- first assignment, for queue latency
    started_at       TIMESTAMPTZ,   -- current attempt
    finished_at      TIMESTAMPTZ
);

-- The queue scan: highest priority first, then oldest.
CREATE INDEX IF NOT EXISTS jobs_queued_idx ON jobs (priority DESC, created_at) WHERE status = 'queued';
-- The lease reaper's scan.
CREATE INDEX IF NOT EXISTS jobs_running_lease_idx ON jobs (lease_expires_at) WHERE status = 'running';

-- One row per job, ever. The primary key is what makes completion idempotent: a second
-- completion for the same job can't insert a second result.
CREATE TABLE IF NOT EXISTS results (
    job_id      UUID PRIMARY KEY REFERENCES jobs (id) ON DELETE CASCADE,
    attempt     INT         NOT NULL,
    worker_id   TEXT        NOT NULL,
    output      JSONB       NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Every attempt, for debugging retries and measuring reassignment.
CREATE TABLE IF NOT EXISTS attempts (
    job_id     UUID        NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    attempt    INT         NOT NULL,
    worker_id  TEXT        NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at   TIMESTAMPTZ,
    outcome    TEXT,  -- succeeded, failed, lease_expired, rejected
    error      TEXT,
    PRIMARY KEY (job_id, attempt)
);
