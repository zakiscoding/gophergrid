-- Phase 2: worker registry and leader fencing.

CREATE TABLE IF NOT EXISTS workers (
    id              TEXT PRIMARY KEY,
    hostname        TEXT        NOT NULL DEFAULT '',
    cpu_millis      BIGINT      NOT NULL,
    memory_mb       BIGINT      NOT NULL,
    gpus            JSONB       NOT NULL DEFAULT '[]',  -- [{"id","model","memory_mb"}]
    gpu_mode        TEXT        NOT NULL DEFAULT 'none',
    job_types       TEXT[]      NOT NULL DEFAULT '{}',
    state           TEXT        NOT NULL DEFAULT 'alive' CHECK (state IN ('alive', 'draining', 'dead')),
    registered_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_heartbeat  TIMESTAMPTZ NOT NULL DEFAULT now(),
    free_cpu_millis BIGINT      NOT NULL DEFAULT 0,
    free_memory_mb  BIGINT      NOT NULL DEFAULT 0,
    running         INT         NOT NULL DEFAULT 0,
    cpu_utilization DOUBLE PRECISION NOT NULL DEFAULT 0
);

-- One row. The leader writes its etcd election revision here when it takes over; every
-- leader-only write (assigning jobs, reaping leases, declaring workers dead) checks it in
-- the same transaction. A deposed leader that hasn't noticed yet fails that check instead
-- of assigning jobs alongside the new one.
CREATE TABLE IF NOT EXISTS leadership (
    id         INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    epoch      BIGINT      NOT NULL,
    holder     TEXT        NOT NULL,
    elected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS jobs_worker_running_idx ON jobs (worker_id) WHERE status = 'running';

-- GPUs the scheduler assigned to the current attempt (ids from the worker's GPU list).
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS gpu_ids TEXT[];
