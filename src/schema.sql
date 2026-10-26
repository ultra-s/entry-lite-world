-- Entry-Lite World schema (single 'entrylite' schema, plain Postgres).
-- Event-sourced: runs/steps/hooks/waits are materialized views of workflow_events.
CREATE SCHEMA IF NOT EXISTS entrylite;

CREATE TABLE IF NOT EXISTS entrylite.workflow_runs (
  run_id            varchar PRIMARY KEY,
  deployment_id     varchar NOT NULL,
  status            varchar NOT NULL DEFAULT 'pending',
  workflow_name     varchar NOT NULL,
  spec_version      integer,
  execution_context jsonb,
  input             jsonb,
  output            jsonb,
  error             text,
  error_code        varchar,
  attributes        jsonb NOT NULL DEFAULT '{}',
  dynamic_workflow_code jsonb,
  dynamic_workflow_code_ref varchar,
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  completed_at      timestamptz,
  started_at        timestamptz,
  expired_at        timestamptz
);
CREATE INDEX IF NOT EXISTS idx_el_runs_name   ON entrylite.workflow_runs (workflow_name);
CREATE INDEX IF NOT EXISTS idx_el_runs_status ON entrylite.workflow_runs (status);

CREATE TABLE IF NOT EXISTS entrylite.workflow_events (
  run_id        varchar NOT NULL,
  event_id      varchar NOT NULL,
  event_type    varchar NOT NULL,
  correlation_id varchar,
  event_data    jsonb,
  spec_version  integer,
  resume_id     varchar,
  created_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, event_id)
);
CREATE INDEX IF NOT EXISTS idx_el_events_corr ON entrylite.workflow_events (correlation_id);
-- one-shot entity creation events are unique per (run, correlation, type)
CREATE UNIQUE INDEX IF NOT EXISTS idx_el_events_entity_creation
  ON entrylite.workflow_events (run_id, correlation_id, event_type)
  WHERE event_type IN ('step_created','hook_created','wait_created');
CREATE UNIQUE INDEX IF NOT EXISTS idx_el_events_hook_resume
  ON entrylite.workflow_events (run_id, resume_id)
  WHERE event_type = 'hook_received' AND resume_id IS NOT NULL;

-- per-run event position counter (slot allocation: MAX+1 inside the insert tx)
CREATE TABLE IF NOT EXISTS entrylite.workflow_event_slots (
  run_id varchar PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS entrylite.workflow_steps (
  run_id       varchar NOT NULL,
  step_id      varchar PRIMARY KEY,
  step_name    varchar NOT NULL,
  status       varchar NOT NULL,
  input        jsonb,
  output       jsonb,
  error        text,
  attempt      integer NOT NULL DEFAULT 0,
  started_at   timestamptz,
  completed_at timestamptz,
  created_at   timestamptz NOT NULL DEFAULT now(),
  updated_at   timestamptz NOT NULL DEFAULT now(),
  retry_after  timestamptz,
  spec_version integer
);
CREATE INDEX IF NOT EXISTS idx_el_steps_run    ON entrylite.workflow_steps (run_id);
CREATE INDEX IF NOT EXISTS idx_el_steps_status ON entrylite.workflow_steps (status);

CREATE TABLE IF NOT EXISTS entrylite.workflow_hooks (
  run_id               varchar NOT NULL,
  hook_id              varchar PRIMARY KEY,
  token                varchar NOT NULL,
  owner_id             varchar NOT NULL DEFAULT '',
  project_id           varchar NOT NULL DEFAULT '',
  environment          varchar NOT NULL DEFAULT '',
  created_at           timestamptz NOT NULL DEFAULT now(),
  token_retention_until timestamptz,
  metadata             jsonb,
  spec_version         integer,
  is_webhook           boolean DEFAULT true,
  is_system            boolean DEFAULT false
);
CREATE INDEX IF NOT EXISTS idx_el_hooks_run   ON entrylite.workflow_hooks (run_id);
CREATE INDEX IF NOT EXISTS idx_el_hooks_token ON entrylite.workflow_hooks (token);

CREATE TABLE IF NOT EXISTS entrylite.workflow_waits (
  wait_id      varchar PRIMARY KEY,
  run_id       varchar NOT NULL,
  status       varchar NOT NULL DEFAULT 'waiting',
  resume_at    timestamptz,
  completed_at timestamptz,
  created_at   timestamptz NOT NULL DEFAULT now(),
  updated_at   timestamptz NOT NULL DEFAULT now(),
  spec_version integer
);
CREATE INDEX IF NOT EXISTS idx_el_waits_run ON entrylite.workflow_waits (run_id);

-- Durable queue (Entry-Lite design): SKIP LOCKED job pickup, one queue per run
-- for run serialization, delay via run_at, attempts for retry accounting.
CREATE TABLE IF NOT EXISTS entrylite.workflow_jobs (
  id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  queue_name    varchar NOT NULL,
  payload       jsonb NOT NULL,
  run_at        timestamptz NOT NULL DEFAULT now(),
  attempts      integer NOT NULL DEFAULT 0,
  max_attempts  integer NOT NULL DEFAULT 25,
  locked_at     timestamptz,
  locked_by     varchar,
  last_error    text,
  created_at    timestamptz NOT NULL DEFAULT now(),
  -- idempotent enqueue: job_key unique while a pending/locked row exists
  job_key       varchar
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_el_jobs_key
  ON entrylite.workflow_jobs (job_key)
  WHERE locked_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_el_jobs_due ON entrylite.workflow_jobs (run_at)
  WHERE locked_at IS NULL;
-- Namespace-scoped claim index: each deployment's due-jobs scan touches only
-- its own rows (prefix-bounded), so independent deployments sharing this
-- table never contend on each other's index range.
CREATE INDEX IF NOT EXISTS idx_el_jobs_ns_due ON entrylite.workflow_jobs (queue_name, run_at)
  WHERE locked_at IS NULL;
-- Stale-lease reclamation is part of claiming (locked_at IS NULL predicate +
-- claim-time WHERE), so no separate reclaimer is needed; this index supports
-- lease-release updates by worker id for observability and hand-cleanup.
CREATE INDEX IF NOT EXISTS idx_el_jobs_locker ON entrylite.workflow_jobs (locked_by)
  WHERE locked_by IS NOT NULL;

-- Stream chunks
CREATE TABLE IF NOT EXISTS entrylite.workflow_stream_chunks (
  stream_id  varchar NOT NULL,
  chunk_id   varchar NOT NULL,
  run_id     varchar,
  data       bytea NOT NULL,
  eof        boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (stream_id, chunk_id)
);
CREATE INDEX IF NOT EXISTS idx_el_chunks_run ON entrylite.workflow_stream_chunks (run_id);

-- NOTIFY wake channel for low-latency job pickup.
-- Payload = stored (namespaced) queue_name so listeners can filter to their
-- own deployment's inserts; empty payload = legacy pre-namespace trigger.
CREATE OR REPLACE FUNCTION entrylite.notify_job() RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('entrylite_job', NEW.queue_name);
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_el_jobs_notify ON entrylite.workflow_jobs;
CREATE TRIGGER trg_el_jobs_notify
AFTER INSERT ON entrylite.workflow_jobs
FOR EACH STATEMENT EXECUTE FUNCTION entrylite.notify_job();

-- Migrations for existing deployments (idempotent)
ALTER TABLE entrylite.workflow_runs ADD COLUMN IF NOT EXISTS dynamic_workflow_code jsonb;
ALTER TABLE entrylite.workflow_runs ADD COLUMN IF NOT EXISTS dynamic_workflow_code_ref varchar;
