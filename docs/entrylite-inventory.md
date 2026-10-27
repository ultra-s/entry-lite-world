# @entrylite/world — Capability Inventory

Repo: `/home/agentuser/entry-lite-world` · pkg `@entrylite/world` v1.0.0 · "Entry-Lite production World for the Vercel Workflow SDK — plain node-postgres, no drizzle/graphile"
Deps: `@workflow/world`/`@workflow/errors` 5.0.5, `pg` ^8.20, `ulid`, `zod`. Build: tsc → dist/; tests: node --test (14 test files, incl. namespace isolation/multiproc/perf, contract conformance, e2e, sdk-runtime).

## 1. Exported surface (`createWorld(config)` from src/index.ts; dist/index.d.ts matches)
Config: `connectionString`, `maxPoolSize` (default 20), `streamFlushIntervalMs`, `queueNamespace` (must equal env NS; process-wide by design).

Top-level methods:
- Lifecycle: `start()` (queue start + crash recovery: re-enqueues pending/running runs with no job rows in this namespace), `close()` (sweeper → queue → streamer → pool), `clear()` (test helper TRUNCATE of all tables), `getRuntimeDeadline()` → always undefined, `describeRun()` (metadata-only, never throws), `getDeploymentId()` → `entrylite[-<ns>]`
- ID/metadata: `createRunId()` (ULID, strips `wrun_`), `regionForRunId()` → null, `validateRunExecutionContext()` → no-op
- `specVersion: number` — from `ENTRYLITE_SPEC_VERSION`, default **8** (workflow@5.2.x; note: src/util.ts also exports `SPEC_VERSION = 4` for beta.40-era event writes; events.ts defaults `data.specVersion ?? 4`)
- runs: `get, list, cancel` (storage.ts) + `waitForTerminalStatus, getMany, cancelMany, experimentalSetAttributes` (extras.ts)
- steps: `get, list` · hooks: `get, getByToken, list` · events: `create, get, list, listByCorrelationId, createBatch`
- queue (flat, runtime calls world.queue directly): `queue, queueBatch, createQueueHandler, getDeploymentId, kick`; route registry: `registerRoute, registerHandler, getRouteForPrefix`
- streams: `write, writeMulti, close, get (ReadableStream), list, getChunks, getInfo`
- `invoke(runId, payload, {idempotencyKey, timeoutMs})` — hook-resume mailbox (workflow_invocations) + invoke-marked wake; polls mailbox ≤ min(30s, 120s cap), 408 on timeout with outcome unknown
- `analytics` (runs/attributes/steps/events/hooks/waits get+list), `getEnvironment()`, `experimental_snapshots` (save/load/delete), `uploadDynamicWorkflowCode`, `getEncryptionKeyForRun`
- Background (eager, not gated on start()): 200ms sweeper interval — due-wait timer sweep (wait_completed + wake), lost-wake backstop (running run with no jobs/waits/recent events for 90s gets idempotent wake), queue.kick(); queue.start() called eagerly.

## 2. capabilities object (index.ts)
```
hookRetention: { active: true }
maxConcurrency: true        // per-run flow topics, maxConcurrency:1 serialization
queueNamespace: !!NAMESPACE // true iff ENTRYLITE_QUEUE_NAMESPACE set
hookResumeDedup: true       // (run_id, resume_id) unique index on hook_received
hookForceClaim: true        // experimental_force at createHook, locked re-check
dynamicWorkflowCode: true   // run_created persists code; runs.get echoes it
invoke: true                // delivery captures flow handler response as outcome
```

## 3. Env vars
- `ENTRYLITE_SPEC_VERSION` (default 8)
- `ENTRYLITE_QUEUE_NAMESPACE` (queue isolation, `^[a-z][a-z0-9]*$`, validated at import)
- `ENTRYLITE_WORLD_URL` (connection; fallback `WORKFLOW_POSTGRES_URL`, `DATABASE_URL`, hardcoded localhost default) — used in index.ts, bootstrap.ts, and the dedicated LISTEN client in queue.ts
- `ENTRYLITE_QUEUE_POLL_MS` (250) · `ENTRYLITE_QUEUE_CONCURRENCY` (50)
- `WORKFLOW_LOCAL_BASE_URL` (delivery target; fallback `http://localhost:$PORT` / :3000) — `WORKFLOW_*` var
- `ENTRYLITE_ENCRYPTION_KEY` (per-run key = SHA-256(`${secret}:${runId}`); unset ⇒ undefined, fail-closed)
- `ENTRYLITE_ENVIRONMENT` (only production/preview/development accepted)
- `ENTRYLITE_DEBUG` (verbose logging) · `PORT` (implicit delivery URL)

## 4. Queue namespace mechanism (util.ts)
- Rows stored as `<ns>::<queueName>`; job_keys stamped `<ns>::<key>`; unset = legacy mode (no prefix).
- Claim scoping: `queue_name LIKE '<ns>::%'` (or `NOT LIKE '%::%'` legacy); claim uses `UPDATE … WHERE id = (SELECT … FOR UPDATE SKIP LOCKED …)`.
- Delivery-side guard: handler rejects with 409 if `x-vqs-queue-name` lacks the namespace prefix.
- NOTIFY payload carries stored namespaced queue name; listener filters via `isOwnQueueName`. Dedicated ns-due index `idx_el_jobs_ns_due (queue_name, run_at) WHERE locked_at IS NULL` bounds each deployment's scan.

## 5. Schema (src/schema.sql, single `entrylite` schema)
Tables: `workflow_runs` (incl. dynamic_workflow_code/_ref, attributes, error/error_code), `workflow_events` (PK (run_id,event_id); partial unique indexes: entity-creation per (run,correlation,type) and hook_resume per (run,resume_id)), `workflow_invocations` (mailbox), `workflow_snapshots` (bytea+metadata), `workflow_dynamic_code`, `workflow_event_slots` (position counter), `workflow_steps`, `workflow_hooks` (token retention, is_webhook/is_system), `workflow_waits` (durable timers), `workflow_jobs` (identity id, run_at, attempts, max_attempts default 25, locked_at/locked_by, last_error, job_key unique-while-unlocked), `workflow_stream_chunks`.
Idempotent migration ALTERs at bottom. Trigger `trg_el_jobs_notify` → `pg_notify('entrylite_job', queue_name)`; streamer uses `entrylite_chunk`.

## 6. Delivery, retries, leases (src/queue.ts)
- Claim = atomic UPDATE with `FOR UPDATE SKIP LOCKED`, sets locked_at/locked_by, attempts+1. **No separate lease-stealer**: stale leases are reclaimed implicitly because release paths always null locked_at; reclaim of a crashed worker's stuck row relies on the claim predicate (`locked_at IS NULL`) — i.e. a row left locked by a dead process is NOT auto-reclaimed by timeout; recovery is via run-level re-enqueue in `start()`/lost-wake sweeper, not per-job lease expiry. (max_attempts column exists but the code's retry policy is attempts-based below.)
- Non-OK HTTP: backoff = min(5000ms × attempts, 60s); at attempts ≥ 48 park job for 1 hour (no dead-letter/delete). Transport error (fetch throw): release + fixed 5s backoff.
- Long-poll: handler `{timeoutSeconds}` response reschedules (run_at += timeout, unlock), cap 2147483s.
- Success: plain 200 deletes the job; invoke-marked jobs record outcome (accepted iff hook_received with resume_id=requestId exists, else rejected HOOK_NOT_FOUND).
- Wake listener auto-reconnects on error (1s); falls back to 250ms polling.
- Enqueue idempotency: `ON CONFLICT (job_key) WHERE locked_at IS NULL DO NOTHING`.

## 7. Error handling / typed errors
Uses `WorkflowRunNotFoundError`, `HookNotFoundError`, `WorkflowWorldError` from @workflow/errors (dynamic imports in places). errors stored via `errorToText` (JSON {message, stack} for Error). runs.cancel returns current status if not cancellable; cancelMany → per-run outcomes (not_found / already_cancelled / not_cancellable / cancelled / failed) + summary counts. createBatch is atomic-per-event (200 or error {status, code: 'conflict'|'batch_not_expressible'|'batch_invalid_pair'}). experimentalSetAttributes rejects `$`-reserved keys (400 reserved_attribute) unless allowed.

## 8. Observability
- `telemetry.recordStepExecution(stepId)` — no-op sink; logs only with ENTRYLITE_DEBUG.
- Logging: console.log/error with `[entrylite-world]` prefix throughout (namespace active, lost-wake, timer sweep, delivery, queue start, crashes).
- Analytics surface (extras.ts): paginated runs/attributes (key aggregation)/steps/events/hooks/waits — offset-cursor pagination.
- No OpenTelemetry / structured trace export. `describeRun` supports SDK observability surfaces. docs/parity-audit.md + docs/evidence exist.

## 9. TODOs / stubs / caveats
- No literal TODO/FIXME markers in src. Soft stubs: `getRuntimeDeadline` always undefined; `regionForRunId` null; `validateRunExecutionContext` no-op; `telemetry` no-op sink; `waitForTerminalStatus` uses 250ms poll ("LISTEN-backed wake would go here"); `analytics.hooks/waits` hardcode status 'created'; analytics.steps.get has a broken WorkflowStepNotFoundError import fallback (dynamic import `.catch(() => ({})) ?? {}` — destructure of `{}` would throw before the intended plain Error path if the import fails); `runs.list` with resolveData 'all' does N+1 get() calls; `streams.get` replays via ORDER BY chunk_id (ULID, so time-ordered) rather than an explicit index column.
- specVersion duality: world.specVersion default 8, event writes default 4, timer sweep hardcodes 8 — configurable via ENTRYLITE_SPEC_VERSION for world-level only.
- Read-only audit; nothing modified.
