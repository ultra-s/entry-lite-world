# Vercel Workflow SDK Research (as of 2026-10-27)

Repo: https://github.com/vercel/workflow · Docs: https://vercel.com/docs/workflows, https://workflow-sdk.dev

## 1. Retry/timeout policies (per step / per run)

There are exactly **three** step-level knobs; there is no per-step `timeoutSeconds` and no `backoff` option in the SDK API.

- **Attempt count: `stepFn.maxRetries = N`** — a property set on the step function, not an options object. Default 3 (4 total attempts); `0` = run once, no retry. Docs: https://workflow-sdk.dev/docs/foundations/errors-and-retries and `docs/content/docs/v5/foundations/errors-and-retries.mdx` in the repo.
- **Retry delay: `throw new RetryableError(msg, { retryAfter })`** — `retryAfter` accepts ms (number), duration string (`'30s'`, `'1m'`), or `Date`. There is **no built-in exponential backoff helper**; compute backoff in userland (e.g. `retryAfter: metadata.attempt ** 2 * 1000` via `getStepMetadata()`). Without `retryAfter`, the world's queue schedules the default cadence (steps are re-enqueued immediately).
- **Give up: `throw new FatalError(msg)`** — bypasses `maxRetries`, surfaces to the workflow caller.

- **Per-step / per-run timeouts are NOT configurable options.** The official pattern is `Promise.race([step(...), sleep('30s')])` with durable `sleep()`; the loser keeps running, so pass an `AbortSignal` for cooperative cancellation and use idempotency keys. Docs: `docs/content/docs/v5/cookbook/common-patterns/timeouts.mdx`. Source reference: `packages/core/src/private.ts` (`StepFunction.maxRetries`), `packages/errors/src/index.ts`.
- Failure classification surfaces as `errorCode` on `WorkflowRunFailedError` (`USER_ERROR`, `MAX_EVENTS_EXCEEDED`, `MAX_DELIVERIES_EXCEEDED`, `REPLAY_TIMEOUT`, `REPLAY_DIVERGENCE`, `CORRUPTED_EVENT_LOG`, `STREAM_ERROR`, `WORLD_CONTRACT_ERROR`, `DEPLOYMENT_MISMATCH`...). Serialization failures fail like `FatalError` (no retries).

## 2. World interface: queue concurrency, delayed messages, leases

Source: `packages/world/src/queue.ts` and `packages/world/src/interfaces.ts` on `main`.

- **World = `Queue & Streamer & Storage`** plus optional `WorldCapabilities` and optional `Telemetry`.
- **Delayed messages**: `QueueOptions.delaySeconds?: number` — "Delay message delivery by this many seconds". Also `idempotencyKey`, `headers`, `deploymentId`, `specVersion`, `region`.
- **Concurrency**: the World queue conceptually supports `maxConcurrency`-limited consumption; per-run "flow topics" are consumed with `maxConcurrency: 1` to serialize a run's orchestrator invocations (`WORKFLOW_SEQUENTIAL_REPLAYS=1`). `WorldCapabilities.maxConcurrency?: boolean` declares *support* for concurrency-limited consumption (not deployed config); worlds without a concurrency concept must leave it unset.
- **Lease/liveness**: queue delivery is itself the lease — the runtime treats inline step ownership as a liveness lease; if the runner dies, crash recovery happens via **queue redelivery** (`attempt` increments on every redelivery). Worlds should not add a response deadline that redelivers a slow-but-healthy delivery. There's a legacy `probeRunId` field from 5.x callers; a handler result may be `{ timeoutSeconds }` to reschedule a wake, and a rejected handler must retry with incremented attempt.
- **Batching**: optional `queueBatch()` for wide fan-outs (results in input order; per-entry `{error, retryable}` outcomes; must not throw per-entry).
- **Reference Postgres world** (`@workflow/world-postgres`, latest 5.0.5): built on **graphile-worker**. Config (`packages/world-postgres/src/config.ts`): `queueConcurrency` (default **50**), `pollInterval` (default **500 ms**; LISTEN/NOTIFY wakes idle workers early), `enableInvoke`, `applicationManagedShutdown`, `streamFlushIntervalMs` (default 10 ms), `jobPrefix`, `namespace`, `maxPoolSize`/`pool`. Delivery timeouts default to **0/disabled** (`WORKFLOW_POSTGRES_HEADERS_TIMEOUT_MS` / `WORKFLOW_POSTGRES_BODY_TIMEOUT_MS`); graphile releases jobs when the worker dies, plus `reenqueueActiveRuns` on start. Job attempts capped at MAX_QUEUE_DELIVERIES+1 (49) + 24 post-ceiling retries (~6 days of graphile exp backoff).

## 3. Observability / telemetry

- **OTel spans**: the SDK depends only on the **OpenTelemetry API** (optional peer dep `@opentelemetry/api`); it never registers an exporter. Spans activate automatically when the app registers any OTel SDK (e.g. `@vercel/otel`). Emitted spans: `workflow.start <name>`, `workflow.execute <name>` (consumer/root per invocation), `step.execute <name>`, lifecycle spans, `http <method>` backend calls, `workflow.stream.*` spans. **One bounded trace per invocation**, stitched via **span links** (enqueue-site link + run-origin link), not one giant trace. Key attributes: `workflow.run.id`, `workflow.name`, `workflow.trace.mode`, `workflow.queue.overhead_ms`, stream RTT/TTFC attrs. Docs: https://workflow-sdk.dev/docs/observability/tracing, repo `docs/content/docs/v5/observability/tracing.mdx`, `packages/core/src/telemetry.ts`, `packages/core/src/telemetry/semantic-conventions.ts`.
- **World-side hooks** (not endpoints): `Telemetry.recordStepExecution?(stepId)` sync hook for platform correlation; `WorldCapabilities` for in-band fence/hook retention/invoke. There is **no required OTel endpoint on a world** — tracing is client-side in the app process.
- **Run search/list**: worlds implement the `Storage` interface — `runs` with `list(ListWorkflowRunsParams)` supporting `workflowName`, `status` (single or array; `[]` matches none), `pagination`, `resolveData`; plus `get`, `wait` (`timeoutMs` upper bound), `cancel` (single + bulk up to `BULK_CANCEL_MAX_RUN_IDS = 500`). `packages/world/src/runs.ts` / `interfaces.ts`. The Vercel-platform dashboard/search APIs are vendor-side; a self-hosted world only needs to satisfy the Storage contract.

## 4. Latest version & changes after 5.2.3

- `workflow` latest on npm: **5.2.3** (released 2026-10-20). **No releases after 5.2.3 on the 5.x line** — so **no breaking changes after 5.2.3**.
- The only newer tag is `workflow@4.8.14` (2026-10-21, the maintained 4.x LTS line): security fix removing `nanoid` (GHSA-28wg-ghj8-5hjv); default hook tokens now come from a built-in copy of the same generator so tokens/seeded randoms stay identical — **the same nanoid fix is already in 5.2.3** (#4335).
- `@workflow/world-postgres` latest: **5.0.5** (pin it to the same 5.x line as `workflow`; specVersion must match).
- Recent 5.2.x notable items: 5.2.2 ordering fix for `setAttributes()` replay barriers (REPLAY_DIVERGENCE risk), 5.2.1 zstd compression of run arguments on the libuv threadpool + `compressArguments` option, 5.2.0 faster wide `Promise.all` fan-out (createBatch chunks of 16).

## 5. Best practices for self-hosted Postgres worlds

- **Version-match everything**: `workflow`, `@workflow/world-postgres`, and the bootstrap CLI (`npx --package=@workflow/world-postgres@<same-version> bootstrap`) must be the same release line — specVersion mismatch silently refuses run creation; schema drift causes missing-column 500s (re-run bootstrap, it's idempotent).
- **Env wiring**: `WORKFLOW_TARGET_WORLD=@workflow/world-postgres`, `WORKFLOW_POSTGRES_URL=postgres://...` on the app runtime; unset `WORKFLOW_TARGET_WORLD` to revert.
- **Concurrency tuning**: keep `queueConcurrency` at its default 50 (parent→child polling patterns hold worker slots; concurrency 10 deadlocks recursive workflows on default Postgres). `pollInterval` 500 ms default; LISTEN/NOTIFY wakes idle workers so latency is mostly fine, but after a restart recovery is queue-poll driven — expect some redelivery latency.
- **Don't set delivery headers/body timeouts** unless you accept double-execution of slow deliveries; graphile lease release + `reenqueueActiveRuns` already cover crash recovery. Consider `WORKFLOW_POSTGRES_APPLICATION_MANAGED_SHUTDOWN=1` (graceful `world.close()`).
- **Data protection**: the Postgres world stores step inputs/outputs/stream chunks **unencrypted** — protect DB credentials and backups; consider `compressArguments`/`streamFlushIntervalMs` for large payloads.
- **Lost-wake backstop**: if building a custom world, event INSERTs must be idempotent on `(run_id, event_id)` and a sweeper should re-arm runs stuck `running` with no pending jobs/timers; note the runtime legitimately schedules far-future delayed wakes (`delaySeconds`) — distinguish those from lost wakes before intervening.
- **E2E verification**: smoke-test every World primitive (steps, durable `sleep`, `RetryableError`/`FatalError`, hooks, streaming, cancellation) and run a SIGKILL crash drill; read outcomes from `workflow_runs.status` / `workflow_events` in the DB, not app logs.

Key source files: `packages/world/src/interfaces.ts`, `packages/world/src/queue.ts`, `packages/world/src/runs.ts`, `packages/world-postgres/src/{queue,config}.ts`, `docs/content/docs/v5/foundations/errors-and-retries.mdx`, `docs/content/docs/v5/cookbook/common-patterns/timeouts.mdx`, `docs/content/docs/v5/observability/tracing.mdx`.
