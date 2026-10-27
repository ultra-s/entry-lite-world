# Vercel Workflow SDK — Feature & World-Interface Inventory

Source: `/home/agentuser/workflow` @ commit `6455bebd7dbc18e08fce5e3dede6df6ac4d7d7ff` (2026-10-20, "world-local: Release orphaned inline steps on restart instead of waiting out the lease (#4940)").

Primary sources: `packages/world/src/interfaces.ts` (World/Storage/Queue/Streamer/Analytics/Telemetry/WorldCapabilities), `packages/world/src/*.ts` (entity schemas), `packages/core/src/**`, `packages/errors/src/index.ts`, `packages/workflow/src/api.ts`, `packages/cli/src/commands/*`, `docs/content/docs/v5/**`, `docs/content/worlds/v5/**`.

---

## 1. The `World` interface (`packages/world/src/interfaces.ts`)

`export interface World extends Queue, Streamer, Storage`

### 1.1 Top-level World members

| Member | Signature | Required |
|---|---|---|
| `analytics` | `analytics?: Analytics` | optional |
| `specVersion` | `specVersion: number` | **required** (declare `SPEC_VERSION_CURRENT`; runtime validates against `[SPEC_VERSION_CURRENT, SPEC_VERSION_MAX_SUPPORTED]`) |
| `capabilities` | `capabilities?: WorldCapabilities` | optional |
| `validateRunExecutionContext` | `(value: Record<string, unknown>): void` | optional (dynamic starts only) |
| `getRuntimeDeadline` | `(): Promise<Date \| undefined>` | optional |
| `start` | `(): Promise<void>` | optional |
| `close` | `(): Promise<void>` | optional |
| `resolveLatestDeploymentId` | `(): Promise<string>` | optional (world-vercel only) |
| `getEncryptionKeyForRun` | `(run: WorkflowRun): Promise<Uint8Array \| undefined>` OR `(runId: string, context?: Record<string, unknown>): Promise<Uint8Array \| undefined>` | optional; absent ⇒ encryption disabled. AES-256-GCM |
| `createRunId` | `(options?: Readonly<Record<string, unknown>>): string` | optional; returns bare ULID (no `wrun_` prefix) |
| `regionForRunId` | `(runId: string): string \| null` | optional; pure, never throws |
| `uploadDynamicWorkflowCode` | `(runId: string, params: { workflowName: string; code: Uint8Array }): Promise<string>` | optional; returns ref key for `run_created.eventData.dynamicWorkflowCodeRef` |
| `getEnvironment` | `(): string \| undefined` | optional; sync, side-effect free (vercel: `'production' \| 'preview' \| 'development'`) |
| `describeRun` | `(run: Readonly<Record<string, unknown>>): Record<string, string \| null> \| null \| Promise<...>` | optional; CLI `workflow inspect` display enrichment |
| `telemetry` | `telemetry?: Telemetry` | optional |

`Telemetry` interface: `recordStepExecution?(stepId: string): void` — sync, called before step user code, must not throw.

### 1.2 `Storage` interface

**`storage.runs`**
- `get(id, params?: GetWorkflowRunParams & { resolveData?: 'none'|'all' })` → `Promise<WorkflowRun | WorkflowRunWithoutData>` (3 overloads; required)
- `waitForTerminalStatus?(id, params?: WaitForTerminalRunStatusParams)` → `Promise<WorkflowRun | WorkflowRunWithoutData>` (3 overloads; OPTIONAL — long poll; absent ⇒ core interval-polls on `WORKFLOW_RETURN_VALUE_POLL_INTERVAL_MS`)
- `getMany?(ids: readonly string[], params?)` → `Promise<(WorkflowRun | WorkflowRunWithoutData | null)[]>` (3 overloads; OPTIONAL; preserves order, `null` for missing)
- `list(params?: ListWorkflowRunsParams)` → `Promise<PaginatedResponse<WorkflowRun | WorkflowRunWithoutData>>` (3 overloads; required; observability use deprecated in favor of `analytics.runs.list`)
- `experimentalSetAttributes?(runId, changes: AttributeChange[], options?: { allowReservedAttributes?: boolean })` → `Promise<ExperimentalSetAttributesResult>` (OPTIONAL, EXPERIMENTAL; upsert on `value: string`, remove on `value: null`; core `setAttributes` feature-detects and no-ops with a one-time warning if absent)
- `cancelMany?(request: BulkCancelWorkflowRunsRequest)` → `Promise<BulkCancelWorkflowRunsResult>` (OPTIONAL; core `cancelRuns` falls back to bounded-concurrency single-run cancels)

**`storage.steps`**
- `get(runId, stepId, params?: GetStepParams)` → `Promise<Step | StepWithoutData>` (3 overloads; required)
- `list(params?: ListWorkflowRunStepsParams)` → `Promise<PaginatedResponse<Step | StepWithoutData>>` (3 overloads; required)

**`storage.events`** (slot-numbered event log; ids `evnt_` + 26-char zero-padded dense 1-based position — NOT optional, `requireEventSlot` fails runs without it)
- `create<T extends RunCreatedEventRequest>(runId: string | null, data: T, params?: CreateEventParams)` → `Promise<EventResult<T['eventType']>>` (required)
- `create<T extends CreateEventRequest>(runId: string, data: T, params?: CreateEventParams)` → `Promise<EventResult<T['eventType']>>` (required)
- `createBatch?(runId: string, events: BatchEventRequest[], params?: CreateEventBatchParams)` → `Promise<EventBatchResult>` (OPTIONAL — presence IS the capability declaration; must be atomic per attempt)
- `get(runId, eventId, params?: GetEventParams)` → `Promise<Event>` (required)
- `list(params: ListEventsParams)` → `Promise<EventListResponse>` (required; `EventListResponse = PaginatedResponse<Event> & { snapshot?: EventLogSnapshot }` where snapshot is `{ seq, seqInBand }`)
- `listByCorrelationId(params: ListEventsByCorrelationIdParams)` → `Promise<PaginatedResponse<Event>>` (required)

**`storage.hooks`**
- `get(hookId, params?: GetHookParams)` → `Promise<Hook>`
- `getByToken(token, params?: GetHookParams)` → `Promise<Hook>`
- `list(params: ListHooksParams)` → `Promise<PaginatedResponse<Hook>>`

**`storage.experimental_snapshots?`** (EXPERIMENTAL, optional; QuickJS VM memory snapshots)
- `save(runId, data: Uint8Array, metadata: SnapshotMetadata): Promise<void>`
- `load(runId): Promise<{ data: Uint8Array; metadata: SnapshotMetadata } | null>`
- `delete(runId): Promise<void>` (must be idempotent)
- Helpers: `encodeSnapshotEnvelope` / `decodeSnapshotEnvelope` pack bytes+metadata into one atomic blob.

### 1.3 `Streamer` interface

- `streamFlushIntervalMs?: number` (streamer option; overridden by `WORKFLOW_STREAM_FLUSH_INTERVAL_MS`)
- `streams`:
  - `createWriteSession?(runId, name, options: CreateStreamWriteSessionOptions /* { writerId: `wrtr_${string}` } */): StreamWriteSession` (optional)
  - `write(runId, name, chunk: string | Uint8Array): Promise<void>` (required)
  - `writeMulti?(runId, name, chunks: (string | Uint8Array)[]): Promise<void>` (optional batch optimization)
  - `close(runId, name): Promise<void>` (required)
  - `get(runId, name, startIndex?: number): Promise<ReadableStream<Uint8Array>>` (required; negative startIndex = from end, clamped to 0)
  - `list(runId): Promise<string[]>` (required)
  - `getChunks(runId, name, options?: GetChunksOptions): Promise<StreamChunksResponse>` (required; cursor pagination, limit default 100 max 1000, `done` flag)
  - `getInfo(runId, name): Promise<StreamInfoResponse>` (required; tail index + completion flag)
- `StreamWriteSession`: `write(chunkSeq, chunks): Promise<void>`, `close(): Promise<void>`, optional `release?()`, optional `dispose?()`

### 1.4 `Queue` interface

- `getDeploymentId(): Promise<string>` (required)
- `invoke?(runId: string, payload: unknown, options?: InvokeOptions): Promise<unknown>` (optional; gated by `capabilities.invoke`. `InvokeOptions { idempotencyKey?: string; timeoutMs?: number }`)
- `isDeploymentUnavailableError?(error: unknown): boolean` (optional)
- `queue(queueName: ValidQueueName, message: QueuePayload, opts?: QueueOptions): Promise<{ messageId: MessageId | null }>` (required)
- `queueBatch?(queueName: ValidQueueName, messages: readonly { message: QueuePayload; opts?: QueueOptions }[]): Promise<QueueBatchResult[]>` (optional; same logical queue; `QueueBatchResult = { messageId: MessageId|null } | { messageId: null; error: string; retryable: boolean }`)
- `createQueueHandler(queueNamePrefix: QueuePrefix, handler: (message: unknown, meta: { attempt: number; deliveryCount?: number; createdAt?: Date; queueName: ValidQueueName; messageId: MessageId; requestId?: string }) => Promise<unknown>): (req: Request) => Promise<Response>` (required)
- `registerHandler?(prefix: QueuePrefix, handler: (req: Request) => Promise<Response>): void` (optional; in-process delivery, takes precedence over registerRoute)
- `registerRoute?(prefix: QueuePrefix, path: string): void` (optional; HTTP delivery route)
- `QueueOptions: { deploymentId?; idempotencyKey?; headers?: Record<string,string>; delaySeconds?; specVersion?; region? }`
- Queue name formats: `QueueKind = 'workflow'`; `QueuePrefix` (e.g. `__wkf_workflow_`), `ValidQueueName` — namespaced as `__{namespace}_wkf_workflow_*` via `WORKFLOW_QUEUE_NAMESPACE`.

### 1.5 `WorldCapabilities` (all default to unsupported when absent; fail closed)

| Capability | Type | Meaning |
|---|---|---|
| `invoke` | `boolean?` | Enables `invoke()` + `createQueueHandler` request/response; at most one active runner per runId across processes |
| `hookRetention` | `{ active: boolean }?` | Supports `experimental_minRetention` for hooks |
| `maxConcurrency` | `boolean?` | Queue supports `maxConcurrency`-limited consumption (used by `WORKFLOW_SEQUENTIAL_REPLAYS=1`) |
| `inBandFence` | `boolean?` | Implements in-band writer fence (`CreateEventParams.inBand`, `expectedSeqInBand`, `InBandSupersededError` HTTP 412 `in-band-superseded`, `EventListResponse.snapshot { seq, seqInBand }`) |
| `hookResumeDedup` | `boolean?` | Dedupes concurrent `hook_received` on `(runId, resumeId)`; must round-trip `resumeId` in `events.list` |
| `hookForceClaim` | `boolean?` | Supports `createHook({ experimental_force: true })` token takeover (formalized in `workflow-server/specs/HookForceClaim.tla`) |
| `deploymentAffinity` | `boolean?` | Atomic/immutable deployments; enables deployment-affinity guard (`DEPLOYMENT_MISMATCH`) |
| `dynamicWorkflowCode` | `boolean?` | Persists `dynamicWorkflowCode` with the run; `start()` refuses dynamic starts otherwise |

### 1.6 `Analytics` interface (optional observability namespace; metadata-only rows)

- `runs: { get(runId): Promise<AnalyticsRun>; list(params?: AnalyticsListRunsParams): Promise<PaginatedResponse<AnalyticsRun>> }`
- `attributes: { list(params?: AnalyticsListAttributesParams): Promise<PaginatedResponse<AnalyticsAttributeKey>> }`
- `steps: { get(runId, stepId): Promise<AnalyticsStep>; list(params): Promise<PaginatedResponse<AnalyticsStep>> }`
- `events: { get(runId, eventId); getMany(runId, eventIds: readonly string[]): Promise<AnalyticsEvent[]>; list(params): Promise<PaginatedResponse<AnalyticsEvent>>; listByCorrelationId (deprecated — use `list({ runId, correlationId })`) }`
- `hooks: { list(params): Promise<PaginatedResponse<AnalyticsHook>> }`
- `waits: { list(params: AnalyticsListWaitsParams): Promise<PaginatedResponse<AnalyticsWait>> }`

### 1.7 Key entity types (`packages/world/src/`)

- **Event types** (`events.ts` `EventTypeSchema`): `run_created, run_started, run_completed, run_failed, run_cancelled, attr_set, step_created, step_completed, step_failed, step_retrying, step_started, hook_created, hook_received, hook_disposed, hook_conflict, wait_created, wait_completed, noop` (noop = sealed-log filler, spec ≥ 7, World-only).
- **`CreateEventParams`**: includes `eventCount`, `inBand?`, `expectedSeqInBand?` (in-band fence), plus batch sizing.
- **Hook** (`hooks.ts`): token, `resumeCapabilities.hookResumeDedupVersion` (server-computed, response-only), `claimedFrom` (force-claim), `experimental_minRetention`.
- **WorkflowRun** (`runs.ts`): statuses (`WorkflowRunStatus`; terminal: completed/failed/cancelled), `executionContext`, `resolveData: 'none'|'all'`, `WaitForTerminalRunStatusParams.timeoutMs`.
- **Wait** (`waits.ts`): `{ waitId, runId, status: 'waiting'|'completed', resumeAt?, completedAt?, createdAt, updatedAt, specVersion? }`.
- **Spec versions** (`spec-version.ts`): `SPEC_VERSION_LEGACY=1`, `SUPPORTS_EVENT_SOURCING=2`, `SUPPORTS_CBOR_QUEUE_TRANSPORT=3`, `SUPPORTS_ATTRIBUTES=4`, `SUPPORTS_COMPRESSION=5`, `SUPPORTS_SLOT_IDENTITY=6`, `SUPPORTS_SEALED_LOG=7`, `SUPPORTS_HOOK_FORCE_CLAIM=8`; `SPEC_VERSION_CURRENT = SPEC_VERSION_MAX_SUPPORTED = 8`.
- First-party worlds (`docs/content/worlds/v5/`): `vercel.mdx`, `postgres.mdx`, `local.mdx`, plus "building-a-world.mdx" guide; `@workflow/world-sim`, `@workflow/world-testing` (conformance suites incl. `test-support/in-band-fence-conformance.ts`).

---

## 2. User-facing SDK features (docs/content/docs/v5)

### 2.1 Directives & build (`how-it-works/understanding-directives.mdx`, `code-transform.mdx`)
- `'use workflow'` — marks a workflow function (orchestration, no I/O; runs in QuickJS VM sandbox).
- `'use step'` — marks a step function (Node.js context, I/O allowed).
- SWC plugin (`@workflow/swc-plugin`), Rollup/Vite/Nuxt/Nitro/SvelteKit/Astro/Nest/Next builders; `workflow transform --mode workflow|step --check-serde` CLI introspection; `workflow validate` serde-compliance checks.

### 2.2 Workflow & step API (`workflow` package)
- `start(myWorkflow, ...args, options?: StartOptions)` (from `workflow/api`) — also callable inside workflows for child runs / versioned continuation.
- `Run` class: `run.returnValue`, `run.getReadable()`, `run.getWritable()`, `run.cancel()`, polling.
- Inside workflows: `sleep()`, `createHook<T>()`, `createWebhook()`, `defineHook<TInput, TOutput>()` (type-safe hooks with `.resume()`, `hook.getConflict()`), `getWritable<W>()` (default run stream), `getWorkflowMetadata()`, `getStepMetadata()`, `setAttributes(attributes, options?: { allowReservedAttributes })`, `fetch()` (workflow-context fetch with retry semantics), `WorkflowAbortSignal` / distributed `AbortController`.
- Host-side API (`workflow/api`): `start`, `getRun`, `Run`, `getHookByToken`, `resumeHook`, `resumeWebhook`, `cancelRun`, `cancelRuns`, `registerLifecycleHooks` (`onRunCompleted`/`onRunFailed`; `WorkflowLifecycleHooks`, `RunCompletedHookParams`, `RunFailedHookParams`), stream option types (`WorkflowReadableStreamOptions`, `WorkflowRunWritableStreamOptions`, `CancelRunOptions`, `StopSleepOptions/StopSleepResult`).
- `workflow/observability`: `hydrateData`, `hydrateResourceIO`, `observabilityRevivers`, `Revivers`, `parseWorkflowName`, `parseStepName`, `parseClassName`.
- `@workflow/ai` (`workflow/ai`): Durable Agent, `WorkflowChatTransport`, AI SDK integration; docs: chat-session-modeling, defining-tools, human-in-the-loop, message-queueing, resumable-streams, sleep-and-delays, streaming-updates-from-tools.

### 2.3 Errors & retries (`foundations/errors-and-retries.mdx`, `packages/errors`)
- `FatalError` (extends Error; `FatalError.is()` gate — step fails, no retry), `RetryableError` (with options `RetryableErrorOptions`).
- Steps default `maxRetries = 3` (6 attempts when set to 5); set as `stepFunc.maxRetries = N`.
- Error classes (`@workflow/errors`): `WorkflowError`, `WorkflowWorldError`, `WorkflowRunFailedError`, `WorkflowRunNotCompletedError`, `WorkflowRuntimeError`, `CorruptedEventLogError`, `ReplayDivergenceError`, `MaxEventsExceededError`, `RuntimeDecryptionError`, `WorkflowBuildError`, `SerializationError`, `StepNotRegisteredError`, `WorkflowNotRegisteredError`, `WorkflowDeploymentMismatchError`, `WorkflowRunNotFoundError`, `HookConflictError`, `HookForceClaimedError`, `HookNotFoundError`, `EntityConflictError`, `RunExpiredError`, `StreamError`, `StreamExpiredError`, `TooEarlyError`, `ThrottleError`, `PreconditionFailedError`, `InBandSupersededError`, `WorkflowRunCancelledError`, `RunNotSupportedError`, `FatalError`, `RetryableError`.
- Error guide pages: abort-signal-timeout-in-workflow, corrupted-event-log, deployment-mismatch, fetch-in-workflow, hook-conflict, hook-force-claimed, max-events-exceeded, node-js-module-in-workflow, replay-divergence, replay-timeout, run-expired, runtime-decryption-failed, serialization-failed, start-invalid-workflow-function, step-executed-multiple-times, step-not-registered, timeout-in-workflow, webhook-invalid-respond-with-value, webhook-response-not-sent, workflow-not-registered.

### 2.4 Durability primitives
- Durable `sleep()` (string durations e.g. `"2d"`; replay-safe; `Promise.race` patterns).
- Hooks / webhooks (`foundations/hooks.mdx`): `createHook`, `defineHook`, `resumeHook(token, payload)`, `getHookByToken`, `createWebhook`/`resumeWebhook`, `hook.getConflict()`, `experimental_force` (token takeover), `experimental_minRetention`.
- Waits entity (`wait_created`/`wait_completed`, `analytics.waits`).
- Scheduling: no native cron primitive — docs pattern = external scheduler calling `start()` + deterministic hook tokens for idempotency (`scheduling.mdx`). `maxDuration` is platform-level (Vercel function config; `World.getRuntimeDeadline()` drives inline-step budget).

### 2.5 Streaming & queues
- `getWritable()` / `run.getReadable()`; chunked streams with `getChunks`/`getInfo`, framed streams, flush intervals.
- Queue: namespaced topics (`WORKFLOW_QUEUE_NAMESPACE`), `delaySeconds`, batch publish, region routing, `maxConcurrency`, sequential replays.

### 2.6 Observability (`observability/`)
- Attributes (`setAttributes`, `$`-reserved namespace, analytics attribute keys), lifecycle hooks, retention (`docs/observability/retention.mdx`), OpenTelemetry tracing (API-only dep; spans `workflow.start <name>`, `workflow.execute <name>`, `step.execute <name>`, `workflow.lifecycle.onRunCompleted/onRunFailed`, `http <method>`, `workflow.stream.write/flush/close/read.complete/read.connect/read`; propagation across queue deliveries; `@vercel/otel` recommended).
- Dashboard/web UI: `workflow web` CLI opens run-inspector UI (`packages/web`, `@workflow/web`, `@workflow/web-shared`).

### 2.7 CLI (`@workflow/cli`, oclif)
- `workflow build` — build workflow bundles for deployment
- `workflow dev` — dev server w/ file watching (coming soon)
- `workflow start` (alias `s`) — start a workflow
- `workflow cancel` (alias `c`) — cancel a run / bulk-cancel by status/name
- `workflow inspect` (alias `i`) — inspect runs, steps, streams, events
- `workflow health` — queue-based health check (flags `--timeout`, `--backend vercel --project --team`)
- `workflow init` — project init (coming soon)
- `workflow transform` — show SWC transform output per context (`--mode workflow|step`, `--check-serde`)
- `workflow validate` — validate workflow files, serde compliance (`--dir`, `--strict`)
- `workflow web` (alias `w`) — open web UI

### 2.8 Other
- Dynamic workflows (`advanced/dynamic-workflows.mdx`): `start()` with workflow source + `experimental_dynamic: { steps }` option; `dynamicWorkflowCode` capability; `uploadDynamicWorkflowCode`; env `WORKFLOW_EXPERIMENTAL_DYNAMIC_WORKFLOWS`.
- Encryption: AES-256-GCM (`packages/core/src/encryption.ts`), per-run key via `World.getEncryptionKeyForRun` (world derives, e.g. HKDF from deployment key), `RuntimeDecryptionError`.
- Serialization: CBOR queue transport (spec 3), compression (spec 5, `compressArguments` / `WORKFLOW_DISABLE_COMPRESSION`), `@workflow/serde` (`workflowSerialize`/`workflowDeserialize`), class serialization, serializable steps cookbook.
- Retention: `experimental_retention` start option (`'default' | 0`), stored as `$retention` attribute (spec ≥ 4), `RunExpiredError`; world env limits `WORKFLOW_POSTGRES_HOOK_RETENTION_LIMIT_DAYS` / `WORKFLOW_LOCAL_HOOK_RETENTION_LIMIT_DAYS`.
- Invoke: request/response via `Queue.invoke` gated by `capabilities.invoke` (vercel, postgres w/ `WORKFLOW_POSTGRES_INVOKE`).
- Snapshots: QuickJS VM memory snapshots (`experimental_snapshots`, optional optimization).
- Replay: event-sourcing replay with divergence detection (`ReplayDivergenceError`), replay payload cache, sealed log (spec 7).
- Deployment management: `deploymentId` (incl. `'latest'` via `resolveLatestDeploymentId`), cross-deployment spec-version probing, `region` placement, `replayedFromRunId` lineage (`recreateRunFromExisting`), environment attribution (`getEnvironment`, cross-env mismatch detection).
- Testing: `@workflow/vitest` package, server-based testing docs.

---

## 3. `WORKFLOW_*` runtime environment variables

### `packages/world-postgres/src`
`WORKFLOW_JSON_MODE`, `WORKFLOW_LOCAL_BASE_URL`, `WORKFLOW_POSTGRES_APPLICATION_MANAGED_SHUTDOWN`, `WORKFLOW_POSTGRES_BODY_TIMEOUT_MS`, `WORKFLOW_POSTGRES_HEADERS_TIMEOUT_MS`, `WORKFLOW_POSTGRES_HOOK_RETENTION_LIMIT_DAYS`, `WORKFLOW_POSTGRES_INVOKE`, `WORKFLOW_POSTGRES_JOB_PREFIX`, `WORKFLOW_POSTGRES_MAX_POOL_SIZE`, `WORKFLOW_POSTGRES_POLL_INTERVAL_MS`, `WORKFLOW_POSTGRES_RUN_STATUS_POLL_INTERVAL_MS`, `WORKFLOW_POSTGRES_URL`, `WORKFLOW_QUEUE_NAMESPACE`, `WORKFLOW_RUN_STATUSES`

### `packages/core/src` (non-test; 88 unique; incl. internal IPC/VM channels)
Core runtime/tuning: `WORKFLOW_BATCH_TRANSITIONS`, `WORKFLOW_DEFERRED_CHECK_DELAY_MS`, `WORKFLOW_DEPLOYMENT_MISMATCH_MAX_RETRIES`, `WORKFLOW_EXPERIMENTAL_DYNAMIC_WORKFLOWS`, `WORKFLOW_FRAMED_STREAM_MAX_RECONNECTS`, `WORKFLOW_FRAMED_STREAM_MAX_TOTAL_RECONNECTS`, `WORKFLOW_INLINE_OWNERSHIP_LEASE_SECONDS`, `WORKFLOW_LOCK_POLL_INTERVAL_MS`, `WORKFLOW_LOG_ORDER_DRAWS`, `WORKFLOW_MAX_EVENTS`, `WORKFLOW_MAX_INLINE_STEPS`, `WORKFLOW_MAX_QUEUE_DELIVERIES`, `WORKFLOW_NEAR_ELAPSED_WAIT_THRESHOLD_SECONDS`, `WORKFLOW_NODE_HTTP`, `WORKFLOW_OPEN_WAIT_CLOCK_SKEW_MS`, `WORKFLOW_OPTIMISTIC_INLINE_START`, `WORKFLOW_PRECONDITION_MAX_INPROCESS_RESTARTS`, `WORKFLOW_PRECONDITION_MAX_REINVOCATIONS`, `WORKFLOW_PRECONDITION_REINVOKE_DELAY_SECONDS`, `WORKFLOW_QUEUE_NAMESPACE`, `WORKFLOW_REPLAY_DIVERGENCE_MAX_RETRIES`, `WORKFLOW_REPLAY_TIMEOUT_MAX_RETRIES`, `WORKFLOW_RETAINED_VM`, `WORKFLOW_RETURN_VALUE_LONG_POLL`, `WORKFLOW_RETURN_VALUE_MAX_LONG_POLLS`, `WORKFLOW_RETURN_VALUE_POLL_INTERVAL_MS`, `WORKFLOW_RETURN_VALUE_WAIT_MS`, `WORKFLOW_SEALED_LOG`, `WORKFLOW_SEQUENTIAL_REPLAYS` (via world), `WORKFLOW_SLEEP`, `WORKFLOW_STABLE_ULID`, `WORKFLOW_STEP_CONTEXT_STORAGE`, `WORKFLOW_STEP_STREAM_DRAIN_TIMEOUT_MS`, `WORKFLOW_STREAM_DRAIN`, `WORKFLOW_STREAM_FLUSH_INTERVAL_MS`, `WORKFLOW_STREAM_FRAMING`, `WORKFLOW_STREAM_MAX_BUFFERED_BYTES`, `WORKFLOW_STREAM_MAX_BYTES_PER_BATCH`, `WORKFLOW_STREAM_MAX_CHUNKS_PER_BATCH`, `WORKFLOW_STREAM_MAX_INFLIGHT_CHUNKS`, `WORKFLOW_WAIT_CONTINUATION_MAX_DELAY_SECONDS`, `WORKFLOW_DISABLE_COMPRESSION` (referenced in start docs).
Internal/VM channel names (feature flags & sandbox keys): `WORKFLOW_ABORT_HOOK_CID`, `WORKFLOW_ABORT_HOOK_TOKEN`, `WORKFLOW_ABORT_LISTENER_ATTACHED`, `WORKFLOW_ABORT_READER_CANCEL`, `WORKFLOW_ABORT_STREAM_NAME`, `WORKFLOW_CONTEXT`, `WORKFLOW_CREATE_HOOK`, `WORKFLOW_GET_STREAM_ID`, `WORKFLOW_SET_ATTRIBUTES`, `WORKFLOW_STEP_STREAM_*`/`WORKFLOW_STREAM_NAME`, `WORKFLOW_STREAM_RELEASE`, `WORKFLOW_STREAM_SERVER_DEPLOYMENT_ID`, `WORKFLOW_STREAM_SERVER_PUBLIC_KEY`, `WORKFLOW_STREAM_SERVER_RUN_ID`, `WORKFLOW_STREAM_TYPE`, `WORKFLOW_USE_STEP`, `WORKFLOW_TRACE_MODE`, `WORKFLOW_TURBO`, `WORKFLOW_TEST_ENV_CONFIG_FIXTURE` (test).

### `packages/world-local` + `world-vercel` (adjacent worlds, for reference)
`WORKFLOW_LOCAL_BODY_TIMEOUT_MS`, `WORKFLOW_LOCAL_DATA_DIR`, `WORKFLOW_LOCAL_HEADERS_TIMEOUT_MS`, `WORKFLOW_LOCAL_HOOK_RETENTION_LIMIT_DAYS`, `WORKFLOW_LOCAL_QUEUE_CONCURRENCY`, `WORKFLOW_LOCAL_QUEUE_MAX_VISIBILITY`, `WORKFLOW_LOCAL_RECOVER_ACTIVE_RUNS`, `WORKFLOW_LOCAL_RUN_STATUS_POLL_INTERVAL_MS`, `WORKFLOW_DISABLE_ANALYTICS_READS`, `WORKFLOW_EVENTS_TRANSPORT`, `WORKFLOW_EVENTS_TRANSPORT_WS_OVERRIDE_WORKFLOWS`, `WORKFLOW_FLOW_ROUTE`, `WORKFLOW_H2_MULTIPLEX`, `WORKFLOW_INTERNAL_EVENTS_TRANSPORT_STRICT`, `WORKFLOW_MAX_CHUNKS_PER_REQUEST`, `WORKFLOW_REQUEST_TIMEOUT_MS`, `WORKFLOW_SERVER_SERVICE`, `WORKFLOW_SERVER_URL`, `WORKFLOW_SERVER_URL_OVERRIDE`, `WORKFLOW_STREAMS_TRANSPORT`, `WORKFLOW_TARGET_WORLD`, `WORKFLOW_TEST_LIMIT_OVERRIDES`, `WORKFLOW_VERCEL_AUTH_TOKEN`, `WORKFLOW_VERCEL_ENV`, `WORKFLOW_VERCEL_PROJECT`, `WORKFLOW_VERCEL_TEAM`.

Full tuning reference: `docs/content/docs/v5/configuration/runtime-tuning.mdx` (~51 documented vars).

---

## 4. Package versions (package.json)

| Package | Version |
|---|---|
| `workflow` | 5.2.3 |
| `@workflow/core` | 5.2.3 |
| `@workflow/world-vercel` | 5.2.3 |
| `@workflow/cli` | 5.0.6 |
| `@workflow/world` | 5.0.5 |
| `@workflow/world-local` | 5.1.2 |
| `@workflow/world-postgres` | 5.0.5 |
| `@workflow/world-testing` | 5.0.6 |
| `@workflow/world-sim` | 0.0.0 |
| `@workflow/errors` | 5.0.5 |
| `@workflow/next` / `nitro` / `nuxt` / `sveltekit` / `astro` / `vite` / `vitest` / `rollup` / `builders` / `web` / `web-shared` | 5.0.6 |
| `@workflow/nest` | 5.1.4 |
| `@workflow/ai` | 5.0.3 |
| `@workflow/utils` | 5.0.2 |
| `@workflow/swc-plugin` | 5.0.1 |
| `@workflow/serde` / `tsconfig` / `typescript-plugin` | 5.0.0 |

---

## Parity notes for third-party World implementations
- **Mandatory**: `Storage.runs.get/list`, `Storage.steps.get/list`, `Storage.events.create (both overloads)/get/list/listByCorrelationId`, `Storage.hooks.get/getByToken/list`, `Queue.getDeploymentId/queue/createQueueHandler`, `Streamer.streams.write/close/get/list/getChunks/getInfo`, `World.specVersion` (slots-based event IDs, spec 4–8).
- **Capability-gated**: `events.createBatch`, `runs.waitForTerminalStatus`, `runs.getMany`, `runs.experimentalSetAttributes`, `runs.cancelMany`, `Queue.invoke`, `Queue.queueBatch`, `Queue.registerHandler`, `Queue.registerRoute`, `streams.createWriteSession`, `streams.writeMulti`, `experimental_snapshots`, and all 8 `WorldCapabilities` flags.
- **Optional lifecycle**: `start/close/getRuntimeDeadline/getEnvironment/describeRun/validateRunExecutionContext/createRunId/regionForRunId/resolveLatestDeploymentId/getEncryptionKeyForRun/uploadDynamicWorkflowCode`, `analytics`, `telemetry`.
