# Feature-Parity Audit: `@entrylite/world` (entry-lite-world) vs `@workflow/world` 5.0.5 / workflow@5.2.3

Audited: `/home/agentuser/entry-lite-world/src/{index,queue,events,storage,util}.ts`, `schema.sql`, `bootstrap.ts` against the official `.d.ts` in `/home/agentuser/entry-agents/node_modules/.pnpm/@workflow+world@5.0.5/node_modules/@workflow/world/dist/` (the world spec shipped alongside `@workflow/core@5.2.3`; `SPEC_VERSION_CURRENT = SPEC_VERSION_MAX_SUPPORTED = 8`).

Legend: ✅ implemented · 🟡 partial · ❌ missing. "Optional" per the World interface does not change the marking — it only changes the runtime consequence.

---

## 1. Core required surface

| Item | Status | Evidence |
|---|---|---|
| `specVersion` | ✅ | `index.ts:100` — `SPEC_VERSION_ENTRYLITE` = 8 (`index.ts:5`), matching the runtime floor/current |
| `Queue.getDeploymentId()` | ✅ | `queue.ts:200` — returns `entrylite` / `entrylite-${NAMESPACE}` |
| `Queue.queue(queueName, message, opts)` | ✅ | `queue.ts:148-171` — durable `workflow_jobs` insert, `SKIP LOCKED` worker |
| `Queue.createQueueHandler(prefix, handler)` | 🟡 | `queue.ts:172-213` — returns an HTTP `(req)=>Response`; also delivers **in-process** via a direct `fetch` to the flow route (`queue.ts:35-38`), so the returned handler is exercised over HTTP. `meta` provides `attempt`, `queueName`, `messageId` but **not** `deliveryCount` / `createdAt` |
| `Storage.runs.get` (+`resolveData`) | ✅ | `storage.ts:16-56`; `resolveData:'none'` honored; throws `WorkflowRunNotFoundError` |
| `Storage.runs.list` | 🟡 | `storage.ts:60-90` — cursor pagination, `sortOrder`, `workflowName` ok; `status` filter accepts a **single** status only (`status = $2`), the SDK's `status: WorkflowRunStatus[]` array form is not handled |
| `Storage.steps.get` / `steps.list` | ✅ | `storage.ts:98-152` (resolveData honored) |
| `Storage.events.create` (all event types → entity materialization) | ✅ | `events.ts:138-598` — run_created/run_started, run terminal events, step lifecycle incl. `step_retrying` with `retryAfter`, hook_created/received/disposed, wait_created/completed, `attr_set` merge (`events.ts:561-570`) |
| Slot-numbered event ids (`evnt_` + dense 1-based position) | ✅ | `events.ts:31-49` — `evnt_` + 26-char zero-padded counter (spec v6 contract) |
| `Storage.events.get` / `list` / `listByCorrelationId` | ✅ | `storage.ts:211-267`; `mapEventRow` round-trips `resumeId` (`storage.ts:265`) |
| `Storage.hooks.get` / `getByToken` / `list` | ✅ | `storage.ts:155-213`; `getByToken` filters expired retention and terminal runs (`storage.ts:162-168`) |
| `Streamer` — `write`, `writeMulti`, `close`, `get(startIndex)`, `list`, `getChunks`, `getInfo` | ✅ | `queue.ts:229-395` — Postgres chunk table + LISTEN/NOTIFY live reader |
| `streamFlushIntervalMs` | ✅ | `index.ts:155` (config pass-through) |
| `start()` / `close()` | ✅ | `index.ts:167-194` — queue start + crash re-enqueue of orphaned runs; close tears down loops/pool in safe order |
| `createRunId` / `regionForRunId` | ✅ | `index.ts:104-108` (bare ULID, `null` region) |
| `getRuntimeDeadline` | ✅ (returns `undefined`) | `index.ts:102` |
| `validateRunExecutionContext` | ✅ (no-op pass-through) | `index.ts:110` |
| `describeRun` | ✅ | `index.ts:112-116` |
| `telemetry.recordStepExecution` | ✅ (debug-log sink) | `index.ts:158-163` |

## 2. WorldCapabilities (`index.ts:117-135`)

| Capability | Declared | Actually implemented | Verdict |
|---|---|---|---|
| `hookRetention: { active: true }` | yes | `tokenRetentionUntil` persisted (`events.ts:475`), respected in `getByToken` (`storage.ts:164`), terminal-run token reclamation on `hook_created` (`events.ts:458-461`) | ✅ |
| `maxConcurrency: true` | yes | worker concurrency limited (`ENTRYLITE_QUEUE_CONCURRENCY`, `queue.ts:20`); per-topic `maxConcurrency:1` semantics rely on per-run flow queues | ✅/🟡 (declared support; no build-side verification possible, as the spec itself notes) |
| `hookResumeDedup: true` | yes | `(run_id, resume_id)` partial unique index (`schema.sql:44`) + ON CONFLICT dedup with canonical-event return (`events.ts:493-518`); `resumeId` round-tripped on read (`storage.ts:265`) | ✅ |
| `hookForceClaim: true` | **yes** | **No `experimental_force` / `forceClaimedBy` / `HookForceClaimedError` handling anywhere in `events.ts`** (grep: zero hits). `hook_created` on a live-owned token throws plain `EntityConflictError` (`events.ts:479-482`) | ⚠️ **Declared but NOT implemented** — runtime will permit `createHook({experimental_force:true})` and get non-conformant behavior. Either implement the TLA-specified takeover or unset the capability |
| `dynamicWorkflowCode: true` | yes | persisted on `run_created` and resilient `run_started` (`events.ts:238-285`), echoed by `runs.get` (`storage.ts:54-55`, `events.ts:65`) | ✅ |
| `queueNamespace` (custom, non-spec key) | yes | deployment isolation via `ENTRYLITE_QUEUE_NAMESPACE`-stamped queue rows, claim scoping (`queue.ts:92-101`), LISTEN payload filtering (`queue.ts:125-129`), 409 on foreign-namespace delivery (`queue.ts:181-184`) | ✅ (extension) |
| `invoke`, `inBandFence`, `deploymentAffinity` | not declared | not implemented — correct fail-closed posture | ✅ (consistent) |

## 3. Optional World methods — missing

| Method | Status | Consequence |
|---|---|---|
| `runs.waitForTerminalStatus` | ❌ missing | Runtime falls back to interval polling (`WORKFLOW_RETURN_VALUE_POLL_INTERVAL_MS`); `await run.returnValue` is slower. `index.ts` sweeps/NOTIFY infra would make a LISTEN/NOTIFY implementation cheap |
| `runs.getMany` | ❌ missing | Per-run `get` fallback |
| `runs.experimentalSetAttributes` | ❌ missing | Core `setAttributes` feature-detects and no-ops with a warning. Note the World *does* handle `attr_set` events (`events.ts:561-570`), so the event-driven path exists once core dispatches through `events.create` |
| `runs.cancelMany` | ❌ missing | Core falls back to bounded-concurrency single-run cancel via `run_cancelled` events (`events.ts:345` handles those) |
| `events.createBatch` | ❌ missing | Runtime batches only when implemented; every write takes single-event `create`. Performance, not correctness |
| `experimental_snapshots` (save/load/delete) | ❌ missing | Every invocation does full event replay — correct, slower |
| `analytics` (runs/attributes/steps/events/hooks/waits get+list, ~14 methods) | ❌ missing entirely | Observability surfaces degrade to canonical storage APIs |
| `invoke()` + `capabilities.invoke` | ❌ missing | Request/response invoke() unavailable |
| `queueBatch` | ❌ missing | Fan-outs publish one message at a time (N round trips → N inserts; cheap locally but N statements) |
| `isDeploymentUnavailableError` | ❌ missing | No deployment-affinity machinery to need it |
| `registerHandler` / `registerRoute` | ❌ missing | Deliveries are hardcoded to `/.well-known/workflow/v1/flow` via `BASE_URL` (`queue.ts:16-18, 35-38`); a consumer registering its own prefix route would not be honored. Acceptable for a workflow-only world, but a divergence from the prefix-routing contract |
| `getEncryptionKeyForRun` | ❌ missing | **Encryption disabled** — all payloads stored plaintext |
| `getEnvironment` | ❌ missing | Cross-tenant environment check skipped (single-tenant, acceptable) |
| `resolveLatestDeploymentId` / `uploadDynamicWorkflowCode` | ❌ missing | Both explicitly optional for non-Vercel worlds; `start({deploymentId:'latest'})` and oversized dynamic-code uploads unavailable |
| `Streamer.streams.createWriteSession` (stateful writer) | ❌ missing | Stateless `write`/`writeMulti`/`close` path is used, which is fully supported |

## 4. User-facing SDK features

| Feature | Status | Evidence / notes |
|---|---|---|
| Hooks (`createHook`/`await hook`/resume) | ✅ | hook_created/received/disposed with token uniqueness, conflict detection (`events.ts:455-525`); queue wake on `hook_received` (`index.ts:142-152`) |
| Hook minimum retention (`experimental_minRetention`) | ✅ | `hookRetention` active; retention timestamp persisted and enforced |
| Hook force-claim (`experimental_force`) | ❌ (capability over-claims) | See §2 |
| Lazy hook resume dedup (`resumeId`) | ✅ | `events.ts:493-518`, `schema.sql:44` |
| Waits / `sleep()` | ✅ | `workflow_waits` table + wait_created/completed + **eager 200 ms timer sweeper** firing `wait_completed` (`index.ts:73-94`); `delaySeconds` on queue wakes supported (`events.ts:584`, `queue.ts:163-165`) |
| Step retry policy knobs (`maxAttempts`, retry after) | ✅ | `step_retrying` updates `attempt` + `retry_after` (`events.ts:440-459`); `mapStep` exposes them |
| Idempotency (`opts.idempotencyKey`) | ✅ | job_key unique index (`queue.ts:166`, `schema.sql:117`); hook resumes deduped |
| Streaming (`workflowWritable`, readable streams) | ✅ | Full Streamer minus `createWriteSession`; live tail via LISTEN/NOTIFY (`queue.ts:232-260`) |
| Dynamic workflow code (`start()` with a function) | ✅ | Inline storage path (`events.ts:249-250`); `uploadDynamicWorkflowCode` tail path missing (per spec, worlds that store runs whole may leave it unset) |
| Queue concurrency control | 🟡 | Global `CONCURRENCY` (50) + per-queue-topic maxConcurrency declared; no per-topic fairness — one hot workflow can starve others in `ORDER BY run_at, id` claiming (`queue.ts:97-104`) |
| Deployment isolation | ✅ | Namespace-scoped queue rows, claims, LISTEN channels, and 409 guard (`queue.ts:92-101, 181-184`); tested in `test/namespace-*.test.mjs` |
| Deployment affinity / multi-region | ❌ | Single region, `regionForRunId → null`, no `DEPLOYMENT_MISMATCH` guard (capability correctly unset) |
| Observability hooks (`describeRun`, `telemetry`) | ✅ | `index.ts:112-116, 158-163` |
| Telemetry / OTel | 🟡 | Only the world-side `telemetry.recordStepExecution` sink; no OTel trace-carrier propagation of its own (carrier passes through queue payloads as opaque data, which is sufficient for core) |
| Run search/list APIs | 🟡 | `runs.list` works but no array-status filter; no `analytics.*`; no `waits`/`hooks` cross-run search beyond `hooks.list` |
| Long-poll `await run.returnValue` fast path | ❌ | `waitForTerminalStatus` missing → interval polling |
| Queue `headers`, `specVersion`, `region` opts | 🟡 | Ignored by `queue()` (`queue.ts:148`); harmless in single-tenant deployments, `specVersion` transport selection unused (JSON only, no CBOR) |

## 5. Correctness risks spotted

1. **`capabilities.hookForceClaim: true` is not backed by code** (`index.ts:130`). The spec requires an atomic victim-log `hook_disposed{forceClaimedBy}` + token re-point + `HookForceClaimedError` semantics (formalized in `HookForceClaim.tla`). `events.ts` has zero force handling. Highest-priority fix or capability removal.
2. **Slot allocation race** — `allocateEventId` (`events.ts:31-49`) computes `COUNT(*)+1` inside the caller's tx without locking the `workflow_event_slots` row. Two concurrent creates for one run can compute the same slot; the loser fails on the event PK/unique instead of the required "bump and report" (advance to the next free slot and return skipped events). `SELECT ... FROM workflow_event_slots WHERE run_id=$1 FOR UPDATE` first would fix it.
3. **`messageId` not stable across redeliveries** — `deliver()` mints a fresh `newMessageId()` per attempt (`queue.ts:31`), violating the spec's SHOULD. Degrades inline step-ownership lease recovery to the delayed-backstop path (graceful per spec, but adds crash-recovery latency). The DB job row id would make a stable id trivial.
4. `runs.list` status filter ignores the array form (`storage.ts:66`) — a caller passing `status: ['pending','running']` silently gets unfiltered results.
5. `events.list` always materializes full `eventData`, ignoring `resolveData:'skip-step-inputs'` — allowed ("MUST treat non-none as all") but leaves quadratic step-input replay traffic on the table.

## 6. Test coverage

`test/contract-conformance.test.mjs` checks capabilities, createRunId shape, dynamicWorkflowCode round-trip, hook_received dedup convergence, and the official duplicate-event fixtures. Dedicated e2e/namespace/perf suites cover isolation, 100-instance, and long-run scenarios. **No test exercises force-claim, in-band fence, createBatch, waitForTerminalStatus, or encryption** — matching the missing implementations.

## 7. Summary

- **Core required surface: essentially complete and conformant** (spec v8, slot ids, event-sourced entity materialization, hooks, waits, streaming, durable Postgres queue with namespace isolation).
- **Optional optimizations missing** (batch writes, snapshots, analytics, long-poll, invoke, encryption) — all fail closed correctly except where noted.
- **One conformance defect to fix now:** `hookForceClaim` declared without implementation.
- **Two robustness fixes recommended:** slot-allocation locking; stable `messageId` per job.
