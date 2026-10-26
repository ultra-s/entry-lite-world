/**
 * Entry-Lite World — createWorld(): the full World implementation.
 */
import { Pool } from 'pg';
const SPEC_VERSION_ENTRYLITE = Number(process.env.ENTRYLITE_SPEC_VERSION ?? 8); // workflow@5.2.x wants 8; workflow@5.0.0-beta.40 wants 5
import { createEvent } from './events.js';
import { createRunsStorage, createStepsStorage, createHooksStorage, createEventsReadStorage } from './storage.js';
import { createQueue, createStreamer } from './queue.js';
import { createExtras } from './extras.js';
import { newRunId, NAMESPACE, nsQueueName, nsJobKey, NS_CLAIM_WHERE } from './util.js';

export function createWorld(config: {
  connectionString?: string;
  maxPoolSize?: number;
  streamFlushIntervalMs?: number;
  /** Per-deployment queue namespace (overrides ENTRYLITE_QUEUE_NAMESPACE env). */
  queueNamespace?: string;
} = {}) {
  // Explicit config wins over env; both validated in util.ts at import time.
  if (config.queueNamespace !== undefined && config.queueNamespace !== process.env.ENTRYLITE_QUEUE_NAMESPACE) {
    process.env.ENTRYLITE_QUEUE_NAMESPACE = config.queueNamespace;
    if ((config.queueNamespace || '') !== NAMESPACE) {
      throw new Error('[entrylite-world] queueNamespace config must be set before module load (env ENTRYLITE_QUEUE_NAMESPACE), or match it — the namespace is process-wide by design.');
    }
  }
  if (NAMESPACE) {
    console.log(`[entrylite-world] queue namespace: "${NAMESPACE}" (isolation active; unset ENTRYLITE_QUEUE_NAMESPACE for legacy shared mode)`);
  }
  const connectionString =
    config.connectionString ??
    process.env.ENTRYLITE_WORLD_URL ??
    process.env.WORKFLOW_POSTGRES_URL ??
    process.env.DATABASE_URL ??
    'postgres://world:world@localhost:5432/workflow';
  const pool = new Pool({
    connectionString,
    max: config.maxPoolSize ?? 20,
  });

  const queue = createQueue(pool);
  const streamer = createStreamer(pool);
  const runs = createRunsStorage(pool);
  const steps = createStepsStorage(pool);
  const hooks = createHooksStorage(pool);
  const eventsRead = createEventsReadStorage(pool);
  // Full-parity extras (batch, bulk ops, analytics, routes, env).
  const extrasWorld = createExtras(pool, {
    nsQueueName,
    nsJobKey: (key: string) => nsJobKey(key) ?? key,
    getNamespace: () => NAMESPACE,
    queue,
    eventsRead,
    runsStorage: runs,
    mapRun: (row: any) => row,
    mapStep: (row: any) => row,
    mapHook: (row: any) => row,
  });

  // Eager sweeper: nitro module may never call world.start(); durable timers must run regardless.
  // Also a lost-wake backstop: a running run with no jobs, no waiting timers, and no events for
  // 90s gets a fresh run-level wake (idempotent; the runtime dedups by event log).
  const sweepLostWakes = async () => {
    try {
      const stale = await pool.query(
        `SELECT r.run_id, r.workflow_name FROM entrylite.workflow_runs r
         WHERE r.status='running'
           AND NOT EXISTS (SELECT 1 FROM entrylite.workflow_jobs j WHERE j.queue_name = $1 || r.workflow_name)
           AND NOT EXISTS (SELECT 1 FROM entrylite.workflow_waits w WHERE w.run_id = r.run_id AND w.status='waiting')
           AND NOT EXISTS (SELECT 1 FROM entrylite.workflow_events e WHERE e.run_id = r.run_id AND e.created_at > now() - interval '90 seconds')
         LIMIT 5`,
        [nsQueueName('__wkf_workflow_')],
      );
      for (const r0 of stale.rows) {
        await pool.query(
          `INSERT INTO entrylite.workflow_jobs (queue_name, payload, run_at, job_key)
           VALUES ($1, jsonb_build_object('runId', $2::text, 'requestedAt', to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')), now(), $3)
           ON CONFLICT (job_key) WHERE locked_at IS NULL DO NOTHING`,
          [nsQueueName(`__wkf_workflow_${r0.workflow_name}`), r0.run_id, nsQueueName(`lostwake_${r0.run_id}_${Math.floor(Date.now() / 90000)}`)],
        );
        if (process.env.ENTRYLITE_DEBUG) console.log('[entrylite-world] lost-wake backstop fired for', r0.run_id);
      }
    } catch (e: any) {
      console.error('[entrylite-world] lost-wake sweep:', String(e).slice(0, 200));
    }
  };
  const sweepTimersEager = async () => {
    try {
      const due = await pool.query(
        `SELECT w.wait_id, w.run_id, r.workflow_name FROM entrylite.workflow_waits w
         JOIN entrylite.workflow_runs r ON r.run_id = w.run_id
         WHERE w.status='waiting' AND w.resume_at <= now() LIMIT 50`,
      );
      for (const w of due.rows) {
        const corr = w.wait_id.slice(w.run_id.length + 1);
        try {
          await createEvent(pool, w.run_id, {
            eventType: 'wait_completed',
            correlationId: corr,
            specVersion: 8,
          }, [{ queueName: nsQueueName(`__wkf_workflow_${w.workflow_name}`), body: { runId: w.run_id } }]);
        } catch (e: any) {
          if (!String(e).includes('already') && !String(e).includes('NotFound')) console.error('[entrylite-world] timer sweep:', String(e).slice(0, 200));
        }
      }
    } catch (e) { /* db not ready yet */ }
  };
  const sweeperTimer = setInterval(() => { void sweepTimersEager(); void sweepLostWakes(); queue.kick(); }, 200);
  sweeperTimer.unref?.();
  // Eager worker: start queue consumption without relying on world.start() being called.
  queue.start().catch((e) => console.error('[entrylite-world] queue start failed:', String(e).slice(0, 200)));

  const world = {
    specVersion: SPEC_VERSION_ENTRYLITE,
    // No fixed platform kill time: invocations run until the handler returns.
    getRuntimeDeadline: async (): Promise<Date | undefined> => undefined,
    // Single-region world: raw ULID run ids (the runtime prefixes `wrun_` itself).
    createRunId: (): string => {
      const id = newRunId();
      return id.startsWith('wrun_') ? id.slice(5) : id;
    },
    regionForRunId: (_runId: string): string | null => null,
    // Dynamic runs: single-tenant local storage, no extra limits beyond defaults.
    validateRunExecutionContext: (_value: Record<string, unknown>): void => {},
    // Metadata-only description for observability surfaces (must not throw).
    describeRun: (run: Readonly<Record<string, unknown>>): Record<string, string | null> => ({
      workflowName: typeof run.workflowName === 'string' ? run.workflowName : null,
      status: typeof run.status === 'string' ? run.status : null,
      deploymentId: typeof run.deploymentId === 'string' ? run.deploymentId : null,
    }),
    capabilities: {
      hookRetention: { active: true },
      // Queue consumption is maxConcurrency-limited (per-run flow topics with
      // maxConcurrency: 1 serialize a run's orchestrator invocations).
      maxConcurrency: true,
      // Queue rows are namespaced per deployment (ENTRYLITE_QUEUE_NAMESPACE):
      // claims, leases, retries, recovery, and sweeps only touch this
      // deployment's rows. True whenever the namespace is set.
      queueNamespace: !!NAMESPACE,
      // events.create deduplicates concurrent hook_received writes carrying the
      // same (runId, resumeId) via the (run_id, resume_id) unique index, and
      // accepts experimental_force at createHook time (locked re-check + insert).
      hookResumeDedup: true,
      hookForceClaim: true,
      // run_created / resilient run_started persist dynamicWorkflowCode and
      // runs.get echoes it back for every replay.
      dynamicWorkflowCode: true,
    },
    runs: { ...runs, ...extrasWorld._runsAdditions },
    steps,
    hooks,
    events: {
      ...eventsRead,
      ...extrasWorld._eventsAdditions,
      async create(runId: string | null, data: any, params?: any): Promise<any> {
        if (params?.resumeId) data = { ...data, resumeId: params.resumeId };
        // Derive wake messages from the event type — the queue coordinates the
        // runtime uses. flow trigger wakes go to the workflow name queue;
        // hook/wait wakes target the run's own queue.
        const wakes: Array<{ queueName: string; body: unknown; delaySeconds?: number; jobKey?: string }> = [];
        if (data.eventType === 'wait_completed' || data.eventType === 'hook_received') {
          wakes.push({
            queueName: nsQueueName(`__wkf_workflow_${(await pool.query('SELECT workflow_name FROM entrylite.workflow_runs WHERE run_id = $1', [runId])).rows[0]?.workflow_name ?? ''}`),
            body: await (async () => { const rr = await pool.query('SELECT * FROM entrylite.workflow_runs WHERE run_id = $1', [runId]); const row = rr.rows[0] ?? {}; return { runId, requestedAt: new Date().toISOString(), traceCarrier: {}, input: row.input ?? undefined, deploymentId: row.deployment_id, workflowName: row.workflow_name, executionContext: row.execution_context ?? undefined, attributes: row.attributes ?? {}, specVersion: row.spec_version ?? 8, waitContinuation: { attempt: 0, correlationId: data.correlationId } }; })(),
          });
        }
        const result = await createEvent(pool, runId, data, wakes);
        return result;
      },
      get: eventsRead.get,
      list: eventsRead.list,
      listByCorrelationId: eventsRead.listByCorrelationId,
    },
    ...streamer,
    ...queue,
    ...extrasWorld._queueAdditions,
    analytics: extrasWorld._analytics,
    getEnvironment: extrasWorld._getEnvironment,
    ...(config.streamFlushIntervalMs !== undefined ? { streamFlushIntervalMs: config.streamFlushIntervalMs } : {}),
    // Non-critical observability hook: correlate step execution (no-op sink).
    telemetry: {
      recordStepExecution: (stepId: string): void => {
        if (process.env.ENTRYLITE_DEBUG) console.log('[entrylite-world] step execution:', stepId);
      },
    },
    async start(): Promise<void> {
      await queue.start();
      // Crash recovery: re-enqueue runs left pending/running by a dead process.
      // Namespace-scoped: only re-enqueue onto queue rows this deployment owns
      // (runs whose workflow queue has a job row in our namespace, or no job
      // rows at all anywhere — the latter covers a crashed sole deployment).
      const r = await pool.query(
        `SELECT run_id, workflow_name FROM entrylite.workflow_runs
         WHERE status IN ('pending','running')
           AND NOT EXISTS (
             SELECT 1 FROM entrylite.workflow_jobs j
             WHERE j.queue_name = $1 || workflow_name
           )`,
        [nsQueueName('__wkf_workflow_')],
      );
      for (const row of r.rows) {
        await queue.queue(`q_entrylite::${row.workflow_name}`, { runId: row.run_id });
      }
    },
    async close(): Promise<void> {
      // Stop background loops BEFORE tearing down the pool, otherwise the
      // lost-wake sweeper fires against a closed pool ("Cannot use a pool
      // after calling end on the pool").
      clearInterval(sweeperTimer);
      await queue.close();
      await streamer.close();
      await pool.end();
    },
    async clear(): Promise<void> {
      // test helper
      await pool.query(`TRUNCATE entrylite.workflow_jobs, entrylite.workflow_stream_chunks,
        entrylite.workflow_waits, entrylite.workflow_hooks, entrylite.workflow_steps,
        entrylite.workflow_events, entrylite.workflow_event_slots, entrylite.workflow_runs`);
    },
  };
  return world;
}
