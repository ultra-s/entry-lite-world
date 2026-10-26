/**
 * Full-parity extras: events.createBatch, runs.waitForTerminalStatus /
 * getMany / cancelMany / experimentalSetAttributes, queue.queueBatch,
 * analytics surface, registerRoute/registerHandler, getEnvironment.
 *
 * All operations are namespace-aware and use the same schema as the core
 * world. Capabilities declared in index.ts.
 */
import crypto from 'node:crypto';
import type { Pool } from 'pg';

export function createExtras(pool: Pool, opts: {
  nsQueueName: (name: string) => string;
  nsJobKey: (key: string) => string;
  getNamespace: () => string;
  queue: {
    queue: (queueName: string, message: unknown, opts?: any) => Promise<{ messageId: string | null }>;
    start?: () => Promise<void>;
  };
  eventsRead: any;
  runsStorage: any;
  mapRun: (row: any) => any;
  mapStep: (row: any) => any;
  mapHook: (row: any) => any;
  newEventId?: () => string;
}) {
  const { nsQueueName, nsJobKey, queue, eventsRead, runsStorage, mapRun, mapStep, mapHook } = opts;

  // ---------------------------------------------------------------- routes
  const routes = new Map<string, (req: Request) => Promise<Response>>();
  const handlers = new Map<string, (req: Request) => Promise<Response>>();

  // ------------------------------------------------------------ helpers
  async function wakeRun(runId: string, extra?: Record<string, unknown>, delaySeconds?: number): Promise<void> {
    const r = await pool.query(
      `SELECT workflow_name, deployment_id, input, execution_context, attributes, spec_version
       FROM entrylite.workflow_runs WHERE run_id = $1`, [runId]);
    const row = r.rows[0];
    if (!row) return;
    const body = {
      runId,
      requestedAt: new Date().toISOString(),
      traceCarrier: {},
      input: row.input ?? undefined,
      deploymentId: row.deployment_id,
      workflowName: row.workflow_name,
      executionContext: row.execution_context ?? undefined,
      attributes: row.attributes ?? {},
      specVersion: row.spec_version ?? 8,
      ...extra,
    };
    await queue.queue(nsQueueName(`__wkf_workflow_${row.workflow_name}`), body,
      delaySeconds ? { delaySeconds } : undefined);
  }

  const TERMINAL = ['completed', 'failed', 'cancelled'];

  // ------------------------------------------------------- events.createBatch
  async function createBatch(runId: string, events: any[], _params?: any): Promise<{ results: any[] }> {
    // Per-event atomic commit (the contract is atomic-per-attempt, not
    // all-or-nothing): each event takes the single-create path, which is a
    // durable atomic write with slot allocation; rejections report the
    // single-path error and commit nothing. Survivors commit in request order.
    const BATCHABLE = new Set(['step_created', 'step_started', 'step_completed', 'step_failed',
      'step_retrying', 'step_timed_out', 'wait_created', 'wait_completed', 'hook_received']);
    const results: any[] = [];
    for (const item of events) {
      const ev = item?.event ?? item;
      try {
        if (!BATCHABLE.has(ev.eventType)) {
          throw Object.assign(new Error(`Event type ${ev.eventType} is not expressible in a batch`), { statusCode: 400, code: 'batch_not_expressible' });
        }
        if (ev.eventType === 'step_started' && ev.eventData?.payload !== undefined) {
          throw Object.assign(new Error('step_started carrying a payload rejects the batch'), { statusCode: 400, code: 'batch_invalid_pair' });
        }
        const { createEvent } = await import('./events.js');
        const created = await createEvent(pool, runId, ev);
        results.push({ status: 200 as const, event: created.event, step: created.step, wait: created.wait, run: created.run });
        if (ev.eventType === 'wait_completed' || ev.eventType === 'hook_received') {
          await wakeRun(runId);
        }
      } catch (err: any) {
        results.push({
          status: err.statusCode ?? 409,
          error: err.code ?? 'conflict',
          message: String(err.message ?? err),
        });
      }
    }
    return { results };
  }

  // ------------------------------------------------ runs.waitForTerminalStatus
  async function waitForTerminalStatus(id: string, params?: any): Promise<any> {
    const timeoutMs = Math.min(params?.timeoutMs ?? 30_000, 120_000);
    const signal = params?.signal;
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      if (signal?.aborted) throw new Error('Aborted');
      // snapshot read
      const snap = await runsStorage.get(id, { resolveData: params?.resolveData ?? 'all' }).catch(() => null);
      if (snap && TERMINAL.includes(snap.status)) return snap;
      const remaining = deadline - Date.now();
      if (remaining <= 0) return snap; // degrade to snapshot, never hang past budget
      // LISTEN-backed wake would go here; 250ms poll degrades gracefully.
      await new Promise(res => setTimeout(res, Math.min(250, remaining)));
    }
  }

  // ------------------------------------------------------------------ getMany
  async function getMany(ids: readonly string[], params?: any): Promise<any[]> {
    if (ids.length === 0) return [];
    const resolveData = params?.resolveData ?? 'all';
    const r = await pool.query(
      `SELECT * FROM entrylite.workflow_runs WHERE run_id = ANY($1)`,
      [ids as string[]],
    );
    const byId = new Map(r.rows.map((row: any) => [row.run_id, row]));
    return ids.map(id => {
      const row = byId.get(id);
      if (!row) return null;
      if (resolveData === 'none') {
        return { runId: row.run_id, deploymentId: row.deployment_id, workflowName: row.workflow_name, status: row.status, attributes: row.attributes ?? {}, createdAt: row.created_at, updatedAt: row.updated_at, startedAt: row.started_at ?? undefined, completedAt: row.completed_at ?? undefined };
      }
      return mapRun(row);
    });
  }

  // ---------------------------------------------------------------- cancelMany
  async function cancelMany(request: { runIds: readonly string[]; cancelReason?: string }): Promise<any> {
    const results: any[] = [];
    for (const runId of request.runIds.slice(0, 500)) {
      try {
        const before = await pool.query(`SELECT status FROM entrylite.workflow_runs WHERE run_id=$1`, [runId]);
        const st = before.rows[0]?.status;
        if (!st) { results.push({ runId, outcome: 'not_found' }); continue; }
        if (st === 'cancelled') { results.push({ runId, outcome: 'already_cancelled' }); continue; }
        if (TERMINAL.includes(st)) { results.push({ runId, outcome: 'not_cancellable' }); continue; }
        const res = await runsStorage.cancel(runId);
        if (res?.status === 'cancelled') {
          results.push({ runId, outcome: 'cancelled' });
          // wake the run's queue so an in-flight orchestrator observes cancellation
          await wakeRun(runId).catch(() => {});
        } else {
          results.push({ runId, outcome: 'not_cancellable' });
        }
      } catch {
        results.push({ runId, outcome: 'failed' });
      }
    }
    const counts = results.reduce((m, r) => { m[r.outcome] = (m[r.outcome] ?? 0) + 1; return m; }, {} as Record<string, number>);
    return { results, summary: counts };
  }

  // --------------------------------------------- experimentalSetAttributes
  async function experimentalSetAttributes(runId: string, changes: Array<{ key: string; value: string | null }>, options?: { allowReservedAttributes?: boolean }): Promise<{ attributes: Record<string, string> }> {
    if (!options?.allowReservedAttributes) {
      const reserved = changes.find(c => c.key.startsWith('$'));
      if (reserved) {
        throw Object.assign(new Error(`Attribute key "${reserved.key}" uses the reserved $ prefix`), { statusCode: 400, code: 'reserved_attribute' });
      }
    }
    const cur = await pool.query(`SELECT attributes FROM entrylite.workflow_runs WHERE run_id=$1`, [runId]);
    if (!cur.rows[0]) {
      const { WorkflowRunNotFoundError } = await import('@workflow/errors');
      throw new WorkflowRunNotFoundError(runId);
    }
    const attrs: Record<string, string> = { ...(cur.rows[0].attributes ?? {}) };
    for (const c of changes) {
      if (c.value === null) delete attrs[c.key];
      else attrs[c.key] = c.value;
    }
    await pool.query(`UPDATE entrylite.workflow_runs SET attributes=$2, updated_at=now() WHERE run_id=$1`, [runId, JSON.stringify(attrs)]);
    return { attributes: attrs };
  }

  // ---------------------------------------------------------------- analytics
  function analytics() {
    const page = (rows: any[], params: any) => {
      const limit = params?.pagination?.limit ?? 50;
      const offset = params?.pagination?.cursor ? Number(params.pagination.cursor) : 0;
      const slice = rows.slice(offset, offset + limit);
      const next = offset + slice.length < rows.length ? String(offset + slice.length) : undefined;
      return { data: slice, pagination: { hasMore: next !== undefined, ...(next ? { cursor: next } : {}) } };
    };
    const evRow = (row: any) => ({
      eventId: row.event_id,
      runId: row.run_id,
      eventType: row.event_type,
      correlationId: row.correlation_id ?? null,
      entityId: row.correlation_id ?? null,
      stepName: null,
      workflowName: row.workflow_name,
      deploymentId: row.deployment_id ?? 'entrylite',
      specVersion: row.spec_version ?? 8,
      runCreatedAt: row.run_created_at ?? row.created_at,
      createdAt: row.created_at,
    });
    return {
      runs: {
        async get(runId: string) {
          const r = await pool.query(`SELECT * FROM entrylite.workflow_runs WHERE run_id=$1`, [runId]);
          if (!r.rows[0]) {
            const { WorkflowRunNotFoundError } = await import('@workflow/errors');
            throw new WorkflowRunNotFoundError(runId);
          }
          const row = r.rows[0];
          return {
            runId: row.run_id, status: row.status, workflowName: row.workflow_name,
            attributes: row.attributes ?? {}, createdAt: row.created_at,
            startedAt: row.started_at ?? null, completedAt: row.completed_at ?? null,
          };
        },
        async list(params?: any) {
          const conds: string[] = []; const vals: any[] = [];
          if (params?.workflowName) { vals.push(params.workflowName); conds.push(`workflow_name=$${vals.length}`); }
          if (params?.status) {
            if (Array.isArray(params.status)) { vals.push(params.status); conds.push(`status=ANY($${vals.length})`); }
            else { vals.push(params.status); conds.push(`status=$${vals.length}`); }
          }
          if (params?.startTime && params?.endTime) {
            vals.push(params.startTime, params.endTime);
            conds.push(`created_at BETWEEN $${vals.length - 1} AND $${vals.length}`);
          }
          const where = conds.length ? `WHERE ${conds.join(' AND ')}` : '';
          const r = await pool.query(
            `SELECT * FROM entrylite.workflow_runs ${where} ORDER BY created_at DESC`, vals);
          return page(r.rows.map((row: any) => ({
            runId: row.run_id, status: row.status, workflowName: row.workflow_name,
            attributes: row.attributes ?? {}, createdAt: row.created_at,
            startedAt: row.started_at ?? null, completedAt: row.completed_at ?? null,
          })), params);
        },
      },
      attributes: {
        async list(params?: any) {
          const r = await pool.query(`SELECT attributes, created_at FROM entrylite.workflow_runs WHERE attributes <> '{}'::jsonb ORDER BY created_at DESC`);
          const agg = new Map<string, { count: number; first: Date; last: Date }>();
          for (const row of r.rows) {
            for (const [k] of Object.entries(row.attributes ?? {})) {
              const a = agg.get(k) ?? { count: 0, first: row.created_at, last: row.created_at };
              a.count++;
              if (row.created_at < a.first) a.first = row.created_at;
              if (row.created_at > a.last) a.last = row.created_at;
              agg.set(k, a);
            }
          }
          const rows = [...agg.entries()].map(([key, a]) => ({ key, runCount: a.count, firstSeenAt: a.first, lastSeenAt: a.last }))
            .sort((a, b) => a.key.localeCompare(b.key));
          return page(rows, params);
        },
      },
      steps: {
        async get(runId: string, stepId: string) {
          const r = await pool.query(`SELECT * FROM entrylite.workflow_steps WHERE run_id=$1 AND step_id=$2`, [runId, stepId]);
          if (!r.rows[0]) {
            const { WorkflowStepNotFoundError } = await import('@workflow/errors').catch(() => ({}) as any) ?? {};
            throw new Error(`Step "${stepId}" not found`);
          }
          const row = r.rows[0];
          return { runId: row.run_id, stepId: row.step_id, status: row.status, createdAt: row.created_at, startedAt: row.started_at ?? null, completedAt: row.completed_at ?? null };
        },
        async list(params: any) {
          const r = await pool.query(`SELECT * FROM entrylite.workflow_steps WHERE run_id=$1 ORDER BY created_at ASC`, [params.runId]);
          return page(r.rows.map((row: any) => ({ runId: row.run_id, stepId: row.step_id, status: row.status, createdAt: row.created_at, startedAt: row.started_at ?? null, completedAt: row.completed_at ?? null })), params);
        },
      },
      events: {
        async get(runId: string, eventId: string) {
          const r = await pool.query(
            `SELECT e.*, r.workflow_name, r.created_at AS run_created_at
             FROM entrylite.workflow_events e JOIN entrylite.workflow_runs r USING (run_id)
             WHERE e.run_id=$1 AND e.event_id=$2`, [runId, eventId]);
          if (!r.rows[0]) throw new Error(`Event "${eventId}" not found`);
          return evRow(r.rows[0]);
        },
        async getMany(runId: string, eventIds: readonly string[]) {
          if (eventIds.length === 0) return [];
          const r = await pool.query(
            `SELECT e.*, r.workflow_name, r.created_at AS run_created_at
             FROM entrylite.workflow_events e JOIN entrylite.workflow_runs r USING (run_id)
             WHERE e.run_id=$1 AND e.event_id=ANY($2)`, [runId, [...eventIds] as string[]]);
          return r.rows.map(evRow);
        },
        async list(params: any) {
          const conds = ['e.run_id=$1']; const vals = [params.runId];
          if (params.eventType) { vals.push(params.eventType); conds.push(`e.event_type=$${vals.length}`); }
          const r = await pool.query(
            `SELECT e.*, r.workflow_name, r.created_at AS run_created_at
             FROM entrylite.workflow_events e JOIN entrylite.workflow_runs r USING (run_id)
             WHERE ${conds.join(' AND ')} ORDER BY e.created_at ASC`, vals);
          return page(r.rows.map(evRow), params);
        },
        async listByCorrelationId(params: any) {
          const r = await pool.query(
            `SELECT e.*, r.workflow_name, r.created_at AS run_created_at
             FROM entrylite.workflow_events e JOIN entrylite.workflow_runs r USING (run_id)
             WHERE e.run_id=$1 AND e.correlation_id=$2 ORDER BY e.created_at ASC`,
            [params.runId, params.correlationId]);
          return page(r.rows.map(evRow), params);
        },
      },
      hooks: {
        async get(hookId: string, _params?: any) {
          const r = await pool.query(`SELECT * FROM entrylite.workflow_hooks WHERE hook_id=$1`, [hookId]);
          if (!r.rows[0]) throw new Error(`Hook "${hookId}" not found`);
          const row = r.rows[0];
          return { runId: row.run_id, hookId: row.hook_id, status: 'created', createdAt: row.created_at ?? new Date(), updatedAt: row.created_at ?? new Date(), isWebhook: row.is_webhook ?? true, isSystem: row.is_system ?? false };
        },
        async list(params: any) {
          const conds: string[] = []; const vals: any[] = [];
          if (params?.runId) { vals.push(params.runId); conds.push(`run_id=$${vals.length}`); }
          const where = conds.length ? `WHERE ${conds.join(' AND ')}` : '';
          const r = await pool.query(`SELECT * FROM entrylite.workflow_hooks ${where} ORDER BY created_at DESC`, vals);
          return page(r.rows.map((row: any) => ({ runId: row.run_id, hookId: row.hook_id, status: 'created', createdAt: row.created_at ?? new Date(), updatedAt: row.created_at ?? new Date(), isWebhook: row.is_webhook ?? true, isSystem: row.is_system ?? false })), params);
        },
      },
      waits: {
        async get(runId: string, waitId: string) {
          const r = await pool.query(`SELECT * FROM entrylite.workflow_waits WHERE run_id=$1 AND wait_id=$2`, [runId, `${runId}-${waitId}`]);
          if (!r.rows[0]) throw new Error(`Wait "${waitId}" not found`);
          const row = r.rows[0];
          return { runId: row.run_id, waitId, status: row.status, resumeAt: row.resume_at ?? null, createdAt: row.created_at ?? new Date(), updatedAt: row.updated_at ?? row.created_at ?? new Date(), completedAt: row.completed_at ?? null };
        },
        async list(params: any) {
          const r = await pool.query(`SELECT * FROM entrylite.workflow_waits WHERE run_id=$1 ORDER BY created_at ASC`, [params.runId]);
          return page(r.rows.map((row: any) => ({ runId: row.run_id, waitId: row.wait_id, status: row.status, resumeAt: row.resume_at ?? null, createdAt: row.created_at ?? new Date(), updatedAt: row.updated_at ?? row.created_at ?? new Date(), completedAt: row.completed_at ?? null })), params);
        },
      },
    };
  }



  // ---------------------------------------------------------------- invoke
  // Hook-resume invocations: world.invoke(runId, {type:'hook_resume',...}) ->
  // mailbox row + wake whose payload carries {runId, invoke:true, requestId,
  // input}; the runtime's withRunInputs handler processes it (handleInvocation
  // -> events.create hook_received) and the flow route's response is captured
  // by deliver() as the outcome. conforms to capabilities.invoke contract:
  // caller polls the mailbox row until responded_at is set (LISTEN wakes it).
  async function invokeImpl(runId: string, payload: unknown, options?: { idempotencyKey?: string; timeoutMs?: number }): Promise<unknown> {
    const requestId = options?.idempotencyKey ?? crypto.randomUUID();
    const timeoutMs = Math.min(options?.timeoutMs ?? 30_000, 120_000);
    await pool.query(
      `INSERT INTO entrylite.workflow_invocations (run_id, request_id, payload)
       VALUES ($1,$2,$3) ON CONFLICT (run_id, request_id) DO NOTHING`,
      [runId, requestId, JSON.stringify(payload)],
    );
    // wake: deliver an invoke-marked message to the run's flow queue
    const r = await pool.query(`SELECT workflow_name FROM entrylite.workflow_runs WHERE run_id=$1`, [runId]);
    const wfName = r.rows[0]?.workflow_name;
    if (!wfName) {
      const { WorkflowRunNotFoundError } = await import('@workflow/errors');
      throw new WorkflowRunNotFoundError(runId);
    }
    await queue.queue(nsQueueName(`__wkf_workflow_${wfName}`), {
      runId,
      invoke: true,
      requestId,
      input: payload,
      requestedAt: new Date().toISOString(),
      traceCarrier: {},
    });
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const row = (await pool.query(
        `SELECT result, responded_at FROM entrylite.workflow_invocations WHERE run_id=$1 AND request_id=$2`,
        [runId, requestId])).rows[0];
      if (row?.responded_at && row.result) return row.result;
      const remaining = deadline - Date.now();
      if (remaining <= 0) {
        throw Object.assign(new Error('Timed out awaiting invocation result; outcome is unknown'), { statusCode: 408 });
      }
      await new Promise(res => setTimeout(res, Math.min(100, remaining)));
    }
  }

  // ------------------------------------------------------------- snapshots
  // Lossless metadata roundtrip: metadata is stored as a jsonb column next to
  // the bytes in the same row (single atomic INSERT ... ON CONFLICT update).
  const snapshots = {
    async save(runId: string, data: Uint8Array, metadata: any): Promise<void> {
      await pool.query(
        `INSERT INTO entrylite.workflow_snapshots (run_id, data, metadata)
         VALUES ($1,$2,$3)
         ON CONFLICT (run_id) DO UPDATE SET data=$2, metadata=$3, created_at=now()`,
        [runId, Buffer.from(data), JSON.stringify(metadata)],
      );
    },
    async load(runId: string): Promise<{ data: Uint8Array; metadata: any } | null> {
      const r = await pool.query(
        `SELECT data, metadata FROM entrylite.workflow_snapshots WHERE run_id=$1`, [runId]);
      if (!r.rows[0]) return null;
      return { data: new Uint8Array(r.rows[0].data), metadata: r.rows[0].metadata };
    },
    async delete(runId: string): Promise<void> {
      await pool.query(`DELETE FROM entrylite.workflow_snapshots WHERE run_id=$1`, [runId]);
    },
  };

  // ------------------------------------------- dynamic workflow code upload
  async function uploadDynamicWorkflowCode(runId: string, params: { workflowName: string; code: Uint8Array }): Promise<string> {
    const ref = `wfcode_${crypto.randomUUID().replaceAll('-', '')}`;
    await pool.query(
      `INSERT INTO entrylite.workflow_dynamic_code (ref, run_id, workflow_name, code)
       VALUES ($1,$2,$3,$4)`,
      [ref, runId, params.workflowName, Buffer.from(params.code)],
    );
    return ref;
  }

  // -------------------------------------------------------- encryption hook
  async function getEncryptionKeyForRun(runOrId: any): Promise<Uint8Array | undefined> {
    const secret = process.env.ENTRYLITE_ENCRYPTION_KEY;
    if (!secret) return undefined; // encryption disabled (fail-closed default)
    const runId = typeof runOrId === 'string' ? runOrId : runOrId?.runId;
    // Derive a per-run 32-byte key: master secret + runId via SHA-256.
    const { createHash } = await import('node:crypto');
    return new Uint8Array(createHash('sha256').update(`${secret}:${runId ?? ''}`).digest());
  }

  return {
    _runsAdditions: { waitForTerminalStatus, getMany, cancelMany, experimentalSetAttributes },
    _invoke: invokeImpl,
    _snapshots: snapshots,
    _uploadDynamicWorkflowCode: uploadDynamicWorkflowCode,
    _getEncryptionKeyForRun: getEncryptionKeyForRun,
    _eventsAdditions: { createBatch },
    // FLAT top-level methods: the runtime calls world.queue(...) directly
    // (core/runtime/start.js), so queue helpers live at world top level.
    _queueAdditions: {
      queueBatch: async (queueName: string, messages: readonly { message: unknown; opts?: any }[]): Promise<any[]> => {
        const out: any[] = [];
        for (const m of messages) out.push(await queue.queue(queueName, m.message, m.opts));
        return out;
      },
      registerRoute: (prefix: string, path: string): void => {
        routes.set(prefix, path as unknown as (req: Request) => Promise<Response>);
      },
      registerHandler: (prefix: string, handler: (req: Request) => Promise<Response>): void => {
        handlers.set(prefix, handler);
      },
      getRouteForPrefix: (prefix: string): string | undefined => routes.get(prefix) as unknown as string | undefined,
    },
    _analytics: analytics(),
    _getEnvironment: (): string | undefined => {
      const env = process.env.ENTRYLITE_ENVIRONMENT;
      return env && ['production', 'preview', 'development'].includes(env) ? env : undefined;
    },
    // internal for delivery: in-process handler takes precedence
    _handlers: handlers,
    _routes: routes,
  };
}
