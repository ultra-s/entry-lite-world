/**
 * Entry-Lite World — storage views (runs/steps/hooks/events reads).
 */
import type { Pool } from 'pg';
import { HookNotFoundError, WorkflowWorldError } from '@workflow/errors';
import { jsonUnsafe } from './events.js';

function page<T>(rows: T[], limit: number, cursorCol: string, cursor: string | undefined, sortOrder: string) {
  const values = rows.slice(0, limit);
  const hasMore = rows.length > limit;
  const last: any = values[values.length - 1];
  return { data: values, cursor: last ? String(last[cursorCol]) : null, hasMore };
}

export function createRunsStorage(pool: Pool) {
  return {
    async get(runId: string, params?: any): Promise<any> {
      const r = await pool.query(`SELECT * FROM entrylite.workflow_runs WHERE run_id=$1`, [runId]);
      if (!r.rows[0]) {
        const { WorkflowRunNotFoundError } = await import('@workflow/errors');
        throw new WorkflowRunNotFoundError(runId);
      }
      const row = r.rows[0];
      const resolveData = params?.resolveData ?? 'all';
      const base = {
        runId: row.run_id,
        deploymentId: row.deployment_id,
        workflowName: row.workflow_name,
        status: row.status,
        specVersion: row.spec_version ?? undefined,
        executionContext: row.execution_context ?? undefined,
        errorCode: row.error_code ?? undefined,
        attributes: row.attributes ?? {},
        startedAt: row.started_at ?? undefined,
        completedAt: row.completed_at ?? undefined,
        createdAt: row.created_at,
        updatedAt: row.updated_at,
        expiredAt: row.expired_at ?? undefined,
      };
      if (resolveData === 'none') {
        const terminal = ['completed', 'failed', 'cancelled'].includes(row.status);
        return {
          ...base,
          input: undefined,
          output: terminal ? (row.status === 'completed' ? null : undefined) : undefined,
          error: row.status === 'failed' && row.error != null ? jsonUnsafe(row.error) : undefined,
        };
      }
      return {
        ...base,
        input: row.input != null ? jsonUnsafe(row.input) : undefined,
        output: row.output != null ? jsonUnsafe(row.output) : undefined,
        error: row.error != null ? jsonUnsafe(row.error) : undefined,
        dynamicWorkflowCode: row.dynamic_workflow_code != null ? jsonUnsafe(row.dynamic_workflow_code) : undefined,
        dynamicWorkflowCodeRef: row.dynamic_workflow_code_ref ?? undefined,
      };
    },
    async list(params?: any): Promise<any> {
      const limit = params?.pagination?.limit ?? 100;
      const cursor = params?.pagination?.cursor;
      const order = params?.pagination?.sortOrder ?? 'desc';
      const cmp = order === 'asc' ? '>' : '<';
      const q = await pool.query(
        `SELECT * FROM entrylite.workflow_runs
         WHERE ($1::varchar IS NULL OR run_id ${cmp} $1)
           AND ($2::varchar IS NULL OR status = $2)
           AND ($3::varchar IS NULL OR workflow_name = $3)
         ORDER BY run_id ${order === 'asc' ? 'ASC' : 'DESC'}
         LIMIT $4`,
        [cursor ?? null, params?.status ?? null, params?.workflowName ?? null, limit + 1],
      );
      const rows = q.rows.slice(0, limit);
      const resolveData = params?.resolveData ?? 'all';
      return {
        data: await Promise.all(rows.map((row: any) =>
          (resolveData === 'none')
            ? { runId: row.run_id, deploymentId: row.deployment_id, workflowName: row.workflow_name, status: row.status, attributes: row.attributes ?? {}, createdAt: row.created_at, updatedAt: row.updated_at, startedAt: row.started_at ?? undefined, completedAt: row.completed_at ?? undefined }
            : this.get(row.run_id),
        )),
        cursor: rows.length ? rows[rows.length - 1].run_id : null,
        hasMore: q.rows.length > limit,
      };
    },
    async cancel(runId: string): Promise<any> {
      const r = await pool.query(
        `UPDATE entrylite.workflow_runs SET status='cancelled', completed_at=now(), updated_at=now()
         WHERE run_id=$1 AND status IN ('pending','running') RETURNING *`,
        [runId],
      );
      const row = r.rows[0];
      if (row) return { status: 'cancelled', completedAt: row.completed_at };
      const cur = await pool.query(`SELECT status FROM entrylite.workflow_runs WHERE run_id=$1`, [runId]);
      return { status: cur.rows[0]?.status ?? 'unknown' };
    },
  };
}

export function createStepsStorage(pool: Pool) {
  return {
    async get(runId: string, stepId: string, params?: any): Promise<any> {
      const r = await pool.query(
        `SELECT * FROM entrylite.workflow_steps WHERE run_id=$1 AND step_id=$2`,
        [runId, stepId],
      );
      if (!r.rows[0]) throw new WorkflowWorldError(`Step not found: ${stepId}`);
      const s = r.rows[0];
      if (params?.resolveData === 'none') {
        return { runId: s.run_id, stepId: s.step_id, stepName: s.step_name, status: s.status, attempt: s.attempt, startedAt: s.started_at ?? undefined, completedAt: s.completed_at ?? undefined, createdAt: s.created_at, updatedAt: s.updated_at, input: undefined, output: undefined };
      }
      return {
        runId: s.run_id,
        stepId: s.step_id,
        stepName: s.step_name,
        status: s.status,
        input: s.input != null ? jsonUnsafe(s.input) : undefined,
        output: s.output != null ? jsonUnsafe(s.output) : undefined,
        error: s.error != null ? jsonUnsafe(s.error) : undefined,
        attempt: s.attempt,
        startedAt: s.started_at ?? undefined,
        completedAt: s.completed_at ?? undefined,
        createdAt: s.created_at,
        updatedAt: s.updated_at,
        retryAfter: s.retry_after ?? undefined,
        specVersion: s.spec_version ?? undefined,
      };
    },
    async list(params: any): Promise<any> {
      const limit = params?.pagination?.limit ?? 20;
      const cursor = params?.pagination?.cursor;
      const order = params?.pagination?.sortOrder ?? 'desc';
      const cmp = order === 'asc' ? '>' : '<';
      const q = await pool.query(
        `SELECT * FROM entrylite.workflow_steps
         WHERE run_id=$1 AND ($2::varchar IS NULL OR step_id ${cmp} $2)
         ORDER BY step_id ${order === 'asc' ? 'ASC' : 'DESC'} LIMIT $3`,
        [params.runId, cursor ?? null, limit + 1],
      );
      const rows = q.rows.slice(0, limit);
      return {
        data: rows.map((s: any) => ({
          runId: s.run_id, stepId: s.step_id, stepName: s.step_name, status: s.status,
          input: params?.resolveData === 'none' ? undefined : (s.input != null ? jsonUnsafe(s.input) : undefined),
          output: params?.resolveData === 'none' ? undefined : (s.output != null ? jsonUnsafe(s.output) : undefined),
          error: s.error != null ? jsonUnsafe(s.error) : undefined, attempt: s.attempt,
          startedAt: s.started_at ?? undefined, completedAt: s.completed_at ?? undefined,
          createdAt: s.created_at, updatedAt: s.updated_at,
        })),
        cursor: rows.length ? rows[rows.length - 1].step_id : null,
        hasMore: q.rows.length > limit,
      };
    },
  };
}

export function createHooksStorage(pool: Pool) {
  return {
    async get(hookId: string, params?: any): Promise<any> {
      const r = await pool.query(`SELECT * FROM entrylite.workflow_hooks WHERE hook_id=$1`, [hookId]);
      if (!r.rows[0]) throw new HookNotFoundError(hookId);
      return mapHookRow(r.rows[0]);
    },
    async getByToken(token: string, params?: any): Promise<any> {
      const r = await pool.query(
        `SELECT h.* FROM entrylite.workflow_hooks h
         LEFT JOIN entrylite.workflow_runs r ON r.run_id = h.run_id
         WHERE h.token=$1 AND (h.token_retention_until IS NULL OR h.token_retention_until > now())
           AND (r.run_id IS NULL OR r.status IN ('pending','running'))`,
        [token],
      );
      if (!r.rows[0]) throw new HookNotFoundError(token);
      return mapHookRow(r.rows[0]);
    },
    async list(params: any): Promise<any> {
      const limit = params?.pagination?.limit ?? 100;
      const cursor = params?.pagination?.cursor;
      const order = params?.pagination?.sortOrder ?? 'asc';
      const cmp = order === 'asc' ? '>' : '<';
      const q = await pool.query(
        `SELECT * FROM entrylite.workflow_hooks
         WHERE ($2::varchar IS NULL OR run_id=$2) AND ($3::varchar IS NULL OR hook_id ${cmp} $3)
         ORDER BY hook_id ${order === 'asc' ? 'ASC' : 'DESC'} LIMIT $4`,
        [null, params?.runId ?? null, cursor ?? null, limit + 1],
      );
      const rows = q.rows.slice(0, limit);
      return {
        data: rows.map(mapHookRow),
        cursor: rows.length ? rows[rows.length - 1].hook_id : null,
        hasMore: q.rows.length > limit,
      };
    },
  };
}

function mapHookRow(h: any): any {
  return {
    runId: h.run_id,
    hookId: h.hook_id,
    token: h.token,
    ownerId: h.owner_id,
    projectId: h.project_id,
    environment: h.environment,
    metadata: h.metadata != null ? jsonUnsafe(h.metadata) : undefined,
    createdAt: h.created_at,
    specVersion: h.spec_version ?? undefined,
    isWebhook: h.is_webhook ?? true,
    isSystem: h.is_system ?? false,
    tokenRetentionUntil: h.token_retention_until ?? undefined,
  };
}

export function createEventsReadStorage(pool: Pool) {
  return {
    async get(runId: string, eventId: string, params?: any): Promise<any> {
      const r = await pool.query(
        `SELECT * FROM entrylite.workflow_events WHERE run_id=$1 AND event_id=$2`,
        [runId, eventId],
      );
      if (!r.rows[0]) throw new WorkflowWorldError(`Event not found: ${eventId}`);
      return mapEventRow(r.rows[0]);
    },
    async list(params: any): Promise<any> {
      const limit = params?.pagination?.limit ?? 200;
      const cursor = params?.pagination?.cursor;
      const order = params?.pagination?.sortOrder ?? 'asc';
      const cmp = order === 'asc' ? '>' : '<';
      const q = await pool.query(
        `SELECT * FROM entrylite.workflow_events
         WHERE run_id=$1 AND ($2::varchar IS NULL OR event_id ${cmp} $2)
         ORDER BY event_id ${order === 'asc' ? 'ASC' : 'DESC'} LIMIT $3`,
        [params.runId, cursor ?? null, limit + 1],
      );
      const rows = q.rows.slice(0, limit);
      return {
        data: rows.map(mapEventRow),
        cursor: rows.length ? rows[rows.length - 1].event_id : null,
        hasMore: q.rows.length > limit,
      };
    },
    async listByCorrelationId(params: any): Promise<any> {
      const limit = params?.pagination?.limit ?? 100;
      const q = await pool.query(
        `SELECT * FROM entrylite.workflow_events WHERE correlation_id=$1
         ORDER BY event_id ASC LIMIT $2`,
        [params.correlationId, limit + 1],
      );
      const rows = q.rows.slice(0, limit);
      return {
        data: rows.map(mapEventRow),
        cursor: rows.length ? rows[rows.length - 1].event_id : null,
        hasMore: q.rows.length > limit,
      };
    },
  };
}

function mapEventRow(e: any): any {
  return {
    eventType: e.event_type,
    correlationId: e.correlation_id ?? undefined,
    eventData: e.event_data != null ? jsonUnsafe(e.event_data) : undefined,
    specVersion: e.spec_version ?? undefined,
    runId: e.run_id,
    eventId: e.event_id,
    createdAt: e.created_at,
    resumeId: e.resume_id ?? undefined,
  };
}
