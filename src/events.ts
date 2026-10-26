/**
 * Entry-Lite World — event creation + entity materialization.
 * Mirrors the official postgres world's semantics with Entry-Lite's schema:
 * one transaction per event: validate → materialize entity → insert event
 * (position = MAX(slot)+1 inside the tx) → return EventResult.
 */
import type { Pool, PoolClient } from 'pg';
import {
  EntityConflictError,
  HookNotFoundError,
  RunExpiredError,
  TooEarlyError,
  WorkflowRunNotFoundError,
  WorkflowWorldError,
} from '@workflow/errors';
import { compact, errorToText, newRunId, TERMINAL_RUN_STATUSES, TERMINAL_STEP_STATUSES, withTx, nsQueueName, nsJobKey } from './util.js';

type Json = any;

export interface EventResultLike {
  event?: any;
  run?: any;
  step?: any;
  hook?: any;
  wait?: any;
}

function isTerminalRun(s: string): boolean { return (TERMINAL_RUN_STATUSES as readonly string[]).includes(s); }
function isTerminalStep(s: string): boolean { return (TERMINAL_STEP_STATUSES as readonly string[]).includes(s); }

async function allocateEventId(db: Pool | PoolClient, runId: string): Promise<string> {
  // Ensure slot row exists (run_created opens it), then MAX+1 within the caller's tx.
  // Lock the slot row for the remainder of the transaction so concurrent
  // event inserts on the same run serialize (the "bump and report" contract);
  // without FOR UPDATE two txs can compute the same position.
  await db.query(
    `INSERT INTO entrylite.workflow_event_slots (run_id) VALUES ($1) ON CONFLICT DO NOTHING`,
    [runId],
  );
  if ('query' in db) {
    await db.query(`SELECT run_id FROM entrylite.workflow_event_slots WHERE run_id = $1 FOR UPDATE`, [runId]);
  }
  const res = await db.query<{ n: string | number }>(
    `SELECT COALESCE(MAX((regexp_replace(event_id, '^evnt_', ''))::text), '') AS n
     FROM entrylite.workflow_events WHERE run_id = $1`,
    [runId],
  );
  // Position = number of events + 1, encoded as zero-padded suffix (sorts lexicographically).
  const res2 = await db.query<{ c: string }>(
    `SELECT COUNT(*)::text AS c FROM entrylite.workflow_events WHERE run_id = $1`,
    [runId],
  );
  const n = BigInt(res2.rows[0].c) + 1n;
  // Slot identity: 'evnt_' + 26-char body = 10 leading zeros + 16 decimal digits.
  return `evnt_0000000000${n.toString().padStart(16, '0')}`;
}

function mapRun(r: any): any {
  if (!r) return r;
  return compact({
    runId: r.run_id,
    deploymentId: r.deployment_id,
    workflowName: r.workflow_name,
    status: r.status,
    specVersion: r.spec_version ?? undefined,
    executionContext: r.execution_context ?? undefined,
    input: r.input != null ? jsonUnsafe(r.input) : undefined,
    output: r.output != null ? jsonUnsafe(r.output) : undefined,
    error: r.error != null ? jsonUnsafe(r.error) : undefined,
    errorCode: r.error_code ?? undefined,
    dynamicWorkflowCode: r.dynamic_workflow_code != null ? jsonUnsafe(r.dynamic_workflow_code) : undefined,
    dynamicWorkflowCodeRef: r.dynamic_workflow_code_ref ?? undefined,
    attributes: r.attributes ?? {},
    startedAt: r.started_at ?? undefined,
    completedAt: r.completed_at ?? undefined,
    createdAt: r.created_at,
    updatedAt: r.updated_at,
    expiredAt: r.expired_at ?? undefined,
  });
}

function mapStep(s: any): any {
  if (!s) return s;
  return compact({
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
  });
}

function mapHook(h: any): any {
  if (!h) return h;
  return compact({
    runId: h.run_id,
    hookId: h.hook_id,
    token: h.token,
    ownerId: h.owner_id,
    projectId: h.project_id,
    environment: h.environment,
    metadata: h.metadata ?? undefined,
    createdAt: h.created_at,
    specVersion: h.spec_version ?? undefined,
    isWebhook: h.is_webhook ?? true,
    isSystem: h.is_system ?? false,
    tokenRetentionUntil: h.token_retention_until ?? undefined,
  });
}

function mapWait(w: any): any {
  if (!w) return w;
  return compact({
    waitId: w.wait_id,
    runId: w.run_id,
    status: w.status,
    resumeAt: w.resume_at ?? undefined,
    completedAt: w.completed_at ?? undefined,
    createdAt: w.created_at,
    updatedAt: w.updated_at,
    specVersion: w.spec_version ?? undefined,
  });
}

async function getRun(db: Pool | PoolClient, runId: string): Promise<any | null> {
  const r = await db.query(`SELECT * FROM entrylite.workflow_runs WHERE run_id = $1`, [runId]);
  return r.rows[0] ?? null;
}

/**
 * Create an event and materialize the entity change in ONE transaction.
 * Enqueues the wake message(s) via the jobs table in the same tx (wake rows
 * are returned so queue layer can rely on their existence).
 */
export async function createEvent(
  pool: Pool,
  runId: string | null,
  data: any,
  wakes: Array<{ queueName: string; body: unknown; delaySeconds?: number; jobKey?: string }> = [],
): Promise<EventResultLike> {
  const isRunCreated = data.eventType === 'run_created';
  let effectiveRunId: string;
  if (isRunCreated && (!runId || runId === '')) {
    effectiveRunId = newRunId();
  } else if (!runId) {
    throw new WorkflowWorldError('runId is required for non-run_created events');
  } else {
    effectiveRunId = runId;
  }
  const specVersion = data.specVersion ?? 4;
  const now = new Date();

  return withTx(pool, async (tx) => {
    let run: any, step: any, hook: any, wait: any;
    let eventId: string;
    const eventData = ('eventData' in data ? data.eventData : undefined) as Json | undefined;

    // ---------- validation ----------
    let currentRun: any = null;
    if (!isRunCreated) {
      currentRun = await getRun(tx, effectiveRunId);
      if (currentRun && isTerminalRun(currentRun.status)) {
        if (data.eventType === 'run_cancelled' && currentRun.status === 'cancelled') {
          // idempotent: record event, return run
          eventId = await allocateEventId(tx, effectiveRunId);
          await tx.query(
            `INSERT INTO entrylite.workflow_events (run_id, event_id, event_type, correlation_id, event_data, spec_version)
             VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (run_id, event_id) DO NOTHING`,
            [effectiveRunId, eventId, data.eventType, data.correlationId ?? null, eventData ?? null, specVersion],
          );
          return { event: { ...data, runId: effectiveRunId, eventId, createdAt: now }, run: mapRun(currentRun) };
        }
        if (data.eventType === 'run_started') {
          throw new RunExpiredError(`Workflow run "${effectiveRunId}" is already in terminal state "${currentRun.status}"`);
        }
        if (isTerminalRunEventType(data.eventType)) {
          throw new EntityConflictError(`Cannot transition run from terminal state "${currentRun.status}"`);
        }
        if (isChildEntityCreation(data.eventType)) {
          throw new EntityConflictError(`Cannot create new entities on run in terminal state "${currentRun.status}"`);
        }
      }
      if (!currentRun && data.eventType !== 'run_started') {
        throw new WorkflowRunNotFoundError(effectiveRunId);
      }
    }

    const createsChild = isChildEntityCreation(data.eventType);
    const lazyStepStart = data.eventType === 'step_started' && eventData?.stepName !== undefined && eventData?.input !== undefined;

    // step validation
    let validatedStep: any = null;
    if ((data.eventType === 'step_started' || data.eventType === 'step_retrying') && data.correlationId) {
      const r = await tx.query(
        `SELECT * FROM entrylite.workflow_steps WHERE run_id = $1 AND step_id = $2`,
        [effectiveRunId, data.correlationId],
      );
      validatedStep = r.rows[0] ?? null;
      if (!validatedStep && !lazyStepStart) {
        throw new WorkflowWorldError(`Step "${data.correlationId}" not found`);
      }
      if (lazyStepStart && validatedStep) {
        throw new EntityConflictError(`Step "${data.correlationId}" already created`);
      }
      if (validatedStep) {
        if (isTerminalStep(validatedStep.status)) {
          throw new EntityConflictError(`Cannot modify step in terminal state "${validatedStep.status}"`);
        }
        if (currentRun && isTerminalRun(currentRun.status)) {
          if (validatedStep.status !== 'running' || data.eventType === 'step_started') {
            throw new RunExpiredError(`Cannot start step on run in terminal state "${currentRun.status}"`);
          }
        }
        if (validatedStep.retry_after && new Date(validatedStep.retry_after).getTime() > Date.now()) {
          const waitMs = new Date(validatedStep.retry_after).getTime() - Date.now();
          throw new TooEarlyError(
            `Cannot start step "${data.correlationId}": retryAfter not reached yet`,
            { retryAfter: Math.ceil(waitMs / 1000) } as any,
          );
        }
      }
    }

    // hook existence validation
    if ((data.eventType === 'hook_received' || data.eventType === 'hook_disposed') && data.correlationId) {
      const r = await tx.query(`SELECT hook_id FROM entrylite.workflow_hooks WHERE hook_id = $1`, [data.correlationId]);
      if (!r.rows[0]) throw new HookNotFoundError(data.correlationId);
    }

    // ---------- materialization ----------
    switch (data.eventType) {
      case 'run_created': {
        const r = await tx.query(
          `INSERT INTO entrylite.workflow_runs
             (run_id, deployment_id, status, workflow_name, spec_version, execution_context, input, attributes, dynamic_workflow_code, dynamic_workflow_code_ref)
           VALUES ($1,$2,'pending',$3,$4,$5,$6,$7,$8,$9)
           ON CONFLICT (run_id) DO NOTHING RETURNING *`,
          [
            effectiveRunId,
            eventData.deploymentId,
            eventData.workflowName,
            specVersion,
            eventData.executionContext ?? null,
            eventData.input === undefined ? null : JSON.stringify(jsonSafe(eventData.input)),
            eventData.attributes ?? {},
            eventData.dynamicWorkflowCode === undefined ? null : JSON.stringify(jsonSafe(eventData.dynamicWorkflowCode)),
            eventData.dynamicWorkflowCodeRef ?? null,
          ],
        );
        if (!r.rows[0]) {
          throw new EntityConflictError(`Workflow run "${effectiveRunId}" already exists`);
        }
        currentRun = r.rows[0];
        eventId = await allocateEventId(tx, effectiveRunId);
        await insertEvent(tx, effectiveRunId, eventId, data, specVersion);
        run = mapRun(currentRun);
        break;
      }
      case 'run_started': {
        // resilient: create run if missing and eventData carries creation info
        if (!currentRun) {
          if (eventData?.deploymentId && eventData?.workflowName) {
            const r = await tx.query(
              `INSERT INTO entrylite.workflow_runs
                 (run_id, deployment_id, status, workflow_name, spec_version, execution_context, input, attributes, dynamic_workflow_code, dynamic_workflow_code_ref)
               VALUES ($1,$2,'pending',$3,$4,$5,$6,$7,$8,$9)
               ON CONFLICT (run_id) DO NOTHING RETURNING *`,
              [
                effectiveRunId,
                eventData.deploymentId,
                eventData.workflowName,
                specVersion,
                eventData.executionContext ?? null,
                eventData.input === undefined ? null : JSON.stringify(jsonSafe(eventData.input)),
                eventData.attributes ?? {},
                eventData.dynamicWorkflowCode === undefined ? null : JSON.stringify(jsonSafe(eventData.dynamicWorkflowCode)),
                eventData.dynamicWorkflowCodeRef ?? null,
              ],
            );
            if (r.rows[0]) {
              await insertEvent(tx, effectiveRunId, await allocateEventId(tx, effectiveRunId),
                { ...data, eventType: 'run_created', eventData: { deploymentId: eventData.deploymentId, workflowName: eventData.workflowName, input: eventData.input, executionContext: eventData.executionContext, attributes: eventData.attributes } }, specVersion);
              currentRun = r.rows[0];
            }
          }
          if (!currentRun) throw new WorkflowRunNotFoundError(effectiveRunId);
        }
        const r = await tx.query(
          `UPDATE entrylite.workflow_runs SET status='running', started_at=COALESCE(started_at,$2), updated_at=$2
           WHERE run_id=$1 AND status='pending' RETURNING *`,
          [effectiveRunId, now.toISOString()],
        );
        if (r.rows[0]) run = mapRun(r.rows[0]);
        else {
          const cur = await tx.query(`SELECT * FROM entrylite.workflow_runs WHERE run_id=$1`, [effectiveRunId]);
          if (cur.rows[0]) run = mapRun(cur.rows[0]);
        }
        eventId = await allocateEventId(tx, effectiveRunId);
        await insertEvent(tx, effectiveRunId, eventId, data, specVersion);
        break;
      }
      case 'run_completed': {
        const r = await tx.query(
          `UPDATE entrylite.workflow_runs SET status='completed', output=$2, completed_at=$3, updated_at=$3
           WHERE run_id=$1 AND status IN ('pending','running') RETURNING *`,
          [effectiveRunId, eventData?.output === undefined ? null : JSON.stringify(jsonSafe(eventData.output)), now.toISOString()],
        );
        if (!r.rows[0]) throw new EntityConflictError(`Cannot complete run "${effectiveRunId}"`);
        run = mapRun(r.rows[0]);
        // cleanup: non-retained hooks and all waits die with the run
        await tx.query(
          `DELETE FROM entrylite.workflow_hooks WHERE run_id=$1 AND (token_retention_until IS NULL OR token_retention_until <= now())`,
          [effectiveRunId],
        );
        await tx.query(`DELETE FROM entrylite.workflow_waits WHERE run_id=$1`, [effectiveRunId]);
        eventId = await allocateEventId(tx, effectiveRunId);
        await insertEvent(tx, effectiveRunId, eventId, data, specVersion);
        break;
      }
      case 'run_failed': {
        const r = await tx.query(
          `UPDATE entrylite.workflow_runs SET status='failed', error=$2, error_code=$3, completed_at=$4, updated_at=$4
           WHERE run_id=$1 AND status IN ('pending','running') RETURNING *`,
          [
            effectiveRunId,
            eventData?.error === undefined ? null : jsonSafe(eventData.error),
            (eventData as any)?.errorCode ?? null,
            now.toISOString(),
          ],
        );
        if (!r.rows[0]) throw new EntityConflictError(`Cannot fail run "${effectiveRunId}"`);
        run = mapRun(r.rows[0]);
        await tx.query(
          `DELETE FROM entrylite.workflow_hooks WHERE run_id=$1 AND (token_retention_until IS NULL OR token_retention_until <= now())`,
          [effectiveRunId],
        );
        await tx.query(`DELETE FROM entrylite.workflow_waits WHERE run_id=$1`, [effectiveRunId]);
        eventId = await allocateEventId(tx, effectiveRunId);
        await insertEvent(tx, effectiveRunId, eventId, data, specVersion);
        break;
      }
      case 'run_cancelled': {
        const r = await tx.query(
          `UPDATE entrylite.workflow_runs SET status='cancelled', completed_at=$2, updated_at=$2
           WHERE run_id=$1 AND status IN ('pending','running') RETURNING *`,
          [effectiveRunId, now.toISOString()],
        );
        run = r.rows[0] ? mapRun(r.rows[0]) : mapRun(currentRun);
        eventId = await allocateEventId(tx, effectiveRunId);
        await insertEvent(tx, effectiveRunId, eventId, data, specVersion);
        break;
      }
      case 'step_created': {
        // dedup: a redelivered duplicate must not double-insert the event
        const dup = await tx.query(
          `SELECT 1 FROM entrylite.workflow_events WHERE run_id=$1 AND correlation_id=$2 AND event_type='step_created' LIMIT 1`,
          [effectiveRunId, data.correlationId],
        );
        if (dup.rows[0]) throw new EntityConflictError(`step_created for correlationId "${data.correlationId}" already exists in run "${effectiveRunId}"`);
        const ins = await tx.query(
          `INSERT INTO entrylite.workflow_steps (run_id, step_id, step_name, input, status, attempt, spec_version)
           VALUES ($1,$2,$3,$4,'pending',0,$5) ON CONFLICT (step_id) DO NOTHING RETURNING *`,
          [effectiveRunId, data.correlationId, eventData.stepName, eventData.input === undefined ? null : JSON.stringify(jsonSafe(eventData.input)), specVersion],
        );
        if (ins.rows[0]) {
          step = mapStep(ins.rows[0]);
        } else {
          const ex = await tx.query(`SELECT * FROM entrylite.workflow_steps WHERE run_id=$1 AND step_id=$2`, [effectiveRunId, data.correlationId]);
          if (ex.rows[0]) step = mapStep(ex.rows[0]);
        }
        eventId = await allocateEventId(tx, effectiveRunId);
        await insertEvent(tx, effectiveRunId, eventId, data, specVersion);
        break;
      }
      case 'step_started': {
        if (lazyStepStart && !validatedStep) {
          const ins = await tx.query(
            `INSERT INTO entrylite.workflow_steps (run_id, step_id, step_name, input, status, attempt, spec_version)
             VALUES ($1,$2,$3,$4,'pending',0,$5) ON CONFLICT (step_id) DO NOTHING RETURNING step_id`,
            [effectiveRunId, data.correlationId, eventData.stepName, JSON.stringify(jsonSafe(eventData.input)), specVersion],
          );
          if (!ins.rows[0]) throw new EntityConflictError(`Step "${data.correlationId}" already created`);
          // synthetic step_created must precede step_started in the log
          await insertEvent(tx, effectiveRunId, await allocateEventId(tx, effectiveRunId),
            { correlationId: data.correlationId, eventType: 'step_created', eventData: { stepName: eventData.stepName, input: eventData.input } }, specVersion);
        }
        const upd = await tx.query(
          `UPDATE entrylite.workflow_steps
           SET status='running', started_at=COALESCE(started_at,$3), retry_after=NULL, updated_at=$3
           WHERE run_id=$1 AND step_id=$2 AND status IN ('pending','running') RETURNING *`,
          [effectiveRunId, data.correlationId, now.toISOString()],
        );
        if (upd.rows[0]) {
          step = mapStep(upd.rows[0]);
        } else {
          const ex = await tx.query(
            `SELECT * FROM entrylite.workflow_steps WHERE run_id=$1 AND step_id=$2`,
            [effectiveRunId, data.correlationId],
          );
          if (!ex.rows[0]) throw new WorkflowWorldError(`Step "${data.correlationId}" not found`);
          if (isTerminalStep(ex.rows[0].status)) throw new EntityConflictError(`Cannot modify step in terminal state "${ex.rows[0].status}"`);
        }
        eventId = await allocateEventId(tx, effectiveRunId);
        await insertEvent(tx, effectiveRunId, eventId, data, specVersion);
        break;
      }
      case 'step_completed': {
        const upd = await tx.query(
          `UPDATE entrylite.workflow_steps
           SET status='completed', output=$3, completed_at=$4, updated_at=$4
           WHERE run_id=$1 AND step_id=$2 AND status IN ('pending','running') RETURNING *`,
          [effectiveRunId, data.correlationId, eventData?.output === undefined ? null : JSON.stringify(jsonSafe(eventData.output)), now.toISOString()],
        );
        if (!upd.rows[0]) throw new EntityConflictError(`Step "${data.correlationId}" not in a startable state`);
        step = mapStep(upd.rows[0]);
        eventId = await allocateEventId(tx, effectiveRunId);
        await insertEvent(tx, effectiveRunId, eventId, data, specVersion);
        break;
      }
      case 'step_failed': {
        const upd = await tx.query(
          `UPDATE entrylite.workflow_steps
           SET status='failed', error=$3, completed_at=$4, updated_at=$4
           WHERE run_id=$1 AND step_id=$2 AND status IN ('pending','running') RETURNING *`,
          [effectiveRunId, data.correlationId, eventData?.error === undefined ? null : jsonSafe(eventData.error), now.toISOString()],
        );
        if (!upd.rows[0]) throw new EntityConflictError(`Step "${data.correlationId}" not in a startable state`);
        step = mapStep(upd.rows[0]);
        eventId = await allocateEventId(tx, effectiveRunId);
        await insertEvent(tx, effectiveRunId, eventId, data, specVersion);
        break;
      }
      case 'step_retrying': {
        const upd = await tx.query(
          `UPDATE entrylite.workflow_steps
           SET status='pending', attempt = attempt + 1, error=$3, retry_after=$4, updated_at=$5
           WHERE run_id=$1 AND step_id=$2 AND status='running' RETURNING *`,
          [
            effectiveRunId,
            data.correlationId,
            eventData?.error === undefined ? null : jsonSafe(eventData.error),
            eventData?.retryAfter ? new Date(eventData.retryAfter).toISOString() : null,
            now.toISOString(),
          ],
        );
        step = upd.rows[0] ? mapStep(upd.rows[0]) : undefined;
        eventId = await allocateEventId(tx, effectiveRunId);
        await insertEvent(tx, effectiveRunId, eventId, data, specVersion);
        break;
      }
      case 'hook_created': {
        // free tokens owned by terminal runs
        await tx.query(
          `DELETE FROM entrylite.workflow_hooks
           WHERE token=$1 AND run_id IN (SELECT run_id FROM entrylite.workflow_runs WHERE status IN ('completed','failed','cancelled'))
             AND (token_retention_until IS NULL OR token_retention_until <= now())`,
          [eventData.token],
        );
        const ins = await tx.query(
          `INSERT INTO entrylite.workflow_hooks
             (run_id, hook_id, token, metadata, spec_version, is_webhook, is_system, token_retention_until)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
           ON CONFLICT (hook_id) DO NOTHING RETURNING *`,
          [
            effectiveRunId,
            data.correlationId,
            eventData.token,
            eventData.metadata === undefined ? null : JSON.stringify(jsonSafe(eventData.metadata)),
            specVersion,
            eventData.isWebhook ?? true,
            eventData.isSystem ?? false,
            eventData.tokenRetentionUntil ? new Date(eventData.tokenRetentionUntil).toISOString() : null,
          ],
        );
        if (ins.rows[0]) {
          hook = mapHook(ins.rows[0]);
        } else {
          const ex = await tx.query(`SELECT * FROM entrylite.workflow_hooks WHERE hook_id=$1`, [data.correlationId]);
          if (ex.rows[0] && ex.rows[0].run_id !== effectiveRunId) {
            // Force claim: the caller takes the token from the victim run,
            // re-pointing the hook row and recording who it was taken from.
            if (eventData.force) {
              const upd = await tx.query(
                `UPDATE entrylite.workflow_hooks
                 SET run_id=$1, metadata=$2, token_retention_until=$3
                 WHERE hook_id=$4
                 RETURNING *`,
                [
                  effectiveRunId,
                  eventData.metadata === undefined ? ex.rows[0].metadata : JSON.stringify(jsonSafe(eventData.metadata)),
                  eventData.tokenRetentionUntil ? new Date(eventData.tokenRetentionUntil).toISOString() : ex.rows[0].token_retention_until,
                  data.correlationId,
                ],
              );
              hook = mapHook(upd.rows[0]);
            } else {
              throw new EntityConflictError(`Hook "${data.correlationId}" already created`);
            }
          }
          // orphaned hook row (crash between insert and event): recover by falling through
          if (!hook && ex.rows[0]) hook = mapHook(ex.rows[0]);
        }
        eventId = await allocateEventId(tx, effectiveRunId);
        await insertEvent(tx, effectiveRunId, eventId, data, specVersion);
        break;
      }
      case 'hook_received': {
        // locked re-check + insert; dedup via resumeId unique index. Concurrent
        // writers with the same (runId, resumeId) converge: first insert wins,
        // losers re-read the canonical committed event.
        const ex = await tx.query(`SELECT * FROM entrylite.workflow_hooks WHERE hook_id=$1 FOR UPDATE`, [data.correlationId]);
        if (!ex.rows[0]) throw new HookNotFoundError(data.correlationId);
        const resumeId = (data as any).resumeId ?? null;
        eventId = await allocateEventId(tx, effectiveRunId);
        if (resumeId) {
          const ins = await tx.query(
            `INSERT INTO entrylite.workflow_events (run_id, event_id, event_type, correlation_id, event_data, spec_version, resume_id)
             VALUES ($1,$2,$3,$4,$5,$6,$7)
             ON CONFLICT (run_id, resume_id) WHERE event_type::text = 'hook_received' AND resume_id IS NOT NULL DO NOTHING
             RETURNING event_id`,
            [effectiveRunId, eventId, data.eventType, data.correlationId ?? null,
             eventData === undefined ? null : JSON.stringify(jsonSafe(eventData)), specVersion, resumeId],
          );
          if (!ins.rows[0]) {
            // lost the race — return the canonical event
            const canon = await tx.query(
              `SELECT * FROM entrylite.workflow_events WHERE run_id=$1 AND resume_id=$2 AND event_type='hook_received'`,
              [effectiveRunId, resumeId],
            );
            eventId = canon.rows[0].event_id;
          }
        } else {
          await insertEvent(tx, effectiveRunId, eventId, data, specVersion);
        }
        break;
      }
      case 'hook_disposed': {
        await tx.query(`DELETE FROM entrylite.workflow_hooks WHERE hook_id=$1`, [data.correlationId]);
        eventId = await allocateEventId(tx, effectiveRunId);
        await insertEvent(tx, effectiveRunId, eventId, data, specVersion);
        break;
      }
      case 'wait_created': {
        const waitId = `${effectiveRunId}-${data.correlationId}`;
        const ins = await tx.query(
          `INSERT INTO entrylite.workflow_waits (wait_id, run_id, status, resume_at, spec_version)
           VALUES ($1,$2,'waiting',$3,$4) ON CONFLICT (wait_id) DO NOTHING RETURNING *`,
          [waitId, effectiveRunId, new Date(eventData.resumeAt).toISOString(), specVersion],
        );
        if (ins.rows[0]) {
          wait = mapWait(ins.rows[0]);
        } else {
          const ex = await tx.query(`SELECT * FROM entrylite.workflow_waits WHERE wait_id=$1`, [waitId]);
          if (ex.rows[0]?.status === 'waiting') wait = mapWait(ex.rows[0]);
          else throw new EntityConflictError(`Wait "${data.correlationId}" already exists`);
        }
        eventId = await allocateEventId(tx, effectiveRunId);
        await insertEvent(tx, effectiveRunId, eventId, data, specVersion);
        break;
      }
      case 'wait_completed': {
        const waitId = `${effectiveRunId}-${data.correlationId}`;
        const upd = await tx.query(
          `UPDATE entrylite.workflow_waits SET status='completed', completed_at=$2, updated_at=$2
           WHERE wait_id=$1 AND status='waiting' RETURNING *`,
          [waitId, now.toISOString()],
        );
        if (upd.rows[0]) wait = mapWait(upd.rows[0]);
        eventId = await allocateEventId(tx, effectiveRunId);
        await insertEvent(tx, effectiveRunId, eventId, data, specVersion);
        break;
      }
      default: {
        // attr_set and anything else: append-only
        eventId = await allocateEventId(tx, effectiveRunId);
        await insertEvent(tx, effectiveRunId, eventId, data, specVersion);
        if (data.eventType === 'attr_set') {
          const attrs: any = {};
          for (const { key, value } of data.eventData.changes ?? []) {
            if (value === null) delete attrs[key];
            else attrs[key] = value;
          }
          const merged = { ...(currentRun?.attributes ?? {}), ...attrs };
          await tx.query(`UPDATE entrylite.workflow_runs SET attributes=$2, updated_at=$3 WHERE run_id=$1`,
            [effectiveRunId, JSON.stringify(merged), now.toISOString()]);
        }
      }
    }

    // ---------- enqueue wakes (same tx = atomic with the event) ----------
    for (const w of wakes) {
      await tx.query(
        `INSERT INTO entrylite.workflow_jobs (queue_name, payload, run_at, job_key)
         VALUES ($1,$2,$3,$4)
         ON CONFLICT (job_key) WHERE locked_at IS NULL DO NOTHING`,
        [
          nsQueueName(w.queueName),
          JSON.stringify(w.body),
          w.delaySeconds && w.delaySeconds > 0 ? new Date(Date.now() + w.delaySeconds * 1000).toISOString() : new Date().toISOString(),
          nsJobKey(w.jobKey),
        ],
      );
    }

    const event = compact({
      ...data,
      runId: effectiveRunId,
      eventId,
      createdAt: now,
      specVersion: specVersion,
    });
    return { event, run, step, hook, wait };
  });
}

function isTerminalRunEventType(t: string): boolean {
  return t === 'run_completed' || t === 'run_failed' || t === 'run_cancelled';
}
function isChildEntityCreation(t: string): boolean {
  return t === 'step_created' || t === 'hook_created' || t === 'wait_created';
}

async function insertEvent(
  tx: PoolClient,
  runId: string,
  eventId: string,
  data: any,
  specVersion: number,
  resumeId: string | null = null,
): Promise<void> {
  const eventData = 'eventData' in data ? data.eventData : undefined;
  await tx.query(
    `INSERT INTO entrylite.workflow_events (run_id, event_id, event_type, correlation_id, event_data, spec_version, resume_id)
     VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (run_id, event_id) DO NOTHING`,
    [runId, eventId, data.eventType, data.correlationId ?? null, eventData === undefined ? null : JSON.stringify(jsonSafe(eventData)), specVersion, resumeId],
  );
}

/** Make a value JSON-safe for storage (Uint8Array → base64 marker, dates → ISO). */
function jsonSafe(v: unknown): unknown {
  if (v === undefined) return null;
  if (v instanceof Uint8Array) return { __type: 'Uint8Array', data: Buffer.from(v).toString('base64') };
  if (v instanceof Date) return { __type: 'Date', data: v.toISOString() };
  if (Array.isArray(v)) return v.map(jsonSafe);
  if (v && typeof v === 'object') {
    const out: Record<string, unknown> = {};
    for (const [k, val] of Object.entries(v)) out[k] = jsonSafe(val);
    return out;
  }
  return v;
}

/** Reverse of jsonSafe for reads. */
export function jsonUnsafe(v: unknown): unknown {
  if (Array.isArray(v)) return v.map(jsonUnsafe);
  if (v && typeof v === 'object' && !Buffer.isBuffer(v)) {
    const o = v as Record<string, unknown>;
    if (o.__type === 'Uint8Array' && typeof o.data === 'string') return new Uint8Array(Buffer.from(o.data, 'base64'));
    if (o.__type === 'Date' && typeof o.data === 'string') return new Date(o.data);
    const out: Record<string, unknown> = {};
    for (const [k, val] of Object.entries(o)) out[k] = jsonUnsafe(val);
    return out;
  }
  return v;
}
