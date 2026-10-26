/**
 * Entry-Lite World — shared low-level utilities.
 * Plain node-postgres, no ORM. Queue payload format = TypedJSON identical to
 * world-local (Uint8Array => { __type: 'Uint8Array', data: base64 }).
 */
import type { Pool, PoolClient } from 'pg';
import { ulid } from 'ulid';

export const SPEC_VERSION = 4; // matched to the SDK's SPEC_VERSION_SUPPORTS_ATTRIBUTES era used by core beta.40 runtime minting
export const TERMINAL_RUN_STATUSES = ['completed', 'failed', 'cancelled'] as const;
export const TERMINAL_STEP_STATUSES = ['completed', 'failed', 'cancelled'] as const;

export type Json = string | number | boolean | null | Json[] | { [k: string]: Json } | Uint8Array;

/** JSON transport preserving Uint8Array (same wire format as world-local). */
export function jsonReplacer(_key: string, value: unknown): unknown {
  if (value instanceof Uint8Array) {
    return { __type: 'Uint8Array', data: Buffer.from(value).toString('base64') };
  }
  return value;
}
export function jsonReviver(_key: string, value: any): unknown {
  if (value && typeof value === 'object' && value.__type === 'Uint8Array') {
    return new Uint8Array(Buffer.from(value.data, 'base64'));
  }
  return value;
}
export function serializeBody(message: unknown): Buffer {
  return Buffer.from(JSON.stringify(message, jsonReplacer));
}
export function deserializeBody(raw: string | Buffer): unknown {
  return JSON.parse(typeof raw === 'string' ? raw : raw.toString(), jsonReviver);
}

export const newRunId = (): string => `wrun_${ulid()}`;
export const newEventId = (): string => `evnt_${ulid()}`;
export const newStepId = (): string => `step_${ulid()}`;
export const newHookId = (): string => `whok_${ulid()}`;
export const newMessageId = (): string => `msg_${ulid()}`;
export const newChunkId = (): string => `chnk_${ulid()}`;

// ---------- per-deployment queue namespace ----------
/**
 * Per-deployment queue namespace. Independent deployments sharing one
 * PostgreSQL database set different ENTRYLITE_QUEUE_NAMESPACE values; every
 * queue row is stored under `<ns>::<queueName>` and every claim/lease/
 * recovery/sweep query is scoped to this deployment's rows, so no deployment
 * can claim, lease, retry, or recover another's jobs. Unset = legacy
 * single-deployment mode (no prefix, unchanged behavior). Validated once at
 * import — explicit and stable across restarts, never auto-generated.
 */
export const RAW_NAMESPACE: string = process.env.ENTRYLITE_QUEUE_NAMESPACE ?? '';
export const NAMESPACE_RE = /^[a-z][a-z0-9]*$/;
export const NS_SEP = '::';
export const NAMESPACE: string = (() => {
  if (RAW_NAMESPACE && !NAMESPACE_RE.test(RAW_NAMESPACE)) {
    throw new Error(`[entrylite-world] ENTRYLITE_QUEUE_NAMESPACE "${RAW_NAMESPACE}" invalid: must match ^[a-z][a-z0-9]*$`);
  }
  return RAW_NAMESPACE;
})();
/** Full DB-stored queue name for a logical queue name. */
export function nsQueueName(queueName: string): string {
  return NAMESPACE ? `${NAMESPACE}${NS_SEP}${queueName}` : queueName;
}
/** True if a DB-stored queue_name belongs to this deployment. */
export function isOwnQueueName(dbQueueName: string): boolean {
  return NAMESPACE
    ? dbQueueName.startsWith(`${NAMESPACE}${NS_SEP}`)
    : !dbQueueName.includes(NS_SEP);
}
/** SQL fragment scoping a queue_name column to this deployment's rows. */
export const NS_CLAIM_WHERE: string = NAMESPACE
  ? `queue_name LIKE '${NAMESPACE}${NS_SEP}%'`
  : `queue_name NOT LIKE '%::%'`;
/** Namespace-stamped idempotency key (job_key). */
export function nsJobKey(key: string | null | undefined): string | null {
  if (!key) return null;
  return NAMESPACE ? `${NAMESPACE}${NS_SEP}${key}` : key;
}

/** Compact: strip undefined values (matches worlds' compact()). */
export function compact<T extends Record<string, unknown>>(obj: T): T {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(obj)) {
    if (v !== undefined) out[k] = v;
  }
  return out as T;
}

/** Parse a queue name "q_<prefix>::<rest>" style used by the SDK. */
export function parseQueueName(queueName: string): { prefix: string; id: string } {
  const sep = queueName.indexOf('::');
  if (sep === -1) return { prefix: '', id: queueName };
  return { prefix: queueName.slice(0, sep + 2), id: queueName.slice(sep + 2) };
}

export async function withTx<T>(pool: Pool, fn: (tx: PoolClient) => Promise<T>): Promise<T> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch { /* connection gone */ }
    throw err;
  } finally {
    client.release();
  }
}

/** Structured error normalization for run.error / step.error text columns. */
export function errorToText(err: unknown): string {
  if (err === undefined || err === null) return String(err);
  if (typeof err === 'string') return err;
  if (err instanceof Error) return JSON.stringify({ message: err.message, stack: err.stack });
  try { return JSON.stringify(err); } catch { return String(err); }
}
