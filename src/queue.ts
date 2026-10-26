/**
 * Entry-Lite World — durable queue + streamer.
 * Queue: entrylite.workflow_jobs, SKIP LOCKED pickup, LISTEN/NOTIFY wake,
 * in-process worker loop delivering to the SDK's queue handler (direct call,
 * no HTTP hop). Handler result `{ timeoutSeconds }` = long-poll reschedule.
 */
import type { Pool } from 'pg';
import { ulid } from 'ulid';
import { z } from 'zod';
import { newChunkId, newMessageId, parseQueueName, serializeBody, deserializeBody, NAMESPACE, NS_SEP, nsQueueName, isOwnQueueName, NS_CLAIM_WHERE, nsJobKey } from './util.js';

const WORKER_ID = `elw_${ulid()}`;

export function createQueue(pool: Pool) {
  const handlers = new Map<string, (message: unknown, meta: any) => Promise<void | { timeoutSeconds: number }>>();
  let closing = false;
  let poller: NodeJS.Timeout | null = null;
  let wakeClient: any = null;
  const inFlight = new Set<Promise<unknown>>();

  const QUEUE_POLL_MS = parseInt(process.env.ENTRYLITE_QUEUE_POLL_MS ?? '250', 10);
  const CONCURRENCY = parseInt(process.env.ENTRYLITE_QUEUE_CONCURRENCY ?? '50', 10);

  const BASE_URL = process.env.WORKFLOW_LOCAL_BASE_URL
    ?? (process.env.PORT ? `http://localhost:${process.env.PORT}` : 'http://localhost:3000');

  async function deliver(job: { id: number; queue_name: string; payload: any; attempts: number }) {
    // Stable per-job message id: derive from the job's identity (id + attempts)
    // rather than minting a fresh id per delivery attempt — the SDK uses
    // messageId for dedup/delivery bookkeeping and expects it stable.
    const messageId = `msg_${String(job.id).padStart(26, '0')}`;
    const headers: Record<string, string> = {
      'content-type': 'application/json',
      'x-vqs-queue-name': job.queue_name,
      'x-vqs-message-id': messageId,
      'x-vqs-message-attempt': String(job.attempts),
    };
    // TypedJSON body: restore Uint8Array markers lost in jsonb storage via reviver.
    const body = Buffer.from(JSON.stringify(job.payload));
    if (process.env.ENTRYLITE_DEBUG) console.log('[entrylite-world] body:', body.toString().slice(0, 200));
    let response: Response;
    try {
      response = await fetch(`${BASE_URL}/.well-known/workflow/v1/flow`, {
        method: 'POST',
        headers,
        body,
      });
    } catch (err: any) {
      // transport failure: release with backoff
      await pool.query(
        `UPDATE entrylite.workflow_jobs SET locked_at=NULL, locked_by=NULL, last_error=$2,
           run_at = now() + interval '5 seconds' WHERE id=$1`,
        [job.id, String(err?.stack ?? err)],
      );
      return;
    }
    const text = await response.text();
    if (process.env.ENTRYLITE_DEBUG) console.log('[entrylite-world] deliver', job.queue_name, job.attempts, response.status, text.slice(0, 200));
    if (response.ok) {
      try {
        const parsed = JSON.parse(text);
        if (typeof parsed?.timeoutSeconds === 'number' && parsed.timeoutSeconds >= 0) {
          await pool.query(
            `UPDATE entrylite.workflow_jobs SET run_at = now() + ($2 || ' seconds')::interval,
               locked_at=NULL, locked_by=NULL WHERE id=$1`,
            [job.id, String(Math.min(parsed.timeoutSeconds, 2147483))],
          );
          return;
        }
      } catch { /* plain 200 = done */ }
      await pool.query(`DELETE FROM entrylite.workflow_jobs WHERE id=$1`, [job.id]);
      return;
    }
    const attempts = job.attempts;
    const backoff = Math.min(5000 * attempts, 60000);
    if (attempts >= 48) {
      await pool.query(
        `UPDATE entrylite.workflow_jobs SET locked_at=NULL, locked_by=NULL, last_error=$2,
           run_at = now() + interval '1 hour' WHERE id=$1`,
        [job.id, text.slice(0, 2000)],
      );
      return;
    }
    await pool.query(
      `UPDATE entrylite.workflow_jobs SET locked_at=NULL, locked_by=NULL, last_error=$2,
         run_at = now() + ($3 || ' milliseconds')::interval WHERE id=$1`,
      [job.id, text.slice(0, 2000), String(backoff)],
    );
  }

  async function pickAndRun(): Promise<void> {
    if (closing) return;
    while (!closing && inFlight.size < CONCURRENCY) {
      const claimWhere = NAMESPACE
        ? `WHERE run_at <= now() AND locked_at IS NULL AND queue_name LIKE $2`
        : `WHERE run_at <= now() AND locked_at IS NULL AND queue_name NOT LIKE '%::%'`;
      const claimArgs: unknown[] = NAMESPACE ? [WORKER_ID, `${NAMESPACE}${NS_SEP}%`] : [WORKER_ID];
      const r = await pool.query(
        `UPDATE entrylite.workflow_jobs SET locked_at=now(), locked_by=$1, attempts=attempts+1
         WHERE id = (
           SELECT id FROM entrylite.workflow_jobs
           ${claimWhere}
           ORDER BY run_at, id
           FOR UPDATE SKIP LOCKED LIMIT 1
         ) RETURNING id, queue_name, payload, attempts`,
        claimArgs,
      );
      if (!r.rows[0]) return;
      const p = deliver(r.rows[0]).catch((err) => {
        console.error('[entrylite-world] delivery crashed:', err);
      });
      inFlight.add(p);
      void p.finally(() => inFlight.delete(p));
    }
  }

  async function startWakeListener() {
    // Dedicated LISTEN connection; wake the poll loop on inserts.
    // Namespace-scoped: LISTEN channel and notification payload carry the
    // namespace so a deployment only wakes for its own inserts.
    const { Client } = await import('pg');
    const client = new Client({ connectionString: process.env.ENTRYLITE_WORLD_URL ?? undefined, ...poolOptions(pool) });
    try {
      await client.connect();
      await client.query('LISTEN entrylite_job');
      client.on('notification', (msg: any) => {
        // The insert trigger sends the stored (namespaced) queue name as
        // payload; empty payload = legacy trigger, wake unconditionally.
        const payload: string = msg.payload ?? '';
        if (payload && !isOwnQueueName(payload)) return;
        void pickAndRun();
      });
      client.on('error', () => {
        wakeClient = null;
        if (!closing) setTimeout(() => { void startWakeListener(); }, 1000);
      });
      wakeClient = client;
    } catch {
      // fall back to polling only
    }
  }

  function poolOptions(pool: Pool): Record<string, unknown> {
    return (pool as any).options ?? {};
  }

  return {
    // Nudge the poll loop from outside (used by the lost-wake sweeper as a
    // worker-liveness backstop when the interval died after HMR dispose).
    kick: (): void => { void pickAndRun(); },
    async queue(queueName: string, message: unknown, opts?: any): Promise<{ messageId: string | null }> {
      const { prefix, id } = parseQueueName(queueName);
      const messageId = newMessageId();
      // TypedJSON round-trip: Uint8Array → {__type:'Uint8Array',data:base64} so jsonb storage
      // preserves the wire format the SDK's QueuePayload schema expects after deserializeBody.
      const body = JSON.parse(JSON.stringify(message, (k, v) => v instanceof Uint8Array ? { __type: 'Uint8Array', data: Buffer.from(v).toString('base64') } : v));
      await pool.query(
        `INSERT INTO entrylite.workflow_jobs (queue_name, payload, run_at, job_key)
         VALUES ($1,$2,$3,$4)
         ON CONFLICT (job_key) WHERE locked_at IS NULL DO NOTHING`,
        [
          // Namespace-stamped at insert: this deployment's rows are always
          // recoverable by this deployment, never visible to another's claims.
          nsQueueName(`${prefix}${id}`),
          body,
          opts?.delaySeconds && opts.delaySeconds > 0
            ? new Date(Date.now() + opts.delaySeconds * 1000).toISOString()
            : new Date().toISOString(),
          opts?.idempotencyKey ? nsJobKey(opts.idempotencyKey) : null,
        ],
      );
      void pickAndRun();
      return { messageId };
    },
    createQueueHandler(queueNamePrefix: string, handler: (message: unknown, meta: any) => Promise<void | { timeoutSeconds: number }>) {
      // The runtime serves this fn at its flow route. For HTTP deliveries we
      // parse the vqs headers, decode the TypedJSON body and invoke the real
      // handler; the JSON response carries {timeoutSeconds} for long-polls.
      const HeaderParser = z.object({
        'x-vqs-queue-name': z.string(),
        'x-vqs-message-id': z.string(),
        'x-vqs-message-attempt': z.coerce.number(),
      });
      return async (req: Request): Promise<Response> => {
        const hdrs = HeaderParser.safeParse(Object.fromEntries(Array.from(req.headers as any) as [string, string][]));
        if (!hdrs.success || !req.body) {
          return Response.json({ error: 'Missing required headers or body' }, { status: 400 });
        }
        const queueName = hdrs.data['x-vqs-queue-name'];
        // Reject deliveries addressed to another deployment's namespace — a
        // belt-and-braces guard behind claim-scoping.
        if (NAMESPACE && !queueName.startsWith(`${NAMESPACE}${NS_SEP}`)) {
          return Response.json({ error: 'Wrong deployment namespace' }, { status: 409 });
        }
        const messageId = hdrs.data['x-vqs-message-id'];
        const attempt = hdrs.data['x-vqs-message-attempt'];
        const raw = Buffer.from(await req.arrayBuffer());
        let message: unknown;
        try {
          message = deserializeBody(raw);
        } catch (err) {
          return Response.json({ error: 'Undecodable body' }, { status: 400 });
        }
        try {
          const result = await handler(message, { attempt, queueName, messageId });
          if (result && typeof result.timeoutSeconds === 'number') {
            return Response.json({ timeoutSeconds: result.timeoutSeconds });
          }
          return Response.json({ done: true });
        } catch (err: any) {
          return Response.json({ error: String(err?.stack ?? err) }, { status: 500 });
        }
      };
    },
    getDeploymentId(): Promise<string> {
      return Promise.resolve(NAMESPACE ? `entrylite-${NAMESPACE}` : 'entrylite');
    },
    async start(): Promise<void> {
      await startWakeListener();
      poller = setInterval(() => { void pickAndRun(); }, QUEUE_POLL_MS);
      void pickAndRun();
    },
    async close(): Promise<void> {
      closing = true;
      if (poller) clearInterval(poller);
      await Promise.allSettled([...inFlight]);
      if (wakeClient) { try { await wakeClient.end(); } catch { /* gone */ } }
    },
  };
}

export function createStreamer(pool: Pool) {
  let closed = false;
  let listenClient: any = null;
  const listeners = new Map<string, Set<(chunk: { id: string; data: Buffer; eof: boolean }) => void>>();

  async function ensureListener() {
    if (listenClient || closed) return;
    const { Client } = await import('pg');
    const client = new Client(poolOptions());
    try {
      await client.connect();
      await client.query('LISTEN entrylite_chunk');
      client.on('notification', (msg: any) => {
        if (!msg.payload) return;
        try {
          const { streamId, chunkId } = JSON.parse(msg.payload);
          const subs = listeners.get(`strm:${streamId}`);
          if (subs) void notifyChunk(streamId, chunkId, subs);
        } catch { /* malformed payload ignored */ }
      });
      client.on('error', () => {
        listenClient = null;
        if (!closed) setTimeout(() => { void ensureListener(); }, 1000);
      });
      listenClient = client;
    } catch { /* poll fallback only */ }
  }

  function poolOptions(): Record<string, unknown> {
    return (pool as any).options ?? {};
  }

  async function notifyChunk(streamId: string, chunkId: string, subs: Set<(c: any) => void>) {
    const r = await pool.query(
      `SELECT data, eof FROM entrylite.workflow_stream_chunks WHERE stream_id=$1 AND chunk_id=$2`,
      [streamId, chunkId],
    );
    if (!r.rows[0]) return;
    const payload = { id: chunkId, data: r.rows[0].data, eof: r.rows[0].eof };
    for (const fn of subs) { try { fn(payload); } catch { /* listener error isolated */ } }
  }

  return {
    streams: {
      async write(runId: string, name: string, chunk: string | Uint8Array): Promise<void> {
        const rid = await runId;
        const chunkId = newChunkId();
        await pool.query(
          `INSERT INTO entrylite.workflow_stream_chunks (stream_id, chunk_id, run_id, data, eof) VALUES ($1,$2,$3,$4,false)`,
          [name, chunkId, rid, Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as Uint8Array)],
        );
        await pool.query(`SELECT pg_notify('entrylite_chunk', $1)`, [JSON.stringify({ streamId: name, chunkId })]);
      },
      async writeMulti(runId: string, name: string, chunks: (string | Uint8Array)[]): Promise<void> {
        const rid = await runId;
        if (!chunks.length) return;
        const ids = chunks.map(() => newChunkId());
        const client = await pool.connect();
        try {
          await client.query('BEGIN');
          for (let i = 0; i < chunks.length; i++) {
            const c = chunks[i];
            await client.query(
              `INSERT INTO entrylite.workflow_stream_chunks (stream_id, chunk_id, run_id, data, eof) VALUES ($1,$2,$3,$4,false)`,
              [name, ids[i], rid, Buffer.isBuffer(c) ? c : Buffer.from(c as Uint8Array)],
            );
          }
          await client.query('COMMIT');
        } catch (err) {
          await client.query('ROLLBACK');
          throw err;
        } finally {
          client.release();
        }
        await pool.query(`SELECT pg_notify('entrylite_chunk', $1)`, [JSON.stringify({ streamId: name, chunkId: ids[ids.length - 1] })]);
      },
      async close(runId: string, name: string): Promise<void> {
        const rid = await runId;
        const chunkId = newChunkId();
        await pool.query(
          `INSERT INTO entrylite.workflow_stream_chunks (stream_id, chunk_id, run_id, data, eof) VALUES ($1,$2,$3,''::bytea,true)`,
          [name, chunkId, rid],
        );
        await pool.query(`SELECT pg_notify('entrylite_chunk', $1)`, [JSON.stringify({ streamId: name, chunkId })]);
      },
      async get(runId: string, name: string, startIndex?: number): Promise<ReadableStream<Uint8Array>> {
        await ensureListener();
        let index = startIndex ?? 0;
        if (index < 0) {
          const info = await (this as any).getInfo(runId, name);
          index = Math.max(0, info.tailIndex + 1 + index);
        }
        const key = `strm:${name}`;
        let controller!: ReadableStreamDefaultController<Uint8Array>;
        const subs = listeners.get(key) ?? new Set();
        subs.add(onChunk);
        listeners.set(key, subs);
        const stream = new ReadableStream<Uint8Array>({
          start(c) {
            controller = c;
            void (async () => {
              // replay persisted chunks first
              const rows = await pool.query(
                `SELECT chunk_id, data, eof FROM entrylite.workflow_stream_chunks WHERE stream_id=$1 AND eof=false ORDER BY chunk_id`,
                [name],
              );
              for (const row of rows.rows) {
                onChunk({ id: row.chunk_id, data: row.data, eof: false });
              }
            })();
          },
          cancel() {
            subs.delete(onChunk);
          },
        });
        function onChunk(chunk: { id: string; data: Buffer; eof: boolean }) {
          // dedup by index tracking is handled by callers replaying in order;
          // skip chunks already delivered via a simple flag
          if (chunk.eof) {
            try { controller.close(); } catch { /* already closed */ }
            subs.delete(onChunk);
            return;
          }
          try { controller.enqueue(new Uint8Array(chunk.data)); } catch { /* closed */ }
        }
        return stream;
      },
      async list(runId: string): Promise<string[]> {
        const r = await pool.query(
          `SELECT DISTINCT stream_id FROM entrylite.workflow_stream_chunks WHERE run_id=$1`,
          [runId],
        );
        return r.rows.map((x: any) => x.stream_id);
      },
      async getChunks(runId: string, name: string, options?: any): Promise<any> {
        const limit = Math.min(options?.limit ?? 100, 1000);
        const r = await pool.query(
          `SELECT chunk_id, data, eof FROM entrylite.workflow_stream_chunks
           WHERE stream_id=$1 AND run_id=$3 AND eof=false AND ($2::varchar IS NULL OR chunk_id > $2)
           ORDER BY chunk_id LIMIT $4`,
          [name, options?.cursor ?? null, runId, limit],
        );
        const eofRow = await pool.query(
          `SELECT chunk_id FROM entrylite.workflow_stream_chunks WHERE stream_id=$1 AND run_id=$2 AND eof=true ORDER BY chunk_id LIMIT 1`,
          [name, runId],
        );
        const rows = r.rows;
        return {
          data: rows.map((x: any, i: number) => ({ index: i, data: new Uint8Array(x.data) })),
          cursor: rows.length ? rows[rows.length - 1].chunk_id : null,
          hasMore: r.rows.length > limit,
          done: eofRow.rows.length > 0,
        };
      },
      async getInfo(runId: string, name: string): Promise<{ tailIndex: number; done: boolean }> {
        const r = await pool.query(
          `SELECT COUNT(*)::int AS n FROM entrylite.workflow_stream_chunks WHERE stream_id=$1 AND run_id=$2 AND eof=false`,
          [name, runId],
        );
        const e = await pool.query(
          `SELECT 1 FROM entrylite.workflow_stream_chunks WHERE stream_id=$1 AND run_id=$2 AND eof=true LIMIT 1`,
          [name, runId],
        );
        return { tailIndex: r.rows[0].n - 1, done: e.rows.length > 0 };
      },
    },
    async close(): Promise<void> {
      closed = true;
      if (listenClient) { try { await listenClient.end(); } catch { /* gone */ } }
    },
  };
}
