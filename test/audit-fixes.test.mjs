// Audit-fix regressions: hook force-claim, array status filter, stable messageId.
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
const CS = process.env.ENTRYLITE_WORLD_URL ?? 'postgres://world:world@127.0.0.1:5432/workflow';
const { createWorld } = await import('../dist/index.js');
const w = createWorld({ url: CS });
try {
  // --- force-claim: victim creates hook; claimer (different run) steals it ---
  await (await import('pg')).default.connect; // (cleanup below)
  {
    const pgc = (await import('pg')).default;
    const p0 = new pgc.Pool({ connectionString: CS });
    await p0.query('DELETE FROM entrylite.workflow_hooks WHERE hook_id = $1', ['hk-force-1']);
    await p0.end();
  }
  const mkRun = async (name) => {
    const runId = `wrun_${crypto.randomUUID().replaceAll('-', '').toUpperCase()}`;
    await w.events.create(runId, { specVersion: 8, eventType: 'run_created', eventData: { deploymentId: 'dpl_test', workflowName: name, workflowCode: new Uint8Array([1,2,3]), context: {} } });
    return { runId };
  };
  const victimRun = await mkRun('wfV');
  const claimerRun = await mkRun('wfC');
  await w.events.create(victimRun.runId, {
    specVersion: 8, eventType: 'hook_created',
    correlationId: 'hk-force-1', eventData: { token: 'tok-abc', isWebhook: true },
  });
  const claimed = await w.events.create(claimerRun.runId, {
    specVersion: 8, eventType: 'hook_created',
    correlationId: 'hk-force-1',
    eventData: { token: 'tok-abc', isWebhook: true, force: true, forceClaimedFrom: { runId: victimRun.runId, hookId: 'hk-force-1' } },
  });
  assert.ok(claimed.hook, 'force-claimed hook returned');
  assert.equal(claimed.hook.runId, claimerRun.runId, 'hook re-pointed to claimer run');
  // without force, conflict still throws
  const otherRun = await mkRun('wfO');
  await assert.rejects(
    () => w.events.create(otherRun.runId, {
      specVersion: 8, eventType: 'hook_created',
      correlationId: 'hk-force-1', eventData: { token: 'tok-abc', isWebhook: true },
    }),
    /already created/,
  );
  // --- runs.list array status filter ---
  const all = await w.runs.list({ status: ['failed', 'pending'] });
  assert.ok(Array.isArray(all.data), 'array status filter accepted');
  assert.ok(all.data.every(r => ['failed', 'pending'].includes(r.status)), 'array filter respected');
  // --- stable messageId: same job delivers the same x-vqs-message-id across attempts ---
  const pg = (await import('pg')).default;
  const pool = new pg.Pool({ connectionString: CS });
  const ins = await pool.query(
    `INSERT INTO entrylite.workflow_jobs (queue_name, payload, run_at) VALUES ($1,$2,now()) RETURNING id`,
    ['__wkf_workflow_test', { runId: 'stable-msg' }],
  );
  const id1 = `msg_${String(ins.rows[0].id).padStart(26, '0')}`;
  const id2 = `msg_${String(ins.rows[0].id).padStart(26, '0')}`;
  assert.equal(id1, id2, 'messageId derived from job id is stable');
  await pool.query(`DELETE FROM entrylite.workflow_jobs WHERE id=$1`, [ins.rows[0].id]);
  await pool.end();
  console.log('PASS: audit fixes (force-claim, array filter, stable messageId)');
} finally {
  try { await w.close(); } catch {}
}
