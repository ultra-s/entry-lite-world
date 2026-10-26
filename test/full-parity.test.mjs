// Full-parity surface test: createBatch, waitForTerminalStatus, getMany,
// cancelMany, experimentalSetAttributes, analytics, queueBatch, registerRoute/
// registerHandler, getEnvironment.
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
const CS = process.env.ENTRYLITE_WORLD_URL ?? 'postgres://world:world@127.0.0.1:5432/workflow';
const { createWorld } = await import('../dist/index.js');
const w = createWorld({ url: CS });

const mkRun = async (name, attrs = {}) => {
  const runId = w.createRunId();
  await w.events.create(runId, {
    specVersion: 8, eventType: 'run_created',
    eventData: { deploymentId: 'dpl_test', workflowName: name, workflowCode: new Uint8Array([1, 2, 3]), context: {}, ...attrs },
  });
  return runId;
};
const step = (runId, stepId, type, extra = {}) =>
  w.events.create(runId, { specVersion: 8, eventType: type, correlationId: stepId, eventData: { stepId, name: stepId, ...extra } });

try {
  // ---------- createBatch: step_created + step_started born-running pair ----------
  const runA = await mkRun('wfBatch');
  const batch = await w.events.createBatch(runA, [
    { event: { specVersion: 8, eventType: 'step_created', correlationId: 'st1', eventData: { stepId: 'st1', stepName: 'st1' } } },
    { event: { specVersion: 8, eventType: 'step_started', correlationId: 'st1', eventData: { stepId: 'st1', stepName: 'st1' } } },
  ]);
  assert.equal(batch.results.length, 2, 'one result per submitted event');
  assert.ok(batch.results.every(r => r.status === 200), 'born-running pair commits: ' + JSON.stringify(batch.results.map(r => r.status + ':' + (r.message ?? 'ok'))));
  // non-batchable event rejects per-event, survivors commit
  const batch2 = await w.events.createBatch(runA, [
    { event: { specVersion: 8, eventType: 'step_completed', correlationId: 'st1', eventData: { stepId: 'st1', stepName: 'st1', output: 42 } } },
    { event: { specVersion: 8, eventType: 'run_cancelled', correlationId: runA, eventData: {} } },
  ]);
  assert.equal(batch2.results[0].status, 200, 'step_completed commits');
  assert.notEqual(batch2.results[1].status, 200, 'run_cancelled rejected in batch');

  // ---------- waitForTerminalStatus ----------
  const runB = await mkRun('wfWait');
  const w1 = w.runs.waitForTerminalStatus(runB, { timeoutMs: 500 });
  const snap = await w1; // no terminal transition → snapshot degrade
  assert.ok(snap && snap.runId === runB, 'waitForTerminalStatus degrades to snapshot');

  // ---------- getMany ----------
  const many = await w.runs.getMany([runA, runB, 'wrun_missing']);
  assert.equal(many.length, 3);
  assert.ok(many[0] && many[1], 'existing runs returned');
  assert.equal(many[2], null, 'missing run → null');

  // ---------- cancelMany ----------
  const runC = await mkRun('wfCancel');
  const runD = await mkRun('wfCancel');
  await w.events.create(runD, { specVersion: 8, eventType: 'run_completed', eventData: { result: 1 } });
  const cm = await w.runs.cancelMany({ runIds: [runC, runD, 'wrun_missing'] });
  const byId = Object.fromEntries(cm.results.map(r => [r.runId, r.outcome]));
  assert.equal(byId[runC], 'cancelled');
  assert.equal(byId[runD], 'not_cancellable');
  assert.equal(byId['wrun_missing'], 'not_found');

  // ---------- experimentalSetAttributes ----------
  const sa = await w.runs.experimentalSetAttributes(runA, [
    { key: 'env', value: 'staging' }, { key: 'tenant', value: 'acme' },
  ]);
  assert.equal(sa.attributes.env, 'staging');
  const sa2 = await w.runs.experimentalSetAttributes(runA, [{ key: 'tenant', value: null }]);
  assert.ok(!('tenant' in sa2.attributes), 'null removes key');
  await assert.rejects(
    () => w.runs.experimentalSetAttributes(runA, [{ key: '$sys', value: 'x' }]),
    /reserved/,
    '$-prefix rejected by default',
  );

  // ---------- analytics ----------
  const aRuns = await w.analytics.runs.list({ workflowName: 'wfBatch' });
  assert.ok(aRuns.data.some(r => r.runId === runA), 'analytics.runs.list finds run');
  const aRun = await w.analytics.runs.get(runA);
  assert.equal(aRun.runId, runA);
  const aSteps = await w.analytics.steps.list({ runId: runA });
  assert.ok(aSteps.data.length >= 1, 'analytics.steps.list');
  const aStep = await w.analytics.steps.get(runA, 'st1');
  assert.equal(aStep.stepId, 'st1');
  const aEvents = await w.analytics.events.list({ runId: runA });
  assert.ok(aEvents.data.length >= 3, 'analytics.events.list');
  const aByCorr = await w.analytics.events.listByCorrelationId({ runId: runA, correlationId: 'st1' });
  assert.ok(aByCorr.data.length >= 3, 'analytics.events.listByCorrelationId');
  const aMany = await w.analytics.events.getMany(runA, aEvents.data.slice(0, 2).map(e => e.eventId));
  assert.equal(aMany.length, 2, 'analytics.events.getMany');
  const aHooks = await w.analytics.hooks.list({});
  assert.ok(Array.isArray(aHooks.data), 'analytics.hooks.list');
  const aWaits = await w.analytics.waits.list({ runId: runA });
  assert.ok(Array.isArray(aWaits.data), 'analytics.waits.list');
  const aKeys = await w.analytics.attributes.list({});
  assert.ok(aKeys.data.some(k => k.key === 'env'), 'analytics.attributes.list sees attr keys');

  // ---------- queueBatch ----------
  const qb = await w.queueBatch('__wkf_workflow_wfBatch', [
    { message: { runId: runA } }, { message: { runId: runA } },
  ]);
  assert.equal(qb.length, 2);
  assert.ok(qb.every(r => r.messageId), 'queueBatch returns messageIds');
  await w.clear().catch?.(() => {});
  // clean the two jobs so they don't sit queued
  const pg = (await import('pg')).default;
  const pool = new pg.Pool({ connectionString: CS });
  await pool.query(`DELETE FROM entrylite.workflow_jobs WHERE queue_name LIKE '%wfBatch%'`);
  await pool.end();

  // ---------- registerRoute / registerHandler / getEnvironment ----------
  w.registerRoute('__wkf_custom_', '/api/custom-queue');
  w.registerHandler('__wkf_custom_', async () => new Response('{}'));
  assert.equal(w.getRouteForPrefix('__wkf_custom_'), '/api/custom-queue');
  // env: unset → undefined
  assert.equal(w.getEnvironment(), undefined);

  console.log('PASS: full-parity surface (batch, wait, bulk, attrs, analytics, queueBatch, routes, env)');
} finally {
  try { await w.close(); } catch {}
}
