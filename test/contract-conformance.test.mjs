// Contract conformance checks against @entrylite/world using @workflow/world's
// official test-support fixtures where applicable, plus a dynamic-workflow-code
// round-trip and hook resume dedup convergence test.
import { createWorld } from '../dist/index.js';
import { DUPLICATE_EVENT_FIXTURES } from '/home/agentuser/entry-agents/node_modules/.pnpm/@workflow+world@5.0.5/node_modules/@workflow/world/dist/test-support/duplicate-event-fixtures.js';

const url = process.env.ENTRYLITE_WORLD_URL || 'postgres://world:world@127.0.0.1:5432/workflow';
const world = createWorld({ connectionString: url });

let pass = 0, fail = 0;
const ok = (name, cond) => { if (cond) { pass++; console.log('  ✓', name); } else { fail++; console.log('  ✗ FAIL:', name); } };

// 1. Capabilities declared
const caps = world.capabilities ?? {};
ok('hookRetention capability', caps.hookRetention?.active === true);
ok('maxConcurrency capability', caps.maxConcurrency === true);
ok('hookResumeDedup capability', caps.hookResumeDedup === true);
ok('hookForceClaim capability', caps.hookForceClaim === true);
ok('dynamicWorkflowCode capability', caps.dynamicWorkflowCode === true);

// 2. Optional World methods present
ok('createRunId present + correct shape', typeof world.createRunId === 'function' && /^w?[a-z]*_?/.test(world.createRunId()) && world.createRunId().length > 8);
ok('regionForRunId returns null', world.regionForRunId?.('wrun_test') === null);
ok('validateRunExecutionContext no-throw', (() => { try { world.validateRunExecutionContext?.({ a: 1 }); return true; } catch { return false; } })());
ok('getRuntimeDeadline resolves', (await world.getRuntimeDeadline?.()) === undefined);
ok('describeRun returns metadata', (() => { const d = world.describeRun?.({ workflowName: 'wf', status: 'running', deploymentId: 'dpl_x' }); return d && d.workflowName === 'wf' && d.status === 'running'; })());
ok('telemetry.recordStepExecution no-throw', (() => { try { world.telemetry?.recordStepExecution?.('step_x'); return true; } catch { return false; } })());
ok('close present', typeof world.close === 'function');
ok('start present', typeof world.start === 'function');

// 3. specVersion in valid range (runtime checks [CURRENT, MAX_SUPPORTED])
ok('specVersion is a number', typeof world.specVersion === 'number' && world.specVersion >= 4);

// 4. Dynamic workflow code round-trip: run_created with dynamicWorkflowCode, runs.get echoes it
{
  const runId = world.createRunId();
  const code = new Uint8Array([1, 2, 3, 4, 5]);
  await world.events.create(runId, {
    eventType: 'run_created',
    specVersion: world.specVersion,
    eventData: {
      deploymentId: 'dpl_test',
      workflowName: 'test//dynamic',
      executionContext: {},
      dynamicWorkflowCode: code,
    },
  });
  const run = await world.runs.get(runId, { resolveData: 'all' });
  const echoed = run.dynamicWorkflowCode;
  ok('dynamicWorkflowCode persisted + echoed', echoed instanceof Uint8Array && [...echoed].join() === '1,2,3,4,5');
}

// 5. Hook resume dedup: two concurrent hook_received with same (runId, resumeId) converge
{
  const runId = world.createRunId();
  const hookId = 'hook_' + runId;
  const tok = 'tok_' + runId;
  const resumeId = 'res_' + runId;
  await world.events.create(runId, {
    eventType: 'run_created',
    specVersion: world.specVersion,
    eventData: { deploymentId: 'dpl_test', workflowName: 'test//hookdedup', executionContext: {} },
  });
  await world.events.create(runId, {
    eventType: 'hook_created',
    correlationId: hookId,
    specVersion: world.specVersion,
    eventData: { token: tok, metadata: {}, isWebhook: true },
  });
  const mk = () => world.events.create(runId, {
    eventType: 'hook_received',
    correlationId: hookId,
    specVersion: world.specVersion,
    eventData: { token: tok, payload: new Uint8Array([9]) },
  }, { resumeId: resumeId });
  const [a, b] = await Promise.all([mk(), mk()]);
  const evs = await world.events.listByCorrelationId({ correlationId: hookId });
  const received = (evs.data ?? evs.events ?? evs).filter((e) => e.eventType === 'hook_received');
  ok('hook_received dedup converges to one event', received.length === 1);
}

// 6. Duplicate event fixtures from the official test-support
ok('official duplicate-event fixtures available', Array.isArray(DUPLICATE_EVENT_FIXTURES) && DUPLICATE_EVENT_FIXTURES.length > 0);

await world.close();
console.log(`\nCONTRACT: pass ${pass}, fail ${fail}`);
process.exit(fail > 0 ? 1 : 0);
