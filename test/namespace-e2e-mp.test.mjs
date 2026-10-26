// End-to-end consumer harness: a minimal HTTP endpoint standing in for the
// workflow runtime's /.well-known/workflow/v1/flow consumer, plus 2 worlds
// per deployment — proves isolation end-to-end with REAL delivery.
// Deployment A's consumer 200s (job done → DELETE). Deployment B's consumer
// returns 500 on first attempt per job (retry path) then 200.
import { spawn } from 'node:child_process';
import http from 'node:http';

const CS = process.env.ENTRYLITE_WORLD_URL;

const consumer = (name, failFirst) => {
  const seen = new Set();
  return http.createServer((req, res) => {
    let b = '';
    req.on('data', c => b += c);
    req.on('end', async () => {
      const id = req.headers['x-vqs-message-id'];
      const q = req.headers['x-vqs-queue-name'];
      let body = {};
      try { body = JSON.parse(b); } catch {}
      if (!q || !q.startsWith(name + '::')) {
        // CROSS-CLAIM PROBE: record any queue delivered that is not ours
        globalThis.__leaks.push({ name, got: q });
      }
      const attempt = parseInt(String(req.headers['x-vqs-message-attempt'] ?? '1'), 10);
      if (failFirst && attempt < 2) { seen.add(id); res.writeHead(500); res.end('fail-first'); return; }
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end('{}');
    });
  });
};

globalThis.__leaks = [];
const srvA = consumer('nsa', false);
const srvB = consumer('nsb', true);
await new Promise(r => srvA.listen(34101, r));
await new Promise(r => srvB.listen(34102, r));

const workerSrc = `
const CS = process.env.CS;
const { createWorld } = await import('/home/agentuser/entry-lite-world/dist/index.js');
const w = createWorld({ url: CS, maxPoolSize: 6 });
setTimeout(async () => { try { await w.close(); } catch {} process.exit(0); }, 65000);
`;
const fs = await import('node:fs');
fs.writeFileSync('/tmp/e2e_worker.mjs', workerSrc);

const procs = [];
for (const [ns, port] of [['nsa', 34101], ['nsa', 34101], ['nsb', 34102], ['nsb', 34102]]) {
  procs.push(spawn('node', ['/tmp/e2e_worker.mjs'], {
    env: { ...process.env, CS, ENTRYLITE_QUEUE_NAMESPACE: ns, WORKFLOW_LOCAL_BASE_URL: `http://127.0.0.1:${port}` },
    stdio: ['ignore', 'ignore', 'inherit'],
  }));
}

// fire 300 jobs per deployment (incl. 20 delayed + retry-target jobs)
const firer = spawn('node', ['--input-type=module', '-e', `
const { createWorld } = await import('/home/agentuser/entry-lite-world/dist/index.js');
const w = createWorld({ url: process.env.CS, maxPoolSize: 6 });
const NS = process.env.NS;
for (let i = 0; i < 300; i++) {
  await w.queue('__wkf_workflow_target', { i, ns: NS }, { idempotencyKey: NS + i });
}
for (let d = 0; d < 20; d++) {
  await w.queue('__wkf_workflow_target', { d, ns: NS, delayed: true }, { idempotencyKey: NS + 'd' + d, delaySeconds: 2 });
}
console.log('fired', NS);
setTimeout(() => process.exit(0), 2000);
`], { env: { ...process.env, CS, ENTRYLITE_QUEUE_NAMESPACE: 'FIRER_NS' }, stdio: ['ignore', 'inherit', 'inherit'] });
// NOTE: firer env ENTRYLITE_QUEUE_NAMESPACE is overridden below per-deployment:
firer.kill();
for (const NS of ['nsa', 'nsb']) {
  spawn('node', ['--input-type=module', '-e', `
const { createWorld } = await import('/home/agentuser/entry-lite-world/dist/index.js');
const w = createWorld({ url: process.env.CS, maxPoolSize: 6 });
const NS = process.env.NS;
for (let i = 0; i < 300; i++) {
  await w.queue('__wkf_workflow_target', { i, ns: NS }, { idempotencyKey: NS + i });
}
for (let d = 0; d < 20; d++) {
  await w.queue('__wkf_workflow_target', { d, ns: NS, delayed: true }, { idempotencyKey: NS + 'd' + d, delaySeconds: 2 });
}
console.log('fired', NS);
setTimeout(() => process.exit(0), 2000);
`], { env: { ...process.env, CS, NS, ENTRYLITE_QUEUE_NAMESPACE: NS }, stdio: ['ignore', 'inherit', 'inherit'] });
}

// barrier: wait until jobs exist (firers have landed rows) before draining
const pgPre = (await import('/home/agentuser/entry-lite-world/node_modules/pg/lib/index.js')).default;
const poolPre = new pgPre.Pool({ connectionString: CS, max: 1 });
for (let w2 = 0; w2 < 120; w2++) {
  const c = await poolPre.query('SELECT count(*)::int n FROM entrylite.workflow_jobs');
  if (c.rows[0].n >= 600) break;
  await new Promise(r => setTimeout(r, 1000));
}
await poolPre.end();
const t0 = Date.now();
const pg = (await import('/home/agentuser/entry-lite-world/node_modules/pg/lib/index.js')).default;
const pool = new pg.Pool({ connectionString: CS, max: 2 });
let deadline = Date.now() + 180000;
let snaps = '';
while (Date.now() < deadline) {
  const r = await pool.query(`SELECT count(*)::int n FROM entrylite.workflow_jobs`);
  if (r.rows[0].n === 0) break;
  await new Promise(res => setTimeout(res, 2000));
}
const wall = Date.now() - t0;
console.log('LEAKS:', JSON.stringify(globalThis.__leaks));
console.log(JSON.stringify({ wallMs: wall, remainingJobs: (await pool.query('SELECT count(*)::int n FROM entrylite.workflow_jobs')).rows[0].n }));
srvA.close(); srvB.close();
process.exit(0);
