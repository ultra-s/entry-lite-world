// Throughput + completion + stale-lease recovery verification (post-namespace).
// 2000 jobs / one namespace / 5 workers / restart 2 mid-run / verify full drain + no cross-ns.
import { spawn } from 'node:child_process';

const CS = process.env.ENTRYLITE_WORLD_URL;
const NS = 'perfns';
const RUNS = 2000;
const LIFETIME = '110000';

const workerSrc = `
const CS = process.env.CS;
const { createWorld } = await import('/home/agentuser/entry-lite-world/dist/index.js');
const w = createWorld({ url: CS, maxPoolSize: 8 });
setTimeout(async () => { try { await w.close(); } catch {} process.exit(0); }, ${LIFETIME});
`;
const fs = await import('node:fs');
fs.writeFileSync('/tmp/perf_worker.mjs', workerSrc.replace('${LIFETIME}', '110000'));

const t0 = Date.now();
const procs = [];
for (let p = 0; p < 5; p++) {
  procs.push(spawn('node', ['/tmp/perf_worker.mjs'], {
    env: { ...process.env, CS, ENTRYLITE_QUEUE_NAMESPACE: NS },
    stdio: ['ignore', 'ignore', 'inherit'],
  }));
}
// restart two mid-run
setTimeout(() => { procs[1].kill(); procs[1] = spawn('node', ['/tmp/perf_worker.mjs'], { env: { ...process.env, CS, ENTRYLITE_QUEUE_NAMESPACE: NS }, stdio: ['ignore', 'ignore', 'inherit'] }); }, 20000);
setTimeout(() => { procs[3].kill(); procs[3] = spawn('node', ['/tmp/perf_worker.mjs'], { env: { ...process.env, CS, ENTRYLITE_QUEUE_NAMESPACE: NS }, stdio: ['ignore', 'ignore', 'inherit'] }); }, 35000);

// fire via a separate namespaced process (the workers consume)
const firer = spawn('node', ['--input-type=module', '-e', `
process.env.ENTRYLITE_QUEUE_NAMESPACE = 'perfns';
const { createWorld } = await import('/home/agentuser/entry-lite-world/dist/index.js');
const w = createWorld({ url: process.env.CS, maxPoolSize: 8 });
for (let i = 0; i < ${RUNS}; i++) {
  await w.queue('__wkf_workflow_target', { i }, { idempotencyKey: 'p' + i });
}
console.log('fired ${RUNS}');
setTimeout(() => process.exit(0), 2000);
`], { env: { ...process.env, CS }, stdio: ['ignore', 'inherit', 'inherit'] });

const pg = (await import('/home/agentuser/entry-lite-world/node_modules/pg/lib/index.js')).default;
const pool = new pg.Pool({ connectionString: CS, max: 2 });
// wait for drain: all jobs consumed (deleted on delivery success; BASE_URL dead → retried → 1h park after 48 attempts, but attempts cap... use drain-to-locked-0 or terminal attempts)
let deadline = Date.now() + 180000;
let last = '';
while (Date.now() < deadline) {
  const r = await pool.query(`SELECT count(*)::int total, count(*) filter (where locked_at is null)::int unlocked, coalesce(sum(attempts),0)::int attempts FROM entrylite.workflow_jobs WHERE queue_name LIKE '${NS}::%'`);
  last = `total=${r.rows[0].total} unlocked=${r.rows[0].unlocked} attempts=${r.rows[0].attempts}`;
  if (r.rows[0].unlocked === 0 && r.rows[0].total > 0) break;
  await new Promise(res => setTimeout(res, 2000));
}
const wall = Date.now() - t0;
const fin = await pool.query(`SELECT count(*)::int total, sum(attempts)::int att FROM entrylite.workflow_jobs WHERE queue_name LIKE '${NS}::%'`);
// cross-namespace leak check: no foreign-ns row ever locked
const leak = await pool.query(`SELECT count(*)::int n FROM entrylite.workflow_jobs WHERE queue_name NOT LIKE '${NS}::%' AND locked_by IS NOT NULL`);
console.log(JSON.stringify({ ns: NS, fired: RUNS, wallMs: wall, remaining: fin.rows[0].total, totalAttempts: fin.rows[0].att, foreignLocked: leak.rows[0].n, last }));
process.exit(0);
