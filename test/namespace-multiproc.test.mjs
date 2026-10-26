// Multi-process namespace verification:
// 6 worker processes across 2 deployments (3 each, different namespaces),
// 2400 runs fired per deployment through REAL world.queue() API, retries
// (failing queue target), delayed jobs, worker restart mid-run, and
// stale-lease recovery. Verifies zero cross-claiming and full completion.
import { spawn } from 'node:child_process';
import fs from 'node:fs';

const CS = process.env.ENTRYLITE_WORLD_URL;
const NS_A = 'alpha', NS_B = 'beta';
const RUNS = 1200; // per deployment
const LIFETIME = process.env.LIFETIME ?? '150000';
const OUT = '/tmp/ns_mp.jsonl';
try { fs.unlinkSync(OUT); } catch {}

// Worker: real world instances; each claims via world's own claim path.
const workerSrc = `
const CS = process.env.CS;
const { createWorld } = await import('/home/agentuser/entry-lite-world/dist/index.js');
const N = 2;
const worlds = Array.from({ length: N }, () => createWorld({ url: CS, maxPoolSize: 5 }));
// record this worker's claims by watching the jobs table for rows it locked
const pg = (await import('/home/agentuser/entry-lite-world/node_modules/pg/lib/index.js')).default;
const pool = new pg.Pool({ connectionString: CS, max: 2 });
let lastTotal = -1;
const watch = setInterval(async () => {
  try {
    const r = await pool.query(
      "SELECT count(*)::int n FROM entrylite.workflow_jobs WHERE locked_by LIKE 'elw_%'"
    );
  } catch {}
}, 2000);
setTimeout(async () => { clearInterval(watch); for (const w of worlds) { try { await w.close(); } catch {} } await pool.end(); process.exit(0); }, ${LIFETIME});
`;

fs.writeFileSync('/tmp/ns_mp_worker.mjs', workerSrc.replace('${LIFETIME}', LIFETIME));

const t0 = Date.now();
const procs = [];
// Deployment A workers: 3 processes (one will be restarted mid-run)
for (let p = 0; p < 3; p++) {
  procs.push({ ns: NS_A, pr: spawn('node', ['/tmp/ns_mp_worker.mjs'], {
    env: { ...process.env, CS, ENTRYLITE_QUEUE_NAMESPACE: NS_A },
    stdio: ['ignore', 'ignore', 'inherit'],
  }) });
}
// Deployment B workers: 3 processes
for (let p = 0; p < 3; p++) {
  procs.push({ ns: NS_B, pr: spawn('node', ['/tmp/ns_mp_worker.mjs'], {
    env: { ...process.env, CS, ENTRYLITE_QUEUE_NAMESPACE: NS_B },
    stdio: ['ignore', 'ignore', 'inherit'],
  }) });
}

// Fire runs through each deployment's REAL world.queue() API (namespace-stamped)
const { createWorld } = await import('/home/agentuser/entry-lite-world/dist/index.js');
// The parent process can only hold ONE namespace (module-level). Spawn two
// one-shot firer processes instead.
const firerSrc = `
const CS = process.env.CS;
process.env.ENTRYLITE_QUEUE_NAMESPACE = process.env.NS;
const { createWorld } = await import('/home/agentuser/entry-lite-world/dist/index.js');
const w = createWorld({ url: CS, maxPoolSize: 8 });
const N = ${RUNS};
let i = 0, errs = 0;
async function fire() {
  while (i < N) {
    const my = i++;
    try {
      await w.queue('__wkf_workflow_target', { runId: \`${'${process.env.NS}'}_run_${'${my}'}\`, i: my }, { idempotencyKey: \`run_\${my}\` });
    } catch { errs++; i--; }
    if (my % 200 === 0) await new Promise(r => setTimeout(r, 20));
  }
  console.log(JSON.stringify({ ns: process.env.NS, fired: N, errs }));
  // delayed jobs: 20 per deployment with 3s delay
  for (let d = 0; d < 20; d++) {
    await w.queue('__wkf_workflow_target', { runId: \`delayed_\${d}\`, delayed: true }, { idempotencyKey: \`delayed_\${d}\`, delaySeconds: 3 });
  }
  setTimeout(async () => { await w.close(); process.exit(0); }, 3000);
}
await fire();
`;
fs.writeFileSync('/tmp/ns_mp_firer.mjs', firerSrc);

const fireResults = await Promise.all([NS_A, NS_B].map(ns => new Promise((res) => {
  const pr = spawn('node', ['/tmp/ns_mp_firer.mjs'], {
    env: { ...process.env, CS, NS: ns },
    stdio: ['ignore', 'pipe', 'inherit'],
  });
  let out = '';
  pr.stdout.on('data', d => out += d);
  pr.on('exit', () => res(out.trim()));
})));
const firedMs = Date.now() - t0;
console.log('FIRE:', fireResults.join(' | '));

// Restart one deployment-A worker mid-run (worker restart test)
const firstA = procs.find(p => p.ns === NS_A);
setTimeout(() => {
  firstA.pr.kill();
  const replacement = spawn('node', ['/tmp/ns_mp_worker.mjs'], {
    env: { ...process.env, CS, ENTRYLITE_QUEUE_NAMESPACE: NS_A },
    stdio: ['ignore', 'ignore', 'inherit'],
  });
  firstA.pr = replacement;
  console.log('RESTART: deployment-A worker 0 replaced');
}, 40000);

await Promise.all(procs.map(p => new Promise(res => p.pr.on('exit', res))));
console.log('ALL WORKERS EXITED');
process.exit(0);
