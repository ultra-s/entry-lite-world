// 100 shared-DB world instances, 3000 runs (5-step workflow) — MULTI-PROCESS.
// 20 worker processes × 5 instances × 30 runs each. Parent aggregates via JSONL file.
import { spawn } from 'node:child_process';
import fs from 'node:fs';

const CS = process.env.ENTRYLITE_WORLD_URL;
const N_PROC = 5, INST_PER = 5, RUNS_PER = 30;
const OUT = '/tmp/inst100_worker.jsonl';
try { fs.unlinkSync(OUT); } catch {}

const workerSrc = `
process.env.WORKFLOW_TARGET_WORLD = process.env.TTW;
process.env.ENTRYLITE_WORLD_URL = process.env.CS;
process.env.WORKFLOW_LOCAL_BASE_URL = process.env.WLB;
const CS = process.env.CS;
const { createWorld } = await import('/home/agentuser/entry-lite-world/dist/index.js');
const W = ${INST_PER}, R = ${RUNS_PER};
const worlds = Array.from({ length: W }, () => createWorld({ url: CS, maxPoolSize: 3 }));
const runIds = [];
let fireErrs = 0;
const t0 = Date.now();
for (const w of worlds) {
  for (let r = 0; r < R; r++) {
    try {
      const runId = await w.createRunId();
      await w.start({ workflowName: 'instTest', workflowCode: process.env.WF, runId, args: [r] });
      runIds.push(runId);
    } catch { fireErrs++; }
  }
}
(await import('node:fs')).appendFileSync(process.env.OUT, JSON.stringify({ runIds, fireErrs, fireMs: Date.now() - t0 }) + '\\n');
// keep worlds alive 15s so queued jobs deliver, then close and exit
setTimeout(async () => { for (const w of worlds) { try { await w.close(); } catch {} } process.exit(0); }, 15000);
`;

fs.writeFileSync('/tmp/inst100_worker.mjs', workerSrc.replace("require('fs')", "(await import('node:fs'))"));

const WF_CODE = `
export async function instTest(n) {
  "use workflow";
  let out = 0;
  for (let i = 0; i < 5; i++) { out = await bump(out, i); }
  return out;
}
export async function bump(acc, i) { "use step"; return acc + 1; }
`;

const t0 = Date.now();
const procs = [];
for (let p = 0; p < N_PROC; p++) {
  procs.push(spawn('node', ['/tmp/inst100_worker.mjs'], {
    env: { ...process.env, CS, OUT, WF: WF_CODE, TTW: './workflow-world.ts', WLB: 'http://localhost:3000' },
    stdio: ['ignore', 'inherit', 'inherit'],
    cwd: '/home/agentuser/entry-agents/apps/web',
  }));
}
await Promise.all(procs.map(pr => new Promise(res => pr.on('exit', res))));
const fireMs = Date.now() - t0;

// aggregate
const lines = fs.readFileSync(OUT, 'utf8').trim().split('\n').filter(Boolean).map(JSON.parse);
const runIds = lines.flatMap(l => l.runIds);
const fireErrs = lines.reduce((a, l) => a + l.fireErrs, 0);

const pg = (await import('/home/agentuser/entry-lite-world/node_modules/pg/lib/index.js')).default;
const pool = new pg.Pool({ connectionString: CS, max: 2 });
let finalM = {};
let deadline = Date.now() + 420000;
while (Date.now() < deadline) {
  const r = await pool.query('SELECT status, count(*)::int n FROM entrylite.workflow_runs WHERE run_id = ANY($1) GROUP BY status', [runIds]);
  finalM = Object.fromEntries(r.rows.map(x => [x.status, x.n]));
  if (((finalM.completed ?? 0) + (finalM.failed ?? 0)) >= runIds.length) break;
  await new Promise(res => setTimeout(res, 2000));
}
const wall = Date.now() - t0;
console.log(JSON.stringify({ processes: N_PROC, instances: N_PROC * INST_PER, runs: runIds.length, fireErrs, fireMs, wallMs: wall, statuses: finalM, runsPerMin: Math.round(runIds.length / (wall / 60000)) }));
process.exit(0);
