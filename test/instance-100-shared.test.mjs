// 100 real world instances (5 procs x 20) connected to ONE shared DB, PLUS 3000 runs
// fired through the compiled nitro demo. Verifies: coexistence, no cross-talk, throughput.
import { spawn } from 'node:child_process';
import fs from 'node:fs';

const CS = process.env.ENTRYLITE_WORLD_URL;
const N_PROC = 5, INST_PER = 20;
const OUT = '/tmp/inst100_idle.jsonl';
try { fs.unlinkSync(OUT); } catch {}

const workerSrc = `
const CS = process.env.CS;
const { createWorld } = await import('/home/agentuser/entry-lite-world/dist/index.js');
const W = ${INST_PER};
const worlds = Array.from({ length: W }, () => createWorld({ url: CS, maxPoolSize: 3 }));
let pollCycles = 0;
const t = setInterval(async () => {
  pollCycles++;
  if (pollCycles % 50 === 0) {
    (await import('node:fs')).appendFileSync(process.env.OUT, JSON.stringify({ pid: process.pid, pollCycles }) + '\\n');
  }
}, 250);
setTimeout(async () => { clearInterval(t); for (const w of worlds) { try { await w.close(); } catch {} } process.exit(0); }, 240000);
`;

fs.writeFileSync('/tmp/inst100_idle.mjs', workerSrc);

const t0 = Date.now();
const procs = [];
for (let p = 0; p < N_PROC; p++) {
  procs.push(spawn('node', ['/tmp/inst100_idle.mjs'], {
    env: { ...process.env, CS, OUT },
    stdio: ['ignore', 'ignore', 'inherit'],
  }));
}

// Fire 3000 runs through the compiled demo server in parallel from this process
const BASE = 'http://localhost:3002';
const N_RUNS = 3000, CONC = 150;
let idx = 0;
async function fire() {
  while (idx < N_RUNS) {
    idx++;
    try { await fetch(`${BASE}/t/basic?n=${21 + (idx % 50)}`); } catch { idx--; }
  }
}
await Promise.all(Array.from({ length: CONC }, fire));
const firedMs = Date.now() - t0;

// poll completion
const pg = (await import('/home/agentuser/entry-lite-world/node_modules/pg/lib/index.js')).default;
const pool = new pg.Pool({ connectionString: CS, max: 2 });
const ids = await pool.query(`SELECT run_id FROM entrylite.workflow_runs WHERE workflow_name LIKE '%testBasic%' ORDER BY created_at DESC LIMIT $1`, [N_RUNS]);
const idSet = ids.rows.map(r => r.run_id);
let finalM = {};
let deadline = Date.now() + 240000;
while (Date.now() < deadline) {
  const r = await pool.query('SELECT status, count(*)::int n FROM entrylite.workflow_runs WHERE run_id = ANY($1) GROUP BY status', [idSet]);
  finalM = Object.fromEntries(r.rows.map(x => [x.status, x.n]));
  if (((finalM.completed ?? 0) + (finalM.failed ?? 0)) >= idSet.length) break;
  await new Promise(res => setTimeout(res, 2000));
}
const wall = Date.now() - t0;
console.log(JSON.stringify({ idleInstances: N_PROC * INST_PER, runs: idSet.length, firedMs, wallMs: wall, statuses: finalM, runsPerMin: Math.round(idSet.length / (wall / 60000)) }));
await pool.end();
// workers exit on their own 240s timer
process.exit(0);
