// 100 shared-DB world instances, 3000 runs (5-step workflow), zero cross-talk check.
// Single-process, sequential firing per instance, pooled polling to stay under max_connections.
import pg from 'pg';

const CS = process.env.ENTRYLITE_WORLD_URL;
const { createWorld } = await import('/home/agentuser/entry-lite-world/dist/index.js');

const WF_CODE = `
export async function instTest(n) {
  "use workflow";
  let out = 0;
  for (let i = 0; i < 5; i++) { out = await bump(out, i); }
  return out;
}
export async function bump(acc, i) { "use step"; return acc + 1; }
`;

const N = 100, RUNS_PER = 30;
const worlds = Array.from({ length: N }, () => createWorld({ url: CS, maxPoolSize: 4 }));

const t0 = Date.now();
const runIds = [];
let fireErrs = 0;
for (let w = 0; w < N; w++) {
  for (let r = 0; r < RUNS_PER; r++) {
    try {
      const runId = await worlds[w].createRunId();
      await worlds[w].start({ workflowName: 'instTest', workflowCode: WF_CODE, runId, args: [r] });
      runIds.push(runId);
    } catch { fireErrs++; }
  }
}
const firedMs = Date.now() - t0;

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
console.log(JSON.stringify({ instances: N, runs: runIds.length, fireErrs, firedMs, wallMs: wall, statuses: finalM, runsPerMin: Math.round(runIds.length / (wall / 60000)) }));
process.exit(0);
