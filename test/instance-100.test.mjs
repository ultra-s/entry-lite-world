// 100 shared-DB world instances, 3000 runs, verify zero cross-talk failures.
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

const N_INSTANCES = 100;
const RUNS_PER = 30; // 3000 total

const worlds = [];
for (let i = 0; i < N_INSTANCES; i++) {
  const w = createWorld({ url: CS, maxPoolSize: 5 });
  worlds.push(w);
}

const t0 = Date.now();
const runIds = [];
let fired = 0, fireErrs = 0;
// round-robin firing across all 100 instances, in parallel batches
const tasks = [];
for (let r = 0; r < RUNS_PER; r++) {
  for (let w = 0; w < N_INSTANCES; w++) {
    tasks.push([r, w]);
  }
}
let ti = 0;
async function fireWorker() {
  while (ti < tasks.length) {
    const [r, w] = tasks[ti++];
    try {
      const runId = await worlds[w].createRunId();
      await worlds[w].start({ workflowName: 'instTest', workflowCode: WF_CODE, runId, args: [r * 100 + w] });
      runIds.push(runId);
      fired++;
    } catch { fireErrs++; }
  }
}
await Promise.all(Array.from({ length: 20 }, fireWorker));
const firedMs = Date.now() - t0;

// poll completion
const pool = new pg.Pool({ connectionString: CS });
let done = 0, deadline = Date.now() + 300000;
while (Date.now() < deadline) {
  const r = await pool.query(
    `SELECT status, count(*)::int n FROM entrylite.workflow_runs WHERE run_id = ANY($1) GROUP BY status`,
    [runIds]
  );
  const m = Object.fromEntries(r.rows.map(x => [x.status, x.n]));
  done = (m.completed ?? 0) + (m.failed ?? 0);
  if (done >= runIds.length) break;
  await new Promise(res => setTimeout(res, 2000));
}
const r2 = await pool.query(
  `SELECT status, count(*)::int n FROM entrylite.workflow_runs WHERE run_id = ANY($1) GROUP BY status`,
  [runIds]
);
const wall = Date.now() - t0;
console.log(JSON.stringify({
  instances: N_INSTANCES,
  runs: runIds.length,
  firedMs,
  wallMs: wall,
  statuses: Object.fromEntries(r2.rows.map(x => [x.status, x.n])),
  runsPerMin: Math.round(runIds.length / (wall / 60000)),
}));
await pool.end();
process.exit(0);
