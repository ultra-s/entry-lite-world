// Long-run test: 60 steps, exercising durable checkpointing depth.
import { createWorld } from "/home/agentuser/entry-lite-world/dist/index.js";
import { createWorkflow, createStep } from "/home/agentuser/entry-agents/apps/web/node_modules/workflow/dist/index.js";
import { run } from "/home/agentuser/entry-agents/apps/web/node_modules/workflow/dist/api.js";

const world = createWorld({ url: process.env.ENTRYLITE_WORLD_URL });
await world.init?.();

let stepCounter = 0;
const steps = [];
for (let i = 0; i < 60; i++) {
  steps.push(createStep({ id: `step-${i}` })(async () => {
    stepCounter = i + 1;
    return { output: `ok-${i}`, step: i + 1 };
  }));
}

const longWf = createWorkflow({
  id: "long-run-test",
  inputSchema: {},
  outputSchema: {},
  steps,
})(async (input) => {
  let prev;
  for (const s of steps) {
    prev = await s.step(prev ?? input);
  }
  return prev;
});

const runId = await world.createRunId();
const started = Date.now();
await world.start({
  workflowName: "long-run-test",
  workflowCode: longWf,
  runId,
  args: [],
});

// Poll for terminal state
let status = "running";
let elapsed = 0;
while (status === "running" && elapsed < 180_000) {
  await new Promise((r) => setTimeout(r, 1000));
  elapsed += 1000;
  try {
    const d = await world.describeRun({ runId });
    status = d?.status ?? d?.run?.status ?? "unknown";
    if (status !== "running") break;
  } catch {}
}

const secs = ((Date.now() - started) / 1000).toFixed(1);
console.log(`steps-executed: ${stepCounter}`);
console.log(`terminal-status: ${status}`);
console.log(`duration-sec: ${secs}`);

await new Promise((r) => setTimeout(r, 2000));
await world.close?.();
process.exit(0);
