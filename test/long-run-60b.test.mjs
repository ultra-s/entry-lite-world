// Long-run test: 60 durable steps in one workflow.
import { createWorld } from "/home/agentuser/entry-lite-world/dist/index.js";

const world = createWorld({ url: process.env.ENTRYLITE_WORLD_URL });

const workflowCode = `
export async function longRunTest() {
  "use workflow";
  let out = 0;
  for (let i = 0; i < 60; i++) {
    out = await incrementStep(out, i);
  }
  return out;
}
export async function incrementStep(acc, i) {
  "use step";
  return acc + 1;
}
`;

const runId = await world.createRunId();
const started = Date.now();

await world.start({
  workflowName: "longRunTest",
  workflowCode,
  runId,
  args: [],
});

let status = "running";
let elapsed = 0;
while (status === "running" && elapsed < 240_000) {
  await new Promise((r) => setTimeout(r, 1000));
  elapsed += 1000;
  try {
    const d = await world.describeRun({ runId });
    status = d?.status ?? d?.run?.status ?? "unknown";
    if (status !== "running") break;
  } catch {}
}

const secs = ((Date.now() - started) / 1000).toFixed(1);
console.log(`terminal-status: ${status}`);
console.log(`duration-sec: ${secs}`);

await new Promise((r) => setTimeout(r, 2000));
await world.close?.();
process.exit(0);
