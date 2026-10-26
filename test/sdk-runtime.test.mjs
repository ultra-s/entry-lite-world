// Ultimate e2e: real Vercel Workflow SDK runtime ("use workflow" code) on the Entry-Lite world.
// The SDK's dev runtime mounts the world and drives replay. We use the same mechanism as
// world-local: the runtime from @workflow/core invoked with our world.
import { createWorld } from '../dist/index.js';
import { start, getRun, sleep as _s } from 'workflow/api';

// Workflow definitions compiled with the workflow transform (esbuild swc plugin equivalent):
// For a direct-node test we use the workflow CLI's build path. Simplest: import from the
// wf-demo workflows already built by nitro's workflow module? Nitro dev server handles that.
// Instead: start wf-demo (nitro+workflow) pointed at OUR world via WORKFLOW_TARGET_WORLD.
console.log('This test drives wf-demo (nitro + workflow SDK) configured with our world.');
console.log('Run: WORKFLOW_TARGET_WORLD=/home/agentuser/entry-lite-world WORKFLOW_ENTRYLITE=1 npm run dev (in ~/wf-demo)');
