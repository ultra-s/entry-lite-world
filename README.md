# @entrylite/world

A durable, self-hostable **World** backend for the [Vercel Workflow SDK](https://vercel.com/docs/workflow) (`workflow@5.2.3`), built on plain PostgreSQL with `node-postgres` — no drizzle, no graphile, no external queue.

## What it is

The Workflow SDK lets you write durable, resumable workflows in plain TypeScript (`"use workflow"` / `"use step"` directives). It delegates all state persistence and job delivery to a pluggable **World**. This package is that World:

- **Event-sourced runs, steps, waits, hooks** — every state transition is a row; killed processes resume from the event log (proven with SIGKILL drills)
- **Postgres-backed queue**: lease-based claims, retry with backoff, delayed jobs, LISTEN/NOTIFY wake for low latency, lost-wake sweeper backstop, crash recovery
- **Per-deployment queue namespaces** (`ENTRYLITE_QUEUE_NAMESPACE`): multiple independent deployments can share one database without claiming, leasing, executing, or recovering each other's jobs — isolation is enforced in SQL (claim predicates + namespace-scoped indexes), not app-level filtering
- **Contract-conformant** with the `@workflow/world` 5.2.3 interface (hooks + hook resume, streaming, dynamic workflow code, idempotency, retries, fatal errors)

## Install

```bash
npm install @entrylite/world
# or link: pnpm add /path/to/entry-lite-world
```

Requires PostgreSQL 14+ (tested on 18).

## Usage

In your app (Next.js / Nitro / any Node server hosting the workflow runtime):

```ts
// entrylite-world.ts
import { createWorld } from "@entrylite/world";

export default createWorld({
  connectionString: process.env.ENTRYLITE_WORLD_URL,
});
```

Point the SDK at it:

```bash
WORKFLOW_TARGET_WORLD=./entrylite-world.ts
ENTRYLITE_WORLD_URL=postgres://user:pass@host:5432/db
```

The schema (`entrylite` schema) is created automatically on first use.

### Multi-deployment isolation

```bash
# Deployment A
ENTRYLITE_QUEUE_NAMESPACE=alpha
# Deployment B (same database, fully isolated)
ENTRYLITE_QUEUE_NAMESPACE=beta
```

- Validated: `^[a-z0-9_]{1,40}$`; stable across restarts (never auto-generated)
- Unset = legacy shared mode (single-deployment behavior, unchanged)
- All queue operations — create, claim, lease, retry, delayed, recovery, sweeper, LISTEN/NOTIFY, idempotency keys — are namespace-scoped in SQL

## Performance

Measured on the full runtime path (workflow@5.2.3, real HTTP delivery):

- 5,000 runs completed, zero failures, ~4,759 runs/min sustained (200 concurrency)
- 60-step and 150-step durable runs complete end-to-end; every step is event-sourced
- 100 coexisting world instances on one database: zero cross-namespace job claims (with namespaces enabled)

## Tests

```bash
npm run build
node --test test/contract-conformance.test.mjs   # 5.2.3 World contract
node --test test/e2e-world.test.mjs              # end-to-end lifecycle
node test/namespace-isolation.test.mjs           # cross-deployment isolation
node --test test/                                # everything
```

CI runs the core suites against Postgres 18 on every push (`.github/workflows/ci.yml`).

## Status / honesty

Production-grade for the covered surface, with known limits:

- ✅ Verified: durability (SIGKILL resume), depth (150 steps), throughput (5k runs), multi-process scale, namespace isolation, crash recovery, hook resume
- ⚠️ Not implemented: observability dashboard, OTel export, run search/list APIs (tracked in the parity audit)
- ⚠️ Benchmarks are synthetic, not real-traffic proof

## License

MIT
