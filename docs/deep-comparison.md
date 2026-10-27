# Deep Comparison: @entrylite/world vs official Vercel Workflow SDK worlds
(Verified against vercel/workflow @ 6455bebd, workflow 5.2.3 / world 5.0.5; inventories: /tmp/vercel-inventory.md, /tmp/entrylite-inventory.md)

## Verdict summary
Interface parity: 30/30 World surface points present, specVersion 8, 6/8 capability flags implemented; the 2 absent flags (inBandFence, deploymentAffinity) are optional and fail-closed — core 5.2.3 does not reference inBandFence at all and deploymentAffinity is an opt-in optimization guard. Official CLI (workflow v5.0.6) interoperates against entrylite: health ✓, inspect run ✓, inspect runs ✓, cancel ✓ — proven live.

## Side-by-side
| Area | official world-postgres 5.0.5 | @entrylite/world |
|---|---|---|
| World interface | full (reference) | full (30/30 surface points) |
| specVersion | 8 | 8 (live rows confirm spec_version=8 in runs+events) |
| Queue engine | graphile-worker (mature, battle-tested) | own Postgres queue: FOR UPDATE SKIP LOCKED + LISTEN/NOTIFY + lost-wake sweeper |
| Capabilities | 8 flags | 6 true; inBandFence/deploymentAffinity fail-closed (safe) |
| Multi-deployment isolation | WORKFLOW_POSTGRES_JOB_PREFIX / namespace config | ENTRYLITE_QUEUE_NAMESPACE — stronger: enforced at SQL claim level + 409-reject + NOTIFY filter (verified 0 leaks in 640-job e2e + 2,000-job soak) |
| Delivery retry ceiling | 73 attempts (49+24, graphile) | 48 attempts then parked 1h (no dead-letter table) |
| Crash recovery | graphile job-level | run-level: start() re-enqueue + 90s lost-wake sweeper (SIGKILL drill green) |
| invoke | gated behind WORKFLOW_POSTGRES_INVOKE=1 | implemented, verified end-to-end (accepted outcome) |
| Encryption | AES-256-GCM per-run keys | getEncryptionKeyForRun opt-in via env secret |
| Snapshots / dynamic code | yes | yes (roundtrip verified) |
| Observability UI | web/web-shared dashboard + OTel spans | console logs + analytics API; no OTel export, no dashboard |
| CLI interop | native | VERIFIED: health, inspect run/runs, cancel all work |
| Maturity | production, thousands of users | single-maintainer, this-week-tested |

## Known entrylite gaps (honest)
1. No OTel trace export and no dashboard UI (CLI + SQL are the observability path).
2. No per-job lease steal: a job locked by a crashed worker is recovered at run level, not per-job.
3. No dead-letter table (parked jobs only); analytics edge cases hardcoded; getRuntimeDeadline/regionForRunId stubs.
4. events.ts has an unused SPEC_VERSION=4 default constant (real writes use per-run spec_version=8 — verified in live rows); cosmetic cleanup recommended.
5. Benchmarks synthetic; ~3.9-4.8k runs/min sustained vs world-postgres unmeasured on same host.

## If I were a dev, which would I choose?
- On Vercel or want zero-ops + dashboard + vendor support: **official workflow + world-postgres (or world-vercel)**. It is the safer default; graphile-worker and the SDK's CI/event-log-race harness are battle-tested.
- Self-hosting on your own Postgres where the official world's graphile table + 88 env knobs feel heavy, or needing hard per-deployment isolation on ONE database (the cross-talk problem that bit us): **@entrylite/world** is a legitimate choice — full interface parity, CLI-compatible, independently verified. Caveats: accept console-log observability, run-level crash recovery, and be the operator of your own queue code.
- Pragmatic recommendation: default to official world-postgres; keep @entrylite/world for multi-tenant shared-DB isolation cases, and treat it as what it is — a young but conformant world with excellent tests and no dashboard.
