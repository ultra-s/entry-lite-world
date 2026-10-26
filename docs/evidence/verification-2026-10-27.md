# Verification evidence (2026-10-27)

All numbers below were observed in actual test runs on the dev host
(Postgres 18, Node 22, workflow@5.2.3, @entrylite/world 1.0.0).

## Durability
- SIGKILL drills: runs resume from event log after process kill
- Real chat workflow (entry-agents app): 5/5 consecutive runs terminal 'completed'
- close() leak fixed: sweeper stops before pool teardown (verified, no errors post-close)

## Depth
- 60-step run: step_created/started/completed = 60/60/60, run_completed
- 150-step run (wrun_01M5WT0NVM6W500JHBXCNEJRQ9): 150/150/150 + run_completed

## Throughput (full runtime path, synthetic load)
- 2,000 runs @ concurrency 100: 2,000/2,000 completed, 4,343 runs/min
- 5,000 runs @ concurrency 200: 5,000/5,000 completed, 4,759 runs/min (fired in 26.3s)

## Queue namespaces (cross-deployment isolation)
- Regression: two worlds (alpha/beta) on one DB each claimed only their own
  20 rows; legacy rows invisible to namespaced claimers; job-key stamping verified
- Multi-process: 6 workers, 2 deployments x 3 processes, different registries,
  2,400 runs: zero cross-namespace rows, all rows stayed in home queue (DB-audited)
- End-to-end with real HTTP delivery: 640 jobs (320/deployment), one deployment
  failing first attempt per job (retry path): 640/640 drained, LEAKS: []
- Soak: 2,000 jobs, 5 workers, 2 mid-run restarts: 37,518 claim/retry attempts,
  foreignLocked = 0

## Full-parity completion (2026-10-27, commits a9c7de4..febb3d2)
- 30/30 World-interface surface points present (see README)
- createBatch: per-event atomic, born-running pair, per-event outcomes
- runs bulk ops: waitForTerminalStatus / getMany / cancelMany / experimentalSetAttributes
- analytics: full runs/attributes/steps/events/hooks/waits surface
- invoke: hook-resume fast path, verified end-to-end ({status: accepted}), 500-run bench 3,879/min
- snapshots / dynamic code upload / per-run encryption keys: roundtrip-verified
- CI green through febb3d2
