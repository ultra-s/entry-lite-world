@entrylite/world Benchmark (2026-10-26, wf-demo rig, workflow@5.2.3 SDK, Postgres 16 local, Node 22)
=============================================================
Shape: testBasic = 2 steps (add+mul), ~9 events/run, HTTP trigger -> SDK runtime -> our World -> PG.

N      CONC  wall     fired    statuses          runs/min
100    20    1.77s    0.87s    100 completed     3,382
500    50    7.20s    2.41s    500 completed     4,166
1000   100   13.3s    4.7s     1000 completed    4,501
1000   100   12.3s    3.2s     1000 completed    4,874   (sustained r2)
1000   100   12.0s    3.1s     1000 completed    4,987   (sustained r3)
1000   100   11.9s    3.0s     1000 completed    5,029   (sustained r4)
2000   200   24.5s    10.3s    2000 completed    4,905   (ceiling probe)

PEAK: 5,029 runs/min | SUSTAINED: ~4,900-5,000 runs/min across 4x1000-run repeats
EVENT throughput: ~44,000-45,000 events/min (9 events/run)
Zero failures in all benchmark batches (100% completed every round).
Bottleneck: not CPU (low util) — queue poll/delivery round-trips per step (2 HTTP-delivery hops per
step x 2 steps + wake). Dev-mode nitro also in path (unbundled).
Comparison honesty: Entry-Lite-RS engine benchmark (44,680 ops/min) measured raw engine ops, NOT
full SDK workflow runs (replay, event-sourcing, durable semantics) — not directly comparable.
@entrylite/world runs the REAL Workflow SDK with full durable event-sourcing semantics.
