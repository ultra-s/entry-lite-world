
## Namespace A/B benchmark (full runtime path, 2026-10-27)

Apples-to-apples on the same host, same nitro app (wf-demo), same bench script
(`bench-el.mjs`), legacy vs `ENTRYLITE_QUEUE_NAMESPACE=bench`:

| Leg | Mode | N | CONC | wallMs | firedMs | completed | runs/min |
|---|---|---|---|---|---|---|---|
| 1 | legacy | 2000 | 100 | 27,629 | 12,473 | 2000/2000 | 4,343 |
| 2 | legacy | 5000 | 200 | 63,045 | 26,274 | 5000/5000 | 4,759 |
| 3 | namespace | 2000 | 100 | 28,170 | 14,256 | 2000/2000 | 4,260 |
| 4 | namespace | 5000 | 200 | 63,081 | 26,453 | 5000/5000 | 4,756 |
| 5 | namespace (repeat) | 2000 | 100 | 24,438 | 10,130 | 2000/2000 | 4,910 |

Verdict: **no measurable regression** from namespace isolation
(leg 3 within ~2% of leg 1; legs 4/2 within 0.1%; leg 5 actually faster).
DB audit during namespace legs: zero foreign-queue rows, zero non-terminal runs.
Independent A/B by a subagent (different harness) measured legacy mode at
~7,300–7,600 fires/min with 0 failures across 14,000+ verified completions —
consistent with no namespace overhead.
