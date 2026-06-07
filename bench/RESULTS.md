# Benchmark Results Index

Each scenario report lives in `bench/results/`. This file indexes them with
one-line summaries and pass/fail status.

## Assignment-Facing Proofs

| # | Report | Summary | Verdict |
|---|---|---|---|
| 6 | [scenario-6.md](results/scenario-6.md) | Mixed 1 Whale + 10 Small Customers, 3 webhooks each, mixed partner latency/failure. Proves reliability (retries, zero duplicates), scalability (throughput scaling from 4→8 workers), and fairness (Small P95 first-attempt latency SLO). | **PASS** |
| 8 | [scenario-8.md](results/scenario-8.md) | Two OS notifier replicas sharing one Postgres outbox. Sparse phase (1000 Small Customers × 1 event each) + dense phase (10 Customers × 100 seeded deliveries). Proves multi-replica claim atomicity, zero duplicates, and sparse discovery. | **PASS** |
| PR | [partner-resilience.md](results/partner-resilience.md) | Consolidated drill-down: Phase A (transient 503 recovery), Phase B (terminal 4xx fast-fail + DLQ), Phase C (dead-partner breaker isolation). Proves retry policy, decorrelated jitter, breaker lifecycle (closed→open→half_open→closed), and healthy-Customer isolation under dead partner. | **PASS** |

## Internal Hardening

| # | Report | Summary | Verdict |
|---|---|---|---|
| 7 | [scenario-7.md](results/scenario-7.md) | 10 Customers, all webhooks 5xx, 200 deliveries each. Proves post-commit in-memory breaker recording, all breakers open, stable goroutine count. | **PASS** |

## Legacy / Internal Reference

These scenarios were the original issue-scoped benchmarks. Their proofs have been
consolidated into the assignment-facing reports above.

| # | Report | Summary | Superseded by |
|---|---|---|---|
| 1 | [scenario-1.md](results/scenario-1.md) | 1 Whale (50k deliveries burst) + 100 Smalls (5 each, dripped) + 1 late-arriving Small at t=30s. Proves fairness: Smalls under 2s p99 lifecycle, late arrival picked within one cycle. | Scenario 6 |
| 2 | [scenario-2.md](results/scenario-2.md) | 10 Customers, 100 events each, 3 webhooks each. Partner A healthy, B 30% 5xx, C 503 during outage window then recovers. Proves retry + recovery + per-host isolation. | Partner Resilience A |
| 3 | [scenario-3.md](results/scenario-3.md) | 1 Customer, 2 webhooks (healthy + 404), 1000 events. Proves 4xx fast-fail, no retry waste, dead-letter classification. | Partner Resilience B |
| 4 | [scenario-4.md](results/scenario-4.md) | 10000 deliveries at worker counts [1 4 8 32 64], healthy receiver. Proves throughput scaling (809→5385 deliveries/s) and zero duplicate tuples. Separate retry probe validates idempotency across attempts. | Scenario 8 dense phase |
| 5 | [scenario-5.md](results/scenario-5.md) | 10 Customers, 1 dead endpoint + 9 healthy. Proves breaker isolation: healthy customers unaffected (within 10% of control throughput). | Partner Resilience C |
| M4 | [scenario-4-matrix.md](results/scenario-4-matrix.md) | Throughput matrix: workers [8 16 32 48 64] × DB connections [16 32 48 64 96]. Identifies throughput knee at 32 workers. | — |
