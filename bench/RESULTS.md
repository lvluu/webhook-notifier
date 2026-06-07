# Benchmark Results Index

Each benchmark report lives in `bench/results/`. This file indexes the
assignment-facing reports plus the legacy issue-scoped reports retained for
reference.

## Assignment-Facing Proofs

| Scenario | Report | Summary | Verdict |
|---|---|---|---|
| `production_workload` | [production-workload.md](results/production-workload.md) | Mixed Whale/Small backlog with 20k Small Customers, 3 Webhooks each, and mixed partner latency/failure. Proves reliability, worker-count scaling, zero duplicates, and Small-vs-Whale first-attempt fairness. | **PASS** |
| `horizontal_scale` | [horizontal-scale.md](results/horizontal-scale.md) | Real OS notifier replicas sharing one Postgres outbox. Sparse phase validates ingest/discovery/distribution; dense phase validates multi-replica claim atomicity and throughput scaling. | **PASS** |
| `partner_resilience` | [partner-resilience.md](results/partner-resilience.md) | Consolidated drill-down: Phase A transient 503 recovery, Phase B terminal 4xx fast-fail + DLQ, Phase C dead-partner breaker isolation. | **PASS** |

## Current Bottleneck Findings

- `production_workload` stress now drains 135k Deliveries per worker-count
  run through the breaker-aware discovery path. Issue #131 reduced
  claimable-Customer discovery from the dominant DB hotspot (~134-153s total
  DB time in stress runs) to a secondary cost. The follow-up outbox hot-path
  change reduced `ClaimNextBatch` from ~74.7s to ~36.9s total DB time,
  `MarkSucceededBatch` from ~48.7s to ~36.3s, and `HasClaimableWorkBatch` from
  ~6.9s to ~0.9s. The remaining ceiling is still claim/mark SQL, with the
  stale-claim existence probe now visible as the next coordination cost.
- `horizontal_scale` stress now runs 60k sparse Deliveries plus 100k dense
  Deliveries per replica-count phase. After removing benchmark-only host-cap
  requeue churn, dense throughput scales strongly from 1 to 2 replicas and then
  knees at 4 replicas, where claim and mark SQL become the remaining ceiling.
- The sharding experiment was not retained: the measured gains came from
  scheduler wake/claim behavior and benchmark/runtime configuration fixes, not
  from sharded Scheduler state.

## Internal Hardening

| Scenario | Report | Summary | Verdict |
|---|---|---|---|
| `breaker_storm` | [breaker-storm.md](results/breaker-storm.md) | Mass breaker tripping under batch mark. Proves O(1) batch breaker bookkeeping, all breakers open, and stable goroutine count. | **PASS** |

## Legacy / Internal Reference

These scenarios were the original issue-scoped benchmarks. Their proofs have been
consolidated into the assignment-facing reports above.

| Scenario | Report | Summary | Superseded by |
|---|---|---|---|
| `late_arrival_fairness` | [late-arrival-fairness.md](results/late-arrival-fairness.md) | Whale burst plus dripped and late-arriving Small Customers. Proves scheduler reactivity and Small Customer lifecycle latency under Whale backlog. | `production_workload` |
| `transient_recovery` | [transient-recovery.md](results/transient-recovery.md) | Healthy, flaky, and outage-then-recovering partners. Proves retry + recovery + per-host isolation. | `partner_resilience` Phase A |
| `terminal_fast_fail` | [terminal-fast-fail.md](results/terminal-fast-fail.md) | Healthy plus 404 Webhook. Proves 4xx fast-fail, no retry waste, and dead-letter classification. | `partner_resilience` Phase B |
| `scale_idempotency` | [scale-idempotency.md](results/scale-idempotency.md) | Dense throughput and retry probe. Proves claim atomicity, throughput scaling, idempotency, and zero duplicate tuples. | `horizontal_scale` dense phase |
| `breaker_isolation` | [breaker-isolation.md](results/breaker-isolation.md) | Dead-partner breaker isolation with healthy Customers. Proves breaker protection and healthy-Customer isolation. | `partner_resilience` Phase C |
| Matrix | [scale-idempotency-matrix.md](results/scale-idempotency-matrix.md) | Historical throughput matrix: workers [8 16 32 48 64] × DB connections [16 32 48 64 96]. Identifies throughput knee at 32 workers. | — |
