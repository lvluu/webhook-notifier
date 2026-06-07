# Production Workload: Assignment-Aligned Production-Like Workload

## Description

Profile **stress** seeds a Delivery backlog with set-based SQL (each Event fans out to one Delivery per matching Webhook, exactly as ingest would) for one Whale Customer and 20000 Small Customers, then measures the delivery pipeline draining it.
Seeding directly avoids a per-Customer ingest `Submit` stampede that would otherwise measure DB connection contention instead of delivery throughput; ingest-path performance is covered separately.
Each Customer has 3 Webhooks (`/hooks/fast`, `/hooks/slow`, `/hooks/flaky`) with mixed latency/failure behavior to approximate production HTTP variability.

Assignment mapping:
- Reliability: retries under transient 503s, duplicate tuple protection.
- Scalability: throughput across worker counts [16 32 64].
- Fairness: Small-vs-Whale P95 first-attempt latency comparison under concurrent load.

Fairness metric note:
- Uses first-attempt queue latency (`delivery_attempts.sent_at(attempt=1) - deliveries.created_at`) at P95.
- Excludes retry/backoff tail so fairness reflects scheduler/queue behavior rather than partner instability.
- Throughput in this scenario is total deliveries divided by wall time from backlog seed start to full drain.

## Results

| Workers | Total Deliveries | Duration | Throughput (Deliveries/sec) | Avg Claim Batch | Claim Calls | Empty Claims | Retry Rate | Dead Rate | Duplicate (delivery_id, attempt_number) tuples | Small P95 First-Attempt Latency (ms) | Whale P95 First-Attempt Latency (ms) | Reliability | Fairness |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|
| 16 | 135000 | 28.868s | 4676.52 | 89.44 | 1525 | 100 | 1.02% | 0.00% | 0 | 18430.26 | 27153.58 | **PASS** | **PASS** |
| 32 | 135000 | 18.972s | 7115.89 | 88.47 | 1541 | 129 | 0.98% | 0.00% | 0 | 9642.37 | 15154.39 | **PASS** | **PASS** |
| 64 | 135000 | 16.65s | 8108.13 | 83.27 | 1637 | 345 | 0.97% | 0.00% | 0 | 6019.15 | 12036.24 | **PASS** | **PASS** |

## DB Hotspot Note

This stress run uses breaker-aware discovery (`breaker_failure_threshold: 1000`)
without intentionally opening breakers. The follow-up claim/mark optimization
moved `ClaimNextBatch` from ~74.7s to ~36.9s total DB time and
`MarkSucceededBatch` from ~48.7s to ~36.3s total DB time in
`pg_stat_statements`. `HasClaimableWorkBatch` dropped from ~6.9s to ~0.9s after
using the same stale-claim guard as the claim path.

The remaining top query costs are still multi-Customer claim SQL and batch mark
SQL. The new visible coordination cost is the stale-claim existence probe
(~15.0s total / ~2.2ms mean in this run); optimizing that further would require
accepting bounded stale-recovery latency or adding a low-frequency cache.
