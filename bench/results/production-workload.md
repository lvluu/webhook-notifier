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
| 16 | 135000 | 32.848s | 4109.87 | 88.69 | 1538 | 109 | 1.03% | 0.00% | 0 | 18521.04 | 29947.65 | **PASS** | **PASS** |
| 32 | 135000 | 17.306s | 7800.85 | 88.20 | 1546 | 131 | 1.00% | 0.00% | 0 | 9495.59 | 14804.06 | **PASS** | **PASS** |
| 64 | 135000 | 13.738s | 9826.43 | 83.74 | 1629 | 232 | 1.04% | 0.00% | 0 | 5592.34 | 11136.48 | **PASS** | **PASS** |

## DB Hotspot Note

This stress run uses breaker-aware discovery (`breaker_failure_threshold: 1000`)
without intentionally opening breakers. `pg_stat_statements` moved
claimable-Customer discovery from the former rank-1 hotspot (~134-153s total DB
time in prior stress runs) to rank 3 at ~9.2s total / ~50.5ms mean. The top
remaining query costs are now multi-Customer claim SQL and batch mark SQL.
