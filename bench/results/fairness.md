# Fairness

## Description

Profile **stress** seeds a Delivery backlog with set-based SQL (each Event fans out to one Delivery per matching Webhook, exactly as ingest would) for one Whale Customer and 20000 Small Customers, then measures the delivery pipeline draining it.
Seeding directly avoids a per-Customer ingest `Submit` stampede that would otherwise measure DB connection contention instead of delivery throughput; ingest-path performance is covered separately.
Each Customer has 3 Webhooks (`/hooks/fast`, `/hooks/slow`, `/hooks/flaky`) with mixed latency/failure behavior to approximate production HTTP variability.

Validation coverage:
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
| 16 | 135000 | 46.928s | 2876.73 | 83.55 | 1632 | 223 | 0.99% | 0.00% | 0 | 23396.15 | 46090.46 | **PASS** | **PASS** |
| 32 | 135000 | 57.336s | 2354.53 | 39.29 | 3470 | 1985 | 0.98% | 0.00% | 0 | 27824.18 | 52841.67 | **PASS** | **PASS** |
| 64 | 135000 | 1m5.424s | 2063.45 | 12.77 | 10684 | 9175 | 1.03% | 0.00% | 0 | 38206.34 | 60577.14 | **PASS** | **PASS** |
