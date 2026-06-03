# Scenario 6: Assignment-Aligned Production-Like Workload

## Description

Profile **dev** executes real Event ingestion (`POST /events`) for one Whale Customer and 10 Small Customers.
Each Customer has 3 Webhooks (`/hooks/fast`, `/hooks/slow`, `/hooks/flaky`) with mixed latency/failure behavior to approximate production HTTP variability.

Assignment mapping:
- Reliability: retries under transient 503s, duplicate tuple protection.
- Scalability: throughput across worker counts [4 8].
- Fairness: Small-vs-Whale P95 first-attempt latency comparison under concurrent load.

Fairness metric note:
- Uses first-attempt queue latency (`delivery_attempts.sent_at(attempt=1) - deliveries.created_at`) at P95.
- Excludes retry/backoff tail so fairness reflects scheduler/queue behavior rather than partner instability.
- Throughput in this scenario is end-to-end: total deliveries divided by wall time from ingest submission start to full drain.

## Results

| Workers | Total Deliveries | Duration | Throughput (Deliveries/sec) | Avg Claim Batch | Claim Calls | Empty Claims | Retry Rate | Dead Rate | Duplicate (delivery_id, attempt_number) tuples | Small P95 First-Attempt Latency (ms) | Whale P95 First-Attempt Latency (ms) | Reliability | Fairness |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|
| 4 | 1200 | 3.06s | 392.19 | 15.35 | 79 | 1 | 1.07% | 0.00% | 0 | 555.04 | 2201.34 | **PASS** | **PASS** |
| 8 | 1200 | 2.053s | 584.62 | 14.80 | 82 | 2 | 1.15% | 0.00% | 0 | 534.75 | 1300.67 | **PASS** | **PASS** |
