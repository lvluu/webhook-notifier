# Scenario 6: Assignment-Aligned Production-Like Workload

## Description

Profile **stress** executes real Event ingestion (`POST /events`) for one Whale Customer and 10000 Small Customers.
Each Customer has 3 Webhooks (`/hooks/fast`, `/hooks/slow`, `/hooks/flaky`) with mixed latency/failure behavior to approximate production HTTP variability.

Assignment mapping:
- Reliability: retries under transient 503s, duplicate tuple protection.
- Scalability: throughput across worker counts [16 32].
- Fairness: Small-vs-Whale P95 first-attempt latency comparison under concurrent load.

Fairness metric note:
- Uses first-attempt queue latency (`delivery_attempts.sent_at(attempt=1) - deliveries.created_at`) at P95.
- Excludes retry/backoff tail so fairness reflects scheduler/queue behavior rather than partner instability.
- Throughput in this scenario is end-to-end: total deliveries divided by wall time from ingest submission start to full drain.

## Results

| Workers | Total Deliveries | Duration | Throughput (Deliveries/sec) | Avg Claim Batch | Claim Calls | Empty Claims | Retry Rate | Dead Rate | Duplicate (delivery_id, attempt_number) tuples | Small P95 First-Attempt Latency (ms) | Whale P95 First-Attempt Latency (ms) | Reliability | Fairness |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|
| 16 | 36000 | 17.544s | 2051.94 | 4.73 | 7689 | 7162 | 0.96% | 0.00% | 0 | 5415.08 | 11869.48 | **PASS** | **PASS** |
| 32 | 36000 | 10.025s | 3591.02 | 4.13 | 8807 | 8257 | 1.04% | 0.00% | 0 | 2768.33 | 4259.08 | **PASS** | **PASS** |
