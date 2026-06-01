# Scenario 6: Assignment-Aligned Production-Like Workload

## Description

Profile **stress** executes real Event ingestion (`POST /events`) for one Whale Customer and 50 Small Customers.
Each Customer has 3 Webhooks (`/hooks/fast`, `/hooks/slow`, `/hooks/flaky`) with mixed latency/failure behavior to approximate production HTTP variability.

Assignment mapping:
- Reliability: retries under transient 503s, duplicate tuple protection.
- Scalability: throughput across worker counts [8 16 32].
- Fairness: Small-vs-Whale P95 first-attempt latency comparison under concurrent load.

Fairness metric note:
- Uses first-attempt queue latency (`delivery_attempts.sent_at(attempt=1) - deliveries.created_at`) at P95.
- Excludes retry/backoff tail so fairness reflects scheduler/queue behavior rather than partner instability.

## Results

| Workers | Total Deliveries | Duration | Throughput (Deliveries/sec) | Retry Rate | Dead Rate | Duplicate (delivery_id, attempt_number) tuples | Small P95 First-Attempt Latency (ms) | Whale P95 First-Attempt Latency (ms) | Reliability | Fairness |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|
| 8 | 21000 | 25.641s | 819.00 | 8.51% | 0.02% | 0 | 6773.24 | 6461.41 | **PASS** | **PASS** |
| 16 | 21000 | 31.453s | 667.67 | 8.11% | 0.04% | 0 | 3488.79 | 1927.55 | **PASS** | **FAIL** |
| 32 | 21000 | 54.557s | 384.92 | 7.89% | 0.03% | 0 | 1534.71 | 660.56 | **PASS** | **FAIL** |
