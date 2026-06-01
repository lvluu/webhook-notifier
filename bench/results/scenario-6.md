# Scenario 6: Assignment-Aligned Production-Like Workload

## Description

Profile **stress** executes real Event ingestion (`POST /events`) for one Whale Customer and 50 Small Customers.
Each Customer has 3 Webhooks (`/hooks/fast`, `/hooks/slow`, `/hooks/flaky`) with mixed latency/failure behavior to approximate production HTTP variability.

Assignment mapping:
- Reliability: retries under transient 503s, duplicate tuple protection.
- Scalability: throughput across worker counts [8 16].
- Fairness: Small-vs-Whale P95 first-attempt latency comparison under concurrent load.

Fairness metric note:
- Uses first-attempt queue latency (`delivery_attempts.sent_at(attempt=1) - deliveries.created_at`) at P95.
- Excludes retry/backoff tail so fairness reflects scheduler/queue behavior rather than partner instability.
- Throughput in this scenario is end-to-end: total deliveries divided by wall time from ingest submission start to full drain.

## Results

| Workers | Total Deliveries | Duration | Throughput (Deliveries/sec) | Retry Rate | Dead Rate | Duplicate (delivery_id, attempt_number) tuples | Small P95 First-Attempt Latency (ms) | Whale P95 First-Attempt Latency (ms) | Reliability | Fairness |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|
| 8 | 21000 | 22.723s | 924.19 | 8.47% | 0.03% | 0 | 7383.95 | 8022.76 | **PASS** | **PASS** |
| 16 | 21000 | 21.202s | 990.50 | 8.33% | 0.03% | 0 | 3259.10 | 3393.84 | **PASS** | **PASS** |
