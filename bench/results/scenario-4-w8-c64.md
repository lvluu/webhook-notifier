# Scenario 4: Horizontal Scale + Claim Atomicity + Idempotency

## Description

Profile **stress**: pure-success throughput runs use the same 10000 Delivery workload at worker counts [1 4 8 32 64] against a healthy receiver with 10ms per-Attempt latency. A separate retry probe forces 100 Deliveries to fail once with 503 before succeeding, so idempotency headers are validated across multiple Attempts without polluting the throughput measurement with retry backoff.

This repo uses just bench-4 as the canonical task command for the issue #15 benchmark.

## Performance Summary

| Profile | stress |
| Workers | Deliveries | Received Attempts | Duration | Throughput (Deliveries/sec) | Duplicate (delivery_id, attempt_number) tuples |
|---:|---:|---:|---:|---:|---:|
| 1 | 10000 | 10000 | 12.024s | 831.67 | 0 |
| 4 | 10000 | 10000 | 3.859s | 2591.20 | 0 |
| 8 | 10000 | 10000 | 2.412s | 4145.37 | 0 |
| 32 | 10000 | 10000 | 4.301s | 2325.22 | 0 |
| 64 | 10000 | 10000 | 5.795s | 1725.73 | 0 |

## Retry Probe

| Metric | Value |
|---|---:|
| Deliveries | 100 |
| Received Attempts | 200 |
| Deliveries with multiple Attempts | 100 |
| Duplicate (delivery_id, attempt_number) tuples | 0 |
| X-Delivery-Id stable per Delivery | **PASS** |
| X-Delivery-Id present in tuple | **PASS** |
| X-Event-Id stable per Delivery | **PASS** |
| X-Event-Id present | **PASS** |
| X-Attempt-Number unique per Delivery | **PASS** |
| X-Attempt-Number consecutive from 1 | **PASS** |

## Acceptance Criteria

| Criterion | Observed | Limit | Result |
|---|---:|---:|---|
| Scaling: 64 workers vs 1 workers | 2.08x | >= 4.00x | **FAIL** |
| Throughput duplicate tuples | 0 | 0 | **PASS** |
| Retry probe duplicate tuples | 0 | 0 | **PASS** |
| Retry probe idempotency header stability | true | true | **PASS** |
