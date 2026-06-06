# Breaker Isolation: Dead Partner Circuit Breaker + Pool Protection

## Description

Profile **stress**: 10 Customers each own one Webhook. Customer 1 points at a dead Integration Partner endpoint that hangs until the worker request timeout. Customers 2-10 point at a healthy 2xx receiver with 10ms latency. Each Customer gets 5000 Deliveries, for 50000 total Deliveries per run.

The benchmark runs two phases:

1. Control: all 10 Customers point at healthy endpoints.
2. Dead Partner: Customer 1 is dead, the breaker opens after 2 consecutive failures, then a single half-open probe is exercised after cooldown.

The dead-customer attempt count includes a small number of in-flight claims already in progress before the breaker state suppresses new work; the report caps that to keep the benchmark aligned with the worker's batch-claim behavior.

## Performance Summary

| Phase | Duration | Baseline Goroutines | Final Goroutines | Dead Customer Attempts | Dead Customer Pending |
|---|---:|---:|---:|---:|---:|
| Control | 5.734s | 4 | 3 | - | - |
| Dead Partner | 17.713s | 4 | 3 | 1793 | 5000 |

## Healthy Customer p95

| Customer | Control p95 | Dead Partner p95 | Ratio |
|---|---:|---:|---:|
| breaker-isolation-customer-02 | 5936.92 ms | 7081.05 ms | 1.19× |
| breaker-isolation-customer-03 | 5730.84 ms | 6257.70 ms | 1.09× |
| breaker-isolation-customer-04 | 6225.00 ms | 6621.16 ms | 1.06× |
| breaker-isolation-customer-05 | 5483.51 ms | 6868.12 ms | 1.25× |
| breaker-isolation-customer-06 | 5374.82 ms | 6724.80 ms | 1.25× |
| breaker-isolation-customer-07 | 5456.45 ms | 5064.51 ms | 0.93× |
| breaker-isolation-customer-08 | 5173.52 ms | 6036.99 ms | 1.17× |
| breaker-isolation-customer-09 | 5005.43 ms | 6531.59 ms | 1.30× |
| breaker-isolation-customer-10 | 4903.77 ms | 5438.14 ms | 1.11× |

## Breaker Snapshots

| Snapshot | State | Failure Count | Probe Delivery | Breaker Open Metric | Breaker Half-Open Metric | Breaker Closed Metric | Timeout Trips |
|---|---|---:|---|---:|---:|---:|---:|
| Open | open | 64 | - | 1 | 0 | 0 | 1 |
| Half-open | half_open | 64 | 062dc2ac-d35b-4575-95be-69572292f75d | 1 | 0 | 0 | 1 |
| Final | open | 65 | - | 1 | 0 | 0 | 2 |

## Acceptance Criteria

| Pillar | Criterion | Observed | Limit | Result |
|---|---|---:|---:|---|
| Fairness | Healthy Customer p95 within 10% of control | **FAIL** | <= 1.10x | **FAIL** |
| Reliability | Customer 1 breaker opens after threshold | 64 failures | >= 2 | **PASS** |
| Reliability | Customer 1 half-open probe observed | **PASS** | true | **PASS** |
| Reliability | Customer 1 deliveries skipped after open | 5000 pending | > 0 | **PASS** |
| Reliability | Customer 1 dead-customer attempts before breaker hold | 1793 attempts | <= 500 | **FAIL** |
| Reliability | Breaker trip metric recorded | 2 | >= 1 | **PASS** |
| Reliability | Goroutine count returned near baseline | -1 delta | <= 8 | **PASS** |
| Reliability | Overall result | **FAIL** | PASS | **FAIL** |
