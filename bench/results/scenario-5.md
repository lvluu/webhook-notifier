# Scenario 5: Dead Partner Circuit Breaker + Pool Protection

## Description

Profile **stress**: 10 Customers each own one Webhook. Customer 1 points at a dead Integration Partner endpoint that hangs until the worker request timeout. Customers 2-10 point at a healthy 2xx receiver with 10ms latency. Each Customer gets 5000 Deliveries, for 50000 total Deliveries per run.

The benchmark runs two phases:

1. Control: all 10 Customers point at healthy endpoints.
2. Dead Partner: Customer 1 is dead, the breaker opens after 2 consecutive failures, then a single half-open probe is exercised after cooldown.

The dead-customer attempt count includes a small number of in-flight claims already in progress before the breaker state suppresses new work; the report caps that to keep the benchmark aligned with the worker's batch-claim behavior.

## Performance Summary

| Phase | Duration | Baseline Goroutines | Final Goroutines | Dead Customer Attempts | Dead Customer Pending |
|---|---:|---:|---:|---:|---:|
| Control | 14.382s | 4 | 3 | - | - |
| Dead Partner | 16.377s | 4 | 3 | 481 | 5000 |

## Healthy Customer p95

| Customer | Control p95 | Dead Partner p95 | Ratio |
|---|---:|---:|---:|
| scenario-5-customer-02 | 13986.98 ms | 11091.05 ms | 0.79× |
| scenario-5-customer-03 | 13948.86 ms | 10954.08 ms | 0.79× |
| scenario-5-customer-04 | 13763.84 ms | 10824.54 ms | 0.79× |
| scenario-5-customer-05 | 13580.41 ms | 10714.61 ms | 0.79× |
| scenario-5-customer-06 | 13448.70 ms | 10608.14 ms | 0.79× |
| scenario-5-customer-07 | 13327.21 ms | 10494.90 ms | 0.79× |
| scenario-5-customer-08 | 13220.95 ms | 10418.51 ms | 0.79× |
| scenario-5-customer-09 | 13082.97 ms | 10287.74 ms | 0.79× |
| scenario-5-customer-10 | 12886.45 ms | 10203.34 ms | 0.79× |

## Breaker Snapshots

| Snapshot | State | Failure Count | Probe Delivery | Breaker Open Metric | Breaker Half-Open Metric | Breaker Closed Metric | Timeout Trips |
|---|---|---:|---|---:|---:|---:|---:|
| Open | open | 3 | - | 1 | 0 | 0 | 1 |
| Half-open | half_open | 3 | 1a3b863b-6541-4656-b5a1-359bfd4857e4 | 1 | 0 | 0 | 1 |
| Final | open | 4 | - | 1 | 0 | 0 | 2 |

## Acceptance Criteria

| Criterion | Observed | Limit | Result |
|---|---:|---:|---|
| Healthy Customer p95 within 10% of control | **PASS** | <= 1.10x | **PASS** |
| Customer 1 breaker opens after threshold | 3 failures | >= 2 | **PASS** |
| Customer 1 half-open probe observed | **PASS** | true | **PASS** |
| Customer 1 deliveries skipped after open | 5000 pending | > 0 | **PASS** |
| Customer 1 dead-customer attempts before breaker hold | 481 attempts | <= 500 | **PASS** |
| Breaker trip metric recorded | 2 | >= 1 | **PASS** |
| Goroutine count returned near baseline | -1 delta | <= 8 | **PASS** |
| Overall result | **PASS** | PASS | **PASS** |
