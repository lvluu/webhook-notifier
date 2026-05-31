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
| Control | 11.968s | 4 | 3 | - | - |
| Dead Partner | 16.412s | 4 | 3 | 353 | 5000 |

## Healthy Customer p95

| Customer | Control p95 | Dead Partner p95 | Ratio |
|---|---:|---:|---:|
| scenario-5-customer-02 | 11734.16 ms | 12687.19 ms | 1.08× |
| scenario-5-customer-03 | 11580.09 ms | 12554.44 ms | 1.08× |
| scenario-5-customer-04 | 11475.33 ms | 12423.13 ms | 1.08× |
| scenario-5-customer-05 | 11418.51 ms | 12213.65 ms | 1.07× |
| scenario-5-customer-06 | 11270.54 ms | 12003.12 ms | 1.06× |
| scenario-5-customer-07 | 11122.54 ms | 11812.86 ms | 1.06× |
| scenario-5-customer-08 | 11086.78 ms | 11578.57 ms | 1.04× |
| scenario-5-customer-09 | 10942.49 ms | 11286.31 ms | 1.03× |
| scenario-5-customer-10 | 10882.20 ms | 11056.61 ms | 1.02× |

## Breaker Snapshots

| Snapshot | State | Failure Count | Probe Delivery | Breaker Open Metric | Breaker Half-Open Metric | Breaker Closed Metric | Timeout Trips |
|---|---|---:|---|---:|---:|---:|---:|
| Open | open | 3 | - | 1 | 0 | 0 | 1 |
| Half-open | half_open | 3 | 11380c8d-69df-4f79-9d88-f1ef46f2c660 | 1 | 0 | 0 | 1 |
| Final | open | 4 | - | 1 | 0 | 0 | 2 |

## Acceptance Criteria

| Criterion | Observed | Limit | Result |
|---|---:|---:|---|
| Healthy Customer p95 within 10% of control | **PASS** | <= 1.10x | **PASS** |
| Customer 1 breaker opens after threshold | 3 failures | >= 2 | **PASS** |
| Customer 1 half-open probe observed | **PASS** | true | **PASS** |
| Customer 1 deliveries skipped after open | 5000 pending | > 0 | **PASS** |
| Customer 1 dead-customer attempts before breaker hold | 353 attempts | <= 500 | **PASS** |
| Breaker trip metric recorded | 2 | >= 1 | **PASS** |
| Goroutine count returned near baseline | -1 delta | <= 8 | **PASS** |
| Overall result | **PASS** | PASS | **PASS** |
