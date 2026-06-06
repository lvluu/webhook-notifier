# Breaker Storm: Mass Breaker Tripping Under Batch Mark

## Description

Profile **stress**: 50 Customers each own one Webhook. All webhooks point at a
500-ing Integration Partner endpoint, simulating a downstream outage.
Workers claim batches of up to **32** deliveries per customer and mark
them concurrently. Each batch of failures exercises the O(1) batch breaker
bookkeeping path (`batchRecordBreakerFailure`), which replaces the
N+1 per-item query pattern with set-based SQL.

The breaker opens after **2** consecutive 5xx responses.

| Metric | Value |
|---|---|
| Customers | 50 |
| Deliveries per Customer | 1000 |
| Total Deliveries | 50000 |
| Workers | 16 |
| Min Batch Hint | 32 |
| Total Attempts | 3200 |
| Duration | 2.42s |
| Baseline Goroutines | 3 |
| Final Goroutines | 3 |
| Goroutine Delta | 0 |

## Delivery Outcomes

| Status | Count |
|---|---|
| Pending / Retry (skipped by breaker) | 50000 |
| Succeeded | 0 |
| Dead | 0 |

## Breaker States

| State | Count |
|---|---|
| Open | 50 / 50 |
| Half-Open | 0 / 50 |
| Closed | 0 / 50 |
| Min failure_count across open breakers | 64 |

## Acceptance Criteria

| Criterion | Observed | Limit | Result |
|---|---:|---:|---|
| All breakers opened | 50 / 50 | 50 | **PASS** |
| Breaker failure_count >= threshold | 64 | >= 2 | **PASS** |
| Goroutine count stable | 0 delta | <= 8 | **PASS** |
| Overall result | **PASS** | PASS | **PASS** |
