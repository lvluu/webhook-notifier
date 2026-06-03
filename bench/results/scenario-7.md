# Scenario 7: Mass Breaker Tripping Under Batch Mark

## Description

Profile **dev**: 10 Customers each own one Webhook. All webhooks point at a
500-ing Integration Partner endpoint, simulating a downstream outage.
Workers claim batches of up to **32** deliveries per customer and mark
them concurrently. Each batch of failures exercises the O(1) batch breaker
bookkeeping path (`batchRecordBreakerFailure`), which replaces the
N+1 per-item query pattern with set-based SQL.

The breaker opens after **2** consecutive 5xx responses.

| Metric | Value |
|---|---|
| Customers | 10 |
| Deliveries per Customer | 200 |
| Total Deliveries | 2000 |
| Workers | 4 |
| Min Batch Hint | 32 |
| Total Attempts | 640 |
| Duration | 842ms |
| Baseline Goroutines | 3 |
| Final Goroutines | 3 |
| Goroutine Delta | 0 |

## Delivery Outcomes

| Status | Count |
|---|---|
| Pending / Retry (skipped by breaker) | 2000 |
| Succeeded | 0 |
| Dead | 0 |

## Breaker States

| State | Count |
|---|---|
| Open | 10 / 10 |
| Half-Open | 0 / 10 |
| Closed | 0 / 10 |
| Min failure_count across open breakers | 3 |

## Acceptance Criteria

| Criterion | Observed | Limit | Result |
|---|---:|---:|---|
| All breakers opened | 10 / 10 | 10 | **PASS** |
| Breaker failure_count >= threshold | 3 | >= 2 | **PASS** |
| Goroutine count stable | 0 delta | <= 8 | **PASS** |
| Overall result | **PASS** | PASS | **PASS** |
