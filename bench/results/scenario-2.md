# Scenario 2: Transient + Recovering Partners

## Description

Profile **stress**: 10 Customers, 100 Events per Customer, 3 Webhooks per Customer. Each Event fans out to Partner A, Partner B, and Partner C for **3000 expected Deliveries**.

- Partner A: healthy.
- Partner B: deterministic 30% 5xx.
- Partner C: 503 from 2026-05-31T12:01:51+07:00 to 2026-05-31T12:02:21+07:00, then recovers.
- Events are submitted every 20ms so the outage happens while new Deliveries are
  still being enqueued.

## Performance Summary

| Metric | Value |
|---|---:|
| Total duration | 4m14s |
| Events submitted | 1000 |
| Deliveries created | 3000 / 3000 |
| Workers | 64 |
| Terminal Deliveries | 3000 / 3000 |

## Acceptance Criteria

| Criterion | Observed | Limit | Result |
|---|---:|---:|---|
| 100% Deliveries terminal | 3000 / 3000 | 3000 / 3000 | **PASS** |
| Partner C outage-window Deliveries succeeded | 268 / 268 | 268 / 268 | **PASS** |
| Partner A lifecycle p99 unaffected | 210 ms | < 2000 ms | **PASS** |
| Partner C post-recovery retry spike | 54 max/s of 269 retries | <= 67 max/s | **PASS** |

## Per-Partner Lifecycle Latency

| Partner | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|
| Partner A | 75 ms | 147 ms | 210 ms | 295 ms |
| Partner B | 95 ms | 105339 ms | 134248 ms | 161419 ms |
| Partner C | 93 ms | 29992 ms | 35409 ms | 40372 ms |

## Attempts Per Delivery

| Attempts | Deliveries |
|---|---:|
| 1 | 2441 |
| 2 | 10 |
| 3 | 14 |
| 4 | 28 |
| 5 | 40 |
| 6 | 53 |
| 7 | 47 |
| 8 | 32 |
| 9 | 21 |
| 10 | 218 |
| 11 | 6 |
| 12 | 2 |
| 13 | 1 |
| 20 | 87 |

## Delivery Outcomes

| Status | Count |
|---|---:|
| dead | 87 |
| succeeded | 2913 |
