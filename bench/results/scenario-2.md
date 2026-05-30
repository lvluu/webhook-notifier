# Scenario 2: Transient + Recovering Partners

## Description

Profile **baseline**: 10 Customers, 100 Events per Customer, 3 Webhooks per Customer. Each Event fans out to Partner A, Partner B, and Partner C for **3000 expected Deliveries**.

- Partner A: healthy.
- Partner B: deterministic 30% 5xx.
- Partner C: 503 from 2026-05-30T22:16:04+07:00 to 2026-05-30T22:16:34+07:00, then recovers.
- Events are submitted every 20ms so the outage happens while new Deliveries are
  still being enqueued.

## Performance Summary

| Metric | Value |
|---|---:|
| Total duration | 3m42s |
| Events submitted | 1000 |
| Deliveries created | 3000 / 3000 |
| Workers | 32 |
| Terminal Deliveries | 3000 / 3000 |

## Acceptance Criteria

| Criterion | Observed | Limit | Result |
|---|---:|---:|---|
| 100% Deliveries terminal | 3000 / 3000 | 3000 / 3000 | **PASS** |
| Partner C outage-window Deliveries succeeded | 315 / 315 | 315 / 315 | **PASS** |
| Partner A lifecycle p99 unaffected | 178 ms | < 2000 ms | **PASS** |
| Partner C post-recovery retry spike | 66 max/s of 316 retries | <= 79 max/s | **PASS** |

## Per-Partner Lifecycle Latency

| Partner | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|
| Partner A | 73 ms | 139 ms | 178 ms | 306 ms |
| Partner B | 97 ms | 109657 ms | 134759 ms | 155295 ms |
| Partner C | 96 ms | 30103 ms | 35058 ms | 42381 ms |

## Attempts Per Delivery

| Attempts | Deliveries |
|---|---:|
| 1 | 2374 |
| 2 | 10 |
| 3 | 24 |
| 4 | 38 |
| 5 | 56 |
| 6 | 65 |
| 7 | 54 |
| 8 | 26 |
| 9 | 25 |
| 10 | 238 |
| 11 | 7 |
| 12 | 1 |
| 14 | 1 |
| 20 | 81 |

## Delivery Outcomes

| Status | Count |
|---|---:|
| dead | 81 |
| succeeded | 2919 |
