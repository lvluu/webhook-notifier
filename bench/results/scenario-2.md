# Scenario 2: Transient + Recovering Partners

## Description

Profile **baseline**: 10 Customers, 100 Events per Customer, 3 Webhooks per Customer. Each Event fans out to Partner A, Partner B, and Partner C for **3000 expected Deliveries**.

- Partner A: healthy.
- Partner B: deterministic 30% 5xx.
- Partner C: 503 from 2026-05-30T19:46:01+07:00 to 2026-05-30T19:46:31+07:00, then recovers.
- Events are submitted every 20ms so the outage happens while new Deliveries are
  still being enqueued.

## Performance Summary

| Metric | Value |
|---|---:|
| Total duration | 3m39s |
| Events submitted | 1000 |
| Deliveries created | 3000 / 3000 |
| Workers | 32 |
| Terminal Deliveries | 3000 / 3000 |

## Acceptance Criteria

| Criterion | Observed | Limit | Result |
|---|---:|---:|---|
| 100% Deliveries terminal | 3000 / 3000 | 3000 / 3000 | **PASS** |
| Partner C outage-window Deliveries succeeded | 403 / 403 | 403 / 403 | **PASS** |
| Partner A lifecycle p99 unaffected | 133 ms | < 2000 ms | **PASS** |
| Partner C post-recovery retry spike | 74 max/s of 403 retries | <= 100 max/s | **PASS** |

## Per-Partner Lifecycle Latency

| Partner | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|
| Partner A | 60 ms | 105 ms | 133 ms | 177 ms |
| Partner B | 72 ms | 107749 ms | 141369 ms | 160804 ms |
| Partner C | 86 ms | 30590 ms | 36426 ms | 42458 ms |

## Attempts Per Delivery

| Attempts | Deliveries |
|---|---:|
| 1 | 2314 |
| 2 | 13 |
| 3 | 30 |
| 4 | 42 |
| 5 | 79 |
| 6 | 85 |
| 7 | 58 |
| 8 | 47 |
| 9 | 29 |
| 10 | 200 |
| 11 | 2 |
| 13 | 2 |
| 20 | 99 |

## Delivery Outcomes

| Status | Count |
|---|---:|
| dead | 99 |
| succeeded | 2901 |
