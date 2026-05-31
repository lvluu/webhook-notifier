# Scenario 2: Transient + Recovering Partners

## Description

Profile **stress**: 10 Customers, 100 Events per Customer, 3 Webhooks per Customer. Each Event fans out to Partner A, Partner B, and Partner C for **3000 expected Deliveries**.

- Partner A: healthy.
- Partner B: deterministic 30% 5xx.
- Partner C: 503 from 2026-05-31T11:52:28+07:00 to 2026-05-31T11:52:58+07:00, then recovers.
- Events are submitted every 20ms so the outage happens while new Deliveries are
  still being enqueued.

## Performance Summary

| Metric | Value |
|---|---:|
| Total duration | 4m30s |
| Events submitted | 1000 |
| Deliveries created | 3000 / 3000 |
| Workers | 64 |
| Terminal Deliveries | 3000 / 3000 |

## Acceptance Criteria

| Criterion | Observed | Limit | Result |
|---|---:|---:|---|
| 100% Deliveries terminal | 3000 / 3000 | 3000 / 3000 | **PASS** |
| Partner C outage-window Deliveries succeeded | 232 / 232 | 232 / 232 | **PASS** |
| Partner A lifecycle p99 unaffected | 205 ms | < 2000 ms | **PASS** |
| Partner C post-recovery retry spike | 48 max/s of 234 retries | <= 58 max/s | **PASS** |

## Per-Partner Lifecycle Latency

| Partner | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|
| Partner A | 82 ms | 161 ms | 205 ms | 320 ms |
| Partner B | 108 ms | 106948 ms | 138936 ms | 167535 ms |
| Partner C | 97 ms | 27737 ms | 34839 ms | 40167 ms |

## Attempts Per Delivery

| Attempts | Deliveries |
|---|---:|
| 1 | 2473 |
| 2 | 9 |
| 3 | 15 |
| 4 | 30 |
| 5 | 38 |
| 6 | 43 |
| 7 | 37 |
| 8 | 35 |
| 9 | 9 |
| 10 | 203 |
| 11 | 4 |
| 12 | 2 |
| 13 | 1 |
| 14 | 1 |
| 15 | 1 |
| 20 | 99 |

## Delivery Outcomes

| Status | Count |
|---|---:|
| dead | 99 |
| succeeded | 2901 |
