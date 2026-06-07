# Transient Recovery: Transient + Recovering Partners

## Description

Profile **stress**: 10 Customers, 100 Events per Customer, 3 Webhooks per Customer. Each Event fans out to Partner A, Partner B, and Partner C for **3000 expected Deliveries**.

- Partner A: healthy.
- Partner B: deterministic 30% 5xx.
- Partner C: 503 from 2026-06-06T20:40:51+07:00 to 2026-06-06T20:41:21+07:00, then recovers.
- Events are submitted every 20ms so the outage happens while new Deliveries are
  still being enqueued.

## Performance Summary

| Metric | Value |
|---|---:|
| Total duration | 50s |
| Events submitted | 1000 |
| Deliveries created | 3000 / 3000 |
| Workers | 64 |
| Terminal Deliveries | 3000 / 3000 |

## Acceptance Criteria

| Pillar | Criterion | Observed | Limit | Result |
|---|---|---:|---:|---|
| Reliability | 100% Deliveries terminal | 3000 / 3000 | 3000 / 3000 | **PASS** |
| Reliability | Partner C outage-window Deliveries succeeded | 778 / 778 | 778 / 778 | **PASS** |
| Reliability | Partner A lifecycle p99 unaffected | 15 ms | < 2000 ms | **PASS** |
| Reliability | Partner C post-recovery retry spike | 122 max/s of 778 retries | <= 194 max/s | **PASS** |

## Per-Partner Lifecycle Latency

| Partner | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|
| Partner A | 4 ms | 11 ms | 15 ms | 23 ms |
| Partner B | 6 ms | 3136 ms | 6900 ms | 24466 ms |
| Partner C | 22995 ms | 34628 ms | 37719 ms | 41630 ms |

## Attempts Per Delivery

| Attempts | Deliveries |
|---|---:|
| 1 | 1910 |
| 2 | 224 |
| 3 | 68 |
| 4 | 15 |
| 5 | 82 |
| 6 | 210 |
| 7 | 203 |
| 8 | 146 |
| 9 | 68 |
| 10 | 36 |
| 11 | 22 |
| 12 | 10 |
| 13 | 5 |
| 14 | 1 |

## Delivery Outcomes

| Status | Count |
|---|---:|
| succeeded | 3000 |
