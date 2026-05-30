# Scenario 1: Fairness Under Load — Late-Arriving Small Customer

## Description

One **Whale** (5k deliveries, burst at T=0) + **100 Small Customers** (5 deliveries each, dripped one per second starting at T=1) + **1 Late Small Customer** (3 deliveries, injected at T=30).

**Chaos conditions**: 15% of requests experience 300ms latency (simulating network round-trips), 5% transient 500 errors (exercising retry), and the whale endpoint returns 502 10% of the time (simulating a partner under load).

The acceptance criterion is that the late-arriving Small Customer must be picked by a worker within at most one round-robin cycle of its Notify, and that small-customer P99 latency stays under 10s despite failures.

## Results

| Metric | Value |
|---|---|
| Total duration | 1m4s |
| Workers | 8 |
| RR cycle picks after late Notify | 9759 |
| Active Customers at late Notify | 3 |
| RR cycle bound satisfied | ❌ No |

| **Whale (P50/P95/P99/Max)** | 29924 ms | 55652 ms | 58029 ms | 63461 ms |
| **All Small (P50/P95/P99/Max)** | 21 ms | 353 ms | 1280 ms | 4241 ms |
| **Late Small (P50/P95/P99/Max)** | 50 ms | 54 ms | 55 ms | 55 ms |
| **Overall (P50/P95/P99/Max)** | 27914 ms | 55314 ms | 58029 ms | 63461 ms |

## Delivery Outcomes

| Status | Count |
|---|---|
| succeeded | 5318 |

## Retry Distribution

| Attempts | Deliveries |
|---|---|
| 1 | 4832 |
| 2 | 428 |
| 3 | 52 |
| 4 | 6 |

## Round-Robin Cycle Bound

Picks before late Notify: **0**
Picks after late Notify: **9759**
Delta (picks during cycle): **9759**
Active Customers at late Notify: **3**

**FAIL**: 9759 Picks > 3 active Customers — the late-arriving Small was not served within one round-robin cycle.
