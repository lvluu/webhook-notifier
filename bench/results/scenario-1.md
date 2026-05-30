# Scenario 1: Fairness Under Load — Late-Arriving Small Customer

## Description

One **Whale** (50k deliveries, burst at T=0) + **100 Small Customers** (5 deliveries each, dripped one per second starting at T=1) + **1 Late Small Customer** (3 deliveries, injected at T=30).

**Chaos conditions**: 15% of requests experience 300ms latency (simulating network round-trips), 5% transient 500 errors (exercising retry), and the whale endpoint returns 502 10% of the time (simulating a partner under load).

The acceptance criteria are that Small Customers have lifecycle p99 under 2s, the late-arriving Small Customer is picked within at most one round-robin cycle of its Notify, and the generated workload matches the deterministic setup.

## Results

| Metric | Value |
|---|---|
| Total duration | 3m38s |
| Total deliveries | 50503 / 50503 |
| Workers | 8 |
| Picks until first late service | 4 |
| Active Customers at late Notify | 7 |
| Late Small first terminal lifecycle latency | 44 ms |
| RR cycle bound satisfied | ✅ Yes |

## Acceptance Criteria

| Criterion | Observed | Limit | Result |
|---|---:|---:|---|
| Full workload created | 50503 deliveries | 50503 deliveries | **PASS** |
| All Small lifecycle p99 | 396 ms | < 2000 ms | **PASS** |
| Late Small picked within one RR cycle | 4 picks | <= 7 active Customers | **PASS** |

## Lifecycle Latency (enqueue to terminal)

| Class | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|

| **Whale** | 41673 ms | 80461 ms | 84068 ms | 84943 ms |
| **All Small** | 317 ms | 377 ms | 396 ms | 483 ms |
| **Late Small** | 36 ms | 43 ms | 44 ms | 44 ms |
| **Overall** | 41202 ms | 80410 ms | 84068 ms | 84943 ms |

## HTTP Attempt Duration

| Scope | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|
| **All attempts** | 0 ms | 301 ms | 302 ms | 349 ms |


## Delivery Outcomes

| Status | Count |
|---|---|
| dead | 5057 |
| succeeded | 45446 |

## Retry Distribution

| Attempts | Deliveries |
|---|---|
| 1 | 45446 |
| 8 | 5057 |

## Round-Robin Cycle Bound

Picks before late Notify: **0**
Picks after late Notify: **108516** (total for remaining benchmark)
Picks until first late service: **4**
Active Customers at late Notify: **7**

**PASS**: 4 Picks ≤ 7 active Customers — the late-arriving Small was served within one round-robin cycle.
