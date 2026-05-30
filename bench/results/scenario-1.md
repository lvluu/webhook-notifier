# Scenario 1: Fairness Under Load — Late-Arriving Small Customer

## Description

One **Whale** (50k deliveries, burst at T=0) + **100 Small Customers** (5 deliveries each, dripped one per second starting at T=1) + **1 Late Small Customer** (3 deliveries, injected at T=30).

**Chaos conditions**: 15% of requests experience 300ms latency (simulating network round-trips). No status-code failures are injected; Scenario 1 isolates fairness under burst load from retry-policy behavior.

The acceptance criteria are that Small Customers have lifecycle p99 under 2s, the late-arriving Small Customer is picked within at most one round-robin cycle of its Notify, and the generated workload matches the deterministic setup.

## Performance Summary

| Metric | Value |
|---|---|
| Verdict | Small Customers stayed responsive while the Whale backlog drained |
| Total duration | 1m41s |
| Total deliveries | 50503 / 50503 |
| End-to-end throughput (includes 100s drip) | 500 deliveries/s |
| Workers | 32 |
| All Small lifecycle p99 | 461 ms |
| Late Small first terminal lifecycle latency | 81 ms |
| Whale backlog-drain p99 | 50273 ms |
| Whale backlog-drain max | 50468 ms |
| Implied whale drain throughput | 991 deliveries/s |
| Projected 1000-whale drain on one node | 14h1m8s |
| HTTP attempt p99 | 303 ms |

## Acceptance Criteria

| Criterion | Observed | Limit | Result |
|---|---:|---:|---|
| Full workload created | 50503 deliveries | 50503 deliveries | **PASS** |
| All Small lifecycle p99 | 461 ms | < 2000 ms | **PASS** |
| Late Small picked within one RR cycle | 3 picks | <= 3 active Customers | **PASS** |

## Backlog Drain Latency

This is enqueue-to-terminal time, not HTTP request time. For the Whale, high values mostly mean the intentional 50k burst is waiting in queue while Small Customers continue to receive turns.
The 1000-whale projection is a single-node capacity estimate from this run, not an acceptance criterion; it shows that production-scale whale load needs horizontal capacity or partitioned whale pools.

| Class | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|
| **Whale** | 34024 ms | 49107 ms | 50273 ms | 50468 ms |
| **All Small** | 95 ms | 419 ms | 461 ms | 635 ms |
| **Late Small** | 60 ms | 79 ms | 80 ms | 81 ms |

## HTTP Attempt Duration

| Scope | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|
| **All attempts** | 0 ms | 301 ms | 303 ms | 334 ms |


## Delivery Outcomes

| Status | Count |
|---|---|
| succeeded | 50503 |

## Retry Distribution

| Attempts | Deliveries |
|---|---|
| 1 | 50503 |

## Round-Robin Reactivity

**PASS**: 3 Picks ≤ 3 active Customers — the late-arriving Small was served within one round-robin cycle.
