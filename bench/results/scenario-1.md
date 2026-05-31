# Scenario 1: Fairness Under Load — Late-Arriving Small Customer

## Description

Profile **stress**: one **Whale** (50000 deliveries, burst at T=0) + **100 Small Customers** (5 deliveries each, dripped every 1s starting at T=1) + **1 Late Small Customer** (3 deliveries, injected at T=30s).

**Chaos conditions**: 15% of requests experience 300ms latency (simulating network round-trips). No status-code failures are injected; Scenario 1 isolates fairness under burst load from retry-policy behavior.

The acceptance criteria are that Small Customers have lifecycle p99 under 2s, the late-arriving Small Customer is picked within at most one round-robin cycle of its Notify, and the generated workload matches the deterministic setup.

## Performance Summary

| Metric | Value |
|---|---|
| Profile | stress |
| Verdict | Small Customers stayed responsive while the Whale backlog drained |
| Total duration | 1m41s |
| Total deliveries | 50503 / 50503 |
| End-to-end throughput (includes drip) | 502 deliveries/s |
| Workers | 8 |
| All Small lifecycle p99 | 411 ms |
| Late Small first terminal lifecycle latency | 89 ms |
| Whale backlog-drain p99 | 34565 ms |
| Whale backlog-drain max | 34883 ms |
| Implied whale drain throughput | 1433 deliveries/s |
| Projected 1000-whale drain on one node | 9h41m23s |
| HTTP attempt p99 | 301 ms |

## Acceptance Criteria

| Criterion | Observed | Limit | Result |
|---|---:|---:|---|
| Full workload created | 50503 deliveries | 50503 deliveries | **PASS** |
| All Small lifecycle p99 | 411 ms | < 2000 ms | **PASS** |
| Late Small picked within one RR cycle | 2 picks | <= 2 active Customers | **PASS** |

## Backlog Drain Latency

This is enqueue-to-terminal time, not HTTP request time. For the Whale, high values mostly mean the intentional burst backlog is waiting in queue while Small Customers continue to receive turns.
The 1000-whale projection is a single-node capacity estimate from this run, not an acceptance criterion; it shows that production-scale whale load needs horizontal capacity or partitioned whale pools.

| Class | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|
| **Whale** | 18574 ms | 33302 ms | 34565 ms | 34883 ms |
| **All Small** | 95 ms | 389 ms | 411 ms | 431 ms |
| **Late Small** | 88 ms | 89 ms | 89 ms | 89 ms |

## HTTP Attempt Duration

| Scope | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|
| **All attempts** | 0 ms | 300 ms | 301 ms | 302 ms |


## Delivery Outcomes

| Status | Count |
|---|---|
| succeeded | 50503 |

## Retry Distribution

| Attempts | Deliveries |
|---|---|
| 1 | 50503 |

## Round-Robin Reactivity

**PASS**: 2 Picks ≤ 2 active Customers — the late-arriving Small was served within one round-robin cycle.
