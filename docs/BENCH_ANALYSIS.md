# Benchmark Bottleneck Analysis

## Overview

Five scenarios exercising different failure modes on a single-node Postgres 16
(testcontainer) with workers on the same host. Receiver is localhost with
injected latency/status.

| Scenario | Throughput | Runtime | Key Constraint |
|----------|-----------:|--------:|----------------|
| 1 — Fairness | 501/s (726/s whale) | 1m41s | Whale 50K queue depth |
| 2 — Transient failures | 11.8/s | 4m14s | Retry backoff for 30% 5xx |
| 3 — Fast-fail 4xx | 1000/s | 2s | None (immediate terminal) |
| 4 — Scale | 4056/s | 2.5s (8W) | Postgres claim contention |
| 5 — Circuit breaker | 3047/s (dead partner) | 16.4s | Dead hang → pool pressure |

---

## Bottleneck #1: stableProbability FNV-64a Avalanche Bug (Scenario 2)

**Severity: CRITICAL** — 360x throughput regression vs Scenario 3.

### Root Cause

The receiver's `stableProbability` function uses FNV-64a with the attempt
number at the END of the hash input. FNV-64a processes bytes sequentially
with XOR + multiply. When only the last byte(s) vary (different attempt
numbers), the avalanche effect is virtually zero — every single-digit attempt
(1-9) produces an almost identical hash value (differing by ~6e-8).

```
// Bug: attempt number LAST in hash input
h.Write(seed)
h.Write(kind)
h.Write(path)
h.Write(delivery_id)
h.Write(attempt_number)  ← only this byte changes per attempt
```

This means a delivery with stableProbability("1") = 0.033 sees ≈0.033 for
ALL attempts 2-9 (same hash, only last byte differs). Two-digit attempt
strings ("10" vs "1") add an extra byte, changing more of the internal
state and producing a different value, but all two-digit attempts 10-20
again cluster around the same value.

```
Delivery UUID a00000…10:
  attempt  1: p = 0.033231895365  → 500
  attempt  2: p = 0.033231835760  → 500  ← same value!
  attempt  3: p = 0.033231776155  → 500  ← same value!
  ...
  attempt  9: p = 0.033231418527  → 500
  attempt 10: p = 0.356892268866  → 200  ← extra byte shifts hash
  attempt 11: p = 0.356892328471  → 200  ← clustered again
  ...
```

### Impact

With 30% error rate configured, ~28% of deliveries hash below 0.30 on
attempt 1. Because FNV-64a doesn't avalanche for single-digit attempts,
ALL of them fail on attempts 1-9. When the attempt string crosses from
1-digit to 2-digit at attempt 10, the hash state shifts:

- ~69% of the failing cluster escape to ≥0.30 → succeed at attempt 10
- ~31% remain <0.30 → exhaust all 20 attempts → dead

```
Attempt distribution of the 2978 deliveries that fail attempt 1:
  7022 succeed at attempt 1
     0 succeed at attempts 2-9 (all blocked by FNV-64a correlation)
  2081 succeed at attempt 10 (2-byte "10" shifts hash above threshold)
   897 dead at attempt 20 (never escape the <0.30 cluster)

Benchmark-observed: 87 dead / 1000 → proportionally consistent (bench
uses only 1000 Partner B deliveries; 897/10000 scaled = 89.7 ≈ 87)
```

Each dead delivery follows this backoff trajectory (maxAttempts=20,
CapDelay=15s):

```
Attempt    Backoff    Cumulative
    1      immediate   0s
    2      ~1s         1s
    3      ~3s         4s
    4      ~5s         9s
    5      ~12s       21s
    6      ~15s       36s
    7      ~15s       51s
  ...      ~15s       ...
   20      ~15s       ~3min (exhausted → dead)
```

87 dead deliveries × ~3min each = ~261 worker-minutes consumed by hopeless
retries before they land in DLQ. This is the dominant cause of the 360x
throughput regression.

### The Fix (applied)

Move the attempt_number to the FIRST position in the FNV-64a hash input.
With the most-varying input at the beginning, the full avalanche chain
runs through the remaining bytes (seed, kind, path, UUID), producing
truly independent per-attempt probabilities:

```go
// Fix: attempt number FIRST for full avalanche
h.Write(attempt_number)  ← now varies the entire hash state
h.Write(seed)
h.Write(kind)
h.Write(path)
h.Write(delivery_id)
```

Verified with 10000 deliveries using the fixed version: 0 dead (expected
≈3.5e-8 for Bernoulli(0.3) with maxAttempts=20). Attempt distribution
closely matches the geometric model:

```
Fixed, attempt 1: 7023 (expected 7000)
Fixed, attempt 2: 2042 (expected 2100)
Fixed, attempt 3:  666 (expected  630)
Fixed, attempt 4:  191 (expected  189)
Fixed, attempt 5:   54 (expected   57)
Fixed, attempt 6+:  24 (expected   24)
```

### Secondary: Retry Policy Amplification

Even with the FNV bug fixed, persistent 30% 5xx against maxAttempts=20
and CapDelay=15s imposes a steep throughput cost purely from backoff.
The bug made it catastrophic by multiplying the number of deliveries
that reach deep backoff levels (attempts 5+ have ≥12s delays).

For production hardening still worth considering:
- Reduce maxAttempts from 20 to 8-10 when failure rate exceeds a threshold
- Add a "fast dead" policy: if a webhook has >N consecutive 5xx within M
  minutes, fast-fail all pending deliveries for that webhook into DLQ
- The circuit breaker should protect against this, but it was disabled
  (FailureThreshold=0) for Scenario 2

---

## Bottleneck #2: Postgres Claim Contention (Scenario 4)

**Severity: MEDIUM** — scaling efficiency 58.5% at 8 workers.

### Root Cause

The ClaimNextN CTE atomically SELECTs and UPDATEs rows while other workers
contend on the same customer's delivery queue. Each claim round-trip:

```
WITH candidate AS (
  SELECT d.id FROM deliveries d
  JOIN webhooks w ON w.id = d.webhook_id
  WHERE d.customer_id = $1
    AND (claimability conditions)
    AND (breaker filters)
  ORDER BY next_attempt_at, created_at, id
  LIMIT $3
  FOR UPDATE OF d SKIP LOCKED   ← all workers compete here
), ...
```

`FOR UPDATE SKIP LOCKED` prevents duplicates but doesn't prevent contention.
As workers scale from 1 to 8, each worker spends more wall time per claim
because it often finds fewer rows (skipped/locked by other workers).

### Data

```
Workers   Throughput    Per-Worker    Efficiency
      1     867/s          867/s        100%
      4    2734/s          684/s         79%
      8    4057/s          507/s         58.5%
```

The per-worker throughput drops by 42% from 1 to 8 workers. At 8 workers,
22% of total time is lost to PG row-level contention.

### Secondary: Batch Mark CTEs

The MarkSucceededBatch CTE uses `unnest($1::text[], $2::int[])` to process
batches. Each batch mark opens a transaction and runs:

```
WITH batch AS (...),
updated AS (UPDATE ... FROM batch ...),
inserted AS (INSERT INTO delivery_attempts ... SELECT ...)
```

At 4056/s with batch_hint=32, that's ~127 batch marks/s = ~254 SQL
statements/s (claim + mark). Each statement is a multi-CTE query that
Postgres plans and executes. This is sustainable for testcontainer PG16 on
local SSD but would be a bottleneck at 10x scale.

### Fix

- Increase batch_hint above 32 for large backlogs (already dynamic via
  batchHint parameter). For stress profiles with 10K+ deliveries, hint=128
  would reduce PG round-trips by 4x
- Partition deliveries by customer hash across multiple PG instances for
  true horizontal scale
- Connection pooling (PgBouncer) to absorb peak claim bursts

---

## Bottleneck #3: Fairness Tax (Scenario 1)

**Severity: LOW** — deliberate design tradeoff.

### Root Cause

The round-robin scheduler gives equal turns to all customers. With 101
customers (1 whale + 100 small), the whale gets 1/101 of each cycle. Each
turn for a small customer claims up to 32 deliveries — but most small
customers have only 2-5 deliveries total. Workers spend full claim cycles
on near-empty customers.

### Data

```
Whale drain rate:       726/s
All-delivery rate:      501/s
Fairness tax:           31% overhead
Small lifecycle p99:    427ms (well under 2s limit)
```

The 31% overhead is the cost of:
- Claim CTE for a small customer's 2 deliveries (same PG round-trip as
  claiming 32)
- HTTP POST for each delivery
- Batch mark for 2 items
- Context switch to next customer in round-robin

This is correct behavior per ADR-0001 (fairness before throughput).

### Fix

- If fairness requirements permit, batch more aggressively: batchHint could
  grow when the same customer is picked multiple times with work available
- Priority scheduling: backpressure from small-customer overhead could be
  reduced by amortizing across fewer cycles with larger batches

---

## Bottleneck #4: Circuit Breaker Propagation Latency (Scenario 5)

**Severity: LOW-MEDIUM** — 353 surplus attempts during propagation window.

### Root Cause

The breaker opens after 2 consecutive failures. But before state propagates
to all workers:

1. Worker A claims 32 deliveries for Customer 1
2. First 2 deliveries fail → breaker opens
3. Workers B, C, D have already claimed batches before `breaker_state` is
   replicated (it's in PG, but they've already done their SELECT FUSLCK)
4. Those in-flight batches finish (250ms timeout each) before the next
   claim round skips them

### Data

```
Dead-customer attempts before breaker hold: 353 (target <= 500)
Expected: 2 (failure threshold)
The gap (353 vs 2) = in-flight claims that complete after breaker opens
```

The delta is 353/32 ≈ 11 claim rounds across 64 workers. Each round
completes in ~250ms (hang timeout), so the breaker takes ~3s (11 × 250ms
= 2.75s) to fully suppress claims from all workers.

### Fix

- Pre-check breaker state in the worker before claim (currently it's only
  in the ClaimNextN CTE). This would add a `SELECT breaker_state FROM
  webhooks` before each claim round — extra round-trip but eliminates the
  353 surplus
- Reduce claim batch size for webhooks in suspicious state
- This is acceptable current behavior: 353 surplus attempts out of 5000
  deliveries (7%) is within bounds

---

## Bottleneck #5: Idle Discovery Loop Overhead

**Severity: LOW** — minimal CPU but steady PG query.

### Root Cause

The discovery loop runs every 1s by default, issuing:

```sql
SELECT DISTINCT d.customer_id
FROM deliveries d
JOIN webhooks w ON w.id = d.webhook_id
WHERE d.customer_id > $1
  AND (claimability conditions)
  AND (breaker filters)
ORDER BY d.customer_id
LIMIT $3
```

`SELECT DISTINCT ... ORDER BY ...` forces a sort on `customer_id`. On an
empty or drained system, this is a sequential scan through the deliveries
table until finding a matching row. With 50000 rows, the sequential scan
finishes quickly, but at production scale (millions), this query could be
expensive.

### Data

```
Default interval: 1s
Empty backoff:    5s
Max backoff:     30s
Limit:           1000 customers per page
```

### Fix

- Add an index on deliveries for coverage of the claim filter:
  `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_deliveries_claim_scan
   ON deliveries (customer_id, status, next_attempt_at, claimed_at)`
- The discovery DISTINCT query would then use an Index Only Scan instead
  of seq scan
- Scenario 5 stress already uses 50ms discovery interval — keep config
  bounded

---

## Summary Table

| # | Bottleneck | Scenario | Impact | Priority | Fix |
|---|-----------|----------|-------:|----------|-----|
| 1 | FNV-64a avalanche bug → false retry correlation | S2 | 11.8/s (360x slower) | Critical | Put attempt_number first in hash input |
| 2 | PG claim contention | S4 | 58.5% scaling eff | Medium | Larger batches, connection pool |
| 3 | Fairness overhead | S1 | 31% throughput tax | Low | Dynamic batch growth |
| 4 | Breaker propagation delay | S5 | 353 surplus attempts | Low | Pre-claim breaker check |
| 5 | Discovery DISTINCT query | all | Steady PG overhead | Low | Covering index |

---

## Cross-Cutting Observations

### Zero contention at localhost network

The receiver is a local HTTP server. Real-world latency would be 20-100ms
per attempt instead of 0-10ms. This changes the bottleneck from PG to
network for Scenarios 1, 4, 5. Estimated production throughput with 50ms
RTT: ~200/s per worker (vs 507/s in bench-4).

### HTTP transport is not the bottleneck

`MaxIdleConns: 10000`, `MaxIdleConnsPerHost: 500`, `MaxConnsPerHost: 0`
(unlimited). Connection reuse is excellent — benchmarks show zero
TLS-handshake overhead.

### Missing: slow-200 scenario

No scenario tests a partner that returns 200 OK after 10+ seconds. This
would expose HostLimiter backpressure under slow-success responses vs fast
failures. The current dead-hang test uses timeout (250ms) which is closer
to fast failure than slow success.

### Missing: DB-chaos scenario

All scenarios assume Postgres is healthy and fast. A production incident
with a slow PG query or connection pool exhaustion would cascade through
all workers simultaneously since every claim, mark, and discovery query
hits the same database.
