# ADR-0005: Discovery Loop vs ID-Block Claim Scalability Evaluation

**Date**: 2026-05-29
**Context**: Issue #22 — DB-backed active Customer discovery for horizontal scheduler fairness

## Background

The Webhook Notifier uses a process-local Scheduler (SWRR) to deliver per-Customer fairness.
With horizontal scaling, each worker instance has a partial view of active Customers.
Postgres `SELECT ... FOR UPDATE SKIP LOCKED` prevents duplicate delivery processing,
but cluster-wide fairness requires all workers to discover the active Customer set
from a shared source of truth.

Issue #22 tracks this work. The issue author's comment sketches two approaches:

## Approach A: Discovery Loop (chosen path)

A periodic goroutine in each worker process queries Postgres for distinct Customer IDs
with ready Deliveries and feeds them into the process-local Scheduler via `Notify()`.

**Discovery query:**
```sql
SELECT DISTINCT customer_id
FROM deliveries
WHERE (status IN ('pending', 'failed_retry') AND next_attempt_at <= now())
   OR (status = 'in_flight' AND claimed_at < now() - $lease)
```

**Worker loop remains unchanged:** Pick → Claim per-Customer → Process → Notify.

## Approach B: ID-Block Claim (Hatchet-style)

Contiguous ID ranges are pre-allocated per Customer. The claim query bounds the scan
window so no single Customer can consume the full batch, producing fairness from SQL alone.

**Claim query:**
```sql
WITH min_id AS (
  SELECT COALESCE(min(id), 0) FROM deliveries WHERE status = 'pending'
), eligible AS (
  SELECT id FROM deliveries
  WHERE status = 'pending'
    AND id >= (SELECT v FROM min_id)
    AND id <  (SELECT v FROM min_id) + $concurrency * (1 << 20)
    AND next_attempt_at <= now()
  ORDER BY id ASC
  FOR UPDATE SKIP LOCKED
  LIMIT $batch
)
```

No per-Customer Scheduler state needed.

---

## 1. Index contention on the claim path (the decisive factor)

### Discovery Loop — per-Customer claims

```sql
WHERE customer_id = $1
  AND (status IN ('pending', 'failed_retry') AND next_attempt_at <= now())
ORDER BY next_attempt_at, created_at, id
FOR UPDATE SKIP LOCKED
LIMIT $batchHint
```

Uses the existing composite index `deliveries(customer_id, status, next_attempt_at)`.
Each worker picks a different Customer → walks a different B-tree leaf page.

| Workers | Active Customers | Index page contention |
|---------|-----------------|----------------------|
| 10      | 50              | Zero — each hits different leaf |
| 100     | 100             | Zero — 1:1 worker:Customer mapping |
| 100     | 5               | Moderate — all workers share same 5 leaf pages |

**Mitigation for hot-Customer contention:** `batchHint` (equal to weight) lets a worker
claim multiple deliveries per Pick, reducing total Pick rounds for high-volume Customers.
With `batchHint=3`, 3× fewer rounds through the same leaf page.

### ID-Block Claim — shared range scan

```sql
WHERE id >= min_id AND id < min_id + window
ORDER BY id ASC
FOR UPDATE SKIP LOCKED
LIMIT $batch
```

Workers scan from a common starting point (`min_id`). Every worker competes for the same
B-tree leaf pages at the leading edge of the PK index.

| Workers | Contention model | Behaviour |
|---------|-----------------|-----------|
| 10      | All share hot pages | Workers scan overlapping pages; `SKIP LOCKED` skips locked rows but cache line bouncing on index pages |
| 100     | 100× fan-in on hot pages | Amplified contention — workers spend time waiting on buffer pool latches for shared pages |

SKIP LOCKED avoids row-level deadlocks but does **not** reduce index page contention.
All workers still read/modify the same buffer pool pages.

**Verdict:** Discovery Loop wins decisively. Per-Customer claim isolation is the
fundamental scaling advantage. The ID-block approach funnels all claim traffic
through a single hot index range.

---

## 2. UUID primary key makes ID-Block impractical

Current schema:
```sql
id uuid PRIMARY KEY DEFAULT gen_random_uuid()
```

Random UUIDs cannot form contiguous Customer blocks. `ORDER BY id ASC` scans
scattered pages across the entire table — cache efficiency is poor.

To fix, one of:
- Switch to `bigint` serial IDs
- Use UUIDv7 (time-ordered) — still random per-Customer
- Add explicit `block_id` column + `task_groups` allocation table

Each option requires:
- Schema migration
- Ingest path rewrite
- Backfill for existing rows

The Discovery Loop works with UUIDs as-is.

---

## 3. Discovery query overhead

### Index support

A partial index makes discovery cheap:
```sql
CREATE INDEX CONCURRENTLY idx_discovery
ON deliveries(customer_id)
WHERE status IN ('pending', 'failed_retry');
```

This index only contains non-terminal, non-in_flight rows — a tiny fraction of the table
at any point (deliveries are either pending briefly or succeeded/terminal).
On PG 16+, `SELECT DISTINCT` uses a skip scan: O(active Customers) leaf pages.

### Cost estimation

| Active Customers | Index pages visited | per query |
|-----------------|-------------------|-----------|
| 10              | ~10               | < 0.1 ms  |
| 1,000           | ~1,000            | ~1 ms     |
| 10,000          | ~10,000           | ~5 ms     |

### Adaptive backoff

- Query returns 0 rows → back off to 5s+ (exponential)
- Query returns >0 rows → tighten to 200ms

Under steady load: ~5 queries/sec hitting a tiny partial index. Negligible.

### Sharing across goroutines

One discovery goroutine per process feeds all worker goroutines in that process.
With 100 workers but 1 discovery loop: still **5 queries/sec** (at 200ms interval)
regardless of worker count. The discovery tax is constant per process, not per worker.

---

## 4. Write-path impact

| Aspect | Discovery Loop | ID-Block Claim |
|--------|---------------|----------------|
| Delivery insert | Unchanged | Needs `task_groups` lookup + block allocation |
| New Customer registration | Implicit (discovery finds them) | Explicit `task_groups` insert before first delivery |
| Existing data | Works as-is | Needs backfill/partition of existing deliveries |
| Transaction scope | Unchanged | Enlarged (allocation table write in same tx) |

The Discovery Loop touches zero code on the write path. The ID-Block approach
adds latency and coupling to every `INSERT INTO deliveries`.

---

## 5. WeightSource preservation

The architecture (ADR-0001, PR #21) depends on `WeightSource` for operational controls:

| Capability | Discovery Loop | ID-Block Claim |
|-----------|---------------|----------------|
| `weight=0` (pause Customer) | ✅ Scheduler skips it in Pick | ❌ Block still scanned; needs separate skip logic |
| `weight=3` (priority tier) | ✅ 3× share, `batchHint=3` | ❌ Not expressible in ID-block math |
| Dynamic weight change | ✅ Picked up next cycle | ❌ Block allocation is static |
| Forward compat with tiers | ✅ WeightSource interface | ❌ No weight abstraction available |

ID-Block trades away WeightSource for the simplicity of a single SQL query.
This is a foundational loss — the whole fairness architecture builds on weighted Customers.

---

## 6. Scale-out path

### Discovery Loop

Partition by `customer_id` hash. Each partition owns a subset of Customers:

```sql
-- Partitioned table
CREATE TABLE deliveries (
    ...
) PARTITION BY HASH (customer_id);
CREATE TABLE deliveries_p0 PARTITION OF deliveries FOR VALUES WITH (MODULUS 4, REMAINDER 0);
...
```

- Discovery queries hit only the local partition
- Claim queries are per-Customer → hit one partition
- Workers pinned to partitions → hot Customer's segment handled by dedicated workers

This is a natural, incremental migration.

### ID-Block Claim

Partition by `id` range. But blocks are per-Customer:

```
Customer A block: IDs 0x00000-0xFFFFF
Customer B block: IDs 0x10000-0x1FFFF
```

A partition boundary at `0x18000` splits Customer B across two partitions.
Cross-partition claim queries don't work with `ORDER BY id ASC ... FOR UPDATE`.
The block scheme and partitioning are fundamentally at odds.

---

## Summary

| Dimension | Discovery Loop | ID-Block Claim |
|-----------|---------------|----------------|
| **Claim contention** | Per-Customer = N independent index ranges | All workers share one hot range |
| **UUID support** | Works as-is | Schema migration required |
| **Discovery overhead** | Partial-index-friendly, ~1ms/query at 5qps | Zero (no separate query) |
| **Write-path impact** | None | Block allocation per insert, registration for new Customers |
| **WeightSource** | Preserved (pause, tiers, dynamic weight) | Sacrificed (not expressible) |
| **Implementation** | ~150 lines, new file, one CREATE INDEX | Schema migration + new table + ingest rewrite |
| **Scale-out by partition** | Trivial by `customer_id` hash | Conflicts with block allocation |
| **Operational tuning** | Polling interval and backoff (runtime config) | Block size (compile-time, hard to change) |

## Recommendation

**Proceed with the Discovery Loop (Approach A).**

The decision is driven by three non-negotiable constraints:
1. **WeightSource** is load-bearing — losing it invalidates ADR-0001 and planned priority-tier/throttle stories.
2. **UUID primary keys** are already committed — the ID-block approach requires a schema migration that touches every delivery insert.
3. **Index contention** at 100+ workers would make the ID-block approach throughput-bottlenecked on the PK index hot page, while the Discovery Loop keeps per-Customer claims isolated to independent index ranges.

The Discovery Loop is a small, isolated addition. Implementation is a single new file
(internal/scheduler/discovery/ or internal/worker/discovery.go) plus one `CREATE INDEX CONCURRENTLY`.
It preserves the entire existing architecture.
