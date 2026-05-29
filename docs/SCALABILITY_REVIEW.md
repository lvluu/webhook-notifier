# Scalability Review: Webhook Notifier PoC

> **Status**: Review complete — 3 explore agents (scheduler, outbox/claim, ingest/host_limiter)
> + 1 Oracle (architecture advisor). All findings grounded in specific code locations.
>
> **Date**: 2026-05-29
> **Reviewer**: Sisyphus (orchestrator) + Oracle (architecture advisor)

---

## Bottom Line

The PoC proves correctness (SWRR fairness, SKIP LOCKED safety, retry lifecycle) but will **not handle peak production volumes** without significant changes. The three hard ceilings, in order they'll be hit:

1. **PG commit rate** — ~2 tx/delivery at default `batchHint=1` caps throughput at ~5k delivery/s per PG instance
2. **SWRR scheduler O(A log A)** — sorting 5k-20k active customers on every Pick becomes a CPU bottleneck
3. **No cross-instance coordination** — process-local schedulers provide no cross-instance fairness and an unbounded "undiscovered customer" latency window

---

## 1. Transaction & Round-Trip Overhead

### Per-Delivery SQL Cost (Success Path, batchHint=1)

| Phase | SQL Statements | Network Round-Trips | Transactions |
|-------|---------------|-------------------|--------------|
| Ingest (shared across N deliveries) | 2 | 2 | 1 |
| Claim | 3 | 3 | 1 |
| MarkSucceeded | 3 | 3 | 1 |
| **Total per delivery** | **~6** | **~6** | **2** |

**Code evidence**: `internal/outbox/outbox.go` lines 106-136 (claim tx: SELECT → per-row UPDATE → per-row SELECT JOIN → COMMIT) and lines 256-300 (mark tx: SELECT FOR UPDATE → INSERT attempt → UPDATE delivery → COMMIT).

### Throughput Ceiling Per PG Connection

At an optimistic 0.5ms per round-trip:
- **333 deliveries/sec/connection** (6 round-trips per delivery)
- At 14k deliveries/sec peak → **~42 busy connections**
- Past that: connection pool contention, queueing, degraded latency

### Full Retry Chain (8 Attempts, Dead Letter)

**~17 transactions per delivery**: 1 ingest + 8×(claim + mark retry) + 1 mark dead.
At 1% failure rate (50k dead letters/hour at peak), retry traffic adds ~800k tx/hour on top of baseline.

---

## 2. Claim Query Performance

### Problem: OR Condition Defeats Single Index

**File**: `internal/outbox/outbox.go` lines 141-153

```sql
WHERE customer_id = $1
  AND (
      (status IN ('pending', 'failed_retry') AND next_attempt_at <= now())
      OR (status = 'in_flight' AND claimed_at < now() - $2)
  )
ORDER BY next_attempt_at, created_at, id
FOR UPDATE SKIP LOCKED
LIMIT $3
```

**Existing index**: `(customer_id, status, next_attempt_at)` — covers the first OR branch only.

**Problems**:
- `claimed_at` in the second OR branch is **not indexed** — forces seq scan or filter per customer
- `ORDER BY next_attempt_at, created_at, id` — index lacks `created_at, id`, so Postgres does a top-N sort
- OR clause prevents Postgres from using a single index scan efficiently
- `id` is a random UUID — poor tie-breaker for locality

### Fix

Split into two partial indexes:

```sql
CREATE INDEX idx_deliveries_ready
    ON deliveries (customer_id, next_attempt_at, created_at, id)
    WHERE status IN ('pending', 'failed_retry');

CREATE INDEX idx_deliveries_inflight_stale
    ON deliveries (customer_id, claimed_at)
    WHERE status = 'in_flight';
```

Or replace the OR with a UNION ALL of two index-friendly queries.

---

## 3. Per-Row Claim Loop

### Problem

**File**: `internal/outbox/outbox.go` lines 122-132

`ClaimNextN` does a batch SELECT FOR UPDATE to get N delivery IDs, then loops per-ID:
- 1x `UPDATE markInFlight` (line 175)
- 1x `SELECT loadDelivery` with JOINs to events, webhooks + subquery (line 193)

Total: **2N + 2 SQL statements per claim batch** (1 SELECT + N UPDATE + N SELECT JOIN + 1 COMMIT).

### Impact

At `batchHint=1`: 3 statements per claim (plus they hold FOR UPDATE locks while doing JOINs/subqueries).

### Fix

Single CTE `UPDATE ... RETURNING`:

```sql
WITH claimed AS (
    UPDATE deliveries
    SET status = 'in_flight',
        attempt_count = attempt_count + 1,
        claimed_at = now(),
        updated_at = now()
    WHERE id IN (
        SELECT id FROM deliveries
        WHERE customer_id = $1
          AND (status IN ('pending', 'failed_retry') AND next_attempt_at <= now())
        ORDER BY next_attempt_at, created_at, id
        LIMIT $2
        FOR UPDATE SKIP LOCKED
    )
    RETURNING id, event_id, webhook_id, customer_id, attempt_count, ...
)
SELECT claimed.*, events.payload, webhooks.url, webhooks.signing_key
FROM claimed
JOIN events ON claimed.event_id = events.id
JOIN webhooks ON claimed.webhook_id = webhooks.id;
```

Reduces round-trips from **2N+2 → 1** for the claim tx.

---

## 4. SWRR Scheduler Complexity

### Problem

**File**: `internal/scheduler/swrr/swrr.go` lines 69-121

Every `Pick()`:
1. Iterates all active customers (potentially 5k-20k)
2. Calls `Weight(id)` for each (could be expensive depending on impl)
3. `sort.SliceStable` the full candidate list — **O(A log A)**
4. Rebuilds `activeList` by appending all non-winner entries — **O(A)**
5. Returns one customer with `batchHint = weight`

With `weight=1` (default), `batchHint=1`, so at 14k delivery/s → **14k Pick calls/sec**. Each sorting 5k-20k entries.

### Fix

Replace with **O(1) deficit round robin** or maintain a heap whose per-pick cost is `O(log A)`. Or simpler: stop removing customers from the active set on Pick — track a per-cursor state and rotate.

---

## 5. Scheduler Discovery Gap

### Problem

**Files**:
- `internal/ingest/ingest.go` lines 54-73 — writes Deliveries to Postgres
- `internal/scheduler/scheduler.go` lines 23-26 — `Notify()` is the only way customers enter rotation
- `internal/worker/worker.go` lines 229, 233 — Notify is called only after processing batches

**No production code calls `Scheduler.Notify(customerID)` when new deliveries are created.** Result: new deliveries sit in Postgres undiscovered. Worst-case latency is **unbounded**.

Tests work around this by calling `sched.Notify()` directly (e.g., `outbox_integration_test.go`), but the production service has no such path.

### Fix

1. **Immediate**: `ingest.Submit` calls `Scheduler.Notify(customerID)` for the local process
2. **Production**: DB-backed active-customer discovery — either LISTEN/NOTIFY or a `customer_work` table with leasing

---

## 6. BatchHint = 1 (Default Weight)

### Problem

**File**: `internal/scheduler/scheduler.go` lines 35-49 — default `ConstantWeights{Value: 1}`

`batchHint = weight`. At default weight=1:
- Each Pick+Claim cycle claims **1 delivery**
- A whale with 200k pending deliveries requires **200k Pick→Claim→Notify cycles**
- Each cycle: scheduler lock + claim tx (3-5 statements) + HTTP + mark tx (3 statements)

### Fix

Set a minimum base batch size independent of weight:

```go
const minBatchHint = 25
hint := max(weight, minBatchHint)
```

Or make `batchHint` backlog-aware: query `SELECT count(*) WHERE customer_id=$1 AND status IN ('pending','failed_retry')` and use `min(count, maxBatch)`.

---

## 7. Process-Local Scheduling Across N Instances

### Problem

Each process maintains its own in-memory `activeSet`. With 100 instances:
- Same customer is independently active on **all 100 schedulers**
- All 100 may Pick the same customer and attempt ClaimNextN — only one gets rows (SKIP LOCKED), 99 waste a PG round-trip
- Different instances have different subsets of active customers → no fleet-wide fairness guarantee
- **Effective weight per customer** becomes proportional to `weight(customer) × instances_that_know_about(customer)`, not just weight

### Impact

- Up to 99% wasted claim attempts on hot customers
- Small customers discovered by fewer instances get proportionally less fleet capacity
- Connection pool pressure grows linearly with instances

### Fix

1. **Customer leasing via Postgres** (`customer_work` table with per-customer lease):
   - Workers claim a customer (SELECT FOR UPDATE SKIP LOCKED on `customer_work`)
   - Sole rights to claim deliveries for that customer for lease duration
   - Eliminates duplicate claims, enables fleet-wide fairness
2. **Consistent hashing**: route customer IDs to specific instances

---

## 8. Idle Polling

### Problem

**File**: `internal/worker/worker.go` line 175 — `idleBackoff = 50ms`

With 100 workers per instance, 100 instances:
- 10,000 goroutines calling `Pick()` at 20/sec each → **200,000 Pick calls/sec** system-wide
- Each call acquires `sync.Mutex` on the scheduler
- When scheduler is empty: no PG traffic, but CPU is wasted
- When scheduler is non-empty but stale (claim returns 0): tight PG polling with no backoff

### Fix

- `sync.Cond` or channel-based blocking instead of poll
- Cooldown per customer after empty claim (negative cache, skip for 1-5s)

---

## 9. Ingest Fan-Out

### Problem

**File**: `internal/ingest/ingest.go` lines 64-73

Single event can fan out to N deliveries (one per matching webhook). For a whale with 20 webhooks:
- 1 event → 20 deliveries in one transaction
- All get `next_attempt_at = now()` → tie density increases
- Large burst at claim time

Not a bottleneck at current scale estimates but worth monitoring. If fanout grows to 1000+, the transaction duration becomes noticeable.

---

## 10. Host Limiter Contention

### Problem

**File**: `internal/worker/host_limiter.go` lines 49-56

Global `sync.Mutex` protecting a `map[string]*semaphore.Weighted`:
- `semFor` holds lock for entire map lookup/insert
- Per-host semaphore `Acquire`/`Release` happens **outside** the lock

**Verdict**: Not a bottleneck at expected scale. The lock hold time is microseconds. At 50M deliveries/hour spread across many hosts, contention is negligible.

If it ever shows in profiles: switch to `sync.RWMutex` or `sync.Map`.

---

## 11. MVCC Bloat & Vacuum

At 50M deliveries/hour:
- Each delivery row is inserted, then updated 2-3 times (claim + succeed/retry/dead)
- At steady state: ~100M-150M row updates/hour
- Dead tuples accumulate on the `deliveries` table and its indexes
- B-tree page splits/contention on hot index pages (same customer_id ranges)

### Mitigation

- Aggressive autovacuum tuning for the deliveries table
- Partition deliveries by customer_id hash or creation date
- Archive succeeded/dead rows out of the hot table

---

## 12. Connection Pool Pressure

At 14k delivery/s with 100 instances:
- Each instance needs 5-10 PG connections → 500-1000 total connections
- Without a pooler (pgbouncer): PG max_connections needs to be very high
- With a pooler: pool becomes a contention point

---

## Action Plan (Prioritized)

| Priority | Change | Impact | Effort | Key File(s) |
|----------|--------|--------|--------|-------------|
| P0 | Collapse claim into 1 CTE statement | 3-4x throughput gain | Medium | `internal/outbox/outbox.go` |
| P0 | Add partial indexes for ready + in_flight | Fixes OR scan → index-only query | Small | `migrations/` |
| P0 | Set batchHint ≥ 25 (not 1) | 25x fewer Pick/Claim cycles | Small (config) | `internal/scheduler/scheduler.go` |
| P1 | Add ingest→Scheduler.Notify bridge | Fixes unbounded latency bug | Small | `internal/ingest/ingest.go` |
| P1 | DB-backed active-customer discovery/leasing | Enables cross-instance fairness | Large | New table + `internal/scheduler/` |
| P1 | Replace SWRR with O(1) deficit round robin | No per-pick sort at 10k+ active customers | Medium | `internal/scheduler/swrr/` |
| P2 | Remove `SELECT ... FOR UPDATE` in mark path | Fewer round-trips per delivery | Medium | `internal/outbox/outbox.go` |
| P2 | Cooldown/backoff on empty claim results | Prevents tight PG polling | Small | `internal/worker/worker.go` |
| P2 | Partition deliveries by customer hash | Spreads write + vacuum load | Medium | `migrations/` |
| P3 | Replace UUID PK with monotonic key | Better index locality | Medium | `migrations/` |
| P3 | `sync.Cond` instead of 50ms poll for idle workers | Reduces wasteful Pick calls | Medium | `internal/worker/worker.go` |

---

## Appendix: Predictive Volume Model

Based on Flodesk customer profile (email marketing SaaS):

| Metric | Normal | Peak (bulk import) |
|--------|--------|-------------------|
| Events/sec | 1,000-5,000 | 10,000-50,000 |
| Deliveries/sec (after fan-out) | 2,000-15,000 | 20,000-150,000 |
| Deliveries/hour | 5-50 million | 50-500 million |
| Active customers with pending work | 100-1,000 | 5,000-20,000 |
| Concurrent in-flight HTTP POSTs | 50-300 | 500-5,000 |
| PG tx/sec (success path) | 4,000-30,000 | 40,000-300,000 |
| Dead letters/hour (1% failure) | 50,000 | 500,000 |
| Storage write rate (raw) | ~30 GB/hour | ~300 GB/hour |
| Instances needed (current design) | 10-30 | 100+ (diminishing returns) |
| Instances needed (with P0 fixes) | 3-10 | 30-50 |

---

## Sources

- `internal/ingest/ingest.go` — Event ingest + fan-out
- `internal/outbox/outbox.go` — Claim/mark SQL (claim tx lines 106-136, selectClaimableDeliveryIDs 141-153, mark tx 256-300)
- `internal/worker/worker.go` — Worker Run loop (line 175: idleBackoff, lines 185-235: Pick→Claim→Notify)
- `internal/worker/host_limiter.go` — Per-host concurrency cap (lines 49-56: semFor mutex)
- `internal/scheduler/swrr/swrr.go` — SWRR implementation (lines 69-121: Pick with sort)
- `internal/scheduler/scheduler.go` — Scheduler interface + ConstantWeights (lines 35-49)
- `migrations/000001_persistence.up.sql` — Schema + index (lines 53-54: current index)
- `docs/DESIGN.md` — Architecture design
- `docs/adr/0002-postgres-as-queue-no-broker-for-poc.md` — Postgres-as-queue rationale
