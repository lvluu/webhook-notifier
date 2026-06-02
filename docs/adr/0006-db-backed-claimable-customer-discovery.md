# DB-backed claimable Customer discovery for horizontal fairness

## Context

The Scheduler is intentionally process-local. ADR-0001 commits to Smooth
Weighted Round Robin over Customers with work registered in the Scheduler, with
`WeightSource` preserving future controls such as paid priority tiers and
`weight=0` operational pauses.
ADR-0002 commits to Postgres as the durable queue and to per-Customer Delivery
claims using `SELECT ... FOR UPDATE SKIP LOCKED`.

That design is duplicate-safe across worker instances, but it is not fully fair
across a horizontally scaled cluster unless every process can discover the same
set of claimable Customers. Today, ingest calls `Notify(customerID)`
only in the local process. A different process can have idle workers and still
miss a claimable Customer until another local event wakes it.

Issue #22 adds DB-backed claimable Customer discovery so each process periodically
seeds its local Scheduler from Postgres.

For this decision, a **claimable Customer** means a Customer with at least one
claimable Delivery: due `pending`/`failed_retry`, or an expired claim lease in
`delivery_claims`.
A Customer with only future `failed_retry` Deliveries is not claimable yet.

An alternative raised on #22 is a Hatchet-style ID-block claim: allocate
contiguous ID ranges per Customer and make fairness emerge from one SQL claim
query instead of a process-local Scheduler.

## Decision

Use a **DB-backed discovery loop feeding the local Scheduler**.

Each notifier process runs one discovery goroutine. It periodically queries
Postgres for distinct claimable Customers and calls
`Scheduler.Notify(customerID)` for each result. Discovery is a wake-up signal,
not authoritative membership reconciliation: it adds visible Customers to the
local Scheduler, but does not remove Customers from Scheduler state. Empty
claims naturally decay stale Scheduler entries because workers do not re-notify
when `ClaimNextN` returns no Deliveries. Worker loops stay unchanged:

1. `Scheduler.Pick(ctx)` chooses a Customer and returns a `batchHint`.
2. The worker claims up to `batchHint` claimable Deliveries for that Customer.
3. Delivery processing remains guarded by `FOR UPDATE SKIP LOCKED`.
4. Successful claim/process paths may still re-notify the Customer while work
   remains.

The discovery query must include all rows that the claim query can legally
claim:

```sql
SELECT DISTINCT customer_id
FROM deliveries
WHERE customer_id > $1
  AND (
    (status IN ('pending', 'failed_retry') AND next_attempt_at <= now())
    OR EXISTS (
      SELECT 1
      FROM delivery_claims dc
      WHERE dc.delivery_id = deliveries.id
        AND dc.claimed_at < now() - $2::interval
    )
  )
ORDER BY customer_id
LIMIT $3
```

Due-time and lease-expiry checks use database time, matching the claim query.
Discovery must not decide eligibility with the application host clock.
Stale claimed Deliveries are claimable solely by expired claim lease; they do not
also require `next_attempt_at` to be due. If claim eligibility changes, discovery
eligibility changes with it.

The loop respects `context.Context`, uses jittered polling, and backs off when
no Customers are found. Discovery is one loop per process, not one loop per
worker goroutine.

Discovery may return a bounded batch of claimable Customers per pass. Bounded
discovery must be paired with cursor/wraparound scanning: a fixed
`ORDER BY customer_id LIMIT N` from the beginning is not sufficient because the
first page can remain claimable forever and starve later-sorted Customers from
discovery. The design requires eventual convergence over repeated passes, not
full claimable-set enumeration on every tick. Discovery order is not the fairness
order.

For Customers that become claimable behind a process's current cursor, worst-case
discovery latency in that process is one full cursor cycle, not one polling
interval. Bench #12 should validate that this bound is acceptable for
late-arriving Small Customers.

The discovery cursor is per-process local state. It is not shared or persisted
in Postgres, because each process must seed its own local Scheduler.

When a pass returns no rows after the current cursor, reset the cursor to the
beginning and wait until the next tick rather than issuing an immediate second
query. This keeps the loop to one discovery query per tick; reactivity is bounded
by one cursor cycle plus one polling interval.

Package boundaries follow the existing ownership lines:

- `internal/outbox` owns the SQL query for claimable Customers because it must
  match Delivery claim and reclaim semantics.
- Worker/runtime wiring owns polling, backoff, context cancellation, and calls
  to `Scheduler.Notify`.
- Service/runtime code starts the discovery loop separately from `Worker.Run`.
  Discovery is a cluster-runtime concern; `Worker.Run` remains the claim/process
  loop and can still be driven by manual `Notify` in focused tests.
- `internal/scheduler` remains storage-free policy/state. It should not learn
  about Postgres.

Discovery finds Customers with claimable Deliveries. The Scheduler and its
`WeightSource` decide whether a discovered Customer is pickable. For example,
`weight=0` pause semantics belong to Scheduler policy, not to the outbox
discovery query. An implementation may skip zero-weight notifications if it
already has cheap access to the same `WeightSource`, but that is an
optimization, not correctness.

Discovery gives **bounded cluster-wide reactivity**, not a globally fair total
order. Fairness remains local per process; cluster behavior is approximate and
improves as every process converges on the same claimable Customer set. Postgres
claim locking prevents duplicate Delivery processing; discovery bounds stale
local views so claimable Customers are eventually visible to every process.

Postgres is the consistency boundary. If a worker cannot reach Postgres, it
cannot safely discover or claim work and should stop processing rather than
guess from local Scheduler state. A stale local Scheduler can degrade fairness
or reactivity, but it must not compromise Delivery ownership correctness.
Issue #22 does not add a Scheduler reset API for database outages. Worker/runtime
code should back off or restart on DB errors, then let discovery repopulate
local Scheduler state after Postgres recovers.

It also does not require changing `Worker.Run` fatal-error semantics; broader DB
reconnect policy is a separate runtime concern.

## Considered options

- **Local ingest `Notify` only.** Already implemented as part of the registry
  refactor. Rejected as sufficient cluster behavior: it only wakes the process
  that performed ingest and does not help another process discover retry-due or
  stale claimed Deliveries.
- **DB-backed discovery loop feeding local SWRR** *(chosen)*. Preserves
  ADR-0001's Scheduler and `WeightSource` semantics. Works with the current
  UUID primary keys and existing per-Customer claim path. Adds a small polling
  cost and one discovery integration point.
- **Postgres `LISTEN/NOTIFY` only.** Low latency for newly inserted work, but
  notifications are ephemeral and do not cover missed events, process restarts,
  retry-due Deliveries, or stale claimed recovery by themselves. Useful later
  as a wake-up optimization layered on top of polling, not as the source of
  truth. Issue #22 implements polling only.
- **Materialized `claimable_customers` / `customer_work` table.** Makes discovery
  cheap and explicit, but adds write-path coupling and correctness work around
  transitions to and from zero claimable Deliveries. Deferred until profiling shows
  `SELECT DISTINCT customer_id` is too expensive.
- **Hatchet-style ID-block claim.** Allocate contiguous Delivery ID blocks per
  Customer and claim from a bounded global ID window so one Whale Customer cannot
  occupy the whole batch. Rejected for this PoC path because it trades away the
  Scheduler abstraction and weight controls, requires schema/write-path changes,
  and is awkward with current UUID Delivery IDs. Keep as a future alternative if
  local-Scheduler discovery proves operationally expensive.
- **Redis Streams or broker-backed per-Customer queues.** Production-shaped
  scale-out path already allowed by ADR-0002. Rejected for this PoC because it
  adds another infrastructure dependency before the Postgres design has been
  benchmarked.

## Why not ID-block claim now?

The ID-block approach has an attractive property: workers can be almost
stateless, and fairness is encoded in SQL rather than maintained in local
Scheduler state. It is still not the right primary path for this codebase.

First, `WeightSource` is load-bearing. SWRR uses Customer weights for equal
share today, future plan tiers, and `weight=0` operational pauses. ID-block
claim can approximate equal share, but priority tiers and pause semantics become
additional SQL rules outside the Scheduler contract.

Second, the current schema uses UUID Delivery IDs. Contiguous per-Customer
blocks require a new sortable allocation key, a task-group registry, or a change
away from UUID primary-key ordering. That would touch migrations, ingest, claim
queries, and tests before benchmark evidence justifies it.

Third, the existing claim path is already per-Customer:

```sql
WHERE customer_id = $1
  AND (
    (status IN ('pending', 'failed_retry') AND next_attempt_at <= now())
    OR EXISTS (
      SELECT 1
      FROM delivery_claims dc
      WHERE dc.delivery_id = deliveries.id
        AND dc.claimed_at < now() - $lease
    )
  )
ORDER BY next_attempt_at, created_at, id
FOR UPDATE SKIP LOCKED
LIMIT $batchHint
```

Under many claimable Customers, workers naturally spread across different
`customer_id` index ranges. A global ID-window claim concentrates workers around
the same leading ID range and relies on `SKIP LOCKED` to skip rows already
touched by peers. That may be acceptable for some queue designs, but it is a
less direct fit for a fairness model whose primary key is Customer.

## Indexing notes

Discovery should be backed by indexes that match ready-row predicates. The
existing claim index on `(customer_id, status, next_attempt_at)` helps
per-Customer claims but is not ideal for global discovery.

Issue #22 should add narrowly scoped partial indexes for discovery unless
`EXPLAIN` against representative data shows the existing indexes are already
sufficient. This is a polling query in every process; benchmark runs should not
pay for an avoidable table scan.

```sql
CREATE INDEX IF NOT EXISTS idx_deliveries_ready_customers
ON deliveries (customer_id, next_attempt_at)
WHERE status IN ('pending', 'failed_retry');

CREATE INDEX IF NOT EXISTS idx_delivery_claims_stale_by_claimed_at
ON delivery_claims (claimed_at, delivery_id);
```

The exact index shape should be validated with `EXPLAIN` against benchmark-sized
data. Do not rely on planner-specific skip-scan behavior as the design
contract. Project migrations may omit `CONCURRENTLY`; production rollout can use
concurrent index creation if the table is already large.

## Consequences

- The `Scheduler` interface remains the load-bearing fairness abstraction.
- Discovery is not a second scheduler. It wakes local Scheduler state from
  shared storage and relies on existing claim/process/re-notify behavior to keep
  Customers registered while claimable work remains.
- Cross-instance reactivity is bounded by discovery interval and polling jitter.
  Cross-instance fairness is approximate and affected by local Scheduler state,
  worker count, and claim batch size; it is not a globally serialized order.
- Delivery processing remains duplicate-safe through Postgres row locking.
- The design favours correctness over availability for the queue/claim path:
  without Postgres, workers do not make progress.
- Retry-due and stale claimed Deliveries become discoverable even when no new
  ingest happens locally.
- Polling introduces a small, tunable DB read cost per process. Backoff and
  jitter are required to avoid synchronized empty scans.
- Discovery should have a configurable per-pass limit so a very large claimable
  Customer set cannot swamp one process with unbounded `Notify` calls.
- A bounded discovery loop must maintain a cursor and wrap to the beginning when
  it reaches the end of the claimable Customer keyspace.
- The cursor is local to each process; sharing it would partition discovery
  across processes instead of converging every local Scheduler.
- Empty pages reset the local cursor and wrap on the next tick, keeping discovery
  to one bounded query per polling interval.
- Late-arrival reactivity is bounded by the cursor cycle. If that is too slow in
  benchmarks, layer `LISTEN/NOTIFY` or a materialized claimable Customer table on
  top of polling.
- The implementation is isolated: one discovery component, optional migration
  indexes, worker wiring, and integration tests with multiple independent
  scheduler/worker loops sharing one Postgres outbox.
- Integration tests should prove two independent Scheduler/Worker loops can
  discover shared Postgres work, make progress for at least two Customers, avoid
  duplicate `delivery_id`/`attempt_number` processing, and shut down cleanly via
  context cancellation. They should not assert a strict globally alternating
  Customer order.
- If benchmark results show discovery overhead or fairness drift is unacceptable,
  revisit either a materialized claimable Customer table or the ID-block claim
  alternative as a separate ADR.
- `LISTEN/NOTIFY` is layered on top of periodic DB discovery as an advisory
  fast path. Successful ingest wakes its local Scheduler synchronously and
  enqueues a bounded remote wake hint. Hints are coalesced per Customer for
  100ms and sent through a dedicated direct Postgres connection so they do not
  contend with PgBouncer sessions used by queue traffic. Dropped or failed hints
  are safe because periodic DB discovery remains the recovery path.
