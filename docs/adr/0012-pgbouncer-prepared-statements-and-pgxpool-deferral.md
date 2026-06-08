# PgBouncer prepared statements and pgxpool deferral

Status: Accepted.

## Context

The notifier hot path is dominated by repeated Postgres CTEs: Delivery claim,
batch mark, and circuit breaker state updates. With pgx forced into simple
protocol, every execution is sent as text and Postgres must parse and plan the
statement every time.

That simple-protocol override was originally chosen because PgBouncer was
running in transaction pooling mode. Older PgBouncer versions could not safely
support protocol-level prepared statements in that mode.

The local stack pins `edoburu/pgbouncer:v1.25.1-p0`, which is newer than
PgBouncer 1.21. PgBouncer 1.21 introduced protocol-level prepared statement
support for transaction pooling when `max_prepared_statements` is non-zero.

## Decision

Pin the local PgBouncer image to `edoburu/pgbouncer:v1.25.1-p0` and configure it
through Docker Compose environment variables only:

- `POOL_MODE=transaction`
- `DEFAULT_POOL_SIZE=80`
- `MAX_PREPARED_STATEMENTS=200`
- `MAX_CLIENT_CONN=400`

Remove the pgx simple-protocol override from service commands that use the
pooler. The pgx default cached-statement execution mode can now be used with the
PgBouncer protocol-level prepared statement tracker.

Do not migrate `internal/outbox`, `internal/ingest`, or `internal/registry` from
`database/sql` to native `pgxpool` in this change. That migration would touch
most persistence boundaries and should be justified by a benchmark that shows a
specific win from `pgx.Batch`, `SendBatch`, or `CopyFrom`.

Do not replace Event ingest fanout with `CopyFrom` in this change. The current
multi-row `INSERT ... VALUES (...)` fanout is correct, bounded by the expected
Webhook count per Customer, and keeps the `database/sql` store API intact.

## Multi-customer claim shaping

Re-enabling cached prepared statements stabilizes the hot-path query plans, so
this change also reshapes `ClaimNextBatch` while the plan cache is in scope:

- De-duplicate the input Customer IDs (`SELECT DISTINCT ... FROM unnest($1)`).
  Without it a repeated Customer ID multiplied that Customer's lock and scan
  work and skewed the batch toward it.
- Probe each distinct Customer independently with a `CROSS JOIN LATERAL`, instead
  of one `customer_id = ANY($1)` scan, so the per-Customer claim index is used
  directly and a sparse Customer is not buried behind a high-volume Customer in
  a single global scan.
- Cap each Customer's candidate contribution to a fair share of the batch limit
  (`limit / number of input Customers`, floored at 1) before the final
  time-ordered `LIMIT`. The cap bounds rows locked `FOR UPDATE`: without it `N`
  co-claimed Customers could each lock up to the full batch limit (`N × limit`
  rows) only to discard most at the final cut, amplifying lock contention and
  letting one whale monopolize the batch.

Align the lock-acquisition order of `withInFlightDelivery` (used by `Requeue`
and breaker state writes) with the claim path. Both now lock the `deliveries`
row before the `delivery_claims` row. The previous `FOR UPDATE OF dc, d` over a
`delivery_claims`-first join acquired locks in the opposite order to the claim
path and deadlocked (`SQLSTATE 40P01`) when claim and requeue ran concurrently
on overlapping rows.

## Consequences

- Hot CTEs can reuse prepared plans through PgBouncer instead of paying parse and
  plan cost on every execution.
- The pooler configuration has one source of truth: Compose environment
  variables. The stale, unmounted `compose/pgbouncer/pgbouncer.ini` and
  `userlist.txt` files are removed to avoid implying different effective pool
  sizes.
- The notifier's `-max-conns` setting should stay at or below the PgBouncer
  `DEFAULT_POOL_SIZE` for the local single-notifier stack. With the current
  Compose defaults, notifier opens 30 client connections and PgBouncer can keep
  up to 80 server connections for the database, leaving headroom for retention
  and admin queries.
- A future native `pgxpool` migration remains available when benchmarks show
  that one-round-trip batching or bulk ingest fanout is worth the larger
  refactor.
- A high-volume Customer can no longer monopolize a multi-customer claim batch
  or lock the whole candidate pool; the per-Customer fair-share cap keeps a
  co-picked Small Customer claimable within the same batch. Per-batch fairness
  remains best-effort (the contract has no per-Customer guarantee) and the
  scheduler (ADR-0001) still owns cross-batch fairness.
- The requeue/breaker `SQLSTATE 40P01` deadlock is resolved by the shared
  `deliveries`-then-`delivery_claims` lock order; this is now a documented
  invariant (see DESIGN.md → Invariants → Lock ordering) and is covered by a
  concurrent claim/requeue regression test.

## Follow-up benchmark

Run the chaos profile after this change and inspect:

- notifier logs for prepared statement errors
- PgBouncer logs for parse/bind/close protocol problems
- `pg_stat_statements` mean and p95 time for claim and batch mark queries
- end-to-end claim and mark latency under the mixed workload
