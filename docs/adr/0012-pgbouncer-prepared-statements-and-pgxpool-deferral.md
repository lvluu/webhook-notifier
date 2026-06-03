# PgBouncer prepared statements and pgxpool deferral

## Context

The notifier hot path is dominated by repeated Postgres CTEs: Delivery claim,
batch mark, and circuit breaker state updates. With pgx forced into simple
protocol, every execution is sent as text and Postgres must parse and plan the
statement every time.

That simple-protocol override was originally chosen because PgBouncer was
running in transaction pooling mode. Older PgBouncer versions could not safely
support protocol-level prepared statements in that mode.

The local stack uses the `edoburu/pgbouncer` image. As of June 3, 2026,
Docker Hub lists `v1.25.1-p0` as the current tag for that image, which is newer
than PgBouncer 1.21. PgBouncer 1.21 introduced protocol-level prepared statement
support for transaction pooling when `max_prepared_statements` is set to a
non-zero value.

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

## Follow-up benchmark

Run the chaos profile after this change and inspect:

- notifier logs for prepared statement errors
- PgBouncer logs for parse/bind/close protocol problems
- `pg_stat_statements` mean and p95 time for claim and batch mark queries
- end-to-end claim and mark latency under the mixed workload
