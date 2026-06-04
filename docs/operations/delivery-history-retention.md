# Delivery History Retention Tuning

This runbook captures archive query shape, cache pressure, and vacuum behavior
under sustained terminal-row churn.

## Recommended Starting Defaults

- Command: `go run ./cmd/delivery-history-retention`
- Retention window: `--retention=24h`
- Batch size: `--batch-size=500`
- Enrichment mode: `--enrichment=deferred` for high-throughput runs, `full` for
  low-throughput/manual runs where full attempt-summary is needed immediately.
- Schedule: every `1m` (cron or Kubernetes CronJob) and tune from evidence.

Rationale:
- `500` keeps each transaction bounded while still amortizing overhead.
- `deferred` removes lateral attempt-summary aggregation from the critical path,
  reducing shared-buffer churn in worker-heavy environments.
- `1m` cadence reduces bursty delete/vacuum pressure compared with infrequent
  very large archive sweeps.

## Query Evidence: EXPLAIN ANALYZE + BUFFERS

Measured on `2026-06-01` against a seeded local dataset:

- `customers`: 200
- `events`: 5,000
- terminal `deliveries`: 5,000 (`succeeded`/`dead`)
- `dead_letters`: 2,000
- `delivery_attempts`: 12,502

Candidate selection (`LIMIT 500`) comparison:

| Query shape | Execution time | Shared buffers hit |
| --- | ---: | ---: |
| Old join-heavy candidates CTE | `80.667 ms` | `16,116` |
| New candidate-first CTE | `6.275 ms` | `604` |

This evidence shows the largest gain comes from narrowing the lock/selection
path before expensive enrichment joins.

Repro query template:

```sql
EXPLAIN (ANALYZE, BUFFERS)
WITH terminal_candidates AS (
    SELECT d.id AS delivery_id, COALESCE(dl.final_at, d.updated_at) AS final_at
    FROM deliveries d
    LEFT JOIN dead_letters dl ON dl.delivery_id = d.id
    WHERE d.status IN ('succeeded', 'dead')
      AND d.updated_at < now() - interval '24 hours'
    ORDER BY d.updated_at, d.id
    LIMIT 500
    FOR UPDATE OF d SKIP LOCKED
)
SELECT count(*) FROM terminal_candidates;
```

Operational caveat:
- `--enrichment=deferred` removes the lateral `delivery_attempts` aggregation
  from the critical path. It still joins `events` and `webhooks` during insert.
  If cache pressure remains material, split archive into two phases:
  minimal copy first, asynchronous enrichment second.

## Vacuum / Bloat Validation

Check live queue churn and autovacuum cadence:

```sql
SELECT relname, n_live_tup, n_dead_tup, vacuum_count, autovacuum_count
FROM pg_stat_user_tables
WHERE relname IN ('deliveries', 'delivery_attempts', 'dead_letters');
```

```sql
SELECT
    now() AS observed_at,
    schemaname,
    relname,
    last_autovacuum,
    n_dead_tup
FROM pg_stat_user_tables
WHERE relname = 'deliveries';
```

If `n_dead_tup` trends upward without regular autovacuum recovery:
- reduce retention `--batch-size`
- increase archive frequency
- revisit `deliveries` autovacuum knobs in migrations

Observed after one deferred retention pass (`archived 4996 deliveries`):

| Table | `n_live_tup` | `n_dead_tup` | `autovacuum_count` |
| --- | ---: | ---: | ---: |
| `deliveries` | 4 | 4,996 | 14 |
| `delivery_attempts` | 8 | 12,494 | 10 |
| `dead_letters` | 4 | 1,996 | 1 |

Interpretation:
- delete-heavy archival still creates substantial dead tuples immediately
  after each pass.
- aggressive autovacuum is active and must remain part of retention operations.

## Operational Note

The retention command is maintenance, not worker-runtime logic. If retention is
paused, queue correctness remains intact, but query latency and storage pressure
can degrade as terminal rows accumulate.
