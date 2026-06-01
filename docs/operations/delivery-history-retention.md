# Delivery History Retention Tuning

This runbook captures the issue #45 follow-up checks for archive query shape,
cache pressure, and vacuum behavior under sustained terminal-row churn.

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

Capture before/after evidence with representative terminal-row volume.

```sql
EXPLAIN (ANALYZE, BUFFERS)
WITH terminal_candidates AS (
    SELECT d.id AS delivery_id, COALESCE(dl.final_at, d.updated_at) AS final_at
    FROM deliveries d
    LEFT JOIN dead_letters dl ON dl.delivery_id = d.id
    WHERE d.status IN ('succeeded', 'dead')
      AND COALESCE(dl.final_at, d.updated_at) < now() - interval '24 hours'
    ORDER BY COALESCE(dl.final_at, d.updated_at), d.id
    LIMIT 500
    FOR UPDATE OF d SKIP LOCKED
)
SELECT count(*) FROM terminal_candidates;
```

Track:
- planning/execution time
- shared hit/read buffers
- whether candidate selection uses terminal index path vs broad sort/scan

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

## Operational Note

The retention command is maintenance, not worker-runtime logic. If retention is
paused, queue correctness remains intact, but query latency and storage pressure
can degrade as terminal rows accumulate.
