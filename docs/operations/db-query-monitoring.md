# DB Query Monitoring Runbook

This runbook defines how to monitor query-level Postgres health for notifier
claim/discovery/retention paths under sustained load.

## Quick Start

Run every 60s during load tests:

```bash
psql "$DATABASE_URL" -f bench/scripts/db_query_health.sql
```

Pair with app-level Prometheus metrics:
- `webhook_claim_duration_seconds`
- `webhook_claim_batch_size`
- `webhook_claim_empty_total`
- `webhook_pg_pool_*`

## What To Watch

1. Queue bloat and backlog pressure
- `dead_pct` for `deliveries`
- `pending_retry_total`
- `oldest_pending_age_sec`

2. DB wait/lock pressure
- `pg_stat_activity` wait-event distribution
- lock counts on `deliveries` and `delivery_claims`

3. Query hotspots (`pg_stat_statements`)
- top SQL by `total_exec_time`
- queue-path query `mean_exec_time`
- `rows_per_call` drift
- `shared_blks_read` growth (cache misses)

## Suggested Alert Thresholds

Use as starting points; tune from environment data.

1. Bloat drift
- Warn: `deliveries.dead_pct > 25%` for 10 minutes
- Critical: `deliveries.dead_pct > 40%` for 10 minutes

2. Backlog drift
- Warn: `oldest_pending_age_sec` rising for 3 consecutive samples
- Critical: monotonic rise plus decreasing succeeded throughput

3. Claim-path degradation
- Warn: p95 claim duration +50% versus baseline
- Critical: p95 claim duration +100% versus baseline

4. Pool saturation
- Warn: `webhook_pg_pool_in_use / webhook_pg_pool_max_open > 0.9` for 5 minutes
- Critical: `= 1.0` with increasing `wait_count`/`wait_duration`

5. Empty-claim churn
- Warn: sustained `webhook_claim_empty_total` growth without throughput gain
- Critical: high empty-claim rate plus backlog growth

## Triage Order

1. Confirm ingest vs succeed rate gap.
2. Check pool saturation and wait events.
3. Check queue bloat and autovacuum recency.
4. Inspect top queue SQL in `pg_stat_statements`.
5. Tune in order:
- retention batch/cadence
- worker count and `min-batch`
- discovery cadence or notify/wakeup optimization
- DB/PgBouncer capacity

## pg_stat_statements Setup

`db_query_health.sql` automatically reports if `pg_stat_statements` is missing.

If needed:

1. Enable in Postgres config:
- `shared_preload_libraries = 'pg_stat_statements'`

2. Create extension:

```sql
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
```

3. Reset stats before controlled A/B windows (optional):

```sql
SELECT pg_stat_statements_reset();
```
