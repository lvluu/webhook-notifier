# Issue #54 Throughput Hardening Checklist

This checklist validates sustained-load queue health after applying tuning for
throughput degradation and dead-tuple bloat on `deliveries`.

## Scope

Use this during long-running stress tests (for example scenario 6 or production-like
mixed ingest) where load is sustained for at least 15-30 minutes.

## Preconditions

- Migrations applied, including autovacuum and claim/retention index migrations.
- Notifier replicas and worker counts set to the intended stress shape.
- Retention command cadence configured (recommended start: every `1m`).

## Pass/Fail Gates

A run passes only if all gates pass.

1. Dead-tuple ratio on `deliveries` stays bounded
- Gate: `dead_pct <= 35%` at steady-state windows.
- Gate: no monotonic rise over 3 consecutive samples at 1-minute cadence.

2. Backlog does not run away
- Gate: `pending_retry_total` plateaus or declines after warmup.
- Gate: `oldest_pending_age_sec` does not grow unbounded after warmup.

3. Claim path remains healthy
- Gate: p95 `webhook_claim_duration_seconds` stays within your target SLO.
- Gate: `webhook_claim_empty_total` growth is not paired with rising backlog.

4. Fairness remains intact
- Gate: small-customer p95 first-attempt latency remains within agreed ratio
  versus whale p95 (use scenario 6 fairness threshold).

5. Retention does not collapse throughput
- Gate: while retention is active, end-to-end throughput regression is within
  agreed tolerance (recommended initial guardrail: <= 10%).

## Sampling Cadence

- During run: every 60s, execute `bench/scripts/issue54_health.sql`.
- After run: capture a final snapshot and compare trend direction.

## SQL Probes

Run:

```bash
psql "$DATABASE_URL" -f bench/scripts/issue54_health.sql
```

The script reports:
- `deliveries` dead/live tuple counts and dead percentage
- pending + retry backlog and oldest pending age
- recent autovacuum timing
- optional terminal-row pressure (succeeded/dead counts)

## Suggested Operating Sequence

1. Start load and allow warmup (3-5 minutes).
2. Collect 15-30 minutes of 60s health samples.
3. Run retention at target cadence during the same window.
4. Compare throughput/fairness against baseline run without retention.
5. Record evidence in `bench/results/` markdown for review.

## If Gates Fail

Apply changes in this order:

1. Reduce retention `--batch-size` and keep `--enrichment=deferred`.
2. Increase retention frequency (smaller, more frequent archive passes).
3. Re-check `deliveries` autovacuum table settings and DB-level autovacuum worker pressure.
4. Re-tune worker count and `MinBatchHint` to reduce claim/query contention.

## Evidence Template

For each run, record:

- Commit SHA and migration set.
- Load shape (replicas, workers/replica, profile/scenario).
- Retention config (`retention`, `batch-size`, `enrichment`, cadence).
- Throughput result (baseline vs retention-on).
- Dead tuple trend summary.
- Backlog trend summary.
- Fairness summary (small p95 vs whale p95).
