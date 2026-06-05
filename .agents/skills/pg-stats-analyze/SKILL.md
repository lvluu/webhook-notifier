---
name: pg-stats-analyze
description: Run the full webhook-notifier stack under load, verify replica health, then mine Postgres stats (pg_stat_statements, deadlock logs, table bloat, index scans) to identify the current bottleneck. Use when asked to find performance bottlenecks, diagnose deadlocks/timeouts/contention, validate a perf fix live, or "look at pg stats".
---

# Postgres Stats Bottleneck Analysis

Empirical bottleneck hunting for the `webhook-notifier` stack: bring the system
up under realistic chaos load, confirm it is healthy, then read what Postgres
actually spent its time on — never guess. Every conclusion must cite a stat.

## When to use

- "find the bottleneck", "look at pg stats", "why is it slow / deadlocking".
- Validating a performance/resilience fix against a real running stack (not just
  unit tests) — measure before vs after with the same load and window.
- Characterising deadlocks (40P01), statement timeouts (57014), or hot-row
  contention.

## Hard-won gotchas (read first)

- **`rtk` compresses `docker logs` output and truncates lines.** For raw Postgres
  logs (deadlock DETAIL, full statements) you MUST bypass it:
  `rtk proxy docker logs <container>`. Dumping to a file via plain `docker logs`
  is also filtered — use `rtk proxy`.
- **Deadlock relations are reported by OID.** Resolve with
  `psql -tAc "SELECT 16468::regclass;"`. Don't assume which table.
- **Container names** are `flo-home-postgres-1`, `flo-home-notifier-1..N`,
  `flo-home-pgbouncer-1`, `flo-home-webhook-receiver-1`. Find with `docker ps`.
- **DB creds** are `notifier:notifier` (db `notifier`). pg_stat_statements is
  already preloaded (see compose). `statement_timeout=30s` — slow queries surface
  as SQLSTATE 57014, not hangs.
- **A fresh measurement needs a fresh DB.** `down -v` wipes `pgdata` so backlog
  and stats start clean; otherwise old backlog/stats skew the comparison.
- **Rebuild the image before measuring a code change** — `up`/`stack-all` will NOT
  pick up new code without `build`.

## Procedure

### 1. (If validating a code change) rebuild the image

```bash
docker compose -f compose/docker-compose.yml --project-directory . build notifier
```

### 2. Bring up the full stack under load

```bash
# fresh DB for a clean measurement
docker compose -f compose/docker-compose.yml --project-directory . \
  --profile observe --profile chaos --profile retention down -v --remove-orphans
just stack-all 3        # 3 notifier replicas + chaos (whales + small) + retention
```

### 3. Verify health (do not analyze an unhealthy/crashing stack)

```bash
docker ps --filter name=notifier --format '{{.Names}} {{.Status}}'   # want all (healthy)
docker ps -a --filter name=notifier --format '{{.Names}} {{.Status}}' # catch Exited/Restarting
# fatal exits in app logs (should be 0):
for c in flo-home-notifier-1 flo-home-notifier-2 flo-home-notifier-3; do \
  docker logs --tail 50 "$c" 2>&1 | grep -c "delivery runtime error\|shutdown complete"; done
```
Let it run a few minutes — bottlenecks emerge under sustained load. Sample
health on a cadence (every ~40s) across the window you care about.

### 4. Rank query time — the primary signal

```bash
docker exec -e PGPASSWORD=notifier flo-home-postgres-1 psql -U notifier -d notifier -P pager=off -c "
  SELECT round(total_exec_time::numeric,0) AS total_ms,
         calls,
         round(mean_exec_time::numeric,2) AS mean_ms,
         round(100*total_exec_time/sum(total_exec_time) OVER (),1) AS pct,
         left(regexp_replace(query,'\s+',' ','g'),90) AS query
  FROM pg_stat_statements
  ORDER BY total_exec_time DESC LIMIT 15;"
```
The rank-1 query by `pct`/`total_ms` is the bottleneck. High `calls` × low `mean`
= chatty; low `calls` × high `mean` = heavy scan. Reset between runs with
`SELECT pg_stat_statements_reset();`.

### 5. Deadlocks (40P01) and timeouts (57014)

```bash
rtk proxy docker logs flo-home-postgres-1 > /tmp/pg.log 2>&1
grep -c 'deadlock detected' /tmp/pg.log
grep -c 'canceling statement due to statement timeout' /tmp/pg.log
# which relation(s) and which statements form the cycle:
grep -A16 'deadlock detected' /tmp/pg.log | grep -oE 'relation [0-9]+' | sort | uniq -c | sort -rn
grep -A16 'deadlock detected' /tmp/pg.log | grep -oE 'UPDATE [a-z_]+|INSERT INTO [a-z_]+|FOR (NO KEY )?UPDATE|FOR KEY SHARE|SELECT id|WITH [a-z_]+' | sort | uniq -c | sort -rn
grep -A16 'deadlock detected' /tmp/pg.log | head -22   # read one full cycle
# resolve OID -> table:
docker exec -e PGPASSWORD=notifier flo-home-postgres-1 psql -U notifier -d notifier -tAc "SELECT <oid>::regclass;"
```
Read the cycle: `Process A waits for <lock> on tuple/relation; blocked by B …
B … blocked by A` is the loop. Note the lock MODE — `FOR UPDATE` vs
`FOR NO KEY UPDATE` vs `FOR KEY SHARE` matters (FK child inserts take KEY SHARE
on the parent; FOR UPDATE conflicts with it, FOR NO KEY UPDATE does not).

### 6. Table bloat & autovacuum (validates the claim-store design, ADR-0011)

```bash
docker exec -e PGPASSWORD=notifier flo-home-postgres-1 psql -U notifier -d notifier -P pager=off -c "
  SELECT relname, n_live_tup, n_dead_tup,
         round(100*n_dead_tup/GREATEST(n_live_tup+n_dead_tup,1),1) AS dead_pct,
         autovacuum_count
  FROM pg_stat_user_tables ORDER BY n_dead_tup DESC LIMIT 12;"
```

### 7. Index usage (find unused / hot indexes)

```bash
docker exec -e PGPASSWORD=notifier flo-home-postgres-1 psql -U notifier -d notifier -P pager=off -c "
  SELECT relname, indexrelname, idx_scan
  FROM pg_stat_user_indexes ORDER BY idx_scan DESC LIMIT 20;"
```

### 8. Backlog state (is the system keeping up?)

```bash
docker exec -e PGPASSWORD=notifier flo-home-postgres-1 psql -U notifier -d notifier -P pager=off -c "
  SELECT status, count(*) FROM deliveries GROUP BY status;
  SELECT count(*) AS active_claims FROM delivery_claims;"
```

### 9. App-side resilience metrics (when relevant)

```bash
docker exec flo-home-notifier-1 sh -c \
  'wget -qO- localhost:9090/metrics | grep -E "webhook_worker_transient_db_errors_total|webhook_breaker|webhook_claim_empty_total"'
```

## Reporting (be explicit about what you did)

Always state, concretely:

1. **Setup**: image rebuilt? fresh DB? how many replicas, what load, how long the
   window.
2. **Health**: were all replicas healthy the whole window; any Exited/Restarting;
   fatal `delivery runtime error` count.
3. **Evidence**: the rank-1 query (`pct`, `calls`, `mean_ms`); deadlock count and
   the cycle (relations + lock modes + statements); timeout count; bloat split;
   backlog state. Quote real numbers.
4. **Conclusion**: name the bottleneck and the mechanism, tied to the evidence.
5. **Before vs after** (when validating a fix): same load + window, compare the
   counts. A fix is proven only when the metric it targets actually moved — never
   claim a win from unit tests alone.

## Teardown

```bash
just stack-down   # or: docker compose ... --profile observe --profile chaos --profile retention down -v
```
