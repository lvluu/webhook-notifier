-- Issue #54 sustained-load queue health probes.
-- Usage: psql "$DATABASE_URL" -f bench/scripts/issue54_health.sql

\timing on

SELECT now() AS observed_at;

-- Dead/live tuple health on hot tables.
SELECT
  relname,
  n_live_tup,
  n_dead_tup,
  round((n_dead_tup::numeric / NULLIF(n_live_tup + n_dead_tup, 0)) * 100, 2) AS dead_pct,
  vacuum_count,
  autovacuum_count,
  last_vacuum,
  last_autovacuum
FROM pg_stat_user_tables
WHERE relname IN ('deliveries', 'delivery_attempts', 'dead_letters', 'delivery_claims')
ORDER BY relname;

-- Claimable backlog shape.
SELECT
  COUNT(*) FILTER (WHERE status IN ('pending', 'failed_retry')) AS pending_retry_total,
  COUNT(*) FILTER (WHERE status = 'pending') AS pending_total,
  COUNT(*) FILTER (WHERE status = 'failed_retry') AS failed_retry_total,
  COUNT(*) FILTER (WHERE status IN ('succeeded', 'dead')) AS terminal_total,
  EXTRACT(EPOCH FROM (
    now() - MIN(created_at) FILTER (WHERE status IN ('pending', 'failed_retry'))
  )) AS oldest_pending_age_sec
FROM deliveries;

-- Oldest-by-status quick breakdown.
SELECT
  status,
  COUNT(*) AS cnt,
  MIN(created_at) AS oldest_created_at,
  MAX(created_at) AS newest_created_at
FROM deliveries
GROUP BY status
ORDER BY status;

-- Table-level size to detect sustained growth pressure.
SELECT
  relname,
  pg_size_pretty(pg_total_relation_size(relid)) AS total_size
FROM pg_catalog.pg_statio_user_tables
WHERE relname IN ('deliveries', 'delivery_attempts', 'dead_letters', 'delivery_claims')
ORDER BY pg_total_relation_size(relid) DESC;
