-- Maintained, read-only Postgres health snapshot for a running notifier load.
-- What: hot-table bloat, backlog age, sessions, locks, relation size, and
-- optional pg_stat_statements evidence.
-- Why: queue throughput can look healthy briefly while bloat, lock pressure,
-- or query-plan cost accumulates underneath it.
-- Prerequisite: psql connected to the notifier database. Query-level output is
-- richer when pg_stat_statements is installed and preloaded.
-- Usage: psql "$DATABASE_URL" -f bench/scripts/db_query_health.sql

\timing on

SELECT now() AS observed_at;

-- Core queue shape and bloat snapshot.
SELECT
  relname,
  n_live_tup,
  n_dead_tup,
  round((n_dead_tup::numeric / NULLIF(n_live_tup + n_dead_tup, 0)) * 100, 2) AS dead_pct,
  autovacuum_count,
  last_autovacuum
FROM pg_stat_user_tables
WHERE relname IN ('deliveries', 'delivery_claims', 'delivery_attempts', 'dead_letters')
ORDER BY relname;

-- Backlog and oldest age (claim pressure context).
SELECT
  COUNT(*) FILTER (WHERE status IN ('pending', 'failed_retry')) AS pending_retry_total,
  COUNT(*) FILTER (WHERE status = 'pending') AS pending_total,
  COUNT(*) FILTER (WHERE status = 'failed_retry') AS failed_retry_total,
  EXTRACT(EPOCH FROM (now() - MIN(created_at) FILTER (WHERE status IN ('pending', 'failed_retry')))) AS oldest_pending_age_sec
FROM deliveries;

-- Active sessions and wait events.
SELECT
  COALESCE(wait_event_type, 'running') AS wait_event_type,
  COALESCE(wait_event, 'running') AS wait_event,
  COUNT(*) AS sessions
FROM pg_stat_activity
WHERE datname = current_database()
GROUP BY 1, 2
ORDER BY sessions DESC, wait_event_type, wait_event;

-- Lock pressure on queue tables.
SELECT
  c.relname,
  l.mode,
  l.granted,
  COUNT(*) AS lock_count
FROM pg_locks l
JOIN pg_class c ON c.oid = l.relation
WHERE c.relname IN ('deliveries', 'delivery_claims', 'delivery_attempts', 'dead_letters')
GROUP BY c.relname, l.mode, l.granted
ORDER BY c.relname, l.mode, l.granted;

-- pg_stat_statements is optional but strongly recommended.
SELECT CASE
  WHEN EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements')
  THEN 'on' ELSE 'off'
END AS has_pg_stat_statements \gset

\if :has_pg_stat_statements
\echo 'pg_stat_statements: TOP SQL by total execution time'
SELECT
  LEFT(regexp_replace(query, '\\s+', ' ', 'g'), 140) AS query_snippet,
  calls,
  round(total_exec_time::numeric, 2) AS total_exec_ms,
  round(mean_exec_time::numeric, 3) AS mean_exec_ms,
  round((rows::numeric / NULLIF(calls, 0)), 3) AS rows_per_call,
  shared_blks_hit,
  shared_blks_read,
  temp_blks_read,
  temp_blks_written
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT 15;

\echo 'pg_stat_statements: queue-path statements (deliveries / claims / retention-like)'
SELECT
  LEFT(regexp_replace(query, '\\s+', ' ', 'g'), 160) AS query_snippet,
  calls,
  round(total_exec_time::numeric, 2) AS total_exec_ms,
  round(mean_exec_time::numeric, 3) AS mean_exec_ms,
  round((rows::numeric / NULLIF(calls, 0)), 3) AS rows_per_call,
  shared_blks_hit,
  shared_blks_read
FROM pg_stat_statements
WHERE lower(query) LIKE '%deliveries%'
   OR lower(query) LIKE '%delivery_claims%'
   OR lower(query) LIKE '%delivery_history%'
ORDER BY total_exec_time DESC
LIMIT 20;
\else
\echo 'pg_stat_statements extension is not enabled. Enable with:'
\echo '  CREATE EXTENSION IF NOT EXISTS pg_stat_statements;'
\echo 'and ensure shared_preload_libraries includes pg_stat_statements.'
\endif
