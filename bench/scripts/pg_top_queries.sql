-- Top-N SQL statements by total execution time, from pg_stat_statements.
-- Invoked by `just db-top-queries [n]` (passes -v limit=N; default 20).
-- Standalone: psql "$DATABASE_URL" -v limit=20 -f bench/scripts/pg_top_queries.sql

\set ON_ERROR_STOP on

SELECT CASE
  WHEN EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements')
  THEN 'on' ELSE 'off'
END AS has_pgss \gset

\if :has_pgss
\echo 'Top SQL by total execution time (ms). pct_total = share of total server exec time.'
SELECT
  LEFT(regexp_replace(query, '\s+', ' ', 'g'), 100)                                    AS query,
  calls,
  round(total_exec_time::numeric, 1)                                                  AS total_ms,
  round(mean_exec_time::numeric, 2)                                                   AS mean_ms,
  round(max_exec_time::numeric, 2)                                                     AS max_ms,
  round(stddev_exec_time::numeric, 2)                                                  AS stddev_ms,
  round((100 * total_exec_time / NULLIF(sum(total_exec_time) OVER (), 0))::numeric, 1) AS pct_total,
  round((rows::numeric / NULLIF(calls, 0)), 1)                                         AS rows_per_call,
  round(100.0 * shared_blks_hit / NULLIF(shared_blks_hit + shared_blks_read, 0), 1)    AS cache_hit_pct
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT :limit;
\else
\echo 'pg_stat_statements is not enabled.'
\echo 'Enable it with: CREATE EXTENSION IF NOT EXISTS pg_stat_statements;'
\echo 'and ensure shared_preload_libraries includes pg_stat_statements.'
\endif
