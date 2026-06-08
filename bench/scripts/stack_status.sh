#!/usr/bin/env bash
# Compact queue/backlog dashboard for `watch -n`. Invoked by `just stack-status`,
# which passes the compose command via the COMPOSE environment variable.
set -euo pipefail

read -ra COMPOSE_CMD <<< "${COMPOSE:?COMPOSE env required (set by justfile)}"

if ! "${COMPOSE_CMD[@]}" ps --status running --services 2>/dev/null | grep -qx postgres; then
  echo "postgres not running"
  exit 0
fi

echo "$(date '+%H:%M:%S') ─── Stack Status ───"
echo

"${COMPOSE_CMD[@]}" exec -T postgres psql -U notifier -d notifier -P pager=off -P border=2 -c "
WITH queue AS (
  SELECT
    count(*) FILTER (WHERE status IN ('pending', 'failed_retry')) AS ready,
    count(*) FILTER (WHERE status = 'succeeded') AS succeeded,
    count(*) FILTER (WHERE status = 'dead') AS dead,
    count(*) FILTER (WHERE status IN ('pending', 'failed_retry') AND next_attempt_at <= now()) AS due_now,
    count(*) FILTER (WHERE status IN ('pending', 'failed_retry') AND next_attempt_at > now()) AS delayed,
    COALESCE(EXTRACT(EPOCH FROM max(now() - created_at) FILTER (WHERE status IN ('pending', 'failed_retry'))), 0)::bigint AS oldest_ready_s,
    count(*) FILTER (WHERE created_at >= now() - interval '1 minute') AS ingested_1m,
    count(*) FILTER (WHERE status = 'succeeded' AND updated_at >= now() - interval '1 minute') AS succeeded_1m,
    count(*) FILTER (WHERE status = 'dead' AND updated_at >= now() - interval '1 minute') AS dead_1m
  FROM deliveries
),
claims AS (
  SELECT count(*) AS active_claims
  FROM delivery_claims
),
attempts AS (
  SELECT
    count(*) FILTER (WHERE sent_at >= now() - interval '1 minute') AS attempts_1m,
    round(count(*) FILTER (WHERE sent_at >= now() - interval '10 seconds') / 10.0, 1) AS attempts_per_s
  FROM delivery_attempts
),
customers AS (
  SELECT count(DISTINCT customer_id) AS ready_customers
  FROM deliveries
  WHERE status IN ('pending', 'failed_retry')
),
breakers AS (
  SELECT
    count(*) FILTER (WHERE breaker_state = 'open') AS open_breakers,
    count(*) FILTER (WHERE breaker_state = 'half_open') AS half_open_breakers
  FROM webhooks
),
health AS (
  SELECT
    q.*,
    c.active_claims,
    a.attempts_1m,
    a.attempts_per_s,
    customers.ready_customers,
    b.open_breakers,
    b.half_open_breakers,
    CASE
      WHEN q.ready = 0 AND c.active_claims = 0 THEN 'CAUGHT UP'
      WHEN q.oldest_ready_s >= 300 THEN 'FAR BEHIND'
      WHEN q.ready > 0
        AND q.oldest_ready_s >= 30
        AND (q.ingested_1m - (q.succeeded_1m + q.dead_1m)) > GREATEST(1000, q.ingested_1m / 20)
        THEN 'FALLING BEHIND'
      WHEN q.ready > 0 THEN 'CATCHING UP'
      ELSE 'ACTIVE'
    END AS verdict
  FROM queue q
  CROSS JOIN claims c
  CROSS JOIN attempts a
  CROSS JOIN customers
  CROSS JOIN breakers b
)
SELECT
  verdict,
  ready AS backlog,
  due_now,
  active_claims AS claims,
  ready_customers AS customers,
  oldest_ready_s,
  ingested_1m,
  succeeded_1m,
  dead_1m,
  succeeded_1m + dead_1m AS drained_1m,
  attempts_1m,
  attempts_per_s AS \"att/s\",
  succeeded,
  dead,
  open_breakers AS open_brk,
  half_open_breakers AS ho_brk
FROM health;
"

echo "Receiver:"
curl -fsS http://localhost:9092/metrics 2>/dev/null | grep '^webhook_requests_received_total' || echo "unavailable"

echo
echo "Retry DB snapshot:"
"${COMPOSE_CMD[@]}" exec -T postgres psql -U notifier -d notifier -P pager=off -P border=2 -c "
SELECT
  COALESCE(last_error, 'unknown') AS reason,
  count(*) AS retry_deliveries,
  min(next_attempt_at) AS next_due_at,
  max(updated_at) AS last_updated_at
FROM deliveries
WHERE status = 'failed_retry'
GROUP BY COALESCE(last_error, 'unknown')
ORDER BY retry_deliveries DESC
LIMIT 10;

SELECT
  breaker_state,
  count(*) AS webhooks,
  max(breaker_failure_count) AS max_failures,
  min(breaker_next_probe_at) FILTER (WHERE breaker_next_probe_at IS NOT NULL) AS next_probe_at
FROM webhooks
GROUP BY breaker_state
ORDER BY breaker_state;
"
