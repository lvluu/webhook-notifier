#!/usr/bin/env bash
set -euo pipefail

floor="${1:-500}"
duration_sec="${2:-900}"
interval_sec="${3:-30}"

if [[ -z "${DATABASE_URL:-}" ]]; then
  echo "DATABASE_URL is required" >&2
  exit 1
fi

psql_cmd=(psql)
if ! command -v psql >/dev/null 2>&1; then
  psql_cmd=(docker compose -f compose/docker-compose.yml --project-directory . exec -T postgres psql -U notifier -d notifier)
fi

end_time=$((SECONDS + duration_sec))
start_time=0
steady_started=0
min_pending=""
max_pending=""
last_pending=""
last_rate=""

sql='
SELECT
  COUNT(*) FILTER (WHERE status IN ('\''pending'\'', '\''failed_retry'\'')) AS pending_retry_total,
  COUNT(*) FILTER (WHERE status IN ('\''succeeded'\'', '\''dead'\'')) AS terminal_total,
  COALESCE(
    EXTRACT(EPOCH FROM (
      now() - MIN(created_at) FILTER (WHERE status IN ('\''pending'\'', '\''failed_retry'\''))
    )),
    0
  ) AS oldest_pending_age_sec
FROM deliveries;
'

echo "steady-state watch: floor=${floor} duration=${duration_sec}s interval=${interval_sec}s"

while (( SECONDS < end_time )); do
  IFS='|' read -r pending terminal oldest <<EOF
$(DATABASE_URL="$DATABASE_URL" "${psql_cmd[@]}" -AtF '|' -c "$sql")
EOF
  rate="n/a"
  if rate_json=$(curl -sf 'http://localhost:9091/api/v1/query?query=sum(rate(webhook_deliveries_total%7Bstatus%3D%22succeeded%22%7D%5B2m%5D))'); then
    rate=$(printf '%s' "$rate_json" | sed -n 's/.*"value":\[[^,]*,"\([^"]*\)"\].*/\1/p')
    rate="${rate:-n/a}"
  fi

  if [[ "$steady_started" -eq 0 ]]; then
    if (( pending >= floor )); then
      steady_started=1
      start_time=$SECONDS
      min_pending="$pending"
      max_pending="$pending"
      echo "steady-state reached at pending=${pending}"
    else
      echo "warming up: pending=${pending} terminal=${terminal} oldest_pending_age_sec=${oldest} rate=${rate}"
      sleep "$interval_sec"
      continue
    fi
  fi

  if (( pending < floor )); then
    echo "not steady: pending dropped below floor (${pending} < ${floor})"
    exit 2
  fi

  if [[ -z "$min_pending" || "$pending" -lt "$min_pending" ]]; then
    min_pending="$pending"
  fi
  if [[ -z "$max_pending" || "$pending" -gt "$max_pending" ]]; then
    max_pending="$pending"
  fi
  last_pending="$pending"
  last_rate="$rate"

  echo "sample: pending=${pending} terminal=${terminal} oldest_pending_age_sec=${oldest} rate=${rate}"
  sleep "$interval_sec"
done

elapsed=$((SECONDS - start_time))
echo "steady-state watch complete: elapsed=${elapsed}s min_pending=${min_pending} max_pending=${max_pending} last_pending=${last_pending} last_rate=${last_rate}"
