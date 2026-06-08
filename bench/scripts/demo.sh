#!/usr/bin/env bash
# End-to-end demo: reset stack, register customers, post events, verify
# delivery, tear down. Invoked by `just demo`, which passes the compose
# command and stack profile/service lists via environment variables.
set -euo pipefail

read -ra COMPOSE_CMD <<< "${COMPOSE:?COMPOSE env required (set by justfile)}"
read -ra PROFILES <<< "${STACK_ALL_PROFILES:-}"
read -ra BASE_SERVICES <<< "${STACK_BASE_SERVICES:-}"

echo "=== Webhook Notifier Demo ==="
echo ""

cleanup() {
    if [[ "${DEMO_KEEP_STACK:-0}" == "1" ]]; then
        echo ""
        echo "DEMO_KEEP_STACK=1 set; leaving stack running."
        return
    fi
    echo ""
    echo "Tearing down demo stack..."
    "${COMPOSE_CMD[@]}" "${PROFILES[@]}" down -v --remove-orphans >/dev/null
}
trap cleanup EXIT

# 1. Reset and start the dev stack. The Postgres init hook applies migrations
# on fresh volume creation.
echo "[1/6] Resetting and starting services..."
"${COMPOSE_CMD[@]}" "${PROFILES[@]}" down -v --remove-orphans >/dev/null
"${COMPOSE_CMD[@]}" up -d --scale notifier=2 "${BASE_SERVICES[@]}"

echo "    Waiting for services..."
"${COMPOSE_CMD[@]}" exec -T postgres bash -c 'until pg_isready -U notifier -d notifier; do sleep 0.5; done'

# Wait for notifier HTTP to be ready
for i in $(seq 1 20); do
    if curl -sf http://localhost:8080/ >/dev/null 2>&1; then break; fi
    sleep 0.5
done
echo "    All services ready."

# 2. Register customers + webhooks
echo "[2/6] Registering customers and webhooks..."
curl -fsS -X POST http://localhost:8080/api/customers \
  -H 'Content-Type: application/json' \
  -d '{"id":"demo-whale","class":3,"url":"http://webhook-receiver:8080/hooks"}' >/dev/null
curl -fsS -X POST http://localhost:8080/api/customers \
  -H 'Content-Type: application/json' \
  -d '{"id":"demo-small","class":1,"url":"http://webhook-receiver:8080/hooks"}' >/dev/null
echo "    Customers registered: demo-whale, demo-small"

# 3. Post events
echo "[3/6] Posting events..."
for i in $(seq 1 20); do
    curl -fsS -X POST http://localhost:8080/api/events \
      -H 'Content-Type: application/json' \
      -d "{\"customer_id\":\"demo-whale\",\"event_name\":\"subscriber.created\",\"event_time\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",\"data\":{\"id\":\"whale-$i\"}}" >/dev/null &
done
for i in $(seq 1 10); do
    curl -fsS -X POST http://localhost:8080/api/events \
      -H 'Content-Type: application/json' \
      -d "{\"customer_id\":\"demo-small\",\"event_name\":\"subscriber.created\",\"event_time\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",\"data\":{\"id\":\"small-$i\"}}" >/dev/null &
done
wait
echo "    20 whale + 10 small events posted."

# 4. Wait for processing and assert all demo deliveries succeeded.
# Each customer registration creates 3 webhooks (/hooks/fast, /hooks/slow,
# /hooks/flaky — see upsertCustomerTx in cmd/notifier/http_handlers.go), so
# every event fans out to 3 deliveries: 30 events * 3 webhooks = 90 deliveries.
expected=90
echo "[4/6] Waiting for delivery processing..."
for i in $(seq 1 60); do
    succeeded="$("${COMPOSE_CMD[@]}" exec -T postgres psql -U notifier -d notifier -At -c "SELECT COUNT(*) FROM deliveries WHERE customer_id IN ('demo-whale', 'demo-small') AND status = 'succeeded';" | tr -d '[:space:]')"
    total="$("${COMPOSE_CMD[@]}" exec -T postgres psql -U notifier -d notifier -At -c "SELECT COUNT(*) FROM deliveries WHERE customer_id IN ('demo-whale', 'demo-small');" | tr -d '[:space:]')"
    if [[ "${succeeded}" == "${expected}" && "${total}" == "${expected}" ]]; then
        echo "    All ${expected} deliveries succeeded."
        break
    fi
    sleep 1
done
if [[ "${succeeded:-0}" != "${expected}" || "${total:-0}" != "${expected}" ]]; then
    echo "    Expected ${expected} succeeded demo deliveries; got succeeded=${succeeded:-0}, total=${total:-0}." >&2
    exit 1
fi

# 5. Show results
echo ""
echo "[5/6] Delivery results:"
"${COMPOSE_CMD[@]}" exec -T postgres psql -U notifier -d notifier -P pager=off -P border=2 -c "
SELECT
  d.customer_id,
  d.status,
  COUNT(*) AS count
FROM deliveries d
WHERE d.customer_id IN ('demo-whale', 'demo-small')
GROUP BY d.customer_id, d.status
ORDER BY d.customer_id, d.status;
"

echo ""
echo "[6/6] Attempts recorded:"
"${COMPOSE_CMD[@]}" exec -T postgres psql -U notifier -d notifier -P pager=off -P border=2 -c "
SELECT COUNT(*) AS total_attempts FROM delivery_attempts;
"

echo ""
echo "Receiver metrics (90 deliveries = 30 events x 3 webhooks):"
curl -fsS http://localhost:9092/metrics 2>/dev/null | grep -E '^webhook_requests_received_total' || echo "    (receiver metrics unavailable)"

echo ""
echo "Notifier health:"
curl -fsS http://localhost:9090/health 2>/dev/null || echo "    (metrics endpoint unavailable)"

echo ""
echo "=== Demo complete ==="
echo ""
echo "Set DEMO_KEEP_STACK=1 to leave containers running for inspection."
