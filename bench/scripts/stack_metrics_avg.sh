#!/usr/bin/env bash
set -euo pipefail

PROM_URL="${PROM_URL:-http://localhost:9091}"
WINDOW_SECONDS="${1:-120}"
SAMPLE_INTERVAL_SECONDS="${2:-5}"

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required for stack_metrics_avg.sh" >&2
  exit 1
fi

if [[ "$WINDOW_SECONDS" -lt 5 ]]; then
  echo "window_seconds must be >= 5" >&2
  exit 1
fi

if [[ "$SAMPLE_INTERVAL_SECONDS" -lt 1 ]]; then
  echo "sample_interval_seconds must be >= 1" >&2
  exit 1
fi

query() {
  local expr="$1"
  curl -fsS -G "${PROM_URL}/api/v1/query" \
    --data-urlencode "query=${expr}" \
  | jq -r '.data.result[0].value[1] // "0"'
}

samples=$(( WINDOW_SECONDS / SAMPLE_INTERVAL_SECONDS ))
if [[ "$samples" -lt 1 ]]; then
  samples=1
fi

sum_succeeded=0
sum_terminal=0
sum_ingress=0
sum_extra_attempts=0
sum_claim_p95=0
sum_small_p95=0
sum_whale_p95=0

for ((i=1; i<=samples; i++)); do
  succeeded="$(query 'sum(rate(webhook_deliveries_total{status="succeeded"}[1m]))')"
  terminal="$(query 'sum(rate(webhook_deliveries_total{status=~"succeeded|dead"}[1m]))')"
  ingress="$(query 'sum(rate(webhook_requests_received_total[1m]))')"
  extra_attempts="$(query 'clamp_min((sum(rate(webhook_attempts_per_delivery_sum[1m])) / clamp_min(sum(rate(webhook_attempts_per_delivery_count[1m])), 1e-9)) - 1, 0)')"
  claim_p95="$(query 'histogram_quantile(0.95, sum by (le) (rate(webhook_claim_duration_seconds_bucket[1m])))')"
  small_p95="$(query 'histogram_quantile(0.95, sum by (le) (rate(webhook_delivery_latency_seconds_bucket{customer_class="small"}[1m])))')"
  whale_p95="$(query 'histogram_quantile(0.95, sum by (le) (rate(webhook_delivery_latency_seconds_bucket{customer_class="whale"}[1m])))')"

  sum_succeeded="$(awk -v a="$sum_succeeded" -v b="$succeeded" 'BEGIN{printf "%.10f", a+b}')"
  sum_terminal="$(awk -v a="$sum_terminal" -v b="$terminal" 'BEGIN{printf "%.10f", a+b}')"
  sum_ingress="$(awk -v a="$sum_ingress" -v b="$ingress" 'BEGIN{printf "%.10f", a+b}')"
  sum_extra_attempts="$(awk -v a="$sum_extra_attempts" -v b="$extra_attempts" 'BEGIN{printf "%.10f", a+b}')"
  sum_claim_p95="$(awk -v a="$sum_claim_p95" -v b="$claim_p95" 'BEGIN{printf "%.10f", a+b}')"
  sum_small_p95="$(awk -v a="$sum_small_p95" -v b="$small_p95" 'BEGIN{printf "%.10f", a+b}')"
  sum_whale_p95="$(awk -v a="$sum_whale_p95" -v b="$whale_p95" 'BEGIN{printf "%.10f", a+b}')"

  if [[ "$i" -lt "$samples" ]]; then
    sleep "$SAMPLE_INTERVAL_SECONDS"
  fi
done

avg() {
  awk -v total="$1" -v n="$samples" 'BEGIN{printf "%.4f", (n > 0 ? total/n : 0)}'
}

avg_succeeded="$(avg "$sum_succeeded")"
avg_terminal="$(avg "$sum_terminal")"
avg_ingress="$(avg "$sum_ingress")"
avg_extra_attempts="$(avg "$sum_extra_attempts")"
avg_claim_p95_ms="$(awk -v s="$(avg "$sum_claim_p95")" 'BEGIN{printf "%.2f", s*1000}')"
avg_small_p95_ms="$(awk -v s="$(avg "$sum_small_p95")" 'BEGIN{printf "%.2f", s*1000}')"
avg_whale_p95_ms="$(awk -v s="$(avg "$sum_whale_p95")" 'BEGIN{printf "%.2f", s*1000}')"
fairness_ratio="$(awk -v w="$avg_whale_p95_ms" -v s="$avg_small_p95_ms" 'BEGIN{if (s <= 0) {print "0.00"} else {printf "%.2f", w/s}}')"

cat <<EOF
window_seconds=${WINDOW_SECONDS}
sample_interval_seconds=${SAMPLE_INTERVAL_SECONDS}
samples=${samples}
avg_succeeded_deliveries_per_sec=${avg_succeeded}
avg_terminal_deliveries_per_sec=${avg_terminal}
avg_ingress_requests_per_sec=${avg_ingress}
avg_extra_attempts_per_delivery=${avg_extra_attempts}
avg_claim_p95_ms=${avg_claim_p95_ms}
avg_small_p95_delivery_latency_ms=${avg_small_p95_ms}
avg_whale_p95_delivery_latency_ms=${avg_whale_p95_ms}
fairness_latency_ratio_whale_over_small=${fairness_ratio}
EOF
