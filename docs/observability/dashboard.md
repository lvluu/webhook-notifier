# Webhook Notifier Dashboard

Prometheus scrapes every scaled `notifier` replica through Docker DNS on
`:9090`. Caddy exposes the business ingest API on `localhost:8080`, but it is
not part of the notifier scrape target set.

## Panels

- Delivery latency p50, p95, and p99 split by `customer_class` from `webhook_delivery_latency_seconds`. Lower is better.
- Latency SLO attainment by `customer_class`, derived from the share of deliveries completed within 3 seconds. Higher is better.
- Deliveries per second by terminal `status` from `webhook_deliveries_total`. Higher is better if the system is healthy.
- Attempts-per-delivery from `webhook_attempts_per_delivery`. Lower is better.
- DLQ ingress rate by `reason` from `webhook_dlq_total`. Lower is better; anything sustained is a problem.
- Notifier instance count and configured worker count. This is context for scaling, not a success metric.
- Per-host semaphore wait p95 for the top hosts from `webhook_host_semaphore_wait_seconds`. Lower is better.
- Claim duration and batch size from `webhook_claim_duration_seconds` and `webhook_claim_batch_size`. Lower claim latency is better; batch size should stay high enough to keep throughput efficient.
- Postgres pool usage from `webhook_pg_pool_*`. Lower pressure is better.
- Receiver throughput from `webhook_requests_received_total`. Higher is only useful when notifier throughput keeps up.

## Deferred Panels

- Open circuit breakers and breaker trip events once breaker metrics exist.
- Caddy ingress request rate and upstream errors if Caddy access logs are enabled.
