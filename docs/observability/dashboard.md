# Webhook Notifier Dashboard

## Panels

- Delivery latency p50, p95, and p99 split by `customer_class` from `webhook_delivery_latency_seconds`.
- Deliveries per second by terminal `status` from `webhook_deliveries_total`.
- Attempts-per-delivery heatmap from `webhook_attempts_per_delivery`.
- DLQ ingress rate by `reason` from `webhook_dlq_total`.
- Pending queue depth by `customer_class` from `webhook_pending_count`.
- Per-host semaphore wait p95 for the top 10 hosts from `webhook_host_semaphore_wait_seconds`.
- Scheduler pick latency from `webhook_scheduler_pick_duration_seconds`.
- Postgres pool usage from `webhook_pg_pool_*`.

## Deferred Panels

- Open circuit breakers and breaker trip events once breaker metrics exist.
