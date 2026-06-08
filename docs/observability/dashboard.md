# Webhook Notifier Dashboard

Prometheus scrapes every notifier replica on `:9090`. Caddy fronts only the
business ingest API and is not a notifier metrics target.

## What The Dashboard Answers

- **Are Small Customers waiting behind Whale traffic?**
  `webhook_queue_wait_seconds` is first-Attempt queue latency split by
  `customer_class`. This is the live metric closest to the benchmark fairness
  measure because it excludes partner response time and retry backoff.
- **Are Deliveries completing, retrying, or dying?**
  `webhook_deliveries_total`, `webhook_attempt_total`,
  `webhook_delivery_latency_seconds`, `webhook_post_duration_seconds`, and
  `webhook_dlq_total` distinguish queue behavior from Integration Partner
  behavior.
- **Is the claim path becoming the bottleneck?**
  `webhook_claim_duration_seconds`, `webhook_claim_batch_size`, and
  `webhook_claim_empty_total` show database cost and wasted claim work. Empty
  claims matter most when backlog is also growing.
- **Is backlog accumulating?**
  `webhook_pending_deliveries` and `webhook_pending_oldest_age_seconds` show
  queue depth and age. Age is the stronger starvation signal because a stable
  backlog can still contain work that is not progressing.
- **Is configured concurrency actually available?**
  `webhook_workers_configured`, `webhook_deliveries_in_flight`, and
  `webhook_pg_pool_*` explain whether workers or database connections are the
  immediate capacity constraint.
- **Are wakeups or database operations degrading?**
  `webhook_pg_notify_broadcast_total` and
  `webhook_worker_transient_db_errors_total` separate dropped advisory wakeups
  from recoverable database failures. DB discovery remains the correctness
  fallback for wakeups.
- **Are failing Webhooks being isolated?**
  `webhook_breaker_state`, `webhook_breaker_trips_total`, and
  `webhook_breaker_sync_fallback_total` show breaker lifecycle and persistence
  pressure. The per-Webhook state metric is high-cardinality and should be used
  for targeted diagnosis rather than broad long-retention aggregation.
- **Is local partner protection active?**
  `webhook_host_semaphore_wait_seconds` records HostLimiter acquisition time.
  Because the delivery path uses non-blocking `TryAcquire`, wait duration is
  normally near zero; requeue behavior and partner-protection benchmarks are
  more useful saturation evidence than a large wait-duration threshold.

## Receiver Metrics

`webhook_requests_received_total` belongs to the benchmark receiver, not the
notifier. It is useful for demos and load tests, but production dashboards need
an equivalent Integration Partner or egress signal from the deployment
environment.

## Deferred Observability

- Caddy ingest request rate and upstream errors require access-log metrics.
- Fleet-wide Host Concurrency Cap metrics depend on a future distributed
  limiter; the current HostLimiter is process-local.
- An absolute fairness SLO requires production traffic and capacity evidence.
  The maintained benchmark currently uses comparative Small-versus-Whale P95.
