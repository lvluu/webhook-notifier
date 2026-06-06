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
- Claim duration and batch size from `webhook_claim_duration_seconds` and `webhook_claim_batch_size`. Lower claim latency is better. The batch-size panel shows both effective average, which includes empty claim queries, and non-empty average, which shows how much work a successful claim returns.
- Oldest pending delivery age from `webhook_pending_oldest_age_seconds`. This measures the oldest row still in `pending` only, not retryable backoff rows.
- Empty claim rate from `webhook_claim_empty_total / webhook_claim_batch_size_count`. Under sparse or drained load this can be high because multiple replicas may be woken for work that one replica already claimed with `SKIP LOCKED`; treat it as a DB churn signal only when it is paired with rising backlog or claim latency.
- Postgres pool usage from `webhook_pg_pool_*`. Lower pressure is better.
- Receiver throughput from `webhook_requests_received_total`. Higher is only useful when notifier throughput keeps up.

## Deferred Panels

- Open circuit breakers and breaker trip events once breaker metrics exist.
- Caddy ingress request rate and upstream errors if Caddy access logs are enabled.
