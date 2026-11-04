# Notifier Metrics Reference

Source of truth: `internal/observe/registry.go` and `internal/observe/dbpool.go`.

## Notifier Metrics

| Metric | Type | Labels | Category | Direction | Description |
|---|---|---|---|---|---|
| `webhook_deliveries_total` | Counter | `status`, `customer_class` | Delivery | — | Total number of webhook deliveries processed, by terminal status |
| `webhook_delivery_latency_seconds` | Histogram | `customer_class` | Latency | lower better | Delivery lifetime from enqueue to terminal |
| `webhook_queue_wait_seconds` | Histogram | `customer_class` | Latency | lower better | Time from delivery enqueue to first Attempt |
| `webhook_attempt_total` | Counter | `outcome` | Reliability | lower better | Total number of delivery Attempts, by classified outcome |
| `webhook_post_duration_seconds` | Histogram | `customer_class` | Latency | lower better | Duration of HTTP POST Attempts |
| `webhook_deliveries_in_flight` | Gauge | — | Capacity | — | Current number of deliveries being processed by workers |
| `webhook_attempts_per_delivery` | Histogram | — | Reliability | lower better | Number of attempts per delivery at terminal state |
| `webhook_dlq_total` | Counter | `reason` | Reliability | lower better | Total number of deliveries sent to the DLQ, by reason |
| `webhook_scheduler_pick_duration_seconds` | Histogram | — | Latency | lower better | Duration of Scheduler.Pick() calls |
| `webhook_host_semaphore_wait_seconds` | Histogram | `host` | Contention | lower better | Time spent waiting for a per-host semaphore slot |
| `webhook_claim_duration_seconds` | Histogram | — | Latency | lower better | Duration of outbox claim transactions |
| `webhook_claim_batch_size` | Histogram | — | Efficiency | higher better | Number of deliveries claimed per batch |
| `webhook_claim_empty_total` | Counter | — | Efficiency | lower better | Total number of claim queries that returned zero rows |
| `webhook_pending_deliveries` | Gauge | `customer_class` | Backlog | lower better | Current number of pending or retryable deliveries awaiting worker processing |
| `webhook_pending_oldest_age_seconds` | Gauge | — | Backlog | lower better | Age in seconds of the oldest pending or retryable delivery |
| `webhook_active_customers` | Gauge | — | Backlog | — | Current number of customers with pending deliveries in the scheduler |
| `webhook_workers_configured` | Gauge | — | Capacity | — | Configured number of notifier worker goroutines |
| `webhook_ingest_fanout_per_event` | Histogram | — | Delivery | — | Number of webhooks fanned out per ingested event |
| `webhook_breaker_state` | Gauge | `webhook_id`, `state` | Circuit Breaker | — | 1 for the active state; high-cardinality, targeted diagnosis only |
| `webhook_breaker_trips_total` | Counter | `reason` | Circuit Breaker | lower better | Total number of webhook circuit breaker trips, by reason |
| `webhook_pg_notify_broadcast_total` | Counter | `result` | Wakeup | — | Total number of remote Customer wake hints handled by the pg_notify broadcaster |
| `webhook_worker_transient_db_errors_total` | Counter | `phase` | Reliability | lower better | Transient database errors a worker absorbed (backed off and retried) |
| `webhook_breaker_sync_fallback_total` | Counter | — | Circuit Breaker | lower better | Times the breaker transition persistence queue fell back to synchronous |

## Database Pool Metrics

| Metric | Type | Category | Direction | Description |
|---|---|---|---|---|
| `webhook_pg_pool_max_open` | GaugeFunc | Database | — | Maximum number of open connections to the database |
| `webhook_pg_pool_open` | GaugeFunc | Database | open < max | Current number of open connections |
| `webhook_pg_pool_in_use` | GaugeFunc | Database | in_use < max | Current number of open connections in use |
| `webhook_pg_pool_idle` | GaugeFunc | Database | — | Current number of idle connections |
| `webhook_pg_pool_wait_count` | GaugeFunc | Database | lower better | Total number of connections waited for |
| `webhook_pg_pool_wait_duration_seconds` | GaugeFunc | Database | lower better | Total time blocked waiting for a new connection |

## Receiver Metric (benchmark fixture)

| Metric | Type | Description |
|---|---|---|
| `webhook_requests_received_total` | Plaintext counter | HTTP requests received by the benchmark webhook receiver (not a Prometheus client metric — defined in `cmd/webhook-receiver/main.go`) |

## Metric Categories

| Category | Meaning |
|---|---|
| **Delivery** | Throughput and fan-out volume |
| **Latency** | End-to-end and sub-component durations |
| **Reliability** | Retries, failures, and DLQ pressure |
| **Capacity** | Concurrency and resource usage |
| **Efficiency** | Claim path performance |
| **Backlog** | Queue depth and starvation signals |
| **Contention** | Resource contention and blocking |
| **Circuit Breaker** | Partner isolation health |
| **Wakeup** | pg_notify wakeup delivery |
| **Database** | Connection pool health |

## Direction Legend

| Direction | Meaning |
|---|---|
| **lower better** | Decreasing values indicate better performance/health |
| **higher better** | Increasing values indicate better performance/health |
| **—** | Volume metric, config constant, or state indicator; no good/bad polarity |
| **open < max** | Healthy when open connections stay below the configured max |
| **in_use < max** | Healthy when in-use connections stay below the configured max |
