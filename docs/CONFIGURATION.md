# Webhook Notifier — Configuration Reference

## Quick Start

Defaults work out of the box for local development. All settings have sensible
zero-config defaults. Override via CLI flags (notifier binary), environment
variables (Compose stack), or benchmark profiles (`bench/profiles.yaml`).

## Notifier Binary

All flags for `cmd/notifier`:

### Database

| Flag | Env | Default | Purpose |
|---|---|---|---|
| `--db` | `DATABASE_URL` | `postgres://notifier:notifier@localhost:5432/notifier?sslmode=disable` | Postgres connection string |
| `--max-conns` | — | `workerCount + 10` | Max open DB connections; zero uses auto-derived default |

### Concurrency & Throughput

| Flag | Default | Purpose |
|---|---|---|
| `--workers` | `64` | Number of delivery worker goroutines |
| `--min-batch` | `32` | Minimum claim batch size per Customer per scheduler pick |
| `--multi-pick` | `1` | Customers to batch per claim query (1 = single, >1 = `ClaimNextBatch`) |
| `--host-cap` | `20` | Max concurrent in-flight requests per Integration Partner hostname |

### Timing

| Flag | Default | Purpose |
|---|---|---|
| `--discovery-interval` | `1s` | Claimable Customer discovery poll interval |
| `--empty-claim-backoff` | `20ms` | Pause after an empty claim cycle (no work found) |
| `--pending-sample-interval` | `5s` | Sample interval for the pending-delivery backlog gauge (0 disables) |

### Addressing

| Flag | Default | Purpose |
|---|---|---|
| `--addr` | `:8080` | Business HTTP server address (ingest API) |
| `--metrics-addr` | `:9090` | Metrics/health/pprof HTTP server address |

### Advanced

| Flag | Default | Purpose |
|---|---|---|
| `--replica-id` | _empty_ | Optional replica identity for `X-Notifier-Replica` diagnostic header |
| `--claim-query-mode` | `split` | Claim query mode: `split` (fast-path + fallback) or `legacy` |
| `--enable-pg-notify-wakeups` | `true` | Use Postgres `LISTEN/NOTIFY` wakeups in addition to polling |
| `--pg-notify-db` | _empty_ | Optional direct `DATABASE_URL` for `LISTEN/NOTIFY` connections (uses main DB pool when empty) |
| `--pg-notify-channel` | `webhook_customer_ready` | Postgres `LISTEN/NOTIFY` channel for Customer wakeups |

## Retry Policy

Configured in `internal/retry/retry.go`. The PoC uses hardcoded ADR-0003
defaults; production would expose these as flags.

| Constant | Default | Purpose |
|---|---|---|
| `DefaultMaxAttempts` | `8` | Maximum delivery Attempts before terminal exhaustion |
| `DefaultBaseDelay` | `1s` | First retry delay before decorrelated jitter |
| `DefaultCapDelay` | `5m` | Maximum retry delay after jitter |

Behavior:
- **2xx** → terminal success.
- **4xx** (except 408, 429) → terminal dead (fast-fail).
- **408** → retryable.
- **429** → honours `Retry-After` header value.
- **5xx** → retryable with decorrelated jitter.
- Network error / timeout → retryable.
- Attempt exhaustion → terminal dead.
- `MaxAge` exceeded → terminal dead regardless of remaining attempts.

## Circuit Breaker

Configured in `internal/outbox/outbox.go`. Per-Webhook state persisted in
`webhooks` table.

| Constant | Default | Purpose |
|---|---|---|
| `DefaultBreakerThreshold` | `20` | Consecutive failures before opening breaker |
| `DefaultBreakerCooldown` | `5m` | Cooldown before first half-open probe |
| `DefaultBreakerMaxCooldown` | `15m` | Maximum cooldown after repeated probe failures |

## Host Concurrency Cap

Per-process in-memory semaphore (`internal/worker/host_limiter.go`).
Production deployments need fleet-wide coordination (see DESIGN.md).

| Flag | Default | Purpose |
|---|---|---|
| `--host-cap` | `20` | Max concurrent in-flight Attempts per partner hostname |

## Database Pool

| Setting | Default | Purpose |
|---|---|---|
| `MaxOpenConns` | `workers + 10` | Maximum pool size (auto-derived) |
| `MaxIdleConns` | `workers + 5` | Maximum idle connections |
| `ConnMaxLifetime` | `30m` | Maximum connection lifetime |

When running behind PgBouncer in transaction pooling mode, the pool is
configured with `MAX_PREPARED_STATEMENTS` non-zero and pgx uses its
cached-statement execution mode to avoid repeated server parse/plan work.

## Subscription Registry Cache

| Setting | Default | Purpose |
|---|---|---|
| LRU capacity | `10,000` | Cached Customer → Webhook mappings |
| TTL | `30s` | Cache entry expiry; stale reads re-fetch from DB |

## Claimable Customer Discovery

Configured through `workerRuntimeConfig.Discovery`:

| Setting | Default | Purpose |
|---|---|---|
| `Interval` | `1s` | Discovery poll interval |
| `EmptyBackoff` | `5s` | Backoff when no claimable Customers found |
| `ErrorBackoff` | `1s` | Backoff on discovery query error |
| `MaxBackoff` | `30s` | Maximum discovery backoff |
| `Limit` | `1000` | Max Customers returned per discovery query |

## Observability Endpoints

| Endpoint | Port (default) | Purpose |
|---|---|---|
| `/metrics` | `:9090` | Prometheus scrape endpoint |
| `/health` | `:9090` | Liveness check |
| `/ready` | `:9090` | Readiness check (DB + schema version) |
| `/debug/pprof/` | `:9090` | Go pprof profiling (disabled by default; enable with `cfg.Pprof`) |

### Key Metrics

| Metric | Type | Labels |
|---|---|---|
| `webhook_deliveries_total` | Counter | `status`, `customer_class` |
| `webhook_delivery_latency_seconds` | Histogram | `customer_class` |
| `webhook_attempts_per_delivery` | Histogram | — |
| `webhook_dlq_total` | Counter | `reason` |
| `webhook_scheduler_pick_duration_seconds` | Histogram | — |
| `webhook_host_semaphore_wait_seconds` | Histogram | `host` |
| `webhook_breaker_state` | Gauge | `webhook_id`, `state` |
| `webhook_breaker_trips_total` | Counter | `reason` |
| `webhook_replicas` | Gauge | `replica_id` |

See [docs/observability/dashboard.md](observability/dashboard.md) for Grafana
panel documentation and [docs/observability/alerts.yml](observability/alerts.yml)
for Prometheus alerting rules.

## Compose Stack

| Service | Port | Purpose |
|---|---|---|
| `postgres` | `5432` | PostgreSQL 17 |
| `pgbouncer` | `6432` | PgBouncer transaction pool |
| `notifier` | `:8080` (×N, via Caddy) | Notifier replicas |
| `caddy` | `:8080` (ingest), `:2019` (admin) | Reverse proxy ingress |
| `webhook-receiver` | `:8081` (deliveries), `:9092` (metrics) | Test webhook receiver |
| `prometheus` | `:9091` | Metrics collection |
| `grafana` | `:3000` | Dashboards |

Profiles: `--profile observe`, `--profile chaos`, `--profile retention`,
`--profile production`.

## Benchmark Profiles

Defined in `bench/profiles.yaml`:

| Profile | Use case |
|---|---|
| `dev` | Fast smoke tests, CI-friendly |
| `stress` | Throughput/fairness baselines |

## Tuning Order

1. **Reliability first**: verify zero duplicate `(delivery_id, attempt_number)`
   tuples under load and acceptable dead-letter rate.
2. **Throughput second**: sweep `worker_count × db_conns × min_batch`.
3. **Fairness third**: verify Small Customer P95 first-attempt queue latency
   stays within SLO under concurrent Whale load.
4. **Infrastructure fourth**: DB sizing, pooling, partitioning/sharding.

See [docs/WORKFLOW.md](WORKFLOW.md) for the full execution path and tuning
checklist.
