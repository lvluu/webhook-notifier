# Benchmarks

The benchmark suite mirrors the four main rows in
`compose/grafana/dashboards/notifier.json`.

## Run

```bash
just bench-reliability
just bench-fairness
just bench-scalability
just bench-partner-protection
```

Direct runner form:

```bash
BENCH_PROFILE=stress go run ./cmd/bench reliability
BENCH_PROFILE=stress go run ./cmd/bench fairness
BENCH_PROFILE=stress go run ./cmd/bench scalability
BENCH_PROFILE=stress go run ./cmd/bench partner-protection
```

Use `BENCH_PROFILE=dev` for local iteration. Stress profiles produce
report-quality results. Each run starts an isolated Postgres testcontainer and
applies migrations unless `BENCH_DATABASE_URL` is set.

## Proof Map

| Dashboard category | Benchmark | Primary gates |
|---|---|---|
| Reliability | `reliability` | terminal completion, transient recovery, retry distribution, 4xx fast-fail, DLQ classification |
| Fairness | `fairness` | Small-vs-Whale First-Attempt Queue Latency, throughput by Customer class, zero duplicate Attempt tuples |
| Scalability | `scalability` | throughput growth across real notifier replicas, claim atomicity, sparse discovery, dense workload scaling |
| Partner Protection | `partner-protection` | breaker transitions, dead-partner Attempt cap, skipped backlog, healthy-Customer queue latency |

**First-Attempt Queue Latency** is the canonical fairness metric: P95 of
`delivery_attempts.sent_at` for Attempt 1 minus `deliveries.created_at`.

## Profiles

`bench/profiles.yaml` contains exactly one `dev` and one `stress` profile for
each benchmark. The shared schema keeps worker, retry, discovery, and breaker
configuration explicit and reviewable.

## Database Isolation

- `BENCH_DATABASE_URL` uses an existing database instead of a container.
- `BENCH_KEEP_DATABASE=1` keeps the benchmark container alive.
- `BENCH_DATABASE_INFO_FILE=/path` changes where the live DSN is written.

Without an override, the DSN is written to `/tmp/bench-postgres.dsn` while the
scenario is running.

## Reports

Each benchmark rewrites one committed report:

- `bench/results/reliability.md`
- `bench/results/fairness.md`
- `bench/results/scalability.md`
- `bench/results/partner-protection.md`

## Dashboard Relationship

The Grafana dashboard is the live observability view. The benchmark harness is
the repeatable proof path. Benchmark reports use database and receiver evidence
corresponding to the dashboard signals: terminal outcomes and DLQ behavior,
Customer-class latency, claim/throughput scaling, and partner-isolation health.

The Compose chaos profile remains useful for interactive dashboard inspection,
but it is not the report-quality benchmark environment.

## Database Health

For sustained-load and query-level checks:

```bash
psql "$DATABASE_URL" -f bench/scripts/issue54_health.sql
psql "$DATABASE_URL" -f bench/scripts/db_query_health.sql
```

Thresholds and operational guidance live in
`docs/operations/db-query-monitoring.md`.
