# Webhook Notifier

A reliable, horizontally scalable webhook delivery service with per-Customer
fair scheduling. It accepts business Events from a multi-tenant platform and
POSTs them to Integration Partners' configured URLs, with retries, signing, and
protection against any one Customer monopolising delivery capacity.

## The Scenario

Your platform has Customers (e.g., an email marketing SaaS). Each Customer uses
Integration Partners (Slack, Mailchimp, custom APIs) to receive webhook
notifications when things happen — a subscriber joins, a campaign ends, a
payment fails. **Webhook Notifier** ensures every partner URL gets a reliable,
signed HTTP POST, with fair scheduling so one high-volume Customer can't starve
the others. It also handles retries, circuit breakers for dead partners, and
per-host concurrency limits.

## Interview Review Path

For a first review, follow this order:

1. [Assignment requirements](REQUIREMENTS.md) — what the solution must prove.
2. [Rendered architecture](docs/animations/overall-architecture.svg) — the
   service and data flow at a glance.
3. [Design](docs/DESIGN.md) — what each architectural choice provides and why.
   A printable version with all rendered diagrams is available as
   [DESIGN.pdf](docs/DESIGN.pdf).
4. `just demo` — a two-minute end-to-end proof.
5. [Benchmark results](bench/RESULTS.md) — measured evidence and architecture
   settings for each scenario.
6. [Known limitations](#known-limitations) — deliberate PoC boundaries and
   production follow-ups.

Supporting references: [Configuration](docs/CONFIGURATION.md) ·
[Vocabulary](CONTEXT.md) · [Architecture decisions](docs/adr/)

## Status

PoC implementation complete. Four benchmark scenarios cover Reliability,
Fairness, Scalability, and Partner Protection. Committed stress evidence is
available for Fairness and Scalability; Reliability and Partner Protection
currently have dev-profile evidence and still require committed stress runs.

The PRD lives at [#1](https://github.com/lvluu/webhook-notifier/issues/1).

## Prerequisites

| Tool | Version | Install |
| --- | --- | --- |
| Go | 1.24+ | https://go.dev/dl/ |
| Docker + Docker Compose | recent | https://docs.docker.com/engine/install/ |
| `just` | 1.40+ | https://github.com/casey/just#installation |
| `gh` (optional) | recent | https://cli.github.com/ — only for issue triage / PR flow |

Everything else needed for normal build and test work (`golangci-lint`,
`golang-migrate`) is fetched on first use by the corresponding `just` recipe.
Maintaining rendered diagrams additionally requires Node.js and `npx`.

## Quick Demo

The fastest proof path is self-contained. It resets the Compose stack, starts
two notifier replicas, registers one Whale and one Small Customer, submits 30
Events, verifies all 90 resulting Deliveries, checks notifier health, and tears
the stack down.

```bash
just demo
```

To inspect the running stack and add Grafana after the proof, keep it alive:

```bash
DEMO_KEEP_STACK=1 just demo
just stack-observe
# Grafana: http://localhost:3000 (admin/admin)
just stack-down
```

## Developer Workflow

```bash
# 1. Clone
git clone https://github.com/lvluu/webhook-notifier.git
cd webhook-notifier

# 2. Start Postgres
just up

# 3. Apply migrations
just migrate

# 4. Build everything
just build

# 5. Run unit tests (fast, no Docker needed)
just test-unit

# 6. Run all tests (including integration, requires Docker running)
just test

# 7. Lint
just lint

# 8. Tear down
just down
```

Run `just` with no arguments to see all recipes.

## Benchmarks

The benchmark harness is the authoritative measured proof path. Recipes default
to the smaller `dev` profile; pass `stress` explicitly for report-quality runs.

```bash
just bench-reliability               # dev profile
just bench-fairness stress           # report-quality profile
just bench-scalability stress
just bench-partner-protection stress
```

Direct runner form:

```bash
BENCH_PROFILE=stress go run ./cmd/bench fairness
```

See [bench/README.md](bench/README.md) for all scenarios and profile-specific
report naming, and [bench/RESULTS.md](bench/RESULTS.md) for the current index.

## Low-Resource Compose Dev

Start the lightweight Compose stack only when you want the app processes locally:

```bash
just stack-up
```

This starts:

- Postgres 17 with migrations applied during first container initialization.
- PgBouncer in transaction-pooling mode between notifier replicas and Postgres.
- One scaled `notifier` replica by default.
- Caddy on `localhost:8080` as the host-facing ingress for `POST /api/customers` and `POST /api/events`.
- A webhook receiver fixture on `localhost:8081`, with receiver metrics on `localhost:9092`.

Start Grafana and Prometheus only when you need dashboards:

```bash
just stack-observe
```

That adds:

- Prometheus on `localhost:9091`.
- Grafana on `localhost:3000`.

## Scaled Compose Stack

For a smoke test closer to production topology, build the images and run three
notifier replicas:

```bash
just stack-build
just stack-up 3
just stack-smoke
```

This provides:

- Postgres 17 with migrations applied during first container initialization.
- PgBouncer in transaction-pooling mode with prepared-statement tracking
  between notifier replicas and Postgres.
- Three scaled `notifier` replicas when you opt in.
- Caddy on `localhost:8080` as the host-facing ingress for `POST /api/customers` and `POST /api/events`.
- A webhook receiver fixture on `localhost:8081`, with receiver metrics on `localhost:9092`.

Useful stack commands:

```bash
just stack-status          # container status
just stack-logs notifier   # follow one service
just stack-observe         # start Prometheus and Grafana
just stack-chaos 3         # opt-in aggressive subscriber.created whale/small load
just stack-down            # remove containers and volumes
```

`stack-smoke` verifies wiring with a single Event; it is not a load test. Run
`just stack-chaos 3` only when interactive load and dashboard inspection are
needed. Report-quality evidence comes from the isolated benchmark harness.

Prometheus discovers scaled notifier replicas through Docker DNS and scrapes
each replica's `:9090` metrics endpoint directly. Caddy fronts the business API
and exposes namespaced health access at `/_notifier/health`; it is not another
notifier process. Transactional traffic uses PgBouncer, while advisory
`LISTEN/NOTIFY` connections use direct Postgres connections so wake hints do not
compete for pooled sessions.

## Configuration

Defaults work out of the box for local development. Override via environment variables (or a `.env` file in the repo root — `just` auto-loads it):

| Var | Default | Purpose |
| --- | --- | --- |
| `DATABASE_URL` | `postgres://notifier:notifier@localhost:5432/notifier?sslmode=disable` | Used by `just migrate` and the service. |
| `BENCH_DATABASE_URL` | unset | Optional benchmark override; skips the testcontainer bootstrap and uses the provided DSN as-is. |
| `BENCH_KEEP_DATABASE` | unset | When set to `1` or `true`, keeps the benchmark Postgres container alive after the run. The current connection string is always written to `/tmp/bench-postgres.dsn` unless `BENCH_DATABASE_INFO_FILE` overrides the path. |
| `BENCH_DATABASE_INFO_FILE` | unset | Optional path for the benchmark Postgres connection string; useful when you want the container left running and the DSN written somewhere explicit. |

A `.env` example:

```env
DATABASE_URL=postgres://notifier:notifier@localhost:5432/notifier?sslmode=disable
```

## Layout

```
.
├── REQUIREMENTS.md       # assignment requirements (de-branded)
├── CONTEXT.md             # domain glossary — read first
├── docs/
│   ├── DESIGN.md          # architecture, components, bench scenarios
│   ├── CONFIGURATION.md   # full configuration reference
│   ├── WORKFLOW.md        # step-by-step execution path + tuning checklist
│   └── adr/               # numbered architectural decisions
├── cmd/
│   ├── notifier/          # service entrypoint
│   └── bench/             # four assignment-aligned benchmarks
├── internal/
│   ├── ingest/            # Event ingest + fan-out to Deliveries
│   ├── scheduler/         # Scheduler interface + round-robin impl (ADR-0001)
│   ├── retry/             # RetryPolicy (ADR-0003)
│   ├── signer/            # HMAC-SHA256 outbound signing
│   ├── worker/            # claim, sign, POST, retry; per-host limiter
│   ├── outbox/            # Postgres claim semantics (ADR-0002)
│   ├── registry/          # (Customer, Event type) → []Webhook ingest cache
│   ├── breaker/           # in-memory decisions + async transition persistence
│   └── observe/           # slog, Prometheus, /health, pprof
├── bench/
│   ├── RESULTS.md         # scenario results index
│   ├── receiver/          # configurable test HTTP server fixture
│   ├── scenarios/         # benchmark scenario drivers
│   └── results/           # committed scenario reports
├── compose/caddy/         # local ingress config for scaled Compose stack
├── compose/grafana/       # provisioned datasource and notifier dashboard
├── compose/prometheus/    # scrape config for notifier + receiver metrics
├── migrations/            # golang-migrate SQL files
├── compose/docker-compose.yml   # production-like local stack
└── justfile
```

## Known Limitations

- Webhook management APIs are demo helpers only; production auth and customer
  self-service management are out of scope.
- Host concurrency limiting is process-local. A horizontally scaled production
  deployment needs fleet-wide coordination to enforce a global partner cap.
- Scheduler state is process-local. Postgres claim semantics prevent duplicate
  processing, while DB-backed discovery provides approximate cluster-wide
  fairness rather than a single global ordering.
- The default runtime uses equal-share round-robin. Weighted scheduling exists
  behind an internal runtime option, is not exposed by the notifier CLI, and
  should be profiled before becoming the default.
- Delivery `MaxAge` expiration is implemented in `RetryPolicy` but defaults to
  disabled and is not exposed by the notifier CLI.
- Postgres is the queue for this PoC. Sustained very high throughput may need
  partitioning or a broker-backed queue after benchmark evidence identifies
  claim contention as the ceiling.
- Strict per-Customer FIFO and multi-region delivery are intentionally out of
  scope.

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| `just test` hangs or fails with container errors | Docker not running or Postgres already on port 5432 | Start Docker; check `lsof -i :5432` for conflicts |
| `just migrate` fails with `connection refused` | Postgres not started yet | Run `just up` first and wait for "done" |
| `just demo` exits with `curl: connection refused` | Docker daemon is unavailable or Caddy did not become ready | Run `docker ps`, then inspect `just stack-logs caddy` if Docker is running |
| `just build` fails with missing packages | Stale `go.mod` | Run `just tidy` to sync dependencies |
| `just lint` is slow on first run | golangci-lint downloading | First run is normal; subsequent runs are fast |
| Integration tests flake | Testcontainers pulling Postgres image | Pull manually: `docker pull postgres:16-alpine` |

### Quick health check

```bash
docker ps | grep notifier     # services running?
curl -sf localhost:8080/_notifier/health # notifier infrastructure healthy?
curl -sf localhost:8080/                  # Caddy business ingress up?
```

## CI

GitHub Actions runs `go vet`, `just build`, `just test`, and `golangci-lint` on every push and PR. See `.github/workflows/ci.yml`.
