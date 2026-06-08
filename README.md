# Webhook Notifier

A reliable, horizontally scalable webhook delivery service with per-tenant fair scheduling. Receives business Events from a multi-tenant platform and POSTs them to Integration Partners' configured URLs, with retries, signing, and protection against any one Customer monopolising delivery capacity.

## The Scenario

Your platform has Customers (e.g., an email marketing SaaS). Each Customer uses
Integration Partners (Slack, Mailchimp, custom APIs) to receive webhook
notifications when things happen — a subscriber joins, a campaign ends, a
payment fails. **Webhook Notifier** ensures every partner URL gets a reliable,
signed HTTP POST, with fair scheduling so one high-volume Customer can't starve
the others. It also handles retries, circuit breakers for dead partners, and
per-host concurrency limits.

Design: [docs/DESIGN.md](docs/DESIGN.md) · Requirements: [REQUIREMENTS.md](REQUIREMENTS.md) · Configuration: [docs/CONFIGURATION.md](docs/CONFIGURATION.md) · Vocabulary: [CONTEXT.md](CONTEXT.md) · Decisions: [docs/adr/](docs/adr/)

## Status

PoC complete. Four dashboard-aligned benchmarks validate Reliability,
Fairness, Scalability, and Partner Protection.

The PRD lives at [#1](https://github.com/lvluu/webhook-notifier/issues/1).

## Prerequisites

| Tool | Version | Install |
| --- | --- | --- |
| Go | 1.24+ | https://go.dev/dl/ |
| Docker + Docker Compose | recent | https://docs.docker.com/engine/install/ |
| `just` | 1.40+ | https://github.com/casey/just#installation |
| `gh` (optional) | recent | https://cli.github.com/ — only for issue triage / PR flow |

Everything else (`golangci-lint`, `golang-migrate`) is fetched on first use by the corresponding `just` recipe.

## Minimal Dev

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

# 8. Run the e2e demo
just demo

# 9. Tear down
just down
```

Run `just` with no arguments to see all recipes.

## Benchmarks

The benchmark harness is the authoritative proof path for fairness,
scalability, and reliability. Use the smaller `BENCH_PROFILE=dev` while
iterating locally and `BENCH_PROFILE=stress` for report-quality runs.

```bash
just bench-reliability        # retry, terminal failure, DLQ proof
just bench-fairness           # mixed Whale/Small fairness proof
just bench-scalability        # multi-replica scalability proof
just bench-partner-protection # host limiter and breaker isolation proof
```

Direct runner form:

```bash
BENCH_PROFILE=stress go run ./cmd/bench fairness
```

See [bench/README.md](bench/README.md) for all scenarios and
[bench/RESULTS.md](bench/RESULTS.md) for the current report index.

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

## Load Test Stack

For a runnable demo closer to production topology, add the heavier services on top of the dev stack:

```bash
just stack-build
just stack-up 3
just stack-smoke
```

This adds:

- Postgres 17 with migrations applied during first container initialization.
- PgBouncer in session-pooling mode between notifier replicas and Postgres.
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

Prometheus discovers scaled notifier replicas through Docker DNS and scrapes each replica's `:9090` metrics endpoint directly when you start the optional observability profile. Caddy only fronts the business ingest API; it is not another notifier process and does not change worker scale. Notifier replicas keep transactional traffic behind PgBouncer, while advisory `LISTEN/NOTIFY` connections use direct Postgres connections so wake hints do not compete for pooled sessions. The opt-in chaos load emits `subscriber.created` Events from Whale Customers (`class=3`) and Small Customers (`class=1`) so fairness and queue-depth metrics map directly to the assignment scenario.

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
│   └── bench/             # four dashboard-aligned benchmarks
├── internal/
│   ├── ingest/            # Event ingest + fan-out to Deliveries
│   ├── scheduler/         # Scheduler interface + round-robin impl (ADR-0001)
│   ├── retry/             # RetryPolicy (ADR-0003)
│   ├── signer/            # HMAC-SHA256 outbound signing
│   ├── worker/            # claim, sign, POST, retry; per-host limiter
│   ├── outbox/            # Postgres claim semantics (ADR-0002)
│   ├── registry/          # Customer → []Webhook lookup + LRU cache
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

## Demo

`just demo` starts the full dev stack (Docker Compose), registers a Whale and
Small Customer, posts Events, and verifies delivery — all in under 2 minutes.

Prerequisites: Docker, Docker Compose, and `just` must be installed. No manual
`just up` or `just migrate` needed — the demo handles everything.

```
just demo
```

What the demo shows:
- Two notifier replicas behind Caddy ingress
- A Whale Customer and a Small Customer registered with webhooks
- 20 Whale + 10 Small Events ingested concurrently (scheduler treats both equally via round-robin)
- Delivery processing with status breakdown and attempt counts
- Receiver metrics and notifier health check

For the full observability experience, start Grafana and Prometheus alongside:
```
just stack-observe
```
Grafana: `http://localhost:3000` (admin/admin) with the pre-built notifier dashboard.

## Known Limitations

- Webhook management APIs are demo helpers only; production auth and customer
  self-service management are out of scope.
- Host concurrency limiting is process-local. A horizontally scaled production
  deployment needs fleet-wide coordination to enforce a global partner cap.
- Scheduler state is process-local. Postgres claim semantics prevent duplicate
  processing, while DB-backed discovery provides approximate cluster-wide
  fairness rather than a single global ordering.
- The default runtime uses equal-share round-robin. Weighted scheduling exists
  behind an opt-in path and should be profiled before becoming the default.
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
| `just demo` exits with `curl: connection refused` | Docker daemon not running | `docker ps` to verify; start Docker first |
| `just build` fails with missing packages | Stale `go.mod` | Run `just tidy` to sync dependencies |
| `just lint` is slow on first run | golangci-lint downloading | First run is normal; subsequent runs are fast |
| Integration tests flake | Testcontainers pulling Postgres image | Pull manually: `docker pull postgres:16-alpine` |

### Quick health check

```bash
docker ps | grep notifier     # services running?
curl -sf localhost:9090/health # notifier alive?
curl -sf localhost:8080/       # Caddy ingress up?
```

## Working across machines

This repo is intentionally machine-portable:

- All tooling versions are pinned (`go.mod`, `justfile`, GitHub Actions workflow, `.golangci.yml`).
- Postgres runs in Docker — no host install required.
- No secrets in the repo. `.env` is git-ignored.
- `golangci-lint` and `golang-migrate` are installed on-demand by `just lint` / `just migrate` via `go install` and `go run` respectively, so a fresh machine needs only Go + Docker + just.

To pick up work on a new machine:

```bash
git clone <repo>
cd webhook-notifier
just up && just migrate && just test-unit
```

### See what needs work

```bash
gh issue list --label ready-for-agent --state open
```

## AI Agent Setup (Claude Code only)

This repo uses a pinned set of agent skills (`to-prd`, `to-issues`, `grill-with-docs`, `triage`, `tdd`, etc.) declared in `skills-lock.json`. Only needed if you use Claude Code with this repo.

```bash
npx -y skills@latest experimental_install   # restore from skills-lock.json
npx -y skills@latest list                   # see what's installed
```

Agent configuration (issue tracker, triage labels, domain docs) lives in [CLAUDE.md](CLAUDE.md) and [docs/agents/](docs/agents/). Humans can ignore these files.

## CI

GitHub Actions runs `go vet`, `just build`, `just test`, and `golangci-lint` on every push and PR. See `.github/workflows/ci.yml`.
