# Webhook Notifier

A reliable, horizontally scalable webhook delivery service with per-tenant fair scheduling. Receives business Events from a multi-tenant platform and POSTs them to Integration Partners' configured URLs, with retries, signing, and protection against any one Customer monopolising delivery capacity.

Design: [docs/DESIGN.md](docs/DESIGN.md) · Vocabulary: [CONTEXT.md](CONTEXT.md) · Decisions: [docs/adr/](docs/adr/)

## Status

PoC scaffolding stage. Track progress via GitHub issues — work is sliced into vertical tracer-bullet tickets, each independently grabbable.

```bash
gh issue list --label ready-for-agent --state open
```

The PRD lives at [#1](https://github.com/lvluu/flodesk-home-assignment/issues/1).

## Prerequisites

| Tool | Version | Install |
| --- | --- | --- |
| Go | 1.24+ | https://go.dev/dl/ |
| Docker + Docker Compose | recent | https://docs.docker.com/engine/install/ |
| `just` | 1.40+ | https://github.com/casey/just#installation |
| `gh` (optional) | recent | https://cli.github.com/ — only for issue triage / PR flow |

Everything else (`golangci-lint`, `golang-migrate`) is fetched on first use by the corresponding `just` recipe.

## Quickstart

```bash
# 1. Clone
git clone https://github.com/lvluu/flodesk-home-assignment.git
cd flodesk-home-assignment

# 2. Start Postgres
just up

# 3. Apply migrations (currently empty — will fill as #4 lands)
just migrate

# 4. Build everything
just build

# 5. Run tests
just test

# 6. Lint
just lint

# 7. Tear down
just down
```

Run `just` with no arguments to see all recipes.

## Production-like stack

For a runnable demo closer to production topology, start the full Compose stack:

```bash
just stack-build
just stack-up 3
just stack-smoke
```

This starts:

- Postgres 17 with migrations applied during first container initialization.
- PgBouncer in transaction-pooling mode between notifier replicas and Postgres.
- Three scaled `notifier` replicas by default.
- Caddy on `localhost:8080` as the host-facing ingress for `POST /api/customers` and `POST /api/events`.
- A webhook receiver fixture on `localhost:8081`, with receiver metrics on `localhost:9092`.
- Prometheus on `localhost:9091`.
- Grafana on `localhost:3000`.

Useful stack commands:

```bash
just stack-status          # container status
just stack-logs notifier   # follow one service
just stack-chaos 3         # opt-in aggressive subscriber.created whale/small load
just stack-down            # remove containers and volumes
```

Prometheus discovers scaled notifier replicas through Docker DNS and scrapes each replica's `:9090` metrics endpoint directly. Caddy only fronts the business ingest API; it is not another notifier process and does not change worker scale. The opt-in chaos load emits `subscriber.created` Events from Whale Customers (`weight=3`) and Small Customers (`weight=1`) so fairness and queue-depth metrics map directly to the assignment scenario.

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
├── CONTEXT.md           # domain glossary — read first
├── docs/
│   ├── DESIGN.md        # architecture, components, bench scenarios
│   └── adr/             # numbered architectural decisions
├── cmd/
│   ├── notifier/        # service entrypoint
│   └── bench/           # benchmark scenario runner
├── internal/
│   ├── ingest/          # Event ingest + fan-out to Deliveries
│   ├── scheduler/       # Scheduler interface + round-robin impl (ADR-0001)
│   ├── retry/           # RetryPolicy (ADR-0003)
│   ├── signer/          # HMAC-SHA256 outbound signing
│   ├── worker/          # claim, sign, POST, retry; per-host limiter
│   ├── outbox/          # Postgres claim semantics (ADR-0002)
│   ├── registry/        # Customer → []Webhook lookup + LRU cache
│   └── observe/         # slog, Prometheus, /health, pprof
├── bench/
│   ├── receiver/        # configurable test HTTP server fixture
│   ├── scenarios/       # the five benchmark scenarios
│   └── results/         # committed scenario reports
├── caddy/               # local ingress config for scaled Compose stack
├── grafana/             # provisioned datasource and notifier dashboard
├── prometheus/          # scrape config for notifier + receiver metrics
├── migrations/          # golang-migrate SQL files
├── docker-compose.yml   # production-like local stack
└── justfile
```

## Demo polish ideas

The current stack demonstrates scale, fairness, delivery, and observability. To make the demo more interview-impressive, the highest-leverage next steps are:

- Add a one-command scripted demo that resets the stack, starts `stack-chaos`, opens Grafana, and prints a short "what to watch" checklist.
- Add a Grafana row for the fairness story: pending Deliveries by Customer class, scheduler picks, successful Deliveries/sec, and Postgres pool usage side by side.
- Add a "bad partner" toggle to the receiver so the demo can show healthy Customers continuing while one Integration Partner fails.
- Add a short terminal summary after chaos runs: total Deliveries, success rate, p95 latency, DLQ count, and receiver request count.
- Add a Caddy access log panel or simple request counter so the ingress layer is visible without distracting from notifier internals.

## Working across machines

This repo is intentionally machine-portable:

- All tooling versions are pinned (`go.mod`, `justfile`, GitHub Actions workflow, `.golangci.yml`).
- Postgres runs in Docker — no host install required.
- No secrets in the repo. `.env` is git-ignored.
- `golangci-lint` and `golang-migrate` are installed on-demand by `just lint` / `just migrate` via `go install` and `go run` respectively, so a fresh machine needs only Go + Docker + just.

To pick up work on a new machine:

```bash
git clone <repo>
cd flodesk-home-assignment
git config user.name "lvluu"
git config user.email "luuvanloi@duck.com"

# Optional: restore the agent skill set used in this project from skills-lock.json
# (only needed if you want the same Matt Pocock skills available to your agent here)
npx -y skills@latest experimental_install

just up && just migrate && just test
gh issue list --label ready-for-agent --state open   # see what's next
```

### Agent skills

This repo uses a pinned set of Matt Pocock engineering/productivity skills (`to-prd`, `to-issues`, `grill-with-docs`, `triage`, `tdd`, etc.) declared in `skills-lock.json`. The lockfile pins each skill's source repo, path, and content hash so the same set is reproducible across machines.

```bash
npx -y skills@latest experimental_install   # restore from skills-lock.json
npx -y skills@latest list                   # see what's installed
npx -y skills@latest update                 # bump to latest + refresh lock
```

Per-repo agent configuration (issue tracker, triage labels, domain doc layout) lives in [CLAUDE.md](CLAUDE.md) and [docs/agents/](docs/agents/). Re-run `/setup-matt-pocock-skills` only if you want to switch issue tracker or label vocabulary.

## CI

GitHub Actions runs `go vet`, `just build`, `just test`, and `golangci-lint` on every push and PR. See `.github/workflows/ci.yml`.
