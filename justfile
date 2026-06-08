set shell := ["bash", "-cu"]
set dotenv-load := true

database_url := env_var_or_default("DATABASE_URL", "postgres://notifier:notifier@localhost:5432/notifier?sslmode=disable")
migrate      := "go run -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@v4.17.1"
compose      := "docker compose -f compose/docker-compose.yml --project-directory ."
compose_prod := "docker compose -f compose/docker-compose.yml -f compose/docker-compose.production.yml --project-directory ."

stack_base_services := "postgres pgbouncer notifier caddy webhook-receiver"
stack_observe_services := "prometheus grafana"
stack_chaos_services := "chaos-whales chaos-small"
stack_all_profiles := "--profile observe --profile chaos"
stack_prod_profiles := "--profile observe --profile chaos --profile production"

# list available recipes
default:
    @just --list

# start Postgres
up:
    {{compose}} up -d postgres
    {{compose}} exec -T postgres bash -c 'until pg_isready -U notifier -d notifier; do sleep 1; done'

# stop and remove containers + volumes
down:
    {{compose}} down -v

# apply migrations
migrate:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! compgen -G "./migrations/*.sql" >/dev/null; then
        echo "No SQL migrations found; skipping."
        exit 0
    fi
    {{migrate}} -path ./migrations -database "{{database_url}}" up

# roll back one migration step
migrate-down:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! compgen -G "./migrations/*.sql" >/dev/null; then
        echo "No SQL migrations found; skipping."
        exit 0
    fi
    {{migrate}} -path ./migrations -database "{{database_url}}" down 1

# build all binaries
build:
    go build ./...

# run all tests (unit + integration, requires Docker)
test:
    go test ./... -race -count=1

# run only unit tests (no Docker needed) — fast feedback (5-10s)
test-unit:
    go test -race -count=1 ./cmd/bench ./cmd/chaos ./cmd/webhook-receiver \
        ./internal/breaker ./internal/chaos ./internal/ingest \
        ./internal/observe ./internal/pgdsn ./internal/retry \
        ./internal/scheduler/roundrobin ./internal/scheduler/swrr \
        ./internal/signer ./internal/worker ./bench/receiver \
        ./bench/scenarios ./internal/testutil/retention

# run golangci-lint (installs locally if missing)
lint:
    #!/usr/bin/env bash
    set -euo pipefail
    GOBIN=$(go env GOPATH)/bin
    if [[ ! -x "$GOBIN/goimports" ]]; then
        go install golang.org/x/tools/cmd/goimports@v0.38.0
    fi
    find . -name '*.go' -not -path './.git/*' -print0 | xargs -0 "$GOBIN/goimports" -w
    if [[ ! -x "$GOBIN/golangci-lint" ]] || ! "$GOBIN/golangci-lint" --version | grep -q 'version 2\.12\.2'; then
        go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2
    fi
    "$GOBIN/golangci-lint" run

# tidy go.mod
tidy:
    go mod tidy

# top-N SQL by total execution time (needs running stack + pg_stat_statements)
db-top-queries n="20":
    {{compose}} exec -T postgres psql -U notifier -d notifier -P pager=off -P border=2 \
        -v limit={{n}} -f - < bench/scripts/pg_top_queries.sql

# reset pg_stat_statements counters (start a clean measurement window)
db-top-queries-reset:
    {{compose}} exec -T postgres psql -U notifier -d notifier -c "SELECT pg_stat_statements_reset();"

# build production-like stack images
stack-build:
    {{compose}} {{stack_all_profiles}} build

# start low-resource dev stack with N notifier replicas behind Caddy
stack-up n="1":
    {{compose}} up -d --scale notifier={{n}} {{stack_base_services}}

# same as stack-up; Caddy exposes host-accessible ingest on localhost:8080
stack-up-host n="3":
    just stack-up {{n}}

# start observability stack on top of the low-resource dev stack
stack-observe:
    {{compose}} --profile observe up -d {{stack_observe_services}}

# start the full local demo stack, including observability and chaos
stack-all n="1":
    {{compose}} {{stack_all_profiles}} up -d --scale notifier={{n}}

# start opt-in chaos load aligned to the smaller affordable mix:
# 50 whales @ 250/s + 5k small @ 50/s (~300 events/s total)
stack-chaos n="3":
    {{compose}} up -d --scale notifier={{n}} {{stack_base_services}}
    {{compose}} --profile chaos up -d {{stack_chaos_services}}

# smoke test Caddy ingest, PgBouncer, workers, and receiver delivery
stack-smoke:
    #!/usr/bin/env bash
    set -euo pipefail
    smoke_id="smoke-$(date +%s)"
    curl -fsS -X POST http://localhost:8080/api/customers \
      -H 'Content-Type: application/json' \
      -d "{\"id\":\"${smoke_id}\",\"weight\":1,\"url\":\"http://webhook-receiver:8080/hooks/${smoke_id}\"}" >/dev/null
    curl -fsS -X POST http://localhost:8080/api/events \
      -H 'Content-Type: application/json' \
      -d "{\"customer_id\":\"${smoke_id}\",\"event_name\":\"subscriber.created\",\"event_time\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",\"data\":{\"subscriber_id\":\"${smoke_id}-sub\"}}" >/dev/null
    sleep 3
    {{compose}} exec -T postgres psql -U notifier -d notifier -P pager=off \
      -c "SELECT status, count(*) FROM deliveries GROUP BY status ORDER BY status; SELECT count(*) AS attempts FROM delivery_attempts;"
    curl -fsS http://localhost:9092/metrics

# compact queue/backlog dashboard for watch -n
stack-status:
    COMPOSE="{{compose}}" bash ./bench/scripts/stack_status.sh

# stop and remove the full stack, including volumes and orphan containers
stack-down:
    {{compose}} {{stack_all_profiles}} down -v --remove-orphans

# follow stack logs, optionally for one service
stack-logs service="":
    {{compose}} logs --tail=100 -f {{service}}

# rebuild images and recreate notifier replicas after code changes
stack-rebuild n="3":
    {{compose}} build
    {{compose}} up -d --scale notifier={{n}} --no-deps --force-recreate notifier

# start production-representative stack (durable PG, resource limits, multi-host receivers)
stack-production:
    {{compose_prod}} --profile production up -d

# start full production stack with observe and chaos
stack-production-all:
    {{compose_prod}} {{stack_prod_profiles}} up -d --scale notifier=3

# stop and remove production stack, including volumes
stack-production-down:
    {{compose_prod}} {{stack_prod_profiles}} down -v

# end-to-end demo: reset stack, register customers, post events, verify delivery, tear down (under 2 min)
demo:
    COMPOSE="{{compose}}" STACK_ALL_PROFILES="{{stack_all_profiles}}" STACK_BASE_SERVICES="{{stack_base_services}}" bash ./bench/scripts/demo.sh

# ── Dashboard-aligned benchmarks ──────────────────────────────────────────

# retry recovery, terminal classification, and DLQ behavior
bench-reliability:
    BENCH_PROFILE=stress go run ./cmd/bench reliability

# Whale/Small scheduling fairness and queue latency
bench-fairness:
    BENCH_PROFILE=stress go run ./cmd/bench fairness

# multi-replica throughput and coordination
bench-scalability:
    BENCH_PROFILE=stress go run ./cmd/bench scalability

# dead-partner host limiting, breaker transitions, and healthy-Customer isolation
bench-partner-protection:
    BENCH_PROFILE=stress go run ./cmd/bench partner-protection

# retain terminal deliveries into delivery_history
delivery-history-retention:
    go run ./cmd/delivery-history-retention

# watch sustained load and fail if pending backlog drops below the floor
sustained-load-watch floor="500" duration="900" interval="30":
    bash ./bench/scripts/sustained_load_watch.sh {{floor}} {{duration}} {{interval}}
