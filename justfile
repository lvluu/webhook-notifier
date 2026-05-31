set shell := ["bash", "-cu"]
set dotenv-load := true

database_url := env_var_or_default("DATABASE_URL", "postgres://notifier:notifier@localhost:5432/notifier?sslmode=disable")
migrate      := "go run -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@v4.17.1"
stack_services := "postgres pgbouncer notifier caddy webhook-receiver prometheus grafana"

# list available recipes
default:
    @just --list

# start Postgres
up:
    docker compose up -d postgres
    docker compose exec -T postgres bash -c 'until pg_isready -U notifier -d notifier; do sleep 1; done'

# stop and remove containers + volumes
down:
    docker compose down -v

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

# run unit + integration tests
test:
    go test ./... -race -count=1

# run golangci-lint (installs locally if missing)
lint:
    #!/usr/bin/env bash
    set -euo pipefail
    GOBIN=$(go env GOPATH)/bin
    if [[ ! -x "$GOBIN/golangci-lint" ]] || ! "$GOBIN/golangci-lint" --version | grep -q 'version 2\.12\.2'; then
        go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2
    fi
    "$GOBIN/golangci-lint" run

# tidy go.mod
tidy:
    go mod tidy

# build production-like stack images
stack-build:
    docker compose build

# start production-like stack with N notifier replicas behind Caddy
stack-up n="3":
    docker compose up -d --scale notifier={{n}} {{stack_services}}

# same as stack-up; Caddy exposes host-accessible ingest on localhost:8080
stack-up-host n="3":
    just stack-up {{n}}

# start opt-in chaos load aligned to SCALABILITY_REVIEW peak mix:
# 50 whales @ 2.5k/s + 5k small @ 500/s (~3k events/s total)
stack-chaos n="3":
    docker compose up -d --scale notifier={{n}} {{stack_services}}
    docker compose --profile chaos up -d chaos-whales chaos-small

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
    docker compose exec -T postgres psql -U notifier -d notifier -P pager=off \
      -c "SELECT status, count(*) FROM deliveries GROUP BY status ORDER BY status; SELECT count(*) AS attempts FROM delivery_attempts;"
    curl -fsS http://localhost:9092/metrics

# show running stack containers
stack-status:
    docker compose ps

# stop and remove the full stack, including volumes
stack-down:
    docker compose down -v

# follow stack logs, optionally for one service
stack-logs service="":
    docker compose logs --tail=100 -f {{service}}

# rebuild images and recreate notifier replicas after code changes
stack-rebuild n="3":
    docker compose build
    docker compose up -d --scale notifier={{n}} --no-deps --force-recreate notifier

# end-to-end smoke (placeholder until #16)
demo: up migrate
    @echo "demo recipe stub — wired in issue #16"

# fairness under load + late-arrival reactivity (issue #12)
bench-1:
    BENCH_PROFILE=stress go run ./cmd/bench scenario1

# transient + recovering partners (issue #13)
bench-2:
    BENCH_PROFILE=stress go run ./cmd/bench scenario2

# terminal classification fast-fail (issue #14)
bench-3:
    BENCH_PROFILE=stress go run ./cmd/bench scenario3

# horizontal scale + atomicity + idempotency (issue #15)
bench-4:
    BENCH_PROFILE=stress go run ./cmd/bench scenario4

# dead partner circuit breaker + pool protection (issue #27)
bench-5:
    BENCH_PROFILE=stress go run ./cmd/bench scenario5

# retain terminal deliveries into delivery_history
delivery-history-retention:
    go run ./cmd/delivery-history-retention
