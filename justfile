set shell := ["bash", "-cu"]
set dotenv-load := true

database_url := env_var_or_default("DATABASE_URL", "postgres://notifier:notifier@localhost:5432/notifier?sslmode=disable")
migrate      := "go run -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@v4.17.1"

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
