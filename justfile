set shell := ["bash", "-cu"]
set dotenv-load := true

database_url := env_var_or_default("DATABASE_URL", "postgres://notifier:notifier@localhost:5432/notifier?sslmode=disable")
migrate      := "go run -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@v4.17.1"
stack_services := "postgres pgbouncer notifier caddy webhook-receiver"
observe_services := "prometheus grafana"
compose_cmd := "docker compose -f compose/docker-compose.yml --project-directory ."

# list available recipes
default:
    @just --list

# start Postgres
up:
    {{compose_cmd}} up -d postgres
    {{compose_cmd}} exec -T postgres bash -c 'until pg_isready -U notifier -d notifier; do sleep 1; done'

# stop and remove containers + volumes
down:
    {{compose_cmd}} down -v

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

# build production-like stack images
stack-build:
    {{compose_cmd}} build

# start low-resource dev stack with N notifier replicas behind Caddy
stack-up n="1":
    {{compose_cmd}} up -d --scale notifier={{n}} {{stack_services}}

# same as stack-up; Caddy exposes host-accessible ingest on localhost:8080
stack-up-host n="3":
    just stack-up {{n}}

# start observability stack on top of the low-resource dev stack
stack-observe:
    {{compose_cmd}} --profile observe up -d {{observe_services}}

# start opt-in chaos load aligned to the smaller affordable mix:
# 50 whales @ 250/s + 5k small @ 50/s (~300 events/s total)
stack-chaos n="3":
    {{compose_cmd}} up -d --scale notifier={{n}} {{stack_services}}
    {{compose_cmd}} --profile chaos up -d chaos-whales chaos-small

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
    {{compose_cmd}} exec -T postgres psql -U notifier -d notifier -P pager=off \
      -c "SELECT status, count(*) FROM deliveries GROUP BY status ORDER BY status; SELECT count(*) AS attempts FROM delivery_attempts;"
    curl -fsS http://localhost:9092/metrics

# show running stack containers
stack-status:
    {{compose_cmd}} ps

# stop and remove the full stack, including volumes
stack-down:
    {{compose_cmd}} down -v

# follow stack logs, optionally for one service
stack-logs service="":
    {{compose_cmd}} logs --tail=100 -f {{service}}

# rebuild images and recreate notifier replicas after code changes
stack-rebuild n="3":
    {{compose_cmd}} build
    {{compose_cmd}} up -d --scale notifier={{n}} --no-deps --force-recreate notifier

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

# scenario4 throughput matrix: workers x DB pool size
bench-4-matrix:
    BENCH_PROFILE=stress \
    BENCH_SCENARIO4_MATRIX_WORKERS=8,16,32,48,64 \
    BENCH_SCENARIO4_MATRIX_DB_CONNS=16,32,48,64,96 \
    go run ./cmd/bench scenario4

# summarize scenario4 matrix files and recommend a throughput knee
bench-4-knee:
    bash ./bench/scripts/scenario4_knee.sh

# dead partner circuit breaker + pool protection (issue #27)
bench-5:
    BENCH_PROFILE=stress go run ./cmd/bench scenario5

# assignment-aligned production-like mixed workload
bench-6:
    BENCH_PROFILE=stress go run ./cmd/bench scenario6

# retain terminal deliveries into delivery_history
delivery-history-retention:
    go run ./cmd/delivery-history-retention

# watch sustained load and fail if pending backlog drops below the floor
sustained-load-watch floor="500" duration="900" interval="30":
    bash ./bench/scripts/sustained_load_watch.sh {{floor}} {{duration}} {{interval}}
