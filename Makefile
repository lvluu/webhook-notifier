SHELL := /bin/bash

DATABASE_URL ?= postgres://notifier:notifier@localhost:5432/notifier?sslmode=disable
MIGRATE      ?= go run -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@v4.17.1

.PHONY: help up down migrate migrate-down build test lint tidy demo bench-1 bench-2 bench-3 bench-4

help: ## list targets
	@awk 'BEGIN{FS=":.*##"} /^[a-zA-Z_-]+:.*##/ {printf "  %-12s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

up: ## start Postgres
	docker compose up -d postgres
	@docker compose exec -T postgres bash -c 'until pg_isready -U notifier -d notifier; do sleep 1; done'

down: ## stop and remove containers
	docker compose down -v

migrate: ## apply migrations
	$(MIGRATE) -path ./migrations -database "$(DATABASE_URL)" up

migrate-down: ## roll back one migration step
	$(MIGRATE) -path ./migrations -database "$(DATABASE_URL)" down 1

build: ## build all binaries
	go build ./...

test: ## run unit + integration tests
	go test ./... -race -count=1

lint: ## run golangci-lint (installs locally if missing)
	@command -v golangci-lint >/dev/null || go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.61.0
	golangci-lint run

tidy: ## go mod tidy
	go mod tidy

demo: up migrate ## end-to-end smoke (placeholder until #16)
	@echo "demo target stub — wired in issue #16"

bench-1: ## fairness under load + late-arrival reactivity (issue #12)
	@echo "bench-1 stub — implemented in issue #12"

bench-2: ## transient + recovering partners (issue #13)
	@echo "bench-2 stub — implemented in issue #13"

bench-3: ## terminal classification fast-fail (issue #14)
	@echo "bench-3 stub — implemented in issue #14"

bench-4: ## horizontal scale + atomicity + idempotency (issue #15)
	@echo "bench-4 stub — implemented in issue #15"
