# Repository Guidelines

Webhook Notifier is a Go service that accepts business Events and POSTs
Webhook Deliveries to Integration Partners with reliability, horizontal
scalability, and per-Customer fairness.

## Project Structure & Module Organization

This is a Go webhook delivery PoC. Service entrypoints live under `cmd/`:
`cmd/notifier` for the service and `cmd/bench` for benchmark scenarios.
Core packages live under `internal/`, organized by domain: `ingest`,
`outbox`, `worker`, `scheduler`, `retry`, `signer`, `registry`, and
`observe`. SQL migrations live in `migrations/`. Benchmark fixtures and
scenarios live in `bench/`. Read `CONTEXT.md`, `docs/DESIGN.md`, and
`docs/adr/` before changing architecture or vocabulary.

## Build, Test, and Development Commands

Use `just` as the project task runner:

- `just up` starts local Postgres with Docker Compose.
- `just migrate` applies `migrations/*.sql` using the configured
  `DATABASE_URL`.
- `just build` runs `go build ./...`.
- `just test` runs `go test ./... -race -count=1`.
- `just lint` runs pinned `golangci-lint`.
- `just down` stops Postgres and removes volumes.

Run `just` with no arguments to list all recipes.

## Coding Style & Naming Conventions

Use idiomatic Go: small packages, clear names, explicit error handling, and
simple control flow. Format all Go files with `gofmt`; imports should satisfy
`goimports`/`golangci-lint`. Prefer constructor-based dependency injection,
small interfaces defined at the consumer boundary, and functional options when
configuration is expected to grow. Wrap errors with actionable context:
`fmt.Errorf("claim delivery: %w", err)`.

## Testing Guidelines

Use the standard `testing` package. Name tests `TestXxx` and prefer
table-driven subtests for validation and edge cases. Integration tests that
depend on Postgres should use testcontainers and live beside the package they
exercise, as in `internal/outbox/outbox_integration_test.go`. Always run
`just test` before pushing because race detection is part of the baseline.

## Commit & Pull Request Guidelines

Follow the existing Conventional Commit style: `feat(scope): ...`,
`fix(scope): ...`, `docs(scope): ...`, `chore: ...`, or `refactor(scope): ...`.
Keep commits focused and issue-scoped. PRs should include a short summary,
linked issue such as `Closes #4`, and verification commands run locally.
Mention schema or migration changes explicitly.

## Agent Workflow

Track work in GitHub Issues for `lvluu/flodesk-home-assignment` via `gh`.
Ready work uses the `ready-for-agent` label. Triage labels are
`needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, and
`wontfix`; see `docs/agents/triage-labels.md`. Domain context is single-root:
use `CONTEXT.md`, `docs/DESIGN.md`, and `docs/adr/` before changing behavior.
Restore pinned project skills with `npx -y skills@latest experimental_install`
when needed.

## Security & Configuration Tips

Do not commit secrets. Local configuration belongs in `.env` or environment
variables; `.env.example` documents supported values. Use parameterized SQL
queries only. Any networked or database behavior should accept
`context.Context` and respect cancellation.
