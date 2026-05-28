# Postgres as the queue, no message broker for the PoC

## Context

The Webhook Notifier needs a durable place to hold Deliveries between Event ingest and worker pickup, plus per-Customer scheduling state. The obvious production-shaped choice is a message broker (Redis Streams, Kafka) keyed by Customer. The obvious PoC-shaped choice is Postgres as both the durable log and the work queue.

## Decision

Use **Postgres alone** as outbox, work queue, and scheduling source-of-truth for the PoC. No Redis, no broker. Workers claim Deliveries with `SELECT ... FOR UPDATE SKIP LOCKED` scoped to the Customer the Scheduler picked.

The `Scheduler` interface (ADR-0001) abstracts the queue source so a future move to Redis Streams or Kafka per-Customer partitions is a Scheduler implementation swap, not a system rewrite.

## Considered options

- **Postgres only** *(chosen)*. One infra dep. Atomic claim. Trivial local spin-up. Sufficient for PoC throughput.
- **Postgres outbox + in-memory per-Customer queues**. Cheaper hot-path scheduling but adds a Dispatcher process and crash-recovery logic. The bench at PoC scale doesn't justify the complexity.
- **Postgres outbox + Redis Streams**. Production-shaped, real horizontal scaling. Two infra deps. Documented as the scale path, not implemented.

## Consequences

- Worker pickup is a two-step query inside one transaction: `Scheduler.Pick()` returns the next `customer_id` to serve; the worker then issues `SELECT … FROM deliveries WHERE customer_id = $1 AND status = 'pending' AND next_attempt_at <= now() FOR UPDATE SKIP LOCKED LIMIT 1`. This preserves the fairness ordering while keeping claim atomicity.
- The `deliveries` table needs an index on `(customer_id, status, next_attempt_at)` to keep that query cheap.
- Scheduler state (which Customers have pending work) is derived from a cheap aggregate query — not maintained as separate state — so it never drifts from the Outbox.
- At higher scale, row-lock contention on a single hot Customer would push us toward partitioning or a broker. That trigger is recorded here so it isn't rediscovered the hard way.
- The bench harness should report `SELECT FOR UPDATE` wait time alongside delivery latency to surface this contention if it appears.
