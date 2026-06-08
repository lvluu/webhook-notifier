# Postgres as the queue, no message broker for the PoC

Status: Accepted. Claim storage is amended by ADR-0011 and discovery semantics
by ADR-0013.

## Context

The Webhook Notifier needs a durable place to hold Deliveries between Event ingest and worker pickup, plus per-Customer scheduling state. The obvious production-shaped choice is a message broker (Redis Streams, Kafka) keyed by Customer. The obvious PoC-shaped choice is Postgres as both the durable log and the work queue.

## Decision

Use **Postgres alone** as outbox, work queue, and scheduling source-of-truth for the PoC. No Redis, no broker. Workers claim Deliveries with `SELECT ... FOR UPDATE SKIP LOCKED` scoped to the Customer the Scheduler picked.

The `Scheduler` interface (ADR-0001) isolates the fairness policy so round-robin vs SWRR is a Scheduler implementation swap. However, the Scheduler alone does not abstract the queue source — the `outbox.Store` claim queries, `Customer` discovery, and ingest fanout are all Postgres-specific. A move to Redis Streams or Kafka would require changes in outbox, discovery, and ingest simultaneously. The Scheduler stays; everything else under it changes.

## Considered options

- **Postgres only** *(chosen)*. One infra dep. Atomic claim. Trivial local spin-up. Sufficient for PoC throughput.
- **Postgres outbox + in-memory per-Customer queues**. Cheaper hot-path scheduling but adds a Dispatcher process and crash-recovery logic. The bench at PoC scale doesn't justify the complexity.
- **Postgres outbox + Redis Streams**. Production-shaped, real horizontal scaling. Two infra deps. Documented as the scale path, not implemented.

## Consequences

- Worker pickup is two-stage: `Scheduler.Pick()` selects one or more Customer
  IDs in memory, then the Outbox executes one atomic CTE that locks eligible
  `deliveries` rows with `FOR UPDATE SKIP LOCKED` and inserts or refreshes
  `delivery_claims` rows. `ClaimNextBatch` can amortize one query across several
  Customers while applying a per-Customer candidate cap.
- The `deliveries` table needs an index on `(customer_id, status, next_attempt_at)` to keep that query cheap.
- Scheduler membership is process-local and populated by local/remote wake hints
  plus periodic DB-backed discovery. Discovery is a claim-agnostic superset
  hint; the claim query remains authoritative (ADR-0006, ADR-0013).
- At higher scale, row-lock contention on a single hot Customer would push us toward partitioning or a broker. That trigger is recorded here so it isn't rediscovered the hard way.
- The bench harness should report `SELECT FOR UPDATE` wait time alongside delivery latency to surface this contention if it appears.
