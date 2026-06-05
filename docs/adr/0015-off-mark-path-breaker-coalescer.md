# Off-mark-path circuit-breaker bookkeeping via a per-process coalescer

## Status

Accepted. Amends [0008](./0008-persisted-webhook-circuit-breaker.md) and
[0014](./0014-worker-resilience-and-counter-based-breaker.md).

## Context

[ADR-0014](./0014-worker-resilience-and-counter-based-breaker.md) stopped the
notifier fleet from self-terminating under a partner outage (transient DB errors
are now absorbed) and removed the breaker history scan (per-failure counter).
With the fleet surviving, live testing under chaos load exposed the next
bottleneck: a storm of Postgres **deadlocks (SQLSTATE 40P01)**.

Root cause: every batch mark (`MarkRetryBatch` / `MarkDeadBatch` /
`MarkSucceededBatch`) updated the failing Webhook's breaker row **inside the same
transaction** that updated `deliveries`, deleted `delivery_claims`, and inserted
`delivery_attempts`. Under a whale outage a handful of hot Webhook rows are
contended by **every worker on every replica** (e.g. 3 replicas × 16 workers =
48 concurrent mark transactions), all updating the same breaker rows. The dense
lock graph on those few rows produced multi-transaction deadlock cycles.

Lock ordering was tried first and **failed empirically**:

- Acquiring the Webhook rows in id order with `SELECT … ORDER BY id FOR UPDATE`
  before the breaker writes *increased* deadlocks (914 in 4 min): `FOR UPDATE`
  conflicts with the `FOR KEY SHARE` lock every `INSERT INTO deliveries` takes on
  its parent Webhook via the foreign key, so it serialized the breaker path
  against the high-rate ingest path.
- Switching to `FOR NO KEY UPDATE` removed the ingest collision but deadlocks
  persisted (651 in ~4 min): `ORDER BY … FOR UPDATE` does not guarantee
  lock-*acquisition* order, and more fundamentally dozens of transactions still
  converge on the same few hot rows. Lock ordering cannot break cycles that
  route through transaction-dependency chains on a single hot tuple.

The contention is inherent to **(workers × replicas)** transactions writing the
same few breaker rows synchronously on the hot path.

## Decision

**Move breaker bookkeeping off the synchronous mark transaction and onto a
single per-process coalescer goroutine.**

- The Store gains `WithDeferredBreaker()`. When set, the batch mark methods skip
  their inline breaker writes; the mark transaction touches only `deliveries`,
  `delivery_claims`, `delivery_attempts`, and `dead_letters`. It never locks a
  Webhook breaker row.
- After a batch mark commits, the worker enqueues `BreakerOutcome`s
  (`{WebhookID, DeliveryID, Success, Reason}`, built from the already-claimed
  `Delivery` rows — no extra lookup) to a `BreakerOutcomeSink`.
- One `BreakerCoalescer` per process consumes outcomes from all workers, batches
  them over a short window (or up to a max batch), and applies them in a single
  transaction via `ApplyBreakerOutcomes`: successes reset, failures increment the
  persisted counter and open breakers that reach the threshold (the ADR-0014
  set-based logic, unchanged). Breaker open/close events are emitted after commit.

This collapses breaker write concurrency from **(workers × replicas)** to
**(replicas)** — e.g. 48 → 3 — and coalesces many mark cycles into far fewer,
larger transactions. With only one writer per process, an id-ordered
`FOR NO KEY UPDATE` lock in `ApplyBreakerOutcomes` cheaply prevents the few
remaining cross-process flush transactions from deadlocking, and `FOR NO KEY
UPDATE` stays compatible with ingest's FK `FOR KEY SHARE`.

The single-delivery mark path (`MarkSucceeded` / `MarkRetry` / `MarkDead`) is
unchanged and still applies breaker state inline; it is low volume and not on the
contended path.

## Consequences

- Breaker state is **eventually consistent**, lagging the mark by at most the
  coalesce window (≈100 ms) plus flush time. The breaker is a heuristic; a small
  delay opening or closing is acceptable and does not affect delivery
  correctness, which is governed by `delivery_claims` + `SKIP LOCKED`.
- Breaker outcomes are **best-effort**. If a process crashes with buffered
  outcomes, or the buffer saturates and drops samples, the breaker self-corrects
  on subsequent attempts. A dropped sample only slightly delays a transition.
- The hot-row breaker contention — and the deadlock storm it caused — is removed
  from the mark path. Any residual deadlocks are confined to the few per-process
  flush transactions and bounded by the id-ordered lock.
- The half-open probe contract is preserved: the claim query still transitions
  `open → half_open` and sets `breaker_probe_delivery_id` atomically; only the
  probe Delivery's outcome closes or reopens the breaker, just applied by the
  coalescer slightly later.
- Transient DB errors during a coalescer flush are logged and the batch dropped
  (self-correcting), consistent with the breaker's best-effort nature.

### Measured result

Live under the chaos profile (3 replicas, whale partner outage, ~6 min window),
Postgres `deadlock detected` counts:

| Configuration | Deadlocks | Window |
|---|---|---|
| Inline breaker on the mark tx (before any fix) | ~340 | 6 min |
| Mark-path lock ordering, `FOR UPDATE` | 914 | 4 min |
| Mark-path lock ordering, `FOR NO KEY UPDATE` | 651 | 4 min |
| **Off-mark coalescer (this ADR)** | **34** | 6.5 min |

Statement timeouts (57014) stayed at 0 (ADR-0014 counter), and all replicas
stayed healthy throughout.

### Residual deadlocks are on the mark path, not the breaker

The remaining ~34 deadlocks no longer involve `webhooks` at all. They are
concurrent batch-mark transactions colliding on `deliveries` / `delivery_claims`
rows (the `FOR UPDATE OF d` lock plus the `DELETE FROM delivery_claims` and the
`delivery_attempts` foreign-key locks). They are **safe and self-healing**:

- A mark transaction is atomic, so a 40P01 abort leaves no half-state — the
  Delivery is not updated, its claim is not deleted, no Attempt row is written.
- The worker run loop classifies 40P01 as transient (ADR-0014), logs, backs off,
  and continues rather than failing fatally. (This is why observed
  `webhook_worker_transient_db_errors_total` tracks the deadlock count.)
- Because the mark rolled back, the Delivery stays claimed; its claim lease
  expires and it is re-claimed via `FOR UPDATE SKIP LOCKED` and re-marked. The
  mark is retried, never lost, and `delivery_claims` + `SKIP LOCKED` keep
  processing correct.
- Caveat (pre-existing at-least-once): the HTTP attempt was sent before marking,
  so a deadlock-forced re-process re-sends the POST — the partner may receive a
  duplicate. This is the same at-least-once property the lease model always had;
  the deadlock only triggers one extra delivery cycle.

Driving these mark-path collisions down further (ordering the
`delivery_claims`/`delivery_attempts` locks) is possible but out of scope: at
34 / 6.5 min they are rare and safely retried.

## Considered options

- **Lock ordering on the mark path** (`FOR UPDATE`, then `FOR NO KEY UPDATE`).
  Rejected: proven ineffective live (see Context). Cannot fix contention where
  many transactions converge on a few hot rows.
- **Reduce concurrency** (fewer workers / replicas, smaller multi-pick). A config
  lever that lowers contention but trades throughput and does not address the
  design; usable as an operational mitigation, not the fix.
- **Per-process coalescer** *(chosen)*. Removes the breaker write from the hot
  mark transaction entirely and cuts writer concurrency by the worker-per-process
  factor, at the cost of eventually-consistent, best-effort breaker state — an
  acceptable trade for a heuristic.
- **Keep breaker writes inline but conditional** (only write near threshold).
  Rejected: the persisted counter must aggregate every failure across processes,
  so per-failure writes are unavoidable while inline.
