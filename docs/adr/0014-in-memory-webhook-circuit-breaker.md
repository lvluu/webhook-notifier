# In-memory Webhook circuit breaker decision engine

## Context

ADR-0008 put Webhook Circuit Breaker state directly in the outbox mark path.
That preserved cross-process visibility, but every success/failure mark could
query or update `webhooks`, and failure marks could scan Attempt history to
recompute consecutive failures. Under high delivery throughput, the Webhook row
became a hot row and breaker bookkeeping competed with the delivery outcome
transaction.

## Decision

Move breaker decision-making to an in-memory component. Workers record
successful and failed Attempts after the mark transaction commits. The
in-memory breaker owns consecutive-failure counting, cooldown backoff, and
closed/open/half-open decisions.

Postgres remains the persistence and claim-visible coordination layer, not the
hot-path decision engine:

- load persisted Webhook breaker state at notifier startup;
- persist only state transitions asynchronously;
- keep claim/discovery filtering against `webhooks.breaker_state`;
- keep half-open probe assignment in the claim query so one probe Delivery is
  assigned per Webhook across workers;
- defer blocked Deliveries when a breaker opens so an open Webhook backlog
  leaves the ready scan until the probe time.

The mark transactions (`MarkSucceeded*`, `MarkRetry*`, `MarkDead*`) must not
write `webhooks`.

## Consequences

- The high-volume mark path no longer updates Webhook rows or scans Attempt
  history for breaker decisions.
- Breaker persistence work is proportional to lifecycle transitions, not
  delivery outcome volume.
- A process may have slightly stale in-memory breaker state until it observes a
  claimed probe or reloads on restart, but persisted state still keeps claim
  filtering cross-process.
- If asynchronous persistence lags, the claim filter may trail the in-memory
  decision briefly. The persister is bounded and applies overflow transitions
  synchronously to avoid unbounded work during partner outages.
