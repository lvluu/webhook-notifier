# Delivery claims as the authoritative claim store

## Context

The original outbox model used `deliveries.status = 'in_flight'` to represent a claimed row. That was simple, but it forced claim ownership, retry accounting, and recovery semantics into one mutable row. Under higher concurrency, batch mark paths also had to infer the claim attempt from the delivery row itself, which made deadlock avoidance and recovery logic harder to reason about.

## Decision

Introduce a dedicated `delivery_claims` table and treat it as the authoritative record for claim ownership while a worker is actively processing a Delivery.

Claiming invariant:

- A Delivery is claimed if and only if a `delivery_claims` row exists for that `delivery_id` and the claim lease is still valid.

The claim table stores:

- `delivery_id`
- `attempt_number`
- `claimed_at`

The `deliveries` row continues to hold the durable lifecycle state:

- `pending`
- `failed_retry`
- `succeeded`
- `dead`

`in_flight` is no longer a persisted lifecycle state. It may still appear as a runtime projection label in Go/SQL result shapes for claimed rows.

The worker flow is:

1. Insert or refresh a claim row.
2. Read the claim metadata for processing.
3. Delete the claim row when the Delivery reaches a terminal state or is explicitly requeued.
4. Update `deliveries.attempt_count` only when a real partner-facing Attempt is recorded.

## Consequences

- Claim ownership is explicit and recoverable.
- Retry accounting stays tied to real HTTP Attempts, not to claim churn.
- Batch mark paths can lock deliveries in deterministic order before clearing claims, which removes the deadlock pattern observed in widened worker sweeps.
- `Requeue` no longer needs to decrement `attempt_count`; it simply removes the claim row and returns the Delivery to `failed_retry`.
- The migration is more invasive than the original `in_flight` column, but the model is easier to extend and debug under concurrency.

## Tradeoffs

- The schema is a little larger and the code path has one more join.
- In exchange, the claim lifecycle is separated from terminal state transitions, which makes the outbox behavior easier to reason about at higher worker counts.
- The design stays compatible with fairness and retry semantics because the scheduler still picks Customers and the retry budget still advances only on partner-facing Attempts.

## Amends / supersedes

- Supersedes legacy wording that treated `deliveries.status = 'in_flight'` as the persisted claim source of truth.
- Amends [0003](./0003-retry-policy-and-delivery-lifecycle.md) and [0006](./0006-db-backed-claimable-customer-discovery.md): references to `in_flight` in claim semantics should now be read as claim ownership represented by `delivery_claims` (unless a section is explicitly historical).
