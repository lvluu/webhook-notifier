# Requeue without Attempt

Status: Accepted. Claim ownership is represented by `delivery_claims` per
ADR-0011.

When a claimed Delivery cannot proceed for a non-partner reason (for example,
the host concurrency cap is reached), it is Requeued: its Claim row is deleted,
its persisted status becomes `failed_retry`, and `next_attempt_at` is bumped,
without recording a `delivery_attempt` row or counting against retry budget.
The Delivery keeps its current `attempt_count`, because only a real HTTP POST
advances the retry budget.

The current `delivery_claims` model separates claim ownership from attempt history. Claim counters live in `delivery_claims.attempt_number` while `deliveries.attempt_count` is updated only when a partner-facing Attempt is recorded. That keeps requeues from consuming retry budget before the partner ever sees a request, while still preserving the history needed to recover the claim after a crash.
