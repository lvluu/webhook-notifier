# Requeue without Attempt

When a Delivery cannot proceed for a non-partner reason (e.g. host concurrency cap reached), it is Requeued: transitioned from `in_flight` to `failed_retry` with a bumped `next_attempt_at`, but without recording a `delivery_attempt` row or counting it against the retry budget. The claim row is deleted and the Delivery keeps its current `attempt_count`, because only a real HTTP POST should advance the retry budget.

The current `delivery_claims` model separates claim ownership from attempt history. Claim counters live in `delivery_claims.attempt_number` while `deliveries.attempt_count` is updated only when a partner-facing Attempt is recorded. That keeps requeues from consuming retry budget before the partner ever sees a request, while still preserving the history needed to recover the claim after a crash.
