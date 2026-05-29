# Requeue without Attempt

When a Delivery cannot proceed for a non-partner reason (e.g. host concurrency cap reached), it is Requeued: transitioned from `in_flight` to `failed_retry` with a bumped `next_attempt_at`, but without recording a `delivery_attempt` row or counting it against the retry budget. The `attempt_count` is decremented to cancel the claim-time increment, preserving the ADR-0003 contract that `attempt_count` reflects real HTTP POSTs against the partner endpoint.

Without this, a Delivery Requeued N times would have `attempt_count = N` before any POST was made, potentially exhausting the retry budget and landing in the DLQ without the partner ever seeing a request.
