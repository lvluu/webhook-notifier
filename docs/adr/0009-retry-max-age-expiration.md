# ADR 0009: Retry MaxAge Expiration

## Status

Accepted

## Context

A Delivery's lifecycle is currently governed by `MaxAttempts` (ADR-0003). While this limits the number of HTTP requests made to an Integration Partner, it does not limit the absolute time a Delivery can remain in the system.

During large backlogs or long-lived outages, a Delivery might remain "pending" or "failed_retry" for days, eventually becoming stale and irrelevant to the Integration Partner.

We need a way to expire Deliveries based on their age since creation.

## Decision

We will introduce a `MaxAge` configuration to the `RetryPolicy`.

1.  **RetryConfig**: Add `MaxAge time.Duration`. A zero value means no age limit.
2.  **RetryPolicy.Next**: Add a `lifetime time.Duration` parameter.
3.  **Terminal Reason**: Introduce `ReasonAgeExceeded = "age_exceeded"`.
4.  **Enforcement**:
    -   **Pre-flight**: Before a worker initiates an HTTP `Send`, it checks if `time.Since(CreatedAt) >= MaxAge`. If so, it moves the Delivery to the DLQ immediately without an Attempt.
    -   **Post-attempt**: After a failed Attempt, the `RetryPolicy` checks if the current lifetime exceeds `MaxAge`. If so, it returns a terminal outcome with `ReasonAgeExceeded`.

## Consequences

-   **Backward Compatibility**: Existing configurations with `MaxAge: 0` (the default) remain unchanged.
-   **Resource Efficiency**: Workers avoid making network requests for already-expired Deliveries.
-   **Staleness Control**: Integration Partners are guaranteed not to receive webhook notifications older than `MaxAge` (modulo the duration of the final Attempt).
-   **Observability**: `ReasonAgeExceeded` provides a clear signal in the DLQ for monitoring backlog-induced expirations.
