# Persisted Webhook circuit breaker

## Amended by

[ADR-0014](./0014-worker-resilience-and-counter-based-breaker.md) makes the
"successes reset the failure counter" behaviour literal: `breaker_failure_count`
is now maintained incrementally on the mark path instead of being recomputed by
scanning `delivery_attempts`, which had become a `statement_timeout` hotspot
during partner outages.

## Context

RetryPolicy limits the number and spacing of Attempts for one Delivery, and the process-local HostLimiter partially enforces the Host Concurrency Cap for one notifier instance. Neither mechanism stops a chronically failing Webhook from repeatedly entering the worker pool across many Deliveries. That wastes worker capacity and keeps a broken Integration Partner in the hot path until every Delivery exhausts its retry budget.

## Decision

Store circuit breaker state on each Webhook. The states are:

- `closed`: normal delivery; successes reset the failure counter.
- `open`: claim and claimable-Customer discovery skip Deliveries for the Webhook.
- `half_open`: exactly one Delivery is claimed as a probe after cooldown.

Closed Webhooks open after a configurable consecutive failure threshold. The default is 20 consecutive failures, followed by a 5 minute initial cooldown and exponential cooldown backoff capped at 15 minutes. Retryable failures and terminal failures both count because both indicate the configured destination is not currently accepting this workload. Opening records `breaker_opened_at`, `breaker_next_probe_at`, the failure count, and the cooldown used.

After `breaker_next_probe_at`, the claim query atomically transitions the Webhook to `half_open` and records `breaker_probe_delivery_id` while claiming that Delivery. This keeps the probe single-row and safe across multiple worker processes. Only that probe Delivery may close the breaker. Probe success closes the breaker and resets counters. Probe failure reopens it and backs off the next cooldown up to a configured maximum.

The breaker exposes Prometheus metrics:

- `webhook_breaker_state{webhook_id,state}`
- `webhook_breaker_trips_total{reason}`

## Consequences

- The breaker complements RetryPolicy and HostLimiter; it does not replace either. RetryPolicy still classifies an Attempt, and HostLimiter still protects one hostname from local concurrency spikes.
- Open Webhooks can leave queued Deliveries pending. That is intentional: the queue preserves work while the breaker prevents repeated worker occupation.
- The half-open probe is implemented in the outbox claim path because the correctness requirement is cross-process. Keeping it in memory would allow one probe per process, not one probe per Webhook.
- A future operator or Customer notification path can subscribe to breaker transitions without changing claim semantics.
- A sliding failure-rate window is deferred. Consecutive failures are enough for the PoC's chronically failing endpoint case without adding time-window bookkeeping to the hot mark path.
