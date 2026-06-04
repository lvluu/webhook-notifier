# Webhook Notifier Workflow

This document is the execution path from Event ingest to terminal Delivery
state, plus the tuning knobs used by the service and benchmark harness.

## End-to-End Flow

1. A producer submits an Event to `POST /api/events` or the compatibility
   route `POST /events`.
2. `EventIngest` opens one Postgres transaction.
3. The Event payload is inserted into `events`.
4. Active matching Webhooks are resolved for `(customer_id, event_name)`.
5. One `deliveries` row is inserted per matching Webhook with `status =
   'pending'`.
6. The transaction commits, then the local Scheduler is notified for that
   Customer.
7. When enabled, the notifier also sends a coalesced Postgres `NOTIFY` wake
   hint so other replicas can notify their local Schedulers.
8. DB-backed claimable-Customer discovery periodically scans Postgres for ready
   work, delayed retries that are due, and stale claims, then notifies the local
   Scheduler.
9. A Worker calls `Scheduler.Pick()` for the next Customer. With
   `MultiPickCount > 1`, it collects several Customers before one claim query.
10. Single-Customer mode calls `ClaimNextN`. Multi-Customer mode calls
    `ClaimNextBatch` with `totalLimit = len(customerIDs) * MinBatchHint`.
11. The claim SQL selects ready rows and stale-claim rows, applies breaker
    eligibility and half-open probe handling, locks candidate `deliveries` rows
    with `FOR UPDATE SKIP LOCKED`, and writes `delivery_claims`.
12. `delivery_claims` is the authoritative Claim store. `deliveries.status`
    remains `pending` or `failed_retry`; the synthetic worker status is
    `in_flight`.
13. For each claimed Delivery, the Worker first checks the process-local
    HostLimiter with `TryAcquire(hostname)`.
14. If the host is at the local cap, the Delivery is requeued with a short
    delay. No HTTP request is sent, no Attempt row is written, and retry budget
    is not consumed.
15. If a host slot is available, the Worker signs the payload, sends an HTTP
    POST with request timeout, drains and closes the response body, and releases
    the host slot.
16. `RetryPolicy.NextWithKey` classifies the response/error, including
    pre-flight MaxAge expiration, 2xx success, terminal 4xx, retryable
    408/429/5xx/network errors, attempt exhaustion, and `Retry-After`.
17. The Worker groups outcomes and writes set-based batch marks:
    `MarkSucceededBatch`, `MarkRetryBatch`, or `MarkDeadBatch`.
18. Batch marks delete the `delivery_claims` row, insert Attempt records when
    an HTTP Attempt happened, update `deliveries`, write `dead_letters` for
    terminal failures, and update breaker state.
19. Retryable Deliveries become claimable again when `next_attempt_at <= now()`.
20. Terminal Deliveries remain in the live queue for the retention window, then
    the retention command archives them to `delivery_history`.

## Demo And Bench Commands

Use `just` as the project task runner:

```bash
just demo                     # local end-to-end demo
just bench-6                  # mixed Whale/Small fairness proof
just bench-8                  # multi-replica scalability proof
just bench-partner-resilience # retry, terminal failure, breaker proof
just bench-7                  # internal breaker hardening
```

The benchmark command can also be run directly:

```bash
BENCH_PROFILE=stress go run ./cmd/bench scenario6
```

See [bench/README.md](../bench/README.md) for the full scenario map and
[bench/RESULTS.md](../bench/RESULTS.md) for the current report index.

## What To Measure

- Reliability:
  - Duplicate `(delivery_id, attempt_number)` tuples.
  - Dead-letter rate and terminal reasons.
  - Retry recovery rate and attempt distribution.
- Scalability:
  - Deliveries/sec as workers or replicas increase.
  - Claim/mark query latency and DB pool saturation.
  - Pending backlog, stale-claim recovery, and Postgres wait signals.
- Fairness:
  - Small Customer P95 first-attempt queue latency under Whale load.
  - First-attempt queue latency =
    `delivery_attempts.sent_at(attempt_number = 1) - deliveries.created_at`.
- Partner protection:
  - Host limiter requeues.
  - Breaker state transitions and probe outcomes.

## Tuning Knobs

| Layer | Knob | Effect | Typical Direction |
|---|---|---|---|
| Worker | `--workers` | Parallelism and DB contention | Increase until throughput knee |
| Worker | `--min-batch` / `MinBatchHint` | Claim batch size per scheduler pick | Higher for throughput, lower for fairness |
| Worker | `--multi-pick` / `MultiPickCount` | Customers batched into one claim query | Increase for sparse-Customer workloads |
| Worker | `--empty-claim-backoff` | Pause after empty claim cycle | Raise to reduce DB churn, lower for reactivity |
| Worker | request timeout | HTTP timeout behavior and retries | Raise if false timeouts; lower to fail fast |
| Worker | `--host-cap` | Process-local Integration Partner hostname cap | Lower to reduce partner pressure |
| Retry | `MaxAttempts` | Reliability under transient failures | Increase for resilience, watch tail latency |
| Retry | `BaseDelay`, `CapDelay` | Retry storm vs recovery speed | Increase to smooth load |
| Breaker | threshold/cooldown | Isolation of broken Webhooks | Lower to protect workers earlier |
| Discovery | interval/backoff/limit | How quickly retries and stale claims are rediscovered | Lower for responsiveness, higher to reduce DB load |
| DB pool | `--max-conns` | DB concurrency and wait time | Tune with worker count and PgBouncer |
| DB | claim query mode | Claim SQL shape | Prefer `split`; keep `legacy` for rollback/bench comparison |
| DB | indexes/partitioning | Claim path cost at scale | Add/tune when queue tables grow |
| DB | autovacuum/WAL/checkpoints | Write-heavy stability | Tune for sustained queue writes |

## Practical Tuning Order

1. Reliability first: duplicate Attempt tuples must stay at zero.
2. Throughput second: sweep `workers x max_conns x min_batch`.
3. Fairness third: verify Small Customer P95 first-attempt queue latency.
4. Sparse workload fourth: tune `MultiPickCount` before raising per-Customer
   batch size.
5. Partner protection fifth: validate host limiter requeues and breaker
   isolation under failing Integration Partners.
6. Infrastructure last: DB sizing, pooling, retention, then partitioning or a
   broker-backed queue if Postgres claim contention becomes the ceiling.

## Related Docs

- [docs/DESIGN.md](DESIGN.md)
- [docs/CONFIGURATION.md](CONFIGURATION.md)
- [bench/README.md](../bench/README.md)
- [bench/RESULTS.md](../bench/RESULTS.md)
