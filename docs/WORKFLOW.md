# Webhook Notifier Workflow

This document is the execution path from `POST /events` to terminal Delivery
state, plus the main tuning knobs used in benchmarks and production hardening.

## End-to-End Flow

1. `POST /events` accepted.
2. `EventIngest` opens one DB transaction.
3. Insert row in `events`.
4. Resolve active matching Webhooks for `(customer_id, event_type)`.
5. Insert one `deliveries` row per Webhook (`status='pending'`).
6. Commit transaction.
7. Notify Scheduler for that Customer.
8. Worker calls `Scheduler.Pick()` and gets next Customer.
9. Worker claims rows via `ClaimNextN` (`FOR UPDATE SKIP LOCKED`) for that Customer.
10. For each claimed Delivery:
11. Apply host limiter (optional) and request timeout.
12. Sign request and send HTTP POST.
13. Classify result via retry policy:
14. Success: mark `succeeded`, insert attempt row.
15. Retryable: mark `failed_retry`, set `next_attempt_at`, insert attempt row.
16. Terminal: mark `dead`, insert attempt row + dead letter row.
17. Scheduler continues; retry-due rows become claimable again when `next_attempt_at <= now()`.

## What To Measure

- Reliability:
  - Duplicate `(delivery_id, attempt_number)` tuples
  - Dead-letter rate
  - Retry success ratio
- Scalability:
  - Deliveries/sec as workers increase
  - Claim/mark query latency and DB pool saturation
- Fairness:
  - Small vs Whale **P95 first-attempt latency**
  - First-attempt latency = `delivery_attempts.sent_at(attempt=1) - deliveries.created_at`

## Tuning Knobs

| Layer | Knob | Effect | Typical Direction |
|---|---|---|---|
| Worker | `worker_count` | Parallelism and DB contention | Increase until throughput knee |
| Worker | `MinBatchHint` | Claim batch size per scheduler pick | Higher for throughput, lower for fairness |
| Worker | `request_timeout` | HTTP timeout behavior and retries | Raise if false timeouts; lower to fail fast |
| Worker | Host limiter (`MaxPerHost`) | Protects partner hosts | Lower to reduce partner pressure |
| Retry | `MaxAttempts` | Reliability under transient failures | Increase for resilience, watch tail latency |
| Retry | Backoff (`BaseDelay`, `CapDelay`) | Retry storm vs recovery speed | Increase to smooth load |
| DB pool | `max_open_conns` / `max_idle_conns` | DB concurrency and wait time | Tune jointly with worker_count |
| DB | Indexes/partitioning | Claim path cost at scale | Add/tune when queue tables grow |
| DB | Autovacuum/WAL/checkpoints | Write-heavy stability | Tune for sustained queue writes |
| Discovery | Claimable-customer discovery interval/backoff | How quickly retries/recovered work are noticed | Lower for responsiveness |

## Practical Tuning Order

1. Reliability first: duplicates = 0 and acceptable dead-letter rate.
2. Throughput second: sweep `worker_count × db_conns × MinBatchHint`.
3. Fairness third: verify Small-vs-Whale P95 first-attempt latency.
4. Infrastructure fourth: DB sizing, pooling, then partitioning/sharding.

## Related Docs

- `docs/DESIGN.md`
- `bench/README.md`
- `bench/results/scenario-4.md`
- `bench/results/scenario-6.md`
