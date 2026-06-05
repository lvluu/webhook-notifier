# Worker resilience to transient DB pressure and counter-based breaker failure tracking

## Status

Accepted. Amends [0008](./0008-persisted-webhook-circuit-breaker.md).

## Context

Running the Docker Compose stack with three notifier replicas under chaos load
(a whale Customer whose Integration Partner returns errors, plus broad Small
traffic) reproduced a fleet-wide outage: after some minutes **all three
notifier replicas exited cleanly (exit 0)** and did not come back.

The container logs showed the trigger and the amplifier:

```
ERROR delivery runtime error
  error="worker 7: batch mark retry: iterate failure counts:
         ERROR: canceling statement due to statement timeout (SQLSTATE 57014)"
WARN  sample pending deliveries failed error="query pending deliveries: context canceled"
INFO  shutdown complete
```

Two independent defects stacked:

1. **Breaker failure counting scanned the full attempt history.** On every mark
   batch, `batchRecordBreakerFailure` recomputed "consecutive failures since the
   last 2xx" for each closed Webhook by scanning `delivery_attempts`. During a
   sustained partner outage the last-success lookup matched no rows, so the
   window spanned the entire per-Webhook history. Under the compose
   `statement_timeout = 30s`, that scan eventually exceeded the timeout and
   returned SQLSTATE 57014. This is the same hotspot flagged for follow-up after
   the [ADR-0013](./0013-claim-agnostic-claimable-customer-discovery.md) work
   (it had become the rank-1 query, ~1.8s mean, ~25% of DB time during outages).

2. **The worker treated any non-`context.Canceled` error as fatal.** A statement
   timeout is not `context.Canceled`, so the worker run loop returned it. That
   return propagated through the runtime group to `main`'s `select`, which
   called `cancel()` and tore down **every** worker, discovery, the wake
   listener, and the ingest HTTP server on that replica. Because all replicas
   share one database, they tripped the same timeout within seconds of each
   other, so the entire fleet exited. No `restart:` policy on the notifier
   service meant they stayed down.

In short: defect 1 fired the bullet, defect 2 made it lethal to the whole fleet.

## Decision

### Counter-based breaker failure tracking

Maintain the persisted `breaker_failure_count` incrementally instead of
recomputing it from `delivery_attempts`:

- A failure on a `closed` Webhook adds the number of failing attempts in the
  current mark batch to `breaker_failure_count`, in a single set-based `UPDATE`
  that also flips the Webhook to `open` when the running count reaches the
  threshold (`batchIncrementClosedBreakers`, and `incrementClosedBreaker` for
  the single-delivery path).
- A success resets `breaker_failure_count` to 0 (unchanged).

Because every success resets the counter and every failure increments it, the
stored value equals the consecutive-failure count that the history scan used to
compute — **without reading `delivery_attempts` on the hot mark path.** The
`UPDATE` is row-locked, so concurrent mark batches for the same Webhook
accumulate correctly. The half-open path is unchanged.

This realises the behaviour ADR-0008 already described ("successes reset the
failure counter") and removes the time-window/history bookkeeping ADR-0008
explicitly wanted to keep off the mark path.

### Worker resilience to transient database errors

The worker run loop classifies database errors (`isTransientDBError`). Transient
conditions — statement/transaction timeout (57014), serialization failure
(40001), deadlock (40P01), lock-not-available (55P03), connection-exception
class 08, too-many-connections, `context.DeadlineExceeded`, bad/closed
connections — are **absorbed**: the worker logs, records
`webhook_worker_transient_db_errors_total{phase}`, backs off briefly
(`transientErrorBackoff`), and continues its loop. Only genuinely
non-recoverable errors are returned, and a returned error still triggers replica
shutdown as before.

A `restart: unless-stopped` policy is added to the notifier service in
`compose/docker-compose.yml` as defense-in-depth.

## Consequences

- A slow or shedding database no longer takes down workers or the whole fleet;
  replicas pause and recover. The claim/mark hot paths survive `statement_timeout`.
- Breaker failure marking cost is independent of attempt-history depth. A
  regression test seeds a 200k-row failing history for one Webhook and asserts a
  failure mark stays well under a latency bound (and bumps the counter by one),
  guarding against a return to the O(history) recompute.
- `webhook_worker_transient_db_errors_total` is the signal to watch. A sustained
  rise means the database is shedding load (timeouts, deadlocks, connection
  churn); the remedy is database capacity / pool tuning, not a code change.
- At-least-once semantics are unchanged. If a mark fails transiently after the
  HTTP attempt was sent, the claim is not released; the lease expires and the
  delivery is reclaimed (`SKIP LOCKED` + lease, per ADR-0011). Absorbing the
  error rather than crashing does not add duplicates beyond what the lease model
  already allows.
- The counter trusts `breaker_failure_count` rather than self-healing from
  history each mark. Every writer keeps it consistent (success → 0, failure →
  +n, open sets it explicitly), and `SKIP LOCKED` plus row locks prevent
  double-counting. The column default of 0 and the success reset keep existing
  rows correct.

## Considered options

- **Index the 2xx lookup / bound the history window** (keep recompute). Narrows
  the scan but keeps `delivery_attempts` on the hot mark path and still scales
  with history for never-succeeded Webhooks. `delivery_attempts` has no
  `webhook_id`, so the per-Webhook 2xx lookup cannot be indexed without
  denormalisation. Rejected in favour of the counter, which removes the read
  entirely.
- **Only add a `restart:` policy** (no code change). Masks the crash loop but
  leaves every replica thrashing under load and drops in-flight work on each
  restart. Kept as defense-in-depth, not the fix.
- **Counter + worker resilience** *(chosen)*. Stops the timeout firing and makes
  any future transient error non-lethal. Independent mechanisms, one incident.
