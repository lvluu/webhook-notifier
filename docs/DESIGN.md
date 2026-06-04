# Webhook Notifier — Design

This document describes the design of the Webhook Notifier component: a service that consumes business Events from a multi-tenant platform and reliably delivers them to Integration Partners over HTTP POST, with horizontal scalability and fair scheduling across Customers of vastly different sizes.

Vocabulary used here is defined in [CONTEXT.md](../CONTEXT.md). Architectural commitments are anchored in [ADR-0001](adr/0001-round-robin-fairness-with-pluggable-scheduler.md), [ADR-0002](adr/0002-postgres-as-queue-no-broker-for-poc.md), [ADR-0003](adr/0003-retry-policy-and-delivery-lifecycle.md), [ADR-0004](adr/0004-requeue-without-attempt.md), [ADR-0005](adr/0005-subscription-registry-lru-ttl-cache.md), [ADR-0006](adr/0006-db-backed-claimable-customer-discovery.md), [ADR-0007](adr/0007-http-transport-and-response-body-drain.md), [ADR-0008](adr/0008-persisted-webhook-circuit-breaker.md), [ADR-0009](adr/0009-retry-max-age-expiration.md), [ADR-0010](adr/0010-terminal-delivery-retention-archive.md), [ADR-0011](adr/0011-delivery-claims-authoritative-claim-store.md), and [ADR-0012](adr/0012-pgbouncer-prepared-statements-and-pgxpool-deferral.md).
For a step-by-step execution path and tuning checklist, see [WORKFLOW.md](WORKFLOW.md).

The implementation is Go plus Postgres. Go keeps the service small,
concurrent, and easy to run as many stateless worker replicas. Postgres is used
as both durable outbox and coordination point for the PoC: it stores Events,
Delivery work, Attempts, terminal state, and atomic claims through transactional
SQL. This avoids adding a broker while still demonstrating reliability,
horizontal scaling, and Customer-fair scheduling.

## Goals

- **Reliability**: no Event lost between business commit and final terminal state. Transient failures are retried with bounded backoff; persistent failures land in the DLQ for inspection.
- **Scalability**: workers are stateless and horizontally scalable. Adding workers increases throughput without changing claim semantics.
- **Fairness**: a Whale Customer cannot starve Small Customers. Scheduling latency for a Small Customer is bounded regardless of any other Customer's backlog. The default runtime gives every Customer equal share through plain round-robin scheduling; the Scheduler contract also supports an opt-in weighted path for differentiated shares without touching the WorkerPool or Outbox.
- **Runnable service composition**: the notifier entrypoint wires ingest, Registry, Scheduler, Outbox, HostLimiter, WorkerPool, DB-backed discovery, observability, and graceful shutdown. Package-level tests and benchmark drivers exercise mechanics, but do not replace a runnable service.

## Non-goals (PoC)

- Webhook management APIs (assumed implemented elsewhere).
- Strict per-Customer FIFO. Per-Customer queues drain in arrival order single-threaded, but with multiple workers some interleaving is possible. Documented trade-off.
- Multi-region delivery.
- Production-grade auth on the ingest path.
- Partner-side replay UI; the DLQ is a queryable table.

## High-level architecture

Animated walkthrough sources for the key runtime flows live in
[docs/animations/](animations/). The simplified system path is in
[overall-architecture.mmd](animations/overall-architecture.mmd); focused
walkthroughs cover multi-Customer claiming, retry classification, Webhook
circuit breaker lifecycle, and host limiter requeue behavior.

```mermaid
flowchart LR
    subgraph Producers
      P1[Business app emits Event JSON]
    end

    subgraph "Webhook Notifier (horizontally scalable)"
      direction LR
      I[HTTP ingest]
      Q[Outbox fanout]
      S[Per-Customer scheduling]
      W[Worker replicas]
      R[Retry and partner protection]
    end

    subgraph Postgres
      direction TB
      PG[(durable Event and Delivery queue)]
    end

    subgraph Partners
      PA[Integration Partners]
    end

    P1 -->|POST /api/events| I
    I -->|persist Event and create Deliveries| Q
    Q --> PG
    PG -->|claimable work| S
    S -->|fair Customer picks| W
    W --> R
    R -->|signed HTTP POST| PA
    PA -->|response| R
    R -->|success, retry, or dead| PG

    classDef store fill:#eef,stroke:#88a;
    class PG store;
```

## Component interaction

The high-level diagram above shows the coarse system shape. This diagram shows
the main runtime interactions between the notifier components and Postgres
tables. Detailed mechanics are in the animation sources for
[claiming](animations/claim-next-batch.mmd),
[retry](animations/retry-policy.mmd),
[breakers](animations/breaker-lifecycle.mmd), and
[host limiting](animations/host-limiter.mmd).

```mermaid
flowchart LR
    subgraph Ingest
      API[HTTP ingest]
      EI[EventIngest]
      N[Scheduler notifier]
    end

    subgraph Runtime
      DISC[DB-backed discovery]
      S[Scheduler]
      W[WorkerPool]
      SIGN[Signer]
      HOST[HostLimiter]
      RETRY[RetryPolicy]
    end

    subgraph Postgres
      EV[(events)]
      WH[(webhooks)]
      DV[(deliveries)]
      CL[(delivery_claims)]
      AT[(delivery_attempts)]
      DL[(dead_letters)]
    end

    subgraph Partners
      P[Integration Partners]
    end

    API --> EI
    EI -->|insert Event| EV
    EI -->|lookup Webhooks| WH
    EI -->|create Deliveries| DV
    EI --> N
    N --> S

    DV -->|claimable Customers| DISC
    CL -->|stale claims| DISC
    WH -->|breaker eligibility| DISC
    DISC --> S
    S -->|next Customer set| W

    W -->|claim Deliveries| DV
    W -->|write claim rows| CL
    W --> SIGN
    SIGN --> HOST
    HOST -->|HTTP POST| P
    P -->|response| W
    W --> RETRY
    RETRY -->|record Attempt| AT
    RETRY -->|success or retry| DV
    RETRY -->|terminal dead| DL

    classDef store fill:#eef,stroke:#88a;
    class EV,WH,DV,CL,AT,DL store;
```

## Component responsibilities

### Deep modules (pure logic, isolated unit tests)

- **Scheduler** — Given the set of Customers with pending Deliveries, returns the next Customer to serve. The default PoC service uses plain round-robin with `container/list.List` + `map` for O(1) Notify and Remove, so all Customers get equal share. An opt-in Smooth Weighted Round Robin (SWRR, Nginx-style) implementation can read weights from `customers.class`, but it uses slice scans and rebuilds rather than the round-robin list+map structure, so divergent weights require profiling before production use. The interface is the load-bearing contract; the implementation (RR / SWRR / DRR) is the trade-off (ADR-0001).
  ```go
  type Scheduler interface {
      // Pick returns the next Customer to serve and an implementation-specific
      // batch hint. The worker clamps this with its own MinBatchHint and
      // MaxBatchHint when deciding how many Deliveries to claim.
      // batchHint == 0 means "no work".
      Pick(ctx context.Context) (id CustomerID, batchHint uint32, ok bool, err error)
      Notify(CustomerID) // wake the scheduler when work arrives
      Remove(CustomerID) // stop tracking the Customer when it has no remaining work
  }

  // WeightSource resolves a Customer's fairness weight. The default service path
  // uses plain round-robin; the opt-in SWRR path reads `customers.class` from DB.
  // The Scheduler depends on this interface, not on storage.
  type WeightSource interface {
      Weight(CustomerID) uint32 // 0 = paused; >=1 = share multiplier
  }
  ```
  SWRR interleaves picks so a `[3,1]` weight set yields `AABAAABA` rather than classic contiguous `AAAB AAAB` cycles — same share, with the low-weight Customer pulled forward as much as the 3:1 ratio permits. With all weights=1, behaviour is identical to plain RR. Weight controls how often a Customer is picked. Claim batch size is a separate database-amortization knob. A throttled Customer with `weight=0` is skipped entirely (useful for incident response).
- **RetryPolicy** — `Next(attempt int, prevDelay time.Duration, resp *http.Response, err error, lifetime time.Duration) (delay time.Duration, terminal bool, reason string)`. Pure function. Encodes decorrelated jitter, 4xx fast-fail, 429 `Retry-After` honouring, exhaustion, and MaxAge expiry (ADR-0003, ADR-0009). Animation source: [retry-policy.mmd](animations/retry-policy.mmd).
- **Signer** — `Sign(secret, body []byte, ts time.Time) (sig string)`. HMAC-SHA256 over `timestamp.body`. Deterministic.

### Service modules

- **EventIngest** — Accepts an Event and synchronously fans out one Delivery row per matching Webhook in a single transaction. Same transaction = no Delivery is ever orphaned from its Event.
  Keep synchronous fanout for the PoC: the expected Webhook count per Customer is small, and the transaction freezes routing at Event acceptance time. If ingest-latency measurements later justify asynchronous fanout, persist an immutable routing snapshot or equivalent durable fanout intent with the Event before returning success. A background fanout worker must not resolve a mutable Webhook set later and silently change which Integration Partners receive the Event.
  Track Event ingest latency by fanout size. Until a product API SLO is defined, use provisional `p99 < 200ms` under realistic Webhook fanout as the threshold for reconsidering asynchronous fanout.
- **Outbox** (the `deliveries` table) — The durable work queue. Workers claim Deliveries atomically through a CTE that selects ready rows (no active claim, `next_attempt_at <= now()`) and stale-claim rows (claim lease expired), locks them with `FOR UPDATE SKIP LOCKED`, and atomically inserts/updates a row in `delivery_claims` via `INSERT … ON CONFLICT DO UPDATE WHERE claimed_at < now() - lease`. This one-CTE claim eliminates the `in_flight` status column from the `deliveries` row, avoids dead-tuple bloat in the hot queue table, and recovers abandoned claims without a separate sweeper process (ADR-0011). The claim query optionally integrates circuit breaker filtering via a probe CTE that transitions `open` Webhooks to `half_open` for a single probe Delivery. For sparse-Customer workloads the store also exposes `ClaimNextBatch` so many low-volume Customers do not pay one claim round-trip each. `ClaimNextBatch` de-duplicates its input Customer IDs and probes each Customer independently through a `CROSS JOIN LATERAL` over the distinct input set (rather than a single `customer_id = ANY($1)` scan), capping each Customer's contribution to a fair share of the batch limit (`batch limit / number of input Customers`, floored at 1) before a final time-ordered cut. The per-Customer cap bounds how many rows the claim locks `FOR UPDATE` — without it a single high-volume Customer could lock up to `N × batch limit` rows across `N` co-claimed Customers and monopolize the batch, starving co-picked Small Customers within that claim. Animation source: [claim-next-batch.mmd](animations/claim-next-batch.mmd).
- **WorkerPool** — N goroutines per process. Each loop:
  1. `id, batchHint, ok := Scheduler.Pick()` — get next Customer and how many Deliveries the worker may serve before yielding.
  2. Claim up to the configured batch size for that Customer (single CTE-based SQL statement, see Outbox). The PoC uses a default minimum batch of 32 Deliveries per pick and validates throughput against the Small Customer scheduling-latency SLO.
  3. When `MultiPickCount > 1`, the worker collects up to N Customers from the Scheduler before issuing a single `ClaimNextBatch` call — one DB round-trip amortised across many sparse Customers. After the batch claim, `HasClaimableWorkBatch` checks whether empty-pick Customers truly have no work (another worker may have claimed their only row) and removes them from rotation if none remain.
  4. For each claimed Delivery, in parallel (semaphore-capped goroutines):
      - Look up Webhook (cached via SubscriptionRegistry).
      - Try to acquire the per-host semaphore without blocking (skip and requeue with short backoff if unavailable; yield back to Scheduler so a saturated partner does not stall fairness).
      - `Signer.Sign()`, build HTTP request with idempotency headers.
      - POST with a shared tuned HTTP client and per-Attempt request timeout.
      - Drain and close the response body so the transport can reuse the connection.
      - `RetryPolicy.Next()` decides outcome.
  5. Batch-mark all outcomes by kind in one SQL round-trip per outcome (`MarkSucceededBatch`, `MarkRetryBatch`, `MarkDeadBatch`) using `UNNEST` arrays + CTE transactions that delete `delivery_claims` rows and update `deliveries` atomically, including breaker state recording via set-based SQL (no per-item loops).
  6. Return to step 1.
- **SubscriptionRegistry** — Read-only `customer_id → []Webhook`. In-memory LRU with short TTL; cache miss falls back to PG. Avoids hot lookup on every Delivery.
- **DLQ** (`dead_letters` table) — Terminal Deliveries with reason (`exhausted`, `terminal_4xx`, `terminal_network`). Written at MarkDead time alongside the `deliveries.status = 'dead'` update.
  Terminal `deliveries` rows stay in the live queue for a configurable operational retention window (initially 24 hours), after which the archive process copies them into a time-partitioned `delivery_history` table and removes them from `deliveries` so claim indexes do not accumulate unbounded terminal rows (ADR-0010).
- **Process-local HostLimiter** — `map[host]*semaphore.Weighted`. Enforces a per-process limit per Integration Partner hostname, default 20. This is the PoC's partial enforcement of the Host Concurrency Cap; a horizontally scaled production deployment needs fleet-wide coordination to preserve the partner-protection invariant. Animation source: [host-limiter.mmd](animations/host-limiter.mmd).
- **Webhook circuit breaker** — Persisted per-Webhook breaker state (`closed`, `open`, `half_open`) that keeps chronically failing Webhooks out of claim/discovery until a cooldown permits one probe. Complements RetryPolicy and the Host Concurrency Cap: RetryPolicy decides the outcome of an Attempt, HostLimiter limits local pressure on a hostname, and the breaker stops a broken Webhook from repeatedly occupying worker capacity across many Deliveries. Animation source: [breaker-lifecycle.mmd](animations/breaker-lifecycle.mmd).

## Data model

```
events                deliveries             delivery_claims        delivery_attempts      dead_letters
─────────────         ───────────────        ──────────────         ─────────────────      ───────────────
id (uuid)             id (uuid)              delivery_id (uuid PK)  delivery_id            delivery_id
customer_id           event_id        ──fk─> id                     id                     last_error
type                  webhook_id             attempt_number         attempt_number         attempts
payload (jsonb)       customer_id            claimed_at             status_code            reason
created_at            status                                        error_class            final_at
                      attempt_count                                 duration_ms
                      next_attempt_at                               sent_at
                      last_error
                      created_at
                      updated_at

webhooks
───────────
id (uuid)
customer_id
url
secret (encrypted)
secret_previous (encrypted, nullable)
secret_rotated_at (timestamptz, nullable)
event_types (text[])
active (bool)
breaker_state (text)
breaker_failure_count
breaker_opened_at
breaker_next_probe_at
breaker_cooldown_ms
breaker_probe_delivery_id

customers                              delivery_history (partitioned)
──────────                             ─────────────────────────────
id (uuid)                              delivery_id + final_at (PK)
class (int, default 1, check >= 0)     event_id, webhook_id, customer_id
                                       event_type, url_host
                                       final_status, terminal_reason
                                       attempt_count, first/last_attempt_at
                                       last_status_code, last_error_class, last_error
                                       delivery_created_at, final_at, archived_at
```

Table purposes:

- `events` stores the accepted business Event payload once.
- `webhooks` stores Integration Partner subscriptions, signing material, and breaker state.
- `customers` stores Customer identity and optional scheduling class for the weighted path.
- `deliveries` is the live Postgres work queue: one row per Event/Webhook fanout target.
- `delivery_claims` is the authoritative worker-claim lease table.
- `delivery_attempts` is the audit trail of HTTP Attempts that reached dispatch.
- `dead_letters` stores terminal failure metadata for operational inspection.
- `delivery_history` archives terminal Deliveries after the live-queue retention window.

`delivery_claims` is the authoritative claim store (ADR-0011). A Delivery is claimed when a row exists in `delivery_claims` with `claimed_at` within the claim lease window. The `deliveries.status` column no longer stores `in_flight`; claim queries join against `delivery_claims` to detect already-claimed rows. `deliveries.claimed_at` (a residual column from the initial schema) is no longer used as the claim source.

`dead_letters` records terminal metadata at MarkDead time. The `deliveries` row persists with `status = 'dead'` until the archive process copies it into `delivery_history` (a time-partitioned table) and deletes it from the live queue (ADR-0010).

Indexes:
- `deliveries (customer_id, status, next_attempt_at)` — scheduler/worker hot path.
- `deliveries (customer_id, next_attempt_at, created_at, id) WHERE status IN ('pending', 'failed_retry')` — ready-candidate claim scan (partial, index-friendly).
- `delivery_claims (claimed_at, delivery_id)` — stale-claim scan.
- `delivery_attempts (delivery_id, attempt_number)` — audit lookup.
- `deliveries (webhook_id)` — webhook circuit-breaker consecutive-failure join (`delivery_attempts ⋈ deliveries` on `webhook_id`).
- `deliveries (customer_id, next_attempt_at) WHERE status IN ('pending', 'failed_retry')` — claimable-customer discovery scan.
- `dead_letters (final_at desc)` — ops inspection.
- `delivery_history (customer_id, final_at desc)` — historical lookup.

## Lifecycle: a Delivery from cradle to terminal state

```mermaid
stateDiagram-v2
    [*] --> pending: EventIngest creates Delivery rows<br/>(one per matching Webhook)
    pending --> claimed: worker claims (atomic CTE<br/>INSERT delivery_claims<br/>SKIP LOCKED)
    claimed --> succeeded: 2xx
    claimed --> dead: terminal classification<br/>(4xx not 408/429,<br/> or attempts exhausted,<br/> or MaxAge expired)
    claimed --> failed_retry: retryable failure
    claimed --> pending: claim-lease expiry<br/>(stale_candidate reclaim)
    failed_retry --> pending: next_attempt_at reached<br/>(becomes claimable again)
    succeeded --> [*]
    dead --> [*]
```

`claimed` is not a persisted `deliveries.status` value. It is represented by a row in the `delivery_claims` table (ADR-0011). When a claim is active the Delivery's `status` remains `pending` or `failed_retry`; the claim ownership lives in `delivery_claims`. Claim queries join against `delivery_claims` to detect already-claimed rows and project a synthetic `'in_flight'` status for the worker. Stale claims (older than `claimLease`) are reclaimed atomically by the claim query's `ON CONFLICT … DO UPDATE WHERE claimed_at < now() - lease` clause — no separate sweeper process.

## Reliability mechanics

- **Outbox semantics**: Events and their derived Deliveries are written in the same transaction as the business write that emitted them. No business commit without scheduled delivery.
- **Claim atomicity**: The claim CTE atomically selects candidate rows with `FOR UPDATE SKIP LOCKED` and inserts into `delivery_claims` using `INSERT … ON CONFLICT (delivery_id) DO UPDATE … WHERE delivery_claims.claimed_at < now() - lease`. The `ON CONFLICT` handles the race where two workers select the same candidate: one succeeds on INSERT, the other's DO UPDATE is blocked by the WHERE clause unless the existing claim is stale. `SKIP LOCKED` prevents queueing behind an in-progress claim on a different row.
- **Crash recovery**: No separate sweeper process. The claim query handles recovery in two stages within the same CTE:
  1. A `stale_candidate` CTE selects deliveries whose `delivery_claims.claimed_at` is older than the claim lease TTL (default 5 minutes).
  2. The `INSERT … ON CONFLICT DO UPDATE WHERE delivery_claims.claimed_at < now() - lease` atomically reclaims the stale claim — a new worker can steal an abandoned claim without a separate timeout loop.
- **Lock ordering**: every transaction that locks both `deliveries` and `delivery_claims` acquires `deliveries` first, then `delivery_claims`. The claim path locks `deliveries` with `FOR UPDATE SKIP LOCKED` before inserting/updating `delivery_claims`; `withInFlightDelivery` (used by `Requeue` and breaker state writes) locks the `deliveries` row before its `delivery_claims` row via ordered CTEs. A consistent acquisition order is required because the claim and requeue paths run concurrently on overlapping rows — an inverted order produces `SQLSTATE 40P01` deadlock aborts.
- **Idempotency contract**: every Attempt carries `X-Delivery-Id` (stable across retries of one Delivery) and `X-Event-Id` (stable across all Deliveries derived from one Event). Partners dedupe on these.

## Security

- **Replay protection**: partners should reject an Attempt when `abs(now() - X-Signature-Timestamp) > 5 minutes`. The HMAC already binds the timestamp to the body; the freshness window prevents captured requests from being replayed indefinitely.
- **Secret rotation**: `webhooks.secret_previous` and `webhooks.secret_rotated_at` support a grace window during customer-driven secret rotation. While the rotation is in progress, partners should accept signatures computed with either the current secret or the previous secret. Once the grace window expires, `secret_previous` can be cleared.
- **Rotation flow**: a Customer updates the Webhook secret through the management API, the notifier stores the new current secret and preserves the prior secret for the configured grace period, and partner verification accepts both values until the rotation window closes.
- **Signer behavior**: the notifier always signs Attempts with the current secret. Dual-acceptance during rotation is a partner-side verification concern, not a second signing path in the notifier.

## Scalability mechanics

- **Stateless workers**: all coordination is through Postgres. Workers can be added or removed without coordination.
- **PgBouncer transaction pool with prepared-statement tracking**: the local stack runs `edoburu/pgbouncer:v1.25.1-p0`, which is newer than PgBouncer 1.21 and supports protocol-level prepared statements in transaction pooling mode when `MAX_PREPARED_STATEMENTS` is non-zero. The notifier keeps the pgx default cached-statement execution mode instead of forcing simple protocol, so hot claim and mark CTEs can avoid repeated server parse/plan work while still using PgBouncer.
- **Shared HTTP transport**: each Worker owns one tuned HTTP client with large idle connection pools and explicit transport timeouts (ADR-0007). Per-Attempt deadlines come from request contexts, not `http.Client.Timeout`, so response bodies can be drained before cancellation and idle connections can be reused.
- **No leader**: the Scheduler is process-local. Postgres claim locking prevents duplicate Delivery processing across instances, but globally balanced Customer fairness requires every instance to discover the claimable Customer set from shared storage. DB-backed claimable Customer discovery is the chosen path in ADR-0006; it gives approximate cluster-wide fairness without a globally serialized Scheduler.
- **Layered wakeups**: successful ingest wakes the local Scheduler synchronously and enqueues an advisory remote wake hint. A bounded broadcaster coalesces hints per Customer for 100ms and sends `pg_notify` through a dedicated direct Postgres connection; periodic DB-backed discovery remains the recovery path if a hint is dropped or Postgres notification is unavailable.
- **Scale ceiling**: at very high throughput, contention on the `deliveries` claim query becomes the bottleneck. The natural next step is to partition by `customer_id`. Migrating to Redis Streams or Kafka is a larger change — the `Scheduler` interface stays, but the outbox claim, discovery, and ingest layers are Postgres-coupled and would need simultaneous rewrites.

## Fairness mechanics

The single technical claim that drives most of the design: **the unit of fairness is the Customer, not the Delivery, not the worker, not the partner host**.

- The Scheduler iterates over **the set of Customers with pending Deliveries**, not over the Deliveries themselves. This is the key insight. If we iterated over Deliveries, a Whale's large burst would dominate. Iterating over Customers gives each one a share per cycle proportional to its weight, regardless of backlog depth.
- A Customer with more Webhooks produces more Deliveries but does not receive more scheduling capacity automatically. Webhook fanout increases backlog, not share. Explicit Customer weight is the only mechanism for differentiated scheduling capacity.
- **Per-Customer concurrency cap is a complementary future safeguard.** Round-robin controls admission order, but slow Attempts from one Customer can still accumulate in flight and occupy worker capacity. Customer weight controls scheduling share; a concurrency cap would bound simultaneous resource use. It does not replace Customer-based scheduling.
- A production per-Customer concurrency cap should be fleet-wide. A process-local layer may reduce coordination load, but cannot preserve the deployment-wide bound alone. It can reuse a lease-based distributed semaphore pattern keyed by Customer, independently of the Host Concurrency Cap keyed by Integration Partner.
- **Equal share by default, weighted path available.** The default service uses plain round-robin, so all Customers get equal scheduling share. The Scheduler interface also supports Smooth Weighted Round Robin (SWRR): with uniform weights it degenerates to plain RR; with diverging weights it allocates proportional, interleaved slots (no contiguous bursts for high-weight Customers). The opt-in SWRR runtime path reads `customers.class` as the Customer weight through `dbweights.DBWeightSource`, with `class=0` treated as paused. Because the default path is still the O(1) list+map round-robin implementation, divergent weighted scheduling should be profiled before being treated as the production default.
- **Claim batch size is independent from Customer weight.** Weight controls how often a Customer is picked. Claim batch size controls database amortization per pick. The PoC uses a default minimum batch size of 32 (`MinBatchHint`) and a default upper bound of 64 (`MaxBatchHint`). In single-customer mode, the effective per-pick limit is the scheduler `batchHint` clamped by those worker-side bounds. In multi-customer mode, the total claim limit is the sum of each picked Customer's effective per-pick limit. Tune against the `< 2s` Small Customer scheduling-latency SLO rather than changing fairness policy to gain throughput.
- **Cross-customer claim batching.** When `MultiPickCount > 1`, the worker collects up to N Customers from the Scheduler and issues a single `ClaimNextBatch` call — one DB round-trip amortised across many sparse Customers. After the batch claim, `HasClaimableWorkBatch` checks whether empty-pick Customers truly have no work (another worker may have claimed their only row) and removes them from rotation. This is critical for sparse-Customer workloads where per-Customer claim queries would otherwise dominate; benchmark coverage for that workload belongs in [bench/README.md](../bench/README.md).
- **Batch-size tuning guidance.** `MinBatchHint` is the worker-side lower bound for claim batch size per pick (default 32). `MaxBatchHint` is the worker-side upper bound (default 64, `0` means unbounded). In single-customer mode (`claimSingle`), the worker clamps the scheduler hint through those bounds. In multi-customer mode (`claimMulti` / `MultiPickCount > 1`), the worker sums those per-pick limits and passes the total to `ClaimNextBatch`, whose SQL divides the limit back into a fair per-Customer share. Higher values reduce claim-query frequency and often increase peak throughput, but can worsen fairness tail latency because one Customer can occupy a worker longer per turn. Lower values increase pick interleaving and usually improve fairness at the cost of extra DB overhead. In practice: start around `32`–`64` for throughput-focused runs, and `1`–`8` for fairness-sensitive mixed workloads; then tune empirically with benchmark reports. Use `MultiPickCount > 1` to amortise DB round-trips across many sparse Customers without increasing per-Customer batch size.
- The Host Concurrency Cap is **orthogonal** to per-Customer fairness. It protects partners. Without it, a Whale's per-cycle Delivery slots still get through, but in close succession against the same hostname — partners 429. The PoC's process-local HostLimiter requeues with a short `next_attempt_at` bump on local cap timeout and returns to the Scheduler, so a locally saturated partner never blocks Pick.
- SWRR is the current weighted-scheduling candidate because Deliveries have uniform cost (one HTTP POST each) and weights are per-Customer, not per-Delivery. DRR exists for variable-cost work units; if Deliveries ever become heterogeneous (e.g. payload-size-weighted), the Scheduler implementation swaps without touching the WorkerPool (ADR-0001).
- The default PoC scheduler is plain round-robin (all Customers equal). It uses `container/list.List` + `map` for O(1) Notify and Remove — membership operations do not degrade with active Customer count. Divergent weighted scheduling is implemented behind the opt-in SWRR runtime path, but should be separately profiled for the expected active-Customer count before becoming the default; `internal/scheduler/swrr/` uses slice-based structures.

### Weight source

```
customers
──────────
id (uuid)
class (int, default 1, check >= 0)  -- 0 = paused
```

The default service path ignores `class` for scheduling and uses plain round-robin. When `EnableWeightedScheduler` is enabled in runtime config, `dbweights.DBWeightSource` reads `customers.class` through the `WeightSource` interface and caches values in memory. The cache is invalidated only through explicit `Refresh` / `RefreshAll`; there is no automatic TTL or live-update watcher in the current implementation.

## Per-host concurrency cap

```go
type HostLimiter struct {
    mu    sync.Mutex
    sems  map[string]*semaphore.Weighted
    cfg   HostLimiterConfig
}

func (h *HostLimiter) Acquire(ctx context.Context, host string) (release func(), err error) { ... }
func (h *HostLimiter) TryAcquire(host string) (release func(), err error) { ... }
```

Workers try to acquire before POSTing. If no slot is immediately available, the Delivery is requeued with the limiter's short `RequeueDelay` — the worker frees up to serve another Customer. This keeps fairness intact when one process saturates its local allowance.

The cap sets the upper bound on concurrent in-flight Attempts per Integration
Partner hostname. Approximate per-host throughput is `host_cap /
partner_latency`: a cap of 20 supports about 200 Attempts/s at 100 ms p50
partner latency, or about 400 Attempts/s at 50 ms. The default of 20 is sized
as a conservative starting point for the 300 events/s PoC target when traffic is
spread across multiple partner hostnames; concentrated traffic to one hostname
needs either lower partner latency, a higher configured cap, or more host-level
coordination capacity.

The Host Concurrency Cap is a fleet-wide partner-protection invariant. The PoC's
in-memory HostLimiter enforces only the local portion of that allowance. A
horizontally scaled production deployment needs distributed coordination, such
as a lease-based distributed semaphore. The mechanism is deferred until
deployment scale and failure semantics are known. A request-rate policy, such
as a token bucket, would be a separate control with separate configuration; it
does not enforce the Host Concurrency Cap.

If distributed coordination is configured but unavailable, new Attempts for
affected hostnames fail closed: Requeue them with a short delay rather than
falling back to the local HostLimiter alone. Existing in-flight Attempts
continue. This avoids multiplying Integration Partner load during a limiter
incident. The production implementation should expose coordination-failure
metrics so operators can distinguish limiter outages from partner saturation.

Production coordination should key the cap by normalized `hostname:port`.
Equivalent URLs using a scheme's default port share one key, while distinct
ports may carry independent allowances. The PoC currently keys its local
HostLimiter with `url.Hostname()` only. Webhook paths and Customer identity do
not belong in the key.

Production configuration should provide a conservative default cap plus
per-key overrides for Integration Partners with known capacity. Customer-level
overrides do not belong in this policy because the Host Concurrency Cap protects
Integration Partners, not Customers.

Distributed semaphore slots should be renewable leases. Acquire a slot before
POSTing, renew it while the Attempt remains in flight, release it when the
Attempt finishes, and allow it to expire after missed renewals. The lease TTL
must exceed the renewal interval and be bounded relative to the configured
request timeout so a crashed notifier instance cannot strand partner capacity
indefinitely.

If lease renewal fails while an Attempt is still in flight, cancel the HTTP
request before the lease expires and classify the outcome as retryable
coordination loss. Allowing the request to continue after its slot can be
reissued would violate the cap. A later retry keeps the stable `X-Delivery-Id`
so the Integration Partner can deduplicate an Attempt that reached it before
cancellation took effect.

Coordination loss before POSTing is a Requeue: it does not record an Attempt or
consume the Delivery retry budget because the Integration Partner saw no
request. Renewal loss after POSTing begins records an Attempt and uses normal
retry accounting because the Integration Partner may have observed the request.
Coordination loss does not contribute to opening the Webhook Circuit Breaker:
it indicates notifier infrastructure failure, not Integration Partner
destination health. Expose it through separate metrics.

Repeated pre-dispatch Requeues caused by coordination unavailability should use
jittered exponential backoff with a cap, then reset after a slot is acquired
successfully. A fixed short delay would create avoidable database churn during
a limiter outage.

## Webhook circuit breaker

Each Webhook has a persisted circuit breaker (ADR-0008):

- `closed`: Deliveries are claimable normally. Attempt success resets the failure counter. Retryable or terminal Attempt failures increment it.
- `open`: Deliveries for that Webhook are excluded from both claim and DB-backed claimable-Customer discovery. The queued work remains in the outbox; it just does not consume worker capacity.
- `half_open`: after cooldown, the claim query atomically picks one Delivery as `breaker_probe_delivery_id`, transitions the Webhook to `half_open`, and claims only that Delivery as the probe. Probe success closes the breaker; probe failure reopens it and backs off the next cooldown.

The breaker is **integrated into the claim/discovery SQL itself**, not applied as a separate filtering step after the claim returns. The claim CTE includes a `probe` CTE that atomically transitions `open` Webhooks to `half_open` and allows a single probe delivery to be claimed. Both the claim and discovery queries use `sqlBreakerEligiblePredicate` to exclude Webhooks that are `open` (unless their `breaker_next_probe_at` has elapsed), and the `claimed` CTE limits deliverable rows to Webhooks in `closed` state plus the designated probe. This keeps open breakers completely out of the work pool without extra round-trips.

The breaker is deliberately per-Webhook, not per-host. A single Integration Partner hostname may serve many independent Webhooks, and one broken Webhook configuration should not suppress other Customers' healthy Webhooks on the same host. The Host Concurrency Cap remains the host-level protection mechanism.

Default policy is conservative: 20 consecutive failures open the breaker, the first cooldown is 5 minutes, and repeated failed probes back off up to 15 minutes. The PoC uses consecutive failures rather than a sliding failure-rate window; that keeps the decision deterministic while still handling 100%-failing Integration Partner endpoints.

## Observability

Metrics are flagged as a discussion point in the assignment brief, but the PoC ships with the endpoints and instrumentation hooks so the discussion has concrete artefacts:

- **Prometheus `/metrics`** for delivery outcomes, latency, retry/DLQ behavior, scheduler/claim health, host limiter pressure, breaker state, backlog, and Postgres pool health. The exact metric catalogue belongs in [CONFIGURATION.md](CONFIGURATION.md#key-metrics).
- **`/health`** and **`/ready`** endpoints.
- **`/debug/pprof`** supported by `observe.InfraServer` when `cfg.Pprof` is enabled; the notifier binary keeps it disabled today.
- **Structured logging** via `log/slog` with `customer_id`, `event_id`, `delivery_id`, `attempt_number` fields on every line.

Dashboards and alerting are pair-session discussion: which p99 thresholds warrant paging, what fairness-violation alerts look like, how to detect a slow partner before its queue depth blows the SLO.

## Testing strategy

### Unit tests

- **Scheduler**: table-driven coverage of empty schedule, single-Customer back-to-back, multi-Customer round-robin invariant, and weighted invariant. Property test: over any arrival/draw sequence, each Customer's share equals `weight_i / Σ weight_j ± ε`. Uniform-weight case reduces to the equal-share invariant. `weight=0` Customers are never picked while pending.
- **RetryPolicy**: truth table from ADR-0003.
- **Signer**: vector tests with known fixtures.

### Integration tests

- **Outbox claim atomicity**: real Postgres (testcontainers). Spawn K concurrent workers, claim N Deliveries, assert no duplicates.
- **End-to-end happy path**: ingest → POST → success → row state.

### Benchmark scenarios

The benchmark harness and scenario map are maintained in [bench/README.md](../bench/README.md). Current pass/fail results are indexed in [bench/RESULTS.md](../bench/RESULTS.md), with detailed reports under `bench/results/`.

Design-level intent: the benchmark suite must keep covering the three assignment claims — reliability, horizontal scalability, and Customer fairness — plus internal hardening for failure modes that are too specific for the assignment-facing proof map. Avoid duplicating scenario setups or pass/fail gates here; those change with benchmark tuning and belong in the benchmark docs.

## What is deliberately out

- **SWRR as the default scheduler** — the Scheduler interface, `WeightSource` plumbing, `customers.class` column, and opt-in SWRR implementation are in place, but the default PoC runtime uses plain round-robin. Making divergent weights the default is a configuration and profiling decision, not a redesign.
- **Plan-tier → weight policy** — the `WeightSource` plumbing is in place, but the actual tier table and admin UI to set non-uniform weights are out of PoC scope. Adding them is a config change, not a redesign.
- **Redis Streams per-Customer queues** — a possible scale-out path if Postgres claim contention becomes the limiting factor. Would require changes in outbox, discovery, and ingest — the Scheduler stays, but the claim and persistence layers beneath it change. Not a free swap.
- **Per-Customer concurrency cap** — round-robin already paces; documented as natural addition under future needs.
- **Additional contention benches beyond the maintained benchmark map** — benchmark coverage and replacements are tracked in [bench/README.md](../bench/README.md) and [bench/RESULTS.md](../bench/RESULTS.md).
- **Mid-flight worker crash chaos scenario** — recoverable by design (stale_candidate CTE reclaim), demonstrated by an integration test rather than a bench scenario.
- **Multi-region**.
- **Webhook management APIs**.

## Risks and open trade-offs

- **No strict per-Customer FIFO.** Documented; partners are expected to handle out-of-order Events on their side using `X-Event-Id` and `created_at`.
- **In-process Scheduler means per-instance round-robin.** Cross-instance duplicate prevention comes from Postgres claim semantics; cross-instance fairness is only approximate until workers periodically discover claimable Customers from shared storage. Under imbalance, one instance can have a stale or partial claimable Customer view. Bounded by claim batch size once discovery is in place.
- **Fleet-wide fairness targets bounded Small Customer scheduling latency, not an exact global order.** DB-backed discovery should converge each process toward the shared claimable Customer set and prevent starvation. If multi-instance benchmarks show the scheduling-latency SLO is violated, escalate to Customer leasing or broker-backed per-Customer queues rather than globally serializing every Scheduler pick by default.
- **Fairness SLO uses Small Customer scheduling latency.** Measure the time from a Delivery becoming claimable to its first claim and evaluate p99 for Small Customers under Whale load. This isolates scheduling behavior from Integration Partner response time and retries. End-to-end Delivery latency remains a separate reliability metric.
- **Initial Small Customer scheduling-latency SLO: p99 < 2 seconds under Whale load.** This is a provisional product target to validate against production traffic assumptions. It is not a guarantee during dependency outages. Benchmark reports should state which current scenarios validate or supersede this target.
- **Retry-due Deliveries use the same fairness SLO once claimable.** Their scheduling-latency clock starts when `next_attempt_at` is reached, not while they are intentionally in backoff. Once due, they compete normally by Customer so recovering Integration Partners are not starved behind fresh Whale traffic.
- **Reclaimable stale claimed Deliveries use the same fairness SLO after claim-lease expiry.** Before lease expiry they remain owned by the original worker via the `delivery_claims` row. After expiry the stale-candidate CTE reclaims them atomically; they then compete normally by Customer. Track reclaimed Deliveries separately so crash-recovery latency remains visible.
- **Customers with only future work leave local Scheduler rotation.** A Customer whose remaining Deliveries are all waiting for `next_attempt_at` or claim-lease expiry is not claimable yet. Remove it from local rotation after an empty claim; DB-backed discovery re-adds it when work becomes claimable. This avoids hot Postgres polling during backoff.
- **Postgres as queue has a throughput ceiling.** The benchmark suite should keep measuring it; the scale-out path is in ADR-0002.
