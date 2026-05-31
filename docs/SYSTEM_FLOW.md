# Webhook Notifier — System Flow

## 1. Data Model

### Tables

```
  EVENTS                          DELIVERIES
  ────────────                    ───────────────────
  id           uuid PK            id              uuid PK  ─┐
  customer_id  uuid FK ─────┐     event_id        uuid FK  │
  type         text         │     webhook_id      uuid FK  ├─ indexes:
  payload      jsonb        │     customer_id     uuid FK  │  (customer_id,
  created_at   timestamptz  │     status          text     │   status,
                             │     attempt_count   int     │   next_attempt_at)
  WEBHOOKS                   │     next_attempt_at timestz │
  ────────────               │     last_error      text   ─┘
  id           uuid PK ─────┤     claimed_at      timestz
  customer_id  uuid FK      │     created_at      timestz
  url          text          │     updated_at      timestz
  secret       text          │
  event_types  text[]        │   DELIVERY_ATTEMPTS
  active       bool          │   ─────────────────
  breaker_state          text│     delivery_id    uuid PK
  breaker_failure_count  int  │     attempt_number int PK
  breaker_opened_at  timestz  │     status_code    int
  breaker_next_probe timestz  │     error_class    text
  breaker_cooldown_ms  bigint │     duration_ms    bigint
  breaker_probe_delivery uuid  │     sent_at        timestz
                              │
  DEAD_LETTERS                │   DELIVERY_HISTORY
  ─────────────────           │   ─────────────────
  delivery_id   uuid PK       │   (same columns as deliveries
  event_id      uuid          │    + archived_at timestamptz)
  webhook_id    uuid          │
  customer_id   uuid          │
  last_error    text          │
  attempts      int           │
  reason        text          │
  final_at      timestamptz   │
```

### Relationships

  events  ──1:N──>  deliveries     (one event fans out to many deliveries)
  webhooks ──1:N──> deliveries     (one webhook is target for many deliveries)
  deliveries ──1:N──> delivery_attempts  (audit trail of each HTTP POST)
  deliveries ──1:0-1──> dead_letters     (terminal failures)

---

## 2. High-Level Architecture

```
                            ┌──────────────────────────────────────┐
                            │         WEBHOOK NOTIFIER            │
                            │                                      │
  ┌──────────────┐         │  ┌──────────┐    ┌────────────┐     │
  │ Event Source  │         │  │  ingest  │    │  registry  │     │
  │ (business     │──/events─>│  .Store   │───>│  .Registry │────>│── webhooks
  │  writes)      │         │  │ Submit() │    │ (LRU TTL)  │     │
  └──────────────┘         │  └────┬─────┘    └────────────┘     │
                            │       │                               │
                            │       │ INSERT events + deliveries    │
                            │       │ Notify scheduler              │
                            │       ▼                               │
                            │  ┌──────────┐    ┌────────────┐     │
                            │  │scheduler │<───│ discovery  │     │
                            │  │ .Pick()  │    │ (periodic) │────>│── deliveries
                            │  └────┬─────┘    └────────────┘     │
                            │       │ customerID + batchHint        │
                            │       ▼                               │
                            │  ┌───────────────────────────────┐   │
                            │  │      worker.Worker.Run()      │   │
                            │  │                               │   │
                            │  │ 1. ClaimNextN(customer, lim)  │   │
                            │  │ 2. For each delivery:         │   │
                            │  │    a. HostLimiter.Acquire()   │   │
                            │  │    b. Pre-flight MaxAge check │   │
                            │  │    c. Signer.Sign()           │   │
                            │  │    d. HTTP POST to partner    │   │
                            │  │    e. RetryPolicy.Next()      │   │
                            │  │ 3. Batch-mark outcomes        │   │
                            │  │ 4. Notify(customer)           │   │
                            │  └──────┬───────┬───────┬────────┘   │
                            └─────────┼───────┼───────┼────────────┘
                                      │       │       │
                  ┌───────────────────┘       │       └──────────────────┐
                  ▼                           ▼                          ▼
         ┌────────────────┐        ┌──────────────────┐        ┌──────────────────┐
         │ Integration    │        │ Integration      │        │    Postgres      │
         │ Partner A      │        │ Partner B        │        │                  │
         │ (healthy 2xx)  │        │ (flaky/5xx/outage)│        │ events           │
         └────────────────┘        └──────────────────┘        │ deliveries       │
                                                                │ attempts         │
                                                                │ dead_letters     │
                                                                │ webhooks         │
                                                                │ history          │
                                                                └──────────────────┘
```

---

## 3. End-to-End Flow

```
  EVENT      INGEST      REGISTRY      POSTGRES      SCHEDULER     WORKER      HOSTLIMITER    PARTNER
  SOURCE       │            │             │              │           │             │            │
    │          │            │             │              │           │             │            │
    │──Submit()─>           │             │              │           │             │            │
    │          │            │             │              │           │             │            │
    │          │──ListActiveForEvent()──> │              │           │             │            │
    │          │<──webhooks──│             │              │           │             │            │
    │          │            │    BEGIN TXN               │           │             │            │
    │          │            │    INSERT events            │           │             │            │
    │          │            │    INSERT deliveries (x N)  │           │             │            │
    │          │            │    COMMIT                   │           │             │            │
    │          │            │             │              │           │             │            │
    │          │────────Notify(customerID)───────────────>           │             │            │
    │          │            │             │              │           │             │            │
    │          ═══════════════════════════ WORKER LOOP ════════════════════════════════════════════╗
    │          │            │             │              │           │             │            │  ░
    │          │            │             │              │<──Pick()──│             │            │  ░
    │          │            │             │              │──(cID,bh)─>             │            │  ░
    │          │            │             │<─ClaimNextN()─│           │             │            │  ░
    │          │            │             │──[Deliveries]─>           │             │            │  ░
    │          │            │             │              │           │             │            │  ░
    │          │            │             │              │           │──Acquire(host)──>         │  ░
    │          │            │             │              │           │<──release─────│           │  ░
    │          │            │             │              │           │             │            │  ░
    │          │            │             │              │           │  pre-flight MaxAge        │  ░
    │          │            │             │              │           │  ┌─ if expired: MarkDead  │  ░
    │          │            │             │              │           │  └─ if ok: POST           │  ░
    │          │            │             │              │           │    Signer.Sign()          │  ░
    │          │            │             │              │           │    X-Signature-256        │  ░
    │          │            │             │              │           │─────────────────────────────>  ░
    │          │            │             │              │           │<──────────200 OK───────────  ░
    │          │            │             │              │           │             │            │  ░
    │          │            │             │              │           │  RetryPolicy.Next()        │  ░
    │          │            │             │              │           │  ┌─ 2xx -> MarkSucceeded  │  ░
    │          │            │             │              │           │  ├─ 5xx -> MarkRetry      │  ░
    │          │            │             │              │           │  └─ 4xx -> MarkDead       │  ░
    │          │            │             │<─batch-mark──│           │             │            │  ░
    │          │            │             │              │           │  Notify(cID)              │  ░
    │          │            │             │              │<─────────────           │            │  ░
    │          ════════════════════════════ END LOOP ═════════════════════════════════════════════╝
```

---

## 4. Delivery State Machine

```
                        EventIngest creates one delivery
                        per matching webhook
                              │
                              ▼
                      ┌───────────────┐
           ┌─────────>│   pending     │
           │          └───────┬───────┘
           │                  │ worker claims
           │                  │ SELECT FOR UPDATE SKIP LOCKED
           │                  ▼
           │          ┌───────────────┐
           │          │  in_flight    │──────────────────────────────┐
           │          └───────┬───────┘                              │
           │                  │                                      │
           │    ┌─────────────┼─────────────────┐                    │
           │    │             │                  │                   │
           │    ▼             ▼                  ▼                   │
           │  ┌────────┐ ┌───────────┐ ┌──────────────┐             │
           │  │success │ │failed_    │ │    dead      │             │
           │  │ -ed    │ │ retry     │ │  (DLQ)       │             │
           │  └────────┘ └─────┬─────┘ └──────────────┘             │
           │                   │                                     │
           │                   │ next_attempt_at reached             │
           └───────────────────┘                                     │
                                                                     │
                          ┌──────────────────────────────────────────┘
                          │
                    ┌─────┴──────┐
                    │ NOTE       │
                    │ in_flight  │
                    │ lease = 5m │
                    │ crash recov│
                    └────────────┘

  Transitions:
    pending ────worker claim───> in_flight     (attempt_count++)
    in_flight ──2xx────────────> succeeded     (final state)
    in_flight ──retryable──────> failed_retry  (5xx/429/timeout, backoff)
    in_flight ──requeue────────> failed_retry  (host backpressure, no attempt)
    in_flight ──terminal───────> dead          (4xx/exhausted/expired, DLQ)
    failed_retry ──timer up────> pending       (reclaimable by any worker)
```

---

## 5. Claim CTE — The Atomic Heart

  The single SQL CTE that atomically selects, filters, claims, and returns
  delivery data to the worker. One round trip, no duplicates.

```
  ClaimNextN(customerID, limit)

  Step 1: Select candidates
  ─────────────────────────────────────────────────────────────
  SELECT FROM deliveries d
  JOIN webhooks w ON w.id = d.webhook_id
  WHERE d.customer_id = $1
    AND ( (d.status IN ('pending','failed_retry')
           AND d.next_attempt_at <= now())
          OR
          (d.status = 'in_flight'
           AND d.claimed_at < now() - $lease) )

  Step 2: Filter through circuit breaker
  ─────────────────────────────────────────────────────────────
    AND ( w.breaker_state = 'closed'
          OR
          (w.breaker_state = 'open'
           AND w.breaker_next_probe_at <= now()
           AND this delivery IS the probe)
          OR
          (w.breaker_state = 'half_open'
           AND d.id = w.breaker_probe_delivery_id) )

  Step 3: Atomic claim (single UPDATE ... RETURNING)
  ─────────────────────────────────────────────────────────────
  UPDATE deliveries SET
    status = 'in_flight',
    attempt_count = attempt_count + 1,
    claimed_at = now()
  WHERE d.id IN (<step 1 candidates filtered by step 2>)
  RETURNING d.*,
    e.payload, e.type,                  -- from events
    w.url, w.secret,                    -- from webhooks
    c.weight,                           -- from customers
    prev.delay_millis                   -- from delivery_attempts

  Safety properties:
    FOR UPDATE SKIP LOCKED  ->  no two workers claim the same row
    leased in_flight        ->  another worker reclaims after 5min
    breaker-filtered        ->  open/half_open webhooks isolated
```

---

## 6. Worker Inner Loop

```
  ┌─────────────────────────────────────────────────────────────┐
  │                    Worker.Run()                             │
  │                                                             │
  │    ┌────────┐    ┌──────────┐    ┌────────┐    ┌─────────┐ │
  │    │ Pick   │───>│ ClaimNext│───>│Any?    │───>│Fan-out  │ │
  │    │ from   │    │ N(cust,  │    │        │    │gorouti- │ │
  │    │ Sched  │    │ max(32,  │    │ yes    │    │nes max16│ │
  │    │        │    │ bhint)   │    │        │    │         │ │
  │    └───┬────┘    └──────────┘    └───┬────┘    └────┬────┘ │
  │        │                              │               │      │
  │    ┌───┴────┐              ┌──────────┴───────┐      │      │
  │    │ no work│              │ yes, none        │      │      │
  │    │backoff │              │ claimable        │      │      │
  │    │ 50ms   │              │                  │      │      │
  │    └────────┘              │ HasPendingWork?   │      │      │
  │        │                   └──────┬───────────┘      │      │
  │   ┌────┴──────┐                  │                  │      │
  │   │back to top│           ┌──────┴──────┐           │      │
  │   └───────────┘           │ yes: Notify  │          │      │
  │                           │ no: Remove   │          │      │
  │                           └──────────────┘          │      │
  │                                                      │      │
  │                          ┌───────────────────────────┘      │
  │                          ▼                                  │
  │                   ┌─────────────┐                           │
  │                   │ Per delivery │                          │
  │                   │              │                          │
  │                   │ 1. HostLimit │                          │
  │                   │    Acquire   │                          │
  │                   │    ┌────┐    │                          │
  │                   │    │time│    │  → Requeue(delay)        │
  │                   │    │out?│    │                          │
  │                   │    └────┘    │                          │
  │                   │    │ yes     │                          │
  │                   │    ▼         │                          │
  │                   │ 2. MaxAge    │                          │
  │                   │    check     │                          │
  │                   │    ┌────┐    │                          │
  │                   │    │exp?│    │  → MarkDead(age_exceeded)│
  │                   │    └────┘    │                          │
  │                   │    │ yes     │                          │
  │                   │    ▼         │                          │
  │                   │ 3. Sign +    │                          │
  │                   │    POST      │                          │
  │                   │              │                          │
  │                   │ 4. classify  │                          │
  │                   │    ┌──────┐  │                          │
  │                   │    │2xx   │  │  → succeeded batch       │
  │                   │    │retry │  │  → retry batch           │
  │                   │    │dead  │  │  → dead batch            │
  │                   │    └──────┘  │                          │
  │                   └──────────────┘                          │
  │                          │                                   │
  │                          ▼                                   │
  │                   ┌──────────────┐                          │
  │                   │ Batch-mark   │                          │
  │                   │ outcomes to  │                          │
  │                   │ DB (1-3 SQL) │                          │
  │                   └──────┬───────┘                          │
  │                          │                                   │
  │                   ┌──────┴──────┐                           │
  │                   │ Notify(cust) │                           │
  │                   │ back to     │                           │
  │                   │ Scheduler   │                           │
  │                   └──────┬──────┘                           │
  │                          │                                   │
  │                          ▼                                   │
  │                    back to top                               │
  └─────────────────────────────────────────────────────────────┘
```

---

## 7. Circuit Breaker State Machine

```
                    Webhook created
                         │
                         ▼
                  ┌───────────────┐
       ┌─────────>│   closed      │
       │          └───────┬───────┘
       │                  │
       │         ┌────────┴────────┐
       │         │                 │
       │    success resets   20 consecutive
       │    failure count    failures
       │         │                 │
       │         ▼                 ▼
       │          ┌───────────────┐
       │          │    open        │
       │          └───────┬───────┘
       │                  │
       │          cooldown expired
       │          (5min, x2 per trip, max 15min)
       │                  │
       │                  ▼
       │          ┌───────────────┐
       │          │  half_open    │
       │          └───────┬───────┘
       │                  │
       │         ┌────────┴────────┐
       │         │                 │
       │    probe success    probe fails
       │    ──> closed       ──> open
       │                         │
       └─────────────────────────┘

  Config:
    FailureThreshold: 20 consecutive failures (0 = disabled)
    InitialCooldown:  5 minutes
    MaxCooldown:     15 minutes

  What happens:
    open:      Deliveries excluded from all claim queries
    half_open: Exactly one probe delivery auto-selected and allowed
    closed:    Normal operation, all deliveries claimable
```

---

## 8. Retry Policy Decision Tree

```
                   ┌───────────────┐
                   │  HTTP response │
                   └───────┬───────┘
                           │
        ┌──────────────────┼──────────────────────┐
        ▼                  ▼                      ▼
   ┌───────────┐    ┌───────────────┐    ┌───────────────┐
   │  2xx      │    │  4xx          │    │  5xx          │
   └─────┬─────┘    └───────┬───────┘    └───────┬───────┘
         │                  │                     │
         ▼           ┌──────┴───────┐            ▼
   Succeeded          │              │      Retryable5xx
   TERMINAL           ▼              ▼             │
                ┌──────────┐  ┌──────────┐         │
                │ 408      │  │ 429      │         │
                │ Retryable│  │          │         │
                └────┬─────┘  │ Retry-   │         │
                     │        │ After?   │         │
                     │        └────┬─────┘         │
                     │             │               │
                     │        ┌────┴─────┐         │
                     │        │ yes      │ no      │
                     │        │ use it   │ jitter  │
                     │        └──────────┘         │
                     │                             │
                     │         ┌───────────────────┘
                     │         │
                     ▼         ▼
                ┌───────────────┐
                │ error (nil?)   │
                └───────┬───────┘
                        │
              ┌─────────┴─────────┐
              │ timeout ──> Timeout│
              │ network ──> Network│
              └───────────────────┘
                        │
                        ▼
                 ┌───────────────┐
                 │ lifetime >=   │── yes ──> AgeExceeded TERMINAL
                 │ MaxAge?       │
                 └───────┬───────┘
                         │ no
                         ▼
                 ┌───────────────┐
                 │ attempt >=    │── yes ──> Exhausted TERMINAL
                 │ maxAttempts?  │
                 └───────┬───────┘
                         │ no
                         ▼
                ┌──────────────────────────┐
                │  decorrelated jitter     │
                │                          │
                │  delay = rand(           │
                │    baseDelay,            │
                │    min(prevDelay * 3,    │
                │        capDelay)         │
                │  )                       │
                │                          │
                │  FNV-64a hash over       │
                │  (attempt + prevDelay    │
                │   + reason + key)        │
                │  => simultaneous retries │
                │     don't collide        │
                └──────────────────────────┘

  Config:
    maxAttempts:  8
    baseDelay:    1 second
    capDelay:     5 minutes
    MaxAge:       24 hours
```

---

## 9. Discovery Loop — Seeding the Scheduler

```
  RunClaimableCustomerDiscovery(ctx, store, sched)

                  ┌─────────────────────┐
                  │ cursor = 0          │
                  │ backoff = 100ms     │
                  └─────────┬───────────┘
                            │
                            ▼
                  ┌─────────────────────┐
                  │ DiscoverClaimable-   │
                  │ Customers(ctx,       │
                  │   cursor, 100)       │
                  └─────────┬───────────┘
                            │
                            ▼
                    ┌───────────────┐
                    │  any found?   │
                    └───────┬───────┘
                            │
              ┌─────────────┴─────────────┐
              │ yes                       │ no
              ▼                           ▼
    ┌──────────────────┐       ┌──────────────────┐
    │ for each c:      │       │ sleep(backoff)   │
    │ sched.Notify(c)  │       │ backoff *= 2     │
    │ cursor = c.id    │       │ cap at max(30s)  │
    │ backoff = 100ms  │       │                  │
    └────────┬─────────┘       └────────┬─────────┘
              │                         │
              └─────────────┬───────────┘
                            │
                            ▼
                      back to top

  Purpose:
    The scheduler is process-local - it only knows about customers that
    called Notify(). When a new instance joins, it needs to discover
    customers with pending work from the database. This loop runs
    continuously in the background, paging through the deliveries table.

    Exponential backoff on empty results prevents hammering Postgres
    when there's nothing to do.
```

---

## 10. Key Design Properties

```
  Property                    Mechanism
  ──────────────────────────  ──────────────────────────────────────
  No event loss               Event + deliveries in same transaction
  No duplicate delivery       SELECT FOR UPDATE SKIP LOCKED
  Crash recovery              claimed_at < now() - lease reclaims
  Per-customer fairness       Scheduler iterates customers, not work
  Partner protection          HostLimiter caps per-hostname concurrency
  Retry backoff               Decorrelated jitter prevents herds
  Fast failure                4xx (except 408/429) -> terminal
  Misconfig isolation         Circuit breaker stops broken webhooks
  Horizontal scale            Workers stateless, PG coordinates
  Idempotency                 X-Delivery-Id stable across retries
  No leader                   Scheduler is process-local
```

---

## Quick Reference: All Components

```
  Package          Role                          Key Detail
  ───────────────  ────────────────────────────  ───────────────────────
  ingest.Store     Accept Event, fan-out         Single transaction
  registry.Registry  Cache webhooks per customer  LRU 10K, 30s TTL
  scheduler        Return next Customer          SWRR (RR for PoC)
  worker.Worker    Claim, POST, record outcome   Max 16 concurrent
  HostLimiter      Cap concurrency per host      Default 20
  retry.RetryPolicy  Classify HTTP outcome       Max 8 attempts
  signer.Signer    HMAC-SHA256 signing           X-Signature-256 header
  Discovery        Seed scheduler from DB        Cursor + backoff

  States:
    pending  ->  in_flight  ->  {succeeded, failed_retry, dead}
    failed_retry  ->  pending  (when next_attempt_at reached)

  Key SQL: ClaimNextN - single CTE with FOR UPDATE SKIP LOCKED,
    breaker filtering, and RETURNING JOINs in one round trip.
```
