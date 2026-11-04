# Pair Programming Interview Prep — Webhook Notifier

40 Q&A scenarios + 10 metrics discussion scenarios with concrete, code-grounded answers.

---

## 1. "Walk me through what happens when an Event arrives"

**Full trace:**

1. **HTTP POST** hits `cmd/notifier/http_handlers.go` → calls `ingestStore.Submit()`
2. **`ingest.Store.Submit()`** (`internal/ingest/ingest.go:65`):
   - Validates `CustomerID`, `Type`, valid JSON payload
   - Calls `registry.ListActiveForEvent()` — LRU cache with 30s TTL + singleflight coalescing
   - Opens a **single Postgres transaction**:
     - `INSERT INTO events` → gets `event_id`
     - `INSERT INTO deliveries` via `unnest()` — one row per matching Webhook
   - Commits transaction → **atomicity guarantee**: either Event + all Deliveries persist, or nothing
   - Calls `s.notifier.Notify(event.CustomerID)` → wakes local Scheduler + `pg_notify` broadcast to other replicas
3. **Scheduler** (`internal/scheduler/roundrobin/roundrobin.go`): `Notify()` adds Customer to doubly-linked list + map (O(1), idempotent)
4. **Worker** (`internal/worker/worker.go`): `Run()` loop calls `Scheduler.Pick()` → gets CustomerID + batchHint
5. **Claim**: Worker calls `outbox.ClaimNextN()` or `ClaimNextBatch()` — CTE with `FOR UPDATE SKIP LOCKED` + `INSERT INTO delivery_claims ON CONFLICT DO UPDATE WHERE claimed_at < lease`
6. **HTTP POST**: Worker signs payload (HMAC-SHA256), checks HostLimiter (`TryAcquire`), sends POST with per-request context timeout (default 30s)
7. **Classify**: `RetryPolicy.Next()` classifies response:
   - 2xx → `MarkSucceeded` → delivery done
   - 408/429/5xx/network error → `MarkRetry` with decorrelated jitter backoff
   - 4xx (non-408) → `MarkDead` → DLQ
   - Exhausted (8 attempts) or MaxAge exceeded → `MarkDead` → DLQ
8. **Breaker update**: If breaker enabled, `RecordSuccess`/`RecordFailure` updates in-memory state; transitions persisted async

**Key point to emphasize:** Event + Deliveries in one transaction is the notifier's atomicity boundary. The upstream producer still needs its own transactional outbox to make the business write + Event submission atomic.

---

## 2. "Why Postgres as a queue instead of Kafka/RabbitMQ?"

**Answer:**

Postgres serves as both durable outbox and coordination point. Three reasons for the PoC:

1. **Single coordination point**: Claims, delivery state, attempts, circuit breaker transitions — all in one transactional store. No distributed state to reconcile.
2. **Atomicity is free**: Event + Deliveries insert in one `BEGIN/COMMIT`. With a broker, you'd need a transactional outbox pattern *anyway* to avoid partial fanout.
3. **Simplicity**: One dependency (Postgres + PgBouncer). No broker ops, no partition management, no consumer group coordination.

**Trade-offs acknowledged:**
- Throughput ceiling from `deliveries` claim contention under high load
- `FOR UPDATE SKIP LOCKED` is row-level — works well up to a point, then hot rows become the bottleneck

**Scale-out path (when asked "what would you change?"):**
1. First: partition `deliveries` by `customer_id` — keeps Postgres, reduces contention
2. Second: PgBouncer tuning (already in stack with `MAX_PREPARED_STATEMENTS` for cached plan reuse)
3. Third: Redis Streams or Kafka per-Customer queues — bigger rewrite (outbox, discovery, ingest change; Scheduler interface stays per ADR-0001)

**ADR reference:** ADR-0002 documents this decision.

---

## 3. "Explain how `FOR UPDATE SKIP LOCKED` works in your claim query"

**Three Postgres locking modes compared:**

| Mode | Behavior | Problem |
|------|----------|---------|
| No lock | Multiple workers read same row → duplicate delivery | Broken |
| `FOR UPDATE` | Worker 1 locks row, Worker 2 **blocks waiting** | Serialized, kills throughput |
| `FOR UPDATE SKIP LOCKED` | Worker 1 locks row, Worker 2 **skips it instantly** | Concurrent, no blocking |

**How it works in the claim CTE** (`internal/outbox/claim.go:91-146`):

```sql
-- Step 1: Find candidate deliveries, skip already-locked ones
ready_candidate AS (
    SELECT d.id, d.attempt_count
    FROM deliveries d
    LEFT JOIN delivery_claims dc ON dc.delivery_id = d.id
    WHERE d.customer_id = ic.customer_id
      AND d.status IN ('pending', 'failed_retry')
      AND d.next_attempt_at <= now()
      AND dc.delivery_id IS NULL  -- not already claimed
    ORDER BY d.next_attempt_at, d.created_at, d.id
    LIMIT $4
    FOR UPDATE OF d SKIP LOCKED   -- <-- the magic
),

-- Step 2: Also find stale claims (crashed workers)
stale_candidate AS (
    SELECT d.id, d.attempt_count
    FROM delivery_claims dc
    JOIN deliveries d ON d.id = dc.delivery_id
    WHERE dc.claimed_at < now() - lease  -- claim expired
    FOR UPDATE OF d SKIP LOCKED
),

-- Step 3: Atomically claim via upsert
claimed AS (
    INSERT INTO delivery_claims (delivery_id, attempt_number, claimed_at)
    SELECT c.id, c.attempt_count + 1, now()
    FROM candidate c
    ON CONFLICT (delivery_id) DO UPDATE
        SET attempt_number = EXCLUDED.attempt_number,
            claimed_at = now()
        WHERE delivery_claims.claimed_at < now() - lease
    RETURNING delivery_id, attempt_number, claimed_at
)
```

**Why this matters:**
- **No blocking between workers** — if Worker A locked row 1, Worker B instantly skips to row 2
- **Stale claim recovery** — no separate sweeper process; the `stale_candidate` CTE + `ON CONFLICT DO UPDATE WHERE claimed_at < lease` atomically reclaims crashed workers' deliveries
- **Lock ordering** — always `deliveries` first, then `delivery_claims`. Inverted ordering causes deadlock (`SQLSTATE 40P01`)

**Follow-up "What if all rows are locked?":**
Empty claim → worker calls `HasClaimableWork()` → if false, `scheduler.Remove(customerID)` → DB-backed discovery re-adds when new work appears.

---

## 4. "Whale Customer imports 500K subscribers with 3 webhooks — how do you prevent starvation?"

**Key insight: iterate Customers, not Deliveries.**

That import creates 500K Events × 3 Webhooks = 1.5M Deliveries. A naive FIFO queue would process all 1.5M before touching any Small Customer.

**How fairness works:**

1. **Scheduler iterates Customers** (`internal/scheduler/roundrobin/roundrobin.go`):
   - Doubly-linked list + map, O(1) Notify/Remove
   - `Pick()` returns next Customer in rotation, advances cursor
   - Whale gets one pick per rotation cycle, same as every Small Customer

2. **Batch size is independent of backlog depth**:
   - Each pick claims up to `MinBatchHint`–`MaxBatchHint` deliveries (default 32–64)
   - Whale has 1.5M backlog but only gets 64 per pick — same as a Small Customer with 5 deliveries
   - `perCustomerClaimLimit()` in multi-pick mode further divides fair share

3. **Multi-pick amortization** (`MultiPickCount > 1`):
   - Worker collects N Customers from scheduler, issues one `ClaimNextBatch` call
   - SQL divides limit back into per-Customer share
   - Reduces DB round-trips for sparse workloads without giving Whale more capacity

4. **Empty-pick cleanup**:
   - If claim returns nothing for a Customer → `HasClaimableWork()` check
   - If truly empty → `scheduler.Remove()` → stops wasting picks
   - DB-backed discovery re-adds when `next_attempt_at` becomes claimable

**Fairness metric:**
- First-Attempt Queue Latency: `delivery_attempts.sent_at - deliveries.created_at` where `attempt_number = 1`
- Benchmark gate: Small Customer P95 stays within configured ratio of Whale P95
- Comparative, not absolute — real SLO needs production traffic data

**With multiple workers across replicas:**
- Per-instance round-robin is approximate
- DB-backed discovery (`RunClaimableCustomerDiscovery`) converges each process toward shared claimable set
- `FOR UPDATE SKIP LOCKED` prevents duplicate claims across instances

---

## 5. "Let's add per-Customer concurrency cap"

**This is explicitly listed as future work in DESIGN.md.** Great live-coding question.

**Design sketch:**

```go
// Where it lives: new file internal/worker/customer_limiter.go
type CustomerLimiter struct {
    sems sync.Map  // customerID -> *semaphore.Weighted
    cfg  CustomerLimiterConfig
}

type CustomerLimiterConfig struct {
    MaxPerCustomer int           // max concurrent in-flight per customer
    RequeueDelay   time.Duration // delay when cap hit
}

func (c *CustomerLimiter) TryAcquire(customerID string) (release func(), ok bool) {
    // Same pattern as HostLimiter.TryAcquire
}
```

**Where to integrate** (in `worker.go` delivery classification loop):
- After scheduler pick, before HTTP dispatch
- Non-blocking `TryAcquire` — if no slot, `Requeue` with short delay (same as HostLimiter pattern)
- Keep fairness intact: cap-miss returns worker to serve another Customer

**Key design decisions:**

| Decision | Choice | Why |
|----------|--------|-----|
| Scope | Fleet-wide (not just process-local) | Process-local can't enforce deployment-wide bound |
| Mechanism | Lease-based distributed semaphore | Same pattern as HostLimiter production path |
| Key | `customerID` | Not `customerID+host` — orthogonal to HostLimiter |
| Relationship to scheduling | Complementary | Round-robin controls admission order; concurrency cap bounds simultaneous resource use |
| Requeue behavior | Same as HostLimiter | No Attempt recorded, no retry budget consumed |

**Interaction with existing components:**
- **HostLimiter**: orthogonal — HostLimiter protects partners, CustomerLimiter protects fairness
- **Scheduler**: unchanged — still controls pick order
- **Breaker**: unaffected — breaker is per-Webhook, not per-Customer

---

## 6. "Webhook endpoint returns 500s for 2 hours — what happens?"

**Timeline:**

```
t=0:00  Delivery 1 attempted → 500 → RetryPolicy classifies as retryable_5xx
        → MarkRetry with decorrelated jitter (baseDelay=1s, cap=5min)
        → Breaker: failureCount++ (now 1)

t=0:01  Delivery 1 retry #2 → 500 → MarkRetry, failureCount=2
...
t=0:15  After 20 consecutive failures → BREAKER OPENS
        → webhooks.breaker_state = 'open'
        → webhooks.breaker_next_probe_at = now() + 5min (initial cooldown)
        → All deliveries for this Webhook excluded from claim AND discovery SQL
        → Queued work stays in outbox, just doesn't consume worker capacity

t=5:15  Cooldown expires → claim CTE's probe subquery runs:
        → Atomically picks ONE delivery as breaker_probe_delivery_id
        → Transitions Webhook to 'half_open'
        → Only that probe delivery is claimable

t=5:15  Probe delivery POSTed → still 500
        → BREAKER REOPENS with backed-off cooldown (up to 15min max)
        → breaker_next_probe_at = now() + longer_cooldown

t=20:00 (example) Probe succeeds → 200
        → BREAKER CLOSES
        → RecordSuccess: failureCount=0, cooldown reset to initial
        → All queued deliveries become claimable again
        → Workers drain backlog normally
```

**Meanwhile, individual deliveries also have their own lifecycle:**
- Each delivery retries up to 8 times (DefaultMaxAttempts) with decorrelated jitter
- If exhausted → `MarkDead` → DLQ with reason `exhausted`
- If MaxAge exceeded → `MarkDead` → DLQ with reason `age_exceeded`

**Key distinction:** Breaker stops claiming *new* deliveries for that Webhook. Already-exhausted deliveries land in DLQ independently.

**Code references:**
- Breaker decision: `internal/breaker/memory.go` — `RecordFailure()` increments count, opens at threshold
- Breaker in claim SQL: `sqlBreakerEligible` predicate excludes open Webhooks
- RetryPolicy: `internal/retry/retry.go:97` — `next()` method with classify + decorrelated jitter

---

## 7. "How would you debug Small Customer P95 latency spike in production?"

**Diagnostic playbook (metrics all exist in `internal/observe/`):**

### Step 1: Is it a scheduling problem or a delivery problem?

| Metric | What it tells you |
|--------|-------------------|
| `webhook_queue_wait_seconds` (by `customer_class`) | Time from claimable to first attempt — if high for `small`, scheduling is the bottleneck |
| `webhook_delivery_latency_seconds` (by `customer_class`) | End-to-end including partner response — if high, partner is slow |
| `webhook_post_duration_seconds` | Raw HTTP POST time — isolates partner latency |

### Step 2: Is a Whale monopolizing?

| Metric | What to look for |
|--------|-----------------|
| `webhook_claim_batch_size` | If batches are consistently large, a Whale may be getting oversized claims |
| `webhook_claim_empty_total` | High empty claims = discovery noise (ADR-0013 trade-off) |
| `webhook_deliveries_in_flight` | If saturated, workers are all busy |

### Step 3: Is a partner slowing everything down?

| Metric | What to look for |
|--------|-----------------|
| `webhook_host_limiter_requeue_total` | High requeues = partner saturated, HostLimiter firing |
| `webhook_breaker_state` | Any Webhooks in `open` state? |
| Partner response times via `webhook_post_duration_seconds` | Slow partner occupying worker threads |

### Step 4: Is it Postgres?

| Metric | What to look for |
|--------|-----------------|
| Pool gauges (`observe.RegisterPoolGauges`) | Connection exhaustion? |
| Claim query duration | Contention on `deliveries` table? |
| `webhook_pending_deliveries` (backlog sampler) | Backlog growing unboundedly? |

### Step 5: Config tuning

- `MinBatchHint` too high? Lower it (1–8) for fairness-sensitive workloads
- `MultiPickCount` too high? Reduces per-Customer interleaving
- `EmptyClaimBackoff` too low? Causes hot polling

---

## 8. "What happens if a notifier process crashes mid-delivery?"

**Scenario:** Worker claimed delivery, sent HTTP POST, process killed before `MarkSucceeded`.

**Recovery path — no sweeper needed:**

1. The delivery has a row in `delivery_claims` with `claimed_at = T`
2. Claim lease = 5 minutes (`DefaultClaimLease`)
3. After 5 minutes, any worker's next claim query picks it up via `stale_candidate` CTE:

```sql
stale_candidate AS (
    SELECT d.id, d.attempt_count
    FROM delivery_claims dc
    JOIN deliveries d ON d.id = dc.delivery_id
    WHERE dc.claimed_at < now() - '300000 milliseconds'::interval
    FOR UPDATE OF d SKIP LOCKED
)
```

4. `INSERT ... ON CONFLICT (delivery_id) DO UPDATE WHERE claimed_at < now() - lease` atomically transfers ownership
5. New worker re-attempts the delivery

**What about the partner?**
- Partner may have received the POST and processed it successfully
- Partner gets a **duplicate** delivery on retry
- **Idempotency headers** handle this:
  - `X-Delivery-Id` — stable across retries of same delivery
  - `X-Event-Id` — stable across all deliveries from same event
- Partner deduplicates on these headers

**What about the attempt count?**
- The crashed attempt's `MarkSucceeded`/`MarkRetry` never committed
- No `delivery_attempts` row was written
- The recovery attempt increments `attempt_number` — so the count may skip one
- This is acceptable: retry budget is conservative (8 attempts), one lost attempt doesn't matter

**Optimization:** `hasStaleClaims()` does a cheap `EXISTS` scan first — if no stale claims exist (normal case), the expensive `stale_candidate` LATERAL is skipped entirely.

---

## 9. "How do you know your fairness claim actually holds? Show me the benchmarks."

**Benchmark harness:** `cmd/bench/main.go` + `bench/scenarios/fairness.go`

**How to run:**
```bash
just up && just migrate
go run ./cmd/bench -scenario fairness
```

**What it measures:**

1. Seeds a **Whale Customer** (high volume) and multiple **Small Customers** (low volume)
2. Runs the notifier with both producing events simultaneously
3. Measures **First-Attempt Queue Latency**: `delivery_attempts.sent_at - deliveries.created_at` where `attempt_number = 1`
4. Computes P95 for Small vs Whale separately
5. **Pass gate:** Small P95 within configured ratio of Whale P95

**Why this specific metric:**
- First-attempt only → isolates scheduling/queue behavior
- Excludes partner response time (that's the partner's problem)
- Excludes retry/backoff delays (those are intentional)
- `created_at` → `sent_at` captures exactly: "how long did the delivery wait in queue before a worker picked it up and POSTed?"

**Weaknesses (expect this follow-up):**

| Weakness | Explanation |
|----------|-------------|
| Comparative, not absolute | "Small P95 < 2x Whale P95" not "Small P95 < 500ms" — absolute SLO needs real capacity assumptions |
| Single-machine | Benchmark runs one process — doesn't capture fleet-wide fairness convergence |
| First-attempt only | Doesn't measure retry fairness — though retry-due deliveries share the same scheduling path once claimable |
| Synthetic load | Bench receiver responds instantly — real partners have variable latency |

**Benchmark categories** (from `bench/README.md`):
- **Reliability**: guaranteed delivery, no lost events
- **Fairness**: Small vs Whale latency comparison
- **Scalability**: horizontal scaling behavior
- **Partner Protection**: host limiter, circuit breaker behavior

---

## 10. "Scale to 10x throughput — what breaks first?"

**Bottleneck sequence, in order:**

### Break 1: Postgres claim contention (first to hit)

The `deliveries` table is the single coordination point. `FOR UPDATE SKIP LOCKED` works well, but at 10x:
- More workers contending on same rows
- LATERAL subqueries scan more candidates
- Index scan on `(customer_id, status, next_attempt_at)` gets hotter

**Fix:** Partition `deliveries` by `customer_id`. Each partition's lock set is independent. Claim queries only touch relevant partition. Schema change, no application logic change.

### Break 2: Connection pool saturation

Current sizing (`runtime.go:66-81`):
```go
maxConns := runtimeCfg.WorkerCount * 3
// capped at 50
```

At 10x workers, 50 connections won't suffice. PgBouncer transaction pooling helps (already in stack), but need to tune `default_pool_size` and potentially add read replicas for discovery queries.

**Fix:** Increase PgBouncer pool, separate read/write connection pools, read replicas for discovery.

### Break 3: HostLimiter accuracy

Process-local `HostLimiter` (default 20 per host per process). With 10x processes, the *fleet-wide* concurrent requests to one partner = 20 × 10 = 200. Partner probably can't handle that.

**Fix:** Distributed semaphore (Redis-based or similar). Design is already documented in DESIGN.md:
- Lease-based slots with renewal
- Fail closed on coordination loss
- Key by `hostname:port`
- Cancel in-flight request if lease renewal fails

### Break 4: Discovery query load

`RunClaimableCustomerDiscovery` periodically scans for Customers with claimable work. At 10x scale, more Customers, more rows to scan.

**Fix:** Already mitigated by claim-agnostic discovery (ADR-0013) — avoids joining `delivery_claims`. But may need to reduce discovery frequency or shard discovery across replicas.

### Break 5 (nuclear option): Postgres as queue entirely

If partitioning + pool tuning + distributed coordination still isn't enough:

**Fix:** Migrate to Redis Streams or Kafka per-Customer queues.
- `Scheduler` interface stays unchanged (ADR-0001)
- Outbox, claim, discovery, ingest all rewrite
- Biggest change, but the interface boundaries were designed for this

**What stays stable:**
- `scheduler.Scheduler` interface (Pick/Notify/Remove)
- `retry.RetryPolicy` (pure logic)
- `breaker.Breaker` interface
- `worker.HostLimiter` pattern
- HTTP transport and signing

---

## Bonus: Quick-fire Questions

**"Why per-Webhook breaker, not per-host?"**
One broken Webhook config shouldn't suppress other Customers' healthy Webhooks on the same host. HostLimiter handles host-level protection.

**"Why decorrelated jitter instead of full jitter?"**
Decorrelated jitter: `delay = rand(base, prev_delay * 3)`. Spreads retries more evenly than exponential backoff. Deterministic via FNV hash of `(attempt, prevDelay, reason, key)` — same inputs produce same delay, testable.

**"Why `ON CONFLICT DO UPDATE` instead of `SELECT ... FOR UPDATE` then `INSERT`?"**
Single round-trip. The upsert handles both fresh claims (INSERT) and stale reclaims (DO UPDATE with WHERE guard) atomically. No race window between SELECT and INSERT.

**"Why not use `database/sql` transactions for claiming?"**
The entire claim is one CTE chain — one statement, one round-trip, one lock acquisition scope. A multi-statement transaction would need multiple round-trips and hold locks longer.

**"What's the `pg_notify` wakeup for?"**
Local ingest notifies the local scheduler synchronously. For other replicas, `pg_notify` sends a hint over a dedicated connection. Replicas wake their scheduler immediately instead of waiting for the next discovery interval. Hints are coalesced per Customer for 100ms to avoid notification storms.

---

## 11. "Explain the difference between a Requeue and a Retry"

**This is a vocabulary trap — getting it wrong signals shallow understanding.**

| | Requeue | Retry |
|---|---------|-------|
| **Did the partner see a POST?** | No | Yes |
| **Attempt row recorded?** | No | Yes |
| **Attempt counter incremented?** | No | Yes |
| **Retry budget consumed?** | No | Yes |
| **Feeds circuit breaker?** | No | Yes |
| **When it happens** | HostLimiter cap hit, coordination loss | Partner returned 5xx/408/429, network error, timeout |

**Requeue** (`internal/outbox/status_single.go:146`):
```go
func (s *Store) Requeue(ctx context.Context, deliveryID string, delay time.Duration) error
```
Sets `status = 'failed_retry'`, bumps `next_attempt_at` by a short delay (default 100ms from HostLimiter). No `delivery_attempts` row. The partner never saw anything — it's purely a notifier-side scheduling decision.

**Retry** (via `MarkRetry`):
Records an `delivery_attempts` row with status code, error class, duration. Increments `attempt_count`. Uses decorrelated jitter for delay (1s base, 5min cap). The partner saw and rejected the POST.

**Why it matters:** If you treated a HostLimiter cap-miss as a retry, a Whale Customer with a slow partner would burn through its 8-attempt budget just from local concurrency saturation — not from actual partner failures. Requeue preserves the retry budget for real failures.

---

## 12. "How does the SubscriptionRegistry cache work, and what's the thundering herd protection?"

**Three layers** (`internal/registry/registry.go`):

1. **LRU + TTL cache** (`hashicorp/golang-lru/v2/expirable`):
   - Capacity: 10,000 entries (configurable)
   - TTL: 30 seconds per entry
   - Key: `customerID + "\x00" + eventType` (null byte prevents collisions like `"abc" + "def"` vs `"ab" + "cdef"`)

2. **Singleflight** (`golang.org/x/sync/singleflight`):
   - If 100 goroutines request the same `(customerID, eventType)` simultaneously, only ONE hits Postgres
   - Others block on a channel and share the result
   - Uses `DoChan` (not `Do`) so each caller can respect its own `ctx.Done()` independently

3. **Isolated context for DB query**:
   ```go
   ch := r.sf.DoChan(key, func() (interface{}, error) {
       qCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
       defer cancel()
       hooks, err := r.queryDB(qCtx, customerID, eventType)
       // ...
   })
   ```
   The DB query uses `context.Background()` + 5s timeout, NOT the caller's context. If caller A cancels, the in-flight query still completes and caches for callers B, C, D.

**Why this design:** During a Whale Customer import (500K events), every event hits `ListActiveForEvent` for the same customer. Without singleflight, you'd send 500K identical queries to Postgres. With it: one query, one cache write, 499,999 cache hits.

---

## 13. "How does your system handle event ordering? Can events arrive out of order at the partner?"

**Short answer: yes, and that's deliberate.**

**Why strict FIFO is a non-goal (documented in DESIGN.md):**

1. Multiple workers process the same Customer concurrently — interleaving is inevitable
2. Retries add reordering: Event A fails, Event B succeeds, Event A retries and succeeds later
3. Multi-pick batching (`MultiPickCount > 1`) trades some ordering for throughput
4. Strict FIFO would require single-threaded per-Customer processing — kills throughput and fairness

**How partners handle it:**

Every Attempt carries:
- `X-Event-Id` — stable across all Deliveries from one Event
- `X-Delivery-Id` — stable across retries of one Delivery
- `X-Signature-Timestamp` — wall clock time of signing

Partners use `X-Event-Id` + `created_at` from the payload to reconstruct order if needed. Standard pattern in webhook ecosystems (Stripe, GitHub, Shopify all do this).

**What IS guaranteed:**
- Per-Customer scheduling order within one worker is arrival order (FIFO within a claim batch)
- No Delivery is lost — every accepted Event reaches a terminal state
- Idempotency headers let partners deduplicate

---

## 14. "Walk me through the HMAC signing and how secret rotation works"

**Signing** (`internal/signer/signer.go` — 22 lines total, pure function):

```go
func Sign(secret, body []byte, ts time.Time) string {
    payload := make([]byte, 0, 12+len(body))
    payload = strconv.AppendInt(payload, ts.Unix(), 10)
    payload = append(payload, '.')
    payload = append(payload, body...)
    mac := hmac.New(sha256.New, secret)
    mac.Write(payload)
    return hex.EncodeToString(mac.Sum(nil))
}
```

Format: `HMAC-SHA256(secret, "<unix_timestamp>.<body>")`

**Replay protection:** Partner checks `abs(now() - X-Signature-Timestamp) > 5min` → reject. Timestamp is bound into the HMAC, so an attacker can't change it without invalidating the signature.

**Secret rotation** (schema support, not yet implemented in management API):

```sql
webhooks
  secret_encrypted      -- current secret
  secret_previous       -- previous secret (nullable)
  secret_rotated_at     -- when rotation started (nullable)
```

**Rotation flow:**
1. Admin sets new secret → old secret moves to `secret_previous`, `secret_rotated_at = now()`
2. Notifier always signs with current `secret_encrypted`
3. Partner verifies against BOTH secrets during grace window
4. After grace window, cleanup job clears `secret_previous`

**PoC gap:** `secret_encrypted` is stored as plaintext in the PoC. Production needs encryption at rest with decryption only at the delivery boundary.

---

## 15. "How does graceful shutdown work? What happens to in-flight deliveries?"

**Shutdown sequence** (`cmd/notifier/main.go:133-166`):

```go
// 1. Wait for SIGINT or SIGTERM
signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)
<-shutdown

// 2. Cancel the root context → all workers/discovery/breaker stop
cancel()

// 3. Shutdown with 10s deadline
shutdownCtx, _ := context.WithTimeout(context.Background(), 10*time.Second)
infra.Shutdown(shutdownCtx)      // stop metrics server
business.Shutdown(shutdownCtx)   // stop accepting new events
deliveryRuntime.Stop(shutdownCtx) // wait for workers to finish
dbConn.Close()                   // close DB pool
```

**What happens to in-flight deliveries:**

1. `cancel()` propagates to all worker `Run()` loops via context
2. Workers finish current HTTP POST (bounded by `RequestTimeout`, default 30s)
3. Workers mark outcomes (succeed/retry/dead) before exiting
4. `runtimeGroup.Stop()` waits for all goroutines via `sync.WaitGroup`
5. If 10s deadline hits before workers finish → `shutdownCtx` expires, process exits
6. Any delivery that didn't get marked → claim expires after 5min → recovered by another replica via stale-candidate CTE

**Key design:** No durable local state. If the process is killed hard (`kill -9`), nothing is lost — just delayed by the claim lease (5min). Stale claims are recovered automatically.

---

## 16. "What is the DB-backed claimable Customer discovery and why is it claim-agnostic?"

**Problem:** Each notifier process has a local Scheduler with a local view. How does it learn about Customers whose deliveries were created by *other* replicas?

**Solution:** `RunClaimableCustomerDiscovery` (`internal/worker/discovery.go:37`):
- Periodically scans Postgres for Customers with `status IN ('pending', 'failed_retry')` and `next_attempt_at <= now()`
- Cursor-based pagination (keyed by `customer_id`, limit 1000)
- Calls `sched.Notify(customerID)` for each discovered Customer
- Backs off when empty (5s → doubles up to 30s)

**Why claim-agnostic (ADR-0013):**

The discovery query does NOT join `delivery_claims`. Two options were evaluated:

| Approach | Pro | Con |
|----------|-----|-----|
| Claim-aware (join `delivery_claims`) | Exact: only surfaces truly unclaimed Customers | Query latency grows with backlog depth — `delivery_claims` table gets large under load |
| Claim-agnostic (no join) | Bounded latency regardless of backlog | May re-notify Customers whose rows are all claimed |

**Trade-off:** Discovery may surface Customers that have no unclaimed work. The worker handles this gracefully:
1. Worker picks Customer from scheduler
2. `ClaimNextN` returns empty (all rows locked by other workers)
3. Worker calls `HasClaimableWork()` → returns false
4. `scheduler.Remove(customerID)` → Customer leaves rotation
5. Next discovery cycle re-adds if new work appears

Monitor: `webhook_claim_empty_total` vs `webhook_claim_batch_size` — if empty claims spike, discovery is producing noise.

---

## 17. "How does delivery_history archival work? Why not just delete old deliveries?"

**Why archive, not delete:**
- Terminal deliveries (`succeeded`, `dead`) are no longer schedulable work
- But they contain audit data: attempt history, failure reasons, timing
- Deleting loses that. Archiving preserves it in a partitioned table optimized for lookups.

**Archive flow** (`internal/outbox/archive.go`):

1. `ArchiveTerminalDeliveries(ctx, retention, limit)` runs periodically
2. Scans `deliveries` where `status IN ('succeeded', 'dead')` and `updated_at < now() - retention` (default 24h)
3. Copies into `delivery_history` (time-partitioned by `final_at`)
4. Deletes the `deliveries` row + associated `delivery_claims` row

**Why this matters for performance:**
- `deliveries` table is the hot claim path — every worker scans it every cycle
- Terminal rows are dead weight in that scan
- Moving them out keeps the index on `(customer_id, status, next_attempt_at)` lean
- `delivery_history` is partitioned → old partitions can be dropped cheaply

**Lock ordering concern:**
Archive locks `deliveries` first, never `delivery_claims` first → same order as claim path → no deadlock risk (documented in code comment).

**Two enrichment modes:**
- `ArchiveEnrichmentFull` — archives attempt-summary metadata (default)
- `ArchiveEnrichmentDeferred` — archives core fields only, defers heavy enrichment (for high-throughput periods)

---

## 18. "How does the weighted scheduler (SWRR) differ from plain round-robin, and when would you use it?"

**Plain round-robin** (default, `internal/scheduler/roundrobin/roundrobin.go`):
- Every Customer gets exactly one pick per cycle
- O(1) Pick, Notify, Remove via `container/list.List` + `map`
- No concept of weight — Customer with class=1 and class=10 get same share

**Smooth Weighted Round Robin** (`internal/scheduler/swrr/`):
- Customers get picks proportional to their `class` weight
- Weight source: `customers.class` column via `dbweights.DBWeightSource` (cached 5min)
- `class=0` → paused (never picked)
- Uses same list+map membership structure, adds O(n) weight accumulation during Pick (zero allocation)

**Example:** Three Customers with weights 5, 1, 1:
- Plain RR: A, B, C, A, B, C, A, B, C...
- SWRR: A, A, B, A, A, C, A — interleaved proportionally, no bursts

**When to use it:**
- Tiered pricing: paying Customers get more scheduling share
- SLA differentiation: enterprise tier processes faster
- Temporarily boosting a Customer recovering from a backlog

**Why it's not the default:**
- PoC requirement is equal fairness → plain RR satisfies that
- SWRR adds O(n) per-pick cost (n = active Customers)
- Needs profiling for expected active Customer counts before production default
- Weight policy (plan-tier → class mapping) is a product decision, not a tech one

**Activation:** `EnableWeightedScheduler` in runtime config → `runtime.go:115` switches `roundrobin.New()` to `swrr.New(dbweights.New(db))`.

---

## 19. "How do you test claim atomicity? Show me your integration test strategy."

**Integration tests use testcontainers** — real Postgres, not mocks:

```go
// internal/outbox/outbox_integration_test.go
// Example: TestClaimNextBatchAndRequeueConcurrentNoDeadlock
```

**Key test scenarios:**

| Test | What it proves |
|------|---------------|
| **Concurrent claim, no duplicates** | Spawn K goroutines, each calling `ClaimNextN`. Assert total claimed = total deliveries, no ID appears twice |
| **Stale claim recovery** | Claim a delivery, wait past lease, claim again from different "worker" → succeeds |
| **Requeue returns to failed_retry** | Requeue a claimed delivery → status back to `failed_retry`, no attempt row written |
| **Concurrent claim + requeue no deadlock** | Interleave claim and requeue operations → no `SQLSTATE 40P01` |
| **End-to-end happy path** | Ingest event → claim → POST → mark succeeded → verify row state |

**Why real Postgres, not mocks (per CLAUDE.md):**
- `FOR UPDATE SKIP LOCKED` behavior can't be mocked meaningfully
- Lock ordering deadlocks only manifest under real concurrency
- CTE atomicity guarantees are Postgres-specific

**Unit tests (pure logic, no DB):**
- `Scheduler`: table-driven — empty schedule, single/multi Customer RR invariant, weighted share property test
- `RetryPolicy`: truth table from ADR-0003 — every status code × attempt count combination
- `Signer`: vector tests with known fixtures

**Benchmark tests:**
- `bench/scenarios/` — reliability, fairness, scalability, partner protection
- Each scenario has a pass/fail gate (e.g., fairness ratio, delivery success rate)
- Results tracked in `bench/RESULTS.md`

---

## 20. "What's the DLQ (Dead-Letter Queue) for, and how would you build a replay mechanism?"

**What goes to DLQ** (`dead_letters` table):

| Reason | When |
|--------|------|
| `exhausted` | 8 attempts used up (DefaultMaxAttempts) |
| `terminal_4xx` | Partner returned 400-499 (except 408/429) — request is malformed, retrying won't help |
| `age_exceeded` | Delivery exceeded MaxAge — too old to be useful |

**What's stored:**
```sql
dead_letters
  delivery_id    -- links back to delivery
  last_error     -- final error message
  attempts       -- total attempts made
  reason         -- exhausted / terminal_4xx / age_exceeded
  final_at       -- when it died
```

**Current state:** DLQ is a queryable table with `idx_dead_letters_final_at_desc` for ops inspection. No replay mechanism — documented as out-of-scope for PoC.

**How you'd build replay (live-coding discussion):**

```
Option 1: Manual replay via API
  POST /admin/replay { delivery_ids: [...] }
  → Reset status to 'pending', clear attempt_count, set next_attempt_at = now()
  → Remove dead_letters row
  → Notify scheduler

Option 2: Conditional auto-replay
  → Operator marks a Webhook as "recovered"
  → System replays all DLQ entries for that Webhook
  → Rate-limited to avoid thundering herd

Option 3: Selective replay with filter
  POST /admin/replay { webhook_id: "...", reason: "retryable_5xx", after: "2026-01-01" }
  → Only replay 5xx failures for a specific Webhook after a date
```

**Key considerations:**
- Replay must re-sign with current secret (not cached signature)
- Must respect HostLimiter and circuit breaker — don't slam a just-recovered partner
- Need idempotency: partner may have processed some of these via late retries
- Replay deliveries should get fresh `X-Signature-Timestamp` but keep original `X-Delivery-Id` and `X-Event-Id`

---

## 21. "Why one shared HTTP client instead of one per worker or one per delivery?"

**Design** (`internal/worker/client.go`):

```go
func DefaultHTTPConfig() HTTPConfig {
    return HTTPConfig{
        MaxIdleConns:          10000,
        MaxIdleConnsPerHost:   500,
        MaxConnsPerHost:       500,  // backstop; HostLimiter is the real cap
        IdleConnTimeout:       90 * time.Second,
        TLSHandshakeTimeout:   10 * time.Second,
        ResponseHeaderTimeout: 10 * time.Second,
    }
}
```

One `*http.Client` created in `runtime.go:169`, shared across all workers in the process.

**Why not per-delivery?**
- Each `http.Client` has its own connection pool
- Per-delivery = no connection reuse = TLS handshake every request
- At 300 deliveries/s → 300 handshakes/s — massive overhead

**Why not per-worker?**
- Workers in same process often hit same partner hosts
- Per-worker pools fragment idle connections across N pools
- Shared pool → one connection to `partner-a.com` serves any worker

**Why no `http.Client.Timeout`?**
Per ADR-0007: per-Attempt deadlines use **request contexts**, not `http.Client.Timeout`. This lets the response body drain before cancellation so Go reuses the TCP connection:

```go
// client.go:53 — drain body BEFORE closing
_, _ = io.Copy(io.Discard, resp.Body)
resp.Body.Close()
```

With `http.Client.Timeout`, cancellation kills mid-body-read → connection can't be reused → pool thrashes.

**The `cancelOnCloseReadCloser` trick** (`client.go:137-146`):
Context cancel deferred until `resp.Body.Close()`. Prevents per-request timeout context from leaking if caller forgets to close.

---

## 22. "Explain the pg_notify wakeup mechanism — how do replicas learn about new work?"

**Problem:** Replica A ingests an Event. Replica B has idle workers. Without notification, B waits for next discovery interval (default 1s).

**Two-layer solution** (`cmd/notifier/pg_notify_listener.go`):

**Layer 1 — Broadcast (sender side):**
```go
// schedulerNotifier.Notify() — called after ingest commit
n.scheduler.Notify(customerID)    // local: O(1) linked list add
n.broadcaster.Notify(customerID)  // remote: queue for pg_notify
```

The `pgNotifyBroadcaster`:
1. Check per-Customer cooldown (100ms) via `sync.Map` — lock-free hot path
2. If within cooldown → coalesced, skip (metric: `coalesced`)
3. If fresh → enqueue to bounded channel (capacity scales with workers)
4. Background goroutine drains queue, calls `SELECT pg_notify($1, $2)` on **dedicated** 1-connection pool (separate from main pool)

**Layer 2 — Listener (receiver side):**
```go
// runCustomerWakeListener — one goroutine per replica
conn.Exec(ctx, "LISTEN " + channel)  // pgx.Identifier.Sanitize() for safety
for {
    notification := conn.WaitForNotification(ctx)
    sched.Notify(notification.Payload)  // customerID
}
```

Uses raw `pgx.Connect` (not pooled) → persistent `LISTEN` → blocks until Postgres pushes notification.

**Failure handling:**
- Queue full → notification dropped, logged, metric `dropped` → discovery recovers in 1-30s
- Listener disconnects → reconnect with 1s delay, re-`LISTEN`
- pg_notify is best-effort by design — periodic discovery is the safety net

**Why cooldown?**
Whale import of 500K events → 500K `pg_notify` calls without cooldown. 100ms cooldown coalesces to ~1 notification per Customer per 100ms. Stale map entries pruned when count exceeds 1024.

---

## 23. "What is lock ordering and why does it matter?"

**The invariant:** Every transaction that touches both tables acquires `deliveries` first, then `delivery_claims`. Always.

**Why:**
```
Thread A (claim):   LOCK deliveries row 1  →  LOCK delivery_claims row 1
Thread B (requeue): LOCK delivery_claims row 1  →  LOCK deliveries row 1
                    ↑ DEADLOCK (SQLSTATE 40P01)
```

Two transactions acquiring locks in opposite order → Postgres detects cycle → aborts one.

**Where enforced:**

| Operation | Lock order | How |
|-----------|------------|-----|
| Claim CTE | `FOR UPDATE OF d SKIP LOCKED` → `INSERT INTO delivery_claims` | CTE structure guarantees order |
| MarkSucceeded/MarkRetry/MarkDead | `deliveries` via `withInFlightDelivery` → `delivery_claims` | CTE acquires `d` first |
| Requeue | Same `withInFlightDelivery` pattern | Fixed in ADR-0012 |
| Archive | `FOR UPDATE` on `deliveries` → `DELETE delivery_claims` | Same order |

**History:** ADR-0012 documents that original `Requeue` used `FOR UPDATE OF dc, d` (delivery_claims-first join). Deadlocked under concurrent claim+requeue. Fixed by rewriting to deliveries-first order. Regression test: `TestClaimNextBatchAndRequeueConcurrentNoDeadlock`.

---

## 24. "How does PgBouncer interact with prepared statements on the hot path?"

**Problem:**
- Hot CTEs (claim, batch mark) run thousands of times/second
- Without prepared statements: Postgres parses + plans each execution → wasted CPU
- PgBouncer transaction pooling traditionally can't handle prepared statements (session-scoped)

**Solution (ADR-0012):**
PgBouncer 1.21+ supports `max_prepared_statements` in transaction mode. Local stack:
```
edoburu/pgbouncer:v1.25.1-p0
POOL_MODE=transaction
DEFAULT_POOL_SIZE=80
MAX_PREPARED_STATEMENTS=200
```

pgx uses cached-statement mode → first execution: Parse + Bind + Execute → subsequent: Bind + Execute only (skip parse/plan).

**Connection sizing:**
```go
// main.go:66-81
maxConns := runtimeCfg.WorkerCount * 3  // capped at 50
```

Notifier ≤50 client connections → PgBouncer ≤80 server connections → headroom for admin/retention.

**Multi-customer claim shaping** (also part of ADR-0012):
- Deduplicate input Customer IDs (`SELECT DISTINCT`)
- `CROSS JOIN LATERAL` per Customer → each uses per-Customer index directly
- Per-Customer candidate cap = `limit / numCustomers` → prevents Whale monopolizing batch lock set

---

## 25. "Walk me through the worker run loop and the delivery concurrency pool"

**The run loop** (`internal/worker/run_loop.go:29`):

```
for {
    1. Pick Customer(s) from Scheduler
    2. Claim deliveries from Postgres (claimSingle or claimMulti)
    3. FetchPayloads (separate query — keeps claim CTE lean)
    4. processDeliveries → fan out to goroutine pool
    5. Collect results → batch mark in ONE DB round-trip
}
```

**The goroutine pool** (`run_loop.go:38-57`):

```go
w.deliveryPool = make(chan deliveryWork, w.Config.DeliveryConcurrency)
for range w.Config.DeliveryConcurrency {  // default 16
    go func() {
        for work := <-w.deliveryPool {
            *work.result = classified{
                delivery: work.delivery,
                outcome:  w.classifyDelivery(work.ctx, work.delivery),
            }
            work.done()
        }
    }()
}
```

**Key design:**
- Pool is **persistent** — created once in `Run()`, reused across all batches
- No per-batch goroutine churn (avoids GC pressure at high throughput)
- `deliveryWork` struct carries context, delivery, result pointer, and `done` callback (WaitGroup)
- `processDeliveries` fans out ALL deliveries in batch, then `batchWg.Wait()` for all to complete

**Batch mark** (`processDeliveries` at line 285-361):
After all HTTP POSTs complete, results are sorted into three slices:
- `succeeded []BatchMarkItem`
- `retries []BatchMarkItem`
- `requeued []string`
- `dead []BatchMarkItem`

Then **one** `MarkOutcomesBatch` call → one DB transaction for all outcomes. Compared to marking each delivery individually, this reduces DB round-trips from N to 1 per batch.

**Why classify and mark are separated:**
- `classifyDelivery` = HTTP POST + response classification (concurrent, I/O bound)
- `MarkOutcomesBatch` = DB write (sequential, single transaction)
- Separating them means HTTP latency doesn't hold a DB transaction open

---

## 26. "How does claimMulti work and why does it exist?"

**Problem:** With 1000 Customers each having 1-2 pending deliveries (sparse workload), `claimSingle` does 1 DB round-trip per Customer → 1000 queries to drain the backlog.

**Solution:** `claimMulti` (`run_loop.go:178`):

```go
func (w *Worker) claimMulti(ctx context.Context, store DeliveryStore) ([]outbox.Delivery, error) {
    // 1. Collect N Customers from scheduler
    for range w.Config.MultiPickCount {
        id, batchHint, ok, _ := w.Scheduler.Pick(ctx)
        customerIDs = append(customerIDs, id)
        totalLimit += w.Config.ClaimLimit(batchHint)
    }

    // 2. Cap customers so each gets ≥2 LATERAL slots
    maxCustomers := totalLimit / 2
    if len(customerIDs) > maxCustomers {
        // Re-notify excess customers → they stay in rotation
        customerIDs = customerIDs[:maxCustomers]
    }

    // 3. ONE ClaimNextBatch call for all customers
    deliveries, err := store.ClaimNextBatch(ctx, customerIDs, totalLimit)

    // 4. Check empty-claim customers
    // hasWork → Notify (keep in rotation)
    // no work → Remove (leave rotation, discovery re-adds later)
}
```

**DB amortization:** Instead of N claim queries for N sparse Customers, one `ClaimNextBatch` with `CROSS JOIN LATERAL` over all Customer IDs. SQL divides `limit` into fair per-Customer share.

**The ≥2 LATERAL slots guard:**
If `totalLimit` is small relative to picked Customers, `perCustomerClaimLimit` would floor to 1 per Customer — each LATERAL locks exactly 1 row, wasteful. Solution: cap Customer count so each gets ≥2 slots, re-notify excess.

**Empty-claim handling:**
`HasClaimableWorkBatch` checks all empty-claim Customers in one query. Those with no work get `Remove()`'d. Prevents dead Customers from consuming scheduler picks.

---

## 27. "What happens if Postgres goes completely down?"

**Immediate effects:**

| Component | Behavior |
|-----------|----------|
| Ingest HTTP | Returns 500 — `BeginTx` fails. Client retries via producer's outbox |
| Workers | `ClaimNextN` fails → logs error → backs off 250ms → retry |
| Discovery | Fails → exponential backoff (1s → 2s → ... → 30s cap) |
| Breaker persister | Async writes fail → logged, metric. In-memory state preserved |
| pg_notify | Broadcast fails → dropped. Listener disconnects → reconnect loop 1s |

**What's NOT lost:**
- No durable local state → nothing to corrupt
- In-memory scheduler rotation preserved (stale but intact)
- In-memory breaker decisions preserved
- HostLimiter counters preserved

**Recovery when Postgres comes back:**
1. Discovery succeeds → populates scheduler
2. Workers claim and process normally
3. Stale claims (from before outage) → recovered by `stale_candidate` CTE after 5min lease
4. Breaker persister flushes buffered transitions
5. No manual intervention needed

**Design principle:** Postgres = single source of truth for durable state. Process-local state is disposable. Outage = pause, not data loss.

---

## 28. "How does the HostLimiter interact with classifyDelivery? Walk me through a cap miss."

**Flow in `classifyDelivery`** (`internal/worker/classify.go:36-68`):

```go
// 1. Resolve hostname (prefer pre-computed url_host, cache fallback)
host := d.URLHost
if host == "" {
    // Parse URL, cache result in urlHostCache
}

// 2. Non-blocking acquire
release, acquireErr := w.HostLimiter.TryAcquire(host)
if acquireErr != nil {
    return deliveryOutcome{requeued: true}  // ← CAP MISS
}
defer release()

// 3. Proceed with HTTP POST...
```

**What happens on cap miss:**
1. `TryAcquire` returns error immediately (non-blocking)
2. `classifyDelivery` returns `deliveryOutcome{requeued: true}`
3. Back in `processDeliveries`, requeued deliveries collected into `requeued []string`
4. `store.RequeueBatch(ctx, requeued, 100ms)` — sets `status = 'failed_retry'`, bumps `next_attempt_at`
5. No attempt row written, no retry budget consumed
6. Worker moves on to serve another Customer

**Why non-blocking?**
If `TryAcquire` blocked, a worker would sit idle waiting for a partner slot instead of processing other Customers' deliveries. Non-blocking + requeue preserves fairness: the worker immediately returns to the scheduler for the next Customer.

**HostLimiter internals** (`internal/worker/host_limiter.go`):
- `sync.Map` of hostname → `*hostEntry` (weighted semaphore)
- Default: 20 concurrent per host per process
- `lastUsed` atomic timestamp for cleanup
- `RunCleanup` goroutine evicts stale entries every 10min (idle > 30min)

**URL host resolution optimization:**
- `webhooks.url_host` is a Postgres generated column → projected at claim time
- Avoids `url.Parse()` on every attempt
- Fallback: parse + cache in `urlHostCache` (sync.Map)

---

## 29. "How would you design monitoring and alerting for this system in production?"

**Metrics already exist** (`internal/observe/`). Here's the alerting strategy:

### Tier 1 — Page (wake someone up)

| Alert | Metric | Threshold | Why |
|-------|--------|-----------|-----|
| Delivery success rate drop | `webhook_deliveries_total{status="succeeded"}` / total | < 95% over 5min | Core reliability SLO |
| Small Customer P95 spike | `webhook_queue_wait_seconds{customer_class="small"}` | > 2x Whale P95 | Fairness violation |
| DLQ spike | `dead_letters` insert rate | > 10/min sustained | Systemic partner failure |
| Postgres connection exhaustion | Pool gauge: open/max | > 90% | All operations blocked soon |

### Tier 2 — Ticket (fix during business hours)

| Alert | Metric | Threshold | Why |
|-------|--------|-----------|-----|
| Breaker opens | `webhook_breaker_state{state="open"}` | Any | Partner down, deliveries pausing |
| HostLimiter requeue rate | `webhook_host_limiter_requeue_total` | Sustained high | Partner under pressure |
| Empty claim ratio | `webhook_claim_empty_total` / `webhook_claim_batch_size` | > 50% | Discovery noise, wasted DB work |
| Backlog growing | `webhook_pending_deliveries` | Monotonically increasing 15min | Workers can't keep up |

### Tier 3 — Dashboard only

| Metric | Purpose |
|--------|---------|
| `webhook_post_duration_seconds` by customer_class | Partner latency trends |
| `webhook_attempt_total` by outcome | Retry/success distribution |
| `webhook_ingest_fanout` | Event-to-delivery ratio |
| `pg_notify_broadcast_total` by outcome | Wakeup health |
| Worker transient DB errors | Infrastructure stability |

### Key dashboard panels:
1. **Fairness panel**: Small vs Whale P95 queue wait, side by side
2. **Throughput panel**: Deliveries/sec by status (succeeded/retry/dead/requeued)
3. **Partner health**: Per-host latency P50/P95, breaker state, requeue rate
4. **Infrastructure**: DB pool utilization, claim query duration, backlog depth

---

## 30. "What headers does your webhook POST include, and why each one?"

**Headers set in `worker/client.go:78-92`:**

| Header | Value | Purpose |
|--------|-------|---------|
| `Content-Type` | `application/json` | Standard payload type |
| `X-Event-Id` | UUID | Stable across all Deliveries from one Event. Partner deduplicates across webhook fanout |
| `X-Delivery-Id` | UUID | Stable across retries of one Delivery. Primary dedup key for partners |
| `X-Event-Type` | e.g. `subscriber.created` | Partner routes to handler without parsing body |
| `X-Attempt-Number` | Integer | Partner knows first attempt vs retry |
| `X-Signature-Timestamp` | Unix epoch | Bound into HMAC → replay protection. Partner rejects if `abs(now - ts) > 5min` |
| `X-Signature-256` | Hex HMAC-SHA256 | `HMAC(secret, "timestamp.body")` → integrity + authenticity |
| `X-Notifier-Replica` | String (optional) | Debugging: which replica sent this attempt |

**Idempotency contract:**
- `X-Delivery-Id` = primary dedup key (same across retries)
- `X-Event-Id` = secondary dedup key (same across all deliveries from one event)
- Partners use either depending on their dedup semantics

**Why these specific names?**
Industry standard pattern. Stripe uses `Stripe-Webhook-Id`, GitHub uses `X-GitHub-Delivery`. NOT `Idempotency-Key` (that's for client→server request dedup, different semantics).

**Conditional headers:**
- `X-Signature-*` only sent when `d.Secret != ""` — unsigned webhooks are valid (cheaper integration)
- `X-Notifier-Replica` only sent when `NOTIFIER_REPLICA_ID` env var is set — opt-in debugging
- `X-Attempt-Number` only sent when `> 0` — first attempt omits it

---

## 31. "Why one shared HTTP client instead of one per worker or one per delivery?"

**Design** (`internal/worker/client.go`):

```go
func DefaultHTTPConfig() HTTPConfig {
    return HTTPConfig{
        MaxIdleConns:          10000,
        MaxIdleConnsPerHost:   500,
        MaxConnsPerHost:       500,  // backstop; HostLimiter is the real cap
        IdleConnTimeout:       90 * time.Second,
        TLSHandshakeTimeout:   10 * time.Second,
        ResponseHeaderTimeout: 10 * time.Second,
    }
}
```

One `*http.Client` created in `runtime.go:169`, shared across all workers in the process.

**Why not per-delivery?**
- Each `http.Client` creates its own connection pool
- Per-delivery = no connection reuse = TLS handshake on every request
- At 300 deliveries/s, that's 300 handshakes/s — massive overhead

**Why not per-worker?**
- Workers in the same process often hit the same partner hosts
- Per-worker pools fragment idle connections across N pools
- Shared pool → one connection to `partner-a.com` serves any worker

**Why no `http.Client.Timeout`?**
Per ADR-0007: per-Attempt deadlines come from **request contexts**, not `http.Client.Timeout`. This lets the response body drain before cancellation so Go can reuse the TCP connection:

```go
// client.go:53 — drain body BEFORE classification
_, _ = io.Copy(io.Discard, resp.Body)
resp.Body.Close()
```

If you used `http.Client.Timeout`, it cancels mid-body-read → connection can't be reused → pool thrashes under load.

**The `cancelOnCloseReadCloser` trick** (`client.go:137-146`):
Context cancel is deferred until `resp.Body.Close()`. This ensures the per-request timeout context doesn't leak if the caller forgets to close.

---

## 32. "Explain the pg_notify wakeup mechanism — how do replicas learn about new work instantly?"

**Problem:** Replica A ingests an Event. Replica B has workers idle. Without notification, B waits for the next discovery interval (default 1s) to find the work.

**Two-layer wakeup** (`cmd/notifier/pg_notify_listener.go`):

**Layer 1 — Broadcast (sender side):**
```go
// schedulerNotifier.Notify() — called after ingest commit
n.scheduler.Notify(customerID)    // local: O(1) add to linked list
n.broadcaster.Notify(customerID)  // remote: queue for pg_notify
```

The `pgNotifyBroadcaster`:
1. Checks per-Customer cooldown (100ms) via `sync.Map` — lock-free hot path
2. If not cooled down → coalesced, skip
3. If fresh → enqueued to bounded channel (default 1024+ capacity)
4. Background goroutine drains queue, calls `SELECT pg_notify($1, $2)` on a **dedicated** Postgres connection (1 conn, separate from main pool)

**Layer 2 — Listener (receiver side):**
```go
// runCustomerWakeListener — one goroutine per replica
conn.Exec(ctx, "LISTEN " + channel)
for {
    notification := conn.WaitForNotification(ctx)
    sched.Notify(notification.Payload)  // customerID
}
```

Uses raw `pgx.Connect` (not pooled) → `LISTEN` on a persistent connection → `WaitForNotification` blocks until Postgres pushes a message.

**Failure handling:**
- Broadcaster queue full → notification dropped, logged, metric incremented → discovery recovers within 1-30s
- Listener disconnects → reconnect with 1s delay → `LISTEN` re-registered
- pg_notify is best-effort by design — periodic discovery is the safety net

**Why cooldown?**
A Whale import of 500K events would fire 500K `pg_notify` calls. Cooldown coalesces them to one per Customer per 100ms. Stale cooldown entries pruned when map exceeds 1024 entries.

---

## 33. "What is lock ordering and why does it matter in your system?"

**The invariant:** Every transaction that touches both tables acquires `deliveries` first, then `delivery_claims`. Always.

**Why:**
```
Thread A (claim):   LOCK deliveries row 1  →  LOCK delivery_claims row 1
Thread B (requeue): LOCK delivery_claims row 1  →  LOCK deliveries row 1
                    ↑ DEADLOCK (40P01)
```

If two transactions acquire locks in opposite order, Postgres detects the cycle and aborts one with `SQLSTATE 40P01`.

**Where it's enforced:**

| Operation | Lock order | How |
|-----------|------------|-----|
| Claim CTE | `FOR UPDATE OF d SKIP LOCKED` → `INSERT INTO delivery_claims` | CTE structure guarantees order |
| MarkSucceeded/MarkRetry/MarkDead | `deliveries` via `withInFlightDelivery` CTE → `delivery_claims` | CTE acquires `d` first |
| Requeue | Same `withInFlightDelivery` pattern | Fixed in ADR-0012 — previously was inverted |
| Archive | `FOR UPDATE` on `deliveries` → `DELETE delivery_claims` | Only touches `deliveries` lock |

**History:** ADR-0012 documents that the original `Requeue` and breaker state writes used `FOR UPDATE OF dc, d` over a `delivery_claims`-first join. This deadlocked under concurrent claim+requeue. Fixed by rewriting to use `deliveries`-then-`delivery_claims` order, matching the claim path. Covered by regression test `TestClaimNextBatchAndRequeueConcurrentNoDeadlock`.

---

## 34. "How does PgBouncer interact with prepared statements, and why does it matter for your hot path?"

**Problem:**
- Hot path CTEs (claim, batch mark, breaker update) run thousands of times per second
- Without prepared statements: Postgres parses + plans each execution → wasted CPU
- PgBouncer in transaction pooling mode traditionally couldn't handle prepared statements (they're session-scoped)

**Solution (ADR-0012):**
- PgBouncer 1.21+ supports `max_prepared_statements` in transaction mode
- Local stack uses `edoburu/pgbouncer:v1.25.1-p0` with:
  ```
  POOL_MODE=transaction
  DEFAULT_POOL_SIZE=80
  MAX_PREPARED_STATEMENTS=200
  ```
- pgx uses cached-statement mode → first execution: `Parse` + `Bind` + `Execute` → subsequent: `Bind` + `Execute` only

**Performance impact:**
- Claim CTE is ~40 lines of SQL with multiple CTEs, joins, and `ON CONFLICT`
- Parse + plan cost is non-trivial for complex CTEs
- Cached plans skip parse/plan → measurable latency reduction on hot path

**Connection sizing:**
```go
// main.go:66-81
maxConns := runtimeCfg.WorkerCount * 3  // capped at 50
```

Notifier opens ≤50 client connections → PgBouncer keeps ≤80 server connections → headroom for admin queries and retention jobs.

---

## 35. "What headers does your webhook POST include, and why each one?"

**Headers set in `worker/client.go:78-92`:**

| Header | Value | Purpose |
|--------|-------|---------|
| `Content-Type` | `application/json` | Standard |
| `X-Event-Id` | UUID | Stable across all Deliveries from one Event. Partner deduplicates across fanout. |
| `X-Delivery-Id` | UUID | Stable across retries of one Delivery. Partner deduplicates retries. |
| `X-Event-Type` | e.g. `subscriber.created` | Partner routes to correct handler without parsing body |
| `X-Attempt-Number` | Integer | Partner knows if this is first attempt or retry |
| `X-Signature-Timestamp` | Unix epoch | Bound into HMAC. Partner rejects if `abs(now - ts) > 5min` → replay protection |
| `X-Signature-256` | Hex HMAC-SHA256 | `HMAC(secret, "timestamp.body")` → integrity + authenticity |
| `X-Notifier-Replica` | String (optional) | Debugging: which notifier instance sent this attempt |

**Idempotency contract:**
- `X-Delivery-Id` is the primary dedup key — same across retries
- `X-Event-Id` is the secondary dedup key — same across all Deliveries from one Event
- Partners can use either depending on their dedup semantics

**Why NOT `Idempotency-Key`?**
That header is typically for client→server request dedup (RFC draft). Webhook deliveries are server→partner pushes — custom headers are the industry standard (Stripe uses `Stripe-Webhook-Id`, GitHub uses `X-GitHub-Delivery`).

---

## 36. "How would you add a new event type, say `subscriber.tagged`?"

**Answer: zero code changes needed.** Event types are data, not code.

**Flow:**
1. Customer configures a Webhook with `event_types: ["subscriber.tagged"]` (via management API, out of PoC scope)
2. Producer sends `POST /events` with `type: "subscriber.tagged"` and payload
3. Ingest calls `registry.ListActiveForEvent(ctx, customerID, "subscriber.tagged")`
4. Registry queries: `WHERE customer_id = $1 AND active = true AND $2 = ANY(event_types)`
5. Returns matching Webhooks → fanout creates Deliveries → normal delivery flow

**What makes this work:**
- `webhooks.event_types` is a `text[]` column — array contains the subscribed types
- Registry query uses `= ANY(event_types)` — any type string matches
- No enum, no code-level event type registry, no type validation
- The PoC trusts the producer to send valid types

**Discussion point the interviewer might raise:**
- "Should you validate event types?" → Product decision. An enum prevents typos but adds coupling. A schema registry is the middle ground.
- "What about schema versioning?" → Payload is `jsonb`, schema is the producer's contract. Versioning (e.g., `subscriber.created.v2`) can be a type string convention.

---

## 37. "What happens if Postgres goes completely down?"

**Immediate effects:**

| Component | Behavior |
|-----------|----------|
| Ingest HTTP | Returns 500 — `BeginTx` fails. Client retries (producer's transactional outbox handles this) |
| Workers | `ClaimNextN` fails → logs error → retries with backoff. No deliveries processed |
| Discovery | `DiscoverClaimableCustomers` fails → backs off exponentially (1s → 2s → ... → 30s cap) |
| Breaker persister | Async writes fail → logged, metric. In-memory breaker state preserved |
| pg_notify broadcaster | `SELECT pg_notify(...)` fails → notification dropped, metric. Not critical — best effort |
| pg_notify listener | Connection drops → reconnect loop with 1s delay |
| Metrics/health | `/health` endpoint should report unhealthy (DB ping fails) |

**What's NOT lost:**
- No durable local state → nothing to corrupt
- In-memory scheduler rotation preserved (stale but intact)
- In-memory breaker state preserved
- HostLimiter counters preserved

**Recovery when Postgres comes back:**
1. Discovery loop succeeds → populates scheduler with claimable Customers
2. Workers claim and process normally
3. Stale claims from before outage → recovered by stale_candidate CTE (5min lease)
4. Breaker persister flushes buffered transitions
5. Backlog drains — no manual intervention needed

**Key design principle:** Postgres is the single source of truth for durable state. Process-local state is disposable and rebuildable. Outage = pause, not data loss.

---

## 38. "Explain the ADR process — why do you have 14 ADRs for a PoC?"

**What ADRs are:**
Architecture Decision Records — short documents that capture the *why* behind a design choice, not just the *what*.

**Why so many for a PoC:**

1. **The assignment asks for discussion** — ADRs are the concrete artifact for discussing trade-offs
2. **Each ADR captures a non-obvious choice:**
   - ADR-0001: Why round-robin over Customers, not FIFO over Deliveries
   - ADR-0002: Why Postgres as queue instead of a broker
   - ADR-0004: Why Requeue is different from Retry
   - ADR-0011: Why `delivery_claims` table instead of a `status` column
   - ADR-0013: Why discovery is claim-agnostic (counter-intuitive)

3. **They show evolution:**
   - ADR-0008 (persisted circuit breaker) → superseded by ADR-0014 (in-memory + persisted hybrid)
   - ADR-0006 (DB-backed discovery) → refined by ADR-0013 (claim-agnostic)

**Structure of each ADR:**
```
# Title
Status: Accepted / Superseded by ADR-XXXX
## Context — what problem
## Decision — what we chose
## Consequences — what we got and what we gave up
```

**Interview value:** When asked "why did you do X?", you can point to the specific ADR. Shows you made deliberate choices, not default ones. Shows you considered alternatives.

---

## 39. "The `delivery_claims` table — why a separate table instead of a `claimed_by` column on `deliveries`?"

**ADR-0011 documents this decision.**

**Option A: `deliveries.status = 'in_flight'` + `claimed_at` column**
- Simpler schema
- But: claim query needs `UPDATE deliveries SET status = 'in_flight'` → changes the value that the claim index (`status IN ('pending', 'failed_retry')`) filters on
- After claim, the row disappears from the partial index → can't be "found" for stale recovery without a separate index on `status = 'in_flight'`
- Mixing scheduling state (`pending`/`failed_retry`) with ephemeral claim state in one column creates coupling

**Option B: Separate `delivery_claims` table (chosen)**
```sql
delivery_claims
  delivery_id (PK)     -- FK to deliveries
  attempt_number       -- which attempt this claim is for
  claimed_at           -- when the lease started
```

**Benefits:**
- `deliveries.status` stays `pending` or `failed_retry` during a claim — the claim index still works
- Claim is an INSERT, not an UPDATE of the row the index relies on
- `ON CONFLICT DO UPDATE WHERE claimed_at < lease` handles both fresh claims and stale recovery in one statement
- Stale claim scan uses its own index: `delivery_claims (claimed_at, delivery_id)`
- Clean separation: `deliveries` = durable scheduling state, `delivery_claims` = ephemeral worker lease

**Trade-off:**
- Extra table, extra JOIN in claim query
- Lock ordering invariant required (deliveries → delivery_claims)
- Slightly more complex SQL

Worth it: the claim query is cleaner, the indexes are more targeted, and recovery is atomic without a sweeper.

---

## 40. "If you were starting this project over, what would you change?"

**Good "growth mindset" question. Honest answers:**

### Would keep
- **Postgres-only for PoC** — proved the design without ops complexity
- **Scheduler iterates Customers** — the core fairness insight, correct from day one
- **`delivery_claims` as separate table** — cleaner than status column approach
- **ADR discipline** — paid off in every design discussion
- **Benchmark suite with pass/fail gates** — catches regressions, proves claims

### Would change

| Change | Why |
|--------|-----|
| **Start with `pgxpool` instead of `database/sql`** | Ended up needing batch operations and `CopyFrom` patterns that `database/sql` makes awkward. ADR-0012 deferred this — but it's tech debt now |
| **Build the per-Customer concurrency cap earlier** | Round-robin controls admission order but not resource consumption. A slow partner serving one Customer can still monopolize workers. Should have been in v1 |
| **Use `LISTEN/NOTIFY` from the start** | Added pg_notify wakeups late. Initial design relied solely on polling discovery. Wakeups dramatically reduce first-attempt latency for sparse workloads |
| **Structured error types instead of string matching** | `RetryPolicy.classify()` returns reason strings. Typed errors would make the classify → mark → breaker chain more explicit and compiler-checked |
| **Fleet-wide HostLimiter from the start** | Process-local is known-insufficient for production. Even a Redis-backed prototype would have surfaced coordination failure modes earlier |
| **Webhook management API** | Even a minimal CRUD would have made demo/benchmark setup easier and shown the full product loop |

**Frame for interviewer:** These aren't regrets — they're the natural backlog of a PoC that prioritized proving the core fairness and reliability mechanics first. Each one has a clean extension path because the interfaces were designed for it.

---
---

# Part 2: Metrics Discussion Scenarios

The assignment brief explicitly requires: *"Discuss monitoring metrics for system performance and latency."* These are the concrete artifacts: `docs/observability/dashboard.md`, `docs/observability/alerts.yml`, and 23 Prometheus metrics in `internal/observe/registry.go`.

---

## M1. "Walk me through your metric catalogue — what are the most important metrics and why?"

**The full metric set** (`internal/observe/registry.go`):

### Delivery lifecycle (the core story)

| Metric | Type | What it tells you |
|--------|------|-------------------|
| `webhook_deliveries_total` | CounterVec (`status`, `customer_class`) | Terminal delivery outcomes: succeeded vs dead, split by customer size |
| `webhook_delivery_latency_seconds` | HistogramVec (`customer_class`) | Full delivery lifetime: created_at → terminal state |
| `webhook_queue_wait_seconds` | HistogramVec (`customer_class`) | **THE fairness metric**: created_at → first attempt sent_at. Excludes partner response time and retry backoff |
| `webhook_post_duration_seconds` | HistogramVec (`customer_class`) | Raw HTTP POST time — isolates partner latency from queue behavior |
| `webhook_attempt_total` | CounterVec (`outcome`) | Attempt-level outcomes: succeeded, retryable_5xx, terminal_4xx, timeout, network_error, exhausted, age_exceeded |
| `webhook_attempts_per_delivery` | Histogram | How many attempts each delivery takes. Healthy = spike at 1, long tail = partner problems |
| `webhook_dlq_total` | CounterVec (`reason`) | Dead letter volume by reason — exhausted vs terminal_4xx vs age_exceeded |

### Scheduling and claiming (system health)

| Metric | Type | What it tells you |
|--------|------|-------------------|
| `webhook_claim_duration_seconds` | Histogram | How long the claim CTE takes. Rising = Postgres contention |
| `webhook_claim_batch_size` | Histogram | Deliveries claimed per batch. Dropping while backlog grows = problem |
| `webhook_claim_empty_total` | Counter | Wasted claim queries. High + backlog > 0 = discovery noise (ADR-0013 trade-off) |
| `webhook_scheduler_pick_duration_seconds` | Histogram | Scheduler pick latency. Should be microseconds (O(1) list operation) |

### Backlog (starvation signal)

| Metric | Type | What it tells you |
|--------|------|-------------------|
| `webhook_pending_deliveries` | GaugeVec (`customer_class`) | Queue depth by customer size. Growing = workers can't keep up |
| `webhook_pending_oldest_age_seconds` | Gauge | Age of oldest pending delivery. **Stronger starvation signal than depth** — stable backlog can still contain stuck work |
| `webhook_active_customers` | Gauge | How many customers have active work. Useful for capacity planning |

### Partner protection

| Metric | Type | What it tells you |
|--------|------|-------------------|
| `webhook_host_semaphore_wait_seconds` | HistogramVec | HostLimiter acquisition time. Normally near-zero (non-blocking TryAcquire). Requeue rate is more useful than wait duration |
| `webhook_breaker_state` | GaugeVec (`webhook_id`, `state`) | Per-webhook circuit breaker state. High-cardinality — use for diagnosis, not long-retention aggregation |
| `webhook_breaker_trips_total` | CounterVec | How often breakers open. Spike = widespread partner problems |

### Infrastructure

| Metric | Type | What it tells you |
|--------|------|-------------------|
| `webhook_workers_configured` | Gauge | Expected worker count — compare with `webhook_deliveries_in_flight` |
| `webhook_deliveries_in_flight` | Gauge | Currently processing. If this equals workers × concurrency → saturated |
| `webhook_pg_pool_*` | GaugeFuncs | Postgres pool: open, in_use, idle, max_open, wait_count, wait_duration |
| `webhook_pg_notify_broadcast_total` | CounterVec (`outcome`) | Wakeup notifications: sent/coalesced/dropped/failed |
| `webhook_worker_transient_db_errors_total` | CounterVec (`phase`) | Recoverable DB errors by phase (claim/mark/fetch) |
| `webhook_breaker_sync_fallback_total` | Counter | Breaker persistence fell back to synchronous writes — async buffer full |
| `webhook_ingest_fanout_per_event` | Histogram | Deliveries created per event. Shows webhook fanout distribution |

---

## M2. "Which single metric would you use to detect a fairness violation?"

**Answer: `webhook_queue_wait_seconds` with `customer_class` label.**

```
webhook_queue_wait_seconds{customer_class="small"} P95
vs
webhook_queue_wait_seconds{customer_class="whale"} P95
```

**Why this metric specifically:**
- Measures `deliveries.created_at` → `delivery_attempts.sent_at` where `attempt_number = 1`
- **Excludes** partner response time (that's the partner's problem, not fairness)
- **Excludes** retry backoff delays (those are intentional, not starvation)
- **Excludes** post-first-attempt lifecycle (retry fairness is a separate concern)
- Isolates exactly: "how long did this delivery wait in queue before a worker picked it and POSTed?"

**Alert rule** (from `alerts.yml`):
```yaml
- alert: WebhookSmallCustomerQueueWaitHigh
  expr: histogram_quantile(0.95, sum by (le) (rate(webhook_queue_wait_seconds_bucket{customer_class="small"}[5m]))) > 5
  for: 10m
  severity: warn
```

**Why comparative, not absolute:**
An absolute SLO like "P95 < 500ms" needs production traffic and capacity data. The benchmark gate is comparative: Small P95 within a ratio of Whale P95 under the same run. Production would establish absolute thresholds based on observed baseline.

---

## M3. "How do you distinguish between 'system is slow' and 'partner is slow'?"

**Three metrics, layered:**

```
webhook_queue_wait_seconds     → system scheduling speed (queue → first POST)
webhook_post_duration_seconds  → partner response time (HTTP POST duration)
webhook_delivery_latency_seconds → end-to-end (created_at → terminal state)
```

**Decision tree:**

| queue_wait high? | post_duration high? | Diagnosis |
|:---:|:---:|---|
| Yes | No | System bottleneck — scheduling/claiming slow. Check `webhook_claim_duration_seconds`, Postgres pool, worker count |
| No | Yes | Partner slow — eating worker time. Check per-host `webhook_host_semaphore_wait_seconds`, breaker state |
| Yes | Yes | Both — partner slowness backing up the queue. HostLimiter requeues occupying claim capacity |
| No | No | System healthy. If delivery_latency is still high, it's retry backoff (expected) |

**Key insight the interviewer wants:** `delivery_latency_seconds` is an end-to-end metric — it includes queue wait + POST time + all retry delays. Debugging requires decomposing it into the three sub-metrics above. One fat metric is not enough.

---

## M4. "Your `webhook_claim_empty_total` is spiking. What does that mean and what do you do?"

**What it means:** Workers are asking Postgres for deliveries and getting nothing back.

**Two very different root causes:**

**Case 1: Empty claims + backlog = 0** → Normal. All work is processed. Discovery hasn't removed these customers from rotation yet. Self-correcting.

**Case 2: Empty claims + backlog > 0** → Problem. Work exists but workers can't claim it. Three sub-causes:

| Sub-cause | How to confirm | Fix |
|-----------|---------------|-----|
| All rows locked by other workers | `webhook_deliveries_in_flight` near max | Add workers or increase batch size |
| Discovery surfacing customers whose rows are all claimed | ADR-0013 trade-off — claim-agnostic discovery | Tune discovery interval or add negative cache |
| Breaker-filtered webhooks dominating backlog | `webhook_breaker_state` shows many open | Wait for probes or investigate partner health |

**The alert** (from `alerts.yml`):
```yaml
- alert: WebhookClaimEmptyRateHigh
  expr: sum(rate(webhook_claim_empty_total[5m])) / clamp_min(sum(rate(webhook_claim_batch_size_count[5m])), 0.001) > 0.25 and sum(webhook_pending_deliveries) > 0
  for: 10m
```

Note the `and sum(webhook_pending_deliveries) > 0` guard — only alerts when there's actual work that isn't being claimed.

---

## M5. "How would you detect that a partner is about to cause problems before the circuit breaker opens?"

**Leading indicators (before 20 consecutive failures trip the breaker):**

1. **`webhook_post_duration_seconds` P95 rising for specific hosts** — partner getting slower, timeouts coming
2. **`webhook_attempt_total{outcome="retryable_5xx"}` rate increasing** — intermittent 500s before they become consecutive
3. **`webhook_host_semaphore_wait_seconds` rising** — HostLimiter slots filling up, partner absorbing all concurrency
4. **`webhook_attempts_per_delivery` distribution shifting right** — deliveries needing more attempts to succeed
5. **`webhook_pending_deliveries{customer_class=...}` growing for specific customers** — backlog building for customers using that partner

**What you'd build (production improvement):**

A per-host error rate metric (doesn't exist yet in the PoC):
```go
// Proposed: track error rate per partner host
WebhookHostErrorRate *prometheus.CounterVec  // labels: host, error_class
```

This would let you alert on `rate(webhook_host_errors{host="partner-a.com"}[5m]) > threshold` before the breaker opens.

**Why the breaker doesn't fully solve this:**
Breaker is per-Webhook, not per-host. If a partner serves 50 webhooks and starts degrading, each webhook's breaker counts independently → 50 × 20 = 1000 failures before all breakers open. A host-level error rate metric would catch this pattern earlier.

---

## M6. "Explain your alert rules — why these specific thresholds?"

**Six alerts in `docs/observability/alerts.yml`:**

| Alert | Threshold | Severity | Rationale |
|-------|-----------|----------|-----------|
| `WebhookDeliveryP95High` | P95 lifetime > 300s for 10min | **page** | 5-minute delivery lifetime means retries are stacking. 300s = ~3 retry cycles with backoff. Something is systematically failing |
| `WebhookErrorRateHigh` | > 5% dead deliveries for 10min | **page** | Dead = permanently failed. 5% sustained means partner outage or misconfigured webhooks. Not self-healing |
| `WebhookQueueDepthGrowing` | `deriv > 0` for 15min AND oldest > 300s | **warn** | Both conditions required: growing backlog alone could be a temporary burst. Growing + aging = workers can't keep up |
| `WebhookSmallCustomerQueueWaitHigh` | Small P95 > 5s for 10min | **warn** | Fairness violation. 5s is generous for the PoC; production would calibrate based on SLO |
| `WebhookClaimEmptyRateHigh` | > 25% empty AND backlog > 0, for 10min | **warn** | Wasted DB work while real work exists. Guard prevents alerting during idle periods |
| `WebhookPostgresPoolSaturated` | > 90% in use for 10min | **warn** | Connection exhaustion imminent. Workers, ingest, discovery all share the pool |
| `WebhookBreakerPersistenceFallback` | Any occurrence | **warn** | Async breaker persistence buffer full → fell back to synchronous writes. Indicates either very high breaker churn or persistence backpressure |

**Key design choices:**
- **Page** = data loss risk or SLO breach. Only 2 alerts: delivery P95 and error rate.
- **Warn** = degradation, not emergency. Ticket during business hours.
- **`for` durations** prevent transient spike alerts. 10min minimum.
- **Composite conditions** (queue depth + oldest age) reduce false positives.

---

## M7. "What metrics would you add for production that aren't in the PoC?"

| Missing metric | Why needed | Implementation |
|----------------|-----------|---------------|
| **Per-host error rate** | Detect partner degradation across webhooks before individual breakers trip | CounterVec by `host`, `error_class` |
| **Fleet-wide concurrency per host** | Process-local HostLimiter can't show total fleet pressure on one partner | Distributed limiter would expose this |
| **Ingest request rate and latency** | Currently behind Caddy reverse proxy — ingest throughput invisible | Add histogram in `http_handlers.go` or use Caddy access log metrics |
| **Per-customer backlog depth** | Current `webhook_pending_deliveries` is by `customer_class`, not individual customer. Can't spot one stuck customer | GaugeVec by `customer_id` — but high-cardinality, use exemplars or top-N |
| **Claim contention rate** | How often `SKIP LOCKED` skips rows (all locked by other workers) | Difference between candidates scanned and candidates claimed |
| **Discovery cycle time** | How long each discovery scan takes and how many customers it finds | Histogram + counter in `RunClaimableCustomerDiscovery` |
| **Secret decryption latency** | When encryption-at-rest is added, this becomes a hot-path cost | Histogram in the decrypt path |

---

## M8. "How does `webhook_pending_oldest_age_seconds` work and why is it a stronger signal than queue depth?"

**What it measures:** Age of the oldest `pending` or `failed_retry` delivery where `next_attempt_at <= now()`.

Sampled by `observe.StartPendingDeliveriesSampler` (background goroutine, configurable interval).

**Why stronger than depth:**

| Scenario | Queue depth | Oldest age | What's happening |
|----------|:----------:|:----------:|-----------------|
| Whale burst arriving | 100K, growing | 2s | Normal — burst just arrived, workers will drain it |
| Steady state | 500, stable | 1s | Healthy — constant throughput with short queue |
| **Starvation** | 500, stable | **300s** | Bad — same 500 deliveries sitting for 5 minutes. Workers busy elsewhere |
| Empty | 0 | 0 | Idle — no work |

**The key insight:** A stable queue depth of 500 looks fine. But if the oldest delivery is 5 minutes old, those 500 deliveries aren't making progress — they're stuck behind other work. Oldest age detects starvation that depth alone misses.

**Alert uses both** (`alerts.yml`):
```yaml
expr: deriv(sum(webhook_pending_deliveries)[15m:]) > 0 and webhook_pending_oldest_age_seconds > 300
```
Growing depth AND aging = real problem. Either condition alone could be transient.

---

## M9. "If I gave you one Grafana dashboard with 6 panels, what would you put on it?"

**The operator's single-pane-of-glass:**

| Panel | Metrics | Visualization | Why |
|-------|---------|---------------|-----|
| **1. Throughput** | `rate(webhook_deliveries_total[5m])` by status | Stacked area: green=succeeded, yellow=retry, red=dead | Are we delivering? |
| **2. Fairness** | `histogram_quantile(0.95, webhook_queue_wait_seconds)` by customer_class | Two lines: small vs whale | Is fairness holding? |
| **3. Backlog** | `webhook_pending_deliveries` + `webhook_pending_oldest_age_seconds` | Dual-axis: bar=depth, line=age | Are we keeping up? |
| **4. Partner health** | `histogram_quantile(0.95, webhook_post_duration_seconds)` + breaker count | Line=latency, stat=open breakers | Are partners healthy? |
| **5. System capacity** | `webhook_deliveries_in_flight` / `webhook_workers_configured` + `webhook_pg_pool_in_use` / `webhook_pg_pool_max_open` | Two gauges: worker util%, pool util% | Are we near limits? |
| **6. Claim efficiency** | `rate(webhook_claim_empty_total[5m])` / `rate(webhook_claim_batch_size_count[5m])` | Percentage line + `webhook_claim_duration_seconds` P95 | Is the DB path healthy? |

**Why these 6:** Each answers one of the assignment's three pillars — reliability (panels 1,3), fairness (panel 2), scalability (panels 5,6) — plus partner health (panel 4) which is the main external dependency.

---

## M10. "What's the difference between an SLI, SLO, and SLA for this system, and what would you propose?"

**Definitions applied to webhook notifier:**

| Term | What it is | Example |
|------|-----------|---------|
| **SLI** (Service Level Indicator) | The metric you measure | `webhook_queue_wait_seconds{customer_class="small"}` P95 |
| **SLO** (Service Level Objective) | The target you set internally | "Small Customer P95 first-attempt queue wait < 5s" |
| **SLA** (Service Level Agreement) | The contract with customers (with penalties) | "99.9% of webhook deliveries will receive a first attempt within 30s" |

**Proposed SLOs for production:**

| SLO | SLI metric | Target | Rationale |
|-----|-----------|--------|-----------|
| **Reliability** | `webhook_deliveries_total{status="succeeded"}` / total | > 99.5% over 7 days | Allows for partner-caused dead letters |
| **Fairness** | Small P95 queue wait / Whale P95 queue wait | < 2.0x ratio | Small customers get comparable treatment |
| **Latency** | `webhook_queue_wait_seconds` P95 (all classes) | < 10s | First attempt starts within 10s of event creation |
| **Availability** | Ingest API success rate | > 99.9% | Events can be submitted |

**What the PoC currently measures:**
- Fairness benchmark uses comparative P95 ratio (SLI exists, SLO is benchmark-specific)
- No absolute SLO — needs production traffic baseline
- Delivery success rate tracked but no formal target

**Discussion point:** The PoC can't set absolute SLOs without production data. But having the SLIs instrumented and the alerting framework in place means SLOs are a configuration decision, not an engineering project.
