# Performance Audit

Audited the webhook-notifier codebase against REQUIREMENTS.md (Reliability, Scalability, Fairness). All `internal/` and `cmd/` packages reviewed.

---

## CRITICAL

### 1. `time.After` timer leak in hot loop — `internal/worker/run_loop.go:60`

`time.After(w.emptyClaimDelay())` creates a new `time.Timer` on every empty claim cycle. The timer is not garbage-collected until it fires. In busy-wait cycles (e.g., no Customers have work), timers accumulate until GC sweeps them.

Same issue at `run_loop.go:92` (`time.After(transientErrorBackoff)`).

- **Threatens**: Scalability
- **Fix**: Replace with `time.NewTimer` + explicit `timer.Stop()` on `ctx.Done()`.

### 2. `ingest.go:124-134` — Dynamically-built INSERT bypasses plan caching

`insertDeliveries()` uses `fmt.Fprintf` to build an INSERT with variable parameter placeholders (`$1, $2, $3` etc.). PostgreSQL cannot cache the query plan — SQL text changes with each call depending on Webhook count. Every insert forces a fresh `Parse` + `Plan` + `Execute` cycle.

- **Threatens**: Reliability (ingest latency under high fanout), Scalability
- **Fix**: Use `UNNEST` array approach (same pattern as `MarkSucceededBatch`).

### 3. `registry.go:96-97` — `context.Background()` in hot ingest path bypasses cancellation

`singleflight.DoChan` wraps the DB query with `context.Background()`, ignoring the caller's `ctx`. When a client disconnects (e.g., HTTP timeout), the DB query still runs to completion and the cached result is discarded. Under high request rates with frequent timeouts, this wastes DB capacity.

- **Threatens**: Scalability, Fairness (ingest goroutines compete with claim goroutines for DB connections)
- **Fix**: Thread through the caller's context for the DB query while keeping singleflight isolation.

---

## HIGH

### 4. `outbox/claim.go` — 8 separate `fmt.Sprintf`-built SQL claim variants (lines 68,123,150,224,278,332,357,392)

Every claim CTE is constructed via `fmt.Sprintf` injecting SQL predicate fragments. PostgreSQL sees different query text for breaker vs no-breaker paths, batch vs single, etc. Each variant triggers a separate prepared statement.

- **Threatens**: Scalability (PgBouncer `MAX_PREPARED_STATEMENTS` exhaustion; driver-side statement cache churn)
- **Fix**: Migrate to stable SQL with `COALESCE`/`CASE` trickery or a single parameterized CTE template.

### 5. `swrr.go:37` — Scheduler `entries` map never evicted (memory leak)

`s.entries map[scheduler.CustomerID]*entry` in the SWRR scheduler grows unboundedly. When a Customer is `Remove`d from the active set (line 131-141), the entry remains in the `entries` map forever.

- **Threatens**: Scalability (memory exhaustion under high Customer churn)
- **Fix**: Delete from `entries` in `Remove()`.

### 6. `pg_notify_listener.go:81-101` — `sync.Mutex` contention on every ingest

Every `schedulerNotifier.Notify()` call (fires on every Event ingest) locks `b.mu` to check/insert into `b.pending`. At high ingest throughput with many concurrent HTTP handlers, this mutex becomes contended.

- **Threatens**: Scalability (ingest latency under high concurrency)
- **Fix**: Replace `b.pending map[string]struct{}` with a second `sync.Map` or a channel-based coalescing goroutine.

### 7. `worker/host_limiter.go:36` — `sync.Map` per-host semaphores never cleaned up

`HostLimiter.sems sync.Map` grows unboundedly with each new partner hostname. If the notifier serves many Integration Partners (or an attacker rotates hostnames), this leaks memory.

- **Threatens**: Scalability
- **Fix**: Periodic cleanup of stale entries via `Range` + `CompareAndDelete`, or wrap with LRU cache.

### 8. `cmd/notifier/main.go:69` — DB pool sized to `WorkerCount + 10`

Pool may be too small under concurrent ingest + claim + discovery + archive activity. Default workers is typically 1-10, but the pool also serves ingest HTTP goroutines, discovery queries, backlog sampler, breaker persister, and archive operations.

- **Threatens**: Reliability (connection starvation under load), Scalability
- **Fix**: Set minimum floor (e.g., `max(25, WorkerCount * 3)`) and make configurable.

---

## MEDIUM

### 9. `pg_notify_listener.go:99-100` — Silent notification drops

When `b.queue` channel is full, `Notify` silently drops the notification. The `dropped` counter is incremented but there is no backpressure signal. Discovery loop (1s default interval) becomes the sole recovery path.

- **Threatens**: Reliability (latency spikes during burst), Fairness (stale worker Customer views)
- **Fix**: Non-blocking send with retry, dynamically grow channel, or at minimum log the drop.

### 10. `run_loop.go:226-368` — `processDeliveries` builds 3 separate maps per batch

For N deliveries, creates `results` (N), `deliveryByID` (N), and `classByID` (N) maps in sequence. Harmless at default batch size (64), but with `MaxBatchHint=0` (unbounded) and many Customers in a batch, allocation overhead grows.

- **Threatens**: Scalability (at very large batch sizes)
- **Fix**: Store `Delivery` in `classified` struct (already done) and add `CustomerClass` to `BatchMarkItem` to eliminate `classByID`.

### 11. `classify.go:42-48` — URL parsing fallback on every attempt

When `d.URLHost` is empty (edge case from bugged generated column or malformed URL), falls back to `url.Parse(d.URL)` — a non-trivial allocation-heavy operation on the hot delivery path.

- **Threatens**: Scalability (CPU/allocation overhead per delivery)
- **Fix**: Cache parsed host at worker level or fix the URLHost generated column.

### 12. `breaker/memory.go:12` — Breaker `sync.Map` entries never evicted

Breaker state entries persist once created (via `get()` at line 157-167). For webhooks that are deleted/inactive, the entry remains forever.

- **Threatens**: Scalability
- **Fix**: Combine breaker state with SubscriptionRegistry LRU lifecycle or add periodic cleanup of `Closed` breakers idle beyond TTL.

### 13. `dbweights/dbweights.go:25` — Cache never invalidated unless explicitly refreshed

`DBWeightSource.cache` has no TTL or automatic expiration. Only refreshes on explicit `Refresh()`/`RefreshAll()`. Customer class changes won't take effect until manual refresh.

- **Threatens**: Fairness (stale weights cause scheduling misallocation)
- **Fix**: Add TTL-based eviction or periodic refresh loop.

### 14. `backlog.go:49-89` — Full table scan for backlog sampling

`SELECT COUNT(*) ... FROM deliveries WHERE d.status IN ('pending', 'failed_retry') GROUP BY ...` scans `idx_deliveries_ready` fully. At high throughput with millions of pending rows, this becomes expensive.

- **Threatens**: Scalability (I/O contention with claim queries)
- **Fix**: Query cancellation to limit cost, sparser sampling interval, or materialized view.

### 15. `archive.go:59` — `FOR UPDATE OF d SKIP LOCKED` on archive scan

Archive locks terminal-status delivery rows. While statuses differ from claim targets, the lock still contends with the claim CTE's `delivery_claims` joins on the same table.

- **Threatens**: Reliability (deadlock `40P01` under concurrent archive + claim)
- **Fix**: Verify lock ordering in archive transactions matches the ordering documented in `DESIGN.md:290-291`.

---

## LOW / INFO

### 16. `client.go:48-58` — `Deliver()` method is dead code

Never called from the main run loop. Classify → `Send` is used instead.

### 17. `status_batch.go:327-344` — Triple sequential validation loops

Each item in succeeded/retries/dead is validated 2-3 times by O(n) loops. Trivial at typical batch sizes.

### 18. `classify.go:31` — Metrics hot-path calls on every delivery

`IncDeliveriesInFlight` / `DecDeliveriesInFlight`. Fine at normal rates, measurable at 10k+ deliveries/s.

### 19. `pg_notify_listener.go:56` — `sched.Notify()` called under same function as `broadcaster.Notify()`

Both synchronous. Scheduler `Notify` is O(1) with mutex lock — fine for normal use.

---

## Summary by Requirement

| Requirement   | # of issues | Key concerns |
|---------------|-------------|-------------|
| Reliability   | 3           | Timer leak (#1), ingest INSERT plan cache miss (#2), DB pool sizing (#8) |
| Scalability   | 8           | SQL plan caching (#2, #4), unbounded maps (#5, #7, #12), mutex contention (#6), allocation (#10) |
| Fairness      | 2           | Stale weight cache (#13), silent wakeup drops (#9) |
