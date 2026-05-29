# SubscriptionRegistry with LRU + TTL cache

## Context

The Webhook Notifier must resolve which Webhooks receive a given Event at ingest time. Today, `ingest.Store.Submit` runs an `INSERT INTO deliveries ... SELECT FROM webhooks WHERE customer_id = $1 AND active AND $2 = ANY(event_types)` inside the same transaction as the Event insert. This hits the `webhooks` table on every Event — a hot path that will dominate DB load under burst conditions (Whale Customer bulk import).

The `internal/registry` package is documented but unimplemented. DESIGN.md specifies it as "read-only `customer_id → []Webhook` lookup, in-memory LRU with short TTL; cache miss falls back to PG."

A cache layer introduces a staleness window: if a Customer adds or deactivates a Webhook, the cache may still return the old set until TTL expires. The design must decide how much staleness is acceptable and how the cache integrates with the existing transactional ingest flow.

A secondary concern: the Scalability Review (Section 5) identifies that no production code calls `Scheduler.Notify(customerID)` when new Deliveries are created, leaving Deliveries undiscovered with unbounded latency. The Registry refactor touches the same constructor and is the natural place to fix this.

## Decision

Implement `SubscriptionRegistry` as a **Postgres-backed, LRU+TTL-cached** lookup with **singleflight stampede protection**, integrated at the **ingest path only**.

### Interface

```go
// Registry resolves which Webhooks should receive a given event type for a Customer.
// Defined at the consumer boundary (ingest package).
type Registry interface {
    ListActiveForEvent(ctx context.Context, customerID string, eventType string) ([]Webhook, error)
}
```

Cache key: `(customerID, eventType)`. Not `customerID` alone — with only 3 event types today the key space is small, and per-event-type entries avoid post-filtering in Go.

### Webhook struct

Full row from the `webhooks` table:

```go
type Webhook struct {
    ID              string
    CustomerID      string
    URL             string
    SecretEncrypted string
    EventTypes      []string
    Active          bool
}
```

Returning the full struct future-proofs for outbox-path caching (follow-up work) and costs negligible memory.

### Cache mechanics

- **Library**: `hashicorp/golang-lru/v2/expirable` — production-proven, thread-safe, dual eviction (LRU + TTL). Not hand-rolled: the correctness surface (concurrent access, eviction ordering, TTL expiry) isn't worth the risk for a cache layer.
- **Defaults**: capacity 10,000 entries, TTL 30 seconds. Both configurable via functional options (`WithCapacity`, `WithTTL`).
- **Singleflight**: `golang.org/x/sync/singleflight.Group` keyed by `customerID:eventType`. Prevents cache stampede when many concurrent Events arrive for the same key after TTL expiry. `golang.org/x/sync` is already in `go.mod`.
- **Invalidation**: TTL-only for now. No explicit `Invalidate` method — there are no webhook management APIs in this codebase (out of scope per DESIGN.md). When admin APIs are built later, adding `Invalidate(customerID, eventType)` is a clean follow-up.

### Integration: ingest path only

`ingest.Store` gains `Registry` and `Notifier` dependencies:

```go
type Notifier interface {
    Notify(customerID string)
}

func New(db *sql.DB, registry Registry, notifier Notifier, opts ...Option) *Store
```

`Submit` is rewritten:

1. Call `registry.ListActiveForEvent(ctx, event.CustomerID, event.Type)` — cache hit returns immediately; miss queries DB and caches result.
2. Insert event row in transaction.
3. Insert delivery rows as a **multi-row `INSERT INTO deliveries VALUES (...), (...) RETURNING id`** — one SQL statement for N webhooks, sorted by webhook ID for deterministic ordering.
4. After `tx.Commit()`, call `notifier.Notify(event.CustomerID)` — fixes the scheduler discovery gap (Scalability Review §5).

The `outbox.loadDelivery` JOIN to `webhooks` stays unchanged. It's a point lookup by primary key and doesn't benefit from caching at this stage.

### Staleness trade-off

The Registry returns webhooks from cache, which may be up to TTL seconds stale. This means:

- A newly added Webhook may not receive Deliveries for up to TTL seconds.
- A recently deactivated Webhook may receive one extra Delivery for up to TTL seconds.

Acceptable because: (a) webhook changes are rare admin operations, not hot-path writes; (b) short TTL (30s) bounds the window; (c) partners handle out-of-order/delayed events via `X-Event-Id` dedup.

### Terminology note

CONTEXT.md defines **Webhook** as the canonical term and explicitly says "Avoid: subscription (collides with Subscriber)." The package and component are called `SubscriptionRegistry` as a name for the *registry of webhooks*, but the data type returned is `Webhook`, not `Subscription`.

## Considered options

- **`Get(customerID) ([]Webhook, error)`** — return all webhooks for a customer, filter by event type in Go. Rejected: with 3 event types it produces overlapping entries per customer and pushes SQL-equivalent filtering into Go. Per-`(customerID, eventType)` keys are smaller, exact, and scale naturally when Flodesk adds more event types.
- **Hand-rolled LRU+TTL** — `map[string]entry` + `sync.Mutex` + TTL check on `Get`. Rejected: concurrent LRU+TTL correctness is subtle. The vetted library gives both for zero design risk.
- **Transactionally consistent fan-out** — keep `INSERT ... SELECT FROM webhooks` inside the ingest transaction, and only use Registry for worker-time metadata. Rejected: the ingest fan-out is the hot path that needs caching. Worker-time metadata (`loadDelivery`) is already a fast primary-key lookup.
- **Registry at both ingest and outbox** — cache webhook metadata for the `loadDelivery` JOIN too. Rejected for this issue: outbox JOIN is fast by PK, and adding a second integration point doubles the surface area. Clean follow-up if profiling shows it matters.
- **Explicit `Invalidate(customerID, eventType)`** — evict cache entries on webhook CRUD. Rejected: no webhook management APIs exist today, so the method has zero consumers. Add when admin APIs land.
- **No singleflight** — simpler code, but a Whale Customer burst (50k `subscriber.created` events) would stampede the same cache key after TTL expiry. `singleflight` is 10 lines and prevents this with `golang.org/x/sync` (already a dependency).

## Consequences

- `hashicorp/golang-lru/v2` is a new dependency. It is widely used (Bytebase, Grafana, Datadog Agent) and maintained under the HashiCorp org.
- `Get()` in `expirable.LRU` does **not** refresh TTL — only `Add()` does. This is desirable: entries expire on a predictable schedule, which is what the integration test relies on.
- The scheduler discovery gap fix means `ingest.Submit` now calls `notifier.Notify(customerID)` after commit. This only notifies the local process's scheduler. Cross-instance discovery (LISTEN/NOTIFY or `customer_work` table) remains a P1 follow-up per the Scalability Review.
- Webhook changes are visible to the Registry after at most TTL seconds. Ops documentation should note this propagation delay.
- The multi-row INSERT in `Submit` requires building a dynamic VALUES list from the Registry result. With max ~20 webhooks per customer, the list is small and safe.
- Integration tests use a 1-second TTL (`WithTTL(1 * time.Second)`) so they complete quickly. Production uses the 30-second default.
- Adding `Invalidate` later is a non-breaking change — the interface can grow.
- If the outbox `loadDelivery` JOIN becomes a bottleneck, a `Get(ctx, webhookID) (Webhook, error)` method can be added to the Registry in a follow-up issue.
