# Weighted round-robin fairness with a pluggable Scheduler interface

## Context

The Webhook Notifier must prevent Whale Customers from starving Small Customers when Delivery volume is imbalanced. Equal-share round-robin solves the PoC requirement, but product has already flagged that Whale vs Small (or paid vs free) Customers will eventually need **differentiated shares** — e.g. paid tiers get more slots per cycle, or a misbehaving Whale gets throttled during an incident. Deficit Round Robin (DRR) is the textbook answer when per-item cost varies, but is overkill while Deliveries are uniform-cost HTTP POSTs.

The design therefore needs to commit to fairness semantics that are equal *today* but extensible to *weighted* without rewriting the WorkerPool, Outbox, or Dispatcher.

## Decision

Implement **Smooth Weighted Round Robin (SWRR) over the set of Customers with pending Deliveries**, with a default weight of `1` for every Customer. Source weights from a `WeightSource` interface (PoC impl: returns `1`; production impl: reads `customers.weight` with caching).

"Smooth" refers to the Nginx-style algorithm (Wensong Zhang) that interleaves picks instead of emitting contiguous weighted cycles. Classic WRR with weights `[3,1]` yields `AAAB AAAB`; SWRR yields `AABAAABA`. Same share invariant, with the low-weight Customer pulled forward as much as the 3:1 ratio permits — important once tiers ship, harmless when all weights are 1.

With uniform weights, SWRR is bit-for-bit equivalent to plain RR — same fairness, same ~30 lines of state, no behavioural regression vs the simpler design. When weights diverge, the same code path allocates proportional, interleaved slots.

Hide the policy behind a `Scheduler` interface so DRR (or any future variant) can be swapped in without touching the WorkerPool. `Pick` returns a `batchHint` equal to the picked Customer's weight, doubling as an amortization knob: with `weight=1` the worker claims one Delivery; with `weight=N` it may claim up to `N` Deliveries before yielding back to the Scheduler, reducing per-Pick PG round-trips for higher-weight tiers without changing the share invariant.

The interface is the contract — the implementation is the trade-off.

```go
type Scheduler interface {
    // Pick returns the next Customer to serve plus a hint of how many
    // Deliveries the worker may claim before returning to the Scheduler.
    // batchHint == 0 means "no work"; batchHint >= 1 otherwise.
    Pick(ctx context.Context) (id CustomerID, batchHint uint32, ok bool, err error)
    Notify(CustomerID)
}

type WeightSource interface {
    Weight(CustomerID) uint32 // 0 = paused; default 1
}
```

## Considered options

- **Plain round-robin**. Simpler by ~10 lines. Rejected because it forces a rewrite (and a re-bench) the day product introduces tiers. WRR-with-uniform-weights is the same simplicity at runtime with the upgrade path already wired.
- **Classic WRR (contiguous runs), default weight = 1**. Cheaper to implement than SWRR by ~5 lines but emits `AAAB AAAB` bursts for `[3,1]`. Rejected because it lengthens worst-case wait for low-weight Customers and re-creates the head-of-line problem WRR was supposed to solve. Same share, worse latency.
- **Smooth WRR (SWRR), default weight = 1** *(chosen)*. Equal share by default, proportional and interleaved share when weights diverge. Per-Customer weight, not per-Delivery — keeps Deliveries uniform-cost and avoids DRR bookkeeping. `batchHint = weight` doubles weight as an amortization knob (higher tier ⇒ larger PG claim batch ⇒ better throughput) without breaking the share invariant. Empirically verifiable in the bench harness with both uniform and non-uniform weight scenarios.
- **DRR (deficit round robin)**. Stronger fairness when per-item cost varies (e.g. payload-size weighted, per-Webhook latency weighted). Adds quantum + deficit bookkeeping per Customer. Deferred — the `Scheduler` interface accommodates the swap. Trigger to flip: bench evidence that Delivery cost variance breaks per-Customer share guarantees.
- **Per-Customer concurrency cap**. Soft fairness via throttling rather than ordering. Rejected as the *primary* mechanism because a Whale can still enqueue unbounded backlog ahead of a Small Customer; throttling slows the Whale but does not interleave Small Customer work. Useful as a complementary safety net under WRR (future work).
- **Static weighted by plan tier in code**. Hard-coded tier→weight map. Rejected: couples scheduler to billing concepts. `WeightSource` abstracts this out — plumbing today, policy when product is ready.

## Consequences

- The `Scheduler` interface stays load-bearing. It must not leak round-robin assumptions (no "next index" in the contract, no Customer ordering in returns).
- The `WeightSource` interface is new but trivial. PoC ships a `ConstantWeights{value: 1}` impl. Production swaps in a DB-backed impl without scheduler changes.
- `weight = 0` is a first-class state meaning "paused" — useful for incident response (throttle a misbehaving tenant without deleting their Webhooks) and for soft-deletes. Schedulers must treat 0-weight Customers as non-pending.
- Live weight changes apply at the next Scheduler cycle. No worker restart.
- Scheduler state is process-local. Postgres `SKIP LOCKED` claims prevent duplicate Delivery processing across horizontally scaled workers, but cluster-wide fairness needs shared claimable Customer discovery so every instance can Notify its local Scheduler from the same source of truth.
- DRR upgrade remains a one-file change with no consumer changes — the goal of the abstraction.
- Bench harness demonstrates fairness empirically (per-Customer-class p50/p95/p99 delivery latency) under (a) uniform weights, mixed Whale/Small load (the assignment's fairness scenario) and (b) non-uniform weights, to prove the share invariant. If the bench shows WRR is insufficient under realistic Delivery cost variance, that is the trigger to flip to DRR — recorded here rather than left as an unspoken assumption.
