# Round-robin fairness with a pluggable Scheduler interface

## Context

The Webhook Notifier must prevent Whale Customers from starving Small Customers when Delivery volume is imbalanced. Deficit Round Robin (DRR) is the textbook answer from network QoS, but DRR's per-item cost weighting is overkill when Deliveries are roughly uniform (one HTTP POST each).

## Decision

Implement **plain round-robin over the set of Customers with pending Deliveries** for the PoC. Hide the policy behind a `Scheduler` interface so DRR (or weighted variants) can be swapped in without touching the WorkerPool, Outbox, or Dispatcher.

The interface is the contract — the implementation is the trade-off.

## Considered options

- **Plain round-robin** *(chosen)*. Equal fairness when Deliveries are uniform cost. ~30 lines. Empirically verifiable in the bench harness.
- **DRR**. Stronger fairness when item cost varies (e.g. payload-size weighted, or per-Webhook latency weighted). Adds quantum + deficit bookkeeping. Documented as the upgrade path.
- **Per-Customer concurrency cap**. Soft fairness via throttling rather than ordering. Rejected because a Whale can still enqueue unbounded backlog ahead of a Small Customer; throttling slows the Whale but does not interleave Small Customer work.
- **Weighted by plan tier**. Out of scope — introduces a billing/plan concept the PoC does not have.

## Consequences

- The `Scheduler` interface is load-bearing for this decision. It must not leak round-robin assumptions (no "next index" in the contract, no Customer ordering in returns). The interface accepts Delivery enqueues and yields the next Customer to serve; the implementation decides how.
- DRR upgrade is a one-file change with no consumer changes — the goal of the abstraction.
- Bench harness must demonstrate fairness empirically (per-Customer-class p50/p95/p99 delivery latency under mixed Whale/Small load). If the bench shows round-robin is insufficient under realistic Delivery cost variance, that is the trigger to flip to DRR — recorded here rather than left as an unspoken assumption.
