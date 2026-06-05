# Claim-agnostic claimable-Customer discovery

## Status

Accepted. Amends [0006](./0006-db-backed-claimable-customer-discovery.md).

## Context

[ADR-0006](./0006-db-backed-claimable-customer-discovery.md) defined DB-backed
discovery and required the discovery query to **match claim eligibility
exactly**: it filtered out Deliveries that already carry an active
`delivery_claims` row (the `dc.delivery_id IS NULL` anti-join) and added a
dedicated stale-claim branch so a Customer whose only ready Delivery was a
stale-leased claim would still be discovered.

[ADR-0011](./0011-delivery-claims-authoritative-claim-store.md) made
`delivery_claims` the authoritative claim store and removed
`deliveries.status = 'in_flight'`. A consequence not fully appreciated in
ADR-0006: **a claimed Delivery keeps `status = 'pending'` (or `'failed_retry'`)
and stays in `idx_deliveries_ready`.** The discovery anti-join against
`delivery_claims` must therefore walk every claimed row to prove it is not
claimable.

Issue #124 reproduced the cost. At a 200k-row backlog, discovery latency was
2.8 ms when the backlog was unclaimed but **206 ms (≈74×) when fully claimed**,
and the loose-index LIMIT-1 scan (finding B-5) collapsed into a full
`Parallel Hash Anti Join` over the entire ready set once unclaimed rows went
sparse. `EXPLAIN (ANALYZE, BUFFERS)` confirmed the plan change. Under a large
in-flight backlog — exactly the high-load condition where discovery runs on
every replica's cadence — discovery became the rank-1 query hotspot
(~1.4 s/call observed in a 16-worker drain).

Re-introducing a claimed marker on the `deliveries` row (so claimed rows leave
the index) was rejected: it is the `status = 'in_flight'` dead-tuple-bloat model
that ADR-0011 deliberately removed.

## Decision

**Discovery is a claim-agnostic rotation hint, not an exact claimability check.**

`DiscoverClaimableCustomers` enumerates distinct Customers that have at least
one **ready, breaker-eligible** Delivery — `status IN ('pending',
'failed_retry') AND next_attempt_at <= now()` plus the breaker predicate —
**ignoring whether those Deliveries already carry a claim**. The
`delivery_claims` anti-join and the dedicated stale-claim branch are removed
from discovery.

`ClaimNextN` / `ClaimNextBatch` with `FOR UPDATE SKIP LOCKED` remain the **sole
authority** on what is actually claimable. The worker loop already tolerates a
discovered Customer yielding no claim: an empty `ClaimNextN` triggers
`HasClaimableWork(Batch)`, which removes the Customer from local rotation.

This keeps the recursive loose-index scan (B-5) able to stop at the first ready
row per Customer, so discovery cost tracks the number of distinct Customers
returned, not the in-flight backlog depth — independent of how many ready rows
are currently claimed.

### Why no discovery coverage is lost

Dropping the stale-claim branch removes no Customer from discovery. Claiming,
marking, and requeue (ADR-0011 worker flow) never push `next_attempt_at` into
the future while a claim is held: `MarkRetry`/`Requeue` delete the claim row
before bumping the schedule. So every actively-claimed or stale-leased Delivery
still satisfies `status IN ('pending','failed_retry') AND next_attempt_at <=
now()` and is already covered by the ready predicate. Stale-claim **recovery**
is unchanged: the claim query's `ON CONFLICT … DO UPDATE WHERE claimed_at <
now() - lease` still reclaims expired leases (ADR-0011).

## Consequences

- Discovery latency is flat with respect to in-flight backlog depth; the #124
  hotspot is removed. A regression test seeds a fully-claimed backlog and
  asserts discovery stays close to the unclaimed baseline.
- Discovery now surfaces Customers whose ready Deliveries are all currently
  claimed (including fresh, non-stale claims). Under a heavy in-flight backlog
  this produces **rotation noise**: a Customer is re-notified, picked, yields an
  empty claim, and is removed — repeatedly across discovery cadences. This is
  bounded (per-Customer `HasClaimableWork` checks are index-scoped, and the
  discovery loop's empty/backoff cadence absorbs it) and is the accepted trade
  for removing the global anti-join walk.
- The signal to watch is the empty-claim rate (`webhook_claim_empty_total`
  against `webhook_claim_batch_size`). A rising empty-claim rate under load
  indicates the in-flight backlog is accumulating because drain rate < arrival
  rate; the remedy is more worker capacity (until `deliveries`-claim
  serialization becomes the ceiling per ADR-0002), not a discovery change.
- The `idx_deliveries_ready_customers (customer_id, next_attempt_at) WHERE
  status IN ('pending','failed_retry')` index from ADR-0006 still backs the
  loose-index scan. `idx_delivery_claims_stale_by_claimed_at` is no longer used
  by discovery (it still serves the claim query's stale-reclaim path).
- ADR-0006's invariant "discovery eligibility must match claim eligibility
  exactly" is superseded by "discovery is a superset hint; the claim query is
  the authority." Correctness is unchanged because `SKIP LOCKED` already
  prevents duplicate processing.

## Considered options

- **Re-add a claimed flag on the `deliveries` row** (`status='in_flight'` or a
  boolean). Fixes discovery, claim, and `HasClaimableWork` walks at once with
  exact semantics, but reintroduces the per-claim `UPDATE` dead-tuple bloat
  ADR-0011 removed, needs aggressive autovacuum on the hottest table, and has a
  large blast radius across claim/mark/reclaim. Rejected.
- **Claim-agnostic discovery** *(chosen)*. One query rewrite, no schema change,
  no claim-model change, no bloat. Trades exact discovery semantics for bounded
  rotation noise that the existing worker loop already handles.
- **Bound the discovery walk** (statement timeout / walk-depth cap). Caps
  worst-case latency without fixing the root cause and can silently miss
  claimable Customers under load, risking starvation. Rejected as a band-aid.
