# Webhook Notifier — Pair Session Presentation

## Slide Deck Overview

| # | Slide | Diagram | Time |
|---|-------|---------|------|
| 1 | Title | — | 1 min |
| 2 | The Problem: Deliver Webhooks, Fairly (hook + roadmap) | — | 2 min |
| 3 | High-Level Architecture | ASCII diagram | 2 min |
| 4 | The Heart: FOR UPDATE SKIP LOCKED | ASCII diagram | 4 min |
| 5 | Delivery Lifecycle | ASCII diagram | 2 min |
| 6 | Solving Fairness: Customer-Based Scheduling | ASCII diagram | 4 min |
| 7 | Solving Scalability: Stateless Replicas | ASCII diagram | 2 min |
| 8 | Solving Reliability: Claim Mechanics | ASCII diagram | 4 min |
| 9 | Now apply the design to real failure modes | — | 2 min |
| 10 | Protect the Integration Partner too | ASCII diagram | 3 min |
| 11 | The claims are measurable, not just architectural | — | 3 min |
| 12 | If this runs in production, Grafana tells us where to look first | — | 2 min |
| 13 | The dashboard becomes a runbook, not a wall of charts | — | 2 min |
| 14 | The roadmap is data-driven: scale only when the signals justify it | — | 2 min |
| 15 | What I shipped matters. What I learned matters more | — | 2 min |

Total target: ~30 min presentation + open discussion. If time is tight, compress slides 5, 7, 9, and 13; protect slides 4, 6, 8, 11, and 15.

## Candidate Positioning

What I want them to remember:

1. I found the non-obvious requirement: fairness is the design center, not an add-on.
2. I chose the simplest durable architecture that proves the assignment: Go + Postgres, no broker.
3. I know where the design breaks: Postgres claim contention, process-local host caps, no strict FIFO, no exactly-once.
4. I measured instead of hand-waving: benchmarks, query plans, stress gates, and regression tests.
5. I can talk about operations and learning, not just code: metrics, dashboards, runbooks, and what I would change next.

Opening thesis:
> "The easy version of this assignment is a webhook retry worker. The hard version is a multi-tenant delivery system where one Whale Customer cannot bury everyone else. I built the hard version, then proved it with benchmarks."

Closing thesis:
> "The core of this design is small: durable work in Postgres, fair Customer scheduling, atomic row claims, and measured production trade-offs. I can defend the choices, show the proof, operate the system, and explain what I learned from building it."

---

## Slide 1 — Title

### Content
- **Webhook Notifier** — Reliable, Scalable, Fair Webhook Delivery
- Multi-tenant SaaS: Events → HTTP POST to Integration Partners
- Go + Postgres | No Broker | Horizontal Workers

### Talking Points
- State the problem — don't read the slide
- Emphasize fairness as the non-obvious challenge driving most design decisions
- "I chose Go + Postgres, no message broker — let me show you why that's enough"
- Candidate signal: I am not just showing features; I am showing the chain from requirement → design choice → proof → trade-off
- Avoid apologizing for PoC scope. Say "intentional scope decision" and immediately name the production path.

### Anticipated Questions
- **Q: Why no Kafka/RabbitMQ/Redis Streams?**
  - A: PoC scope — Postgres as queue keeps the system simple and demonstrates all three challenges. Broker is a documented scale-out path (ADR-0002). Postgres claim contention is the measured ceiling; the benchmark suite tracks it.
- **Q: What's the expected throughput?**
  - A: The assignment-scale stress profile targets roughly 300 events/s. Benchmarks pass at stress profile. Postgres becomes the bottleneck at higher scale — partitioning `deliveries` by `customer_id` is the next step.

---

## Slide 2 — The Problem: Deliver Webhooks, Fairly

### Content

**Three hard problems, one system:**

| Challenge | What makes it hard |
|-----------|-------------------|
| **Reliability** | Accepted Events must never be silently lost. Transient failures need retries. Crashed workers can't leave orphaned Deliveries. |
| **Scalability** | Add workers horizontally. No leader, no state transfer, no migration. Kill a replica, start five new ones. |
| **Fairness** | Whale Customer's burst must not starve Small Customer's Deliveries. This one drives most design decisions. |

**Our approach:** Go + Postgres, no message broker. For this PoC, Postgres row claiming covers the worker-coordination role we would otherwise need a broker or lock service for. Per-Customer scheduling replaces global FIFO. One CTE replaces sweeper processes.

**What we'll prove (roadmap):**
1. Architecture walk — how the pieces fit (slides 3-4)
2. Delivery lifecycle — what happens when an Event arrives (slide 5)
3. Fairness — per-Customer scheduling in action (slide 6)
4. Scalability — stateless replicas, no coordinator (slide 7)
5. Reliability — claim mechanics and crash recovery (slide 8)
6. Failure modes and partner protection — what happens when things go wrong (slides 9-10)
7. Evidence — benchmarks and proof that the claims hold (slide 11)
8. Operations — Grafana and runbook thinking (slides 12-13)
9. Scale-up plan — how I would expand the system with more data (slide 14)
10. Close — what I shipped and what I learned (slide 15)

**Domain vocabulary (quick reference):**

| Term | Meaning |
|------|---------|
| **Customer** | A tenant. The unit of fairness — scheduling decisions happen per-Customer. |
| **Event** | A business fact (`subscriber.created`). Fans out to many Deliveries. |
| **Webhook** | A Customer-owned destination: URL + event type subscription. |
| **Delivery** | One `(Event × Webhook)` pair. The unit of work. |
| **Attempt** | One HTTP POST for a Delivery. Up to N retry Attempts. |

**Delivery guarantee:** At-least-once. Partners deduplicate via `X-Event-Id` and `X-Delivery-Id` headers.

### Talking Points
- Lead with the three challenges — audience feels the problem before the solution
- Vocabulary defined organically as we go; the table is reference, not lecture
- The roadmap gives them a mental itinerary — they know where we're going
- "Go + Postgres, no broker" is the hook — say it with conviction
- At-least-once stated upfront — no ambiguity about the guarantee
- Strongest personal angle: "I treated fairness as a first-class system property, then let that drive the scheduler, data model, benchmarks, and production trade-offs."

### Anticipated Questions
- **Q: Why not exactly-once?**
  - A: Exactly-once across network boundaries requires two-phase commit or partner cooperation. At-least-once with idempotency headers is the industry standard (Stripe, GitHub, Shopify all do this).
- **Q: Why not just use a message broker?**
  - A: Postgres is already the system of record. Using it as queue avoids broker operational overhead for evaluation phase. Scale-out path documented in ADR-0002.
- **Q: What's the difference between a Webhook and an Attempt?**
  - A: Webhook = destination config (URL + event types). Attempt = one HTTP POST to that URL for a specific Delivery. A Webhook has many Deliveries over time; each Delivery has one or more Attempts.

---

## Slide 3 — High-Level Architecture

### ASCII Diagram

```
╔══════════════════════════════════════════════════════════════════════════╗
║   HIGH-LEVEL ARCHITECTURE                                               ║
║                                                                          ║
║   ┌──────────┐    ┌──────────────────────────────────────────────┐      ║
║   │          │    │                 NOTIFIER                      │      ║
║   │  Events  │───►│                                              │      ║
║   │  (HTTP)  │    │  ┌──────────┐  ┌──────────┐  ┌──────────┐   │      ║
║   │          │    │  │  Ingest  │  │ Scheduler│  │  Worker  │   │      ║
║   └──────────┘    │  │  (fan-   │─►│(round-   │─►│  Pool    │───┼──►   ║
║                   │  │   out)   │  │  robin)  │  │(claim+   │   │ POST ║
║                   │  └──────────┘  └──────────┘  │  POST)   │   │      ║
║                   │                              └──────────┘   │      ║
║                   └───────────┬──────────────────────────────────┘      ║
║                               │                                        ║
║                               ▼                                        ║
║                    ┌──────────────────┐                                ║
║                    │    Postgres      │                                ║
║                    │  (deliveries,    │                                ║
║                    │   delivery_claims,│                                ║
║                    │   webhooks)      │                                ║
║                    └──────────────────┘                                ║
║                                                                          ║
╚══════════════════════════════════════════════════════════════════════════╝
```

### Talking Points
- Walk left to right: Producer → Ingest → Scheduler → Workers → Partners
- Postgres is both queue AND coordination point — no broker
- Everything inside the dashed box is process-local and rebuildable — kill a replica, nothing is lost
- PgBouncer sits between notifier and Postgres for connection pooling
- Two distinct table roles: events table (INSERT-only, append log) and deliveries table (claimed via SKIP LOCKED)
- "SKIP LOCKED is the core mechanism that makes all of this work — let me show you"
- Keep this slide short. The goal is orientation, not proof. The proof starts on slide 4.

### Anticipated Questions
- **Q: How does ingest fanout work?**
  - A: SubscriptionRegistry (LRU cache, 30s TTL, singleflight miss coalescing) maps `(customer_id, event_type) → []Webhook`. One transaction writes the event + all delivery rows. Synchronous fanout — async considered but not needed at PoC scale.
- **Q: What if ingest is slow under high fanout?**
  - A: Tracked with `webhook_ingest_fanout_latency` metric. If p99 exceeds 200ms, that's the signal to consider async fanout. Haven't hit it yet.

---

## Slide 4 — The Heart: FOR UPDATE SKIP LOCKED

### ASCII Diagram

```
╔══════════════════════════════════════════════════════════════════════════╗
║   FOR UPDATE SKIP LOCKED                                                ║
║                                                                          ║
║   ┌──────────────────┬──────────────────────────────────────────┐       ║
║   │  Approach        │  Result                                  │       ║
║   ├──────────────────┼──────────────────────────────────────────┤       ║
║   │  SELECT          │  Both workers get D-1 → double delivery  │       ║
║   │  FOR UPDATE      │  Correct but Worker B waits for A        │       ║
║   │  SKIP LOCKED     │  Both work, no wait, no duplicate        │       ║
║   └──────────────────┴──────────────────────────────────────────┘       ║
║                                                                          ║
║   One CTE, three things at once:                                        ║
║   1. SELECT ... FOR UPDATE SKIP LOCKED  → non-blocking claim           ║
║   2. INSERT INTO delivery_claims       → ownership in separate table   ║
║   3. ON CONFLICT WHERE lease expired   → crash recovery, no sweeper    ║
║                                                                          ║
╚══════════════════════════════════════════════════════════════════════════╝
```

### Content

**The core question:** How do multiple workers claim deliveries from Postgres without duplicates, without blocking, and without an external lock service?

**Three approaches, one winner:**

| Query | Behavior |
|-------|----------|
| Plain SELECT | Both workers get D-1 → duplicate delivery. Broken. |
| FOR UPDATE | Correct but Worker B blocks waiting for A. Serialized, can't scale. |
| **FOR UPDATE SKIP LOCKED** | B sees D-1 locked, skips to D-2. Both work in parallel. No duplicates, no blocking. |

**This single SQL primitive gives us:**
- **Fairness** — Scheduler picks which Customer's deliveries to claim; SKIP LOCKED ensures workers don't collide
- **Scalability** — Add more workers, they skip each other's locked rows. No leader election, no partitioning
- **Reliability** — Combined with `ON CONFLICT DO UPDATE WHERE claimed_at < lease`, stale claims from crashed workers are recovered atomically in the same query

**What you'd build without SKIP LOCKED:**

| Alternative | Complexity Added |
|-------------|-----------------|
| Kafka consumer groups | Partition assignment, rebalancing, consumer offset management, at-least-once via offset commits |
| Redis distributed locks | Lock service availability, lock expiry tuning, fencing tokens, STALE read risk |
| External queue (RabbitMQ/Streams) | New infrastructure, operational overhead, exactly-once semantics at protocol level vs practical delivery |
| Naive `SELECT ... FOR UPDATE` | Correct but serialized — workers block, throughput collapses under concurrency |

SKIP LOCKED eliminates the worker-claim coordination problem for this design. It does not replace broker features like replay, partition ownership, retention policy, or cross-service event distribution. The system has no external queue, no lock service, no partition coordinator. Postgres *is* the queue for this PoC.

**Two table roles, two strategies:**

| Table | Strategy | Why |
|-------|----------|-----|
| `events` | INSERT-only (append log) | Consumers track position. No DELETEs → no dead tuples → no MVCC pressure from consumption. |
| `deliveries` | SKIP LOCKED claiming | Workers claim and release rows. Mutations bounded by worker count, not event rate. |

Available in Postgres 9.5+, MySQL 8.0+, Oracle. Standard SQL row-level locking.

### Talking Points
- This is the architectural decision everything else builds on
- Show the three-query comparison — it's immediately convincing
- SKIP LOCKED covers the worker-claim coordination role that might otherwise require broker consumer groups or distributed locks
- Hybrid design: INSERT-only where volume is highest (events), SKIP LOCKED where coordination is needed (claims)
- "Now let me show how this flows through a delivery's lifecycle"

### Anticipated Questions
- **Q: What if all rows are locked?**
  - A: Query returns empty set. Worker asks scheduler for next customer. No spinning.
- **Q: If deliveries use SKIP LOCKED with UPDATEs, don't you have MVCC issues?**
  - A: Mutation rate on deliveries is bounded by worker concurrency (tens of rows/sec), not event ingestion rate (thousands/sec). Autovacuum handles this comfortably. The pathology hits when mutation rate matches ingestion rate — which is what pure SKIP LOCKED queues do.
- **Q: Have you benchmarked the held-xmin scenario?**
  - A: The committed benchmark evidence points to claim contention and empty-claim churn as the practical ceiling, not consumer-side MVCC churn on the append-only `events` table. I would not claim an exact CPU number without the run artifact in front of me.
- **Q: Could you skip SKIP LOCKED entirely with cursor-based consumption?**
  - A: For event reading, yes — and that's what we do. For delivery claiming, you need mutual exclusion between workers. SKIP LOCKED is the lightest mechanism that provides it without an external lock service.

---

## Slide 5 — Delivery Lifecycle

### ASCII Diagram

```
╔══════════════════════════════════════════════════════════════════════════╗
║   DELIVERY LIFECYCLE                                                    ║
║                                                                          ║
║   Event arrives ──► Fan-out creates Delivery rows (state=pending)       ║
║                          │                                               ║
║                          ▼                                               ║
║   Worker ──► SKIP LOCKED ──► claims 20 deliveries for that customer     ║
║        │                                                                ║
║   ┌────┴──────────────┐                                                 ║
║   │  POST to webhook  │                                                 ║
║   └────┬──────────────┘                                                 ║
║        │                                                                ║
║   ┌────┴─────────┬────────────┐                                         ║
║   ▼              ▼            ▼                                         ║
║  2xx            4xx         5xx/timeout                                 ║
║  delivered  permanent_fail    attempt recorded → failed_retry           ║
║                               backoff → claimable again                 ║
║                                                                          ║
╚══════════════════════════════════════════════════════════════════════════╝
```

### Talking Points
- Happy path: pending → claimed → success
- Retry loop: claimed → failed_retry → (backoff) → claimable again
- Terminal: claimed → dead → archived to `delivery_history` (time-partitioned)
- "Claimed" is NOT a column — it's a row in `delivery_claims`. The `deliveries.status` stays pending or failed_retry; ownership lives in the separate table
- Stale claim recovery is for crashed workers, not normal retry. No sweeper process: the claim CTE detects and re-claims atomically — zero extra infrastructure
- "Now that you see the lifecycle, let me show how SKIP LOCKED solves each of the three challenges"

### Anticipated Questions
- **Q: Why separate delivery_claims table instead of a status column?**
  - A: ADR-0011. A status column means every claim/release is an UPDATE on the deliveries row — hot row contention. Separate claims table + INSERT/DELETE keeps the deliveries row stable during in-flight work.
- **Q: How long before a stale claim is recovered?**
  - A: Configurable claim lease, default 5 minutes. No timer — next claim CTE detects and atomically re-claims in the same query.
- **Q: What about delivery ordering?**
  - A: Per-customer queues drain in arrival order single-threaded, but with multiple workers some interleaving is possible. Documented non-goal. Partners dedupe via idempotency headers.

---

## Slide 6 — Solving Fairness: Customer-Based Scheduling

### ASCII Diagram

```
╔══════════════════════════════════════════════════════════════════════════╗
║   CUSTOMER-BASED SCHEDULING                                             ║
║                                                                          ║
║   Claim CTE drains ONE customer at a time                                ║
║                                                                          ║
║   Workers:                                    Deliveries queue:          ║
║   ┌──────┐                                    ┌──────────────────┐      ║
║   │ W-1  │──── Customer A (20 deliveries) ───►│  CUSTOMER A      │      ║
║   └──────┘                                    │  10,000 pending  │      ║
║   ┌──────┐                                    ├──────────────────┤      ║
║   │ W-2  │──── Customer B (20 deliveries) ───►│  CUSTOMER B      │      ║
║   └──────┘                                    │  5 pending       │      ║
║   ┌──────┐                                    ├──────────────────┤      ║
║   │ W-3  │──── Customer C (20 deliveries) ───►│  CUSTOMER C      │      ║
║   └──────┘                                    │  3 pending       │      ║
║                                               └──────────────────┘      ║
║   Big Customer A cannot drown out B or C — each worker picks            ║
║   a different customer, claims a batch, delivers, then picks again      ║
║                                                                          ║
║   CROSS JOIN LATERAL customer_discovery → FOR UPDATE ... SKIP LOCKED    ║
║                                                                          ║
╚══════════════════════════════════════════════════════════════════════════╝
```

### Content

**The insight:** The unit of fairness is the *Customer*, not the *Delivery*.

| Approach | Behavior under whale burst |
|----------|---------------------------|
| Global FIFO | Whale has 10,000 events → small customer waits behind all of them |
| **Customer round-robin** | Scheduler iterates over *customers*. Each gets equal turns regardless of backlog depth. |

**How SKIP LOCKED enables this:** Scheduler picks Customer A, worker claims Customer A's deliveries with SKIP LOCKED. Meanwhile another worker claims Customer B's deliveries. No collision, no coordination — SKIP LOCKED handles the mutual exclusion.

**Concrete narrative:**
> Whale Customer imports 10,000 subscribers. Each triggers `subscriber.created` fanned out to 3 webhooks → 30,000 pending deliveries. *At the same time*, Small Customer creates 5 events → 15 deliveries.
>
> **Without per-Customer scheduling:** Worker A claims 20 deliveries from the global queue. Worker B claims 20 more. Both get Whale deliveries. Small's 15 deliveries sit behind all 30,000.
>
> **With per-Customer scheduling:**
> - Scheduler assigns Customer A (Whale) → Worker A claims 20
> - Scheduler assigns Customer B (Small) → Worker B claims 20
> - Every cycle, Small gets at least one worker turn — regardless of Whale's backlog depth
>
> Result: Small's deliveries complete in seconds, not hours. Whale's throughput scales with available workers, not by starving peers.

**Cross-instance fairness:** Each replica has its own scheduler. DB-backed discovery converges toward the shared claimable set, and FOR UPDATE SKIP LOCKED is the actual deduplication. Approximate, not globally serialized — documented trade-off.

### Talking Points
- This is the core design insight — use the diagram, it makes it immediately clear
- Whale has more work queued but doesn't get more scheduling capacity — turns, not volume
- Small customer's P95 first-attempt latency stays bounded relative to whale's, even under burst
- Default is plain round-robin. SWRR (Smooth Weighted Round Robin) available for differentiated shares — `customers.class` column is the weight
- Claim batch size is independent from weight — weight controls frequency, batch size controls DB amortization
- Fairness benchmark: compare Small P95 vs Whale P95 ratio under mixed load — stress profile PASS

### Anticipated Questions
- **Q: How do you measure fairness?**
  - A: `delivery_attempts.sent_at - deliveries.created_at` for `attempt_number = 1`. Compare Small P95 vs Whale P95 under mixed load. Comparative metric, not absolute SLO — absolute needs real traffic patterns.
- **Q: What if a customer has 100 webhooks per event?**
  - A: More webhooks = more deliveries, but NOT more scheduling capacity. Webhook fanout increases backlog, not share. Deliberate.
- **Q: Can a customer be paused?**
  - A: `customers.class = 0` → SWRR never picks them. Default round-robin ignores class entirely.
- **Q: Cross-instance fairness?**
  - A: Each replica has its own scheduler. DB-backed discovery converges each process toward the shared claimable set. FOR UPDATE SKIP LOCKED is the actual deduplication. Approximate, not globally serialized — documented trade-off.

---

## Slide 7 — Solving Scalability: Stateless Replicas

### ASCII Diagram

```
╔══════════════════════════════════════════════════════════════════════════╗
║   STATELESS REPLICAS                                                    ║
║                                                                          ║
║                        ┌─────────────┐                                  ║
║         ┌──────────────│  Postgres   │──────────────┐                   ║
║         │              └─────────────┘              │                   ║
║         ▼                                           ▼                   ║
║   ┌──────────┐                                ┌──────────┐              ║
║   │ Replica 1│                                │ Replica 2│              ║
║   ├──────────┤                                ├──────────┤              ║
║   │ Scheduler│  same code                     │ Scheduler│  same code   ║
║   │ Workers  │  same CTE                      │ Workers  │  same CTE    ║
║   │ Ingest   │  no coordinator                │ Ingest   │  no coord.   ║
║   └──────────┘                                └──────────┘              ║
║                                                                          ║
║   Add replicas → more parallel SKIP LOCKED claimers                     ║
║   No leader election. No partition assignment.                          ║
║   Postgres is the state. Replicas are ephemeral.                        ║
║                                                                          ║
╚══════════════════════════════════════════════════════════════════════════╝
```

### Content

**How SKIP LOCKED enables this:** Every replica runs the same code, issues the same claim CTE. SKIP LOCKED means they never collide — add replicas, throughput scales. No leader election, no partition assignment, no state transfer.

**Concrete narrative — adding a replica:**
> You have 2 replicas. Throughput is good but latency is creeping up under load. You start a 3rd replica:
> 1. Replica 3 boots, connects to Postgres via PgBouncer, starts its scheduler
> 2. DB-backed discovery finds customers with claimable work within ~1s
> 3. Replica 3 issues the same claim CTE as replicas 1 and 2
> 4. SKIP LOCKED ensures all three claim different rows — no collision, no coordination
> 5. Ingest load balances across all three (Caddy round-robin)
>
> No partition reassignment. No rebalancing event. No state migration. Downtime: zero.

**What resets when a replica dies:**
- Scheduler membership (rebuilds from DB discovery next cycle)
- HostLimiter counters (process-local, resets to zero — safe, cap is per-process)
- Breaker in-memory decisions (re-reads from `webhooks.breaker_state` on first access)

**What survives:**
- In-flight claims on that replica expire via lease (5 min default)
- All other replicas continue unaffected — discovery is claim-agnostic
- Expired claims are re-claimed by any replica atomically (no sweeper needed)

### Talking Points
- Every replica is identical — kill one, start five new ones
- Scheduler membership rebuilds from DB discovery, HostLimiter counters reset, breaker decisions rebuild from persisted webhook state
- Coordination through Postgres: SKIP LOCKED for claims, no leader election
- Wakeups: successful ingest wakes local scheduler synchronously, pg_notify hints remote replicas, periodic DB discovery is the fallback
- PgBouncer + prepared statements for connection efficiency

### Anticipated Questions
- **Q: What's the bottleneck?**
  - A: Postgres `deliveries` claim contention. Measured in benchmarks. Next step: partition by `customer_id`. After that: Redis Streams or Kafka (but that rewrites outbox/claim/discovery/ingest — scheduler interface stays).
- **Q: How many replicas tested?**
  - A: Scalability stress runs real notifier OS processes at 1, 2, and 4 replicas against one shared Postgres database — PASS. Claim deduplication via SKIP LOCKED scales with replica count. Discovery convergence time increases slightly.
- **Q: PgBouncer setup?**
  - A: Transaction pooling with `MAX_PREPARED_STATEMENTS` non-zero. Hot claim and mark CTEs avoid repeated parse/plan work. ADR-0012.
- **Q: How does discovery work across replicas?**
  - A: ADR-0006 + ADR-0013. DB query finds customers with claimable deliveries. Claim-agnostic — doesn't join delivery_claims (keeps latency flat). Workers check HasClaimableWork after empty claims to prune false positives.
- **Q: What happens on Postgres restart or failover?**
  - A: All workers lose connections, reconnect via PgBouncer. In-flight claims expire (lease-based). No data loss — pending deliveries are re-claimable once DB is back. Recovery time = reconnect + one claim cycle.

---

## Slide 8 — Solving Reliability: Claim Mechanics

### ASCII Diagram

```
╔══════════════════════════════════════════════════════════════════════════╗
║   CLAIM CTE — ONE QUERY, THREE THINGS                                   ║
║                                                                          ║
║   Discover customer → Select delivery batch → Claim ownership           ║
║                                                                          ║
║   WITH customer AS (                                                     ║
║     SELECT c.id FROM customer_discovery()                                ║
║   ), batch AS (                                                          ║
║     SELECT d.id FROM deliveries d                                        ║
║     WHERE d.customer_id = (SELECT id FROM customer)                      ║
║     AND d.status = ANY(claimable_states)                                 ║
║     ORDER BY d.created_at                                                ║
║     LIMIT 20                                                             ║
║     FOR UPDATE OF d SKIP LOCKED               ── 1. non-blocking lock   ║
║   ), claim AS (                                                          ║
║     INSERT INTO delivery_claims (...)                                     ║
║     SELECT ... FROM batch                    ── 2. claim ownership     ║
║     ON CONFLICT (delivery_id) WHERE claimed_at < lease                  ║
║     DO UPDATE SET claimed_at = now()           ── 3. crash recovery    ║
║   )                                                                      ║
║   SELECT ... FROM batch;                                                ║
║                                                                          ║
║   No sweeper. No coordinator. One query.                                ║
║                                                                          ║
╚══════════════════════════════════════════════════════════════════════════╝
```

### Content

**The problem:** Multiple workers race for deliveries. Need: no duplicates, no blocking, crash recovery without a sweeper.

**The solution:** A single CTE that does three things atomically:
1. Select candidates with `FOR UPDATE SKIP LOCKED` — losers skip, not block
2. INSERT into `delivery_claims` — records ownership
3. `ON CONFLICT DO UPDATE WHERE claimed_at < lease` — recovers stale claims from crashed workers

One query. No coordinator. No timer. No sweeper process.

**Why a separate `delivery_claims` table (ADR-0011):**
A naive design uses `deliveries.status = 'in_flight'`. Problem: every claim/release is an UPDATE on the deliveries row — hot row contention. By separating ownership into a dedicated table, INSERT (claim) and DELETE (release) keep `deliveries` stable during in-flight work. Mutations on `deliveries` stay bounded by delivery lifecycle, not worker concurrency.

**The claim CTE composes three primitives into one architectural win:**
- `FOR UPDATE SKIP LOCKED` — non-blocking mutual exclusion (not a queue primitive)
- Separate `delivery_claims` table — avoids hot updates on deliveries (ADR-0011)
- `ON CONFLICT ... WHERE claimed_at < lease` — stale crash recovery embedded in the claim, no sweeper, no timer (ADR-0002)

That third one is easy to miss — it means crashed workers are recovered in the *next* claim cycle, not after a timeout process scans for orphans. Zero extra infrastructure.

### Deadlock Story (PR #154, ADR-0014)

**The bug:** Concurrent claim + breaker `persistOpen` deadlocked (`SQLSTATE 40P01`). Claim path locked `deliveries` first, then `delivery_claims`. But `persistOpen` locked `webhooks` (UPDATE breaker_state), then `deliveries` (deferBlockedDeliveries) — opposite lock order within the same transaction.

**The fix:** Split `persistOpen` into two standalone transactions:
- Phase 1: UPDATE `webhooks` only (commits immediately)
- Phase 2: Defer blocked deliveries (no `webhooks` lock)

Since Phase 1 commits before Phase 2 starts, there is no lock-order cycle with worker claims. Deadlock impossible by construction, not by timing.

**The pattern:** One transaction touched two lock domains → split them → deadlock eliminated. Enforced as a project-wide rule: lock ordering is always `deliveries` first, then `delivery_claims`.

### Discovery Optimization: Loose Index-Scan

**The problem:** `DiscoverClaimableCustomers` used `UNION/DISTINCT` which materialized ALL ready rows before the outer LIMIT. Latency grew linearly with backlog — ~100× at 100k rows. Every replica paid this on every discovery cadence (1s default).

**The fix:** Recursive CTE loose index-scan — each step jumps to the next distinct customer via `WHERE customer_id > $prev ... LIMIT 1`. Cost tracks *customers returned*, not backlog size.

**The complication (ADR-0013):** After ADR-0011, claimed deliveries stay `status='pending'` and remain in the ready index. The original loose index-scan fix still had an anti-join against `delivery_claims`, which collapsed the loose scan into a full `Parallel Hash Anti Join` under heavy in-flight backlog (2.8ms → 206ms at 200K rows).

**Final fix:** Discovery is claim-agnostic — a superset hint, not exact claimability. The anti-join is removed. `ClaimNextN` with `SKIP LOCKED` is the sole authority. Tradeoff: bounded rotation noise instead of a global scan.

**Proof:** `TestDiscoverClaimableCustomersDoesNotScanFullBacklog` — 1000× backlog growth, same latency, EXPLAIN ANALYZE captured in test output.

### Talking Points
- Walk through the race: Worker A and B both issue claim CTE simultaneously
- SKIP LOCKED = optimistic, non-blocking — loser instantly moves to next row
- ON CONFLICT = fresh claims and crash recovery in the same atomic statement
- Lock ordering: always deliveries first, then delivery_claims — prevents deadlocks (ADR-0014, learned from PR #154)
- The deadlock wasn't a code bug — it was an architecture bug. One transaction touching two lock domains. Split them.
- This is why we don't need a separate lock service or coordination layer

### Anticipated Questions
- **Q: What if all rows are locked?**
  - A: Claim returns empty. Worker calls HasClaimableWork, removes customer from rotation. DB-backed discovery re-adds when work becomes claimable.
- **Q: How does batch claiming work?**
  - A: MultiPickCount > 1 → collect N customers from scheduler, single ClaimNextBatch call, one DB round-trip amortized. Critical for sparse-customer workloads.
- **Q: Have you seen deadlocks?**
  - A: Yes — PR #154. The fix wasn't a retry loop (band-aid). It was splitting `persistOpen` into two standalone transactions so lock-order conflict is structurally impossible. The system has a lock-ordering rule now: deliveries first, delivery_claims second, webhooks in a separate transaction.
- **Q: What's the claim lease duration?**
  - A: Default 5 minutes. Tunable. Should exceed max request timeout + mark transaction time.
- **Q: Why not use a status column on deliveries instead of a separate claims table?**
  - A: ADR-0011. Status column = hot row — every claim/release UPDATES the row. Separate claims table = INSERT/DELETE — `deliveries` stays stable during in-flight work. Also makes stale recovery cleaner: stale claim row, not stale status that needs reverting.

---

## Slide 9 — Now apply the design to real failure modes.

### Content

**Reliability in practice:** three common failure modes, one design.

| Failure mode | What happens |
|--------------|--------------|
| Worker dies mid-delivery | Claim lease expires and another worker reclaims the Delivery atomically. |
| Partner endpoint fails | Attempt is recorded, classified, and retried with bounded backoff. |
| Whale Customer floods the queue | Scheduler keeps rotating by Customer, so Small Customers still get turns. |

**The point:** no silent loss, no duplicate claim ownership, and no fairness collapse under burst load.

### Talking Points
- This is the bridge from architecture to reality: failures happen, and the system keeps moving.
- Worker death is a lease problem, not a data-loss problem.
- Partner failure is contained by retry, breaker, and limiter.
- Whale bursts are handled by the scheduler, not by hoping the queue is kind.

### Anticipated Questions
- **Q: What is the main takeaway of this slide?**
  - A: The system is designed to fail in bounded ways: worker death, partner failure, or tenant burst do not become silent correctness problems.
- **Q: Why put this before partner protection?**
  - A: It sets up the next slide. First I show the failure modes, then I show the controls that handle them.
- **Q: Where does fairness show up here?**
  - A: In the scheduler. Reliability is not enough if one tenant can monopolize progress.

---

## Slide 10 — Protect the Integration Partner too

### ASCII Diagram

```
╔══════════════════════════════════════════════════════════════════════════╗
║   PARTNER PROTECTION — THREE LAYERS                                     ║
║                                                                          ║
║   ┌─────────────────────────────────────────────────────────────────┐   ║
║   │  RETRY POLICY         Per-delivery                              │   ║
║   │  2xx → done    5xx → retry    4xx → DLQ    429 → respect       │   ║
║   │  Decorrelated jitter — avoids thundering herd on recovery       │   ║
║   └─────────────────────────────────────────────────────────────────┘   ║
║   ┌─────────────────────────────────────────────────────────────────┐   ║
║   │  CIRCUIT BREAKER      Per-webhook                               │   ║
║   │  20 consecutive fails → open. 5 min → probe. Probe fails → 15m │   ║
║   │  In-memory decisions. Persisted transitions (cluster-wide).     │   ║
║   └─────────────────────────────────────────────────────────────────┘   ║
║   ┌─────────────────────────────────────────────────────────────────┐   ║
║   │  HOST LIMITER          Per-hostname, process-local              │   ║
║   │  20 concurrent attempts. Non-blocking: cap miss = requeue.      │   ║
║   └─────────────────────────────────────────────────────────────────┘   ║
║                                                                          ║
║   Requeue ≠ Retry: if limiter full or coordination loss before POST,   ║
║   delivery is requeued with short delay — no attempt recorded.          ║
║   Partner never saw the request.                                        ║
║                                                                          ║
╚══════════════════════════════════════════════════════════════════════════╝
```

### Content

**Three layers of partner protection:**

| Layer | Scope | Mechanism |
|-------|-------|-----------|
| **Retry Policy** | Per-delivery | Decorrelated jitter backoff. 2xx=done, 5xx/timeout=retry, 4xx=DLQ. 429 respects Retry-After. |
| **Circuit Breaker** | Per-webhook | 20 consecutive failures → open. 5 min cooldown → single probe. Probe succeeds → close. Fails → backoff to 15 min. |
| **Host Limiter** | Per-hostname | 20 concurrent attempts per process. TryAcquire is non-blocking — cap miss requeues, worker stays productive. |

**Key design decision:** Breaker decisions are in-memory, but transitions are persisted so all replicas filter the same open webhooks.

**Important distinction:** Requeue is not retry. If the limiter is full before POST, the Delivery is requeued with a short delay. No attempt recorded.

### Talking Points
- The partner needs protection too; otherwise the system becomes noisy and brittle.
- Retry handles transient failure, breaker handles persistent failure, limiter protects host capacity.
- This is about keeping the worker pool productive under bad downstream conditions.

### Anticipated Questions
- **Q: Why per-webhook and not just per-host?**
  - A: One host can serve many independent webhooks. A broken endpoint should not suppress healthy ones on the same host.
- **Q: What happens when coordination is lost before POST?**
  - A: The Delivery is requeued, not retried. The partner never saw the request, so retry budget should not burn.

---

## Slide 11 — The claims are measurable, not just architectural.

### Content

**The benchmark suite is the proof surface.**

| Area | Stress volume | Gate / observed result |
|------|---------------|------------------------|
| Reliability | 1,000 Events -> 3,000 Deliveries; plus 100 terminal-4xx Events | 3,000 / 3,000 terminal; 740 / 740 recovered; 0 retry waste on bad Webhook |
| Scalability | 4 real notifier replicas; 60,000 sparse + 100,000 dense Deliveries | 0 duplicate tuples; dense throughput 6,192 -> 20,506 Deliveries/s from 1 -> 4 replicas |
| Fairness | 135,000 Deliveries; 20,000 Small Customers + 1 Whale | Small P95 first-attempt latency stays ahead of Whale; PASS at 16/32/64 Workers |
| Partner protection | Dead Integration Partner + healthy Customers; 1,000 Deliveries per Customer | Healthy p95 ratio 1.03x vs control; breaker opened and re-opened; goroutines stable 5 -> 5 |

**What this says in the interview:** I did not stop at a nice architecture diagram. I measured the failure modes, the throughput ceiling, and the fairness gate.

### Talking Points
- Benchmarking is not bonus material here; the brief asked for repeatable evidence.
- The fairness number is the one to remember: 20k Small Customers plus 1 Whale.
- The scaling number is the other one to remember: 1 replica to 4 replicas, with throughput going from 6,192 to 20,506 Deliveries/s.
- The partner protection line matters because it proves the system stays calm under failure.

### Anticipated Questions
- **Q: Why quote stress numbers on the slide?**
  - A: Because the audience should see the gate, not just be told it passed.
- **Q: What is the hardest metric here?**
  - A: Fairness. It needs a comparative gate, not a single absolute threshold.
- **Q: Why put the partner failure case next to fairness?**
  - A: Because both are about protecting a tenant from a noisy neighbor, just at different boundaries.

---

## Slide 12 — If this runs in production, Grafana tells us where to look first.

### Content

**Dashboard groups**
- Overview: `Terminal Success Rate`, `Throughput by Customer Class`, `Queue Wait P95`
- Reliability: `DLQ Rate`, `Attempt Outcome Breakdown`, `Oldest Pending Delivery`
- Fairness: `Delivery Latency P95`, `POST Duration P95`, `Active Customers`
- Scalability: `In-flight Deliveries`, `instances/workers`, `Claim Duration`, `Empty Claim Rate`
- Database / wakeups: `Claim Batch Size`, `Database Pool`, `Remote Wake Hints`, `Scheduler Pick Duration`
- Partner protection: `Host Limiter Wait`, `Circuit Breaker State`, `Trips`, `Sync Fallback`

**What I look at first:** queue wait, backlog age, DLQ rate, empty claims, breaker growth.

### Talking Points
- These names line up with `compose/grafana/dashboards/notifier.json`.
- The dashboard is grouped by diagnosis question, not by implementation detail.
- Grafana should answer “where do I look first?” before it answers anything else.

### Anticipated Questions
- **Q: Why group the panels this way?**
  - A: So ops can work from symptoms, not from code structure.
- **Q: What is the first signal you would inspect?**
  - A: Small Customer queue wait, then backlog age and DLQ rate.

## Slide 13 — The dashboard becomes a runbook, not a wall of charts.

### Content

| Symptom | What I inspect | What I infer |
|---------|----------------|--------------|
| Small Customer P95 queue wait rises | Active Customers, Claim Duration, Empty Claim Rate | Fairness regression, DB pressure, or stale discovery |
| Oldest pending age climbs | Terminal Success Rate, DLQ Rate, Attempts per Delivery | Work is stuck or retry-amplified |
| Partner panels light up | Host Limiter Wait, Breaker State, Trips | Platform overload or one partner is having a bad day |

**The ops loop:** symptom → panel → hypothesis → tuning change.

### Talking Points
- This is the transition from “it works” to “I know where to look when it doesn’t.”
- The dashboard is a runbook, not decoration.
- I want the narrative to feel operational, not theoretical.

### Anticipated Questions
- **Q: Why do you need both slide 12 and 13?**
  - A: One names the panels. The other shows how I would use them.
- **Q: What is the main operational signal?**
  - A: Small Customer P95 queue wait.

## Slide 14 — The roadmap is data-driven: scale only when the signals justify it.

### Content

**Scale-up plan**
1. Tune worker count, batch size, and discovery cadence from production traffic shape.
2. Use Small Customer P95 queue wait as the fairness guardrail.
3. Use claim latency and empty-claim rate to detect the Postgres ceiling.
4. Use host pressure and breaker-open counts to separate partner pain from platform load.

**Next upgrades**
- Encrypt secrets at rest.
- Add a distributed HostLimiter when partner pressure spans replicas.
- Partition deliveries by `customer_id` when claim contention becomes dominant.
- Move to Redis Streams or Kafka only after measured throughput needs exceed Postgres.

### Talking Points
- This slide is not an apology slide. It is a measured growth plan.
- The next step is usually a tuning change or a bounded expansion, not a rewrite.
- The broker path is there, but it is a threshold decision, not the default.

### Anticipated Questions
- **Q: What would you change first?**
  - A: Encrypt secrets at rest, then distributed HostLimiter.
- **Q: When do you move off Postgres?**
  - A: When the measured ceiling is clearly the bottleneck.

## Slide 15 — What I shipped matters. What I learned matters more.

### Content

**Shipping receipt**
- 34 Go test files
- 215 test / benchmark entrypoints
- 67 closed GitHub issues
- 81 merged PRs

**What I learned**

| What I learned | How it changed the system |
|----------------|---------------------------|
| Fairness has to be designed in early | The scheduler, claim path, benchmark suite, and metrics all use Customer as the fairness unit. |
| Load tests teach different lessons than unit tests | Stress runs exposed claim contention, empty-claim churn, and the real Postgres ceiling. |
| Operational metrics must answer diagnosis questions | Dashboards separate scheduler pressure, partner failure, and platform overload. |
| Simple architecture still needs proof | Go + Postgres is defensible because it has ADRs, tests, benchmarks, and known production gaps. |

### Talking Points
- Close on learning, not just output.
- TDD and benchmarks were how the design got steered.
- The biggest lesson is fairness: it has to be designed into the scheduler, data model, metrics, and tests.
- Final line: I can defend the architecture, show the proof, operate the system, and explain what I learned.

### Anticipated Questions
- **Q: Why close on learning?**
  - A: Because it shows judgment, not just implementation volume.
- **Q: What should they remember from the closing slide?**
  - A: The work shipped, but the engineering maturity is what matters.

## General Session Prep

### Questions YOU should ask them
- What scale are you targeting? (informs Postgres ceiling discussion)
- Any existing event bus or outbox pattern in the platform?
- Multi-region requirements?
- Partner SLA expectations for delivery latency?

### Presenter mode
- Press `S` in the deck to open presenter notes in a separate browser window or tab.
- Keep the audience deck clean; notes are only visible in presenter mode.
- The live deck now uses fewer animation steps, so do not try to narrate every reveal.

### If they push on weaknesses
- Acknowledge the Postgres ceiling honestly — it's measured
- Frame PoC choices as intentional scope decisions, not ignorance
- Point to ADRs for documentation of alternatives considered
- Show benchmark evidence rather than theoretical claims

### If they ask about AI usage
- "I used AI as an implementation accelerator, but I own the architecture, trade-offs, tests, and code review. I can walk through the hot paths and explain the why behind each major decision. The best examples are the deadlock fix, the loose index-scan optimization, and the breaker ballast elimination: each started from a measured failure mode, not from generated code."

### Time management
- Target ~30 min for 15 slides
- If deep Q&A on early slides, compress: Slide 7 (scalability) can be a short verbal summary since SKIP LOCKED already explained the mechanism
- Slides 4, 6, 8, 11, and 15 are the showcase — protect their time
- Slide 4 (SKIP LOCKED) is the keystone — if audience gets this, everything after clicks
- If they are highly technical, spend more time in `internal/outbox/claim.go` and the benchmark slide, less on the ops prose
