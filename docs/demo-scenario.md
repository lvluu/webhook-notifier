# Demo Scenario

This is the shortest demo that still covers the assignment requirements:
reliability, scalability, fairness, and metrics-driven operations.

Goal: show the stack running, then use Grafana, Prometheus metrics, and a few
Postgres checks to explain what the system is doing and what each signal means.

## Demo Order

### 1. Start the full stack

```bash
just stack-all 3
```

What to say:
- This brings up Postgres, PgBouncer, Caddy, Grafana, Prometheus, the receiver,
  and three notifier replicas.
- The point is to show horizontal scaling in a real local topology.
- I am not opening code first; I am opening the operational surface.

### 2. Verify the system is alive

```bash
just stack-status
```

What to say:
- This is the quick health view.
- I use it to confirm replicas, backlog shape, and whether work is moving.
- If the backlog is non-zero, that is fine; the question is whether it is
  draining and whether one Customer is being starved.

### 3. Open Grafana

Open Grafana at `http://localhost:3000`.

What to say:
- The dashboard is grouped by diagnosis question, not by code structure.
- I start with Overview, then Fairness, then Scalability, then Partner
  protection.

Panels to focus on:

- `Terminal Success Rate`
  - Indicates whether terminal outcomes are healthy overall.
  - A drop suggests reliability regression or a downstream failure mode.
- `Throughput By Customer Class`
  - Indicates whether small Customers keep making progress under mixed load.
  - If Whale throughput rises while Small stays flat, fairness is broken.
- `Queue Wait P95 By Customer Class`
  - This is the fairness signal.
  - Rising Small Customer P95 means Small traffic is waiting too long before
    first Attempt.
- `Pending Deliveries`
  - Indicates backlog depth.
  - Stable backlog with rising age usually means starvation, not just volume.
- `Oldest Pending Delivery`
  - Stronger than backlog count for spotting stuck work.
  - If this climbs, something is not progressing.
- `Attempts Per Delivery`
  - Indicates retry amplification.
  - Values well above 1 mean downstream failures are causing extra work.
- `Worker Transient DB Errors`
  - Indicates database instability or lock pressure that workers are absorbing.
  - Small spikes are tolerable; sustained growth is not.
- `Delivery Latency P95 By Customer Class`
  - Indicates end-to-end delivery lifetime, not just queue wait.
  - Useful for showing fairness after the first Attempt starts.
- `POST Duration P95 By Customer Class`
  - Separates Integration Partner slowness from queue or scheduler problems.
- `Active Customers`
  - Shows how many Customers currently have work in play.
- Lets you distinguish "few Customers dominating" from "many Customers
    progressing fairly.”
- `Deliveries In Flight`
  - Indicates worker saturation.
  - If this is pegged while claim duration rises, worker capacity or DB claim
    cost is the bottleneck.
- `Notifier Instances And Workers`
  - Confirms horizontal scale.
  - Use this to point at the 3 replicas and show the deployment is not single
    process.
- `Claim Duration`
  - Indicates database claim cost and lock pressure.
  - Rising p95 suggests Postgres claim contention.
- `Claim Batch Size`
  - Indicates how much work each claim transaction is pulling.
  - Helps explain whether the scheduler is feeding work efficiently.
- `Empty Claim Rate`
  - Indicates wasted claim attempts.
  - Rising empty claims with backlog still present points to discovery or
    scheduling inefficiency.
- `Database Pool`
  - Indicates whether the DB connection pool is the current limiter.
  - Useful when explaining whether the bottleneck is workers or connections.
- `Remote Wake Hints`
  - Indicates how often remote replicas are nudged to re-check for work.
  - Useful for showing the wake-up path is active without relying on it for
    correctness.
- `Scheduler Pick Duration`
  - Indicates scheduler overhead.
  - If this grows, round-robin or discovery work may be too expensive.
- `Host Limiter Wait`
  - Indicates host-level partner pressure.
  - Should normally stay low because the limiter is process-local and
    non-blocking.
- `Circuit Breaker State`
  - Indicates whether a partner is open, closed, or probing.
  - Use it to show broken partners are isolated.
- `Trips`
  - Indicates breaker transitions.
  - Good for showing a dead partner does not consume infinite worker capacity.
- `Sync Fallback`
  - Indicates persistence pressure on breaker state.
  - Any growth here is worth mentioning as an operational concern.

### 4. Show the Prometheus metrics endpoint

```bash
curl -s http://localhost:9090/metrics | rg 'webhook_(queue_wait_seconds|deliveries_total|pending_deliveries|claim_duration_seconds|claim_empty_total|breaker_state|host_semaphore_wait_seconds|dlq_total|attempt_total)'
```

What to say:
- The metrics match the Grafana dashboard.
- This is the raw evidence behind the panels.
- `webhook_queue_wait_seconds` is the core fairness metric.
- `webhook_claim_duration_seconds` and `webhook_claim_empty_total` are the
  claim-path health signals.
- `webhook_pending_deliveries` and `webhook_pending_oldest_age_seconds` show
  backlog depth and age.
- `webhook_breaker_state`, `webhook_breaker_trips_total`, and
  `webhook_host_semaphore_wait_seconds` show partner protection.
- `webhook_deliveries_total`, `webhook_attempt_total`, and
  `webhook_dlq_total` show delivery outcome quality.

### 5. Show the Postgres query-health probe

```bash
psql "$DATABASE_URL" -f bench/scripts/db_query_health.sql
```

What to say:
- This is the database runbook view the project already maintains.
- It combines backlog, bloat, locks, sessions, and `pg_stat_statements`.
- I use it to show whether the database is healthy enough to support the
  fairness and scalability story.
- If I want a clean before/after window, I reset `pg_stat_statements` first.

Useful signals from the probe:
- `pending_retry_total`
  - How much retryable work is waiting.
  - Rising backlog means the system is not keeping up.
- `oldest_pending_age_sec`
  - Strong starvation indicator.
  - More important than raw backlog count when I want to know whether work is
    actually moving.
- `dead_pct`
  - Dead-tuple ratio on `deliveries`.
  - If this grows too high, queue churn or retention is hurting the table.
- `pg_stat_activity` wait-event distribution
  - Shows lock, WAL, or IO pressure while the queue is hot.
  - Helps separate database contention from application-level backlog.
- `pg_stat_statements` top SQL by total execution time
  - Shows where the DB is spending time.
  - Claim and discovery queries should dominate here, not surprise statements.

### 6. Open the Postgres Query Health dashboard

Open the Grafana dashboard named `Postgres Query Health`.

What to say:
- This dashboard is the DB version of the app dashboard.
- It answers, "Which SQL is costing us time, and why?"

Panels to focus on:

- `Total Query Calls`
  - Indicates overall SQL volume since the last reset.
  - Useful as a baseline during a controlled demo window.
- `Total Exec Time`
  - Indicates how much database time the workload has consumed.
  - Good for showing whether the run is expensive or calm.
- `Slowest Mean Query`
  - Indicates the slowest average statement in the current set.
  - Useful for spotting an outlier before it becomes a bottleneck.
- `Calls Per Second Since Reset`
  - Indicates query pressure over the demo window.
  - Useful when you want to show the database load rate, not just totals.
- `Exec Seconds Per Second Since Reset`
  - Indicates how much DB time is being burned per second of wall time.
  - A high value means the database is doing too much work for the current
    load shape.
- `Mean Exec Time Since Reset`
  - Indicates the average statement latency across the current window.
  - Good for comparing a clean baseline to a stressed one.
- `Top SQL By Total Exec Time`
  - Indicates the statements spending the most total time.
  - This is where I would expect claim, discovery, and retention queries to
    appear.
- `Claim / Discovery / Retention SQL`
  - Narrows the view to the queue-related SQL families.
  - Best panel for explaining how the notifier spends DB time on its hot path.
- `Queue-Path SQL`
  - Shows queries touching `deliveries`, `delivery_claims`, and
    `delivery_history`.
  - This is the one I use when I want to talk about queue path regressions.
- `Current Wait Events`
  - Shows live Postgres wait-event mix.
  - Good for lock, WAL, or IO pressure while the queue is hot.

What to say:
- This is the "top Postgres stats" part of the demo.
- Grafana tells me what is happening at the system level.
- `pg_stat_statements` tells me which SQL is paying for it.
- `Current Wait Events` tells me whether the database is waiting on lock,
  WAL, or IO pressure.

## Demo Flow In One Sentence

Start the full stack, confirm it is alive, open Grafana, point at the fairness
and reliability panels, then use the Postgres query-health probe and
Prometheus to prove the numbers behind the dashboard.

## What The Metrics Mean

| Metric | What it tells you | What to say |
|---|---|---|
| `webhook_queue_wait_seconds` | First-Attempt scheduling delay by Customer class | "This is the fairness signal." |
| `webhook_deliveries_total` | Terminal outcome mix | "This tells me whether the system is succeeding, retrying, or dying." |
| `webhook_pending_deliveries` | Live backlog depth | "This tells me how much work is waiting." |
| `webhook_pending_oldest_age_seconds` | Starvation signal | "A growing oldest age is more important than a stable backlog count." |
| `webhook_claim_duration_seconds` | DB claim cost | "This shows whether Postgres claim contention is the current ceiling." |
| `webhook_claim_empty_total` | Wasted claim attempts | "Empty claims tell me the scheduler is looking but not finding enough work." |
| `webhook_attempt_total` | HTTP outcome mix | "This shows how partners are responding." |
| `webhook_dlq_total` | Terminal failures | "Any sustained rise means the reliability path needs attention." |
| `webhook_breaker_state` | Partner isolation state | "This tells me if a webhook is open, closed, or probing." |
| `webhook_host_semaphore_wait_seconds` | Host pressure | "This shows whether partner capacity is being protected." |
| `webhook_delivery_latency_seconds` | End-to-end lifetime | "This is the broader latency view after queueing and retries." |
| `webhook_post_duration_seconds` | Raw partner response time | "This separates partner slowness from scheduler slowness." |

## If Time Is Short

Cut in this order:
1. Skip the raw Prometheus scrape.
2. Skip the detailed Postgres query-health probe.
3. Keep Grafana plus `stack-status`.
4. Keep the fairness panel, the backlog/claim panel, and the Postgres query-health dashboard.

## If They Want The Deep Dive

Stay on:
- `Queue Wait P95 By Customer Class`
- `Pending Deliveries`
- `Claim Duration`
- `Empty Claim Rate`
- `Circuit Breaker State`
- `Host Limiter Wait`
- `Top SQL By Total Exec Time`
- `Queue-Path SQL`
- `Current Wait Events`

That combination gives the cleanest operations story:
fairness, backlog, DB pressure, partner protection, recovery, and the SQL
that is actually costing you time.
