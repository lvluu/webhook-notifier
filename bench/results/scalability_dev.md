# Scalability

## Description

Profile **dev** runs real notifier OS processes (`go run ./cmd/notifier`) sharing one benchmark Postgres database.
Each replica has unique business/metrics ports and explicit identity (`NOTIFIER_REPLICA_ID`).
Outbound attempts include `X-Notifier-Replica` only when replica identity is configured.

## Architecture Settings

| Module | Setting in this scenario |
|---|---|
| Topology | Real notifier OS processes; replica counts [1 2]; each replica has 8 Workers and DeliveryConcurrency 16 |
| Database | One shared Postgres database; notifier processes connect directly in the benchmark harness (no PgBouncer) |
| Scheduler | Equal-share process-local round-robin; weighted scheduling disabled |
| Wakeups/discovery | PgNotify wakeups enabled by notifier defaults plus DB discovery every 50ms |
| Claim batching | `MinBatchHint=8`, `MultiPickCount=32`, default maximum batch hint 64 |
| Circuit breaker | Disabled (`breaker_failure_threshold=0`) so breaker filtering cannot affect scaling |
| HostLimiter | Enabled with cap 128 per hostname per replica, intentionally high enough not to be the expected bottleneck |
| Retry | Notifier defaults: 8 attempts, 1s base delay, 5m cap delay; MaxAge disabled |
| Ingest | Sparse phase uses real HTTP ingest; dense phase pre-seeds Deliveries to isolate delivery-pipeline throughput |

### Phase 1 — Sparse end-to-end ingest

- 1000 Small Customers × 1 Events each, mixed fast/slow/flaky receivers.
- Event ingest submitted through real `/events` endpoints round-robin across replicas.
- Validates first-attempt queue latency, duplicate tuple safety, sparse discovery behavior,
  and replica distribution diagnostics.

### Phase 2 — Dense delivery drain

- 10 Customers × 100 pre-seeded Deliveries each, healthy receiver only.
- Validates throughput scaling and duplicate safety from 1 → N replicas.

## Sparse Phase Summary

| Metric | Value |
|---|---:|
| Replicas | 2 |
| Expected Deliveries | 3000 |
| Terminal Deliveries | 3000 |
| Duplicate (delivery_id, attempt_number) tuples | 0 |
| Small P95 first-attempt latency (ms) | 276.48 |
| Small P95 threshold (ms) | 6000.00 |
| Avg Deliveries per claim query | 1.94 |
| Avg Deliveries per Customer claim | 0.06 |
| Empty claim rate | 62.32% |

| Sparse hard gate | Result |
|---|---|
| All expected Deliveries terminal | **PASS** |
| Zero duplicate tuples | **PASS** |
| Small P95 within sparse fairness threshold | **PASS** |
| No zero-work replica in multi-replica run | **PASS** |

## Sparse per-replica distribution (diagnostic)

| Replica | Delivered Attempts |
|---|---:|
| replica-1 | 1379 |
| replica-2 | 1657 |

Sparse distribution diagnostic: imbalance ratio 1.20x (max/min)

## Dense Phase Summary

| Replicas | Expected Deliveries | Terminal Deliveries | Duration | Throughput (Deliveries/s) | Duplicate Tuples | All Terminal | Duplicates |
|---:|---:|---:|---:|---:|---:|---|---|
| 1 | 1000 | 1000 | 253ms | 3953.50 | 0 | **PASS** | **PASS** |
| 2 | 1000 | 1000 | 1ms | 935808.23 | 0 | **PASS** | **PASS** |

## Scaling Verdict

| Verdict | Value |
|---|---|
| Mode | improvement |
| Summary | throughput improved 3953.50 -> 935808.23 deliveries/s |
| Bottleneck (if no improvement/regression) | - |
| Gate | **PASS** |
