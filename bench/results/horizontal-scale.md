# Scenario 8: Horizontal Scalability (Shared Postgres Outbox)

## Description

Profile **dev** runs real notifier OS processes (`go run ./cmd/notifier`) sharing one benchmark Postgres database.
Each replica has unique business/metrics ports and explicit identity (`NOTIFIER_REPLICA_ID`).
Outbound attempts include `X-Notifier-Replica` only when replica identity is configured.

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
| Small P95 first-attempt latency (ms) | 301.72 |
| Small P95 threshold (ms) | 6000.00 |
| Avg Deliveries per claim query | 2.12 |
| Avg Deliveries per Customer claim | 0.07 |
| Empty claim rate | 58.20% |

| Sparse hard gate | Result |
|---|---|
| All expected Deliveries terminal | **PASS** |
| Zero duplicate tuples | **PASS** |
| Small P95 within Scenario 6 threshold | **PASS** |
| No zero-work replica in multi-replica run | **PASS** |

## Sparse per-replica distribution (diagnostic)

| Replica | Delivered Attempts |
|---|---:|
| replica-1 | 1461 |
| replica-2 | 1568 |

Sparse distribution diagnostic: imbalance ratio 1.07x (max/min)

## Dense Phase Summary

| Replicas | Expected Deliveries | Terminal Deliveries | Duration | Throughput (Deliveries/s) | Duplicate Tuples | All Terminal | Duplicates | Replica Zero-Work Gate |
|---:|---:|---:|---:|---:|---:|---|---|---|
| 1 | 1000 | 1000 | 12.84s | 77.88 | 0 | **PASS** | **PASS** | **PASS** |
| 2 | 1000 | 1000 | 6.834s | 146.32 | 0 | **PASS** | **PASS** | **PASS** |

## Scaling Verdict

| Verdict | Value |
|---|---|
| Mode | improvement |
| Summary | throughput improved 77.88 -> 146.32 deliveries/s |
| Bottleneck (if no improvement/regression) | - |
| Gate | **PASS** |
