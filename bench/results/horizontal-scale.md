# Horizontal Scale: Horizontal Scalability (Shared Postgres Outbox)

## Description

Profile **stress** runs real notifier OS processes (`go run ./cmd/notifier`) sharing one benchmark Postgres database.
Each replica has unique business/metrics ports and explicit identity (`NOTIFIER_REPLICA_ID`).
Outbound attempts include `X-Notifier-Replica` only when replica identity is configured.

### Phase 1 — Sparse end-to-end ingest

- 20000 Small Customers × 1 Events each, mixed fast/slow/flaky receivers.
- Event ingest submitted through real `/events` endpoints round-robin across replicas.
- Validates first-attempt queue latency, duplicate tuple safety, sparse discovery behavior,
  and replica distribution diagnostics.

### Phase 2 — Dense delivery drain

- 100 Customers × 1000 pre-seeded Deliveries each, healthy receiver only.
- Validates throughput scaling and duplicate safety from 1 → N replicas.

## Sparse Phase Summary

| Metric | Value |
|---|---:|
| Replicas | 4 |
| Expected Deliveries | 60000 |
| Terminal Deliveries | 60000 |
| Duplicate (delivery_id, attempt_number) tuples | 0 |
| Small P95 first-attempt latency (ms) | 290.55 |
| Small P95 threshold (ms) | 3000.00 |
| Avg Deliveries per claim query | 9.01 |
| Avg Deliveries per Customer claim | 0.28 |
| Empty claim rate | 33.39% |

| Sparse hard gate | Result |
|---|---|
| All expected Deliveries terminal | **PASS** |
| Zero duplicate tuples | **PASS** |
| Small P95 within sparse fairness threshold | **PASS** |
| No zero-work replica in multi-replica run | **PASS** |

## Sparse per-replica distribution (diagnostic)

| Replica | Delivered Attempts |
|---|---:|
| replica-1 | 15858 |
| replica-2 | 15310 |
| replica-3 | 14817 |
| replica-4 | 14611 |

Sparse distribution diagnostic: imbalance ratio 1.09x (max/min)

## Dense Phase Summary

| Replicas | Expected Deliveries | Terminal Deliveries | Duration | Throughput (Deliveries/s) | Duplicate Tuples | All Terminal | Duplicates | Replica Zero-Work Gate |
|---:|---:|---:|---:|---:|---:|---|---|---|
| 1 | 100000 | 100000 | 6.331s | 15794.48 | 0 | **PASS** | **PASS** | **PASS** |
| 2 | 100000 | 100000 | 3.797s | 26339.67 | 0 | **PASS** | **PASS** | **PASS** |
| 4 | 100000 | 100000 | 3.468s | 28835.59 | 0 | **PASS** | **PASS** | **PASS** |

## Scaling Verdict

| Verdict | Value |
|---|---|
| Mode | improvement |
| Summary | throughput improved 15794.48 -> 28835.59 deliveries/s |
| Bottleneck (if no improvement/regression) | - |
| Gate | **PASS** |
