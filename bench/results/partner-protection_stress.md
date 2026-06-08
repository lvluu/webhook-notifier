# Partner Protection

## Description

Profile **stress**: validates the dashboard's Partner Protection category with one
dead Integration Partner and healthy Customers on distinct hostnames. The hard
gates cover breaker transitions, bounded dead-partner Attempts, skipped backlog,
stable goroutine count, and healthy-Customer First-Attempt Queue Latency.

## Architecture Settings

| Module | Setting in this scenario |
|---|---|
| Topology | 32 in-process Workers sharing one Postgres database and Scheduler |
| Scheduler | Equal-share round-robin; weighted scheduling disabled |
| Ingest | Bypassed; one Event and 1000 Deliveries per Customer are seeded directly |
| Discovery | DB discovery: interval 50ms, empty backoff 50ms, limit 100; no PgNotify path |
| Circuit breaker | Enabled: in-memory ADR-0014 decisions plus persisted claim filtering; threshold 2, cooldown 15s, max cooldown 15s |
| HostLimiter | Enabled with cap 1 per hostname, 50ms Requeue delay; each Customer uses a distinct hostname |
| Worker execution | `MinBatchHint=1`, `DeliveryConcurrency=1` per Worker |
| Retry | 20 attempts, fixed 50ms retry delay; request timeout 250ms; MaxAge disabled |

| Metric | Control | Dead Partner |
|---|---:|---:|
| Duration | 13.752s | 48.226s |
| Worst healthy Customer p95 First-Attempt Queue Latency | 12910.97 ms | 13285.13 ms |
| Worst healthy Customer ratio | - | 1.03x |
| Goroutines (baseline to final) | 5 | 5 |
| Dead-customer Attempts | - | 100 |
| Dead-customer Attempt cap | - | 1000 |
| Dead-customer pending after breaker open | - | 1000 |

| Gate | Result |
|---|---|
| Healthy Customer p95 within 10% of control | **PASS** |
| Breaker opened | **PASS** |
| Half-open probe observed | **PASS** |
| Breaker re-opened after probe | **PASS** |
| Goroutine count stable | **PASS** |
| Dead-customer Attempt cap met | **PASS** |
| Deliveries skipped after breaker open | **PASS** |
