# Configuration Tuning

## Description

Profile **stress** sweeps a grid of worker concurrency parameters against a fixed
Delivery backlog to identify the highest-throughput and lowest-latency configuration.

Each row seeds 500 Customers × 3 Webhooks each (fast/slow/flaky) with
100 Deliveries per Customer via bulk SQL (same fan-out shape as real ingest),
then measures the drain.

## Sweep Dimensions

| Parameter | Values tested |
|---|---|
| Workers | [16 32] |
| DeliveryConcurrency | [8 16] |
| MinBatchHint | [8 32] |
| MaxBatchHint | [32 128] |

## Architecture Settings

| Module | Setting in this scenario |
|---|---|
| Topology | In-process worker goroutines sharing one Postgres database and one process-local Scheduler |
| Scheduler | Equal-share round-robin; weighted scheduling disabled |
| Ingest | Bypassed for load generation; Events and Deliveries are set-based SQL seeded with the same Event-to-Webhook fanout shape |
| Discovery | Manual claimable-Customer DB poll every 200ms; no PgNotify path |
| Claim batching | `MultiPickCount=32` |
| Circuit breaker | Disabled (`breaker_failure_threshold=0`) to isolate throughput tuning from partner isolation |
| HostLimiter | Disabled; receiver latency and failures are part of Delivery cost, not capped by hostname |
| Retry | Max attempts 10, base delay 25ms, cap delay 2s; MaxAge disabled |
| HTTP | Default shared Worker client, request timeout 1.5s |

## Results (ranked by throughput)

| Rank | Config | Workers | DeliveryConcurrency | MinBatchHint | MaxBatchHint | Total Deliveries | Duration | Throughput (del/s) | Avg Claim Batch | Claim Calls | Empty Claims | Retry Rate | Dead Rate | P95 Latency (ms) | Duplicate tuples | Reliability |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | w32-dc16-mn32-mx32 | 32 | 16 | 32 | 32 | 150000 | 26.637s | 5631.18 | 46.24 | 3278 | 3057 | 1.03% | 0.00% | 17668.74 | 0 | **PASS** |
| 2 | w32-dc16-mn8-mx32 | 32 | 16 | 8 | 32 | 150000 | 27.294s | 5495.64 | 348.47 | 435 | 194 | 1.04% | 0.00% | 25779.26 | 0 | **PASS** |
| 3 | w32-dc16-mn8-mx128 | 32 | 16 | 8 | 128 | 150000 | 27.577s | 5439.30 | 258.19 | 587 | 400 | 1.03% | 0.00% | 25557.34 | 0 | **PASS** |
| 4 | w32-dc8-mn32-mx32 | 32 | 8 | 32 | 32 | 150000 | 28.238s | 5311.92 | 180.17 | 841 | 612 | 1.00% | 0.00% | 25869.00 | 0 | **PASS** |
| 5 | w16-dc16-mn8-mx128 | 16 | 16 | 8 | 128 | 150000 | 29.308s | 5118.04 | 656.18 | 231 | 107 | 1.04% | 0.00% | 28303.15 | 0 | **PASS** |
| 6 | w32-dc8-mn8-mx128 | 32 | 8 | 8 | 128 | 150000 | 29.458s | 5091.92 | 145.14 | 1044 | 883 | 1.01% | 0.00% | 27091.10 | 0 | **PASS** |
| 7 | w16-dc16-mn32-mx128 | 16 | 16 | 32 | 128 | 150000 | 29.955s | 5007.50 | 522.75 | 290 | 162 | 1.05% | 0.00% | 28964.99 | 0 | **PASS** |
| 8 | w32-dc16-mn32-mx128 | 32 | 16 | 32 | 128 | 150000 | 33.716s | 4448.88 | 122.83 | 1234 | 1043 | 1.04% | 0.00% | 27567.10 | 0 | **PASS** |
| 9 | w32-dc8-mn32-mx128 | 32 | 8 | 32 | 128 | 150000 | 34.12s | 4396.20 | 178.88 | 847 | 665 | 1.00% | 0.00% | 32356.68 | 0 | **PASS** |
| 10 | w32-dc8-mn8-mx32 | 32 | 8 | 8 | 32 | 150000 | 37.384s | 4012.43 | 47.99 | 3157 | 2909 | 0.99% | 0.00% | 29457.08 | 0 | **PASS** |
| 11 | w16-dc8-mn8-mx32 | 16 | 8 | 8 | 32 | 150000 | 39.283s | 3818.44 | 535.19 | 283 | 98 | 0.96% | 0.00% | 37356.04 | 0 | **PASS** |
| 12 | w16-dc16-mn32-mx32 | 16 | 16 | 32 | 32 | 150000 | 40.077s | 3742.77 | 357.32 | 424 | 219 | 0.99% | 0.00% | 37633.31 | 0 | **PASS** |
| 13 | w16-dc16-mn8-mx32 | 16 | 16 | 8 | 32 | 150000 | 41.917s | 3578.52 | 603.64 | 251 | 48 | 1.00% | 0.00% | 38847.89 | 0 | **PASS** |
| 14 | w16-dc8-mn8-mx128 | 16 | 8 | 8 | 128 | 150000 | 45.307s | 3310.76 | 420.94 | 360 | 232 | 1.01% | 0.00% | 44071.67 | 0 | **PASS** |
| 15 | w16-dc8-mn32-mx32 | 16 | 8 | 32 | 32 | 150000 | 58.421s | 2567.59 | 582.77 | 260 | 73 | 1.00% | 0.00% | 56620.57 | 0 | **PASS** |
| 16 | w16-dc8-mn32-mx128 | 16 | 8 | 32 | 128 | 150000 | 1m4.583s | 2322.59 | 390.58 | 388 | 253 | 1.02% | 0.00% | 61021.06 | 0 | **PASS** |

## Key Takeaways

- **Throughput** is total deliveries / wall time from seed to full drain.
- **Workers** of 16 vs 32 shows whether additional goroutines improve throughput or add claim contention.
- **DeliveryConcurrency** controls how many concurrent HTTP calls a single worker goroutine sends.
  Higher values overlap partner latency better; too high saturates the host limiter or DB.
- **MinBatchHint / MaxBatchHint** control how many deliveries a worker claims per scheduler pick.
  Larger batches amortise DB round-trips but delay fairness rotation to other Customers.
- **P95 Latency** is first-attempt queue latency — time from delivery creation to first send.
- **Reliability** fails if any duplicate (delivery_id, attempt_number) tuple or >1% dead-letter rate is observed.
