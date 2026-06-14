# Configuration Tuning

## Description

Profile **dev** sweeps a grid of worker concurrency parameters against a fixed
Delivery backlog to identify the highest-throughput and lowest-latency configuration.

Each row seeds 50 Customers × 3 Webhooks each (fast/slow/flaky) with
200 Deliveries per Customer via bulk SQL (same fan-out shape as real ingest),
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
| 1 | w16-dc8-mn8-mx32 | 16 | 8 | 8 | 32 | 30000 | 8.647s | 3469.31 | 268.13 | 113 | 63 | 0.99% | 0.00% | 8256.13 | 0 | **PASS** |
| 2 | w32-dc8-mn8-mx32 | 32 | 8 | 8 | 32 | 30000 | 13.697s | 2190.20 | 12.02 | 2521 | 2413 | 0.96% | 0.00% | 6813.74 | 0 | **PASS** |
| 3 | w32-dc8-mn32-mx32 | 32 | 8 | 32 | 32 | 30000 | 14.156s | 2119.23 | 18.43 | 1645 | 1547 | 1.07% | 0.00% | 7827.09 | 0 | **PASS** |
| 4 | w32-dc8-mn32-mx128 | 32 | 8 | 32 | 128 | 30000 | 14.595s | 2055.49 | 28.56 | 1062 | 939 | 1.08% | 0.00% | 8594.19 | 0 | **PASS** |
| 5 | w16-dc16-mn8-mx128 | 16 | 16 | 8 | 128 | 30000 | 15.268s | 1964.89 | 214.91 | 141 | 88 | 1.00% | 0.00% | 6834.09 | 0 | **PASS** |
| 6 | w16-dc16-mn8-mx32 | 16 | 16 | 8 | 32 | 30000 | 15.381s | 1950.43 | 286.05 | 106 | 36 | 1.06% | 0.00% | 14740.63 | 0 | **PASS** |
| 7 | w16-dc8-mn8-mx128 | 16 | 8 | 8 | 128 | 30000 | 16.842s | 1781.28 | 27.91 | 1086 | 1037 | 1.04% | 0.00% | 8501.07 | 0 | **PASS** |
| 8 | w16-dc16-mn32-mx128 | 16 | 16 | 32 | 128 | 30000 | 18.134s | 1654.33 | 39.23 | 772 | 717 | 0.95% | 0.00% | 15624.33 | 0 | **PASS** |
| 9 | w32-dc16-mn8-mx32 | 32 | 16 | 8 | 32 | 30000 | 19.529s | 1536.21 | 60.16 | 504 | 390 | 1.05% | 0.00% | 16602.79 | 0 | **PASS** |
| 10 | w32-dc16-mn8-mx128 | 32 | 16 | 8 | 128 | 30000 | 19.809s | 1514.43 | 82.57 | 367 | 292 | 1.00% | 0.00% | 10638.55 | 0 | **PASS** |
| 11 | w32-dc16-mn32-mx128 | 32 | 16 | 32 | 128 | 30000 | 20.844s | 1439.26 | 31.70 | 955 | 860 | 0.91% | 0.00% | 11431.25 | 0 | **PASS** |
| 12 | w32-dc16-mn32-mx32 | 32 | 16 | 32 | 32 | 30000 | 20.965s | 1430.95 | 34.93 | 868 | 760 | 1.06% | 0.00% | 15124.55 | 0 | **PASS** |
| 13 | w16-dc8-mn32-mx32 | 16 | 8 | 32 | 32 | 30000 | 21.083s | 1422.97 | 57.01 | 532 | 460 | 1.08% | 0.00% | 16862.51 | 0 | **PASS** |
| 14 | w32-dc8-mn8-mx128 | 32 | 8 | 8 | 128 | 30000 | 21.26s | 1411.09 | 26.34 | 1152 | 1065 | 1.11% | 0.00% | 13723.72 | 0 | **PASS** |
| 15 | w16-dc8-mn32-mx128 | 16 | 8 | 32 | 128 | 30000 | 21.526s | 1393.65 | 126.91 | 239 | 168 | 1.09% | 0.00% | 19725.91 | 0 | **PASS** |
| 16 | w16-dc16-mn32-mx32 | 16 | 16 | 32 | 32 | 30000 | 23.519s | 1275.58 | 65.23 | 465 | 396 | 1.10% | 0.00% | 13749.18 | 0 | **PASS** |

## Key Takeaways

- **Throughput** is total deliveries / wall time from seed to full drain.
- **Workers** of 16 vs 32 shows whether additional goroutines improve throughput or add claim contention.
- **DeliveryConcurrency** controls how many concurrent HTTP calls a single worker goroutine sends.
  Higher values overlap partner latency better; too high saturates the host limiter or DB.
- **MinBatchHint / MaxBatchHint** control how many deliveries a worker claims per scheduler pick.
  Larger batches amortise DB round-trips but delay fairness rotation to other Customers.
- **P95 Latency** is first-attempt queue latency — time from delivery creation to first send.
- **Reliability** fails if any duplicate (delivery_id, attempt_number) tuple or >1% dead-letter rate is observed.
