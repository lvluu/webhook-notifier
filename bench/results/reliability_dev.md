# Reliability

## Description

Profile **dev**: validates transient partner recovery, retry distribution,
terminal 4xx fast-fail, DLQ classification, and complete terminal outcomes.

## Architecture Settings

| Module | Phase A: transient recovery | Phase B: terminal 4xx |
|---|---|---|
| Topology | 8 in-process Workers | 4 in-process Workers |
| Scheduler | Equal-share round-robin | Equal-share round-robin |
| Ingest | Real `ingest.Store.Submit` with Registry cache | Real `ingest.Store.Submit` with Registry cache |
| Discovery | DB discovery: interval 50ms, empty backoff 50ms, limit 100 | Same discovery configuration |
| Circuit breaker | Disabled in Outbox and Worker to isolate RetryPolicy recovery | Disabled to prove 4xx fast-fail without breaker interference |
| HostLimiter | Enabled: cap 16 per hostname, 100ms Requeue delay | Disabled |
| Retry | 20 attempts, 1s base delay, 15s cap delay; MaxAge disabled | Worker defaults: 8 attempts, 1s base delay, 5m cap delay; MaxAge disabled |
| HTTP | Shared rewritten test client; Worker default 30s request timeout | Default Worker client and 30s request timeout |

## Phase A — Transient + Recovering Partners

| Metric | Value |
|---|---:|
| Events | 100 |
| Expected Deliveries | 300 |
| Total Deliveries | 300 |
| Terminal Deliveries | 300 |
| Partner A p99 (ms) | 35.56 |
| Recovery: Successfully recovered | 44 / 44 |
| Jitter: max retries per second post-recovery | 12 / 46 total |

| Gate | OK |
|---|---|
| 100% terminal | **PASS** |
| Recovery complete | **PASS** |
| Partner A p99 unaffected | **PASS** |
| No post-recovery retry spike | **PASS** |

## Phase B — Terminal 4xx Fast-Fail

| Metric | Value |
|---|---:|
| Events submitted | 20 |
| Expected Deliveries | 40 |
| Total Deliveries | 40 |
| Terminal Deliveries | 40 |
| Good webhook: succeeded / total | 20 / 20 |
| Bad webhook: dead / total | 20 / 20 |
| Bad webhook: retry waste (deliveries with >1 attempt) | 0 |

| Gate | OK |
|---|---|
| 100% terminal | **PASS** |
| Good webhook all succeeded | **PASS** |
| Bad webhook all dead, no retry waste | **PASS** |
