# Partner Resilience: Reliability Drill-Down

## Description

Profile **dev**: consolidated reliability proof covering transient partner
recovery, terminal 4xx fast-fail, and dead-partner circuit-breaker isolation.

Folded from the legacy Scenarios 2, 3, and 5.

## Phase A — Transient + Recovering Partners

| Metric | Value |
|---|---:|
| Events | 100 |
| Expected Deliveries | 300 |
| Total Deliveries | 300 |
| Terminal Deliveries | 300 |
| Partner A p99 (ms) | 25.26 |
| Recovery: Successfully recovered | 0 / 0 |
| Jitter: max retries per second post-recovery | 0 / 0 total |

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

## Phase C — Dead Partner Breaker Isolation

| Metric | Control | Dead Partner |
|---|---|---:|---:|
| Duration | 1.512s | 18.87s |
| Goroutines (baseline → final) | 6 | 5 |
| Dead-customer attempts | — | 65 |
| Dead-customer attempt cap | — | 100 |
| Dead-customer pending (skipped) | — | 100 |

| Gate | OK |
|---|---|
| Healthy Customer p95 within 10% of control | **PASS** |
| Breaker opened | **PASS** |
| Half-open probe observed | **PASS** |
| Breaker re-opened after probe | **PASS** |
| Goroutine count stable | **PASS** |
| Dead-customer attempt cap met | **PASS** |
| Deliveries skipped after breaker open | **PASS** |
