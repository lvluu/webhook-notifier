# Partner Resilience: Reliability Drill-Down

## Description

Profile **stress**: consolidated reliability proof covering transient partner
recovery, terminal 4xx fast-fail, and dead-partner circuit-breaker isolation.

Folded from the legacy Scenarios 2, 3, and 5.

## Phase A — Transient + Recovering Partners

| Metric | Value |
|---|---:|
| Events | 1000 |
| Expected Deliveries | 3000 |
| Total Deliveries | 3000 |
| Terminal Deliveries | 3000 |
| Partner A p99 (ms) | 86.25 |
| Recovery: Successfully recovered | 622 / 622 |
| Jitter: max retries per second post-recovery | 124 / 623 total |

| Gate | OK |
|---|---|
| 100% terminal | **PASS** |
| Recovery complete | **PASS** |
| Partner A p99 unaffected | **PASS** |
| No post-recovery retry spike | **PASS** |

## Phase B — Terminal 4xx Fast-Fail

| Metric | Value |
|---|---:|
| Events submitted | 100 |
| Expected Deliveries | 200 |
| Total Deliveries | 200 |
| Terminal Deliveries | 200 |
| Good webhook: succeeded / total | 100 / 100 |
| Bad webhook: dead / total | 100 / 100 |
| Bad webhook: retry waste (deliveries with >1 attempt) | 0 |

| Gate | OK |
|---|---|
| 100% terminal | **PASS** |
| Good webhook all succeeded | **PASS** |
| Bad webhook all dead, no retry waste | **PASS** |

## Phase C — Dead Partner Breaker Isolation

| Metric | Control | Dead Partner |
|---|---|:---:|---:|
| Duration | 12.628s | 49.006s |
| Goroutines (baseline → final) | 7 | 7 |
| Dead-customer attempts | — | 192 |
| Dead-customer attempt cap | — | 1000 |
| Dead-customer pending (skipped) | — | 1000 |

| Gate | OK |
|---|---|
| Healthy Customer p95 within 10% of control | **FAIL** |
| Breaker opened | **PASS** |
| Half-open probe observed | **PASS** |
| Breaker re-opened after probe | **PASS** |
| Goroutine count stable | **PASS** |
| Dead-customer attempt cap met | **PASS** |
| Deliveries skipped after breaker open | **PASS** |
