# Review: Issue #13 Alignment with assignment.md

**Reviewer instructions**: This document is prepared for another AI agent to review.
Verify every claim against the source files cited. Report any incorrect assertions,
missing evidence, or flawed reasoning.

---

## 1. Purpose

Evaluate whether [Issue #13](https://github.com/lvluu/flodesk-home-assignment/issues/13)
("Bench 2: transient + recovering partners") aligns with the requirements in
[assignment.md](/home/aloidaika/flo-home/assignment.md), and flag any gaps,
naming collisions, or scope mismatches before implementation begins.

## 2. Source Documents

| Document | Path | Role |
|----------|------|------|
| Assignment brief | `assignment.md` | Source of truth for requirements |
| Design doc | `docs/DESIGN.md` | Architecture + benchmark scenario definitions |
| Issue #13 | GitHub #13 | The work item under review |
| ADR-0001 | `docs/adr/0001-round-robin-fairness-with-pluggable-scheduler.md` | Fairness mechanism |
| ADR-0003 | `docs/adr/0003-retry-policy-and-delivery-lifecycle.md` | Retry policy spec |
| Existing scenario2.go | `bench/scenarios/scenario2.go` | Current code occupying the scenario-2 slot |
| Scenario 1 (template) | `bench/scenarios/scenario1.go` | Reference pattern for scenario implementation |
| ReceiverServer | `bench/receiver/receiver.go` | Test fixture with latency/status/downtime injection |
| Justfile | `justfile` | Task recipes, including bench stubs |
| cmd/bench | `cmd/bench/main.go` | Scenario CLI dispatcher |

## 3. What assignment.md Requires

The assignment defines three technical pillars (lines 19-29) plus deliverables:

```
Pillar        | Requirement
--------------|------------------------------------------------------------
Reliability   | Retry mechanism for failures/networking issues
Scalability   | Horizontally scalable workers
Fairness      | Balanced processing whale vs small accounts
```

Deliverables (lines 31-46):
- System design docs (HLD diagram, solution description, tech choices)
- Proof of concept (core functionality working)
- Balanced event processing (small accounts not delayed by whales)
- A way to test and benchmark

## 4. What Issue #13 Requires

**Scenario**: 10 Customers, Webhooks split across 3 partner hostnames.

| Partner | Behavior | What It Exercises |
|---------|----------|-------------------|
| A       | Healthy (baseline) | Control — p99 must stay unaffected |
| B       | 30% 5xx errors    | RetryPolicy, repeated-attempt handling |
| C       | 100% down for 30s mid-run, then recovers | Decorrelated jitter, backoff, recovery throughput spike |

**Pass criteria** (from GitHub Issue #13 body):
- [ ] `make bench-2` runs the scenario end-to-end
- [ ] Report includes terminal-resolution rate, Attempts-per-Delivery distribution, recovery-window success rate, per-partner latency
- [ ] Pass: 100% Deliveries terminal (`succeeded` ∪ `dead`); Partner-C Deliveries enqueued during outage all `succeeded` after recovery; Partner-A p99 unaffected
- [ ] Demonstrates decorrelated jitter (no synchronized retry spike after Partner-C recovery)

**Blocked by**: #8 (HostLimiter — ✅ CLOSED), #11 (ReceiverServer — ✅ CLOSED)

**DESIGN.md reference** (line 247):
> Scenario 2: Transient + recovering partners. 10 Customers, Webhooks split across 3 partners. Partner B: 30% 5xx. Partner C: down for 30s then recovers. 100% terminal; Partner-C Deliveries succeed after recovery; Partner-A latency unaffected. Claims exercised: Retry policy, decorrelated jitter (no thundering herd), per-host isolation.

Issue #13 and DESIGN.md are in perfect agreement.

## 5. Alignment Analysis

### 5.1 Reliability

| assignment.md says | Issue #13 tests | Match? |
|--------------------|-----------------|--------|
| "Retry mechanism should be in place" | Partner B (30% 5xx) exercises RetryPolicy end-to-end | ✅ Direct |
| "Networking issues from integration partners" | Partner C (100% down 30s → recovery) exercises backoff + jitter + recovery | ✅ Direct |
| — | Decorrelated jitter assertion prevents thundering herd on recovery | ✅ Covered |

**Evidence**: ADR-0003 specifies decorrelated jitter (`base=1s, cap=5m, max=8 attempts`).
The scenario exercises this by having all Partner-C deliveries fail simultaneously
during the 30s outage, then retry with jitter after recovery.

### 5.2 Scalability

| assignment.md says | Issue #13 tests | Match? |
|--------------------|-----------------|--------|
| "Workers horizontally scalable" | Not tested here | ⚠️ Implicit (worker pool runs, but no scaling measurement) |
| — | Scenario 4 (Issue #15) covers {1,4,8} worker count scaling | ✅ Covered elsewhere |

**Verdict**: Acceptable gap. Issue #13 focuses on reliability and partner isolation.
Scalability is explicitly tested by Issue #15 (Bench 4: horizontal scale + claim
atomicity). DESIGN.md assigns each pillar to a specific scenario — not every
scenario must test every pillar.

### 5.3 Fairness

| assignment.md says | Issue #13 tests | Match? |
|--------------------|-----------------|--------|
| "Balanced processing small vs large accounts" | Not tested here | ❌ Not tested |
| "Prevent delays for smaller accounts" | Not tested here | ❌ Not tested |

**However**: Issue #13 tests **per-partner isolation** (Partner A's p99 must be
unaffected by Partners B and C failures). This is a related but distinct concept:

| Concept | Definition | Tested by |
|---------|------------|-----------|
| **Customer fairness** | Per-Customer scheduling: whale can't starve small | Issue #12 (Bench 1) |
| **Partner isolation** | Per-partner concurrency: one partner's outage doesn't block other partners' deliveries | Issue #13 (Bench 2) |

**Verdict**: Acceptable gap. Customer fairness is Issue #12's domain. Partner
isolation is a meaningful separate concern. DESIGN.md's scenario table confirms
this split. The assignment's "fairness" language is about customers, not partners.

### 5.4 Proof of Concept & Benchmarking

| assignment.md says | Issue #13 delivers | Match? |
|--------------------|--------------------|--------|
| "Proof that concept is feasible" | Benchmark demonstrating reliability under partner failures contributes to the PoC | ✅ |
| "A way to test and benchmark" | This IS a benchmark scenario | ✅ Direct |
| — | Markdown report with terminal-resolution rate, attempts-per-delivery, per-partner latency | ✅ Per acceptance criteria |

### 5.5 Alignment Summary

| Requirement | Covered? | Where |
|-------------|----------|-------|
| Reliability | ✅ Direct | Partner B (30% 5xx) + Partner C (outage + recover) |
| Scalability | ⚠️ Implicit only | Not measured — delegated to Issue #15 |
| Customer fairness | ❌ Not tested | Delegated to Issue #12 |
| Partner isolation | ✅ Direct | Partner-A p99 unaffected check |
| Proof of concept | ✅ Contributes | Reliability proof |
| Benchmarking | ✅ Direct | This is a benchmark scenario |

**Overall verdict**: Issue #13 is aligned with assignment.md. It directly
addresses the Reliability pillar and the Benchmarking deliverable. The customer-
fairness gap is intentional and covered by Issue #12. The scalability gap is
intentional and covered by Issue #15.

---

## 6. 🚩 Critical Finding: Naming Collision

The codebase has a scenario identity collision that MUST be resolved before
implementing Issue #13.

### 6.1 The Collision

| Source | "Scenario 2" Content | Matches Issue #13? |
|--------|---------------------|--------------------|
| **GitHub Issue #13** | Transient + recovering partners | — (definition source) |
| **`docs/DESIGN.md`** line 247 | Transient + recovering partners | ✅ Perfect match |
| **`bench/scenarios/scenario2.go`** | **Happy-case throughput**: 8 `fat-*` customers, 1250 deliveries each, 0% chaos, ~3194 del/s | ❌ **Wrong scenario** |
| **`bench/results/scenario-2.md`** | Report for happy-case throughput (10000 deliveries, 3s duration) | ❌ Does not match Issue #13 |
| **`justfile`** line 71-72 | `bench-2: @echo "bench-2 stub — implemented in issue #13"` | 🟡 Stub — not wired |
| **`cmd/bench/main.go`** line 31-32 | `case "scenario2": err = scenarios.RunScenario2(ctx)` | ❌ Dispatches wrong scenario |

### 6.2 Root Cause

Someone implemented a "happy-case throughput" benchmark as `scenario2.go`
without associating it with a GitHub issue. It occupies the Scenario 2 slot
that DESIGN.md and Issue #13 reserve for "transient + recovering partners."
The throughput benchmark is useful but misnumbered.

### 6.3 Resolution Required

Before Issue #13 can be implemented:

**Option A — Rename existing scenario2 → scenario5** (recommended):
1. Rename `bench/scenarios/scenario2.go` to `bench/scenarios/scenario5.go`
2. Rename function `RunScenario2` → `RunScenario5`
3. Rename report file `bench/results/scenario-2.md` → `bench/results/scenario-5.md`
4. Add `case "scenario5":` to `cmd/bench/main.go`
5. Implement Issue #13 as the new canonical `scenario2.go` with `RunScenario2`
6. Wire `just bench-2` → `go run ./cmd/bench scenario2`

**Option B — Disambiguate by code path**:
- Keep existing `scenario2.go` as-is
- Implement Issue #13 as a new file (e.g., `scenario2_partners.go`)
- Wire `just bench-2` to the partner-failure scenario
- Update DESIGN.md's scenario table to reflect the actual numbering

**Recommendation**: Option A. It's cleaner and restores alignment between
the issue tracker, design doc, and code.

---

## 7. Additional Gaps and Ambiguities

### 7.1 Decorrelated Jitter: No Quantitative Pass/Fail

Issue #13 says "Demonstrates decorrelated jitter (no synchronized retry spike
after Partner-C recovery)" but does not specify a measurement.

**Problem**: Without a threshold, the pass criterion is subjective. A reviewer
could disagree on what constitutes "no synchronized spike."

**Recommendation**: Define jitter pass/fail as:
- "No more than X concurrent in-flight Attempts to Partner-C in any 1-second
  window after recovery"
- OR: "The inter-arrival time of retries to Partner-C follows a decorrelated
  distribution (measured by coefficient of variation > Y)"
- Simpler: "Concurrent Attempts to Partner-C at T+31s ≤ 2× the steady-state
  HostLimiter cap" (proves retries spread out rather than all firing at once)

### 7.2 "Partner-A p99 Unaffected": No Baseline

**Problem**: The pass criterion says "Partner-A p99 unaffected" but doesn't
define the baseline. Unaffected compared to what? Compared to before Partner-C
went down? Compared to a separate healthy-only run?

**Recommendation**: The scenario should either:
- Measure Partner-A latency during the first 10s (before any partner failure),
  then compare against Partner-A latency during the Partner-C outage window
  — pass if p99 increase ≤ 10%.
- OR run Partner-A alone as a baseline sub-test, then compare against the mixed
  run.

### 7.3 "100% Terminal": No Time Bound

**Problem**: The criterion "100% Deliveries terminal" doesn't state a timeout.
If the bench runs for 5 seconds but retries have a 5-minute cap (per ADR-0003),
deliveries would still be in `failed_retry` state, and the criterion would fail
even though the system is behaving correctly.

**Recommendation**: The scenario must either:
- Wait long enough for all retries to exhaust or succeed (sum of worst-case
  backoff for 8 attempts ≈ 4 minutes per ADR-0003), or
- Define a deadline and treat any delivery still retrying at the deadline as
  "in-flight, excluded from terminality metric."

### 7.4 Scenario Runtime vs RetryPolicy Ceiling

ADR-0003 sets `max=8` attempts with `base=1s cap=5m` decorrelated jitter.
The worst-case backoff chain is approximately 4 minutes. Partner-C is down for
30 seconds. After recovery, deliveries may still be in backoff for several
minutes.

**Risk**: If the bench polling timeout is too short (e.g., 2 minutes), some
retries may not complete, false-failing the pass criterion.

**Required**: The bench must either:
- Set a poll timeout ≥ 5 minutes to accommodate the retry schedule, or
- Override the RetryPolicy for bench mode (e.g., `max=4, cap=10s`) so the
  backoff chain fits within a shorter runtime window.

---

## 8. Implementation Readiness

### 8.1 What Already Exists

| Component | Status | Evidence |
|-----------|--------|----------|
| ReceiverServer with per-route injection | ✅ Ready | `WithRouteStatus(0.3, 500)`, `WithDowntime(afterN, 30s)` |
| ReceiverServer with recording | ✅ Ready | `Recorded() []Recording` for post-run assertions |
| DB seed/teardown | ✅ Ready | `resetBenchData()`, `seedCustomers()`, `seedWebhooks()` |
| Worker pool + Scheduler | ✅ Proven | Scenario 1 (50k deliveries) already passes |
| Report writer pattern | ✅ Ready | `writeScenario1Report()` is a proven template |
| Scenario driver pattern | ✅ Ready | `RunScenario1()` is a proven template |
| just bench-2 recipe | 🟡 Stub | Justfile line 71-72 |
| cmd/bench dispatch | 🟡 Happy-case only | No Issue #13 scenario wired |

### 8.2 What Needs Building

1. **Resolve naming collision** (see §6.3)
2. **Implement `RunScenario2()`** in new `bench/scenarios/scenario2.go`:
   - 10 customers with webhooks pointing to 3 ReceiverServer routes
   - Partner A: plain healthy endpoint
   - Partner B: `WithRouteStatus("/partner-b", 0.3, 500)`
   - Partner C: `WithDowntime(afterN=?, 30*time.Second)` — the ? value must
     be calibrated so the outage triggers mid-run after enough deliveries queue
3. **Write report** to `bench/results/scenario-2.md` with:
   - Terminal-resolution rate (succeeded vs dead)
   - Attempts-per-Delivery distribution
   - Recovery-window success rate (Partner-C deliveries enqueued during outage
     that eventually succeeded)
   - Per-partner latency (p50/p95/p99)
4. **Wire `just bench-2`** → `go run ./cmd/bench scenario2`
5. **Add `case "scenario2"`** to `cmd/bench/main.go`
6. **Define quantitative thresholds** for decorrelated jitter and Partner-A p99
   (see §7.1, §7.2)

---

## 9. Summary

| Dimension | Assessment |
|-----------|------------|
| **Alignment with assignment.md** | ✅ Good — covers Reliability and Benchmarking directly |
| **Alignment with DESIGN.md** | ✅ Perfect match with scenario table |
| **Naming collision** | 🚩 Must fix — existing `scenario2.go` is wrong scenario |
| **Pass criteria quantification** | ⚠️ Decorrelated jitter and Partner-A p99 need numeric thresholds |
| **Implementation readiness** | ✅ All building blocks exist; ~1 day of implementation |
| **Blocking dependencies** | ✅ Both (#8, #11) closed |

**Bottom line**: Issue #13 is well-scoped, aligned with the assignment, and
ready for implementation once the naming collision and threshold definitions
are resolved.
