# Retry policy and Delivery lifecycle

## Context

Reliability is one of the three central claims of the Webhook Notifier. The retry shape — how many Attempts, how long apart, when to give up, what counts as terminal — needs to be explicit rather than ad-hoc, because (a) reviewers will probe for specifics and (b) production hardening later will start from these defaults.

## Decision

### Backoff

**Decorrelated jitter**: `delay = random_between(base, min(cap, prev_delay × 3))`. Base 1s, cap 5 minutes. Chosen over full jitter to spread retry storms more evenly when many Deliveries to the same partner endpoint fail simultaneously (recovery thundering herd).

### Max Attempts

**8 Attempts** for the PoC. Worst-case spread ≈ 4 minutes. Production would extend to days, gated by a configurable `max_age` rather than only attempt count. Recorded here so the PoC limit isn't mistaken for a final answer.

### Terminal classification

| Outcome | Action |
| --- | --- |
| `2xx` | success — Delivery `succeeded` |
| `408`, `429`, `5xx`, network error, timeout | retry per backoff |
| `429` with `Retry-After` header | retry, honour header (overrides backoff) |
| `4xx` except `408`/`429` | terminal — straight to DLQ, do not retry |
| Attempts exhausted | terminal — DLQ |

Rationale for terminal-on-4xx: a misconfigured Webhook URL returning 404 should not consume 8 Attempts × every Event for that Customer. Fail fast, surface to ops.

### Delivery lifecycle

```
pending ──claim──> in_flight ──2xx──> succeeded
                          │
                          ├──retryable──> failed_retry ──next_attempt_at──> pending
                          │                                  (loop until exhausted)
                          └──terminal────────────────────> dead (DLQ)
```

Five states. `failed_retry` is a distinct state from `pending` so the bench harness and ops dashboards can distinguish "never tried" from "in retry backoff".

### Partner-side idempotency contract

Every Attempt carries:

- `X-Event-Id` — stable across all Deliveries derived from the same Event
- `X-Delivery-Id` — stable across all Attempts of the same Delivery
- `X-Attempt-Number` — incremented per Attempt
- `X-Signature-Timestamp` — UNIX seconds at signing time
- `X-Signature-256` — HMAC-SHA256 over `timestamp.body`

Partners are expected to dedupe on `X-Delivery-Id` (preferred) or `X-Event-Id` (if they only care about Event-level dedup).

## Consequences

- The RetryPolicy module is a pure function `Next(attempt int, prevDelay time.Duration, resp *http.Response, err error) (delay time.Duration, terminal bool)` — directly unit-testable with the table above as the truth table.
- DLQ rows must carry the terminal reason (exhausted vs 4xx vs network class) so ops can distinguish "partner endpoint is broken" from "partner endpoint is slow".
- Changing max Attempts or the backoff curve is a config change, not a code change — both live in `RetryConfig` passed at startup.
- The decorrelated-jitter choice is the non-obvious decision in this ADR. Full jitter is the more common default; if a future engineer sees the formula and wonders, this ADR is the answer.
