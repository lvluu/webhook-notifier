# Terminal Fast-Fail: Terminal Classification Fast-Fail

## Description

Profile **stress**: 1 Customer with 2 Webhooks: one healthy URL returning 2xx and one misconfigured URL returning 404. 1000 Events fan out to **2000 expected Deliveries**.

## Performance Summary

| Metric | Value |
|---|---:|
| Profile | stress |
| Total duration | 2s |
| Events submitted | 1000 |
| Deliveries created | 2000 / 2000 |
| Workers | 32 |
| Terminal Deliveries | 2000 / 2000 |

## Acceptance Criteria

| Pillar | Criterion | Observed | Limit | Result |
|---|---|---:|---:|---|
| Reliability | 100% Deliveries terminal | 2000 / 2000 | 2000 / 2000 | **PASS** |
| Reliability | Good Webhook Deliveries succeeded | 1000 / 1000 | 1000 / 1000 | **PASS** |
| Reliability | 404 Webhook Deliveries dead with terminal_4xx | 1000 / 1000 | 1000 / 1000 | **PASS** |
| Reliability | 404 Webhook retry waste | 0 Deliveries with >1 Attempt | 0 | **PASS** |

## Per-Webhook Attempts Per Delivery

| Webhook | Attempts | Deliveries |
|---|---:|---:|
| good | 1 | 1000 |
| misconfigured-404 | 1 | 1000 |

## DLQ Rows By Reason

| Reason | Rows |
|---|---:|
| terminal_4xx | 1000 |

## Delivery Outcomes

| Status | Count |
|---|---:|
| dead | 1000 |
| succeeded | 1000 |
