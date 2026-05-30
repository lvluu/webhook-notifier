# Scenario 2: Happy-Case Throughput

## Description

**10000 deliveries** across **8 customers** (1250 each), burst at T=0. No chaos injection (0% latency, 0% errors). Measures raw throughput of the claim→POST→mark pipeline.

Compared to Scenario 1 (~17.6 deliveries/s with 5% 500s + 10% whale 502s + 15% latency + 1s drip).

## Results

| Metric | Value |
|---|---|
| Total duration | 3s |
| Total deliveries | 10000 |
| Workers | 8 |
| Throughput | **3194 deliveries/s** |
| SCALABILITY_REVIEW: Normal target | 2,000-15,000/s |
| SCALABILITY_REVIEW: Peak target | 20,000-150,000/s |
| Verdict | Normal range (3194/s) — meets SCALABILITY_REVIEW normal target of 2,000-15,000/s |

| **Fat Customers (P50/P95/P99/Max)** | 0 ms | 0 ms | 0 ms | 0 ms |
| **Overall (P50/P95/P99/Max)** | 1997 ms | 3399 ms | 3529 ms | 3599 ms |

## Delivery Outcomes

| Status | Count |
|---|---|
| succeeded | 10000 |
