// Package worker drains claimed Deliveries, signs, POSTs, and applies
// the retry policy. Also hosts the per-host concurrency limiter.
package worker
