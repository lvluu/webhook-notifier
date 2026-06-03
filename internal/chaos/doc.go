// Package chaos emits synthetic webhook events as a reproducible load
// generator for benchmarks and chaos engineering.
//
// Emitter provides smooth, rate-limited arrival via
// golang.org/x/time/rate.Limiter, a channel-based MaxInFlight semaphore
// that caps concurrent in-flight POSTs, and three separate counters
// keyed on HTTP status code: sent (2xx), shed (429/503), and failed
// (network errors / all other responses).
package chaos
