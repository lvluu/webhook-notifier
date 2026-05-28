// Package retry implements the pure-function retry policy.
//
// Encodes decorrelated jitter, 8-attempt cap, 4xx fast-fail, and
// Retry-After honouring (ADR-0003).
package retry
