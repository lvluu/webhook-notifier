// Package scenarios contains the surviving benchmark scenario drivers
// (production_workload, horizontal_scale, partner_resilience) and shared
// infrastructure (profiles, seed, db, query helpers) invoked from cmd/bench.
// Legacy scenarios (late_arrival_fairness–breaker_isolation, breaker_storm)
// live in bench/scenarios/legacy and are invoked from cmd/bench-legacy.
// All scenarios write a markdown report under bench/results/.
package scenarios
