// Command bench runs the Webhook Notifier benchmark scenarios.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/lvluu/webhook-notifier/bench/scenarios"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	if len(os.Args) < 2 {
		_, _ = fmt.Fprintln(os.Stderr, "usage: bench <scenario>")
		os.Exit(1)
	}

	scenario := os.Args[1]
	ctx := context.Background()

	var err error
	switch scenario {
	case "production_workload", "production-workload":
		err = scenarios.RunProductionWorkload(ctx)
	case "horizontal_scale", "horizontal-scale":
		err = scenarios.RunHorizontalScale(ctx)
	case "partner_resilience", "partner-resilience":
		err = scenarios.RunPartnerResilience(ctx)
	case "late_arrival_fairness", "late-arrival-fairness",
		"transient_recovery", "transient-recovery",
		"terminal_fast_fail", "terminal-fast-fail",
		"scale_idempotency", "scale-idempotency",
		"breaker_isolation", "breaker-isolation",
		"breaker_storm", "breaker-storm",
		"scenario1", "scenario2", "scenario3", "scenario4", "scenario5", "scenario7":
		err = fmt.Errorf("scenario %q moved to cmd/bench-legacy: %s", scenario, legacyHint(scenario))
	default:
		err = fmt.Errorf("unknown scenario: %s", scenario)
	}

	if err != nil {
		if errors.Is(err, context.Canceled) {
			slog.Warn("scenario cancelled")
		} else {
			slog.Error("scenario failed", "error", err)
		}
		os.Exit(1)
	}
}

// legacyHint returns the surviving scenario(s) a legacy scenario was folded into.
func legacyHint(scenario string) string {
	hints := map[string]string{
		"scenario1":             "fairness proof -> production-workload",
		"late_arrival_fairness": "fairness proof -> production-workload",
		"late-arrival-fairness": "fairness proof -> production-workload",
		"scenario2":             "transient/recovering partner -> partner-resilience Phase A",
		"transient_recovery":    "transient/recovering partner -> partner-resilience Phase A",
		"transient-recovery":    "transient/recovering partner -> partner-resilience Phase A",
		"scenario3":             "terminal 4xx fast-fail -> partner-resilience Phase B",
		"terminal_fast_fail":    "terminal 4xx fast-fail -> partner-resilience Phase B",
		"terminal-fast-fail":    "terminal 4xx fast-fail -> partner-resilience Phase B",
		"scenario4":             "horizontal scale/idempotency -> horizontal-scale dense phase",
		"scale_idempotency":     "horizontal scale/idempotency -> horizontal-scale dense phase",
		"scale-idempotency":     "horizontal scale/idempotency -> horizontal-scale dense phase",
		"scenario5":             "dead-partner breaker -> partner-resilience Phase C",
		"breaker_isolation":     "dead-partner breaker -> partner-resilience Phase C",
		"breaker-isolation":     "dead-partner breaker -> partner-resilience Phase C",
		"scenario7":             "mass breaker tripping (internal hardening, not a requirements proof)",
		"breaker_storm":         "mass breaker tripping (internal hardening, not a requirements proof)",
		"breaker-storm":         "mass breaker tripping (internal hardening, not a requirements proof)",
	}
	if hint, ok := hints[scenario]; ok {
		return "Legacy: folded into " + hint + ". Run via: go run ./cmd/bench-legacy " + scenario
	}
	return "Run via: go run ./cmd/bench-legacy " + scenario
}
