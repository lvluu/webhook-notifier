// Command bench-legacy runs legacy benchmark scenarios that have been folded
// into the surviving bench scenarios (production_workload, horizontal_scale, partner_resilience).
// It shares the same bench/scenarios package and any missing profiles.yaml config.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"

	legacy "github.com/lvluu/webhook-notifier/bench/scenarios/legacy"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	if len(os.Args) < 2 {
		_, _ = fmt.Fprintln(os.Stderr, "usage: bench-legacy <scenario>")
		os.Exit(1)
	}

	scenario := os.Args[1]
	ctx := context.Background()

	var err error
	switch scenario {
	case "late_arrival_fairness", "late-arrival-fairness", "scenario1":
		err = 	legacy.RunLateArrivalFairness(ctx)
	case "transient_recovery", "transient-recovery", "scenario2":
		err = 	legacy.RunTransientRecovery(ctx)
	case "terminal_fast_fail", "terminal-fast-fail", "scenario3":
		err = 	legacy.RunTerminalFastFail(ctx)
	case "scale_idempotency", "scale-idempotency", "scenario4":
		err = 	legacy.RunScaleIdempotency(ctx)
	case "breaker_isolation", "breaker-isolation", "scenario5":
		err = 	legacy.RunBreakerIsolation(ctx)
	case "breaker_storm", "breaker-storm", "scenario7":
		err = 	legacy.RunBreakerStorm(ctx)
	default:
		err = fmt.Errorf("unknown or non-legacy scenario: %s", scenario)
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
