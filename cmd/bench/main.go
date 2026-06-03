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
	case "scenario1":
		// Legacy: fairness proof folded into Scenario 6 reporting.
		err = scenarios.RunScenario1(ctx)
	case "scenario2":
		// Legacy: transient/recovering partner proof folded into Partner Resilience Phase A.
		err = scenarios.RunScenario2(ctx)
	case "scenario3":
		// Legacy: terminal 4xx fast-fail proof folded into Partner Resilience Phase B.
		err = scenarios.RunScenario3(ctx)
	case "scenario4":
		// Legacy: horizontal scale/idempotency proof folded into Scenario 8 dense phase.
		err = scenarios.RunScenario4(ctx)
	case "scenario5":
		// Legacy: dead-partner breaker proof folded into Partner Resilience Phase C.
		err = scenarios.RunScenario5(ctx)
	case "scenario6":
		err = scenarios.RunScenario6(ctx)
	case "scenario7":
		err = scenarios.RunScenario7(ctx)
	case "scenario8":
		err = scenarios.RunScenario8(ctx)
	case "partner_resilience", "partner-resilience":
		err = scenarios.RunPartnerResilience(ctx)
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
