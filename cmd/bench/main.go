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
	case "reliability":
		err = scenarios.RunReliability(ctx)
	case "fairness":
		err = scenarios.RunFairness(ctx)
	case "scalability":
		err = scenarios.RunScalability(ctx)
	case "tuning":
		err = scenarios.RunTuning(ctx)
	case "partner-protection":
		err = scenarios.RunPartnerProtection(ctx)
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
