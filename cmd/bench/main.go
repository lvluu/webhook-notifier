// Command bench runs the Webhook Notifier benchmark scenarios.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	_ "github.com/lib/pq"

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
		err = scenarios.RunScenario1(ctx)
	case "scenario2":
		err = scenarios.RunScenario2(ctx)
	case "scenario3":
		err = scenarios.RunScenario3(ctx)
	case "scenario4":
		err = scenarios.RunScenario4(ctx)
	case "scenario5":
		err = scenarios.RunScenario5(ctx)
	case "scenario6":
		err = scenarios.RunScenario6(ctx)
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
