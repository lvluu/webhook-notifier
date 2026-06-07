package scenarios

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/lvluu/webhook-notifier/internal/breaker"
)

func newBenchBreaker(ctx context.Context, db *sql.DB, threshold int, initialCooldown, maxCooldown time.Duration) (breaker.Breaker, error) {
	if threshold <= 0 {
		return nil, nil
	}
	config := breaker.NormalizeConfig(breaker.Config{
		FailureThreshold: threshold,
		InitialCooldown:  initialCooldown,
		MaxCooldown:      maxCooldown,
	})
	snapshots, err := breaker.LoadFromDB(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("load breaker state: %w", err)
	}
	persister := breaker.NewAsyncPersister(db, slog.Default(), 64)
	go func() {
		if err := persister.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("bench breaker persister stopped", "error", err)
		}
	}()
	return breaker.NewMemory(config, snapshots, persister.Sink()), nil
}
