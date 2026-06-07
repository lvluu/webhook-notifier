package scenarios

import (
	"context"
	"database/sql"
	"fmt"
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
	persister := breaker.NewAsyncPersister(db, nil, 64)
	go persister.Run(ctx)
	return breaker.NewMemory(config, snapshots, persister.Sink()), nil
}
