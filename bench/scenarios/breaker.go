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
	// Benchmarks persist synchronously so breaker state is visible to claim
	// queries without adding a second lifecycle goroutine to the harness.
	return breaker.NewMemory(config, snapshots, func(transition breaker.Transition) {
		if err := breaker.PersistTransition(ctx, db, transition); err != nil {
			panic(fmt.Sprintf("persist breaker transition: %v", err))
		}
	}), nil
}
