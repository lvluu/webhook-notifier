package scenarios

import (
	"context"
	"database/sql"
	"fmt"
)

const eventType = "bench.test"

func resetBenchData(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
		TRUNCATE
			webhooks,
			events,
			deliveries,
			delivery_attempts,
			dead_letters,
			customers
		CASCADE
	`)
	if err != nil {
		return fmt.Errorf("reset bench data: %w", err)
	}
	return nil
}

func countPendingDeliveries(ctx context.Context, db *sql.DB) (int, error) {
	var n int
	err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM deliveries
		WHERE status NOT IN ('succeeded', 'dead')
	`).Scan(&n)
	return n, err
}
