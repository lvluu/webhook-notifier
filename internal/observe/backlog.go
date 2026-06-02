package observe

import (
	"context"
	"database/sql"
	"log/slog"
	"time"
)

// StartPendingDeliveriesSampler keeps the backlog gauge in sync with the DB.
// It samples the current pending/retry backlog on a fixed interval until ctx ends.
func StartPendingDeliveriesSampler(ctx context.Context, db *sql.DB, interval time.Duration, logger *slog.Logger) {
	if db == nil || !metricsRegistered() {
		return
	}
	if interval <= 0 {
		return
	}

	update := func() {
		sampleCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		var pending float64
		err := db.QueryRowContext(sampleCtx, `
			SELECT COUNT(*)
			FROM deliveries
			WHERE status IN ('pending', 'failed_retry')
		`).Scan(&pending)
		if err != nil {
			if logger != nil {
				logger.Warn("sample pending deliveries failed", "error", err)
			}
			return
		}
		SetPendingDeliveries(pending)
	}

	update()

	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				update()
			}
		}
	}()
}
