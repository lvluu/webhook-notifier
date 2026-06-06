package observe

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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

		if err := samplePendingDeliveries(sampleCtx, db); err != nil {
			if logger != nil {
				logger.Warn("sample pending deliveries failed", "error", err)
			}
		}
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

func samplePendingDeliveries(ctx context.Context, db *sql.DB) (err error) {
	rows, err := db.QueryContext(ctx, `
		SELECT COALESCE(c.class, 1),
		       COUNT(*),
		       COALESCE(MAX(EXTRACT(EPOCH FROM now() - d.created_at)) FILTER (WHERE d.status = 'pending'), 0)
		FROM deliveries d
		LEFT JOIN customers c ON c.id = d.customer_id
		WHERE d.status IN ('pending', 'failed_retry')
		GROUP BY COALESCE(c.class, 1)
	`)
	if err != nil {
		return fmt.Errorf("query pending deliveries: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close pending deliveries rows: %w", closeErr))
		}
	}()

	pendingByClass := make(map[string]float64, len(customerClasses))
	var oldestAge float64
	for rows.Next() {
		var class uint32
		var pending, classOldestAge float64
		if err := rows.Scan(&class, &pending, &classOldestAge); err != nil {
			return fmt.Errorf("scan pending deliveries: %w", err)
		}
		pendingByClass[CustomerClassFromValue(class)] += pending
		if classOldestAge > oldestAge {
			oldestAge = classOldestAge
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate pending deliveries: %w", err)
	}
	for _, customerClass := range customerClasses {
		SetPendingDeliveries(customerClass, pendingByClass[customerClass])
	}
	SetPendingOldestAge(oldestAge)
	return nil
}
