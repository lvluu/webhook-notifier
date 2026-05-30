package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/lvluu/webhook-notifier/internal/scheduler"
)

const (
	defaultDiscoveryInterval = time.Second
	defaultDiscoveryLimit    = 1000
)

// ClaimableCustomerStore discovers Customers with at least one Delivery
// eligible for claim. Satisfied by *outbox.Store.
type ClaimableCustomerStore interface {
	DiscoverClaimableCustomers(ctx context.Context, afterCustomerID string, limit int) ([]string, error)
}

// DiscoveryConfig configures the claimable Customer discovery loop.
type DiscoveryConfig struct {
	Interval time.Duration
	Limit    int
}

// RunClaimableCustomerDiscovery periodically discovers claimable Customers from
// shared storage and notifies the local Scheduler.
func RunClaimableCustomerDiscovery(
	ctx context.Context,
	store ClaimableCustomerStore,
	sched scheduler.Scheduler,
	cfg DiscoveryConfig,
) error {
	if store == nil {
		return fmt.Errorf("claimable customer store is required")
	}
	if sched == nil {
		return fmt.Errorf("scheduler is required")
	}

	interval := cfg.Interval
	if interval <= 0 {
		interval = defaultDiscoveryInterval
	}
	limit := cfg.Limit
	if limit <= 0 {
		limit = defaultDiscoveryLimit
	}

	var cursor string
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		customers, err := store.DiscoverClaimableCustomers(ctx, cursor, limit)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("discover claimable customers: %w", err)
		}
		if len(customers) == 0 {
			cursor = ""
		} else {
			for _, customerID := range customers {
				sched.Notify(customerID)
			}
			cursor = customers[len(customers)-1]
		}

		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
