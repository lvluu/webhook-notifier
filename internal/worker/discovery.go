package worker

import (
	"context"
	cryptorand "crypto/rand"
	"fmt"
	"math/big"
	"time"

	"github.com/lvluu/webhook-notifier/internal/scheduler"
)

const (
	defaultDiscoveryInterval     = time.Second
	defaultDiscoveryEmptyBackoff = 5 * time.Second
	defaultDiscoveryErrorBackoff = time.Second
	defaultDiscoveryMaxBackoff   = 30 * time.Second
	defaultDiscoveryLimit        = 1000
)

// ClaimableCustomerStore discovers Customers with at least one Delivery
// eligible for claim. Satisfied by *outbox.Store.
type ClaimableCustomerStore interface {
	DiscoverClaimableCustomers(ctx context.Context, afterCustomerID string, limit int) ([]string, error)
}

// DiscoveryConfig configures the claimable Customer discovery loop.
type DiscoveryConfig struct {
	Interval     time.Duration
	EmptyBackoff time.Duration
	ErrorBackoff time.Duration
	MaxBackoff   time.Duration
	Limit        int
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
	emptyBackoff := cfg.EmptyBackoff
	if emptyBackoff <= 0 {
		emptyBackoff = defaultDiscoveryEmptyBackoff
	}
	errorBackoff := cfg.ErrorBackoff
	if errorBackoff <= 0 {
		errorBackoff = defaultDiscoveryErrorBackoff
	}
	maxBackoff := cfg.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = defaultDiscoveryMaxBackoff
	}
	limit := cfg.Limit
	if limit <= 0 {
		limit = defaultDiscoveryLimit
	}

	var cursor string
	nextEmptyBackoff := emptyBackoff
	nextErrorBackoff := errorBackoff
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		customers, err := store.DiscoverClaimableCustomers(ctx, cursor, limit)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := sleepDiscovery(ctx, nextErrorBackoff); err != nil {
				return err
			}
			nextErrorBackoff = growBackoff(nextErrorBackoff, maxBackoff)
			continue
		}

		var delay time.Duration
		if len(customers) == 0 {
			cursor = ""
			delay = nextEmptyBackoff
			nextEmptyBackoff = growBackoff(nextEmptyBackoff, maxBackoff)
			nextErrorBackoff = errorBackoff
		} else {
			for _, customerID := range customers {
				sched.Notify(customerID)
			}
			cursor = customers[len(customers)-1]
			delay = interval
			nextEmptyBackoff = emptyBackoff
			nextErrorBackoff = errorBackoff
		}

		if err := sleepDiscovery(ctx, delay); err != nil {
			return err
		}
	}
}

func growBackoff(current time.Duration, maxBackoff time.Duration) time.Duration {
	if current <= 0 {
		return maxBackoff
	}
	if current >= maxBackoff {
		return maxBackoff
	}
	next := current * 2
	if next < current || next > maxBackoff {
		return maxBackoff
	}
	return next
}

func sleepDiscovery(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		return nil
	}

	timer := time.NewTimer(jitterDelay(delay))
	select {
	case <-ctx.Done():
		timer.Stop()
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func jitterDelay(delay time.Duration) time.Duration {
	if delay <= 1 {
		return delay
	}

	half := delay / 2
	n, err := cryptorand.Int(cryptorand.Reader, big.NewInt(int64(delay-half)))
	if err != nil {
		return delay
	}
	return half + time.Duration(n.Int64())
}
