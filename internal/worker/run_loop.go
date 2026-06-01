package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/lvluu/webhook-notifier/internal/observe"
	"github.com/lvluu/webhook-notifier/internal/outbox"
	"github.com/lvluu/webhook-notifier/internal/retry"
)

const (
	idleBackoff = 50 * time.Millisecond
)

// Run continuously claims scheduled Deliveries and processes them.
func (w *Worker) Run(ctx context.Context, store DeliveryStore) error {
	if store == nil {
		return fmt.Errorf("outbox store is required")
	}
	if w.Scheduler == nil {
		return fmt.Errorf("scheduler is required")
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		customerID, batchHint, ok, err := w.Scheduler.Pick(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("scheduler pick: %w", err)
		}
		if !ok {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(idleBackoff):
			}
			continue
		}

		deliveries, err := store.ClaimNextN(ctx, customerID, max(int(batchHint), w.MinBatchHint))
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("claim delivery for %s: %w", customerID, err)
		}
		if len(deliveries) == 0 {
			hasWork, checkErr := store.HasClaimableWork(ctx, customerID)
			switch {
			case checkErr != nil:
				w.Logger.Warn(
					"claimability check failed; keeping customer in rotation",
					"customer_id", customerID,
					"error", checkErr,
				)
				w.Scheduler.Notify(customerID)
			case hasWork:
				w.Scheduler.Notify(customerID)
			default:
				w.Scheduler.Remove(customerID)
			}
			continue
		}

		type classified struct {
			delivery outbox.Delivery
			outcome  deliveryOutcome
		}
		results := make([]classified, len(deliveries))
		sem := make(chan struct{}, w.DeliveryConcurrency)
		var wg sync.WaitGroup
		for i, d := range deliveries {
			wg.Add(1)
			go func(idx int, delivery outbox.Delivery) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				results[idx] = classified{
					delivery: delivery,
					outcome:  w.classifyDelivery(ctx, delivery),
				}
			}(i, d)
		}
		wg.Wait()

		var succeeded, retries, dead []outbox.BatchMarkItem

		for _, r := range results {
			delivery, outcome := r.delivery, r.outcome

			if outcome.err != nil {
				return fmt.Errorf("process delivery %s: %w", delivery.ID, outcome.err)
			}
			if outcome.requeued {
				delay := w.requeueDelay()
				if err := store.Requeue(ctx, delivery.ID, delay); err != nil {
					return fmt.Errorf("requeue delivery %s: %w", delivery.ID, err)
				}
				continue
			}

			log := w.Logger.With(
				slog.String("delivery_id", delivery.ID),
				slog.String("event_id", delivery.EventID),
				slog.String("customer_id", delivery.CustomerID),
				slog.Int("attempt_number", delivery.AttemptNumber),
			)

			switch {
			case outcome.reason == retry.ReasonSucceeded || (outcome.reason == "" && outcome.errorClass == ""):
				succeeded = append(succeeded, outbox.BatchMarkItem{
					DeliveryID:    delivery.ID,
					StatusCode:    outcome.statusCode,
					Duration:      outcome.duration,
					Lifetime:      outcome.lifetime,
					AttemptNumber: delivery.AttemptNumber,
				})
				log.Info("delivery succeeded",
					"status_code", outcome.statusCode,
					"duration_ms", outcome.duration.Milliseconds(),
				)
			case outcome.reason != "":
				dead = append(dead, outbox.BatchMarkItem{
					DeliveryID:    delivery.ID,
					StatusCode:    outcome.statusCode,
					Duration:      outcome.duration,
					ErrorClass:    outcome.errorClass,
					Reason:        outcome.reason,
					Lifetime:      outcome.lifetime,
					AttemptNumber: delivery.AttemptNumber,
				})
				log.Warn("delivery dead",
					"reason", outcome.reason,
					"status_code", outcome.statusCode,
					"duration_ms", outcome.duration.Milliseconds(),
				)
			default:
				retries = append(retries, outbox.BatchMarkItem{
					DeliveryID:    delivery.ID,
					StatusCode:    outcome.statusCode,
					Duration:      outcome.duration,
					ErrorClass:    outcome.errorClass,
					Delay:         outcome.delay,
					AttemptNumber: delivery.AttemptNumber,
				})
				log.Warn("delivery scheduled for retry",
					"reason", outcome.errorClass,
					"retry_delay_ms", outcome.delay.Milliseconds(),
					"status_code", outcome.statusCode,
					"duration_ms", outcome.duration.Milliseconds(),
				)
			}
		}

		if len(succeeded) > 0 {
			if err := store.MarkSucceededBatch(ctx, succeeded); err != nil {
				return fmt.Errorf("batch mark succeeded: %w", err)
			}
			for _, item := range succeeded {
				customerClass := observe.CustomerClassFromValue(findDeliveryClass(item.DeliveryID, deliveries))
				recordSucceededMetrics(customerClass, item.Lifetime.Seconds(), item.AttemptNumber)
			}
		}
		if len(retries) > 0 {
			if err := store.MarkRetryBatch(ctx, retries); err != nil {
				return fmt.Errorf("batch mark retry: %w", err)
			}
		}
		if len(dead) > 0 {
			if err := store.MarkDeadBatch(ctx, dead); err != nil {
				return fmt.Errorf("batch mark dead: %w", err)
			}
			for _, item := range dead {
				customerClass := observe.CustomerClassFromValue(findDeliveryClass(item.DeliveryID, deliveries))
				recordDeadMetrics(customerClass, item.Lifetime.Seconds(), item.AttemptNumber, item.Reason)
			}
		}

		w.Scheduler.Notify(customerID)
	}
}

func (w *Worker) requeueDelay() time.Duration {
	if w.HostLimiter != nil {
		return w.HostLimiter.RequeueDelay()
	}
	return 100 * time.Millisecond
}

func findDeliveryClass(id string, deliveries []outbox.Delivery) uint32 {
	for _, d := range deliveries {
		if d.ID == id {
			return d.CustomerClass
		}
	}
	return 1
}
