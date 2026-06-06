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
	idleBackoff              = 50 * time.Millisecond
	defaultEmptyClaimBackoff = 20 * time.Millisecond
	maxEmptyClaimBackoff     = 250 * time.Millisecond
	// transientErrorBackoff is the pause after a recoverable database error
	// (statement timeout, deadlock, connection churn). Long enough to let an
	// overloaded database shed load, short enough to resume promptly. Kept
	// modest so a single replica does not stall its whole worker pool on a
	// transient blip.
	transientErrorBackoff = 250 * time.Millisecond
)

func (w *Worker) emptyClaimDelay() time.Duration {
	if w.Config.EmptyClaimBackoff > 0 {
		return w.Config.EmptyClaimBackoff
	}
	return defaultEmptyClaimBackoff
}

func (w *Worker) maxEmptyClaimDelay() time.Duration {
	base := w.emptyClaimDelay()
	if base > maxEmptyClaimBackoff {
		return base
	}
	return maxEmptyClaimBackoff
}

// Run continuously claims scheduled Deliveries and processes them.
func (w *Worker) Run(ctx context.Context, store DeliveryStore) error {
	if store == nil {
		return fmt.Errorf("outbox store is required")
	}
	if w.Scheduler == nil {
		return fmt.Errorf("scheduler is required")
	}

	emptyDelay := w.emptyClaimDelay()
	maxEmptyDelay := w.maxEmptyClaimDelay()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		deliveries, err := w.claimOrCollect(ctx, store)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if isTransientDBError(err) {
				if waitErr := w.backoffTransient(ctx, "claim", err); waitErr != nil {
					return waitErr
				}
				continue
			}
			return err
		}

		if len(deliveries) == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(emptyDelay):
			}
			emptyDelay = growBackoff(emptyDelay, maxEmptyDelay)
			continue
		}

		emptyDelay = w.emptyClaimDelay()
		if err := w.processDeliveries(ctx, store, deliveries); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if isTransientDBError(err) {
				if waitErr := w.backoffTransient(ctx, "mark", err); waitErr != nil {
					return waitErr
				}
				continue
			}
			return err
		}
	}
}

// backoffTransient logs a recoverable database error, records it for
// monitoring, and pauses before the next loop iteration. It returns a non-nil
// error only if the run context is canceled during the pause, in which case the
// caller should stop. A transient error must never tear down the replica: every
// worker shares one database, so a fatal return here cascades into a fleet-wide
// shutdown (see isTransientDBError).
func (w *Worker) backoffTransient(ctx context.Context, phase string, cause error) error {
	if w.Logger != nil {
		w.Logger.Warn("transient database error; backing off and retrying",
			"phase", phase,
			"backoff", transientErrorBackoff,
			"error", cause,
		)
	}
	observe.IncWorkerTransientDBError(phase)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(transientErrorBackoff):
		return nil
	}
}

func (w *Worker) claimOrCollect(ctx context.Context, store DeliveryStore) ([]outbox.Delivery, error) {
	if w.Config.MultiPickCount <= 1 {
		return w.claimSingle(ctx, store)
	}
	return w.claimMulti(ctx, store)
}

func (w *Worker) claimSingle(ctx context.Context, store DeliveryStore) ([]outbox.Delivery, error) {
	for {
		customerID, batchHint, ok, err := w.Scheduler.Pick(ctx)
		if err != nil {
			return nil, err
		}
		if !ok {
			if err := w.Scheduler.WaitForWork(ctx); err != nil {
				return nil, err
			}
			continue
		}

		limit := w.Config.ClaimLimit(batchHint)
		deliveries, err := store.ClaimNextN(ctx, customerID, limit)
		if err != nil {
			return nil, fmt.Errorf("claim delivery for %s: %w", customerID, err)
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
			return nil, nil
		}

		w.Scheduler.Notify(customerID)
		return deliveries, nil
	}
}

func (w *Worker) claimMulti(ctx context.Context, store DeliveryStore) ([]outbox.Delivery, error) {
	for {
		customerIDs := make([]string, 0, w.Config.MultiPickCount)
		totalLimit := 0
		for range w.Config.MultiPickCount {
			id, batchHint, ok, err := w.Scheduler.Pick(ctx)
			if err != nil {
				return nil, err
			}
			if !ok {
				break
			}
			customerIDs = append(customerIDs, id)
			totalLimit += w.Config.ClaimLimit(batchHint)
		}

		if len(customerIDs) == 0 {
			if err := w.Scheduler.WaitForWork(ctx); err != nil {
				return nil, err
			}
			continue
		}

		deliveries, err := store.ClaimNextBatch(ctx, customerIDs, totalLimit)
		if err != nil {
			return nil, fmt.Errorf("claim batch: %w", err)
		}

		// Track which customers actually produced deliveries.
		claimedSet := make(map[string]struct{}, len(deliveries))
		for _, d := range deliveries {
			claimedSet[d.CustomerID] = struct{}{}
		}

		// Find customers that had zero deliveries claimed.
		var empties []string
		for _, cid := range customerIDs {
			if _, ok := claimedSet[cid]; ok {
				w.Scheduler.Notify(cid)
			} else {
				empties = append(empties, cid)
			}
		}

		if len(empties) > 0 {
			hasWork, checkErr := store.HasClaimableWorkBatch(ctx, empties)
			if checkErr != nil {
				w.Logger.Warn(
					"claimability batch check failed; keeping customers in rotation",
					"empty_count", len(empties),
					"error", checkErr,
				)
				for _, cid := range empties {
					w.Scheduler.Notify(cid)
				}
			} else {
				for _, cid := range empties {
					if hasWork[cid] {
						w.Scheduler.Notify(cid)
					} else {
						w.Scheduler.Remove(cid)
					}
				}
			}
		}

		return deliveries, nil
	}
}

type classified struct {
	delivery outbox.Delivery
	outcome  deliveryOutcome
}

func (w *Worker) processDeliveries(ctx context.Context, store DeliveryStore, deliveries []outbox.Delivery) error {
	results := make([]classified, len(deliveries))
	sem := make(chan struct{}, w.Config.DeliveryConcurrency)
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
	var requeued []string
	// Breaker outcomes are collected here and enqueued to the per-process
	// coalescer only after all marks commit, so the hot webhook breaker rows are
	// not updated inside these high-concurrency mark transactions (ADR-0015).
	// Nil sink (e.g. tests, breaker disabled) means the store applies breaker
	// state inline as before.
	var breakerOutcomes []outbox.BreakerOutcome

	for _, r := range results {
		delivery, outcome := r.delivery, r.outcome

		if outcome.err != nil {
			return fmt.Errorf("process delivery %s: %w", delivery.ID, outcome.err)
		}
		if outcome.requeued {
			requeued = append(requeued, delivery.ID)
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
			if w.BreakerSink != nil {
				breakerOutcomes = append(breakerOutcomes, outbox.BreakerOutcome{
					WebhookID:  delivery.WebhookID,
					DeliveryID: delivery.ID,
					Success:    true,
				})
			}
			log.Debug("delivery succeeded",
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
			if w.BreakerSink != nil {
				breakerOutcomes = append(breakerOutcomes, outbox.BreakerOutcome{
					WebhookID:  delivery.WebhookID,
					DeliveryID: delivery.ID,
					Reason:     outcome.reason,
				})
			}
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
			if w.BreakerSink != nil {
				breakerOutcomes = append(breakerOutcomes, outbox.BreakerOutcome{
					WebhookID:  delivery.WebhookID,
					DeliveryID: delivery.ID,
					Reason:     outcome.errorClass,
				})
			}
			log.Warn("delivery scheduled for retry",
				"reason", outcome.errorClass,
				"retry_delay_ms", outcome.delay.Milliseconds(),
				"status_code", outcome.statusCode,
				"duration_ms", outcome.duration.Milliseconds(),
			)
		}
	}

	// Customer class by delivery ID, built once so the per-outcome metric
	// loops below are O(1) lookups rather than O(n) scans of the batch. Only
	// built when an outcome that records metrics is present (a retry-only batch
	// reads it nowhere).
	var classByID map[string]uint32
	if len(succeeded) > 0 || len(dead) > 0 {
		classByID = make(map[string]uint32, len(deliveries))
		for _, d := range deliveries {
			classByID[d.ID] = d.CustomerClass
		}
	}

	if len(requeued) > 0 {
		if err := store.RequeueBatch(ctx, requeued, w.requeueDelay()); err != nil {
			return fmt.Errorf("batch requeue: %w", err)
		}
	}
	if len(succeeded) > 0 {
		if err := store.MarkSucceededBatch(ctx, succeeded); err != nil {
			return fmt.Errorf("batch mark succeeded: %w", err)
		}
		for _, item := range succeeded {
			customerClass := observe.CustomerClassFromValue(classByID[item.DeliveryID])
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
			customerClass := observe.CustomerClassFromValue(classByID[item.DeliveryID])
			recordDeadMetrics(customerClass, item.Lifetime.Seconds(), item.AttemptNumber, item.Reason)
		}
	}

	// All marks committed: hand breaker outcomes to the coalescer for out-of-band
	// application. Done last so a failed mark (early return above) never records
	// a breaker outcome for work that was not persisted.
	if w.BreakerSink != nil && len(breakerOutcomes) > 0 {
		w.BreakerSink.Enqueue(breakerOutcomes)
	}

	return nil
}

func (w *Worker) requeueDelay() time.Duration {
	if w.HostLimiter != nil {
		return w.HostLimiter.RequeueDelay()
	}
	return 100 * time.Millisecond
}
