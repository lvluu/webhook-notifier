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
	transientErrorBackoff    = 250 * time.Millisecond
)

func (w *Worker) emptyClaimDelay() time.Duration {
	if w.Config.EmptyClaimBackoff > 0 {
		return w.Config.EmptyClaimBackoff
	}
	return defaultEmptyClaimBackoff
}

// Run continuously claims scheduled Deliveries and processes them.
func (w *Worker) Run(ctx context.Context, store DeliveryStore) error {
	if store == nil {
		return fmt.Errorf("outbox store is required")
	}
	if w.Scheduler == nil {
		return fmt.Errorf("scheduler is required")
	}

	// Start the persistent goroutine pool.
	poolCtx, poolCancel := context.WithCancel(context.Background())
	defer poolCancel()

	w.deliveryPool = make(chan deliveryWork, w.Config.DeliveryConcurrency)
	for range w.Config.DeliveryConcurrency {
		go func() {
			for {
				select {
				case <-poolCtx.Done():
					return
				case work := <-w.deliveryPool:
					*work.result = classified{
						delivery: work.delivery,
						outcome:  w.classifyDelivery(work.ctx, work.delivery),
					}
					work.done()
				}
			}
		}()
	}

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
			t := time.NewTimer(w.emptyClaimDelay())
			select {
			case <-ctx.Done():
				t.Stop()
				return ctx.Err()
			case <-t.C:
			}
			continue
		}

		if err := store.FetchPayloads(ctx, deliveries); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if isTransientDBError(err) {
				if waitErr := w.backoffTransient(ctx, "fetch-payloads", err); waitErr != nil {
					return waitErr
				}
				continue
			}
			return fmt.Errorf("fetch payloads: %w", err)
		}

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

func (w *Worker) backoffTransient(ctx context.Context, phase string, cause error) error {
	if w.Logger != nil {
		w.Logger.Warn("transient database error; backing off and retrying",
			"phase", phase,
			"backoff", transientErrorBackoff,
			"error", cause,
		)
	}
	observe.IncWorkerTransientDBError(phase)
	t := time.NewTimer(transientErrorBackoff)
	select {
	case <-ctx.Done():
		t.Stop()
		return ctx.Err()
	case <-t.C:
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
	customerID, batchHint, ok, err := w.Scheduler.Pick(ctx)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
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

	return deliveries, nil
}

func (w *Worker) claimMulti(ctx context.Context, store DeliveryStore) ([]outbox.Delivery, error) {
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
		return nil, nil
	}

	// Ensure each customer sent to ClaimNextBatch gets at least 2 LATERAL claim
	// slots (perCustomerClaimLimit >= 2). When totalLimit is small relative to
	// the number of picked customers the per-customer share would floor to 1,
	// causing N LATERAL subqueries that each lock exactly 1 row — wasteful under
	// sparse customer sets with low MinBatchHint. Excess customers are
	// re-notified immediately so they stay in the scheduler rotation.
	maxCustomers := totalLimit / 2
	if maxCustomers < 1 {
		maxCustomers = 1
	}
	if len(customerIDs) > maxCustomers {
		for _, cid := range customerIDs[maxCustomers:] {
			w.Scheduler.Notify(cid)
		}
		customerIDs = customerIDs[:maxCustomers]
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

type classified struct {
	delivery outbox.Delivery
	outcome  deliveryOutcome
}

func (w *Worker) processDeliveries(ctx context.Context, store DeliveryStore, deliveries []outbox.Delivery) error {
	for _, delivery := range deliveries {
		if delivery.BreakerProbe && w.Breaker != nil {
			w.Breaker.SetProbeDelivery(delivery.WebhookID, delivery.ID)
		}
	}

	results := make([]classified, len(deliveries))
	var batchWg sync.WaitGroup
	batchWg.Add(len(deliveries))
	for i, d := range deliveries {
		w.deliveryPool <- deliveryWork{
			ctx:      ctx,
			delivery: d,
			result:   &results[i],
			done:     batchWg.Done,
		}
	}
	batchWg.Wait()

	var succeeded, retries, dead []outbox.BatchMarkItem
	var requeued []string

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
				CustomerClass: delivery.CustomerClass,
			})
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
				CustomerClass: delivery.CustomerClass,
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
				CustomerClass: delivery.CustomerClass,
			})
			log.Warn("delivery scheduled for retry",
				"reason", outcome.errorClass,
				"retry_delay_ms", outcome.delay.Milliseconds(),
				"status_code", outcome.statusCode,
				"duration_ms", outcome.duration.Milliseconds(),
			)
		}
	}

	if len(requeued) > 0 {
		if err := store.RequeueBatch(ctx, requeued, w.requeueDelay()); err != nil {
			return fmt.Errorf("batch requeue: %w", err)
		}
	}
	if len(succeeded) > 0 || len(retries) > 0 || len(dead) > 0 {
		if err := store.MarkOutcomesBatch(ctx, succeeded, retries, dead); err != nil {
			return fmt.Errorf("batch mark outcomes: %w", err)
		}
		for _, r := range results {
			delivery, outcome := r.delivery, r.outcome
			if outcome.requeued {
				continue
			}
			customerClass := observe.CustomerClassFromValue(delivery.CustomerClass)
			switch {
			case outcome.reason == retry.ReasonSucceeded || (outcome.reason == "" && outcome.errorClass == ""):
				w.recordBreakerSuccess(delivery)
				recordSucceededMetrics(customerClass, outcome.lifetime.Seconds(), delivery.AttemptNumber)
			case outcome.reason != "":
				w.recordBreakerFailure(delivery, outcome.reason)
				recordDeadMetrics(customerClass, outcome.lifetime.Seconds(), delivery.AttemptNumber, outcome.reason)
			default:
				w.recordBreakerFailure(delivery, outcome.errorClass)
			}
		}
	}

	return nil
}

func (w *Worker) recordBreakerSuccess(delivery outbox.Delivery) {
	if w.Breaker == nil {
		return
	}
	w.Breaker.RecordSuccess(delivery.WebhookID)
}

func (w *Worker) recordBreakerFailure(delivery outbox.Delivery, reason string) {
	if w.Breaker == nil {
		return
	}
	w.Breaker.RecordFailure(delivery.WebhookID, reason)
}

func (w *Worker) requeueDelay() time.Duration {
	if w.HostLimiter != nil {
		return w.HostLimiter.RequeueDelay()
	}
	return 100 * time.Millisecond
}
