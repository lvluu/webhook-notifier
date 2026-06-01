package worker

import (
	"context"
	"log/slog"

	"github.com/lvluu/webhook-notifier/internal/observe"
	"github.com/lvluu/webhook-notifier/internal/outbox"
	"github.com/lvluu/webhook-notifier/internal/retry"
)

// Process sends a claimed Delivery and records its success, retry, or death.
func (w *Worker) Process(ctx context.Context, store DeliveryStore, d outbox.Delivery) ProcessResult {
	outcome := w.classifyDelivery(ctx, d)
	if outcome.err != nil {
		return ProcessResult{Err: outcome.err}
	}
	if outcome.requeued {
		return ProcessResult{Status: StatusRequeued}
	}

	customerClass := observe.CustomerClassFromValue(d.CustomerClass)
	log := w.Logger.With(
		slog.String("delivery_id", d.ID),
		slog.String("event_id", d.EventID),
		slog.String("customer_id", d.CustomerID),
		slog.Int("attempt_number", d.AttemptNumber),
	)

	if outcome.reason == retry.ReasonSucceeded || (outcome.reason == "" && outcome.errorClass == "") {
		resultErr := store.MarkSucceeded(ctx, d.ID, outcome.statusCode, outcome.duration)
		if resultErr == nil {
			recordSucceededMetrics(customerClass, outcome.lifetime.Seconds(), d.AttemptNumber)
		}
		log.Info("delivery succeeded", "status_code", outcome.statusCode, "duration_ms", outcome.duration.Milliseconds())
		return ProcessResult{Err: resultErr}
	}
	if outcome.reason != "" {
		resultErr := store.MarkDead(ctx, d.ID, outcome.statusCode, outcome.errorClass, outcome.reason, outcome.duration)
		if resultErr == nil {
			recordDeadMetrics(customerClass, outcome.lifetime.Seconds(), d.AttemptNumber, outcome.reason)
		}
		log.Warn("delivery dead", "reason", outcome.reason, "status_code", outcome.statusCode, "duration_ms", outcome.duration.Milliseconds())
		return ProcessResult{Err: resultErr}
	}

	resultErr := store.MarkRetry(ctx, d.ID, outcome.statusCode, outcome.errorClass, outcome.delay, outcome.duration)
	logArgs := []any{"reason", outcome.errorClass, "retry_delay_ms", outcome.delay.Milliseconds(), "status_code", outcome.statusCode, "duration_ms", outcome.duration.Milliseconds()}
	log.Warn("delivery scheduled for retry", logArgs...)
	return ProcessResult{Err: resultErr}
}
