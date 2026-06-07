package worker

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/lvluu/webhook-notifier/internal/observe"
	"github.com/lvluu/webhook-notifier/internal/outbox"
	"github.com/lvluu/webhook-notifier/internal/retry"
)

// deliveryOutcome captures the result of sending a delivery and classifying the
// response, before any database mark is applied.
type deliveryOutcome struct {
	statusCode int
	duration   time.Duration
	lifetime   time.Duration
	errorClass string
	delay      time.Duration
	reason     string
	requeued   bool
	err        error
}

// classifyDelivery sends the HTTP request and classifies the outcome without
// touching the database. Separated from Process so the Run loop can batch
// multiple outcomes into a single round trip per outcome kind.
func (w *Worker) classifyDelivery(ctx context.Context, d outbox.Delivery) deliveryOutcome {
	observe.IncDeliveriesInFlight()
	defer observe.DecDeliveriesInFlight()

	customerClass := observe.CustomerClassFromValue(d.CustomerClass)
	if w.HostLimiter != nil {
		// d.URLHost is projected from webhooks.url_host (a generated column,
		// issue #115) so the limiter avoids re-parsing the URL on every Attempt.
		// Fall back to url.Parse for rows where the host could not be extracted
		// (malformed scheme, IPv6 literal) so behaviour never regresses.
		// The parsed result is cached in urlHostCache to avoid re-parsing the
		// same URL on subsequent attempts.
		host := d.URLHost
		if host == "" {
			if cached, ok := w.urlHostCache.Load(d.URL); ok {
				host = cached.(string)
			} else {
				u, parseErr := url.Parse(d.URL)
				if parseErr != nil {
					return deliveryOutcome{err: fmt.Errorf("parse delivery URL: %w", parseErr)}
				}
				host = u.Hostname()
				w.urlHostCache.Store(d.URL, host)
			}
		}

		release, acquireErr := w.HostLimiter.TryAcquire(host)
		if acquireErr != nil {
			if ctx.Err() != nil {
				return deliveryOutcome{err: ctx.Err()}
			}
			return deliveryOutcome{requeued: true}
		}
		defer release()
	}

	createdAt := d.CreatedAt
	lifetime := time.Since(createdAt)

	// Pre-flight MaxAge check
	_, terminal, reason := w.RetryPolicy.NextWithKey(d.AttemptNumber, d.PreviousDelay, nil, nil, d.ID, lifetime)
	if terminal && reason == retry.ReasonAgeExceeded {
		w.Logger.Warn("delivery expired pre-flight, skipping HTTP call",
			"delivery_id", d.ID,
			"lifetime_ms", lifetime.Milliseconds(),
			"max_age_ms", w.RetryPolicy.MaxAge.Milliseconds(),
		)
		return deliveryOutcome{
			lifetime:   lifetime,
			errorClass: reason,
			reason:     reason,
		}
	}

	started := time.Now()
	if d.AttemptNumber == 1 {
		observe.ObserveQueueWait(customerClass, started.Sub(d.CreatedAt).Seconds())
	}
	resp, err := w.Send(ctx, Delivery{
		EventID:       d.EventID,
		DeliveryID:    d.ID,
		CustomerID:    d.CustomerID,
		URL:           d.URL,
		Body:          d.Payload,
		EventType:     d.EventType,
		Secret:        d.Secret,
		AttemptNumber: d.AttemptNumber,
	})
	duration := time.Since(started)
	observe.ObservePostDuration(customerClass, duration.Seconds())
	lifetime = time.Since(createdAt)

	if resp != nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		if closeErr := resp.Body.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("close response body: %w", closeErr)
		}
	}

	statusCode := 0
	if resp != nil {
		statusCode = resp.StatusCode
	}

	delay, terminal, reason := w.RetryPolicy.NextWithKey(d.AttemptNumber, d.PreviousDelay, resp, err, d.ID, lifetime)
	observe.IncAttempt(reason)

	if reason == retry.ReasonSucceeded {
		return deliveryOutcome{
			statusCode: statusCode,
			duration:   duration,
			lifetime:   lifetime,
		}
	}
	if terminal {
		return deliveryOutcome{
			statusCode: statusCode,
			duration:   duration,
			lifetime:   lifetime,
			errorClass: reason,
			reason:     reason,
		}
	}
	return deliveryOutcome{
		statusCode: statusCode,
		duration:   duration,
		errorClass: reason,
		delay:      delay,
	}
}
