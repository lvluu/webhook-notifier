package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/lvluu/webhook-notifier/internal/observe"
	"github.com/lvluu/webhook-notifier/internal/outbox"
	"github.com/lvluu/webhook-notifier/internal/retry"
	"github.com/lvluu/webhook-notifier/internal/scheduler"
	"github.com/lvluu/webhook-notifier/internal/signer"
)

// Delivery contains the outbound HTTP request data for one webhook delivery.
type Delivery struct {
	EventID       string
	DeliveryID    string
	CustomerID    string
	URL           string
	Body          []byte
	EventType     string
	Secret        string
	AttemptNumber int
}

// ProcessStatus describes the outcome of processing one claimed Delivery.
type ProcessStatus int

const (
	// StatusSucceeded means the Delivery reached a terminal success/dead state.
	StatusSucceeded ProcessStatus = iota
	// StatusRequeued means host-level backpressure deferred this Delivery.
	StatusRequeued
)

// ProcessResult is the outcome returned after processing a Delivery.
type ProcessResult struct {
	Status ProcessStatus
	Err    error
}

// DeliveryStore mutates and claims Delivery rows. Satisfied by *outbox.Store.
type DeliveryStore interface {
	ClaimNextN(ctx context.Context, customerID string, limit int) ([]outbox.Delivery, error)
	MarkSucceeded(ctx context.Context, deliveryID string, statusCode int, duration time.Duration) error
	MarkRetry(ctx context.Context, deliveryID string, statusCode int, errorClass string, delay time.Duration, duration time.Duration) error
	MarkDead(ctx context.Context, deliveryID string, statusCode int, errorClass string, reason string, duration time.Duration) error
	Requeue(ctx context.Context, deliveryID string, delay time.Duration) error
}

// Worker sends webhook Deliveries and records retry outcomes.
type Worker struct {
	HTTP        *http.Client
	RetryPolicy retry.RetryPolicy
	Scheduler   scheduler.Scheduler
	HostLimiter *HostLimiter
	Logger      *slog.Logger
}

// Option configures a Worker.
type Option func(*Worker)

// WithHTTPClient sets the HTTP client used for webhook requests.
func WithHTTPClient(c *http.Client) Option {
	return func(w *Worker) { w.HTTP = c }
}

// WithRetryPolicy sets the retry policy used to classify delivery attempts.
func WithRetryPolicy(rp retry.RetryPolicy) Option {
	return func(w *Worker) { w.RetryPolicy = rp }
}

// WithHostLimiter sets the optional per-host concurrency limiter.
func WithHostLimiter(hl *HostLimiter) Option {
	return func(w *Worker) { w.HostLimiter = hl }
}

// WithLogger sets the base structured logger used by the Worker.
func WithLogger(logger *slog.Logger) Option {
	return func(w *Worker) { w.Logger = logger }
}

// New returns a Worker using scheduler s and optional overrides.
func New(s scheduler.Scheduler, opts ...Option) *Worker {
	w := &Worker{
		HTTP:        http.DefaultClient,
		RetryPolicy: retry.New(retry.Config{}),
		Scheduler:   s,
		Logger:      observe.NewLogger(nil),
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

// Deliver sends d and returns the HTTP status code.
func (w *Worker) Deliver(ctx context.Context, d Delivery) (int, error) {
	resp, err := w.Send(ctx, d)
	if err != nil {
		return 0, err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	if err := resp.Body.Close(); err != nil {
		return 0, fmt.Errorf("close response body: %w", err)
	}

	return resp.StatusCode, nil
}

// Send builds and executes the signed webhook HTTP request for d.
func (w *Worker) Send(ctx context.Context, d Delivery) (*http.Response, error) {
	if w.HTTP == nil {
		w.HTTP = http.DefaultClient
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.URL, bytes.NewReader(d.Body))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Event-Id", d.EventID)
	req.Header.Set("X-Delivery-Id", d.DeliveryID)
	req.Header.Set("X-Event-Type", d.EventType)
	if d.AttemptNumber > 0 {
		req.Header.Set("X-Attempt-Number", fmt.Sprintf("%d", d.AttemptNumber))
	}
	if d.Secret != "" {
		ts := time.Now()
		req.Header.Set("X-Signature-Timestamp", strconv.FormatInt(ts.Unix(), 10))
		req.Header.Set("X-Signature-256", signer.Sign([]byte(d.Secret), d.Body, ts))
	}

	resp, err := w.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("post %s: %w", d.URL, err)
	}

	return resp, nil
}

// Process sends a claimed Delivery and records its success, retry, or death.
func (w *Worker) Process(ctx context.Context, store DeliveryStore, d outbox.Delivery) ProcessResult {
	if w.HostLimiter != nil {
		u, parseErr := url.Parse(d.URL)
		if parseErr != nil {
			return ProcessResult{Err: fmt.Errorf("parse delivery URL: %w", parseErr)}
		}

		release, acquireErr := w.HostLimiter.Acquire(ctx, u.Hostname())
		if acquireErr != nil {
			if ctx.Err() != nil {
				return ProcessResult{Err: ctx.Err()}
			}
			return ProcessResult{Status: StatusRequeued}
		}
		defer release()
	}

	started := time.Now()
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
	deliveryLifetime := time.Since(d.CreatedAt)
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

	delay, terminal, reason := w.RetryPolicy.Next(d.AttemptNumber, d.PreviousDelay, resp, err)

	customerClass := observe.CustomerClassFromWeight(d.CustomerWeight)
	log := w.Logger.With(
		slog.String("delivery_id", d.ID),
		slog.String("event_id", d.EventID),
		slog.String("customer_id", d.CustomerID),
		slog.Int("attempt_number", d.AttemptNumber),
	)

	if reason == retry.ReasonSucceeded {
		resultErr := store.MarkSucceeded(ctx, d.ID, statusCode, duration)
		if resultErr == nil {
			observe.IncDelivery("succeeded")
			observe.ObserveDeliveryLatency(customerClass, deliveryLifetime.Seconds())
			observe.ObserveAttempts(float64(d.AttemptNumber))
		}
		log.Info("delivery succeeded", "status_code", statusCode, "duration_ms", duration.Milliseconds())
		return ProcessResult{Err: resultErr}
	}
	if terminal {
		resultErr := store.MarkDead(ctx, d.ID, statusCode, reason, reason, duration)
		if resultErr == nil {
			observe.IncDelivery("dead")
			observe.ObserveDeliveryLatency(customerClass, deliveryLifetime.Seconds())
			observe.ObserveAttempts(float64(d.AttemptNumber))
			observe.IncDlq(reason)
		}
		log.Warn("delivery dead", "reason", reason, "status_code", statusCode, "duration_ms", duration.Milliseconds())
		return ProcessResult{Err: resultErr}
	}

	resultErr := store.MarkRetry(ctx, d.ID, statusCode, reason, delay, duration)
	log.Warn("delivery scheduled for retry", "reason", reason, "retry_delay_ms", delay.Milliseconds(), "status_code", statusCode, "duration_ms", duration.Milliseconds())
	return ProcessResult{Err: resultErr}
}

const idleBackoff = 50 * time.Millisecond

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

		deliveries, err := store.ClaimNextN(ctx, customerID, int(batchHint))
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("claim delivery for %s: %w", customerID, err)
		}
		if len(deliveries) == 0 {
			continue
		}

		for i, delivery := range deliveries {
			result := w.Process(ctx, store, delivery)
			if result.Err != nil {
				return fmt.Errorf("process delivery %s: %w", delivery.ID, result.Err)
			}
			if result.Status == StatusRequeued {
				delay := w.requeueDelay()
				if err := store.Requeue(ctx, delivery.ID, delay); err != nil {
					return fmt.Errorf("requeue delivery %s: %w", delivery.ID, err)
				}
				for _, remaining := range deliveries[i+1:] {
					if err := store.Requeue(ctx, remaining.ID, delay); err != nil {
						return fmt.Errorf("requeue delivery %s: %w", remaining.ID, err)
					}
				}
				w.Scheduler.Notify(customerID)
				break
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
