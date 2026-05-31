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
	"sync"
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
	HasClaimableWork(ctx context.Context, customerID string) (bool, error)
	MarkSucceeded(ctx context.Context, deliveryID string, statusCode int, duration time.Duration) error
	MarkRetry(ctx context.Context, deliveryID string, statusCode int, errorClass string, delay time.Duration, duration time.Duration) error
	MarkDead(ctx context.Context, deliveryID string, statusCode int, errorClass string, reason string, duration time.Duration) error
	Requeue(ctx context.Context, deliveryID string, delay time.Duration) error
	MarkSucceededBatch(ctx context.Context, items []outbox.BatchMarkItem) error
	MarkRetryBatch(ctx context.Context, items []outbox.BatchMarkItem) error
	MarkDeadBatch(ctx context.Context, items []outbox.BatchMarkItem) error
}

// Worker sends webhook Deliveries and records retry outcomes.
type Worker struct {
	HTTP           *http.Client
	RetryPolicy    retry.RetryPolicy
	Scheduler      scheduler.Scheduler
	HostLimiter    *HostLimiter
	Logger         *slog.Logger
	RequestTimeout time.Duration
	MinBatchHint   int
}

// HTTPConfig configures the shared HTTP client used for webhook Attempts.
type HTTPConfig struct {
	MaxIdleConns          int
	MaxIdleConnsPerHost   int
	MaxConnsPerHost       int
	IdleConnTimeout       time.Duration
	TLSHandshakeTimeout   time.Duration
	ResponseHeaderTimeout time.Duration
	ExpectContinueTimeout time.Duration
}

// DefaultHTTPConfig returns the worker's production-oriented HTTP transport
// defaults.
func DefaultHTTPConfig() HTTPConfig {
	return HTTPConfig{
		MaxIdleConns:          10000,
		MaxIdleConnsPerHost:   500,
		MaxConnsPerHost:       0,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
}

// NewHTTPClient returns a shared HTTP client configured for webhook delivery.
func NewHTTPClient(config HTTPConfig) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:          config.MaxIdleConns,
			MaxIdleConnsPerHost:   config.MaxIdleConnsPerHost,
			MaxConnsPerHost:       config.MaxConnsPerHost,
			IdleConnTimeout:       config.IdleConnTimeout,
			TLSHandshakeTimeout:   config.TLSHandshakeTimeout,
			ResponseHeaderTimeout: config.ResponseHeaderTimeout,
			ExpectContinueTimeout: config.ExpectContinueTimeout,
		},
	}
}

// Option configures a Worker.
type Option func(*Worker)

// WithHTTPClient sets the HTTP client used for webhook requests.
func WithHTTPClient(c *http.Client) Option {
	return func(w *Worker) { w.HTTP = c }
}

// WithHTTPConfig configures the worker's shared HTTP client transport.
func WithHTTPConfig(config HTTPConfig) Option {
	return func(w *Worker) { w.HTTP = NewHTTPClient(config) }
}

// WithRequestTimeout sets the per-Attempt request timeout.
func WithRequestTimeout(timeout time.Duration) Option {
	return func(w *Worker) {
		if timeout > 0 {
			w.RequestTimeout = timeout
		}
	}
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

// WithMinBatchHint sets the minimum batch size used during ClaimNextN.
func WithMinBatchHint(minBatchHint int) Option {
	return func(w *Worker) {
		if minBatchHint > 0 {
			w.MinBatchHint = minBatchHint
		}
	}
}

// New returns a Worker using scheduler s and optional overrides.
func New(s scheduler.Scheduler, opts ...Option) *Worker {
	w := &Worker{
		HTTP:           NewHTTPClient(DefaultHTTPConfig()),
		RetryPolicy:    retry.New(retry.Config{}),
		Scheduler:      s,
		Logger:         observe.NewLogger(nil),
		RequestTimeout: 30 * time.Second,
		MinBatchHint:   32,
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
		w.HTTP = NewHTTPClient(DefaultHTTPConfig())
	}
	var cancel context.CancelFunc
	if w.RequestTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, w.RequestTimeout)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.URL, bytes.NewReader(d.Body))
	if err != nil {
		if cancel != nil {
			cancel()
		}
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
		if cancel != nil {
			cancel()
		}
		return nil, fmt.Errorf("post %s: %w", d.URL, err)
	}
	if cancel != nil && resp != nil && resp.Body != nil {
		resp.Body = cancelOnCloseReadCloser{
			ReadCloser: resp.Body,
			cancel:     cancel,
		}
	}

	return resp, nil
}

type cancelOnCloseReadCloser struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c cancelOnCloseReadCloser) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()
	return err
}

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
	if w.HostLimiter != nil {
		u, parseErr := url.Parse(d.URL)
		if parseErr != nil {
			return deliveryOutcome{err: fmt.Errorf("parse delivery URL: %w", parseErr)}
		}

		release, acquireErr := w.HostLimiter.Acquire(ctx, u.Hostname())
		if acquireErr != nil {
			if ctx.Err() != nil {
				return deliveryOutcome{err: ctx.Err()}
			}
			return deliveryOutcome{requeued: true}
		}
		defer release()
	}

	lifetime := time.Since(d.CreatedAt)

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
	lifetime = time.Since(d.CreatedAt)

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
			observe.IncDelivery("succeeded")
			observe.ObserveDeliveryLatency(customerClass, outcome.lifetime.Seconds())
			observe.ObserveAttempts(float64(d.AttemptNumber))
		}
		log.Info("delivery succeeded", "status_code", outcome.statusCode, "duration_ms", outcome.duration.Milliseconds())
		return ProcessResult{Err: resultErr}
	}
	if outcome.reason != "" {
		resultErr := store.MarkDead(ctx, d.ID, outcome.statusCode, outcome.errorClass, outcome.reason, outcome.duration)
		if resultErr == nil {
			observe.IncDelivery("dead")
			observe.ObserveDeliveryLatency(customerClass, outcome.lifetime.Seconds())
			observe.ObserveAttempts(float64(d.AttemptNumber))
			observe.IncDlq(outcome.reason)
		}
		log.Warn("delivery dead", "reason", outcome.reason, "status_code", outcome.statusCode, "duration_ms", outcome.duration.Milliseconds())
		return ProcessResult{Err: resultErr}
	}

	resultErr := store.MarkRetry(ctx, d.ID, outcome.statusCode, outcome.errorClass, outcome.delay, outcome.duration)
	logArgs := []any{"reason", outcome.errorClass, "retry_delay_ms", outcome.delay.Milliseconds(), "status_code", outcome.statusCode, "duration_ms", outcome.duration.Milliseconds()}
	log.Warn("delivery scheduled for retry", logArgs...)
	return ProcessResult{Err: resultErr}
}

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
		sem := make(chan struct{}, 16) // max concurrent HTTP calls per worker
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
				observe.IncDelivery("succeeded")
				observe.ObserveDeliveryLatency(customerClass, item.Lifetime.Seconds())
				observe.ObserveAttempts(float64(item.AttemptNumber))
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
				observe.IncDelivery("dead")
				observe.ObserveDeliveryLatency(customerClass, item.Lifetime.Seconds())
				observe.ObserveAttempts(float64(item.AttemptNumber))
				observe.IncDlq(item.Reason)
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
