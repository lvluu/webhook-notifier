package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/lvluu/webhook-notifier/internal/outbox"
	"github.com/lvluu/webhook-notifier/internal/retry"
	"github.com/lvluu/webhook-notifier/internal/scheduler"
	"github.com/lvluu/webhook-notifier/internal/signer"
)

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

type ProcessStatus int

const (
	StatusSucceeded ProcessStatus = iota
	StatusRequeued
)

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

type Worker struct {
	HTTP        *http.Client
	RetryPolicy retry.RetryPolicy
	Scheduler   scheduler.Scheduler
	HostLimiter *HostLimiter
}

type Option func(*Worker)

func WithHTTPClient(c *http.Client) Option {
	return func(w *Worker) { w.HTTP = c }
}

func WithRetryPolicy(rp retry.RetryPolicy) Option {
	return func(w *Worker) { w.RetryPolicy = rp }
}

func WithHostLimiter(hl *HostLimiter) Option {
	return func(w *Worker) { w.HostLimiter = hl }
}

func New(s scheduler.Scheduler, opts ...Option) *Worker {
	w := &Worker{
		HTTP:        http.DefaultClient,
		RetryPolicy: retry.New(retry.Config{}),
		Scheduler:   s,
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

func (w *Worker) Deliver(ctx context.Context, d Delivery) (int, error) {
	resp, err := w.Send(ctx, d)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	return resp.StatusCode, nil
}

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
	if resp != nil {
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
	}

	statusCode := 0
	if resp != nil {
		statusCode = resp.StatusCode
	}

	delay, terminal, reason := w.RetryPolicy.Next(d.AttemptNumber, d.PreviousDelay, resp, err)
	if reason == retry.ReasonSucceeded {
		return ProcessResult{Err: store.MarkSucceeded(ctx, d.ID, statusCode, duration)}
	}
	if terminal {
		return ProcessResult{Err: store.MarkDead(ctx, d.ID, statusCode, reason, reason, duration)}
	}

	return ProcessResult{Err: store.MarkRetry(ctx, d.ID, statusCode, reason, delay, duration)}
}

const idleBackoff = 50 * time.Millisecond

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
