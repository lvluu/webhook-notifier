package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/lvluu/webhook-notifier/internal/outbox"
	"github.com/lvluu/webhook-notifier/internal/retry"
	"github.com/lvluu/webhook-notifier/internal/signer"
)

// Delivery is the unit of work the Worker drives to terminal state.
// Fields mirror what the planned `deliveries` table row (#4) will hold,
// kept thin for the walking skeleton.
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

// Worker performs HTTP delivery of a single Delivery.
//
// The walking skeleton intentionally has no Scheduler or per-host
// HostLimiter — those collaborators land in subsequent slices (#5-#8).
// The struct exists so those slices can add fields without changing
// this package's public surface.
type Worker struct {
	HTTP        *http.Client
	RetryPolicy retry.RetryPolicy
}

// New returns a Worker using http.DefaultClient.
func New() *Worker {
	return &Worker{
		HTTP:        http.DefaultClient,
		RetryPolicy: retry.New(retry.Config{}),
	}
}

// Deliver POSTs the Delivery's Body to its URL and returns the response
// status code. No retry, no fairness — the skeleton path. Signing is
// applied when the Delivery carries a Secret.
func (w *Worker) Deliver(ctx context.Context, d Delivery) (int, error) {
	resp, err := w.Send(ctx, d)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	return resp.StatusCode, nil
}

// Send POSTs one Attempt and returns the raw response so callers can classify
// status code and Retry-After headers before the body is closed.
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

// Process sends a claimed Delivery and persists the resulting lifecycle
// transition: succeeded, failed_retry, or dead.
func (w *Worker) Process(ctx context.Context, store *outbox.Store, d outbox.Delivery) error {
	if store == nil {
		return fmt.Errorf("outbox store is required")
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
		return store.MarkSucceeded(ctx, d.ID, statusCode, duration)
	}
	if terminal {
		return store.MarkDead(ctx, d.ID, statusCode, reason, reason, duration)
	}

	return store.MarkRetry(ctx, d.ID, statusCode, reason, delay, duration)
}
