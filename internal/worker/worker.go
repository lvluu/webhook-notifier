package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
)

// Delivery is the unit of work the Worker drives to terminal state.
// Fields mirror what the planned `deliveries` table row (#4) will hold,
// kept thin for the walking skeleton.
type Delivery struct {
	EventID    string
	DeliveryID string
	CustomerID string
	URL        string
	Body       []byte
	EventType  string
}

// Worker performs HTTP delivery of a single Delivery.
//
// The walking skeleton intentionally has no Scheduler, RetryPolicy, Signer,
// or per-host HostLimiter — those collaborators land in subsequent slices
// (#5-#8). The struct exists so those slices can add fields without
// changing this package's public surface.
type Worker struct {
	HTTP *http.Client
}

// New returns a Worker using http.DefaultClient.
func New() *Worker {
	return &Worker{HTTP: http.DefaultClient}
}

// Deliver POSTs the Delivery's Body to its URL and returns the response
// status code. No retry, no signing, no fairness — the skeleton path.
func (w *Worker) Deliver(ctx context.Context, d Delivery) (int, error) {
	if w.HTTP == nil {
		w.HTTP = http.DefaultClient
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.URL, bytes.NewReader(d.Body))
	if err != nil {
		return 0, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Event-Id", d.EventID)
	req.Header.Set("X-Delivery-Id", d.DeliveryID)
	req.Header.Set("X-Event-Type", d.EventType)

	resp, err := w.HTTP.Do(req)
	if err != nil {
		return 0, fmt.Errorf("post %s: %w", d.URL, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	return resp.StatusCode, nil
}
