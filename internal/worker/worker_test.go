package worker_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/lvluu/webhook-notifier/internal/worker"
)

// TestDeliver_PostsEventToReceiver is the walking-skeleton end-to-end test:
// hand one Delivery to the Worker, observe the resulting POST at an
// httptest.Server, and assert the headers and body match what we sent.
//
// This intentionally exercises no fairness, retry, signing, or per-host
// limiter — those are added in subsequent slices (#5-#8).
func TestDeliver_PostsEventToReceiver(t *testing.T) {
	t.Parallel()

	var got struct {
		Method  string
		Headers http.Header
		Body    map[string]any
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.Method = r.Method
		got.Headers = r.Header.Clone()
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &got.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	payload, err := json.Marshal(map[string]any{
		"type":          "subscriber.created",
		"subscriber_id": "sub_123",
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	d := worker.Delivery{
		EventID:    "evt_1",
		DeliveryID: "dlv_1",
		CustomerID: "cus_1",
		URL:        srv.URL,
		Body:       payload,
		EventType:  "subscriber.created",
	}

	w := worker.New()
	status, err := w.Deliver(context.Background(), d)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	if status != http.StatusOK {
		t.Errorf("status: got %d, want 200", status)
	}
	if got.Method != http.MethodPost {
		t.Errorf("method: got %q, want POST", got.Method)
	}
	if ct := got.Headers.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type: got %q, want application/json", ct)
	}
	if id := got.Headers.Get("X-Event-Id"); id != "evt_1" {
		t.Errorf("X-Event-Id: got %q, want evt_1", id)
	}
	if id := got.Headers.Get("X-Delivery-Id"); id != "dlv_1" {
		t.Errorf("X-Delivery-Id: got %q, want dlv_1", id)
	}
	if et := got.Headers.Get("X-Event-Type"); et != "subscriber.created" {
		t.Errorf("X-Event-Type: got %q, want subscriber.created", et)
	}
	if got.Body["subscriber_id"] != "sub_123" {
		t.Errorf("body subscriber_id: got %#v, want sub_123", got.Body["subscriber_id"])
	}
}

// TestDeliver_PropagatesContextCancellation guards against a worker that
// silently ignores ctx cancellation — important once retries land (#5) and
// shutdown semantics matter.
func TestDeliver_PropagatesContextCancellation(t *testing.T) {
	t.Parallel()

	hang := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-hang
	}))
	t.Cleanup(func() {
		close(hang)
		srv.Close()
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel

	w := worker.New()
	_, err := w.Deliver(ctx, worker.Delivery{
		EventID:    "evt_cancel",
		DeliveryID: "dlv_cancel",
		CustomerID: "cus_cancel",
		URL:        srv.URL,
		Body:       []byte(`{}`),
		EventType:  "test.event",
	})
	if err == nil {
		t.Fatal("expected error from cancelled context, got nil")
	}
}
