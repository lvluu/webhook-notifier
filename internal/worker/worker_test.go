package worker_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

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

func TestSend_SetsSignatureHeaders(t *testing.T) {
	t.Parallel()

	secret := []byte("webhook-secret")
	body := []byte(`{"event":"test"}`)

	var gotHeaders http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeaders = r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	w := worker.New()
	resp, err := w.Send(context.Background(), worker.Delivery{
		EventID:    "evt_sig",
		DeliveryID: "dlv_sig",
		CustomerID: "cus_sig",
		URL:        srv.URL,
		Body:       body,
		EventType:  "test.event",
		Secret:     string(secret),
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	defer resp.Body.Close()

	tsStr := gotHeaders.Get("X-Signature-Timestamp")
	if tsStr == "" {
		t.Fatal("X-Signature-Timestamp header missing")
	}
	sig := gotHeaders.Get("X-Signature-256")
	if sig == "" {
		t.Fatal("X-Signature-256 header missing")
	}

	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		t.Fatalf("parse timestamp: %v", err)
	}

	payload := fmt.Sprintf("%d.%s", ts, string(body))
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(payload))
	wantSig := hex.EncodeToString(mac.Sum(nil))

	if sig != wantSig {
		t.Errorf("X-Signature-256: got %s, want %s", sig, wantSig)
	}
}

func TestSend_SignatureTimestampIsRecent(t *testing.T) {
	t.Parallel()

	var tsStr string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tsStr = r.Header.Get("X-Signature-Timestamp")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	before := time.Now().Unix()

	w := worker.New()
	resp, err := w.Send(context.Background(), worker.Delivery{
		EventID:    "evt_ts",
		DeliveryID: "dlv_ts",
		URL:        srv.URL,
		Body:       []byte(`{}`),
		Secret:     "secret",
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	defer resp.Body.Close()

	after := time.Now().Unix()

	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		t.Fatalf("parse timestamp: %v", err)
	}
	if ts < before || ts > after {
		t.Errorf("timestamp %d not in [%d, %d]", ts, before, after)
	}
}

func TestIntegration_ReceiverVerifiesHMACAndStableDeliveryID(t *testing.T) {
	t.Parallel()

	secret := []byte("integration-secret")
	body := []byte(`{"type":"order.created"}`)

	var (
		mu          sync.Mutex
		receivedIDs []string
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tsStr := r.Header.Get("X-Signature-Timestamp")
		sig := r.Header.Get("X-Signature-256")
		deliveryID := r.Header.Get("X-Delivery-Id")

		if tsStr == "" || sig == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		ts, err := strconv.ParseInt(tsStr, 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		b, _ := io.ReadAll(r.Body)
		payload := fmt.Sprintf("%d.%s", ts, string(b))
		mac := hmac.New(sha256.New, secret)
		mac.Write([]byte(payload))
		expected := hex.EncodeToString(mac.Sum(nil))

		if !hmac.Equal([]byte(sig), []byte(expected)) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		mu.Lock()
		receivedIDs = append(receivedIDs, deliveryID)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	w := worker.New()
	deliveryID := "dlv_stable_1"

	for i := 1; i <= 3; i++ {
		resp, err := w.Send(context.Background(), worker.Delivery{
			EventID:       "evt_int",
			DeliveryID:    deliveryID,
			CustomerID:    "cus_int",
			URL:           srv.URL,
			Body:          body,
			EventType:     "order.created",
			Secret:        string(secret),
			AttemptNumber: i,
		})
		if err != nil {
			t.Fatalf("attempt %d: Send: %v", i, err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("attempt %d: status %d, want 200", i, resp.StatusCode)
		}
	}

	wrongSecretWorker := worker.New()
	badResp, err := wrongSecretWorker.Send(context.Background(), worker.Delivery{
		EventID:    "evt_bad",
		DeliveryID: "dlv_bad",
		URL:        srv.URL,
		Body:       body,
		Secret:     "wrong-secret",
	})
	if err != nil {
		t.Fatalf("bad sig send: %v", err)
	}
	badResp.Body.Close()

	if badResp.StatusCode != http.StatusUnauthorized {
		t.Errorf("wrong secret: status %d, want 401", badResp.StatusCode)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(receivedIDs) != 3 {
		t.Fatalf("expected 3 successful deliveries, got %d", len(receivedIDs))
	}
	for i, id := range receivedIDs {
		if id != deliveryID {
			t.Errorf("attempt %d: X-Delivery-Id = %q, want %q", i+1, id, deliveryID)
		}
	}
}
