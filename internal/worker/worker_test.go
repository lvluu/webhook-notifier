package worker_test

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/lvluu/webhook-notifier/internal/outbox"
	"github.com/lvluu/webhook-notifier/internal/retry"
	"github.com/lvluu/webhook-notifier/internal/worker"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type stubScheduler struct{}

func (stubScheduler) Pick(_ context.Context) (string, uint32, bool, error) { return "", 0, false, nil }
func (stubScheduler) Notify(string)                                        {}
func (stubScheduler) Remove(string)                                        {}

type recordingScheduler struct {
	mu       sync.Mutex
	notified []string
}

func (r *recordingScheduler) Pick(_ context.Context) (string, uint32, bool, error) {
	return "", 0, false, nil
}

func (r *recordingScheduler) Remove(string) {}

func (r *recordingScheduler) Notify(customerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.notified = append(r.notified, customerID)
}

func (r *recordingScheduler) Notified() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.notified))
	copy(out, r.notified)
	return out
}

type oneShotScheduler struct {
	mu       sync.Mutex
	picked   bool
	customer string
	removed  []string
	notified []string
}

func (s *oneShotScheduler) Pick(ctx context.Context) (string, uint32, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.picked {
		s.picked = true
		if s.customer == "" {
			s.customer = "cus_future_only"
		}
		return s.customer, 1, true, nil
	}
	return "", 0, false, ctx.Err()
}

func (s *oneShotScheduler) Notify(customerID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.notified = append(s.notified, customerID)
}

func (s *oneShotScheduler) Remove(customerID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.removed = append(s.removed, customerID)
}

func (s *oneShotScheduler) Removed() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.removed))
	copy(out, s.removed)
	return out
}

func (s *oneShotScheduler) Notified() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.notified))
	copy(out, s.notified)
	return out
}

type recordingClaimableCustomerStore struct {
	mu        sync.Mutex
	responses []claimableCustomerResponse
	calls     chan string
}

type claimableCustomerResponse struct {
	customers []string
	err       error
}

func (r *recordingClaimableCustomerStore) DiscoverClaimableCustomers(_ context.Context, afterCustomerID string, _ int) ([]string, error) {
	r.calls <- afterCustomerID

	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.responses) == 0 {
		return nil, nil
	}
	response := r.responses[0]
	r.responses = r.responses[1:]
	if response.err != nil {
		return nil, response.err
	}
	out := make([]string, len(response.customers))
	copy(out, response.customers)
	return out, nil
}

func newTestWorker(opts ...worker.Option) *worker.Worker {
	return worker.New(stubScheduler{}, opts...)
}

type recordingDeliveryStore struct {
	succeeded int
	retried   int
	dead      int
}

func (r *recordingDeliveryStore) ClaimNextN(context.Context, string, int) ([]outbox.Delivery, error) {
	return nil, nil
}

func (r *recordingDeliveryStore) ClaimNextBatch(context.Context, []string, int) ([]outbox.Delivery, error) {
	return nil, nil
}

func (r *recordingDeliveryStore) HasClaimableWorkBatch(context.Context, []string) (map[string]bool, error) {
	return nil, nil
}

func (r *recordingDeliveryStore) HasClaimableWork(context.Context, string) (bool, error) {
	return false, nil
}

func (r *recordingDeliveryStore) MarkSucceeded(context.Context, string, int, time.Duration) error {
	r.succeeded++
	return nil
}

func (r *recordingDeliveryStore) MarkRetry(context.Context, string, int, string, time.Duration, time.Duration) error {
	r.retried++
	return nil
}

func (r *recordingDeliveryStore) MarkDead(context.Context, string, int, string, string, time.Duration) error {
	r.dead++
	return nil
}

func (r *recordingDeliveryStore) Requeue(context.Context, string, time.Duration) error {
	return nil
}

func (r *recordingDeliveryStore) MarkSucceededBatch(context.Context, []outbox.BatchMarkItem) error {
	return nil
}

func (r *recordingDeliveryStore) MarkRetryBatch(context.Context, []outbox.BatchMarkItem) error {
	return nil
}

func (r *recordingDeliveryStore) MarkDeadBatch(context.Context, []outbox.BatchMarkItem) error {
	return nil
}

type removeOnEmptyClaimStore struct {
	done            chan struct{}
	once            sync.Once
	claimCalls      int
	claimableChecks int
	claimableResult bool
	claimableErr    error
}

func (s *removeOnEmptyClaimStore) ClaimNextN(context.Context, string, int) ([]outbox.Delivery, error) {
	s.claimCalls++
	return nil, nil
}

func (s *removeOnEmptyClaimStore) ClaimNextBatch(context.Context, []string, int) ([]outbox.Delivery, error) {
	return nil, nil
}

func (s *removeOnEmptyClaimStore) HasClaimableWorkBatch(context.Context, []string) (map[string]bool, error) {
	return nil, nil
}

func (s *removeOnEmptyClaimStore) HasClaimableWork(context.Context, string) (bool, error) {
	s.claimableChecks++
	s.once.Do(func() {
		close(s.done)
	})
	return s.claimableResult, s.claimableErr
}

func (s *removeOnEmptyClaimStore) MarkSucceeded(context.Context, string, int, time.Duration) error {
	return nil
}
func (s *removeOnEmptyClaimStore) MarkRetry(context.Context, string, int, string, time.Duration, time.Duration) error {
	return nil
}
func (s *removeOnEmptyClaimStore) MarkDead(context.Context, string, int, string, string, time.Duration) error {
	return nil
}
func (s *removeOnEmptyClaimStore) Requeue(context.Context, string, time.Duration) error { return nil }
func (s *removeOnEmptyClaimStore) MarkSucceededBatch(context.Context, []outbox.BatchMarkItem) error {
	return nil
}
func (s *removeOnEmptyClaimStore) MarkRetryBatch(context.Context, []outbox.BatchMarkItem) error {
	return nil
}
func (s *removeOnEmptyClaimStore) MarkDeadBatch(context.Context, []outbox.BatchMarkItem) error {
	return nil
}

func TestRun_RemovesCustomerWhenEmptyClaimHasNoClaimableWork(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sched := &oneShotScheduler{customer: "cus_future_only"}
	store := &removeOnEmptyClaimStore{
		done:            make(chan struct{}),
		claimableResult: false,
	}
	w := worker.New(sched)

	errCh := make(chan error, 1)
	go func() {
		errCh <- w.Run(ctx, store)
	}()

	<-store.done
	cancel()

	err := <-errCh
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error: got %v, want context.Canceled", err)
	}
	if store.claimCalls != 1 {
		t.Fatalf("ClaimNextN calls: got %d, want 1", store.claimCalls)
	}
	if store.claimableChecks != 1 {
		t.Fatalf("HasClaimableWork calls: got %d, want 1", store.claimableChecks)
	}
	removed := sched.Removed()
	if !slices.Equal(removed, []string{"cus_future_only"}) {
		t.Fatalf("Scheduler.Remove calls: got %v, want [cus_future_only]", removed)
	}
	if notified := sched.Notified(); len(notified) != 0 {
		t.Fatalf("Scheduler.Notify calls: got %v, want none", notified)
	}
}

func TestRun_NotifiesCustomerWhenEmptyClaimHasClaimableWork(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sched := &oneShotScheduler{customer: "cus_claimable"}
	store := &removeOnEmptyClaimStore{
		done:            make(chan struct{}),
		claimableResult: true,
	}
	w := worker.New(sched)

	errCh := make(chan error, 1)
	go func() {
		errCh <- w.Run(ctx, store)
	}()

	<-store.done
	cancel()

	err := <-errCh
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error: got %v, want context.Canceled", err)
	}
	if removed := sched.Removed(); len(removed) != 0 {
		t.Fatalf("Scheduler.Remove calls: got %v, want none", removed)
	}
	if notified := sched.Notified(); !slices.Equal(notified, []string{"cus_claimable"}) {
		t.Fatalf("Scheduler.Notify calls: got %v, want [cus_claimable]", notified)
	}
}

func TestRun_NotifiesCustomerWhenClaimabilityCheckErrors(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sched := &oneShotScheduler{customer: "cus_check_error"}
	store := &removeOnEmptyClaimStore{
		done:         make(chan struct{}),
		claimableErr: errors.New("db down"),
	}
	w := worker.New(sched)

	errCh := make(chan error, 1)
	go func() {
		errCh <- w.Run(ctx, store)
	}()

	<-store.done
	cancel()

	err := <-errCh
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error: got %v, want context.Canceled", err)
	}
	if removed := sched.Removed(); len(removed) != 0 {
		t.Fatalf("Scheduler.Remove calls: got %v, want none", removed)
	}
	if notified := sched.Notified(); !slices.Equal(notified, []string{"cus_check_error"}) {
		t.Fatalf("Scheduler.Notify calls: got %v, want [cus_check_error]", notified)
	}
}

func TestNew_DefaultHTTPClientUsesTunedTransport(t *testing.T) {
	t.Parallel()

	w := newTestWorker()

	transport, ok := w.HTTP.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("HTTP.Transport: got %T, want *http.Transport", w.HTTP.Transport)
	}

	if transport.MaxIdleConns != 10000 {
		t.Errorf("MaxIdleConns: got %d, want 10000", transport.MaxIdleConns)
	}
	if transport.MaxIdleConnsPerHost != 500 {
		t.Errorf("MaxIdleConnsPerHost: got %d, want 500", transport.MaxIdleConnsPerHost)
	}
	if transport.MaxConnsPerHost != 0 {
		t.Errorf("MaxConnsPerHost: got %d, want 0", transport.MaxConnsPerHost)
	}
	if transport.IdleConnTimeout != 90*time.Second {
		t.Errorf("IdleConnTimeout: got %s, want 90s", transport.IdleConnTimeout)
	}
	if transport.TLSHandshakeTimeout != 10*time.Second {
		t.Errorf("TLSHandshakeTimeout: got %s, want 10s", transport.TLSHandshakeTimeout)
	}
	if transport.ResponseHeaderTimeout <= 0 {
		t.Errorf("ResponseHeaderTimeout: got %s, want positive timeout", transport.ResponseHeaderTimeout)
	}
	if transport.ExpectContinueTimeout <= 0 {
		t.Errorf("ExpectContinueTimeout: got %s, want positive timeout", transport.ExpectContinueTimeout)
	}
}

func TestNew_HTTPConfigOptionOverridesTransportDefaults(t *testing.T) {
	t.Parallel()

	w := newTestWorker(worker.WithHTTPConfig(worker.HTTPConfig{
		MaxIdleConns:          111,
		MaxIdleConnsPerHost:   22,
		MaxConnsPerHost:       33,
		IdleConnTimeout:       44 * time.Second,
		TLSHandshakeTimeout:   55 * time.Second,
		ResponseHeaderTimeout: 66 * time.Second,
		ExpectContinueTimeout: 77 * time.Second,
	}))

	transport, ok := w.HTTP.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("HTTP.Transport: got %T, want *http.Transport", w.HTTP.Transport)
	}

	if transport.MaxIdleConns != 111 {
		t.Errorf("MaxIdleConns: got %d, want 111", transport.MaxIdleConns)
	}
	if transport.MaxIdleConnsPerHost != 22 {
		t.Errorf("MaxIdleConnsPerHost: got %d, want 22", transport.MaxIdleConnsPerHost)
	}
	if transport.MaxConnsPerHost != 33 {
		t.Errorf("MaxConnsPerHost: got %d, want 33", transport.MaxConnsPerHost)
	}
	if transport.IdleConnTimeout != 44*time.Second {
		t.Errorf("IdleConnTimeout: got %s, want 44s", transport.IdleConnTimeout)
	}
	if transport.TLSHandshakeTimeout != 55*time.Second {
		t.Errorf("TLSHandshakeTimeout: got %s, want 55s", transport.TLSHandshakeTimeout)
	}
	if transport.ResponseHeaderTimeout != 66*time.Second {
		t.Errorf("ResponseHeaderTimeout: got %s, want 66s", transport.ResponseHeaderTimeout)
	}
	if transport.ExpectContinueTimeout != 77*time.Second {
		t.Errorf("ExpectContinueTimeout: got %s, want 77s", transport.ExpectContinueTimeout)
	}
}

func TestRunClaimableCustomerDiscoveryNotifiesWithCursorWraparound(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := &recordingClaimableCustomerStore{
		responses: []claimableCustomerResponse{
			{customers: []string{"cus_a", "cus_b"}},
			{},
			{customers: []string{"cus_a"}},
		},
		calls: make(chan string, 3),
	}
	sched := &recordingScheduler{}

	errCh := make(chan error, 1)
	go func() {
		errCh <- worker.RunClaimableCustomerDiscovery(ctx, store, sched, worker.DiscoveryConfig{
			Interval:     time.Millisecond,
			EmptyBackoff: time.Nanosecond,
			Limit:        2,
		})
	}()

	var afterCursors []string
	for len(afterCursors) < 3 {
		select {
		case after := <-store.calls:
			afterCursors = append(afterCursors, after)
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for discovery calls; got %v", afterCursors)
		}
	}
	cancel()

	select {
	case err := <-errCh:
		if err != context.Canceled {
			t.Fatalf("RunClaimableCustomerDiscovery: got %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for discovery loop shutdown")
	}

	wantCursors := []string{"", "cus_b", ""}
	if !slices.Equal(afterCursors, wantCursors) {
		t.Fatalf("after cursors: got %v, want %v", afterCursors, wantCursors)
	}

	wantNotified := []string{"cus_a", "cus_b", "cus_a"}
	if got := sched.Notified(); !slices.Equal(got, wantNotified) {
		t.Fatalf("notified customers: got %v, want %v", got, wantNotified)
	}
}

func TestRunClaimableCustomerDiscoveryRetriesAfterStoreError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := &recordingClaimableCustomerStore{
		responses: []claimableCustomerResponse{
			{err: errors.New("database unavailable")},
			{customers: []string{"cus_recovered"}},
		},
		calls: make(chan string, 2),
	}
	sched := &recordingScheduler{}

	errCh := make(chan error, 1)
	go func() {
		errCh <- worker.RunClaimableCustomerDiscovery(ctx, store, sched, worker.DiscoveryConfig{
			Interval:     time.Millisecond,
			ErrorBackoff: time.Nanosecond,
			Limit:        1,
		})
	}()

	for range 2 {
		select {
		case <-store.calls:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for discovery retry")
		}
	}
	cancel()

	select {
	case err := <-errCh:
		if err != context.Canceled {
			t.Fatalf("RunClaimableCustomerDiscovery: got %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for discovery loop shutdown")
	}

	want := []string{"cus_recovered"}
	if got := sched.Notified(); !slices.Equal(got, want) {
		t.Fatalf("notified customers: got %v, want %v", got, want)
	}
}

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

	w := newTestWorker()
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

func TestDeliver_DrainsLargeResponseBodySoConnectionIsReused(t *testing.T) {
	t.Parallel()

	body := bytes.Repeat([]byte("x"), 512*1024)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	var reused []bool
	baseTransport := worker.NewHTTPClient(worker.DefaultHTTPConfig()).Transport
	traceTransport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		trace := &httptrace.ClientTrace{
			GotConn: func(info httptrace.GotConnInfo) {
				reused = append(reused, info.Reused)
			},
		}
		return baseTransport.RoundTrip(req.WithContext(httptrace.WithClientTrace(req.Context(), trace)))
	})

	w := newTestWorker(worker.WithHTTPClient(&http.Client{Transport: traceTransport}))
	d := worker.Delivery{
		EventID:    "evt_reuse",
		DeliveryID: "dlv_reuse",
		CustomerID: "cus_reuse",
		URL:        srv.URL,
		Body:       []byte(`{}`),
		EventType:  "test.event",
	}

	for i := 0; i < 2; i++ {
		status, err := w.Deliver(context.Background(), d)
		if err != nil {
			t.Fatalf("Deliver[%d]: %v", i, err)
		}
		if status != http.StatusOK {
			t.Fatalf("Deliver[%d] status: got %d, want 200", i, status)
		}
	}

	if len(reused) != 2 {
		t.Fatalf("GotConn calls: got %d, want 2", len(reused))
	}
	if !reused[1] {
		t.Fatalf("second request did not reuse the first connection: %v", reused)
	}
}

func TestProcess_DrainsResponseBodyBeforeClassifyingEveryOutcome(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		statusCode int
		wantMark   string
	}{
		{name: "success", statusCode: http.StatusOK, wantMark: "succeeded"},
		{name: "retryable", statusCode: http.StatusServiceUnavailable, wantMark: "retried"},
		{name: "terminal", statusCode: http.StatusNotFound, wantMark: "dead"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := bytes.Repeat([]byte("x"), 512*1024)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				_ = r.Body.Close()
				w.Header().Set("Content-Length", strconv.Itoa(len(body)))
				w.WriteHeader(tt.statusCode)
				_, _ = w.Write(body)
			}))
			defer srv.Close()

			var reused []bool
			baseTransport := worker.NewHTTPClient(worker.DefaultHTTPConfig()).Transport
			traceTransport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
				trace := &httptrace.ClientTrace{
					GotConn: func(info httptrace.GotConnInfo) {
						reused = append(reused, info.Reused)
					},
				}
				return baseTransport.RoundTrip(req.WithContext(httptrace.WithClientTrace(req.Context(), trace)))
			})

			w := newTestWorker(worker.WithHTTPClient(&http.Client{Transport: traceTransport}))
			store := &recordingDeliveryStore{}
			delivery := outbox.Delivery{
				ID:            "dlv_" + tt.name,
				EventID:       "evt_" + tt.name,
				CustomerID:    "cus_" + tt.name,
				URL:           srv.URL,
				Payload:       []byte(`{}`),
				EventType:     "test.event",
				AttemptNumber: 1,
				CreatedAt:     time.Now(),
			}

			for i := 0; i < 2; i++ {
				result := w.Process(context.Background(), store, delivery)
				if result.Err != nil {
					t.Fatalf("Process[%d]: %v", i, result.Err)
				}
			}

			if len(reused) != 2 {
				t.Fatalf("GotConn calls: got %d, want 2", len(reused))
			}
			if !reused[1] {
				t.Fatalf("second request did not reuse the first connection: %v", reused)
			}

			switch tt.wantMark {
			case "succeeded":
				if store.succeeded != 2 {
					t.Fatalf("MarkSucceeded calls: got %d, want 2", store.succeeded)
				}
			case "retried":
				if store.retried != 2 {
					t.Fatalf("MarkRetry calls: got %d, want 2", store.retried)
				}
			case "dead":
				if store.dead != 2 {
					t.Fatalf("MarkDead calls: got %d, want 2", store.dead)
				}
			}
		})
	}
}

func TestProcess_MaxAge(t *testing.T) {
	t.Parallel()

	maxAge := 1 * time.Hour

	t.Run("pre-flight expiration skips Send", func(t *testing.T) {
		t.Parallel()

		called := false
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			called = true
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		w := newTestWorker(worker.WithRetryPolicy(retry.New(retry.Config{MaxAge: maxAge})))
		store := &recordingDeliveryStore{}

		// Delivery created 2 hours ago (exceeds 1h MaxAge)
		delivery := outbox.Delivery{
			ID:            "dlv_expired",
			URL:           srv.URL,
			AttemptNumber: 1,
			CreatedAt:     time.Now().Add(-2 * time.Hour),
		}

		result := w.Process(context.Background(), store, delivery)
		if result.Err != nil {
			t.Fatalf("Process: %v", result.Err)
		}

		if called {
			t.Error("HTTP Send was called for expired delivery")
		}
		if store.dead != 1 {
			t.Errorf("MarkDead calls: got %d, want 1", store.dead)
		}
	})

	t.Run("post-attempt expiration after failure", func(t *testing.T) {
		t.Parallel()

		called := false
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			called = true
			time.Sleep(10 * time.Millisecond)
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer srv.Close()

		tightMaxAge := 10 * time.Millisecond
		w := newTestWorker(worker.WithRetryPolicy(retry.New(retry.Config{MaxAge: tightMaxAge})))
		store := &recordingDeliveryStore{}

		delivery := outbox.Delivery{
			ID:            "dlv_expires_soon",
			URL:           srv.URL,
			AttemptNumber: 1,
			CreatedAt:     time.Now().Add(-5 * time.Millisecond),
		}

		result := w.Process(context.Background(), store, delivery)
		if result.Err != nil {
			t.Fatalf("Process: %v", result.Err)
		}

		if !called {
			t.Error("HTTP Send was not called")
		}
		if store.dead != 1 {
			t.Errorf("MarkDead calls: got %d, want 1", store.dead)
		}
		if store.retried != 0 {
			t.Errorf("MarkRetry calls: got %d, want 0", store.retried)
		}
	})
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

	w := newTestWorker()
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

func TestDeliver_UsesPerAttemptRequestTimeout(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	w := newTestWorker(worker.WithRequestTimeout(20 * time.Millisecond))
	started := time.Now()
	_, err := w.Deliver(context.Background(), worker.Delivery{
		EventID:    "evt_timeout",
		DeliveryID: "dlv_timeout",
		CustomerID: "cus_timeout",
		URL:        srv.URL,
		Body:       []byte(`{}`),
		EventType:  "test.event",
	})
	elapsed := time.Since(started)

	if err == nil {
		t.Fatal("Deliver: got nil error, want timeout")
	}
	if elapsed >= 150*time.Millisecond {
		t.Fatalf("Deliver elapsed %s, want request timeout before handler completes", elapsed)
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

	w := newTestWorker()
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

	w := newTestWorker()
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

	w := newTestWorker()
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

	wrongSecretWorker := newTestWorker()
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
