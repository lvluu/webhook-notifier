package worker_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/lvluu/webhook-notifier/internal/outbox"
	"github.com/lvluu/webhook-notifier/internal/retry"
	"github.com/lvluu/webhook-notifier/internal/scheduler"
	"github.com/lvluu/webhook-notifier/internal/worker"
)

// alwaysScheduler offers the same customer on every Pick so the worker proceeds
// to ClaimNextN each loop iteration.
type alwaysScheduler struct{ id string }

func (s alwaysScheduler) Pick(context.Context) (scheduler.CustomerID, uint32, bool, error) {
	return scheduler.CustomerID(s.id), 0, true, nil
}
func (alwaysScheduler) WaitForWork(context.Context) error  { return nil }
func (alwaysScheduler) Notify(scheduler.CustomerID)        {}
func (alwaysScheduler) Remove(scheduler.CustomerID)        {}

// claimErrorStore returns a fixed error from ClaimNextN and counts attempts.
type claimErrorStore struct {
	recordingDeliveryStore
	err   error
	calls atomic.Int64
}

func (s *claimErrorStore) ClaimNextN(context.Context, string, int) ([]outbox.Delivery, error) {
	s.calls.Add(1)
	return nil, s.err
}

// markErrorStore claims one delivery every cycle and fails MarkRetryBatch with a
// fixed error, counting mark attempts. Used to exercise the process/mark phase
// of the run loop rather than the claim phase.
type markErrorStore struct {
	recordingDeliveryStore
	delivery  outbox.Delivery
	markErr   error
	markCalls atomic.Int64
}

func (s *markErrorStore) ClaimNextN(context.Context, string, int) ([]outbox.Delivery, error) {
	return []outbox.Delivery{s.delivery}, nil
}

func (s *markErrorStore) MarkRetryBatch(context.Context, []outbox.BatchMarkItem) error {
	s.markCalls.Add(1)
	return s.markErr
}

// TestRunAbsorbsTransientMarkError covers the process/mark phase of the
// fleet-survival contract (the claim phase is covered by
// TestRunAbsorbsTransientClaimError). A delivery is claimed and POSTed to a 503
// endpoint (retryable), so the worker calls MarkRetryBatch — which returns a
// transient statement_timeout (57014). The worker must back off and retry the
// loop, not return fatally; a regression that made a mark-phase 57014 fatal
// would re-introduce the fleet-wide shutdown.
func TestRunAbsorbsTransientMarkError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable) // 503 -> retryable -> MarkRetryBatch
	}))
	defer srv.Close()

	store := &markErrorStore{
		delivery: outbox.Delivery{
			ID:            "d-mark-transient",
			CustomerID:    "cus_mark_transient",
			WebhookID:     "wh-1",
			URL:           srv.URL,
			Secret:        "test-secret",
			EventType:     "subscriber.created",
			Payload:       []byte(`{}`),
			AttemptNumber: 1,
		},
		markErr: &pgconn.PgError{Code: "57014", Message: "canceling statement due to statement timeout"},
	}

	w := worker.New(alwaysScheduler{id: "cus_mark_transient"},
		worker.WithRetryPolicy(retry.New(retry.Config{
			MaxAttempts: 5,
			BaseDelay:   time.Millisecond,
			CapDelay:    time.Millisecond,
		})),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- w.Run(ctx, store) }()

	deadline := time.After(5 * time.Second)
	for store.markCalls.Load() < 2 {
		select {
		case <-deadline:
			cancel()
			t.Fatalf("worker did not retry after transient mark error (markCalls=%d)", store.markCalls.Load())
		case err := <-errCh:
			t.Fatalf("Run returned early on transient mark error: %v", err)
		case <-time.After(5 * time.Millisecond):
		}
	}

	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop after context cancel")
	}
}

// TestRunAbsorbsTransientClaimError verifies the fleet-survival contract: a
// transient database error (here statement_timeout, SQLSTATE 57014) must make
// the worker back off and retry, NOT return fatally. Returning would cascade
// into a full replica shutdown (and, under shared DB pressure, a fleet-wide
// outage). The worker keeps looping until the run context is canceled, then
// returns context.Canceled.
func TestRunAbsorbsTransientClaimError(t *testing.T) {
	store := &claimErrorStore{
		err: &pgconn.PgError{Code: "57014", Message: "canceling statement due to statement timeout"},
	}
	w := worker.New(alwaysScheduler{id: "cus_transient"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- w.Run(ctx, store) }()

	// Wait until the worker has retried the claim at least twice despite the
	// error, proving the loop continued rather than returning fatally.
	deadline := time.After(3 * time.Second)
	for store.calls.Load() < 2 {
		select {
		case <-deadline:
			cancel()
			t.Fatalf("worker did not retry after transient error (calls=%d)", store.calls.Load())
		case err := <-errCh:
			t.Fatalf("Run returned early on transient error: %v", err)
		case <-time.After(5 * time.Millisecond):
		}
	}

	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop after context cancel")
	}
}

// TestRunFailsFastOnNonTransientError verifies the worker still surfaces
// genuine, non-recoverable errors (e.g. a programming/constraint fault) by
// returning them, so they are not silently retried forever.
func TestRunFailsFastOnNonTransientError(t *testing.T) {
	sentinel := errors.New("permanent claim failure")
	store := &claimErrorStore{err: sentinel}
	w := worker.New(alwaysScheduler{id: "cus_fatal"})

	errCh := make(chan error, 1)
	go func() { errCh <- w.Run(context.Background(), store) }()

	select {
	case err := <-errCh:
		if !errors.Is(err, sentinel) {
			t.Fatalf("Run returned %v, want wrapping %v", err, sentinel)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return on non-transient error")
	}
}
