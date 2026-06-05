package worker_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/lvluu/webhook-notifier/internal/outbox"
	"github.com/lvluu/webhook-notifier/internal/scheduler"
	"github.com/lvluu/webhook-notifier/internal/worker"
)

// alwaysScheduler offers the same customer on every Pick so the worker proceeds
// to ClaimNextN each loop iteration.
type alwaysScheduler struct{ id string }

func (s alwaysScheduler) Pick(context.Context) (scheduler.CustomerID, uint32, bool, error) {
	return scheduler.CustomerID(s.id), 0, true, nil
}
func (alwaysScheduler) Notify(scheduler.CustomerID) {}
func (alwaysScheduler) Remove(scheduler.CustomerID) {}

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
