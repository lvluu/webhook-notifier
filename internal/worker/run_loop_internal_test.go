package worker

import (
	"context"
	"testing"
	"time"

	"github.com/lvluu/webhook-notifier/internal/outbox"
)

type emptyDelayTestScheduler struct{}

func (emptyDelayTestScheduler) Pick(context.Context) (string, uint32, bool, error) {
	return "", 0, false, nil
}
func (emptyDelayTestScheduler) WaitForWork(context.Context) error { return nil }
func (emptyDelayTestScheduler) Notify(string)                     {}
func (emptyDelayTestScheduler) Remove(string)                     {}

func TestEmptyClaimDelayBackoffGrowsToCap(t *testing.T) {
	w := New(emptyDelayTestScheduler{}, WithEmptyClaimBackoff(20*time.Millisecond))

	delay := w.emptyClaimDelay()
	maxDelay := w.maxEmptyClaimDelay()
	for range 10 {
		delay = growBackoff(delay, maxDelay)
	}

	if delay != maxEmptyClaimBackoff {
		t.Fatalf("empty claim backoff: got %s, want cap %s", delay, maxEmptyClaimBackoff)
	}
}

func TestEmptyClaimDelayBackoffDoesNotShrinkLargeConfiguredDelay(t *testing.T) {
	configured := time.Hour
	w := New(emptyDelayTestScheduler{}, WithEmptyClaimBackoff(configured))

	if got := w.maxEmptyClaimDelay(); got != configured {
		t.Fatalf("max empty claim backoff: got %s, want configured delay %s", got, configured)
	}
}

type wakeThenPickScheduler struct {
	picks       int
	waitForWork int
}

func (s *wakeThenPickScheduler) Pick(context.Context) (string, uint32, bool, error) {
	s.picks++
	if s.picks == 1 {
		return "", 0, false, nil
	}
	return "cus_ready", 8, true, nil
}

func (s *wakeThenPickScheduler) WaitForWork(context.Context) error {
	s.waitForWork++
	return nil
}

func (s *wakeThenPickScheduler) Notify(string) {}
func (s *wakeThenPickScheduler) Remove(string) {}

type immediateClaimStore struct {
	claimNextN     int
	claimNextBatch int
}

func (s *immediateClaimStore) ClaimNextN(context.Context, string, int) ([]outbox.Delivery, error) {
	s.claimNextN++
	return []outbox.Delivery{{ID: "dlv_ready", CustomerID: "cus_ready"}}, nil
}

func (s *immediateClaimStore) ClaimNextBatch(context.Context, []string, int) ([]outbox.Delivery, error) {
	s.claimNextBatch++
	return []outbox.Delivery{{ID: "dlv_ready", CustomerID: "cus_ready"}}, nil
}

func (s *immediateClaimStore) HasClaimableWork(context.Context, string) (bool, error) {
	return false, nil
}

func (s *immediateClaimStore) HasClaimableWorkBatch(context.Context, []string) (map[string]bool, error) {
	return nil, nil
}

func (s *immediateClaimStore) MarkSucceeded(context.Context, string, int, time.Duration) error {
	return nil
}

func (s *immediateClaimStore) MarkRetry(context.Context, string, int, string, time.Duration, time.Duration) error {
	return nil
}

func (s *immediateClaimStore) MarkDead(context.Context, string, int, string, string, time.Duration) error {
	return nil
}

func (s *immediateClaimStore) Requeue(context.Context, string, time.Duration) error {
	return nil
}

func (s *immediateClaimStore) RequeueBatch(context.Context, []string, time.Duration) error {
	return nil
}

func (s *immediateClaimStore) MarkSucceededBatch(context.Context, []outbox.BatchMarkItem) error {
	return nil
}

func (s *immediateClaimStore) MarkRetryBatch(context.Context, []outbox.BatchMarkItem) error {
	return nil
}

func (s *immediateClaimStore) MarkDeadBatch(context.Context, []outbox.BatchMarkItem) error {
	return nil
}

func TestClaimSingleClaimsImmediatelyAfterWaitForWork(t *testing.T) {
	sched := &wakeThenPickScheduler{}
	store := &immediateClaimStore{}
	w := New(sched, WithEmptyClaimBackoff(time.Hour))

	deliveries, err := w.claimSingle(context.Background(), store)
	if err != nil {
		t.Fatalf("claimSingle: %v", err)
	}
	if len(deliveries) != 1 {
		t.Fatalf("deliveries: got %d, want 1", len(deliveries))
	}
	if sched.waitForWork != 1 {
		t.Fatalf("WaitForWork calls: got %d, want 1", sched.waitForWork)
	}
	if store.claimNextN != 1 {
		t.Fatalf("ClaimNextN calls: got %d, want 1", store.claimNextN)
	}
}

func TestClaimMultiClaimsImmediatelyAfterWaitForWork(t *testing.T) {
	sched := &wakeThenPickScheduler{}
	store := &immediateClaimStore{}
	w := New(sched, WithMultiPickCount(2), WithEmptyClaimBackoff(time.Hour))

	deliveries, err := w.claimMulti(context.Background(), store)
	if err != nil {
		t.Fatalf("claimMulti: %v", err)
	}
	if len(deliveries) != 1 {
		t.Fatalf("deliveries: got %d, want 1", len(deliveries))
	}
	if sched.waitForWork != 1 {
		t.Fatalf("WaitForWork calls: got %d, want 1", sched.waitForWork)
	}
	if store.claimNextBatch != 1 {
		t.Fatalf("ClaimNextBatch calls: got %d, want 1", store.claimNextBatch)
	}
}
