package main

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"
)

type recordingPgNotifyExecutor struct {
	mu      sync.Mutex
	calls   []string
	started chan string
	release <-chan struct{}
	err     error
}

func (e *recordingPgNotifyExecutor) ExecContext(ctx context.Context, _ string, args ...any) (sql.Result, error) {
	customerID, _ := args[1].(string)
	e.mu.Lock()
	e.calls = append(e.calls, customerID)
	e.mu.Unlock()
	if e.started != nil {
		e.started <- customerID
	}
	if e.release != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-e.release:
		}
	}
	return nil, e.err
}

func (e *recordingPgNotifyExecutor) callCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.calls)
}

func TestPgNotifyBroadcaster_CoalescesPendingCustomer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	release := make(chan struct{})
	executor := &recordingPgNotifyExecutor{
		started: make(chan string, 1),
		release: release,
	}
	broadcaster := newPgNotifyBroadcaster(executor, slogDiscard(), "customer_ready", 1)
	go func() {
		_ = broadcaster.Run(ctx)
	}()

	broadcaster.Notify("cus_1")
	broadcaster.Notify("cus_1")
	broadcaster.Notify("cus_1")
	waitForStartedCustomer(t, executor.started, "cus_1")
	close(release)

	time.Sleep(10 * time.Millisecond)
	if got := executor.callCount(); got != 1 {
		t.Fatalf("pg_notify calls: got %d, want 1", got)
	}
}

func TestPgNotifyBroadcaster_CoalescesCustomerDuringCooldown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	now := time.Now()
	executor := &recordingPgNotifyExecutor{
		started: make(chan string, 2),
	}
	broadcaster := newPgNotifyBroadcaster(executor, slogDiscard(), "customer_ready", 1)
	broadcaster.now = func() time.Time { return now }
	go func() {
		_ = broadcaster.Run(ctx)
	}()

	broadcaster.Notify("cus_1")
	waitForStartedCustomer(t, executor.started, "cus_1")
	waitForCallCount(t, executor, 1)
	waitForCustomerReleased(t, broadcaster, "cus_1")

	now = now.Add(pgNotifyBroadcastCooldown - time.Nanosecond)
	broadcaster.Notify("cus_1")
	time.Sleep(10 * time.Millisecond)
	if got := executor.callCount(); got != 1 {
		t.Fatalf("pg_notify calls during cooldown: got %d, want 1", got)
	}

	now = now.Add(2 * time.Nanosecond)
	broadcaster.Notify("cus_1")
	waitForStartedCustomer(t, executor.started, "cus_1")
	waitForCallCount(t, executor, 2)
}

func TestPgNotifyBroadcaster_DropsWhenQueueIsFullWithoutBlocking(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	release := make(chan struct{})
	executor := &recordingPgNotifyExecutor{
		started: make(chan string, 1),
		release: release,
	}
	broadcaster := newPgNotifyBroadcaster(executor, slogDiscard(), "customer_ready", 1)
	go func() {
		_ = broadcaster.Run(ctx)
	}()

	broadcaster.Notify("cus_1")
	waitForStartedCustomer(t, executor.started, "cus_1")
	broadcaster.Notify("cus_2")

	done := make(chan struct{})
	go func() {
		broadcaster.Notify("cus_3")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Notify blocked while the queue was full")
	}
	close(release)
}

func TestPgNotifyBroadcaster_ReleasesCustomerAfterFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	executor := &recordingPgNotifyExecutor{
		started: make(chan string, 2),
		err:     errors.New("database unavailable"),
	}
	broadcaster := newPgNotifyBroadcaster(executor, slogDiscard(), "customer_ready", 1)
	now := time.Now()
	broadcaster.now = func() time.Time { return now }
	go func() {
		_ = broadcaster.Run(ctx)
	}()

	broadcaster.Notify("cus_1")
	waitForStartedCustomer(t, executor.started, "cus_1")
	waitForCallCount(t, executor, 1)
	waitForCustomerReleased(t, broadcaster, "cus_1")

	now = now.Add(pgNotifyBroadcastCooldown + time.Nanosecond)
	broadcaster.Notify("cus_1")
	waitForStartedCustomer(t, executor.started, "cus_1")
	waitForCallCount(t, executor, 2)
}

func waitForStartedCustomer(t *testing.T, started <-chan string, want string) {
	t.Helper()
	select {
	case got := <-started:
		if got != want {
			t.Fatalf("started Customer: got %q, want %q", got, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for Customer %q", want)
	}
}

func waitForCallCount(t *testing.T, executor *recordingPgNotifyExecutor, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if executor.callCount() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("pg_notify calls: got %d, want %d", executor.callCount(), want)
}

func waitForCustomerReleased(t *testing.T, broadcaster *pgNotifyBroadcaster, customerID string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		broadcaster.mu.Lock()
		_, isPending := broadcaster.pending[customerID]
		broadcaster.mu.Unlock()
		if !isPending {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("Customer %q remained pending", customerID)
}
