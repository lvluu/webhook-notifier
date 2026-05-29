package registry_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lvluu/webhook-notifier/internal/registry"
)

func TestListActiveForEvent_CacheMissThenHit(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	insertWebhook(t, ctx, db, "cus_1", true, "subscriber.created")
	insertWebhook(t, ctx, db, "cus_1", true, "subscriber.deleted")
	insertWebhook(t, ctx, db, "cus_1", false, "subscriber.created")
	insertWebhook(t, ctx, db, "cus_2", true, "subscriber.created")

	reg := registry.New(db, registry.WithTTL(5*time.Minute))

	// Miss → queries DB and caches
	hooks, err := reg.ListActiveForEvent(ctx, "cus_1", "subscriber.created")
	if err != nil {
		t.Fatalf("ListActiveForEvent (miss): %v", err)
	}
	if len(hooks) != 1 {
		t.Fatalf("first call: got %d webhooks, want 1", len(hooks))
	}
	if hooks[0].CustomerID != "cus_1" {
		t.Errorf("first call customer_id: got %q, want cus_1", hooks[0].CustomerID)
	}
	if hooks[0].URL != "https://partner.example/webhook" {
		t.Errorf("first call url: got %q, want https://partner.example/webhook", hooks[0].URL)
	}
	if !hooks[0].Active {
		t.Errorf("first call active: got false, want true")
	}
	if len(hooks[0].EventTypes) != 1 || hooks[0].EventTypes[0] != "subscriber.created" {
		t.Errorf("first call event_types: got %v, want [subscriber.created]", hooks[0].EventTypes)
	}
	if hooks[0].SecretEncrypted != "test-secret" {
		t.Errorf("first call secret: got %q, want test-secret", hooks[0].SecretEncrypted)
	}
	if hooks[0].ID == "" {
		t.Errorf("first call id: got empty, want non-empty")
	}

	// Hit — cached result without DB query
	hooks, err = reg.ListActiveForEvent(ctx, "cus_1", "subscriber.created")
	if err != nil {
		t.Fatalf("ListActiveForEvent (hit): %v", err)
	}
	if len(hooks) != 1 {
		t.Fatalf("second call: got %d webhooks, want 1", len(hooks))
	}
	if hooks[0].ID == "" {
		t.Errorf("second call id: got empty, want non-empty")
	}
}

func TestListActiveForEvent_TTLExpiry(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	id1 := insertWebhook(t, ctx, db, "cus_ttl", true, "subscriber.created")

	reg := registry.New(db, registry.WithTTL(1*time.Second))

	// Miss → cache
	hooks, err := reg.ListActiveForEvent(ctx, "cus_ttl", "subscriber.created")
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	if len(hooks) != 1 {
		t.Fatalf("first call: got %d webhooks, want 1", len(hooks))
	}
	if hooks[0].ID != id1 {
		t.Fatalf("first call id: got %s, want %s", hooks[0].ID, id1)
	}

	// Add a second webhook while the first is cached
	id2 := insertWebhook(t, ctx, db, "cus_ttl", true, "subscriber.created")

	// Wait for TTL to expire
	time.Sleep(1500 * time.Millisecond)

	// Should miss (TTL expired), re-query DB, and find both
	hooks, err = reg.ListActiveForEvent(ctx, "cus_ttl", "subscriber.created")
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if len(hooks) != 2 {
		t.Fatalf("after TTL expiry: got %d webhooks, want 2", len(hooks))
	}
	got := map[string]bool{hooks[0].ID: true, hooks[1].ID: true}
	if !got[id1] {
		t.Errorf("expected webhook %s to be present (got IDs: %s, %s)", id1, hooks[0].ID, hooks[1].ID)
	}
	if !got[id2] {
		t.Errorf("expected webhook %s to be present (got IDs: %s, %s)", id2, hooks[0].ID, hooks[1].ID)
	}
}

func TestListActiveForEvent_EvictionAtCapacity(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	idA := insertWebhook(t, ctx, db, "cus_evict_a", true, "subscriber.created")
	idB := insertWebhook(t, ctx, db, "cus_evict_b", true, "subscriber.created")

	reg := registry.New(db, registry.WithCapacity(1), registry.WithTTL(5*time.Minute))

	// Fill cache with A
	hooks, err := reg.ListActiveForEvent(ctx, "cus_evict_a", "subscriber.created")
	if err != nil {
		t.Fatalf("first call (A): %v", err)
	}
	if len(hooks) != 1 || hooks[0].ID != idA {
		t.Fatalf("first call (A): got id %v, want %s", hooks[0].ID, idA)
	}

	// Fill cache with B — evicts A because capacity is 1
	hooks, err = reg.ListActiveForEvent(ctx, "cus_evict_b", "subscriber.created")
	if err != nil {
		t.Fatalf("second call (B): %v", err)
	}
	if len(hooks) != 1 || hooks[0].ID != idB {
		t.Fatalf("second call (B): got id %v, want %s", hooks[0].ID, idB)
	}

	// A was evicted — re-queries DB and finds it again
	hooks, err = reg.ListActiveForEvent(ctx, "cus_evict_a", "subscriber.created")
	if err != nil {
		t.Fatalf("third call (A after eviction): %v", err)
	}
	if len(hooks) != 1 || hooks[0].ID != idA {
		t.Fatalf("third call (A after eviction): got id %v, want %s", hooks[0].ID, idA)
	}
}

func TestListActiveForEvent_SingleflightCoalescing(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	insertWebhook(t, ctx, db, "cus_sf", true, "subscriber.created")

	reg := registry.New(db, registry.WithTTL(5*time.Minute))

	var wg sync.WaitGroup
	errs := make(chan error, 20)

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			hooks, err := reg.ListActiveForEvent(ctx, "cus_sf", "subscriber.created")
			if err != nil {
				errs <- err
				return
			}
			if len(hooks) != 1 {
				errs <- fmt.Errorf("got %d webhooks, want 1", len(hooks))
			}
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("concurrent call: %v", err)
	}
}

func TestListActiveForEvent_EmptyResult(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	// No webhooks for this customer
	reg := registry.New(db, registry.WithTTL(5*time.Minute))

	// Miss — returns empty, caches empty
	hooks, err := reg.ListActiveForEvent(ctx, "cus_empty", "subscriber.created")
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	if len(hooks) != 0 {
		t.Fatalf("first call: got %d webhooks, want 0", len(hooks))
	}

	// Hit — should not panic on empty slice
	hooks, err = reg.ListActiveForEvent(ctx, "cus_empty", "subscriber.created")
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if len(hooks) != 0 {
		t.Fatalf("second call: got %d webhooks, want 0", len(hooks))
	}
}

func TestListActiveForEvent_DifferentEventTypes(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	insertWebhook(t, ctx, db, "cus_types", true, "subscriber.created", "subscriber.unsubscribed")
	insertWebhook(t, ctx, db, "cus_types", true, "subscriber.added_to_segment")

	reg := registry.New(db, registry.WithTTL(5*time.Minute))

	// Query for subscriber.created — should find webhook 1
	hooks, err := reg.ListActiveForEvent(ctx, "cus_types", "subscriber.created")
	if err != nil {
		t.Fatalf("subscriber.created: %v", err)
	}
	if len(hooks) != 1 {
		t.Fatalf("subscriber.created: got %d webhooks, want 1", len(hooks))
	}

	// Query for subscriber.added_to_segment — should find webhook 2 (different key in cache)
	hooks, err = reg.ListActiveForEvent(ctx, "cus_types", "subscriber.added_to_segment")
	if err != nil {
		t.Fatalf("subscriber.added_to_segment: %v", err)
	}
	if len(hooks) != 1 {
		t.Fatalf("subscriber.added_to_segment: got %d webhooks, want 1", len(hooks))
	}

	// Query for subscriber.deleted — should return empty (different key)
	hooks, err = reg.ListActiveForEvent(ctx, "cus_types", "subscriber.deleted")
	if err != nil {
		t.Fatalf("subscriber.deleted: %v", err)
	}
	if len(hooks) != 0 {
		t.Fatalf("subscriber.deleted: got %d webhooks, want 0", len(hooks))
	}
}

func TestListActiveForEvent_UnambiguousCacheKey(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	id1 := insertWebhook(t, ctx, db, "a:b", true, "c")
	id2 := insertWebhook(t, ctx, db, "a", true, "b:c")

	reg := registry.New(db, registry.WithTTL(5*time.Minute))

	hooks1, err := reg.ListActiveForEvent(ctx, "a:b", "c")
	if err != nil {
		t.Fatalf("(a:b, c): %v", err)
	}
	if len(hooks1) != 1 || hooks1[0].ID != id1 {
		t.Fatalf("(a:b, c): got %d webhooks (id=%s), want 1 with id %s", len(hooks1), hookID(hooks1), id1)
	}

	hooks2, err := reg.ListActiveForEvent(ctx, "a", "b:c")
	if err != nil {
		t.Fatalf("(a, b:c): %v", err)
	}
	if len(hooks2) != 1 || hooks2[0].ID != id2 {
		t.Fatalf("(a, b:c): got %d webhooks (id=%s), want 1 with id %s", len(hooks2), hookID(hooks2), id2)
	}
}

func TestListActiveForEvent_CancelledContextDoesNotCorruptCache(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)
	id := insertWebhook(t, ctx, db, "cus_ok", true, "subscriber.created")
	reg := registry.New(db, registry.WithTTL(5*time.Minute))

	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()
	_, err := reg.ListActiveForEvent(cancelCtx, "cus_ok", "subscriber.created")
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}

	hooks, err := reg.ListActiveForEvent(ctx, "cus_ok", "subscriber.created")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(hooks) != 1 || hooks[0].ID != id {
		t.Fatalf("got %d webhooks, want 1 with id %s", len(hooks), id)
	}
}

func TestListActiveForEvent_CancelledCallerDoesNotCancelCoalescedWaiter(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)
	id := insertWebhook(t, ctx, db, "cus_waiter", true, "subscriber.created")
	reg := registry.New(db, registry.WithTTL(5*time.Minute))

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	t.Cleanup(func() {
		_ = tx.Rollback()
	})
	if _, err := tx.ExecContext(ctx, "LOCK TABLE webhooks IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatalf("lock webhooks: %v", err)
	}

	firstCtx, cancelFirst := context.WithCancel(ctx)
	firstErr := make(chan error, 1)
	go func() {
		_, err := reg.ListActiveForEvent(firstCtx, "cus_waiter", "subscriber.created")
		firstErr <- err
	}()

	time.Sleep(100 * time.Millisecond)

	type lookupResult struct {
		hooks []registry.Webhook
		err   error
	}
	secondResult := make(chan lookupResult, 1)
	go func() {
		hooks, err := reg.ListActiveForEvent(ctx, "cus_waiter", "subscriber.created")
		secondResult <- lookupResult{hooks: hooks, err: err}
	}()

	time.Sleep(100 * time.Millisecond)
	cancelFirst()

	select {
	case err := <-firstErr:
		if err == nil {
			t.Fatal("first caller: expected cancellation error, got nil")
		}
	case <-time.After(time.Second):
		t.Fatal("first caller did not return after cancellation")
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("release webhooks lock: %v", err)
	}

	select {
	case res := <-secondResult:
		if res.err != nil {
			t.Fatalf("second caller: unexpected error: %v", res.err)
		}
		if len(res.hooks) != 1 || res.hooks[0].ID != id {
			t.Fatalf("second caller: got %d webhooks (id=%s), want 1 with id %s", len(res.hooks), hookID(res.hooks), id)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("second caller did not receive coalesced lookup result")
	}
}

func TestListActiveForEvent_Validation(t *testing.T) {
	t.Parallel()

	db := newPostgresDB(t, context.Background())
	reg := registry.New(db, registry.WithTTL(5*time.Minute))

	tests := []struct {
		name       string
		customerID string
		eventType  string
	}{
		{name: "empty customer_id", customerID: "", eventType: "subscriber.created"},
		{name: "empty event_type", customerID: "cus_val", eventType: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := reg.ListActiveForEvent(context.Background(), tt.customerID, tt.eventType)
			if err == nil {
				t.Fatal("expected validation error, got nil")
			}
		})
	}
}

func hookID(hooks []registry.Webhook) string {
	if len(hooks) == 0 {
		return "<none>"
	}
	return hooks[0].ID
}
