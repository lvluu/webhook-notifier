package breaker

import (
	"context"
	"database/sql"
	"sync"
	"testing"
)

func TestAsyncPersisterSinkFallsBackSynchronouslyWhenQueueFull(t *testing.T) {
	p := NewAsyncPersister(nil, nil, 1)
	first := Transition{WebhookID: "wh_1", State: Open}
	second := Transition{WebhookID: "wh_2", State: Open}

	var mu sync.Mutex
	var persisted []Transition
	p.persist = func(_ context.Context, _ *sql.DB, transition Transition) error {
		mu.Lock()
		defer mu.Unlock()
		persisted = append(persisted, transition)
		return nil
	}

	sink := p.Sink()
	sink(first)
	sink(second)

	mu.Lock()
	defer mu.Unlock()
	if len(persisted) != 1 {
		t.Fatalf("persisted fallback transitions = %d, want 1", len(persisted))
	}
	if persisted[0].WebhookID != second.WebhookID {
		t.Fatalf("persisted fallback webhook = %q, want %q", persisted[0].WebhookID, second.WebhookID)
	}

	select {
	case queued := <-p.ch:
		if queued.WebhookID != first.WebhookID {
			t.Fatalf("queued webhook = %q, want %q", queued.WebhookID, first.WebhookID)
		}
	default:
		t.Fatal("expected first transition to remain queued")
	}
}
