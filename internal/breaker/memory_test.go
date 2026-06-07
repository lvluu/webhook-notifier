package breaker

import (
	"sync"
	"testing"
	"time"
)

func TestMemoryBreakerOpensAfterThreshold(t *testing.T) {
	t.Parallel()

	var transitions []Transition
	br := NewMemory(Config{
		FailureThreshold: 2,
		InitialCooldown:  50 * time.Millisecond,
		MaxCooldown:      time.Second,
	}, nil, func(t Transition) {
		transitions = append(transitions, t)
	})

	br.RecordFailure("wh_1", "retryable_5xx")
	if got := br.State("wh_1"); got != Closed {
		t.Fatalf("state after first failure: got %s, want %s", got, Closed)
	}

	br.RecordFailure("wh_1", "retryable_5xx")
	if got := br.State("wh_1"); got != Open {
		t.Fatalf("state after threshold failure: got %s, want %s", got, Open)
	}
	if len(transitions) != 1 {
		t.Fatalf("transitions: got %d, want 1", len(transitions))
	}
	if transitions[0].State != Open || transitions[0].FailureCount != 2 {
		t.Fatalf("transition: got state=%s failures=%d, want open/2", transitions[0].State, transitions[0].FailureCount)
	}
}

func TestMemoryBreakerCooldownProbeAndSuccessClose(t *testing.T) {
	t.Parallel()

	var transitions []Transition
	br := NewMemory(Config{
		FailureThreshold: 1,
		InitialCooldown:  10 * time.Millisecond,
		MaxCooldown:      time.Second,
	}, nil, func(t Transition) {
		transitions = append(transitions, t)
	})

	br.RecordFailure("wh_1", "timeout")
	time.Sleep(30 * time.Millisecond)

	if got := br.State("wh_1"); got != HalfOpen {
		t.Fatalf("state after cooldown: got %s, want %s", got, HalfOpen)
	}
	if !br.IsEligible("wh_1") {
		t.Fatal("half-open breaker should be eligible for a probe")
	}

	br.SetProbeDelivery("wh_1", "del_1")
	br.RecordSuccess("wh_1")

	if got := br.State("wh_1"); got != Closed {
		t.Fatalf("state after probe success: got %s, want %s", got, Closed)
	}
	if len(transitions) != 3 {
		t.Fatalf("transitions: got %d, want open, half_open, closed", len(transitions))
	}
	if transitions[1].State != HalfOpen || transitions[1].ProbeDeliveryID != "del_1" {
		t.Fatalf("probe transition: got state=%s probe=%q, want half_open/del_1", transitions[1].State, transitions[1].ProbeDeliveryID)
	}
	if transitions[2].State != Closed || transitions[2].FailureCount != 0 {
		t.Fatalf("close transition: got state=%s failures=%d, want closed/0", transitions[2].State, transitions[2].FailureCount)
	}
}

func TestMemoryBreakerHalfOpenFailureReopensWithBackoff(t *testing.T) {
	t.Parallel()

	var transitions []Transition
	br := NewMemory(Config{
		FailureThreshold: 1,
		InitialCooldown:  10 * time.Millisecond,
		MaxCooldown:      25 * time.Millisecond,
	}, nil, func(t Transition) {
		transitions = append(transitions, t)
	})

	br.RecordFailure("wh_1", "timeout")
	time.Sleep(30 * time.Millisecond)
	br.SetProbeDelivery("wh_1", "del_1")
	br.RecordFailure("wh_1", "timeout")

	if got := br.State("wh_1"); got != Open {
		t.Fatalf("state after failed probe: got %s, want %s", got, Open)
	}
	if len(transitions) != 3 {
		t.Fatalf("transitions: got %d, want open, half_open, open", len(transitions))
	}
	if got := transitions[2].Cooldown; got != 20*time.Millisecond {
		t.Fatalf("reopen cooldown: got %s, want 20ms", got)
	}
}

func TestMemoryBreakerDisabledStaysClosed(t *testing.T) {
	t.Parallel()

	br := NewMemory(Config{FailureThreshold: 0}, nil, nil)
	for i := 0; i < 10; i++ {
		br.RecordFailure("wh_1", "timeout")
	}
	if got := br.State("wh_1"); got != Closed {
		t.Fatalf("disabled state: got %s, want %s", got, Closed)
	}
}

func TestMemoryBreakerConcurrentAccess(t *testing.T) {
	t.Parallel()

	br := NewMemory(Config{
		FailureThreshold: 100,
		InitialCooldown:  time.Minute,
		MaxCooldown:      time.Minute,
	}, nil, nil)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				br.RecordFailure("wh_1", "timeout")
				_ = br.State("wh_1")
				_ = br.IsEligible("wh_1")
			}
		}()
	}
	wg.Wait()

	if got := br.State("wh_1"); got != Open {
		t.Fatalf("state after concurrent failures: got %s, want %s", got, Open)
	}
}

func TestMemoryBreakerCleanupRemovesOnlyIdleClosedEntries(t *testing.T) {
	t.Parallel()

	br := NewMemory(Config{
		FailureThreshold: 1,
		InitialCooldown:  time.Minute,
		MaxCooldown:      time.Minute,
	}, nil, nil)

	br.RecordSuccess("closed")
	br.RecordFailure("open", "timeout")
	br.Cleanup()

	if _, ok := br.items.Load("closed"); ok {
		t.Fatal("closed idle breaker entry was not cleaned up")
	}
	if _, ok := br.items.Load("open"); !ok {
		t.Fatal("open breaker entry was cleaned up")
	}
	if got := br.State("open"); got != Open {
		t.Fatalf("open state after cleanup: got %s, want %s", got, Open)
	}
}

func TestMemoryBreakerCleanupRemovesIdleClosedEntryWithFailureHistory(t *testing.T) {
	t.Parallel()

	br := NewMemory(Config{
		FailureThreshold: 3,
		InitialCooldown:  time.Minute,
		MaxCooldown:      time.Minute,
	}, nil, nil)

	br.RecordFailure("historical", "timeout")
	br.Cleanup()

	if _, ok := br.items.Load("historical"); ok {
		t.Fatal("idle closed breaker entry with failure history was not cleaned up")
	}
}
