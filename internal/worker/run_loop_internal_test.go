package worker

import (
	"context"
	"testing"
	"time"
)

type emptyDelayTestScheduler struct{}

func (emptyDelayTestScheduler) Pick(context.Context) (string, uint32, bool, error) {
	return "", 0, false, nil
}
func (emptyDelayTestScheduler) Notify(string) {}
func (emptyDelayTestScheduler) Remove(string) {}

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
