package breaker

import (
	"context"
	"sync"
	"time"
)

const defaultCleanupInterval = 10 * time.Minute

// Memory is a process-local Webhook Circuit Breaker implementation.
type Memory struct {
	config Config
	sink   TransitionSink
	items  sync.Map
}

type webhookBreaker struct {
	mu              sync.Mutex
	state           State
	failureCount    int
	cooldown        time.Duration
	nextProbeAt     time.Time
	probeDeliveryID string
	active          int
	deleted         bool
}

// NewMemory creates a process-local breaker seeded with persisted snapshots.
func NewMemory(config Config, initial map[string]Snapshot, sink TransitionSink) *Memory {
	config = NormalizeConfig(config)
	m := &Memory{
		config: config,
		sink:   sink,
	}
	for webhookID, snapshot := range initial {
		if snapshot.WebhookID != "" {
			webhookID = snapshot.WebhookID
		}
		if webhookID == "" {
			continue
		}
		state := snapshot.State
		if state == "" {
			state = Closed
		}
		cooldown := snapshot.Cooldown
		if cooldown <= 0 {
			cooldown = config.InitialCooldown
		}
		m.items.Store(webhookID, &webhookBreaker{
			state:           state,
			failureCount:    snapshot.FailureCount,
			cooldown:        cooldown,
			nextProbeAt:     snapshot.NextProbeAt,
			probeDeliveryID: snapshot.ProbeDeliveryID,
		})
	}
	return m
}

// RecordSuccess records a successful Webhook Attempt.
func (m *Memory) RecordSuccess(webhookID string) {
	if webhookID == "" {
		return
	}
	b := m.acquire(webhookID)
	var transition *Transition
	b.mu.Lock()
	b.advanceLocked(m.now())
	changed := b.state != Closed || b.failureCount != 0 || b.probeDeliveryID != ""
	b.state = Closed
	b.failureCount = 0
	b.cooldown = m.config.InitialCooldown
	b.nextProbeAt = time.Time{}
	b.probeDeliveryID = ""
	if changed && m.config.FailureThreshold > 0 {
		transition = &Transition{
			WebhookID:    webhookID,
			State:        Closed,
			FailureCount: 0,
			Cooldown:     b.cooldown,
		}
	}
	b.mu.Unlock()
	b.release()
	m.emit(transition)
}

// RecordFailure records a failed Webhook Attempt.
func (m *Memory) RecordFailure(webhookID string, reason string) {
	if webhookID == "" || m.config.FailureThreshold == 0 {
		return
	}
	b := m.acquire(webhookID)
	var transition *Transition
	b.mu.Lock()
	b.advanceLocked(m.now())
	switch b.state {
	case HalfOpen:
		b.failureCount++
		cooldown := nextCooldown(b.cooldown, m.config)
		transition = b.openLocked(webhookID, b.failureCount, cooldown, reason)
	case Closed:
		b.failureCount++
		if b.failureCount >= m.config.FailureThreshold {
			transition = b.openLocked(webhookID, b.failureCount, m.config.InitialCooldown, reason)
		}
	case Open:
		// Already open: the state transition has been emitted, so avoid making
		// partner outages produce unbounded persistence work.
	}
	b.mu.Unlock()
	b.release()
	m.emit(transition)
}

// State returns the current local breaker state for a Webhook.
func (m *Memory) State(webhookID string) State {
	if webhookID == "" || m.config.FailureThreshold == 0 {
		return Closed
	}
	b := m.acquire(webhookID)
	b.mu.Lock()
	b.advanceLocked(m.now())
	state := b.state
	b.mu.Unlock()
	b.release()
	return state
}

// IsEligible reports whether a Webhook is locally eligible for delivery.
func (m *Memory) IsEligible(webhookID string) bool {
	state := m.State(webhookID)
	return state == Closed || state == HalfOpen
}

// SetProbeDelivery records the DB-assigned half-open probe delivery.
func (m *Memory) SetProbeDelivery(webhookID, deliveryID string) {
	if webhookID == "" || deliveryID == "" || m.config.FailureThreshold == 0 {
		return
	}
	b := m.acquire(webhookID)
	var transition *Transition
	b.mu.Lock()
	b.advanceLocked(m.now())
	if b.state != HalfOpen {
		b.state = HalfOpen
	}
	if b.probeDeliveryID != deliveryID {
		b.probeDeliveryID = deliveryID
		transition = &Transition{
			WebhookID:       webhookID,
			State:           HalfOpen,
			FailureCount:    b.failureCount,
			Cooldown:        b.cooldown,
			NextProbeAt:     b.nextProbeAt,
			ProbeDeliveryID: deliveryID,
		}
	}
	b.mu.Unlock()
	b.release()
	m.emit(transition)
}

func (m *Memory) acquire(webhookID string) *webhookBreaker {
	for {
		if existing, ok := m.items.Load(webhookID); ok {
			b := existing.(*webhookBreaker)
			b.mu.Lock()
			if b.deleted {
				b.mu.Unlock()
				m.items.CompareAndDelete(webhookID, b)
				continue
			}
			b.active++
			b.mu.Unlock()
			return b
		}
		created := &webhookBreaker{
			state:    Closed,
			cooldown: m.config.InitialCooldown,
			active:   1,
		}
		actual, loaded := m.items.LoadOrStore(webhookID, created)
		if !loaded {
			return created
		}
		b := actual.(*webhookBreaker)
		b.mu.Lock()
		if b.deleted {
			b.mu.Unlock()
			m.items.CompareAndDelete(webhookID, b)
			continue
		}
		b.active++
		b.mu.Unlock()
		return b
	}
}

func (b *webhookBreaker) release() {
	b.mu.Lock()
	b.active--
	b.mu.Unlock()
}

func (m *Memory) emit(transition *Transition) {
	if transition == nil || m.sink == nil {
		return
	}
	m.sink(*transition)
}

func (m *Memory) now() time.Time {
	return time.Now()
}

// Cleanup removes breaker entries that are Closed with no pending state and no
// active caller. acquire recreates entries on demand, so this is safe to call
// periodically to prevent unbounded growth in the items map.
func (m *Memory) Cleanup() {
	m.items.Range(func(key, value any) bool {
		b := value.(*webhookBreaker)
		b.mu.Lock()
		// Any idle closed breaker can be evicted safely. Failure history is only
		// needed while the entry is live; once a webhook has gone idle, recreating
		// the entry on demand is preferable to keeping the map unbounded.
		closed := b.state == Closed && b.active == 0
		if closed {
			b.deleted = true
		}
		b.mu.Unlock()
		if closed {
			m.items.CompareAndDelete(key, value)
		}
		return true
	})
}

// RunCleanup periodically removes idle Closed entries until ctx is canceled.
func (m *Memory) RunCleanup(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = defaultCleanupInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			m.Cleanup()
		}
	}
}

func (b *webhookBreaker) advanceLocked(now time.Time) {
	if b.state == Open && !b.nextProbeAt.IsZero() && !now.Before(b.nextProbeAt) {
		b.state = HalfOpen
		b.probeDeliveryID = ""
	}
}

func (b *webhookBreaker) openLocked(webhookID string, failureCount int, cooldown time.Duration, reason string) *Transition {
	b.state = Open
	b.failureCount = failureCount
	b.cooldown = cooldown
	b.nextProbeAt = time.Now().Add(cooldown)
	b.probeDeliveryID = ""
	return &Transition{
		WebhookID:    webhookID,
		State:        Open,
		FailureCount: failureCount,
		Cooldown:     cooldown,
		NextProbeAt:  b.nextProbeAt,
		Reason:       reason,
	}
}
