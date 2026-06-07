package breaker

import (
	"sync"
	"time"
)

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
	b := m.get(webhookID)
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
	m.emit(transition)
}

// RecordFailure records a failed Webhook Attempt.
func (m *Memory) RecordFailure(webhookID string, reason string) {
	if webhookID == "" || m.config.FailureThreshold == 0 {
		return
	}
	b := m.get(webhookID)
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
	m.emit(transition)
}

// State returns the current local breaker state for a Webhook.
func (m *Memory) State(webhookID string) State {
	if webhookID == "" || m.config.FailureThreshold == 0 {
		return Closed
	}
	b := m.get(webhookID)
	b.mu.Lock()
	b.advanceLocked(m.now())
	state := b.state
	b.mu.Unlock()
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
	b := m.get(webhookID)
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
	m.emit(transition)
}

func (m *Memory) get(webhookID string) *webhookBreaker {
	if existing, ok := m.items.Load(webhookID); ok {
		return existing.(*webhookBreaker)
	}
	created := &webhookBreaker{
		state:    Closed,
		cooldown: m.config.InitialCooldown,
	}
	actual, _ := m.items.LoadOrStore(webhookID, created)
	return actual.(*webhookBreaker)
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
