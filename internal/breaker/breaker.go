// Package breaker contains Webhook Circuit Breaker decision and persistence
// code kept outside the outbox mark hot path.
package breaker

import "time"

// State is a Webhook Circuit Breaker lifecycle state.
type State string

const (
	// Closed means normal delivery is allowed.
	Closed State = "closed"
	// Open means deliveries should be skipped until the next probe time.
	Open State = "open"
	// HalfOpen means one probe delivery is allowed.
	HalfOpen State = "half_open"
)

// Config controls breaker failure thresholds and cooldown backoff.
type Config struct {
	FailureThreshold int
	InitialCooldown  time.Duration
	MaxCooldown      time.Duration
}

// Breaker records Webhook Attempt outcomes and answers local eligibility.
type Breaker interface {
	RecordSuccess(webhookID string)
	RecordFailure(webhookID string, reason string)
	State(webhookID string) State
	IsEligible(webhookID string) bool
	SetProbeDelivery(webhookID, deliveryID string)
}

// Snapshot is a persisted breaker state loaded at process startup.
type Snapshot struct {
	WebhookID       string
	State           State
	FailureCount    int
	Cooldown        time.Duration
	NextProbeAt     time.Time
	ProbeDeliveryID string
}

// Transition is a lifecycle change that should be persisted for claim filtering.
type Transition struct {
	WebhookID       string
	State           State
	FailureCount    int
	Cooldown        time.Duration
	NextProbeAt     time.Time
	ProbeDeliveryID string
	Reason          string
}

// TransitionSink receives lifecycle transitions emitted by a Breaker.
type TransitionSink func(Transition)

// NormalizeConfig fills defaults and clamps invalid cooldowns.
func NormalizeConfig(config Config) Config {
	defaults := Config{
		FailureThreshold: 20,
		InitialCooldown:  5 * time.Minute,
		MaxCooldown:      15 * time.Minute,
	}
	if config.FailureThreshold < 0 {
		config.FailureThreshold = defaults.FailureThreshold
	}
	if config.InitialCooldown <= 0 {
		config.InitialCooldown = defaults.InitialCooldown
	}
	if config.MaxCooldown <= 0 {
		config.MaxCooldown = defaults.MaxCooldown
	}
	if config.MaxCooldown < config.InitialCooldown {
		config.MaxCooldown = config.InitialCooldown
	}
	return config
}

func nextCooldown(current time.Duration, config Config) time.Duration {
	if current <= 0 {
		return config.InitialCooldown
	}
	next := current * 2
	if next > config.MaxCooldown {
		return config.MaxCooldown
	}
	return next
}
