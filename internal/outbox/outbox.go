package outbox

import (
	"database/sql"
	"time"
)

// DeliveryStatus is the lifecycle state stored on a Delivery row.
type DeliveryStatus string

const (
	// DefaultClaimLease is the period a worker has to finish a claimed Delivery
	// before another worker may recover it.
	DefaultClaimLease = 5 * time.Minute

	// StatusPending means the Delivery has not been attempted yet.
	StatusPending DeliveryStatus = "pending"
	// StatusInFlight means a worker has claimed the Delivery (logical status
	// returned to Go code; the delivery row itself never stores 'in_flight').
	StatusInFlight DeliveryStatus = "in_flight"
	// StatusFailedRetry means the Delivery is waiting for a retry.
	StatusFailedRetry DeliveryStatus = "failed_retry"
	// StatusSucceeded means the partner accepted the Delivery with 2xx.
	StatusSucceeded DeliveryStatus = "succeeded"
	// StatusDead means the Delivery reached a terminal failure.
	StatusDead DeliveryStatus = "dead"
)

// ArchiveEnrichmentMode controls how much related data retention loads during
// archive passes.
type ArchiveEnrichmentMode string

const (
	// ArchiveEnrichmentFull archives attempt-summary and related metadata.
	ArchiveEnrichmentFull ArchiveEnrichmentMode = "full"
	// ArchiveEnrichmentDeferred archives core fields and defers heavy enrichment.
	ArchiveEnrichmentDeferred ArchiveEnrichmentMode = "deferred"
)

const (
	// BreakerStateClosed means the Webhook is claimable normally.
	BreakerStateClosed = "closed"
	// BreakerStateOpen means Deliveries for the Webhook are skipped until the next probe time.
	BreakerStateOpen = "open"
	// BreakerStateHalfOpen means exactly one Delivery is allowed as a recovery probe.
	BreakerStateHalfOpen = "half_open"
)

// ClaimQueryMode controls which SQL shape the store uses for claim discovery.
type ClaimQueryMode string

const (
	// ClaimQueryModeSplit uses branch-specific scans for ready vs stale claims.
	ClaimQueryModeSplit ClaimQueryMode = "split"
	// ClaimQueryModeLegacy uses the combined claimable predicate.
	ClaimQueryModeLegacy ClaimQueryMode = "legacy"
)

// BreakerConfig controls persisted per-Webhook circuit breaker transitions.
// A FailureThreshold of 0 disables breaker tripping entirely.
type BreakerConfig struct {
	FailureThreshold int
	InitialCooldown  time.Duration
	MaxCooldown      time.Duration
}

func defaultBreakerConfig() BreakerConfig {
	return BreakerConfig{
		FailureThreshold: 20,
		InitialCooldown:  5 * time.Minute,
		MaxCooldown:      15 * time.Minute,
	}
}

func normalizeBreakerConfig(config BreakerConfig) BreakerConfig {
	defaults := defaultBreakerConfig()
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

// Delivery is a claimed outbox row with enough data for the worker to POST it.
type Delivery struct {
	ID            string
	EventID       string
	WebhookID     string
	CustomerID    string
	CustomerClass uint32
	EventType     string
	Payload       []byte
	URL           string
	Secret        string
	Status        DeliveryStatus
	AttemptNumber int
	PreviousDelay time.Duration
	NextAttemptAt time.Time
	ClaimedAt     time.Time
	LastError     sql.NullString
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// Store claims and mutates Delivery rows in Postgres.
type Store struct {
	db            *sql.DB
	claimLease    time.Duration
	breakerConfig BreakerConfig
	claimMode     ClaimQueryMode
}

// Option configures a Store.
type Option func(*Store)

// WithClaimLease configures how long an in-flight Delivery can remain claimed
// before another worker may recover it.
func WithClaimLease(claimLease time.Duration) Option {
	return func(s *Store) {
		if claimLease > 0 {
			s.claimLease = claimLease
		}
	}
}

// WithBreakerConfig configures per-Webhook circuit breaker transitions.
func WithBreakerConfig(config BreakerConfig) Option {
	return func(s *Store) {
		s.breakerConfig = normalizeBreakerConfig(config)
	}
}

// WithClaimQueryMode selects which claim SQL shape the store should use.
func WithClaimQueryMode(mode ClaimQueryMode) Option {
	return func(s *Store) {
		switch mode {
		case ClaimQueryModeLegacy:
			s.claimMode = ClaimQueryModeLegacy
		default:
			s.claimMode = ClaimQueryModeSplit
		}
	}
}

// New returns an outbox store backed by db.
func New(db *sql.DB, opts ...Option) *Store {
	store := &Store{
		db:            db,
		claimLease:    DefaultClaimLease,
		breakerConfig: defaultBreakerConfig(),
		claimMode:     ClaimQueryModeSplit,
	}
	for _, opt := range opts {
		opt(store)
	}

	return store
}
