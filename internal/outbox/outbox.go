package outbox

import (
	"database/sql"
	"fmt"
	"time"
)

// DeliveryStatus is the lifecycle state stored on a Delivery row.
type DeliveryStatus string

const (
	// DefaultClaimLease is the period a worker has to finish a claimed Delivery
	// before another worker may recover it.
	DefaultClaimLease = 5 * time.Minute

	// DefaultClaimTimeout is the maximum time a single claim or discovery query
	// may run. Prevents a stalled Postgres connection from blocking a worker
	// indefinitely; the worker will receive a context.DeadlineExceeded error
	// and retry after transientErrorBackoff.
	DefaultClaimTimeout = 10 * time.Second

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
	URLHost       string
	Secret        string
	Status        DeliveryStatus
	AttemptNumber int
	PreviousDelay time.Duration
	NextAttemptAt time.Time
	ClaimedAt     time.Time
	LastError     sql.NullString
	CreatedAt     time.Time
	UpdatedAt     time.Time
	BreakerProbe  bool
}

// Store claims and mutates Delivery rows in Postgres.
type Store struct {
	db               *sql.DB
	claimLease       time.Duration
	breakerFiltering bool
	claimTimeout     time.Duration

	staleSQL string // pre-computed stale predicate with claimLease baked in
	anySQL   string // pre-computed (ready OR stale) predicate
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

// WithBreakerFiltering configures whether claim queries respect persisted
// Webhook circuit breaker state. Breaker decision-making lives in
// internal/breaker; this only controls the DB claim predicate.
func WithBreakerFiltering(enabled bool) Option {
	return func(s *Store) {
		s.breakerFiltering = enabled
	}
}

// WithClaimTimeout sets the maximum duration for a single claim or
// claimable-customer discovery query. Defaults to DefaultClaimTimeout.
// A value of 0 disables the per-query deadline.
func WithClaimTimeout(d time.Duration) Option {
	return func(s *Store) {
		s.claimTimeout = d
	}
}

// New returns an outbox store backed by db.
func New(db *sql.DB, opts ...Option) *Store {
	store := &Store{
		db:               db,
		claimLease:       DefaultClaimLease,
		breakerFiltering: true,
		claimTimeout:     DefaultClaimTimeout,
	}
	for _, opt := range opts {
		opt(store)
	}

	leaseMS := store.claimLease.Milliseconds()
	store.staleSQL = fmt.Sprintf("dc.claimed_at < now() - (%d * interval '1 millisecond')", leaseMS)
	store.anySQL = fmt.Sprintf(`(
      (%s)
      OR (%s)
  )`, sqlReady, store.staleSQL)

	return store
}
