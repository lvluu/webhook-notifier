package worker

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/lvluu/webhook-notifier/internal/observe"
)

// DistributedHostLimiterConfig configures the fleet-wide distributed host limiter.
type DistributedHostLimiterConfig struct {
	MaxPerHost     int
	LeaseTTL       time.Duration
	AcquireTimeout time.Duration
	RequeueDelay   time.Duration
}

func (c DistributedHostLimiterConfig) defaults() DistributedHostLimiterConfig {
	cfg := c
	if cfg.MaxPerHost <= 0 {
		cfg.MaxPerHost = 20
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = 30 * time.Second
	}
	if cfg.AcquireTimeout <= 0 {
		cfg.AcquireTimeout = 500 * time.Millisecond
	}
	if cfg.RequeueDelay <= 0 {
		cfg.RequeueDelay = 100 * time.Millisecond
	}
	return cfg
}

// DistributedHostLimiter enforces a per-host concurrency cap across multiple
// notifier replicas using a Postgres lease table as the coordination point.
// Lease rows carry an expires_at TTL; stale rows (expires_at < now()) are
// excluded from the active count so crashed replicas release their slots
// automatically.
type DistributedHostLimiter struct {
	db  *sql.DB
	cfg DistributedHostLimiterConfig
}

// NewDistributedHostLimiter returns a fleet-wide limiter backed by Postgres.
func NewDistributedHostLimiter(db *sql.DB, cfg DistributedHostLimiterConfig) *DistributedHostLimiter {
	cfg = cfg.defaults()
	return &DistributedHostLimiter{
		db:  db,
		cfg: cfg,
	}
}

// Acquire blocks until a slot for host is available or the configured timeout
// expires. On success it returns a release function that must be called exactly
// once. On timeout or ctx cancellation it returns an error without consuming a slot.
func (h *DistributedHostLimiter) Acquire(ctx context.Context, host string) (release func(), err error) {
	acquireStarted := time.Now()
	acquireCtx, cancel := context.WithTimeout(ctx, h.cfg.AcquireTimeout)
	defer cancel()

	for {
		release, err := h.tryAcquire(ctx, host)
		if err == nil {
			observe.ObserveSemaphoreWait(host, time.Since(acquireStarted).Seconds())
			return release, nil
		}
		if acquireCtx.Err() != nil {
			observe.ObserveSemaphoreWait(host, time.Since(acquireStarted).Seconds())
			if ctx.Err() != nil {
				return nil, fmt.Errorf("host limiter acquire %s: %w", host, ctx.Err())
			}
			return nil, fmt.Errorf("host limiter acquire %s: capacity reached: %w", host, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TryAcquire attempts to acquire a slot for host without blocking. On success
// it returns a release function. On capacity exhaustion it returns an error
// immediately without consuming a slot.
func (h *DistributedHostLimiter) TryAcquire(host string) (release func(), err error) {
	acquireStarted := time.Now()
	release, err = h.tryAcquire(context.Background(), host)
	observe.ObserveSemaphoreWait(host, time.Since(acquireStarted).Seconds())
	return release, err
}

func (h *DistributedHostLimiter) tryAcquire(ctx context.Context, host string) (func(), error) {
	var leaseID string
	err := h.db.QueryRowContext(ctx, `
		INSERT INTO host_concurrency_leases (hostname, lease_id, expires_at)
		SELECT $1, gen_random_uuid(), now() + ($2 * interval '1 millisecond')
		WHERE (
			SELECT count(*)
			FROM host_concurrency_leases
			WHERE hostname = $1
			  AND expires_at > now()
		) < $3
		RETURNING lease_id
	`, host, h.cfg.LeaseTTL.Milliseconds(), h.cfg.MaxPerHost).Scan(&leaseID)
	if err == sql.ErrNoRows {
		observe.IncHostConcurrencyRejected(host)
		return nil, fmt.Errorf("host limiter acquire %s: capacity reached", host)
	}
	if err != nil {
		return nil, fmt.Errorf("host limiter acquire %s: %w", host, err)
	}

	observe.IncHostConcurrencyAcquired(host)

	released := false
	return func() {
		if !released {
			released = true
			h.releaseLease(context.Background(), host, leaseID)
			observe.IncHostConcurrencyAcquired(host)
			// Note: InFlight gauge is best-effort; accurate count requires a
			// periodic reconciliation query which is deferred to the
			// observability follow-up.
		}
	}, nil
}

func (h *DistributedHostLimiter) releaseLease(ctx context.Context, host, leaseID string) {
	_, err := h.db.ExecContext(ctx, `
		DELETE FROM host_concurrency_leases
		WHERE hostname = $1 AND lease_id = $2
	`, host, leaseID)
	if err != nil {
		_ = err
	}
}

// RequeueDelay returns the configured delay for requeuing a Delivery that could
// not acquire a host slot.
func (h *DistributedHostLimiter) RequeueDelay() time.Duration {
	return h.cfg.RequeueDelay
}
