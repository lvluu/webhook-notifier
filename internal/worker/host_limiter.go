package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/lvluu/webhook-notifier/internal/observe"
	"golang.org/x/sync/semaphore"
)

// HostLimiter caps concurrent in-flight requests per partner hostname.
// Implementations may be process-local (single replica) or fleet-wide
// (distributed across replicas via a lease store).
type HostLimiter interface {
	Acquire(ctx context.Context, host string) (release func(), err error)
	TryAcquire(host string) (release func(), err error)
	RequeueDelay() time.Duration
}

// HostLimiterConfig configures the per-host concurrency limiter.
type HostLimiterConfig struct {
	MaxPerHost     int
	AcquireTimeout time.Duration
	RequeueDelay   time.Duration
}

func (c HostLimiterConfig) defaults() HostLimiterConfig {
	cfg := c
	if cfg.MaxPerHost <= 0 {
		cfg.MaxPerHost = 20
	}
	if cfg.AcquireTimeout <= 0 {
		cfg.AcquireTimeout = 500 * time.Millisecond
	}
	if cfg.RequeueDelay <= 0 {
		cfg.RequeueDelay = 100 * time.Millisecond
	}
	return cfg
}

// LocalHostLimiter enforces a per-host concurrency cap within a single process
// using an in-memory weighted semaphore. Suitable for single-replica deployments.
type LocalHostLimiter struct {
	mu   sync.Mutex
	sems map[string]*semaphore.Weighted
	cfg  HostLimiterConfig
}

// NewHostLimiter returns a local limiter with the given configuration.
func NewHostLimiter(cfg HostLimiterConfig) *LocalHostLimiter {
	cfg = cfg.defaults()
	return &LocalHostLimiter{
		sems: make(map[string]*semaphore.Weighted),
		cfg:  cfg,
	}
}

func (h *LocalHostLimiter) semFor(host string) *semaphore.Weighted {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.sems[host] == nil {
		h.sems[host] = semaphore.NewWeighted(int64(h.cfg.MaxPerHost))
	}
	return h.sems[host]
}

// Acquire blocks until a slot for host is available or the configured timeout
// expires. On success it returns a release function that must be called exactly
// once. On timeout or ctx cancellation it returns an error without consuming a slot.
func (h *LocalHostLimiter) Acquire(ctx context.Context, host string) (release func(), err error) {
	sem := h.semFor(host)
	acquireStarted := time.Now()

	acquireCtx, cancel := context.WithTimeout(ctx, h.cfg.AcquireTimeout)
	defer cancel()

	if err := sem.Acquire(acquireCtx, 1); err != nil {
		observe.ObserveSemaphoreWait(host, time.Since(acquireStarted).Seconds())
		if ctx.Err() != nil {
			return nil, fmt.Errorf("host limiter acquire %s: %w", host, ctx.Err())
		}
		return nil, fmt.Errorf("host limiter acquire %s: capacity reached: %w", host, err)
	}

	observe.ObserveSemaphoreWait(host, time.Since(acquireStarted).Seconds())

	released := false
	return func() {
		if !released {
			released = true
			sem.Release(1)
		}
	}, nil
}

// TryAcquire attempts to acquire a slot for host without blocking. On success
// it returns a release function. On capacity exhaustion it returns an error
// immediately without consuming a slot.
func (h *LocalHostLimiter) TryAcquire(host string) (release func(), err error) {
	sem := h.semFor(host)
	acquireStarted := time.Now()
	if !sem.TryAcquire(1) {
		observe.ObserveSemaphoreWait(host, time.Since(acquireStarted).Seconds())
		return nil, fmt.Errorf("host limiter acquire %s: capacity reached", host)
	}

	observe.ObserveSemaphoreWait(host, time.Since(acquireStarted).Seconds())

	released := false
	return func() {
		if !released {
			released = true
			sem.Release(1)
		}
	}, nil
}

// RequeueDelay returns the configured delay for requeuing a Delivery that could
// not acquire a host slot.
func (h *LocalHostLimiter) RequeueDelay() time.Duration {
	return h.cfg.RequeueDelay
}
