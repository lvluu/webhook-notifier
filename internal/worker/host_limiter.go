package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/lvluu/webhook-notifier/internal/observe"
	"golang.org/x/sync/semaphore"
)

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

// HostLimiter caps concurrent in-flight requests per partner hostname.
type HostLimiter struct {
	sems sync.Map
	cfg  HostLimiterConfig
}

// NewHostLimiter returns a limiter with the given configuration.
func NewHostLimiter(cfg HostLimiterConfig) *HostLimiter {
	cfg = cfg.defaults()
	return &HostLimiter{
		cfg: cfg,
	}
}

func (h *HostLimiter) semFor(host string) *semaphore.Weighted {
	if v, ok := h.sems.Load(host); ok {
		return v.(*semaphore.Weighted)
	}
	sem := semaphore.NewWeighted(int64(h.cfg.MaxPerHost))
	actual, _ := h.sems.LoadOrStore(host, sem)
	return actual.(*semaphore.Weighted)
}

// Acquire blocks until a slot for host is available or the configured timeout
// expires. On success it returns a release function that must be called exactly
// once. On timeout or ctx cancellation it returns an error without consuming a slot.
func (h *HostLimiter) Acquire(ctx context.Context, host string) (release func(), err error) {
	sem := h.semFor(host)
	acquireStarted := time.Now()

	acquireCtx, cancel := context.WithTimeout(ctx, h.cfg.AcquireTimeout)
	defer cancel()

	if err := sem.Acquire(acquireCtx, 1); err != nil {
		// record wait even on timeout — useful SRE signal
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
func (h *HostLimiter) TryAcquire(host string) (release func(), err error) {
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
func (h *HostLimiter) RequeueDelay() time.Duration {
	return h.cfg.RequeueDelay
}
