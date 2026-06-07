package worker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
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

type hostEntry struct {
	sem      *semaphore.Weighted
	lastUsed atomic.Int64
}

func (e *hostEntry) touch() {
	e.lastUsed.Store(time.Now().UnixNano())
}

func (e *hostEntry) staleSince(maxAge time.Duration) bool {
	return time.Since(time.Unix(0, e.lastUsed.Load())) > maxAge
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

func (h *HostLimiter) semFor(host string) *hostEntry {
	if v, ok := h.sems.Load(host); ok {
		entry := v.(*hostEntry)
		entry.touch()
		return entry
	}
	entry := &hostEntry{
		sem: semaphore.NewWeighted(int64(h.cfg.MaxPerHost)),
	}
	entry.touch()
	actual, loaded := h.sems.LoadOrStore(host, entry)
	if loaded {
		actual.(*hostEntry).touch()
		return actual.(*hostEntry)
	}
	return entry
}

// Acquire blocks until a slot for host is available or the configured timeout
// expires. On success it returns a release function that must be called exactly
// once. On timeout or ctx cancellation it returns an error without consuming a slot.
func (h *HostLimiter) Acquire(ctx context.Context, host string) (release func(), err error) {
	sem := h.semFor(host).sem
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
	sem := h.semFor(host).sem
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

// Cleanup removes host entries that have been idle for longer than maxAge.
// New entries are created on demand, so this is safe to call periodically
// to prevent unbounded growth in the sems map.
func (h *HostLimiter) Cleanup(maxAge time.Duration) {
	h.sems.Range(func(key, value any) bool {
		entry := value.(*hostEntry)
		if entry.staleSince(maxAge) {
			h.sems.Delete(key)
		}
		return true
	})
}

// RunCleanup periodically calls Cleanup until ctx is canceled.
func (h *HostLimiter) RunCleanup(ctx context.Context, interval, maxAge time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			h.Cleanup(maxAge)
		}
	}
}
