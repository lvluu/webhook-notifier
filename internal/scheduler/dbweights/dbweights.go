// Package dbweights provides a DB-backed WeightSource that reads Customer
// class from the customers table with in-memory caching.
package dbweights

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/lvluu/webhook-notifier/internal/scheduler"
)

const (
	defaultQueryTimeout = 2 * time.Second
	defaultCacheTTL     = 5 * time.Minute
)

type cachedEntry struct {
	value     uint32
	fetchedAt time.Time
}

// Option customizes a DBWeightSource.
type Option func(*DBWeightSource)

// WithCacheTTL sets how long a loaded Customer weight remains fresh.
func WithCacheTTL(ttl time.Duration) Option {
	return func(s *DBWeightSource) {
		if ttl > 0 {
			s.ttl = ttl
		}
	}
}

// DBWeightSource resolves Customer fairness weights from the customers table,
// caching results in memory to avoid per-Pick DB queries. Unknown customers
// default to weight 1. Use Refresh to re-read changed classes.
//
// A background goroutine evicts expired cache entries every ttl/4 so the map
// does not grow unboundedly under high Customer churn. Call Stop to shut the
// goroutine down cleanly.
type DBWeightSource struct {
	db     *sql.DB
	mu     sync.RWMutex
	cache  map[scheduler.CustomerID]cachedEntry
	flight singleflight.Group
	ttl    time.Duration
	ctx    context.Context
	cancel context.CancelFunc
}

// New returns a DBWeightSource backed by db and starts its background eviction
// goroutine. Call Stop to shut the goroutine down cleanly.
func New(db *sql.DB, opts ...Option) *DBWeightSource {
	ctx, cancel := context.WithCancel(context.Background())
	s := &DBWeightSource{
		db:     db,
		cache:  make(map[scheduler.CustomerID]cachedEntry),
		ttl:    defaultCacheTTL,
		ctx:    ctx,
		cancel: cancel,
	}
	for _, opt := range opts {
		opt(s)
	}
	go s.evictLoop()
	return s
}

// Stop shuts down the background eviction goroutine. The cache is not flushed;
// subsequent Weight calls still work but will not evict expired entries until a
// new DBWeightSource is created.
func (s *DBWeightSource) Stop() {
	s.cancel()
}

// Weight returns the cached or freshly loaded class value for customerID.
func (s *DBWeightSource) Weight(customerID scheduler.CustomerID) uint32 {
	if customerID == "" {
		return 1
	}

	s.mu.RLock()
	entry, ok := s.cache[customerID]
	s.mu.RUnlock()
	if ok && time.Since(entry.fetchedAt) < s.ttl {
		return entry.value
	}

	// Cache miss or stale: use singleflight to coalesce concurrent loads for
	// the same customer into a single DB query. The exclusive write lock is
	// acquired only after the query returns, so other Weight callers are not
	// blocked for the duration of the round trip.
	v, _, _ := s.flight.Do(customerID, func() (any, error) {
		w := s.loadWeight(customerID)
		s.mu.Lock()
		s.cache[customerID] = cachedEntry{value: w, fetchedAt: time.Now()}
		s.mu.Unlock()
		return w, nil
	})
	w, ok := v.(uint32)
	if !ok {
		return 0
	}
	return w
}

// Refresh invalidates the cache entry for customerID and reloads from DB.
func (s *DBWeightSource) Refresh(customerID scheduler.CustomerID) {
	if customerID == "" {
		return
	}

	w := s.loadWeight(customerID)

	s.mu.Lock()
	s.cache[customerID] = cachedEntry{value: w, fetchedAt: time.Now()}
	s.mu.Unlock()
}

// evictLoop periodically removes expired cache entries so the map does not
// grow unboundedly. Runs at ttl/4 intervals so eviction delay is bounded.
func (s *DBWeightSource) evictLoop() {
	interval := s.ttl / 4
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.evict()
		}
	}
}

func (s *DBWeightSource) evict() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for id, entry := range s.cache {
		if now.Sub(entry.fetchedAt) >= s.ttl {
			delete(s.cache, id)
		}
	}
}

func (s *DBWeightSource) loadWeight(customerID scheduler.CustomerID) uint32 {
	ctx, cancel := context.WithTimeout(context.Background(), defaultQueryTimeout)
	defer cancel()

	var class uint32
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE(class, 1) FROM customers WHERE id = $1`, customerID).Scan(&class)
	if err != nil {
		// Interface contract: Weight() cannot surface errors.
		// Fall back to 1 so the scheduler degrades to equal-share
		// round-robin rather than dropping the customer entirely.
		return 1
	}
	return class
}
