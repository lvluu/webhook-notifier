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
type DBWeightSource struct {
	db     *sql.DB
	mu     sync.RWMutex
	cache  map[scheduler.CustomerID]cachedEntry
	flight singleflight.Group
	ttl    time.Duration
}

// New returns a DBWeightSource backed by db.
func New(db *sql.DB, opts ...Option) *DBWeightSource {
	s := &DBWeightSource{
		db:    db,
		cache: make(map[scheduler.CustomerID]cachedEntry),
		ttl:   defaultCacheTTL,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
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
	return v.(uint32)
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
