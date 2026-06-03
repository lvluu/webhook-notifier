// Package dbweights provides a DB-backed WeightSource that reads Customer
// class from the customers table with in-memory caching.
package dbweights

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/lvluu/webhook-notifier/internal/scheduler"
)

const defaultQueryTimeout = 2 * time.Second

// DBWeightSource resolves Customer fairness weights from the customers table,
// caching results in memory to avoid per-Pick DB queries. Unknown customers
// default to weight 1. Use Refresh / RefreshAll to re-read changed classes.
type DBWeightSource struct {
	db    *sql.DB
	mu    sync.RWMutex
	cache map[scheduler.CustomerID]uint32
}

// New returns a DBWeightSource backed by db.
func New(db *sql.DB) *DBWeightSource {
	return &DBWeightSource{
		db:    db,
		cache: make(map[scheduler.CustomerID]uint32),
	}
}

// Weight returns the cached or freshly loaded class value for customerID.
func (s *DBWeightSource) Weight(customerID scheduler.CustomerID) uint32 {
	if customerID == "" {
		return 1
	}

	s.mu.RLock()
	w, ok := s.cache[customerID]
	s.mu.RUnlock()
	if ok {
		return w
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if w, ok := s.cache[customerID]; ok {
		return w
	}

	w = s.loadWeight(customerID)
	s.cache[customerID] = w
	return w
}

// Refresh invalidates the cache entry for customerID and reloads from DB.
func (s *DBWeightSource) Refresh(customerID scheduler.CustomerID) {
	if customerID == "" {
		return
	}

	w := s.loadWeight(customerID)

	s.mu.Lock()
	s.cache[customerID] = w
	s.mu.Unlock()
}

// RefreshAll reloads every cached weight from the DB.
func (s *DBWeightSource) RefreshAll() {
	s.mu.Lock()
	for id := range s.cache {
		s.cache[id] = s.loadWeight(id)
	}
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
