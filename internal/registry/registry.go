package registry

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/jackc/pgx/v5/pgtype"
	"golang.org/x/sync/singleflight"
)

var defaultPGMap = pgtype.NewMap()

// Webhook is a destination URL and its configuration for a Customer.
type Webhook struct {
	ID              string
	CustomerID      string
	URL             string
	SecretEncrypted string
	EventTypes      []string
	Active          bool
}

// Registry is a Postgres-backed webhook lookup with an LRU + TTL front cache.
type Registry struct {
	db       *sql.DB
	lru      *expirable.LRU[string, []Webhook]
	sf       singleflight.Group
	query    string
	capacity int
	ttl      time.Duration
}

const (
	defaultCapacity = 10_000
	defaultTTL      = 30 * time.Second
)

// Option configures a Registry.
type Option func(*Registry)

// WithCapacity sets the maximum number of cache entries.
func WithCapacity(capacity int) Option {
	return func(r *Registry) {
		if capacity > 0 {
			r.capacity = capacity
		}
	}
}

// WithTTL sets the time-to-live for each cache entry.
func WithTTL(ttl time.Duration) Option {
	return func(r *Registry) {
		if ttl > 0 {
			r.ttl = ttl
		}
	}
}

// New returns a concrete *Registry backed by Postgres with an LRU + TTL front cache.
func New(db *sql.DB, opts ...Option) *Registry {
	r := &Registry{
		db:       db,
		capacity: defaultCapacity,
		ttl:      defaultTTL,
		query:    `SELECT id, customer_id, url, secret_encrypted, event_types, active FROM webhooks WHERE customer_id = $1 AND active = true AND $2 = ANY(event_types) ORDER BY id`,
	}
	for _, opt := range opts {
		opt(r)
	}
	r.lru = expirable.NewLRU[string, []Webhook](r.capacity, nil, r.ttl)
	return r
}

// ListActiveForEvent returns the active Webhooks for a Customer/Event pair.
func (r *Registry) ListActiveForEvent(ctx context.Context, customerID string, eventType string) ([]Webhook, error) {
	if customerID == "" {
		return nil, fmt.Errorf("customer_id is required")
	}
	if eventType == "" {
		return nil, fmt.Errorf("event_type is required")
	}

	key := cacheKey(customerID, eventType)

	if hooks, ok := r.lru.Get(key); ok {
		return hooks, nil
	}

	// Use DoChan + isolated context so one caller's cancellation never
	// propagates to other coalesced goroutines. Each caller selects on its
	// own ctx.Done() independently.
	ch := r.sf.DoChan(key, func() (interface{}, error) {
		qCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		hooks, err := r.queryDB(qCtx, customerID, eventType)
		if err != nil {
			return nil, err
		}
		r.lru.Add(key, hooks)
		return hooks, nil
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.([]Webhook), nil
	}
}

func (r *Registry) queryDB(ctx context.Context, customerID string, eventType string) ([]Webhook, error) {
	rows, err := r.db.QueryContext(ctx, r.query, customerID, eventType)
	if err != nil {
		return nil, fmt.Errorf("query webhooks: %w", err)
	}
	var hooks []Webhook
	for rows.Next() {
		var h Webhook
		if err := rows.Scan(&h.ID, &h.CustomerID, &h.URL, &h.SecretEncrypted, defaultPGMap.SQLScanner(&h.EventTypes), &h.Active); err != nil {
			return nil, fmt.Errorf("scan webhook: %w", err)
		}
		hooks = append(hooks, h)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close webhook rows: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate webhooks: %w", err)
	}
	if hooks == nil {
		hooks = []Webhook{}
	}
	return hooks, nil
}

func cacheKey(customerID, eventType string) string {
	// \x00 cannot appear in text identifiers — prevents ambiguous key collisions.
	return customerID + "\x00" + eventType
}
