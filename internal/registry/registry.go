package registry

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/lib/pq"
	"golang.org/x/sync/singleflight"
)

// Webhook is a destination URL and its configuration for a Customer.
type Webhook struct {
	ID              string
	CustomerID      string
	URL             string
	SecretEncrypted string
	EventTypes      []string
	Active          bool
}

// Registry resolves which Webhooks should receive a given Event type for a Customer.
type Registry interface {
	ListActiveForEvent(ctx context.Context, customerID string, eventType string) ([]Webhook, error)
}

const (
	defaultCapacity = 10_000
	defaultTTL      = 30 * time.Second
)

type cache struct {
	db       *sql.DB
	lru      *expirable.LRU[string, []Webhook]
	sf       singleflight.Group
	query    string
	capacity int
	ttl      time.Duration
}

// Option configures a Registry cache.
type Option func(*cache)

// WithCapacity sets the maximum number of cache entries.
func WithCapacity(capacity int) Option {
	return func(c *cache) {
		if capacity > 0 {
			c.capacity = capacity
		}
	}
}

// WithTTL sets the time-to-live for each cache entry.
func WithTTL(ttl time.Duration) Option {
	return func(c *cache) {
		if ttl > 0 {
			c.ttl = ttl
		}
	}
}

// New returns a Registry backed by Postgres with an LRU + TTL front cache.
func New(db *sql.DB, opts ...Option) Registry {
	c := &cache{
		db:       db,
		capacity: defaultCapacity,
		ttl:      defaultTTL,
		query:    `SELECT id, customer_id, url, secret_encrypted, event_types, active FROM webhooks WHERE customer_id = $1 AND active = true AND $2 = ANY(event_types) ORDER BY id`,
	}
	for _, opt := range opts {
		opt(c)
	}
	c.lru = expirable.NewLRU[string, []Webhook](c.capacity, nil, c.ttl)
	return c
}

func (c *cache) ListActiveForEvent(ctx context.Context, customerID string, eventType string) ([]Webhook, error) {
	if customerID == "" {
		return nil, fmt.Errorf("customer_id is required")
	}
	if eventType == "" {
		return nil, fmt.Errorf("event_type is required")
	}

	key := cacheKey(customerID, eventType)

	if hooks, ok := c.lru.Get(key); ok {
		return hooks, nil
	}

	result, err, _ := c.sf.Do(key, func() (interface{}, error) {
		hooks, err := c.queryDB(ctx, customerID, eventType)
		if err != nil {
			return nil, err
		}
		c.lru.Add(key, hooks)
		return hooks, nil
	})
	if err != nil {
		return nil, err
	}
	return result.([]Webhook), nil
}

func (c *cache) queryDB(ctx context.Context, customerID string, eventType string) ([]Webhook, error) {
	rows, err := c.db.QueryContext(ctx, c.query, customerID, eventType)
	if err != nil {
		return nil, fmt.Errorf("query webhooks: %w", err)
	}
	defer rows.Close()

	var hooks []Webhook
	for rows.Next() {
		var h Webhook
		if err := rows.Scan(&h.ID, &h.CustomerID, &h.URL, &h.SecretEncrypted, pq.Array(&h.EventTypes), &h.Active); err != nil {
			return nil, fmt.Errorf("scan webhook: %w", err)
		}
		hooks = append(hooks, h)
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
	return customerID + ":" + eventType
}
