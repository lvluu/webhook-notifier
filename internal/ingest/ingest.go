package ingest

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/lvluu/webhook-notifier/internal/observe"
	"github.com/lvluu/webhook-notifier/internal/registry"
)

const claimableCustomersChannel = "claimable_customers"

// Registry resolves which Webhooks should receive a given Event type for a Customer.
// Defined at the consumer boundary; implementations live in internal/registry.
type Registry interface {
	ListActiveForEvent(ctx context.Context, customerID string, eventType string) ([]Webhook, error)
}

// Webhook aliases the registry package's Webhook shape at the ingest boundary.
type Webhook = registry.Webhook

// Event is a business fact that should be fanned out to matching Webhooks.
type Event struct {
	CustomerID string
	Type       string
	Payload    json.RawMessage
}

// SubmittedEvent describes the durable rows created for an Event.
type SubmittedEvent struct {
	EventID     string
	DeliveryIDs []string
}

// Notifier informs the Scheduler that a Customer has new pending Deliveries.
type Notifier interface {
	Notify(customerID string)
}

// Store persists Events and their derived Delivery rows in Postgres.
type Store struct {
	db                *sql.DB
	registry          Registry
	notifier          Notifier
	claimNotifyEnable bool
}

// New returns an Event ingest store backed by db.
func New(db *sql.DB, reg Registry, notifier Notifier, opts ...Option) *Store {
	s := &Store{
		db:                db,
		registry:          reg,
		notifier:          notifier,
		claimNotifyEnable: true,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Option configures a Store.
type Option func(*Store)

// WithClaimableCustomerNotify enables/disables cross-replica pg_notify hints.
func WithClaimableCustomerNotify(enabled bool) Option {
	return func(s *Store) {
		s.claimNotifyEnable = enabled
	}
}

// Submit writes the Event and one Delivery per matching active Webhook, then
// notifies the Scheduler that the Customer has pending Deliveries.
func (s *Store) Submit(ctx context.Context, event Event) (SubmittedEvent, error) {
	if event.CustomerID == "" {
		return SubmittedEvent{}, fmt.Errorf("customer_id is required")
	}
	if event.Type == "" {
		return SubmittedEvent{}, fmt.Errorf("type is required")
	}
	if !json.Valid(event.Payload) {
		return SubmittedEvent{}, fmt.Errorf("payload must be valid JSON")
	}

	hooks, err := s.registry.ListActiveForEvent(ctx, event.CustomerID, event.Type)
	if err != nil {
		return SubmittedEvent{}, fmt.Errorf("lookup webhooks: %w", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SubmittedEvent{}, fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	var submitted SubmittedEvent
	err = tx.QueryRowContext(ctx, `
		INSERT INTO events (customer_id, type, payload)
		VALUES ($1, $2, $3)
		RETURNING id
	`, event.CustomerID, event.Type, string(event.Payload)).Scan(&submitted.EventID)
	if err != nil {
		return SubmittedEvent{}, fmt.Errorf("insert event: %w", err)
	}

	if len(hooks) > 0 {
		deliveryIDs, err := insertDeliveries(ctx, tx, submitted.EventID, hooks)
		if err != nil {
			return SubmittedEvent{}, err
		}
		submitted.DeliveryIDs = deliveryIDs
	}

	if err := tx.Commit(); err != nil {
		return SubmittedEvent{}, fmt.Errorf("commit transaction: %w", err)
	}

	if len(hooks) > 0 {
		s.notifier.Notify(event.CustomerID)
		s.emitClaimableCustomerNotify(event.CustomerID)
	}

	observe.ObserveIngestFanout(float64(len(submitted.DeliveryIDs)))
	return submitted, nil
}

func (s *Store) emitClaimableCustomerNotify(customerID string) {
	if !s.claimNotifyEnable || s.db == nil || customerID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	_, _ = s.db.ExecContext(ctx, `SELECT pg_notify($1, $2)`, claimableCustomersChannel, customerID)
}

func insertDeliveries(ctx context.Context, tx *sql.Tx, eventID string, hooks []Webhook) ([]string, error) {
	var sb strings.Builder
	sb.WriteString(`INSERT INTO deliveries (event_id, webhook_id, customer_id, status, attempt_count, next_attempt_at) VALUES `)

	args := make([]interface{}, 0, len(hooks)*3)
	for i, h := range hooks {
		if i > 0 {
			sb.WriteString(", ")
		}
		base := i * 3
		fmt.Fprintf(&sb, "($%d, $%d, $%d, 'pending', 0, now())", base+1, base+2, base+3)
		args = append(args, eventID, h.ID, h.CustomerID)
	}
	sb.WriteString(" RETURNING id")

	rows, err := tx.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("insert deliveries: %w", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan delivery id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close delivery rows: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scan delivery ids: %w", err)
	}

	return ids, nil
}
