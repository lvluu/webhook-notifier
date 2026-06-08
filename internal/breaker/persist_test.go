package breaker

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
)

// closeErrorRows implements driver.Rows and returns an error on Close.
type closeErrorRows struct {
	sent     bool
	closeErr error
}

func (r *closeErrorRows) Columns() []string {
	return []string{"id", "breaker_state", "breaker_failure_count",
		"breaker_cooldown_ms", "breaker_next_probe_at", "breaker_probe_delivery_id"}
}

func (r *closeErrorRows) Close() error { return r.closeErr }
func (r *closeErrorRows) Next(dest []driver.Value) error {
	if r.sent {
		return io.EOF
	}
	r.sent = true
	dest[0] = "wh_1"
	dest[1] = "closed"
	dest[2] = int64(0)
	dest[3] = int64(30000)
	dest[4] = nil
	dest[5] = nil
	return nil
}

// closeErrorConn implements driver.Conn with QueryContext only.
type closeErrorConn struct{ closeErr error }

func (closeErrorConn) Prepare(_ string) (driver.Stmt, error) { return nil, nil }
func (closeErrorConn) Close() error                          { return nil }
func (closeErrorConn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("begin not supported in closeErrorConn")
}
func (c closeErrorConn) QueryContext(_ context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	return &closeErrorRows{closeErr: c.closeErr}, nil
}

// closeErrorConnector implements driver.Connector for sql.OpenDB.
type closeErrorConnector struct{ closeErrorDriver }

func (c closeErrorConnector) Connect(_ context.Context) (driver.Conn, error) {
	return closeErrorConn{closeErr: c.closeErr}, nil
}
func (c closeErrorConnector) Driver() driver.Driver { return c.closeErrorDriver }

// closeErrorDriver implements driver.Driver.
type closeErrorDriver struct{ closeErr error }

func (d closeErrorDriver) Open(_ string) (driver.Conn, error) {
	return closeErrorConn(d), nil
}

func TestLoadFromDB_propagates_close_error(t *testing.T) {
	closeErr := errors.New("connection closed unexpectedly")
	db := sql.OpenDB(closeErrorConnector{closeErrorDriver{closeErr: closeErr}})
	t.Cleanup(func() { db.Close() })

	_, err := LoadFromDB(context.Background(), db)
	t.Logf("LoadFromDB returned: %v", err)
	if err == nil {
		t.Fatal("expected error from rows.Close, got nil")
	}
	if !strings.Contains(err.Error(), closeErr.Error()) {
		t.Fatalf("expected error to contain %q, got: %v", closeErr.Error(), err)
	}
}

func TestAsyncPersisterSinkFallsBackSynchronouslyWhenQueueFull(t *testing.T) {
	p := NewAsyncPersister(nil, nil, 1)
	first := Transition{WebhookID: "wh_1", State: Open}
	second := Transition{WebhookID: "wh_2", State: Open}

	var mu sync.Mutex
	var persisted []Transition
	p.persist = func(_ context.Context, _ *sql.DB, transition Transition) error {
		mu.Lock()
		defer mu.Unlock()
		persisted = append(persisted, transition)
		return nil
	}

	sink := p.Sink()
	sink(first)
	sink(second)

	mu.Lock()
	defer mu.Unlock()
	if len(persisted) != 1 {
		t.Fatalf("persisted fallback transitions = %d, want 1", len(persisted))
	}
	if persisted[0].WebhookID != second.WebhookID {
		t.Fatalf("persisted fallback webhook = %q, want %q", persisted[0].WebhookID, second.WebhookID)
	}

	select {
	case queued := <-p.ch:
		if queued.WebhookID != first.WebhookID {
			t.Fatalf("queued webhook = %q, want %q", queued.WebhookID, first.WebhookID)
		}
	default:
		t.Fatal("expected first transition to remain queued")
	}
}
