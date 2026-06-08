package breaker

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// execStubResult satisfies driver.Result for stubbed ExecContext calls.
type execStubResult struct{}

func (execStubResult) LastInsertId() (int64, error) { return 0, nil }
func (execStubResult) RowsAffected() (int64, error) { return 0, nil }

// execStubConn returns a scripted error (or nil) per ExecContext call so tests
// can exercise the deferBlockedDeliveries retry loop deterministically.
type execStubConn struct {
	mu     sync.Mutex
	calls  int
	script []error
}

func (c *execStubConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("not implemented")
}
func (c *execStubConn) Close() error              { return nil }
func (c *execStubConn) Begin() (driver.Tx, error) { return nil, errors.New("not implemented") }

func (c *execStubConn) ExecContext(_ context.Context, _ string, _ []driver.NamedValue) (driver.Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	i := c.calls
	c.calls++
	if i < len(c.script) {
		if err := c.script[i]; err != nil {
			return nil, err
		}
	}
	return execStubResult{}, nil
}

// CheckNamedValue accepts any argument (e.g. []string) without invoking the
// default driver value converter, which rejects slice types.
func (c *execStubConn) CheckNamedValue(*driver.NamedValue) error { return nil }

func (c *execStubConn) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

type execStubConnector struct{ conn *execStubConn }

func (c execStubConnector) Connect(context.Context) (driver.Conn, error) { return c.conn, nil }
func (c execStubConnector) Driver() driver.Driver                        { return execStubDriver{} }

type execStubDriver struct{}

func (execStubDriver) Open(string) (driver.Conn, error) { return nil, errors.New("use OpenDB") }

func newStubDB(t *testing.T, script []error) (*sql.DB, *execStubConn) {
	t.Helper()
	conn := &execStubConn{script: script}
	db := sql.OpenDB(execStubConnector{conn: conn})
	t.Cleanup(func() { _ = db.Close() })
	// One shared connection keeps the call script deterministic across retries.
	db.SetMaxOpenConns(1)
	return db, conn
}

func TestDeferBlockedDeliveries_retriesThenSucceeds(t *testing.T) {
	deadlock := &pgconn.PgError{Code: "40P01"}
	db, conn := newStubDB(t, []error{deadlock, deadlock, nil})

	if err := deferBlockedDeliveries(context.Background(), db, []string{"wh_1"}); err != nil {
		t.Fatalf("expected success after retries, got: %v", err)
	}
	if got := conn.callCount(); got != 3 {
		t.Fatalf("ExecContext calls = %d, want 3", got)
	}
}

func TestDeferBlockedDeliveries_exhaustsRetries(t *testing.T) {
	deadlock := &pgconn.PgError{Code: "40P01"}
	db, conn := newStubDB(t, []error{deadlock, deadlock, deadlock})

	err := deferBlockedDeliveries(context.Background(), db, []string{"wh_1"})
	if err == nil {
		t.Fatal("expected error after exhausting retries, got nil")
	}
	if !errors.Is(err, deadlock) {
		t.Fatalf("expected wrapped deadlock error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "after 3 attempts") {
		t.Fatalf("expected attempt count in message, got: %v", err)
	}
	if got := conn.callCount(); got != deferMaxAttempts {
		t.Fatalf("ExecContext calls = %d, want %d", got, deferMaxAttempts)
	}
}

func TestDeferBlockedDeliveries_nonRetryableFailsFast(t *testing.T) {
	uniqueViolation := &pgconn.PgError{Code: "23505"}
	db, conn := newStubDB(t, []error{uniqueViolation})

	err := deferBlockedDeliveries(context.Background(), db, []string{"wh_1"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if errors.Is(err, uniqueViolation) == false {
		t.Fatalf("expected wrapped pg error, got: %v", err)
	}
	if got := conn.callCount(); got != 1 {
		t.Fatalf("ExecContext calls = %d, want 1 (no retry on non-lock error)", got)
	}
}

func TestDeferBlockedDeliveries_noWebhooksIsNoop(t *testing.T) {
	db, conn := newStubDB(t, nil)
	if err := deferBlockedDeliveries(context.Background(), db, nil); err != nil {
		t.Fatalf("expected nil for empty webhook list, got: %v", err)
	}
	if got := conn.callCount(); got != 0 {
		t.Fatalf("ExecContext calls = %d, want 0", got)
	}
}
