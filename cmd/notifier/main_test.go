package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/lvluu/webhook-notifier/internal/ingest"
	"github.com/lvluu/webhook-notifier/internal/outbox"
	"github.com/lvluu/webhook-notifier/internal/registry"
	"github.com/lvluu/webhook-notifier/internal/testutil/retention"
	"github.com/lvluu/webhook-notifier/internal/worker"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

type stubIngestSubmitter struct {
	submitFn func(context.Context, ingest.Event) (ingest.SubmittedEvent, error)
}

func (s stubIngestSubmitter) Submit(ctx context.Context, event ingest.Event) (ingest.SubmittedEvent, error) {
	if s.submitFn == nil {
		return ingest.SubmittedEvent{}, errors.New("submitFn not configured")
	}
	return s.submitFn(ctx, event)
}

func TestBusinessHandler_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	logger := slogDiscard()
	handler := newBusinessHandler(logger, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/events", nil)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status: got %d, want %d", rr.Code, http.StatusMethodNotAllowed)
	}
}

func TestBusinessHandler_InvalidJSON(t *testing.T) {
	t.Parallel()

	logger := slogDiscard()
	handler := newBusinessHandler(logger, stubIngestSubmitter{
		submitFn: func(_ context.Context, _ ingest.Event) (ingest.SubmittedEvent, error) {
			return ingest.SubmittedEvent{}, nil
		},
	}, nil)
	req := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader("{"))
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status: got %d, want %d", rr.Code, http.StatusBadRequest)
	}
}

func TestBusinessHandler_NilStoreReturnsSanitizedError(t *testing.T) {
	t.Parallel()

	logger := slogDiscard()
	handler := newBusinessHandler(logger, nil, nil)
	req := httptest.NewRequest(
		http.MethodPost,
		"/events",
		strings.NewReader(`{"customer_id":"cus_1","event_name":"subscriber.created","event_time":"2026-05-31T00:00:00Z","subscriber":{"id":"sub_1"}}`),
	)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status: got %d, want %d", rr.Code, http.StatusInternalServerError)
	}
	if strings.Contains(strings.ToLower(rr.Body.String()), "nil") {
		t.Fatalf("response should be sanitized, got %q", rr.Body.String())
	}
}

func TestBusinessHandler_RequiresFlodeskEventShape(t *testing.T) {
	t.Parallel()

	logger := slogDiscard()
	handler := newBusinessHandler(logger, stubIngestSubmitter{
		submitFn: func(_ context.Context, _ ingest.Event) (ingest.SubmittedEvent, error) {
			return ingest.SubmittedEvent{}, nil
		},
	}, nil)
	req := httptest.NewRequest(
		http.MethodPost,
		"/events",
		strings.NewReader(`{"customer_id":"cus_1","type":"subscriber.created","payload":{"x":1}}`),
	)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status: got %d, want %d", rr.Code, http.StatusBadRequest)
	}
}

func TestBusinessHandler_RejectsMissingEventName(t *testing.T) {
	t.Parallel()

	logger := slogDiscard()
	handler := newBusinessHandler(logger, stubIngestSubmitter{
		submitFn: func(_ context.Context, _ ingest.Event) (ingest.SubmittedEvent, error) {
			return ingest.SubmittedEvent{}, nil
		},
	}, nil)
	req := httptest.NewRequest(
		http.MethodPost,
		"/events",
		strings.NewReader(`{"customer_id":"cus_1","event_time":"2026-05-31T00:00:00Z","subscriber":{"id":"sub_1"}}`),
	)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status: got %d, want %d", rr.Code, http.StatusBadRequest)
	}
}

func TestBusinessHandler_RejectsMissingEventTime(t *testing.T) {
	t.Parallel()

	logger := slogDiscard()
	handler := newBusinessHandler(logger, stubIngestSubmitter{
		submitFn: func(_ context.Context, _ ingest.Event) (ingest.SubmittedEvent, error) {
			return ingest.SubmittedEvent{}, nil
		},
	}, nil)
	req := httptest.NewRequest(
		http.MethodPost,
		"/events",
		strings.NewReader(`{"customer_id":"cus_1","event_name":"subscriber.created","subscriber":{"id":"sub_1"}}`),
	)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status: got %d, want %d", rr.Code, http.StatusBadRequest)
	}
}

func TestBusinessHandler_SubmitsFlodeskEnvelope(t *testing.T) {
	t.Parallel()

	logger := slogDiscard()
	var got ingest.Event
	handler := newBusinessHandler(logger, stubIngestSubmitter{
		submitFn: func(_ context.Context, event ingest.Event) (ingest.SubmittedEvent, error) {
			got = event
			return ingest.SubmittedEvent{EventID: "evt_1", DeliveryIDs: []string{"del_1"}}, nil
		},
	}, nil)
	req := httptest.NewRequest(
		http.MethodPost,
		"/events",
		strings.NewReader(`{"customer_id":"cus_1","event_name":"subscriber.created","event_time":"2026-05-31T00:00:00Z","subscriber":{"id":"sub_1"}}`),
	)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("status: got %d, want %d", rr.Code, http.StatusAccepted)
	}
	if got.CustomerID != "cus_1" {
		t.Fatalf("CustomerID: got %q, want %q", got.CustomerID, "cus_1")
	}
	if got.Type != "subscriber.created" {
		t.Fatalf("Type: got %q, want %q", got.Type, "subscriber.created")
	}
	if strings.Contains(string(got.Payload), "customer_id") {
		t.Fatalf("payload should not include customer_id, got %s", string(got.Payload))
	}
	if !strings.Contains(string(got.Payload), `"event_name":"subscriber.created"`) {
		t.Fatalf("payload missing event_name, got %s", string(got.Payload))
	}
	if !strings.Contains(string(got.Payload), `"event_time":"2026-05-31T00:00:00Z"`) {
		t.Fatalf("payload missing event_time, got %s", string(got.Payload))
	}
}

func TestStartDeliveryRuntime_ColdBootDiscoveryDrainsExistingDeliveries(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer receiver.Close()

	const customerID = "cus_cold_boot"
	const deliveries = 12

	insertWebhook(t, ctx, db, customerID, receiver.URL, true, "subscriber.created")
	ingestStore := ingest.New(db, registry.New(db), retention.NoopNotifier{})
	for i := 0; i < deliveries; i++ {
		_, err := ingestStore.Submit(ctx, ingest.Event{
			CustomerID: customerID,
			Type:       "subscriber.created",
			Payload:    json.RawMessage(`{"subscriber_id":"sub_123"}`),
		})
		if err != nil {
			t.Fatalf("seed delivery %d: %v", i+1, err)
		}
	}

	logger := slogDiscard()
	runtime, runtimeErrCh, err := startDeliveryRuntime(ctx, logger, db, workerRuntimeConfig{
		WorkerCount:    2,
		RequestTimeout: 2 * time.Second,
		Discovery: worker.DiscoveryConfig{
			Interval:     10 * time.Millisecond,
			EmptyBackoff: 10 * time.Millisecond,
			ErrorBackoff: 10 * time.Millisecond,
			MaxBackoff:   50 * time.Millisecond,
			Limit:        100,
		},
	})
	if err != nil {
		t.Fatalf("startDeliveryRuntime: %v", err)
	}
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = runtime.Stop(stopCtx)
	}()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case runErr, ok := <-runtimeErrCh:
			if ok {
				t.Fatalf("runtime error: %v", runErr)
			}
		default:
		}
		if got := countSucceededForCustomer(t, ctx, db, customerID); got == deliveries {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}

	got := countSucceededForCustomer(t, ctx, db, customerID)
	t.Fatalf("succeeded deliveries: got %d, want %d", got, deliveries)
}

func countSucceededForCustomer(t *testing.T, ctx context.Context, db *sql.DB, customerID string) int {
	t.Helper()
	var n int
	err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM deliveries
		WHERE customer_id = $1 AND status = $2
	`, customerID, outbox.StatusSucceeded).Scan(&n)
	if err != nil {
		t.Fatalf("count succeeded for %s: %v", customerID, err)
	}
	return n
}

func newPostgresDB(t *testing.T, ctx context.Context) *sql.DB {
	t.Helper()

	ctr, err := postgres.RunContainer(
		ctx,
		testcontainers.WithImage("postgres:16-alpine"),
		postgres.WithDatabase("notifier"),
		postgres.WithUsername("notifier"),
		postgres.WithPassword("notifier"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(10*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() {
		if err := ctr.Terminate(ctx); err != nil {
			t.Errorf("terminate postgres container: %v", err)
		}
	})

	connString, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	db, err := sql.Open("postgres", connString)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close postgres: %v", err)
		}
	})

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping postgres: %v", err)
	}
	applyMigrations(t, ctx, db)

	return db
}

func applyMigrations(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()

	files, err := filepath.Glob(filepath.Join("..", "..", "migrations", "*.up.sql"))
	if err != nil {
		t.Fatalf("find migrations: %v", err)
	}
	sort.Strings(files)
	for _, file := range files {
		sqlBytes, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read migration %s: %v", file, err)
		}
		if _, err := db.ExecContext(ctx, string(sqlBytes)); err != nil {
			t.Fatalf("apply migration %s: %v", file, err)
		}
	}
}

func insertWebhook(t *testing.T, ctx context.Context, db *sql.DB, customerID, url string, active bool, eventTypes ...string) {
	t.Helper()

	if len(eventTypes) == 0 {
		t.Fatal("insertWebhook requires at least one event type")
	}

	_, err := db.ExecContext(ctx, `
		INSERT INTO webhooks (customer_id, url, secret_encrypted, event_types, active)
		VALUES ($1, $2, 'test-secret', $3, $4)
	`, customerID, url, pq.Array(eventTypes), active)
	if err != nil {
		t.Fatalf("insert webhook: %v", err)
	}
}

func slogDiscard() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
