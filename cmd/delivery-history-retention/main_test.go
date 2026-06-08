package main

import (
	"context"
	"database/sql"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/lvluu/webhook-notifier/internal/ingest"
	"github.com/lvluu/webhook-notifier/internal/outbox"
	"github.com/lvluu/webhook-notifier/internal/registry"
	"github.com/lvluu/webhook-notifier/internal/retry"
	"github.com/lvluu/webhook-notifier/internal/testutil/retention"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestRunArchivesOldTerminalDeliveriesAndLeavesRecentRows(t *testing.T) {
	tests := []struct {
		name       string
		enrichment outbox.ArchiveEnrichmentMode
	}{
		{name: "full", enrichment: outbox.ArchiveEnrichmentFull},
		{name: "deferred", enrichment: outbox.ArchiveEnrichmentDeferred},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			db := newPostgresDB(t, ctx)
			store := outbox.New(db)

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))
			defer srv.Close()

			oldSucceededID := submitOneDeliveryToURL(t, ctx, db, "cus_archive_success", srv.URL)
			claimedSucceeded, ok, err := store.ClaimNext(ctx, "cus_archive_success")
			if err != nil {
				t.Fatalf("ClaimNext succeeded delivery: %v", err)
			}
			if !ok {
				t.Fatal("ClaimNext succeeded delivery: got no delivery, want one")
			}
			if err := store.MarkSucceeded(ctx, claimedSucceeded.ID, http.StatusOK, 10*time.Millisecond); err != nil {
				t.Fatalf("MarkSucceeded: %v", err)
			}

			oldDeadID := submitOneDeliveryToURL(t, ctx, db, "cus_archive_dead", srv.URL)
			claimedDead, ok, err := store.ClaimNext(ctx, "cus_archive_dead")
			if err != nil {
				t.Fatalf("ClaimNext dead delivery: %v", err)
			}
			if !ok {
				t.Fatal("ClaimNext dead delivery: got no delivery, want one")
			}
			if err := store.MarkDead(ctx, claimedDead.ID, http.StatusNotFound, retry.ReasonTerminal4xx, retry.ReasonTerminal4xx, 10*time.Millisecond); err != nil {
				t.Fatalf("MarkDead: %v", err)
			}

			recentID := submitOneDeliveryToURL(t, ctx, db, "cus_archive_recent", srv.URL)
			claimedRecent, ok, err := store.ClaimNext(ctx, "cus_archive_recent")
			if err != nil {
				t.Fatalf("ClaimNext recent delivery: %v", err)
			}
			if !ok {
				t.Fatal("ClaimNext recent delivery: got no delivery, want one")
			}
			if err := store.MarkSucceeded(ctx, claimedRecent.ID, http.StatusOK, 10*time.Millisecond); err != nil {
				t.Fatalf("MarkSucceeded recent: %v", err)
			}

			retention.AgeTerminalDelivery(t, ctx, db, oldSucceededID, 48*time.Hour)
			retention.AgeTerminalDelivery(t, ctx, db, oldDeadID, 48*time.Hour)

			archived, err := runDeliveryHistoryRetention(ctx, config{
				DatabaseURL: "",
				Retention:   24 * time.Hour,
				BatchSize:   1,
				Enrichment:  tt.enrichment,
				DB:          db,
				Out:         io.Discard,
			})
			if err != nil {
				t.Fatalf("runDeliveryHistoryRetention: %v", err)
			}
			if archived != 2 {
				t.Fatalf("archived count: got %d, want 2", archived)
			}

			assertDeliveryMissing(t, ctx, db, oldSucceededID)
			assertDeliveryMissing(t, ctx, db, oldDeadID)
			assertDeliveryStatus(t, ctx, db, recentID, string(outbox.StatusSucceeded))
			assertDeliveryHistory(t, ctx, db, oldSucceededID, "succeeded", sql.NullString{}, 1)
			assertDeliveryHistory(t, ctx, db, oldDeadID, "dead", sql.NullString{String: retry.ReasonTerminal4xx, Valid: true}, 1)
		})
	}
}

func newPostgresDB(t *testing.T, ctx context.Context) *sql.DB {
	t.Helper()

	ctr, err := postgres.RunContainer(
		ctx,
		testcontainers.WithImage("postgres:17-alpine"),
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

	db, err := sql.Open("pgx", connString)
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
	`, customerID, url, eventTypes, active)
	if err != nil {
		t.Fatalf("insert webhook: %v", err)
	}
}

func submitOneDeliveryToURL(t *testing.T, ctx context.Context, db *sql.DB, customerID, url string) string {
	t.Helper()

	insertWebhook(t, ctx, db, customerID, url, true, "subscriber.created")
	submitted, err := ingest.New(db, registry.New(db), retention.NoopNotifier{}).Submit(ctx, ingest.Event{
		CustomerID: customerID,
		Type:       "subscriber.created",
		Payload:    []byte(`{"subscriber_id":"sub_123"}`),
	})
	if err != nil {
		t.Fatalf("Submit %s: %v", customerID, err)
	}
	if len(submitted.DeliveryIDs) != 1 {
		t.Fatalf("delivery count for %s: got %d, want 1", customerID, len(submitted.DeliveryIDs))
	}
	return submitted.DeliveryIDs[0]
}

func assertDeliveryMissing(t *testing.T, ctx context.Context, db *sql.DB, deliveryID string) {
	t.Helper()

	var exists bool
	err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM deliveries WHERE id = $1)`, deliveryID).Scan(&exists)
	if err != nil {
		t.Fatalf("check delivery missing: %v", err)
	}
	if exists {
		t.Fatalf("delivery %s still exists", deliveryID)
	}
}

func assertDeliveryStatus(t *testing.T, ctx context.Context, db *sql.DB, deliveryID, want string) {
	t.Helper()

	var got string
	err := db.QueryRowContext(ctx, `SELECT status FROM deliveries WHERE id = $1`, deliveryID).Scan(&got)
	if err != nil {
		t.Fatalf("select delivery status: %v", err)
	}
	if got != want {
		t.Fatalf("delivery status: got %q, want %q", got, want)
	}
}

func assertDeliveryHistory(t *testing.T, ctx context.Context, db *sql.DB, deliveryID, wantStatus string, wantReason sql.NullString, wantAttempts int) {
	t.Helper()

	var gotStatus string
	var gotReason sql.NullString
	var gotAttempts int
	var finalAt time.Time
	err := db.QueryRowContext(ctx, `
		SELECT final_status, terminal_reason, attempt_count, final_at
		FROM delivery_history
		WHERE delivery_id = $1
	`, deliveryID).Scan(&gotStatus, &gotReason, &gotAttempts, &finalAt)
	if err != nil {
		t.Fatalf("select delivery history: %v", err)
	}
	if gotStatus != wantStatus {
		t.Fatalf("history status: got %q, want %q", gotStatus, wantStatus)
	}
	if gotReason.Valid != wantReason.Valid || gotReason.String != wantReason.String {
		t.Fatalf("history reason: got %q valid=%v, want %q valid=%v", gotReason.String, gotReason.Valid, wantReason.String, wantReason.Valid)
	}
	if gotAttempts != wantAttempts {
		t.Fatalf("history attempts: got %d, want %d", gotAttempts, wantAttempts)
	}
	if finalAt.IsZero() {
		t.Fatalf("history final_at not set")
	}
}
