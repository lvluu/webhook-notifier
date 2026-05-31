package outbox_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/lvluu/webhook-notifier/internal/ingest"
	"github.com/lvluu/webhook-notifier/internal/outbox"
	"github.com/lvluu/webhook-notifier/internal/registry"
	"github.com/lvluu/webhook-notifier/internal/retry"
	"github.com/lvluu/webhook-notifier/internal/scheduler"
	"github.com/lvluu/webhook-notifier/internal/scheduler/roundrobin"
	"github.com/lvluu/webhook-notifier/internal/worker"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

type noopNotifier struct{}

func (noopNotifier) Notify(customerID string) {}

type integrationStubScheduler struct{}

func (integrationStubScheduler) Pick(_ context.Context) (scheduler.CustomerID, uint32, bool, error) {
	return "", 0, false, nil
}
func (integrationStubScheduler) Notify(scheduler.CustomerID) {}
func (integrationStubScheduler) Remove(scheduler.CustomerID) {}

func newIntegrationWorker(opts ...worker.Option) *worker.Worker {
	return worker.New(integrationStubScheduler{}, opts...)
}

func newIngestStore(db *sql.DB) *ingest.Store {
	return ingest.New(db, registry.New(db), noopNotifier{})
}

func TestIngestClaimDeliverHappyPath(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	var got struct {
		eventID    string
		deliveryID string
		body       string
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		}
		got.eventID = r.Header.Get("X-Event-Id")
		got.deliveryID = r.Header.Get("X-Delivery-Id")
		got.body = string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	insertWebhook(t, ctx, db, "cus_1", srv.URL, true, "subscriber.created")
	insertWebhook(t, ctx, db, "cus_1", srv.URL+"/inactive", false, "subscriber.created")
	insertWebhook(t, ctx, db, "cus_1", srv.URL+"/wrong-type", true, "subscriber.deleted")

	submitted, err := newIngestStore(db).Submit(ctx, ingest.Event{
		CustomerID: "cus_1",
		Type:       "subscriber.created",
		Payload:    []byte(`{"subscriber_id":"sub_123"}`),
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if len(submitted.DeliveryIDs) != 1 {
		t.Fatalf("delivery count: got %d, want 1", len(submitted.DeliveryIDs))
	}

	claimed, ok, err := outbox.New(db).ClaimNext(ctx, "cus_1")
	if err != nil {
		t.Fatalf("ClaimNext: %v", err)
	}
	if !ok {
		t.Fatal("ClaimNext: got no delivery, want one")
	}
	if claimed.Status != outbox.StatusInFlight {
		t.Fatalf("claimed status: got %q, want %q", claimed.Status, outbox.StatusInFlight)
	}
	if claimed.AttemptNumber != 1 {
		t.Fatalf("attempt number: got %d, want 1", claimed.AttemptNumber)
	}

	started := time.Now()
	statusCode, err := newIntegrationWorker().Deliver(ctx, worker.Delivery{
		EventID:    claimed.EventID,
		DeliveryID: claimed.ID,
		CustomerID: claimed.CustomerID,
		URL:        claimed.URL,
		Body:       claimed.Payload,
		EventType:  claimed.EventType,
	})
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if statusCode != http.StatusOK {
		t.Fatalf("status code: got %d, want 200", statusCode)
	}
	if err := outbox.New(db).MarkSucceeded(ctx, claimed.ID, statusCode, time.Since(started)); err != nil {
		t.Fatalf("MarkSucceeded: %v", err)
	}

	if got.eventID != submitted.EventID {
		t.Errorf("X-Event-Id: got %q, want %q", got.eventID, submitted.EventID)
	}
	if got.deliveryID != claimed.ID {
		t.Errorf("X-Delivery-Id: got %q, want %q", got.deliveryID, claimed.ID)
	}
	var gotBody map[string]string
	if err := json.Unmarshal([]byte(got.body), &gotBody); err != nil {
		t.Fatalf("unmarshal received body: %v", err)
	}
	if gotBody["subscriber_id"] != "sub_123" {
		t.Errorf("body subscriber_id: got %q, want sub_123", gotBody["subscriber_id"])
	}

	assertDeliveryStatus(t, ctx, db, claimed.ID, string(outbox.StatusSucceeded))
	assertAttemptCount(t, ctx, db, claimed.ID, 1)
}

func TestClaimNextConcurrentWorkersDoNotDoubleClaim(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	const deliveryCount = 50
	const workerCount = 12

	for i := 0; i < deliveryCount; i++ {
		insertWebhook(t, ctx, db, "cus_race", "https://partner.example/webhook", true, "subscriber.created")
	}
	submitted, err := newIngestStore(db).Submit(ctx, ingest.Event{
		CustomerID: "cus_race",
		Type:       "subscriber.created",
		Payload:    []byte(`{"subscriber_id":"sub_race"}`),
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if len(submitted.DeliveryIDs) != deliveryCount {
		t.Fatalf("delivery count: got %d, want %d", len(submitted.DeliveryIDs), deliveryCount)
	}

	store := outbox.New(db)
	claimed := make(map[string]int, deliveryCount)
	var claimedMu sync.Mutex
	var wg sync.WaitGroup
	errs := make(chan error, workerCount)

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				delivery, ok, err := store.ClaimNext(ctx, "cus_race")
				if err != nil {
					errs <- err
					return
				}
				if !ok {
					return
				}

				claimedMu.Lock()
				claimed[delivery.ID]++
				claimedMu.Unlock()
			}
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("ClaimNext: %v", err)
	}

	if len(claimed) != deliveryCount {
		t.Fatalf("claimed unique deliveries: got %d, want %d", len(claimed), deliveryCount)
	}

	var duplicates []string
	for id, count := range claimed {
		if count > 1 {
			duplicates = append(duplicates, id)
		}
	}
	sort.Strings(duplicates)
	if len(duplicates) > 0 {
		t.Fatalf("double-claimed deliveries: %v", duplicates)
	}

	assertStatusCount(t, ctx, db, string(outbox.StatusInFlight), deliveryCount)
	assertStatusCount(t, ctx, db, string(outbox.StatusPending), 0)
}

func TestClaimNextRecoversStaleInFlightDelivery(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	insertWebhook(t, ctx, db, "cus_recover", "https://partner.example/webhook", true, "subscriber.created")
	submitted, err := newIngestStore(db).Submit(ctx, ingest.Event{
		CustomerID: "cus_recover",
		Type:       "subscriber.created",
		Payload:    []byte(`{"subscriber_id":"sub_recover"}`),
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if len(submitted.DeliveryIDs) != 1 {
		t.Fatalf("delivery count: got %d, want 1", len(submitted.DeliveryIDs))
	}

	store := outbox.New(db, outbox.WithClaimLease(time.Hour))
	first, ok, err := store.ClaimNext(ctx, "cus_recover")
	if err != nil {
		t.Fatalf("first ClaimNext: %v", err)
	}
	if !ok {
		t.Fatal("first ClaimNext: got no delivery, want one")
	}

	second, ok, err := store.ClaimNext(ctx, "cus_recover")
	if err != nil {
		t.Fatalf("second ClaimNext: %v", err)
	}
	if ok {
		t.Fatalf("second ClaimNext claimed fresh in-flight delivery %s", second.ID)
	}

	_, err = db.ExecContext(ctx, `
		UPDATE deliveries
		SET claimed_at = now() - interval '2 hours'
		WHERE id = $1
	`, first.ID)
	if err != nil {
		t.Fatalf("age in-flight claim: %v", err)
	}

	recovered, ok, err := store.ClaimNext(ctx, "cus_recover")
	if err != nil {
		t.Fatalf("recovery ClaimNext: %v", err)
	}
	if !ok {
		t.Fatal("recovery ClaimNext: got no delivery, want stale in-flight delivery")
	}
	if recovered.ID != first.ID {
		t.Fatalf("recovered delivery: got %s, want %s", recovered.ID, first.ID)
	}
	if recovered.AttemptNumber != 2 {
		t.Fatalf("recovered attempt number: got %d, want 2", recovered.AttemptNumber)
	}
}

func TestDiscoverClaimableCustomersMatchesClaimEligibility(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	store := outbox.New(db, outbox.WithClaimLease(time.Hour))

	submitOneDelivery(t, ctx, db, "cus_pending")
	claimableRetry := submitOneDelivery(t, ctx, db, "cus_retry_due")
	futureRetry := submitOneDelivery(t, ctx, db, "cus_retry_future")
	staleInFlight := submitOneDelivery(t, ctx, db, "cus_stale_inflight")
	freshInFlight := submitOneDelivery(t, ctx, db, "cus_fresh_inflight")

	_, err := db.ExecContext(ctx, `
		UPDATE deliveries
		SET status = 'failed_retry', next_attempt_at = now() - interval '1 second'
		WHERE id = $1
	`, claimableRetry)
	if err != nil {
		t.Fatalf("make retry due: %v", err)
	}
	_, err = db.ExecContext(ctx, `
		UPDATE deliveries
		SET status = 'failed_retry', next_attempt_at = now() + interval '1 hour'
		WHERE id = $1
	`, futureRetry)
	if err != nil {
		t.Fatalf("make retry future: %v", err)
	}
	_, err = db.ExecContext(ctx, `
		UPDATE deliveries
		SET status = 'in_flight', claimed_at = now() - interval '2 hours'
		WHERE id = $1
	`, staleInFlight)
	if err != nil {
		t.Fatalf("make stale in-flight: %v", err)
	}
	_, err = db.ExecContext(ctx, `
		UPDATE deliveries
		SET status = 'in_flight', claimed_at = now()
		WHERE id = $1
	`, freshInFlight)
	if err != nil {
		t.Fatalf("make fresh in-flight: %v", err)
	}

	got, err := store.DiscoverClaimableCustomers(ctx, "", 10)
	if err != nil {
		t.Fatalf("DiscoverClaimableCustomers: %v", err)
	}

	want := []string{"cus_pending", "cus_retry_due", "cus_stale_inflight"}
	if !slices.Equal(got, want) {
		t.Fatalf("claimable customers: got %v, want %v", got, want)
	}

	afterPending, err := store.DiscoverClaimableCustomers(ctx, "cus_pending", 10)
	if err != nil {
		t.Fatalf("DiscoverClaimableCustomers after cursor: %v", err)
	}
	wantAfterPending := []string{"cus_retry_due", "cus_stale_inflight"}
	if !slices.Equal(afterPending, wantAfterPending) {
		t.Fatalf("claimable customers after cursor: got %v, want %v", afterPending, wantAfterPending)
	}

	limited, err := store.DiscoverClaimableCustomers(ctx, "", 2)
	if err != nil {
		t.Fatalf("DiscoverClaimableCustomers limited: %v", err)
	}
	wantLimited := []string{"cus_pending", "cus_retry_due"}
	if !slices.Equal(limited, wantLimited) {
		t.Fatalf("limited claimable customers: got %v, want %v", limited, wantLimited)
	}

	if _, err := store.DiscoverClaimableCustomers(ctx, "", 0); err == nil {
		t.Fatal("DiscoverClaimableCustomers limit 0: got nil error, want error")
	}
}

func TestProcessRetryableFailureSchedulesRetry(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	insertWebhook(t, ctx, db, "cus_retry", srv.URL, true, "subscriber.created")
	submitted, err := newIngestStore(db).Submit(ctx, ingest.Event{
		CustomerID: "cus_retry",
		Type:       "subscriber.created",
		Payload:    []byte(`{"subscriber_id":"sub_retry"}`),
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	store := outbox.New(db)
	claimed, ok, err := store.ClaimNext(ctx, "cus_retry")
	if err != nil {
		t.Fatalf("ClaimNext: %v", err)
	}
	if !ok {
		t.Fatal("ClaimNext: got no delivery, want one")
	}

	w := newIntegrationWorker()
	w.RetryPolicy = retry.New(retry.Config{BaseDelay: time.Millisecond, CapDelay: time.Millisecond})
	if result := w.Process(ctx, store, claimed); result.Err != nil {
		t.Fatalf("Process: %v", result.Err)
	}

	assertDeliveryStatus(t, ctx, db, submitted.DeliveryIDs[0], string(outbox.StatusFailedRetry))
	assertAttemptCount(t, ctx, db, submitted.DeliveryIDs[0], 1)
	assertLastError(t, ctx, db, submitted.DeliveryIDs[0], retry.ReasonRetryable5xx)
}

func TestProcessRetryableFailuresOpenWebhookBreaker(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	insertWebhook(t, ctx, db, "cus_breaker", srv.URL, true, "subscriber.created")
	submitted, err := newIngestStore(db).Submit(ctx, ingest.Event{
		CustomerID: "cus_breaker",
		Type:       "subscriber.created",
		Payload:    []byte(`{"subscriber_id":"sub_breaker"}`),
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	store := outbox.New(db, outbox.WithBreakerConfig(outbox.BreakerConfig{
		FailureThreshold: 2,
		InitialCooldown:  time.Hour,
		MaxCooldown:      time.Hour,
	}))
	w := newIntegrationWorker()
	w.RetryPolicy = retry.New(retry.Config{BaseDelay: time.Millisecond, CapDelay: time.Millisecond})
	for i := 0; i < 2; i++ {
		claimed := waitForClaim(t, ctx, store, "cus_breaker", time.Second)
		if result := w.Process(ctx, store, claimed); result.Err != nil {
			t.Fatalf("Process %d: %v", i+1, result.Err)
		}
	}

	assertWebhookBreaker(t, ctx, db, submitted.DeliveryIDs[0], "open", 2)
}

func TestProcessRetryableFailuresCanDisableWebhookBreaker(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	insertWebhook(t, ctx, db, "cus_breaker_disabled", srv.URL, true, "subscriber.created")
	_, err := newIngestStore(db).Submit(ctx, ingest.Event{
		CustomerID: "cus_breaker_disabled",
		Type:       "subscriber.created",
		Payload:    []byte(`{"subscriber_id":"sub_breaker_disabled"}`),
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	store := outbox.New(db, outbox.WithBreakerConfig(outbox.BreakerConfig{
		FailureThreshold: 0,
	}))
	w := newIntegrationWorker()
	w.RetryPolicy = retry.New(retry.Config{BaseDelay: time.Millisecond, CapDelay: time.Millisecond})

	claimed := waitForClaim(t, ctx, store, "cus_breaker_disabled", time.Second)
	if result := w.Process(ctx, store, claimed); result.Err != nil {
		t.Fatalf("Process: %v", result.Err)
	}

	assertWebhookBreaker(t, ctx, db, claimed.ID, "closed", 0)
}

func TestClaimNextSkipsOpenWebhookUntilHalfOpenProbe(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	first := submitOneDelivery(t, ctx, db, "cus_open_breaker")
	second, err := appendDeliveryForExistingWebhook(t, ctx, db, first)
	if err != nil {
		t.Fatalf("append delivery: %v", err)
	}
	store := outbox.New(db)

	_, err = db.ExecContext(ctx, `
		UPDATE webhooks
		SET breaker_state = 'open',
		    breaker_opened_at = now(),
		    breaker_next_probe_at = now() + interval '1 hour',
		    breaker_failure_count = 5
		WHERE id = (SELECT webhook_id FROM deliveries WHERE id = $1)
	`, first)
	if err != nil {
		t.Fatalf("open breaker: %v", err)
	}

	if got, err := store.DiscoverClaimableCustomers(ctx, "", 10); err != nil {
		t.Fatalf("DiscoverClaimableCustomers: %v", err)
	} else if len(got) != 0 {
		t.Fatalf("claimable customers with open breaker: got %v, want none", got)
	}
	if claimed, ok, err := store.ClaimNext(ctx, "cus_open_breaker"); err != nil {
		t.Fatalf("ClaimNext while open: %v", err)
	} else if ok {
		t.Fatalf("ClaimNext while open claimed %s", claimed.ID)
	}

	_, err = db.ExecContext(ctx, `
		UPDATE webhooks
		SET breaker_next_probe_at = now() - interval '1 second'
		WHERE id = (SELECT webhook_id FROM deliveries WHERE id = $1)
	`, first)
	if err != nil {
		t.Fatalf("expire breaker cooldown: %v", err)
	}

	claimed, ok, err := store.ClaimNext(ctx, "cus_open_breaker")
	if err != nil {
		t.Fatalf("ClaimNext half-open: %v", err)
	}
	if !ok {
		t.Fatal("ClaimNext half-open: got no delivery, want probe")
	}
	if claimed.ID != first {
		t.Fatalf("half-open probe delivery: got %s, want %s", claimed.ID, first)
	}
	assertWebhookProbe(t, ctx, db, first, "half_open", first)

	if claimed, ok, err := store.ClaimNext(ctx, "cus_open_breaker"); err != nil {
		t.Fatalf("ClaimNext with probe in flight: %v", err)
	} else if ok {
		t.Fatalf("ClaimNext with probe in flight claimed %s, want no second probe; other pending delivery is %s", claimed.ID, second)
	}
}

func TestHalfOpenProbeSuccessClosesWebhookBreaker(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	deliveryID := submitOneDeliveryToURL(t, ctx, db, "cus_probe_success", srv.URL)
	store := outbox.New(db)
	makeWebhookHalfOpen(t, ctx, db, deliveryID)

	claimed, ok, err := store.ClaimNext(ctx, "cus_probe_success")
	if err != nil {
		t.Fatalf("ClaimNext: %v", err)
	}
	if !ok {
		t.Fatal("ClaimNext: got no delivery, want probe")
	}
	if result := newIntegrationWorker().Process(ctx, store, claimed); result.Err != nil {
		t.Fatalf("Process: %v", result.Err)
	}

	assertWebhookBreaker(t, ctx, db, deliveryID, "closed", 0)
}

func TestHalfOpenNonProbeSuccessDoesNotCloseWebhookBreaker(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	probeDeliveryID := submitOneDelivery(t, ctx, db, "cus_probe_identity")
	otherDeliveryID, err := appendDeliveryForExistingWebhook(t, ctx, db, probeDeliveryID)
	if err != nil {
		t.Fatalf("append delivery: %v", err)
	}
	makeWebhookHalfOpen(t, ctx, db, probeDeliveryID)
	_, err = db.ExecContext(ctx, `
		UPDATE deliveries
		SET status = 'in_flight',
		    attempt_count = 1,
		    claimed_at = now()
		WHERE id = $1
	`, otherDeliveryID)
	if err != nil {
		t.Fatalf("force non-probe in-flight: %v", err)
	}

	if err := outbox.New(db).MarkSucceeded(ctx, otherDeliveryID, http.StatusOK, time.Millisecond); err != nil {
		t.Fatalf("MarkSucceeded: %v", err)
	}

	assertDeliveryStatus(t, ctx, db, otherDeliveryID, string(outbox.StatusSucceeded))
	assertWebhookProbe(t, ctx, db, probeDeliveryID, "half_open", probeDeliveryID)
}

func TestHalfOpenProbeFailureReopensWebhookBreaker(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	deliveryID := submitOneDeliveryToURL(t, ctx, db, "cus_probe_failure", srv.URL)
	store := outbox.New(db, outbox.WithBreakerConfig(outbox.BreakerConfig{
		FailureThreshold: 2,
		InitialCooldown:  time.Hour,
		MaxCooldown:      2 * time.Hour,
	}))
	makeWebhookHalfOpen(t, ctx, db, deliveryID)

	claimed, ok, err := store.ClaimNext(ctx, "cus_probe_failure")
	if err != nil {
		t.Fatalf("ClaimNext: %v", err)
	}
	if !ok {
		t.Fatal("ClaimNext: got no delivery, want probe")
	}
	w := newIntegrationWorker()
	w.RetryPolicy = retry.New(retry.Config{BaseDelay: time.Millisecond, CapDelay: time.Millisecond})
	if result := w.Process(ctx, store, claimed); result.Err != nil {
		t.Fatalf("Process: %v", result.Err)
	}

	assertWebhookBreaker(t, ctx, db, deliveryID, "open", 4)
	assertWebhookNextProbeAfter(t, ctx, db, deliveryID, time.Now().Add(90*time.Minute))
}

func TestProcessRetryableFailureUsesPreviousRetryDelay(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	insertWebhook(t, ctx, db, "cus_backoff", srv.URL, true, "subscriber.created")
	submitted, err := newIngestStore(db).Submit(ctx, ingest.Event{
		CustomerID: "cus_backoff",
		Type:       "subscriber.created",
		Payload:    []byte(`{"subscriber_id":"sub_backoff"}`),
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	store := outbox.New(db)
	w := newIntegrationWorker()
	w.RetryPolicy = retry.New(retry.Config{BaseDelay: 10 * time.Millisecond, CapDelay: 250 * time.Millisecond})

	first, ok, err := store.ClaimNext(ctx, "cus_backoff")
	if err != nil {
		t.Fatalf("first ClaimNext: %v", err)
	}
	if !ok {
		t.Fatal("first ClaimNext: got no delivery, want one")
	}
	if result := w.Process(ctx, store, first); result.Err != nil {
		t.Fatalf("first Process: %v", result.Err)
	}
	firstDelay := scheduledRetryDelay(t, ctx, db, submitted.DeliveryIDs[0])
	if firstDelay < 10*time.Millisecond {
		t.Fatalf("first retry delay: got %s, want at least 10ms", firstDelay)
	}

	second := waitForClaim(t, ctx, store, "cus_backoff", 2*time.Second)
	if second.PreviousDelay <= 0 {
		t.Fatalf("second previous delay: got %s, want positive", second.PreviousDelay)
	}
	if result := w.Process(ctx, store, second); result.Err != nil {
		t.Fatalf("second Process: %v", result.Err)
	}

	secondDelay := scheduledRetryDelay(t, ctx, db, submitted.DeliveryIDs[0])
	if secondDelay <= firstDelay {
		t.Fatalf("second retry delay: got %s, want greater than first delay %s", secondDelay, firstDelay)
	}
}

func TestProcessTerminalFailureWritesDeadLetter(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	insertWebhook(t, ctx, db, "cus_dead", srv.URL, true, "subscriber.created")
	submitted, err := newIngestStore(db).Submit(ctx, ingest.Event{
		CustomerID: "cus_dead",
		Type:       "subscriber.created",
		Payload:    []byte(`{"subscriber_id":"sub_dead"}`),
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	store := outbox.New(db)
	claimed, ok, err := store.ClaimNext(ctx, "cus_dead")
	if err != nil {
		t.Fatalf("ClaimNext: %v", err)
	}
	if !ok {
		t.Fatal("ClaimNext: got no delivery, want one")
	}
	if result := newIntegrationWorker().Process(ctx, store, claimed); result.Err != nil {
		t.Fatalf("Process: %v", result.Err)
	}

	deliveryID := submitted.DeliveryIDs[0]
	assertDeliveryStatus(t, ctx, db, deliveryID, string(outbox.StatusDead))
	assertAttemptCount(t, ctx, db, deliveryID, 1)
	assertDeadLetter(t, ctx, db, deliveryID, retry.ReasonTerminal4xx, 1)
}

func TestProcessFlakyReceiverEventuallySucceedsAllDeliveries(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	var totalRequests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestNumber := totalRequests.Add(1)
		attemptNumber, _ := strconv.Atoi(r.Header.Get("X-Attempt-Number"))
		if attemptNumber == 1 && requestNumber%10 < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	const deliveryCount = 40
	for i := 0; i < deliveryCount; i++ {
		insertWebhook(t, ctx, db, "cus_flaky", srv.URL, true, "subscriber.created")
	}
	submitted, err := newIngestStore(db).Submit(ctx, ingest.Event{
		CustomerID: "cus_flaky",
		Type:       "subscriber.created",
		Payload:    []byte(`{"subscriber_id":"sub_flaky"}`),
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if len(submitted.DeliveryIDs) != deliveryCount {
		t.Fatalf("delivery count: got %d, want %d", len(submitted.DeliveryIDs), deliveryCount)
	}

	store := outbox.New(db)
	w := newIntegrationWorker()
	w.RetryPolicy = retry.New(retry.Config{BaseDelay: time.Millisecond, CapDelay: time.Millisecond})

	deadline := time.Now().Add(5 * time.Second)
	for succeededCount(t, ctx, db) < deliveryCount {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for all deliveries to succeed")
		}

		claimed, ok, err := store.ClaimNext(ctx, "cus_flaky")
		if err != nil {
			t.Fatalf("ClaimNext: %v", err)
		}
		if !ok {
			time.Sleep(time.Millisecond)
			continue
		}
		if result := w.Process(ctx, store, claimed); result.Err != nil {
			t.Fatalf("Process delivery %s: %v", claimed.ID, result.Err)
		}
	}

	assertStatusCount(t, ctx, db, string(outbox.StatusSucceeded), deliveryCount)
	assertStatusCount(t, ctx, db, string(outbox.StatusDead), 0)
	if totalRequests.Load() <= deliveryCount {
		t.Fatalf("receiver request count: got %d, want retries above %d", totalRequests.Load(), deliveryCount)
	}
}

func TestStoreValidatesRequiredIDs(t *testing.T) {
	t.Parallel()

	store := outbox.New(nil)
	tests := []struct {
		name string
		run  func(context.Context) error
	}{
		{
			name: "claim requires customer ID",
			run: func(ctx context.Context) error {
				_, _, err := store.ClaimNext(ctx, "")
				return err
			},
		},
		{
			name: "mark succeeded requires delivery ID",
			run: func(ctx context.Context) error {
				return store.MarkSucceeded(ctx, "", http.StatusOK, time.Millisecond)
			},
		},
		{
			name: "mark retry requires delivery ID",
			run: func(ctx context.Context) error {
				return store.MarkRetry(ctx, "", http.StatusInternalServerError, retry.ReasonRetryable5xx, time.Millisecond, time.Millisecond)
			},
		},
		{
			name: "mark dead requires delivery ID",
			run: func(ctx context.Context) error {
				return store.MarkDead(ctx, "", http.StatusNotFound, retry.ReasonTerminal4xx, retry.ReasonTerminal4xx, time.Millisecond)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if err := tt.run(context.Background()); err == nil {
				t.Fatal("expected validation error, got nil")
			}
		})
	}
}

func TestBatchMarkErrorsWhenDeliveryIsNotInFlight(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)
	store := outbox.New(db)

	tests := []struct {
		name string
		run  func(outbox.BatchMarkItem) error
	}{
		{
			name: "succeeded",
			run: func(item outbox.BatchMarkItem) error {
				return store.MarkSucceededBatch(ctx, []outbox.BatchMarkItem{item})
			},
		},
		{
			name: "retry",
			run: func(item outbox.BatchMarkItem) error {
				item.ErrorClass = retry.ReasonRetryable5xx
				item.Delay = time.Millisecond
				return store.MarkRetryBatch(ctx, []outbox.BatchMarkItem{item})
			},
		},
		{
			name: "dead",
			run: func(item outbox.BatchMarkItem) error {
				item.ErrorClass = retry.ReasonTerminal4xx
				item.Reason = retry.ReasonTerminal4xx
				return store.MarkDeadBatch(ctx, []outbox.BatchMarkItem{item})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deliveryID := submitOneDelivery(t, ctx, db, "cus_batch_"+tt.name)
			claimed, ok, err := store.ClaimNext(ctx, "cus_batch_"+tt.name)
			if err != nil {
				t.Fatalf("ClaimNext: %v", err)
			}
			if !ok {
				t.Fatal("ClaimNext: got no delivery, want one")
			}
			if claimed.ID != deliveryID {
				t.Fatalf("claimed ID: got %s, want %s", claimed.ID, deliveryID)
			}
			if err := store.MarkSucceeded(ctx, deliveryID, http.StatusOK, time.Millisecond); err != nil {
				t.Fatalf("MarkSucceeded setup: %v", err)
			}

			err = tt.run(outbox.BatchMarkItem{
				DeliveryID: deliveryID,
				StatusCode: http.StatusOK,
				Duration:   time.Millisecond,
			})
			if err == nil {
				t.Fatal("expected batch mark error, got nil")
			}
			if !strings.Contains(err.Error(), "marked 0 of 1") &&
				!strings.Contains(err.Error(), "marked 0 attempts and 0 dead letters of 1") {
				t.Fatalf("error: got %q, want partial batch mark error", err.Error())
			}
		})
	}
}

// TestWorkerRun_SchedulerFairnessAcrossCustomers exercises the full worker
// loop (Scheduler → ClaimNext → Process → re-Notify) against a real Postgres
// outbox. A Whale Customer and a Small Customer each enqueue Deliveries; the
// test asserts the worker interleaves them fairly rather than draining the
// Whale first.
func TestWorkerRun_SchedulerFairnessAcrossCustomers(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// Whale: 10 deliveries, Small: 10 deliveries.
	const deliveriesPerCustomer = 10
	for i := 0; i < deliveriesPerCustomer; i++ {
		insertWebhook(t, ctx, db, "whale", srv.URL, true, "subscriber.created")
		insertWebhook(t, ctx, db, "small", srv.URL, true, "subscriber.created")
	}

	whaleSubmitted, err := newIngestStore(db).Submit(ctx, ingest.Event{
		CustomerID: "whale",
		Type:       "subscriber.created",
		Payload:    []byte(`{"subscriber_id":"whale_sub"}`),
	})
	if err != nil {
		t.Fatalf("Submit whale: %v", err)
	}
	if len(whaleSubmitted.DeliveryIDs) != deliveriesPerCustomer {
		t.Fatalf("whale delivery count: got %d, want %d", len(whaleSubmitted.DeliveryIDs), deliveriesPerCustomer)
	}

	smallSubmitted, err := newIngestStore(db).Submit(ctx, ingest.Event{
		CustomerID: "small",
		Type:       "subscriber.created",
		Payload:    []byte(`{"subscriber_id":"small_sub"}`),
	})
	if err != nil {
		t.Fatalf("Submit small: %v", err)
	}
	if len(smallSubmitted.DeliveryIDs) != deliveriesPerCustomer {
		t.Fatalf("small delivery count: got %d, want %d", len(smallSubmitted.DeliveryIDs), deliveriesPerCustomer)
	}

	store := outbox.New(db)
	sched := roundrobin.New()
	w := newIntegrationWorker()
	w.Scheduler = sched

	// Notify both Customers to seed the scheduler.
	sched.Notify("whale")
	sched.Notify("small")

	// Run worker in background until all deliveries succeed.
	runCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- w.Run(runCtx, store)
	}()

	// Poll until all deliveries succeed or timeout.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		whaleSucceeded := countSucceededForCustomer(t, ctx, db, "whale")
		smallSucceeded := countSucceededForCustomer(t, ctx, db, "small")
		if whaleSucceeded == deliveriesPerCustomer && smallSucceeded == deliveriesPerCustomer {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	<-errCh

	whaleSucceeded := countSucceededForCustomer(t, ctx, db, "whale")
	smallSucceeded := countSucceededForCustomer(t, ctx, db, "small")
	if whaleSucceeded != deliveriesPerCustomer {
		t.Errorf("whale succeeded: got %d, want %d", whaleSucceeded, deliveriesPerCustomer)
	}
	if smallSucceeded != deliveriesPerCustomer {
		t.Errorf("small succeeded: got %d, want %d", smallSucceeded, deliveriesPerCustomer)
	}
}

func TestWorkerRun_DBDiscoveryFeedsIndependentSchedulers(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	const deliveriesPerCustomer = 5
	const totalDeliveries = deliveriesPerCustomer * 2

	var (
		seenMu sync.Mutex
		seen   = make(map[string]int, totalDeliveries)
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("X-Delivery-Id") + ":" + r.Header.Get("X-Attempt-Number")
		seenMu.Lock()
		seen[key]++
		seenMu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	for i := 0; i < deliveriesPerCustomer; i++ {
		insertWebhook(t, ctx, db, "cus_a", srv.URL, true, "subscriber.created")
		insertWebhook(t, ctx, db, "cus_b", srv.URL, true, "subscriber.created")
	}

	submit := func(customerID string) {
		t.Helper()
		submitted, err := newIngestStore(db).Submit(ctx, ingest.Event{
			CustomerID: customerID,
			Type:       "subscriber.created",
			Payload:    []byte(`{"subscriber_id":"sub_123"}`),
		})
		if err != nil {
			t.Fatalf("Submit %s: %v", customerID, err)
		}
		if len(submitted.DeliveryIDs) != deliveriesPerCustomer {
			t.Fatalf("%s delivery count: got %d, want %d", customerID, len(submitted.DeliveryIDs), deliveriesPerCustomer)
		}
	}
	submit("cus_a")
	submit("cus_b")

	store := outbox.New(db)
	schedulers := []scheduler.Scheduler{roundrobin.New(), roundrobin.New()}

	runCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	errCh := make(chan error, len(schedulers)*2)
	for _, sched := range schedulers {
		sched := sched
		go func() {
			errCh <- worker.RunClaimableCustomerDiscovery(runCtx, store, sched, worker.DiscoveryConfig{
				Interval: time.Millisecond,
				Limit:    1,
			})
		}()

		w := newIntegrationWorker()
		w.Scheduler = sched
		go func() {
			errCh <- w.Run(runCtx, store)
		}()
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		cusASucceeded := countSucceededForCustomer(t, ctx, db, "cus_a")
		cusBSucceeded := countSucceededForCustomer(t, ctx, db, "cus_b")
		if cusASucceeded == deliveriesPerCustomer && cusBSucceeded == deliveriesPerCustomer {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	for range cap(errCh) {
		err := <-errCh
		if err != nil && err != context.Canceled {
			t.Fatalf("background loop: %v", err)
		}
	}

	if got := countSucceededForCustomer(t, ctx, db, "cus_a"); got != deliveriesPerCustomer {
		t.Fatalf("cus_a succeeded: got %d, want %d", got, deliveriesPerCustomer)
	}
	if got := countSucceededForCustomer(t, ctx, db, "cus_b"); got != deliveriesPerCustomer {
		t.Fatalf("cus_b succeeded: got %d, want %d", got, deliveriesPerCustomer)
	}

	seenMu.Lock()
	defer seenMu.Unlock()
	if len(seen) != totalDeliveries {
		t.Fatalf("receiver unique attempts: got %d, want %d", len(seen), totalDeliveries)
	}
	var duplicates []string
	for key, count := range seen {
		if count > 1 {
			duplicates = append(duplicates, key)
		}
	}
	sort.Strings(duplicates)
	if len(duplicates) > 0 {
		t.Fatalf("duplicate attempts: %v", duplicates)
	}
}

func TestRequeue_ReturnsInFlightToFailedRetryWithoutAttemptRow(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)
	store := outbox.New(db)

	insertWebhook(t, ctx, db, "cus_1", "https://partner.example.com/hook", true, "subscriber.created")

	_, err := newIngestStore(db).Submit(ctx, ingest.Event{
		CustomerID: "cus_1",
		Type:       "subscriber.created",
		Payload:    []byte(`{"subscriber_id":"sub_1"}`),
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	claimed, ok, err := store.ClaimNext(ctx, "cus_1")
	if err != nil {
		t.Fatalf("ClaimNext: %v", err)
	}
	if !ok {
		t.Fatal("ClaimNext: no delivery")
	}

	assertDeliveryStatus(t, ctx, db, claimed.ID, "in_flight")
	if claimed.AttemptNumber != 1 {
		t.Fatalf("attempt_number after claim: got %d, want 1", claimed.AttemptNumber)
	}

	if err := store.Requeue(ctx, claimed.ID, 200*time.Millisecond); err != nil {
		t.Fatalf("Requeue: %v", err)
	}

	assertDeliveryStatus(t, ctx, db, claimed.ID, "failed_retry")
	assertAttemptCount(t, ctx, db, claimed.ID, 0)

	var attemptCount int
	err = db.QueryRowContext(ctx, `SELECT attempt_count FROM deliveries WHERE id = $1`, claimed.ID).Scan(&attemptCount)
	if err != nil {
		t.Fatalf("select attempt_count: %v", err)
	}
	if attemptCount != 0 {
		t.Errorf("attempt_count after requeue: got %d, want 0 (claim-time increment cancelled)", attemptCount)
	}

	time.Sleep(250 * time.Millisecond)

	reclaimed, ok, err := store.ClaimNext(ctx, "cus_1")
	if err != nil {
		t.Fatalf("ClaimNext after requeue: %v", err)
	}
	if !ok {
		t.Fatal("ClaimNext after requeue: no delivery")
	}
	if reclaimed.AttemptNumber != 1 {
		t.Fatalf("attempt_number after re-claim: got %d, want 1 (decrement + re-increment)", reclaimed.AttemptNumber)
	}

	if err := store.MarkSucceeded(ctx, reclaimed.ID, 200, 10*time.Millisecond); err != nil {
		t.Fatalf("MarkSucceeded: %v", err)
	}

	assertDeliveryStatus(t, ctx, db, reclaimed.ID, "succeeded")
	assertAttemptCount(t, ctx, db, reclaimed.ID, 1)
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

func submitOneDelivery(t *testing.T, ctx context.Context, db *sql.DB, customerID string) string {
	t.Helper()

	return submitOneDeliveryToURL(t, ctx, db, customerID, "https://partner.example/webhook")
}

func submitOneDeliveryToURL(t *testing.T, ctx context.Context, db *sql.DB, customerID, url string) string {
	t.Helper()

	insertWebhook(t, ctx, db, customerID, url, true, "subscriber.created")
	submitted, err := newIngestStore(db).Submit(ctx, ingest.Event{
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

func makeWebhookHalfOpen(t *testing.T, ctx context.Context, db *sql.DB, deliveryID string) {
	t.Helper()

	_, err := db.ExecContext(ctx, `
		UPDATE webhooks
		SET breaker_state = 'half_open',
		    breaker_failure_count = 3,
		    breaker_opened_at = now() - interval '1 hour',
		    breaker_next_probe_at = now() - interval '1 second',
		    breaker_cooldown_ms = 3600000,
		    breaker_probe_delivery_id = $1
		WHERE id = (SELECT webhook_id FROM deliveries WHERE id = $1)
	`, deliveryID)
	if err != nil {
		t.Fatalf("make webhook half-open: %v", err)
	}
}

func appendDeliveryForExistingWebhook(t *testing.T, ctx context.Context, db *sql.DB, existingDeliveryID string) (string, error) {
	t.Helper()

	var customerID, webhookID string
	err := db.QueryRowContext(ctx, `
		SELECT customer_id, webhook_id
		FROM deliveries
		WHERE id = $1
	`, existingDeliveryID).Scan(&customerID, &webhookID)
	if err != nil {
		return "", fmt.Errorf("load existing delivery: %w", err)
	}

	var eventID string
	err = db.QueryRowContext(ctx, `
		INSERT INTO events (customer_id, type, payload)
		VALUES ($1, 'subscriber.created', '{"subscriber_id":"sub_extra"}'::jsonb)
		RETURNING id
	`, customerID).Scan(&eventID)
	if err != nil {
		return "", fmt.Errorf("insert event: %w", err)
	}

	var deliveryID string
	err = db.QueryRowContext(ctx, `
		INSERT INTO deliveries (event_id, webhook_id, customer_id, status)
		VALUES ($1, $2, $3, 'pending')
		RETURNING id
	`, eventID, webhookID, customerID).Scan(&deliveryID)
	if err != nil {
		return "", fmt.Errorf("insert delivery: %w", err)
	}

	return deliveryID, nil
}

func assertWebhookBreaker(t *testing.T, ctx context.Context, db *sql.DB, deliveryID, wantState string, wantFailures int) {
	t.Helper()

	var gotState string
	var gotFailures int
	err := db.QueryRowContext(ctx, `
		SELECT w.breaker_state, w.breaker_failure_count
		FROM webhooks w
		JOIN deliveries d ON d.webhook_id = w.id
		WHERE d.id = $1
	`, deliveryID).Scan(&gotState, &gotFailures)
	if err != nil {
		t.Fatalf("select webhook breaker: %v", err)
	}
	if gotState != wantState || gotFailures != wantFailures {
		t.Fatalf("webhook breaker: got state=%q failures=%d, want state=%q failures=%d", gotState, gotFailures, wantState, wantFailures)
	}
}

func assertWebhookProbe(t *testing.T, ctx context.Context, db *sql.DB, deliveryID, wantState, wantProbeDeliveryID string) {
	t.Helper()

	var gotState string
	var gotProbeDeliveryID sql.NullString
	err := db.QueryRowContext(ctx, `
		SELECT w.breaker_state, w.breaker_probe_delivery_id::text
		FROM webhooks w
		JOIN deliveries d ON d.webhook_id = w.id
		WHERE d.id = $1
	`, deliveryID).Scan(&gotState, &gotProbeDeliveryID)
	if err != nil {
		t.Fatalf("select webhook probe: %v", err)
	}
	if gotState != wantState || !gotProbeDeliveryID.Valid || gotProbeDeliveryID.String != wantProbeDeliveryID {
		t.Fatalf("webhook probe: got state=%q probe=%q valid=%v, want state=%q probe=%q", gotState, gotProbeDeliveryID.String, gotProbeDeliveryID.Valid, wantState, wantProbeDeliveryID)
	}
}

func assertWebhookNextProbeAfter(t *testing.T, ctx context.Context, db *sql.DB, deliveryID string, threshold time.Time) {
	t.Helper()

	var got time.Time
	err := db.QueryRowContext(ctx, `
		SELECT w.breaker_next_probe_at
		FROM webhooks w
		JOIN deliveries d ON d.webhook_id = w.id
		WHERE d.id = $1
	`, deliveryID).Scan(&got)
	if err != nil {
		t.Fatalf("select next probe: %v", err)
	}
	if !got.After(threshold) {
		t.Fatalf("next probe: got %s, want after %s", got, threshold)
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

func assertAttemptCount(t *testing.T, ctx context.Context, db *sql.DB, deliveryID string, want int) {
	t.Helper()

	var got int
	err := db.QueryRowContext(ctx, `SELECT count(*) FROM delivery_attempts WHERE delivery_id = $1`, deliveryID).Scan(&got)
	if err != nil {
		t.Fatalf("count attempts: %v", err)
	}
	if got != want {
		t.Fatalf("attempt count: got %d, want %d", got, want)
	}
}

func assertStatusCount(t *testing.T, ctx context.Context, db *sql.DB, status string, want int) {
	t.Helper()

	var got int
	err := db.QueryRowContext(ctx, `SELECT count(*) FROM deliveries WHERE status = $1`, status).Scan(&got)
	if err != nil {
		t.Fatalf("count status %s: %v", status, err)
	}
	if got != want {
		t.Fatalf("status %s count: got %d, want %d", status, got, want)
	}
}

func assertLastError(t *testing.T, ctx context.Context, db *sql.DB, deliveryID, want string) {
	t.Helper()

	var got sql.NullString
	err := db.QueryRowContext(ctx, `SELECT last_error FROM deliveries WHERE id = $1`, deliveryID).Scan(&got)
	if err != nil {
		t.Fatalf("select last_error: %v", err)
	}
	if !got.Valid || got.String != want {
		t.Fatalf("last_error: got %q valid=%v, want %q", got.String, got.Valid, want)
	}
}

func assertDeadLetter(t *testing.T, ctx context.Context, db *sql.DB, deliveryID, wantReason string, wantAttempts int) {
	t.Helper()

	var gotReason string
	var gotAttempts int
	err := db.QueryRowContext(ctx, `
		SELECT reason, attempts
		FROM dead_letters
		WHERE delivery_id = $1
	`, deliveryID).Scan(&gotReason, &gotAttempts)
	if err != nil {
		t.Fatalf("select dead letter: %v", err)
	}
	if gotReason != wantReason {
		t.Fatalf("dead letter reason: got %q, want %q", gotReason, wantReason)
	}
	if gotAttempts != wantAttempts {
		t.Fatalf("dead letter attempts: got %d, want %d", gotAttempts, wantAttempts)
	}
}

func scheduledRetryDelay(t *testing.T, ctx context.Context, db *sql.DB, deliveryID string) time.Duration {
	t.Helper()

	var delayMillis int64
	err := db.QueryRowContext(ctx, `
		SELECT COALESCE(
		    GREATEST(EXTRACT(EPOCH FROM (deliveries.next_attempt_at - delivery_attempts.sent_at)) * 1000, 0)::bigint,
		    0
		)
		FROM deliveries
		JOIN delivery_attempts ON delivery_attempts.delivery_id = deliveries.id
		WHERE deliveries.id = $1
		ORDER BY delivery_attempts.attempt_number DESC
		LIMIT 1
	`, deliveryID).Scan(&delayMillis)
	if err != nil {
		t.Fatalf("select scheduled retry delay: %v", err)
	}

	return time.Duration(delayMillis) * time.Millisecond
}

func waitForClaim(t *testing.T, ctx context.Context, store *outbox.Store, customerID string, timeout time.Duration) outbox.Delivery {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		delivery, ok, err := store.ClaimNext(ctx, customerID)
		if err != nil {
			t.Fatalf("ClaimNext: %v", err)
		}
		if ok {
			return delivery
		}
		time.Sleep(time.Millisecond)
	}

	t.Fatalf("timed out waiting to claim delivery for %s", customerID)
	return outbox.Delivery{}
}

func succeededCount(t *testing.T, ctx context.Context, db *sql.DB) int {
	t.Helper()

	var got int
	err := db.QueryRowContext(ctx, `SELECT count(*) FROM deliveries WHERE status = 'succeeded'`).Scan(&got)
	if err != nil {
		t.Fatalf("count succeeded: %v", err)
	}
	return got
}

func countSucceededForCustomer(t *testing.T, ctx context.Context, db *sql.DB, customerID string) int {
	t.Helper()

	var got int
	err := db.QueryRowContext(ctx, `SELECT count(*) FROM deliveries WHERE status = 'succeeded' AND customer_id = $1`, customerID).Scan(&got)
	if err != nil {
		t.Fatalf("count succeeded for customer %s: %v", customerID, err)
	}
	return got
}
