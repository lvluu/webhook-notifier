package observe

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// fakeDriver and fakeConn implement enough of the database/sql/driver contract
// so that RegisterPoolGauges tests pass without a real Postgres installation.
type fakeDriver struct {
	driver.Driver
}
type fakeConn struct {
	driver.Conn
}
type fakeRows struct {
	sent bool
}

type backlogDriver struct{}
type backlogConn struct{}
type backlogRows struct {
	index int
	rows  [][]driver.Value
}

func (fakeDriver) Open(_ string) (driver.Conn, error)  { return fakeConn{}, nil }
func (fakeConn) Prepare(_ string) (driver.Stmt, error) { return nil, nil }
func (fakeConn) Close() error                          { return nil }
func (fakeConn) QueryContext(_ context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	return &fakeRows{}, nil
}
func (fakeRows) Columns() []string { return []string{"count"} }
func (fakeRows) Close() error      { return nil }
func (r *fakeRows) Next(dest []driver.Value) error {
	if r.sent {
		return io.EOF
	}
	r.sent = true
	dest[0] = int64(7)
	return nil
}

func (backlogDriver) Open(_ string) (driver.Conn, error) { return backlogConn{}, nil }
func (backlogConn) Prepare(_ string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not supported")
}
func (backlogConn) Close() error { return nil }
func (backlogConn) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions are not supported")
}
func (backlogConn) QueryContext(_ context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	return &backlogRows{rows: [][]driver.Value{
		{int64(0), int64(4), float64(5)},
		{int64(1), int64(2), float64(7)},
		{int64(2), int64(3), float64(11)},
		{int64(3), int64(6), float64(9)},
		{int64(4), int64(1), float64(3)},
	}}, nil
}
func (*backlogRows) Columns() []string { return []string{"class", "count", "oldest_age"} }
func (*backlogRows) Close() error      { return nil }
func (r *backlogRows) Next(dest []driver.Value) error {
	if r.index >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.index])
	r.index++
	return nil
}

func init() {
	sql.Register("fake", fakeDriver{})
	sql.Register("backlog-fake", backlogDriver{})
}

func TestRegisterMetrics_registers_metric_families(t *testing.T) {
	ResetMetricsForTest()
	reg := prometheus.NewRegistry()
	RegisterMetrics(reg)
	// Force Vec metrics to materialise a series so Gather includes them.
	WebhookDeliveriesTotal.WithLabelValues("succeeded", customerClassSmall).Add(0)
	WebhookDeliveryLatencySeconds.WithLabelValues("small").Observe(0)
	WebhookQueueWaitSeconds.WithLabelValues(customerClassSmall).Observe(0)
	WebhookAttemptTotal.WithLabelValues("succeeded").Add(0)
	WebhookPostDurationSeconds.WithLabelValues(customerClassSmall).Observe(0)
	WebhookDlqTotal.WithLabelValues("exhausted").Add(0)
	WebhookHostSemaphoreWaitSeconds.WithLabelValues("example.com").Observe(0)
	WebhookBreakerState.WithLabelValues("webhook_1", "closed").Set(1)
	WebhookBreakerTripsTotal.WithLabelValues("retryable_5xx").Add(0)
	WebhookPendingDeliveries.WithLabelValues(customerClassSmall).Set(0)
	WebhookPgNotifyBroadcastTotal.WithLabelValues("sent").Add(0)

	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if len(families) < 13 {
		t.Fatalf("expected at least 13 metric families, got %d", len(families))
	}
}

func TestMetricsEndpoint_returns_Prometheus_text(t *testing.T) {
	ResetMetricsForTest()
	reg := prometheus.NewRegistry()
	RegisterMetrics(reg)
	WebhookDeliveriesTotal.WithLabelValues("succeeded", customerClassSmall).Add(1)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !bytes.Contains(body, []byte("webhook_deliveries_total")) {
		t.Fatal("/metrics response missing webhook_deliveries_total")
	}
}

func TestNewLogger_outputs_JSON(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	logger.Info("test message", "key", "value")

	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("expected JSON output: %s", buf.String())
	}
	if m["msg"] != "test message" {
		t.Errorf("expected msg='test message', got %v", m["msg"])
	}
}

func TestRegisterPoolGauges_registers_six_families(t *testing.T) {
	ResetMetricsForTest()
	reg := prometheus.NewRegistry()

	db, err := sql.Open("fake", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	RegisterMetrics(reg)
	RegisterPoolGauges(reg, db)

	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var poolCount int
	for _, f := range families {
		if strings.HasPrefix(f.GetName(), "webhook_pg_pool") {
			poolCount++
		}
	}
	if poolCount < 6 {
		t.Fatalf("expected at least 6 webhook_pg_pool_* families, got %d", poolCount)
	}
}

func TestMetricsEndpoint_returns_nonzero_after_seed(t *testing.T) {
	ResetMetricsForTest()
	reg := prometheus.NewRegistry()
	RegisterMetrics(reg)

	WebhookDeliveriesTotal.WithLabelValues("succeeded", customerClassSmall).Add(3)
	WebhookDeliveryLatencySeconds.WithLabelValues("small").Observe(0.5)
	WebhookQueueWaitSeconds.WithLabelValues(customerClassSmall).Observe(0.5)
	WebhookAttemptTotal.WithLabelValues("succeeded").Add(3)
	WebhookPostDurationSeconds.WithLabelValues(customerClassSmall).Observe(0.1)
	WebhookDeliveriesInFlight.Inc()
	WebhookDeliveriesInFlight.Dec()
	WebhookAttemptsPerDelivery.Observe(2)
	WebhookDlqTotal.WithLabelValues("exhausted").Add(1)
	WebhookSchedulerPickDurationSeconds.Observe(0.001)
	WebhookHostSemaphoreWaitSeconds.WithLabelValues("example.com").Observe(0.01)
	WebhookClaimDurationSeconds.Observe(0.1)
	WebhookClaimBatchSize.Observe(5)
	WebhookClaimEmptyTotal.Add(2)
	WebhookPendingDeliveries.WithLabelValues(customerClassSmall).Set(7)
	WebhookPendingOldestAgeSeconds.Set(12)
	WebhookActiveCustomers.Set(4)
	WebhookIngestFanoutPerEvent.Observe(3)
	SetBreakerState("webhook_1", "open")
	IncBreakerTrip("retryable_5xx")

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)

	requireContains := func(sub string) {
		t.Helper()
		if !bytes.Contains(body, []byte(sub)) {
			t.Fatalf("/metrics response missing %q", sub)
		}
	}
	requireContains(`webhook_deliveries_total{customer_class="small",status="succeeded"} 3`)
	// Vec histograms include the customer_class label before le, so match the full label set.
	requireContains(`webhook_delivery_latency_seconds_bucket{customer_class="small",le="0.5"}`)
	requireContains(`webhook_queue_wait_seconds_bucket{customer_class="small",le="0.5"}`)
	requireContains(`webhook_attempt_total{outcome="succeeded"} 3`)
	requireContains(`webhook_post_duration_seconds_bucket{customer_class="small",le="0.1"}`)
	requireContains(`webhook_deliveries_in_flight 0`)
	requireContains(`webhook_attempts_per_delivery_bucket{le="2"}`)
	requireContains(`webhook_dlq_total{reason="exhausted"} 1`)
	requireContains(`webhook_scheduler_pick_duration_seconds_bucket{le="1e-05"}`)
	requireContains(`webhook_host_semaphore_wait_seconds_bucket`)
	requireContains(`webhook_claim_duration_seconds_bucket`)
	requireContains(`webhook_claim_batch_size_bucket`)
	requireContains(`webhook_claim_empty_total 2`)
	requireContains(`webhook_pending_deliveries{customer_class="small"} 7`)
	requireContains(`webhook_pending_oldest_age_seconds 12`)
	requireContains(`webhook_active_customers 4`)
	requireContains(`webhook_ingest_fanout_per_event_bucket`)
	requireContains(`webhook_breaker_state{state="open",webhook_id="webhook_1"} 1`)
	requireContains(`webhook_breaker_trips_total{reason="retryable_5xx"} 1`)
}

func TestCustomerClassFromValue(t *testing.T) {
	tests := []struct {
		class uint32
		want  string
	}{
		{5, "whale"},
		{3, "whale"},
		{1, "small"},
		{2, "small"},
		{0, "paused"},
	}
	for _, tt := range tests {
		got := CustomerClassFromValue(tt.class)
		if got != tt.want {
			t.Errorf("CustomerClassFromValue(%d) = %q, want %q", tt.class, got, tt.want)
		}
	}
}

func TestSamplePendingDeliveries_aggregates_customer_classes(t *testing.T) {
	ResetMetricsForTest()
	reg := prometheus.NewRegistry()
	RegisterMetrics(reg)

	db, err := sql.Open("backlog-fake", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := samplePendingDeliveries(context.Background(), db); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		customerClass string
		want          float64
	}{
		{customerClassSmall, 5},
		{customerClassWhale, 7},
		{customerClassPaused, 4},
	}
	for _, tt := range tests {
		t.Run(tt.customerClass, func(t *testing.T) {
			got := testutil.ToFloat64(WebhookPendingDeliveries.WithLabelValues(tt.customerClass))
			if got != tt.want {
				t.Fatalf("pending deliveries: got %v, want %v", got, tt.want)
			}
		})
	}

	if got := testutil.ToFloat64(WebhookPendingOldestAgeSeconds); got != 11 {
		t.Fatalf("oldest pending age: got %v, want 11", got)
	}
}
