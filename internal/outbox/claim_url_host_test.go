package outbox_test

import (
	"context"
	"testing"

	"github.com/lvluu/webhook-notifier/internal/outbox"
)

// TestClaimProjectsWebhookURLHost verifies the webhooks.url_host generated
// column (migration 000017, issue #115) is populated from webhooks.url and
// projected onto the claimed Delivery, so the worker host limiter reads a
// column instead of re-parsing the URL on every Attempt.
func TestClaimProjectsWebhookURLHost(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	cases := []struct {
		name     string
		customer string
		url      string
		wantHost string
	}{
		{
			name:     "https with path",
			customer: "cus_host_https",
			url:      "https://partner.example.com/webhooks/inbound",
			wantHost: "partner.example.com",
		},
		{
			name:     "http with port stripped",
			customer: "cus_host_port",
			url:      "http://hooks.internal:8443/ingest?token=abc",
			wantHost: "hooks.internal",
		},
		{
			name:     "no scheme falls back to raw url",
			customer: "cus_host_raw",
			url:      "partner-no-scheme.example.com",
			wantHost: "partner-no-scheme.example.com",
		},
	}

	store := outbox.New(db)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			deliveryID := submitOneDeliveryToURL(t, ctx, db, tc.customer, tc.url)

			delivery, ok, err := store.ClaimNext(ctx, tc.customer)
			if err != nil {
				t.Fatalf("ClaimNext: %v", err)
			}
			if !ok {
				t.Fatalf("expected to claim delivery %s", deliveryID)
			}
			if delivery.ID != deliveryID {
				t.Fatalf("claimed delivery: got %s, want %s", delivery.ID, deliveryID)
			}
			if delivery.URLHost != tc.wantHost {
				t.Errorf("URLHost: got %q, want %q (url %q)", delivery.URLHost, tc.wantHost, tc.url)
			}
			if delivery.URL != tc.url {
				t.Errorf("URL: got %q, want %q", delivery.URL, tc.url)
			}
		})
	}
}
