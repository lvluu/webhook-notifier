package ingest_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/lvluu/webhook-notifier/internal/ingest"
)

type errRegistry struct{}

func (errRegistry) ListActiveForEvent(_ context.Context, _, _ string) ([]ingest.Webhook, error) {
	return nil, errors.New("db connection refused")
}

func TestSubmit_ValidatesInput(t *testing.T) {
	t.Parallel()

	s := ingest.New(nil, nil, nil)

	tests := []struct {
		name  string
		event ingest.Event
		want  string
	}{
		{
			name:  "rejects empty customer_id",
			event: ingest.Event{CustomerID: "", Type: "subscriber.created", Payload: json.RawMessage(`{}`)},
			want:  "customer_id is required",
		},
		{
			name:  "rejects empty type",
			event: ingest.Event{CustomerID: "cus_1", Type: "", Payload: json.RawMessage(`{}`)},
			want:  "type is required",
		},
		{
			name:  "rejects invalid json payload",
			event: ingest.Event{CustomerID: "cus_1", Type: "subscriber.created", Payload: json.RawMessage(`{invalid}`)},
			want:  "payload must be valid JSON",
		},
		{
			name:  "rejects nil payload",
			event: ingest.Event{CustomerID: "cus_1", Type: "subscriber.created", Payload: nil},
			want:  "payload must be valid JSON",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.Submit(context.Background(), tt.event)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("got error %q, want substring %q", err.Error(), tt.want)
			}
		})
	}
}

func TestSubmit_PropagatesRegistryError(t *testing.T) {
	t.Parallel()

	s := ingest.New(nil, errRegistry{}, nil)

	_, err := s.Submit(context.Background(), ingest.Event{
		CustomerID: "cus_1",
		Type:       "subscriber.created",
		Payload:    json.RawMessage(`{}`),
	})
	if err == nil {
		t.Fatal("expected error from registry")
	}
	if !strings.Contains(err.Error(), "lookup webhooks") {
		t.Errorf("error should wrap registry failure, got %v", err)
	}
	if !strings.Contains(err.Error(), "db connection refused") {
		t.Errorf("error should include underlying cause, got %v", err)
	}
}
