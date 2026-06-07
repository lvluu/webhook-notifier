package scenarios

import (
	"context"
	"errors"
	"slices"
	"testing"
)

type fakeProductionWorkloadClaimableStore struct {
	calls     []string
	responses [][]string
	err       error
}

func (s *fakeProductionWorkloadClaimableStore) DiscoverClaimableCustomers(_ context.Context, afterCustomerID string, _ int) ([]string, error) {
	s.calls = append(s.calls, afterCustomerID)
	if s.err != nil {
		return nil, s.err
	}
	if len(s.responses) == 0 {
		return nil, nil
	}
	customers := s.responses[0]
	s.responses = s.responses[1:]
	return customers, nil
}

type fakeProductionWorkloadNotifier struct {
	notified []string
}

func (n *fakeProductionWorkloadNotifier) Notify(customerID string) {
	n.notified = append(n.notified, customerID)
}

func TestDiscoverProductionWorkloadCustomersAdvancesCursor(t *testing.T) {
	store := &fakeProductionWorkloadClaimableStore{
		responses: [][]string{
			{"cus_a", "cus_b"},
			{"cus_c"},
			{},
		},
	}
	notifier := &fakeProductionWorkloadNotifier{}

	_, cursor, err := discoverProductionWorkloadCustomers(context.Background(), store, notifier, "", 2)
	if err != nil {
		t.Fatalf("discover first page: %v", err)
	}
	if cursor != "cus_b" {
		t.Fatalf("cursor after first page: got %q, want cus_b", cursor)
	}

	_, cursor, err = discoverProductionWorkloadCustomers(context.Background(), store, notifier, cursor, 2)
	if err != nil {
		t.Fatalf("discover second page: %v", err)
	}
	if cursor != "cus_c" {
		t.Fatalf("cursor after second page: got %q, want cus_c", cursor)
	}

	_, cursor, err = discoverProductionWorkloadCustomers(context.Background(), store, notifier, cursor, 2)
	if err != nil {
		t.Fatalf("discover empty page: %v", err)
	}
	if cursor != "" {
		t.Fatalf("cursor after empty page: got %q, want empty wraparound cursor", cursor)
	}

	if !slices.Equal(store.calls, []string{"", "cus_b", "cus_c"}) {
		t.Fatalf("discovery cursors: got %v", store.calls)
	}
	if !slices.Equal(notifier.notified, []string{"cus_a", "cus_b", "cus_c"}) {
		t.Fatalf("notified customers: got %v", notifier.notified)
	}
}

func TestDiscoverProductionWorkloadCustomersReturnsStoreError(t *testing.T) {
	storeErr := errors.New("database unavailable")
	store := &fakeProductionWorkloadClaimableStore{err: storeErr}
	notifier := &fakeProductionWorkloadNotifier{}

	_, cursor, err := discoverProductionWorkloadCustomers(context.Background(), store, notifier, "cus_a", 2)
	if !errors.Is(err, storeErr) {
		t.Fatalf("discover error: got %v, want %v", err, storeErr)
	}
	if cursor != "cus_a" {
		t.Fatalf("cursor on error: got %q, want original cursor", cursor)
	}
	if len(notifier.notified) != 0 {
		t.Fatalf("notified on error: got %v, want none", notifier.notified)
	}
}
