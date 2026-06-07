package outbox

import (
	"strings"
	"testing"
	"time"
)

func TestSqlFragmentsReady(t *testing.T) {
	if !strings.Contains(sqlReady, "d.status IN ('pending', 'failed_retry')") {
		t.Error("sqlReady missing delivery alias in status clause")
	}
	if !strings.Contains(sqlReady, "d.next_attempt_at <= now()") {
		t.Error("sqlReady missing next_attempt_at clause")
	}
	if !strings.Contains(sqlReady, "dc.delivery_id IS NULL") {
		t.Error("sqlReady missing claim alias in delivery_id IS NULL")
	}
	if strings.Contains(sqlReady, "$") {
		t.Errorf("sqlReady must not contain positional parameters, got: %s", sqlReady)
	}
}

func TestSqlFragmentsStaleViaStore(t *testing.T) {
	s := New(nil)
	if !strings.Contains(s.staleSQL, "dc.claimed_at < now()") {
		t.Error("staleSQL should reference dc.claimed_at")
	}
	if strings.Contains(s.staleSQL, "$") {
		t.Errorf("staleSQL must not contain positional parameters, got: %s", s.staleSQL)
	}
}

func TestSqlFragmentsBreakerEligible(t *testing.T) {
	if !strings.Contains(sqlBreakerEligible, "w.breaker_state = 'closed'") {
		t.Error("sqlBreakerEligible missing closed state")
	}
	if !strings.Contains(sqlBreakerEligible, "w.breaker_state = 'half_open'") {
		t.Error("sqlBreakerEligible missing half_open state")
	}
	if !strings.Contains(sqlBreakerEligible, "w.breaker_state = 'open'") {
		t.Error("sqlBreakerEligible missing open state")
	}
	if !strings.Contains(sqlBreakerEligible, "d.id") {
		t.Error("sqlBreakerEligible missing delivery alias for probe_delivery_id")
	}
}

func TestSqlFragmentsAnyComposesReadyAndStale(t *testing.T) {
	// anySQL is pre-computed on Store; this test verifies the pattern
	// via Store creation with default config.
	s := New(nil)
	if !strings.Contains(s.anySQL, "d.status IN ('pending', 'failed_retry')") {
		t.Error("anySQL missing Ready fragment")
	}
	if !strings.Contains(s.anySQL, "dc.delivery_id IS NULL") {
		t.Error("anySQL missing Ready claim fragment")
	}
	if !strings.Contains(s.anySQL, "dc.claimed_at < now()") {
		t.Error("anySQL missing Stale fragment")
	}
	if !strings.Contains(s.anySQL, "OR") {
		t.Error("anySQL missing OR between Ready and Stale")
	}
	if strings.Contains(s.anySQL, "$") {
		t.Errorf("anySQL must not contain positional parameters, got: %s", s.anySQL)
	}
}

func TestSqlFragmentsBreakerEligibleViaSelection(t *testing.T) {
	sel := sqlClaimSelectionFor("d", "dc").WithWebhookAlias("w")
	got := sel.BreakerEligible()
	if got != sqlBreakerEligible {
		t.Errorf("BreakerEligible() = %q, want %q", got, sqlBreakerEligible)
	}
}

func TestSqlFragmentsReadyWithCustomAliases(t *testing.T) {
	// sqlClaimSelection is a compatibility shim; Ready() always returns
	// the hardcoded sqlReady constant regardless of constructor args.
	sel := sqlClaimSelectionFor("del", "clm")
	got := sel.Ready()

	if !strings.Contains(got, "d.status IN") {
		t.Error("Ready() should contain d.status even with custom aliases")
	}
	if !strings.Contains(got, "dc.delivery_id") {
		t.Error("Ready() should contain dc.delivery_id even with custom aliases")
	}
}

func TestSqlFragmentsStaleRejectsDynamicInput(t *testing.T) {
	sel := sqlClaimSelectionFor("d", "dc")
	defer func() {
		if r := recover(); r == nil {
			t.Error("Stale() should panic when called with dynamic lease value")
		}
	}()
	sel.Stale(5000)
}

func TestSqlFragmentsAnyRejectsDynamicInput(t *testing.T) {
	sel := sqlClaimSelectionFor("d", "dc")
	defer func() {
		if r := recover(); r == nil {
			t.Error("Any() should panic when called with dynamic lease value")
		}
	}()
	sel.Any(5000)
}

func TestStoreFragmentsWithCustomClaimLease(t *testing.T) {
	s := New(nil, WithClaimLease(30*time.Second))
	if !strings.Contains(s.staleSQL, "30000") {
		t.Errorf("staleSQL with 30s lease should contain 30000ms, got: %s", s.staleSQL)
	}
	if !strings.Contains(s.anySQL, "30000") {
		t.Errorf("anySQL with 30s lease should contain 30000ms, got: %s", s.anySQL)
	}
}

func TestStoreFragmentsWithDefaultClaimLease(t *testing.T) {
	s := New(nil)
	if !strings.Contains(s.staleSQL, "300000") {
		t.Errorf("staleSQL with default 5m lease should contain 300000ms, got: %s", s.staleSQL)
	}
}

func TestStoreFragmentsWithShortClaimLease(t *testing.T) {
	s := New(nil, WithClaimLease(100*time.Millisecond))
	if !strings.Contains(s.staleSQL, "100") {
		t.Errorf("staleSQL with 100ms lease should contain 100, got: %s", s.staleSQL)
	}
}

func TestStoreFragmentsReadyWithoutClaim(t *testing.T) {
	if !strings.Contains(sqlReadyWithoutClaim, "d.status IN ('pending', 'failed_retry')") {
		t.Error("sqlReadyWithoutClaim missing status clause")
	}
	if !strings.Contains(sqlReadyWithoutClaim, "d.next_attempt_at <= now()") {
		t.Error("sqlReadyWithoutClaim missing next_attempt_at clause")
	}
	if strings.Contains(sqlReadyWithoutClaim, "dc.delivery_id IS NULL") {
		t.Error("sqlReadyWithoutClaim should NOT contain claim alias clause")
	}
	if strings.Contains(sqlReadyWithoutClaim, "$") {
		t.Errorf("sqlReadyWithoutClaim must not contain positional parameters, got: %s", sqlReadyWithoutClaim)
	}
}

func TestStoreFragmentsAnyCompositionOrder(t *testing.T) {
	s := New(nil)
	// anySQL should wrap ready and stale in parentheses with OR
	if !strings.HasPrefix(s.anySQL, "(") {
		t.Error("anySQL should start with opening paren")
	}
	if !strings.HasSuffix(s.anySQL, ")") {
		t.Error("anySQL should end with closing paren")
	}
	// Should contain OR between the two fragments
	readyIdx := strings.Index(s.anySQL, sqlReady)
	staleIdx := strings.Index(s.anySQL, s.staleSQL)
	orIdx := strings.Index(s.anySQL, "OR")
	if readyIdx < 0 {
		t.Error("anySQL missing sqlReady")
	}
	if staleIdx < 0 {
		t.Error("anySQL missing staleSQL")
	}
	if orIdx < 0 {
		t.Error("anySQL missing OR")
	}
	if orIdx < readyIdx || orIdx > staleIdx {
		t.Error("OR should appear between ready and stale fragments")
	}
}
