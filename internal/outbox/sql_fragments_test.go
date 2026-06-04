package outbox

import (
	"strings"
	"testing"
)

func TestSqlClaimSelectionReady(t *testing.T) {
	sel := sqlClaimSelectionFor("d", "dc")

	got := sel.Ready()

	if !strings.Contains(got, "d.status IN ('pending', 'failed_retry')") {
		t.Error("Ready() missing delivery alias in status clause")
	}
	if !strings.Contains(got, "d.next_attempt_at <= now()") {
		t.Error("Ready() missing next_attempt_at clause")
	}
	if !strings.Contains(got, "dc.delivery_id IS NULL") {
		t.Error("Ready() missing claim alias in delivery_id IS NULL")
	}
	if strings.Contains(got, "$") {
		t.Errorf("Ready() must not contain positional parameters, got: %s", got)
	}
}

func TestSqlClaimSelectionReadyCustomAliases(t *testing.T) {
	sel := sqlClaimSelectionFor("del", "clm")

	got := sel.Ready()

	if !strings.Contains(got, "del.status IN") {
		t.Error("Ready() should use custom delivery alias")
	}
	if !strings.Contains(got, "del.next_attempt_at") {
		t.Error("Ready() should use custom delivery alias for next_attempt_at")
	}
	if !strings.Contains(got, "clm.delivery_id") {
		t.Error("Ready() should use custom claim alias")
	}
}

func TestSqlClaimSelectionStaleEmbedsClaimLease(t *testing.T) {
	sel := sqlClaimSelectionFor("d", "dc")

	t.Run("short_lease", func(t *testing.T) {
		got := sel.Stale(5000)

		if !strings.Contains(got, "now() - (5000 * interval '1 millisecond')") {
			t.Errorf("Stale(5000) should embed claimLeaseMS, got: %s", got)
		}
		if strings.Contains(got, "$2") {
			t.Errorf("Stale() must not contain $2 parameter reference, got: %s", got)
		}
	})

	t.Run("long_lease", func(t *testing.T) {
		got := sel.Stale(300000)

		if !strings.Contains(got, "now() - (300000 * interval '1 millisecond')") {
			t.Errorf("Stale(300000) should embed claimLeaseMS, got: %s", got)
		}
	})

	t.Run("zero_lease", func(t *testing.T) {
		got := sel.Stale(0)

		if !strings.Contains(got, "now() - (0 * interval '1 millisecond')") {
			t.Errorf("Stale(0) should embed 0, got: %s", got)
		}
	})
}

func TestSqlClaimSelectionStaleUsesClaimAlias(t *testing.T) {
	sel := sqlClaimSelectionFor("d", "clm")

	got := sel.Stale(5000)

	if !strings.Contains(got, "clm.claimed_at") {
		t.Errorf("Stale() should use claim alias 'clm', got: %s", got)
	}
}

func TestSqlClaimSelectionAnyNoPositionalParams(t *testing.T) {
	sel := sqlClaimSelectionFor("d", "dc")

	got := sel.Any(5000)

	if strings.Contains(got, "$2") {
		t.Errorf("Any() must not contain $2 parameter reference, got: %s", got)
	}
	if strings.Contains(got, "$") {
		t.Errorf("Any() must not contain any positional parameters, got: %s", got)
	}
}

func TestSqlClaimSelectionAnyComposesReadyAndStale(t *testing.T) {
	sel := sqlClaimSelectionFor("d", "dc")

	got := sel.Any(7500)

	if !strings.Contains(got, "d.status IN ('pending', 'failed_retry')") {
		t.Error("Any() missing Ready fragment")
	}
	if !strings.Contains(got, "dc.delivery_id IS NULL") {
		t.Error("Any() missing Ready claim fragment")
	}
	if !strings.Contains(got, "now() - (7500 * interval '1 millisecond')") {
		t.Error("Any() missing Stale fragment with claimLease")
	}
	if !strings.Contains(got, "OR") {
		t.Error("Any() missing OR between Ready and Stale")
	}
}

func TestSqlClaimablePredicateNoPositionalParams(t *testing.T) {
	got := sqlClaimablePredicate("d", "dc", 5000)

	if strings.Contains(got, "$2") {
		t.Errorf("sqlClaimablePredicate must not contain $2, got: %s", got)
	}
	if strings.Contains(got, "$1") {
		t.Errorf("sqlClaimablePredicate must not contain $1, got: %s", got)
	}
	if strings.Contains(got, "$") {
		t.Errorf("sqlClaimablePredicate must not contain any positional parameters, got: %s", got)
	}
}

func TestSqlClaimablePredicateEmbedsClaimLease(t *testing.T) {
	got := sqlClaimablePredicate("d", "dc", 9999)

	if !strings.Contains(got, "now() - (9999 * interval '1 millisecond')") {
		t.Errorf("sqlClaimablePredicate should embed 9999, got: %s", got)
	}
}

func TestSqlClaimablePredicateContainsNecessaryClauses(t *testing.T) {
	got := sqlClaimablePredicate("d", "dc", 5000)

	required := []string{
		"d.status IN ('pending', 'failed_retry')",
		"d.next_attempt_at <= now()",
		"NOT EXISTS",
		"delivery_claims dc",
		"dc.delivery_id = d.id",
		"EXISTS",
		"dc.claimed_at < now() - (5000 * interval '1 millisecond')",
	}

	for _, clause := range required {
		if !strings.Contains(got, clause) {
			t.Errorf("sqlClaimablePredicate missing clause: %q", clause)
		}
	}
}

func TestSqlClaimSelectionBreakerEligible(t *testing.T) {
	sel := sqlClaimSelectionFor("d", "dc").WithWebhookAlias("w")

	got := sel.BreakerEligible()

	if !strings.Contains(got, "w.breaker_state = 'closed'") {
		t.Error("BreakerEligible missing closed state")
	}
	if !strings.Contains(got, "w.breaker_state = 'half_open'") {
		t.Error("BreakerEligible missing half_open state")
	}
	if !strings.Contains(got, "w.breaker_state = 'open'") {
		t.Error("BreakerEligible missing open state")
	}
	if !strings.Contains(got, "d.id") {
		t.Error("BreakerEligible missing delivery alias for probe_delivery_id")
	}
}

func TestSqlClaimSelectionWithWebhookAliasIsImmutable(t *testing.T) {
	original := sqlClaimSelectionFor("d", "dc")
	withWebhook := original.WithWebhookAlias("w")

	if original.webhookAlias != "" {
		t.Errorf("WithWebhookAlias mutated original: webhookAlias=%q, want empty", original.webhookAlias)
	}
	if withWebhook.webhookAlias != "w" {
		t.Errorf("WithWebhookAlias returned incorrect value: webhookAlias=%q, want 'w'", withWebhook.webhookAlias)
	}
}

func TestSqlClaimSelectionBreakerEligibleWithEmptyWebhook(t *testing.T) {
	sel := sqlClaimSelectionFor("d", "dc")

	got := sel.BreakerEligible()

	if !strings.Contains(got, ".breaker_state = 'closed'") {
		t.Error("BreakerEligible should still produce SQL with empty webhook alias")
	}
}
