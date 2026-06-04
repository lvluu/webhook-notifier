package outbox

import "fmt"

type sqlClaimSelection struct {
	deliveryAlias string
	claimAlias    string
	webhookAlias  string
}

func sqlClaimSelectionFor(deliveryAlias, claimAlias string) sqlClaimSelection {
	return sqlClaimSelection{
		deliveryAlias: deliveryAlias,
		claimAlias:    claimAlias,
	}
}

func (s sqlClaimSelection) WithWebhookAlias(webhookAlias string) sqlClaimSelection {
	s.webhookAlias = webhookAlias
	return s
}

func (s sqlClaimSelection) Ready() string {
	return fmt.Sprintf(`%s.status IN ('pending', 'failed_retry')
          AND %s.next_attempt_at <= now()
          AND %s.delivery_id IS NULL`, s.deliveryAlias, s.deliveryAlias, s.claimAlias)
}

func (s sqlClaimSelection) Stale(claimLeaseMS int64) string {
	return fmt.Sprintf(`%s.claimed_at < now() - (%d * interval '1 millisecond')`, s.claimAlias, claimLeaseMS)
}

func (s sqlClaimSelection) Any(claimLeaseMS int64) string {
	return fmt.Sprintf(`(
      (%s)
      OR (%s)
  )`, s.Ready(), s.Stale(claimLeaseMS))
}

func (s sqlClaimSelection) BreakerEligible() string {
	return sqlBreakerEligiblePredicate(s.webhookAlias, s.deliveryAlias)
}

func sqlClaimablePredicate(deliveryAlias, claimAlias string, claimLeaseMS int64) string {
	return fmt.Sprintf(`(
      (%s.status IN ('pending', 'failed_retry') AND %s.next_attempt_at <= now()
       AND NOT EXISTS (SELECT 1 FROM delivery_claims %s WHERE %s.delivery_id = %s.id))
      OR EXISTS (SELECT 1 FROM delivery_claims %s WHERE %s.delivery_id = %s.id AND %s.claimed_at < now() - (%d * interval '1 millisecond'))
  )`, deliveryAlias, deliveryAlias, claimAlias, claimAlias, deliveryAlias, claimAlias, claimAlias, deliveryAlias, claimAlias, claimLeaseMS)
}

func sqlBreakerEligiblePredicate(webhookAlias, deliveryAlias string) string {
	return fmt.Sprintf(`(
      %s.breaker_state = 'closed'
      OR (%s.breaker_state = 'half_open' AND %s.breaker_probe_delivery_id = %s.id)
      OR (%s.breaker_state = 'open' AND %s.breaker_next_probe_at <= now())
  )`, webhookAlias, webhookAlias, webhookAlias, deliveryAlias, webhookAlias, webhookAlias)
}

// sqlClaimedDeliveryProjection is the shared SELECT-FROM-JOIN-ORDER block used
// by every claim variant to project the final claimed-delivery result set.
// Defined once so the previous_delay_millis gate from #102 and any future
// column changes are single-site edits instead of 6-site copies.
const sqlClaimedDeliveryProjection = `
	SELECT
	    d.id, d.event_id, d.webhook_id, d.customer_id,
	    COALESCE(cust.class, 1),
	    e.type, e.payload, w.url, w.secret_encrypted,
	    'in_flight' AS status,
	    dc.attempt_number, d.next_attempt_at, dc.claimed_at,
	    d.last_error, d.created_at, d.updated_at,
	    CASE WHEN d.attempt_count = 0 THEN NULL ELSE (
	        SELECT GREATEST(EXTRACT(EPOCH FROM (d.next_attempt_at - da.sent_at)) * 1000, 0)::bigint
	        FROM delivery_attempts da
	        WHERE da.delivery_id = d.id
	        ORDER BY da.attempt_number DESC
	        LIMIT 1
	    ) END AS previous_delay_millis
	FROM claimed dc
	JOIN deliveries d ON d.id = dc.delivery_id
	JOIN events e ON e.id = d.event_id
	JOIN webhooks w ON w.id = d.webhook_id
	LEFT JOIN customers cust ON cust.id = d.customer_id
	ORDER BY d.next_attempt_at, d.created_at, d.id`
