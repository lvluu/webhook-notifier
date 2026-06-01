package outbox

import "fmt"

func sqlClaimablePredicate(deliveryAlias, claimAlias string) string {
	return fmt.Sprintf(`(
      (%s.status IN ('pending', 'failed_retry') AND %s.next_attempt_at <= now()
       AND NOT EXISTS (SELECT 1 FROM delivery_claims %s WHERE %s.delivery_id = %s.id))
      OR EXISTS (SELECT 1 FROM delivery_claims %s WHERE %s.delivery_id = %s.id AND %s.claimed_at < now() - ($2 * interval '1 millisecond'))
  )`, deliveryAlias, deliveryAlias, claimAlias, claimAlias, deliveryAlias, claimAlias, claimAlias, deliveryAlias, claimAlias)
}

func sqlBreakerEligiblePredicate(webhookAlias, deliveryAlias string) string {
	return fmt.Sprintf(`(
      %s.breaker_state = 'closed'
      OR (%s.breaker_state = 'half_open' AND %s.breaker_probe_delivery_id = %s.id)
      OR (%s.breaker_state = 'open' AND %s.breaker_next_probe_at <= now())
  )`, webhookAlias, webhookAlias, webhookAlias, deliveryAlias, webhookAlias, webhookAlias)
}
