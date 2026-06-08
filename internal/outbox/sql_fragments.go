package outbox

// Pre-computed SQL fragments — computed once at Store construction to
// eliminate fmt.Sprintf on every claim query.

const (
	sqlReady = `d.status IN ('pending', 'failed_retry')
          AND d.next_attempt_at <= now()
          AND dc.delivery_id IS NULL`

	// ReadyWithoutClaim matches deliveries whose status and schedule make them
	// ready to attempt, ignoring whether an active claim already exists. Discovery
	// uses this claim-agnostic predicate so its loose index-scan can stop at the
	// first ready row per customer instead of walking claimed-but-still-'pending'
	// rows that the dc.delivery_id IS NULL anti-join would otherwise filter out
	// (issue #124: claimed rows stay in idx_deliveries_ready, so the anti-join
	// collapses the LIMIT-1 scan into a full hash anti-join once unclaimed rows go
	// sparse). DiscoverClaimableCustomers is only a rotation hint; ClaimNextN with
	// FOR UPDATE SKIP LOCKED remains the exact claim-eligibility authority.
	sqlReadyWithoutClaim = `d.status IN ('pending', 'failed_retry')
          AND d.next_attempt_at <= now()`

	sqlBreakerEligible = `(
      w.breaker_state = 'closed'
      OR (w.breaker_state = 'half_open' AND w.breaker_probe_delivery_id = d.id)
      OR (w.breaker_state = 'open' AND w.breaker_next_probe_at <= now())
  )`
)

// sqlClaimedDeliveryProjection is the shared SELECT-FROM-JOIN-ORDER block used
// by every claim variant to project the final claimed-delivery result set.
// Defined once so column changes are single-site edits instead of 6-site
// copies. previous_delay_millis reads deliveries.last_delay_millis directly
// (issue #112): MarkRetry(Batch) persists the applied delay on the row, so the
// claim path needs no per-row delivery_attempts lookup.
const sqlClaimedDeliveryProjection = `
	SELECT
	    d.id, d.event_id, d.webhook_id, d.customer_id,
	    COALESCE(cust.class, 1),
	    e.type, w.url, w.url_host, w.secret_encrypted,
	    'in_flight' AS status,
	    dc.attempt_number, d.next_attempt_at, dc.claimed_at,
	    d.last_error, d.created_at, d.updated_at,
	    d.last_delay_millis AS previous_delay_millis,
	    (w.breaker_state = 'half_open' AND w.breaker_probe_delivery_id = d.id) AS breaker_probe
	FROM claimed dc
	JOIN deliveries d ON d.id = dc.delivery_id
	JOIN events e ON e.id = d.event_id
	JOIN webhooks w ON w.id = d.webhook_id
	LEFT JOIN customers cust ON cust.id = d.customer_id
	ORDER BY d.next_attempt_at, d.created_at, d.id`
