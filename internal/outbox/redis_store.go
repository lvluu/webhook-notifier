package outbox

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lvluu/webhook-notifier/internal/observe"
	"github.com/redis/go-redis/v9"
)

const (
	redisConsumerGroup      = "notifier"
	redisStreamPrefix       = "deliveries:"
	redisDeliveryPrefix     = "delivery:"
	redisBreakerPrefix      = "breaker:"
	redisRetryScheduled     = "retry:scheduled"
	redisDefaultClaimLease  = 5 * time.Minute
	redisRetryPollInterval  = 100 * time.Millisecond
	redisRetryBatchSize     = 100
	redisDiscoveryBatchSize = 100
)

// RedisStore implements DeliveryStore using Redis Streams for the delivery queue
// and Postgres for audit records, webhook state, and circuit breaker persistence.
type RedisStore struct {
	rdb           *redis.Client
	pgdb          *sql.DB
	consumerID    string
	claimLease    time.Duration
	breakerConfig BreakerConfig

	mu           sync.Mutex
	retryStarted bool
	retryStop    chan struct{}
	retryDone    chan struct{}
}

// RedisOption configures a RedisStore.
type RedisOption func(*RedisStore)

// WithRedisConsumerID sets the consumer ID for Redis consumer group claims.
// Defaults to "default" if not set or empty.
func WithRedisConsumerID(id string) RedisOption {
	return func(s *RedisStore) {
		if id != "" {
			s.consumerID = id
		}
	}
}

// WithRedisClaimLease sets the minimum idle time before a delivery can be
// claimed by another consumer. Defaults to 5 minutes.
func WithRedisClaimLease(d time.Duration) RedisOption {
	return func(s *RedisStore) {
		if d > 0 {
			s.claimLease = d
		}
	}
}

// WithRedisBreakerConfig sets the circuit breaker configuration for webhook
// delivery. Defaults to no breaker.
func WithRedisBreakerConfig(cfg BreakerConfig) RedisOption {
	return func(s *RedisStore) {
		s.breakerConfig = normalizeBreakerConfig(cfg)
	}
}

// NewRedisStore returns a Redis-backed DeliveryStore. The store writes
// delivery claims and results to Redis Streams while persisting audit
// records and circuit breaker state to Postgres.
func NewRedisStore(rdb *redis.Client, pgdb *sql.DB, opts ...RedisOption) *RedisStore {
	s := &RedisStore{
		rdb:           rdb,
		pgdb:          pgdb,
		consumerID:    "default",
		claimLease:    redisDefaultClaimLease,
		breakerConfig: defaultBreakerConfig(),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func redisStreamKey(customerID string) string {
	return redisStreamPrefix + customerID
}

func redisDeliveryKey(deliveryID string) string {
	return redisDeliveryPrefix + deliveryID
}

func redisBreakerKey(webhookID string) string {
	return redisBreakerPrefix + webhookID
}

func ensureConsumerGroup(ctx context.Context, rdb *redis.Client, streamKey string) error {
	err := rdb.XGroupCreateMkStream(ctx, streamKey, redisConsumerGroup, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("create consumer group for %s: %w", streamKey, err)
	}
	return nil
}

// ClaimNext claims a single pending delivery from the Redis stream.
func (s *RedisStore) ClaimNext(ctx context.Context, customerID string) (Delivery, bool, error) {
	deliveries, err := s.ClaimNextN(ctx, customerID, 1)
	if err != nil {
		return Delivery{}, false, err
	}
	if len(deliveries) == 0 {
		return Delivery{}, false, nil
	}
	return deliveries[0], true, nil
}

// ClaimNextN claims up to limit pending deliveries from the Redis stream for
// the given customer. Uses XAutoClaim to rebalance idle deliveries across
// consumers and XReadGroup for unread entries.
func (s *RedisStore) ClaimNextN(ctx context.Context, customerID string, limit int) ([]Delivery, error) {
	if customerID == "" {
		return nil, fmt.Errorf("customer_id is required")
	}
	if limit < 1 {
		return nil, fmt.Errorf("limit must be >= 1")
	}

	claimStarted := time.Now()
	streamKey := redisStreamKey(customerID)

	if err := ensureConsumerGroup(ctx, s.rdb, streamKey); err != nil {
		return nil, err
	}

	claims, _, err := s.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   streamKey,
		Group:    redisConsumerGroup,
		Consumer: s.consumerID,
		MinIdle:  s.claimLease,
		Start:    "0-0",
		Count:    int64(limit),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("xautoclaim stale deliveries: %w", err)
	}
	claimed := make(map[string]redis.XMessage)
	for _, msg := range claims {
		claimed[msg.ID] = msg
	}

	remaining := limit - len(claimed)
	if remaining > 0 {
		streams, err := s.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    redisConsumerGroup,
			Consumer: s.consumerID,
			Streams:  []string{streamKey, ">"},
			Count:    int64(remaining),
			Block:    0,
		}).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("xreadgroup: %w", err)
		}
		for _, stream := range streams {
			for _, msg := range stream.Messages {
				claimed[msg.ID] = msg
			}
		}
	}

	if len(claimed) == 0 {
		observe.IncClaimEmpty()
		observe.ObserveClaimDuration(time.Since(claimStarted).Seconds())
		observe.ObserveClaimBatchSize(0)
		return nil, nil
	}

	deliveries := make([]Delivery, 0, len(claimed))
	for msgID, msg := range claimed {
		deliveryID, ok := msg.Values["id"].(string)
		if !ok || deliveryID == "" {
			continue
		}
		webhookID, _ := msg.Values["webhook_id"].(string)

		if s.breakerConfig.FailureThreshold > 0 {
			ok, err := s.checkBreaker(ctx, webhookID, deliveryID)
			if err != nil {
				return nil, fmt.Errorf("check breaker for %s: %w", deliveryID, err)
			}
			if !ok {
				if err := s.rdb.XAck(ctx, streamKey, redisConsumerGroup, msgID).Err(); err != nil {
					return nil, fmt.Errorf("xack breaker-blocked %s: %w", msgID, err)
				}
				continue
			}
		}

		d, err := s.deliveryFromStream(ctx, msg, deliveryID)
		if err != nil {
			return nil, fmt.Errorf("build delivery %s: %w", deliveryID, err)
		}
		deliveries = append(deliveries, d)
	}

	if len(deliveries) == 0 {
		observe.IncClaimEmpty()
	}
	observe.ObserveClaimDuration(time.Since(claimStarted).Seconds())
	observe.ObserveClaimBatchSize(float64(len(deliveries)))
	return deliveries, nil
}

func (s *RedisStore) deliveryFromStream(ctx context.Context, msg redis.XMessage, deliveryID string) (Delivery, error) {
	vals := msg.Values

	var secret string
	if v, ok := vals["secret"].(string); ok {
		secret = v
	}

	var eventType string
	if v, ok := vals["event_type"].(string); ok {
		eventType = v
	}

	var payload []byte
	if v, ok := vals["payload"].(string); ok {
		payload = []byte(v)
	}

	var url string
	if v, ok := vals["url"].(string); ok {
		url = v
	}

	var eventID string
	if v, ok := vals["event_id"].(string); ok {
		eventID = v
	}

	var webhookID string
	if v, ok := vals["webhook_id"].(string); ok {
		webhookID = v
	}

	var customerID string
	if v, ok := vals["customer_id"].(string); ok {
		customerID = v
	}

	var customerClass uint32 = 1
	if v, ok := vals["customer_class"].(string); ok {
		if n, err := strconv.ParseUint(v, 10, 32); err == nil {
			customerClass = uint32(n)
		}
	}

	var attemptNumber int
	if v, ok := vals["attempt_count"].(string); ok {
		if n, err := strconv.Atoi(v); err == nil {
			attemptNumber = n
		}
	}

	var previousDelay time.Duration
	if v, ok := vals["previous_delay_ms"].(string); ok {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			previousDelay = time.Duration(n) * time.Millisecond
		}
	}

	var createdAt time.Time
	if v, ok := vals["created_at"].(string); ok {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			createdAt = time.UnixMilli(n)
		}
	}

	var lastError sql.NullString
	if v, ok := vals["last_error"].(string); ok && v != "" {
		lastError = sql.NullString{String: v, Valid: true}
	}

	now := time.Now()
	return Delivery{
		ID:            deliveryID,
		EventID:       eventID,
		WebhookID:     webhookID,
		CustomerID:    customerID,
		CustomerClass: customerClass,
		EventType:     eventType,
		Payload:       payload,
		URL:           url,
		Secret:        secret,
		Status:        StatusInFlight,
		AttemptNumber: attemptNumber,
		PreviousDelay: previousDelay,
		NextAttemptAt: now,
		ClaimedAt:     now,
		LastError:     lastError,
		CreatedAt:     createdAt,
		UpdatedAt:     now,
	}, nil
}

// HasClaimableWork checks whether the customer has unread stream entries or
// pending retry work in Redis.
func (s *RedisStore) HasClaimableWork(ctx context.Context, customerID string) (bool, error) {
	streamKey := redisStreamKey(customerID)

	info, err := s.rdb.XPending(ctx, streamKey, redisConsumerGroup).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return s.hasRetryWork(ctx, customerID)
		}
		return false, fmt.Errorf("xpending: %w", err)
	}

	len, err := s.rdb.XLen(ctx, streamKey).Result()
	if err != nil {
		return false, fmt.Errorf("xlen: %w", err)
	}

	if len > info.Count {
		return true, nil
	}

	return s.hasRetryWork(ctx, customerID)
}

func (s *RedisStore) hasRetryWork(ctx context.Context, customerID string) (bool, error) {
	res, _, err := s.rdb.ZScan(ctx, redisRetryScheduled, 0, customerID, 1).Result()
	if err != nil {
		return false, fmt.Errorf("zscan retry: %w", err)
	}
	return len(res) > 0, nil
}

// DiscoverClaimableCustomers returns customers with pending Redis stream
// entries by scanning stream keys matching the delivery prefix.
func (s *RedisStore) DiscoverClaimableCustomers(ctx context.Context, afterCustomerID string, limit int) ([]string, error) {
	if limit < 1 {
		return nil, fmt.Errorf("limit must be >= 1")
	}

	match := redisStreamPrefix + "*"
	var cursor uint64
	var customerIDs []string

	for {
		keys, nextCursor, err := s.rdb.Scan(ctx, cursor, match, int64(limit)).Result()
		if err != nil {
			return nil, fmt.Errorf("scan streams: %w", err)
		}
		for _, key := range keys {
			cid := strings.TrimPrefix(key, redisStreamPrefix)
			if cid <= afterCustomerID {
				continue
			}
			hasWork, err := s.HasClaimableWork(ctx, cid)
			if err != nil {
				continue
			}
			if hasWork {
				customerIDs = append(customerIDs, cid)
				if len(customerIDs) >= limit {
					return customerIDs, nil
				}
			}
		}
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	return customerIDs, nil
}

// MarkSucceeded marks a delivery as successful in the Redis stream and PG
// audit log, and records the breaker success.
func (s *RedisStore) MarkSucceeded(ctx context.Context, deliveryID string, statusCode int, duration time.Duration) error {
	if deliveryID == "" {
		return fmt.Errorf("delivery_id is required")
	}
	return s.markInTransaction(ctx, deliveryID, func(tx *sql.Tx, attemptNumber int, webhookID string) error {
		if err := s.ackStreamMessage(ctx, deliveryID); err != nil {
			return err
		}
		if err := s.cleanupDeliveryHash(ctx, deliveryID); err != nil {
			return err
		}
		if err := s.insertAttempt(ctx, tx, deliveryID, attemptNumber, statusCode, "", duration); err != nil {
			return err
		}
		if err := s.updateDeliveryStatusPG(ctx, tx, deliveryID, StatusSucceeded, attemptNumber, 0, sql.NullString{}); err != nil {
			return err
		}
		_, err := s.recordBreakerSuccessPG(ctx, tx, webhookID, deliveryID)
		return err
	})
}

// MarkRetry schedules a delivery for retry by adding it to the Redis sorted
// set and updating PG audit state.
func (s *RedisStore) MarkRetry(ctx context.Context, deliveryID string, statusCode int, errorClass string, delay time.Duration, duration time.Duration) error {
	if deliveryID == "" {
		return fmt.Errorf("delivery_id is required")
	}
	if delay < 0 {
		return fmt.Errorf("delay must be non-negative")
	}
	return s.markInTransaction(ctx, deliveryID, func(tx *sql.Tx, attemptNumber int, webhookID string) error {
		if err := s.ackStreamMessage(ctx, deliveryID); err != nil {
			return err
		}
		if err := s.scheduleRetry(ctx, deliveryID, delay); err != nil {
			return err
		}
		if err := s.insertAttempt(ctx, tx, deliveryID, attemptNumber, statusCode, errorClass, duration); err != nil {
			return err
		}
		if err := s.updateDeliveryStatusPG(ctx, tx, deliveryID, StatusFailedRetry, attemptNumber, delay, nullIfEmpty(errorClass)); err != nil {
			return err
		}
		tripped, err := s.recordBreakerFailurePG(ctx, tx, webhookID, errorClass)
		if tripped {
			slog.Default().Warn("webhook circuit breaker opened", "webhook_id", webhookID, "reason", errorClass)
			observe.SetBreakerState(webhookID, BreakerStateOpen)
			observe.IncBreakerTrip(errorClass)
		}
		return err
	})
}

// MarkDead moves a delivery to the dead-letter state in both Redis and PG.
func (s *RedisStore) MarkDead(ctx context.Context, deliveryID string, statusCode int, errorClass string, reason string, duration time.Duration) error {
	if deliveryID == "" {
		return fmt.Errorf("delivery_id is required")
	}
	if reason == "" {
		return fmt.Errorf("reason is required")
	}
	return s.markInTransaction(ctx, deliveryID, func(tx *sql.Tx, attemptNumber int, webhookID string) error {
		if err := s.ackStreamMessage(ctx, deliveryID); err != nil {
			return err
		}
		if err := s.cleanupDeliveryHash(ctx, deliveryID); err != nil {
			return err
		}
		if err := s.insertAttempt(ctx, tx, deliveryID, attemptNumber, statusCode, errorClass, duration); err != nil {
			return err
		}
		if err := s.updateDeliveryStatusPG(ctx, tx, deliveryID, StatusDead, attemptNumber, 0, nullIfEmpty(errorClass)); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO dead_letters (delivery_id, last_error, attempts, reason, final_at)
			VALUES ($1, $2, $3, $4, now())
			ON CONFLICT (delivery_id) DO UPDATE
			SET last_error = EXCLUDED.last_error,
			    attempts = EXCLUDED.attempts,
			    reason = EXCLUDED.reason,
			    final_at = EXCLUDED.final_at
		`, deliveryID, nullIfEmpty(errorClass), attemptNumber, reason); err != nil {
			return fmt.Errorf("insert dead letter: %w", err)
		}
		tripped, err := s.recordBreakerFailurePG(ctx, tx, webhookID, reason)
		if tripped {
			slog.Default().Warn("webhook circuit breaker opened", "webhook_id", webhookID, "reason", reason)
			observe.SetBreakerState(webhookID, BreakerStateOpen)
			observe.IncBreakerTrip(reason)
		}
		return err
	})
}

// Requeue re-adds a delivery to the stream for retry, removing it from the
// retry sorted set and updating PG.
func (s *RedisStore) Requeue(ctx context.Context, deliveryID string, delay time.Duration) error {
	if deliveryID == "" {
		return fmt.Errorf("delivery_id is required")
	}
	if delay < 0 {
		return fmt.Errorf("delay must be non-negative")
	}

	if err := s.ackStreamMessage(ctx, deliveryID); err != nil {
		return err
	}
	return s.scheduleRetry(ctx, deliveryID, delay)
}

// MarkSucceededBatch atomically marks multiple deliveries as successful.
func (s *RedisStore) MarkSucceededBatch(ctx context.Context, items []BatchMarkItem) error {
	if len(items) == 0 {
		return nil
	}
	for _, item := range items {
		if err := s.MarkSucceeded(ctx, item.DeliveryID, item.StatusCode, item.Duration); err != nil {
			return fmt.Errorf("batch succeeded %s: %w", item.DeliveryID, err)
		}
	}
	return nil
}

// MarkRetryBatch atomically schedules multiple deliveries for retry.
func (s *RedisStore) MarkRetryBatch(ctx context.Context, items []BatchMarkItem) error {
	if len(items) == 0 {
		return nil
	}
	for _, item := range items {
		if err := s.MarkRetry(ctx, item.DeliveryID, item.StatusCode, item.ErrorClass, item.Delay, item.Duration); err != nil {
			return fmt.Errorf("batch retry %s: %w", item.DeliveryID, err)
		}
	}
	return nil
}

// MarkDeadBatch atomically moves multiple deliveries to the dead-letter state.
func (s *RedisStore) MarkDeadBatch(ctx context.Context, items []BatchMarkItem) error {
	if len(items) == 0 {
		return nil
	}
	for _, item := range items {
		if err := s.MarkDead(ctx, item.DeliveryID, item.StatusCode, item.ErrorClass, item.Reason, item.Duration); err != nil {
			return fmt.Errorf("batch dead %s: %w", item.DeliveryID, err)
		}
	}
	return nil
}

func (s *RedisStore) ackStreamMessage(ctx context.Context, deliveryID string) error {
	hashKey := redisDeliveryKey(deliveryID)
	customerID, err := s.rdb.HGet(ctx, hashKey, "customer_id").Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil
		}
		return fmt.Errorf("hget customer_id for %s: %w", deliveryID, err)
	}
	streamKey := redisStreamKey(customerID)
	if err := s.rdb.XAck(ctx, streamKey, redisConsumerGroup, deliveryID).Err(); err != nil {
		return fmt.Errorf("xack %s: %w", deliveryID, err)
	}
	return nil
}

func (s *RedisStore) cleanupDeliveryHash(ctx context.Context, deliveryID string) error {
	return s.rdb.Del(ctx, redisDeliveryKey(deliveryID)).Err()
}

func (s *RedisStore) scheduleRetry(ctx context.Context, deliveryID string, delay time.Duration) error {
	nextAttemptAt := time.Now().Add(delay)
	score := float64(nextAttemptAt.UnixMilli())
	member := deliveryID

	hashKey := redisDeliveryKey(deliveryID)
	_, err := s.rdb.HIncrBy(ctx, hashKey, "attempt_count", 1).Result()
	if err != nil {
		return fmt.Errorf("incr attempt_count for %s: %w", deliveryID, err)
	}

	_, err = s.rdb.HSet(ctx, hashKey, "delay_ms", delay.Milliseconds()).Result()
	if err != nil {
		return fmt.Errorf("hset delay for %s: %w", deliveryID, err)
	}

	return s.rdb.ZAdd(ctx, redisRetryScheduled, redis.Z{
		Score:  score,
		Member: member,
	}).Err()
}

func (s *RedisStore) insertAttempt(ctx context.Context, tx *sql.Tx, deliveryID string, attemptNumber int, statusCode int, errorClass string, duration time.Duration) error {
	var nullableStatus sql.NullInt64
	if statusCode > 0 {
		nullableStatus = sql.NullInt64{Int64: int64(statusCode), Valid: true}
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO delivery_attempts (delivery_id, attempt_number, status_code, error_class, duration_ms, sent_at)
		VALUES ($1, $2, $3, $4, $5, now())
	`, deliveryID, attemptNumber, nullableStatus, nullIfEmpty(errorClass), duration.Milliseconds())
	if err != nil {
		return fmt.Errorf("insert delivery attempt: %w", err)
	}
	return nil
}

func (s *RedisStore) updateDeliveryStatusPG(ctx context.Context, tx *sql.Tx, deliveryID string, status DeliveryStatus, attemptNumber int, delay time.Duration, lastError sql.NullString) error {
	switch status {
	case StatusSucceeded:
		_, err := tx.ExecContext(ctx, `
			UPDATE deliveries SET status = 'succeeded', attempt_count = $2, updated_at = now(), last_error = NULL
			WHERE id = $1
		`, deliveryID, attemptNumber)
		return err
	case StatusFailedRetry:
		_, err := tx.ExecContext(ctx, `
			UPDATE deliveries SET status = 'failed_retry', attempt_count = $4, next_attempt_at = now() + ($2 * interval '1 millisecond'), last_error = $3, updated_at = now()
			WHERE id = $1
		`, deliveryID, delay.Milliseconds(), lastError, attemptNumber)
		return err
	case StatusDead:
		_, err := tx.ExecContext(ctx, `
			UPDATE deliveries SET status = 'dead', attempt_count = $2, last_error = $3, updated_at = now()
			WHERE id = $1
		`, deliveryID, attemptNumber, lastError)
		return err
	default:
		return fmt.Errorf("unknown status: %s", status)
	}
}

func (s *RedisStore) markInTransaction(ctx context.Context, deliveryID string, mutate func(*sql.Tx, int, string) error) error {
	hashKey := redisDeliveryKey(deliveryID)
	data, err := s.rdb.HGetAll(ctx, hashKey).Result()
	if err != nil {
		return fmt.Errorf("hgetall delivery %s: %w", deliveryID, err)
	}
	if len(data) == 0 {
		return fmt.Errorf("delivery %s not found", deliveryID)
	}

	attemptNumStr := data["attempt_count"]
	if attemptNumStr == "" {
		attemptNumStr = "0"
	}
	attemptNumber, _ := strconv.Atoi(attemptNumStr)
	webhookID := data["webhook_id"]
	customerID := data["customer_id"]
	if webhookID == "" || customerID == "" {
		return fmt.Errorf("delivery %s missing webhook_id or customer_id", deliveryID)
	}

	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := mutate(tx, attemptNumber, webhookID); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}

	if _, ok := data["status"]; ok && data["status"] != string(StatusFailedRetry) && data["status"] != string(StatusPending) {
		s.rdb.Del(ctx, hashKey)
	}

	return nil
}

func (s *RedisStore) checkBreaker(ctx context.Context, webhookID string, deliveryID string) (bool, error) {
	if s.breakerConfig.FailureThreshold == 0 {
		return true, nil
	}

	bKey := redisBreakerKey(webhookID)
	data, err := s.rdb.HGetAll(ctx, bKey).Result()
	if err != nil {
		return false, fmt.Errorf("hgetall breaker: %w", err)
	}

	switch data["state"] {
	case "":
		return true, nil
	case BreakerStateClosed:
		return true, nil
	case BreakerStateOpen:
		nextProbeStr, ok := data["next_probe_at"]
		if !ok {
			return false, nil
		}
		nextProbe, err := strconv.ParseInt(nextProbeStr, 10, 64)
		if err != nil {
			return false, nil
		}
		if time.Now().UnixMilli() < nextProbe {
			return false, nil
		}
		return true, nil
	case BreakerStateHalfOpen:
		probeID := data["probe_delivery_id"]
		return probeID == deliveryID, nil
	default:
		return true, nil
	}
}

func (s *RedisStore) recordBreakerSuccessPG(ctx context.Context, tx *sql.Tx, webhookID string, deliveryID string) (bool, error) {
	if s.breakerConfig.FailureThreshold == 0 {
		return false, nil
	}

	bKey := redisBreakerKey(webhookID)
	s.rdb.HSet(ctx, bKey, map[string]interface{}{
		"state":             BreakerStateClosed,
		"failure_count":     0,
		"next_probe_at":     "",
		"cooldown_ms":       s.breakerConfig.InitialCooldown.Milliseconds(),
		"probe_delivery_id": "",
	})

	var updated string
	err := tx.QueryRowContext(ctx, `
		UPDATE webhooks
		SET breaker_state = 'closed',
		    breaker_failure_count = 0,
		    breaker_opened_at = NULL,
		    breaker_next_probe_at = NULL,
		    breaker_cooldown_ms = $2,
		    breaker_probe_delivery_id = NULL
		WHERE id = $1::uuid
		  AND breaker_state <> 'closed'
		  AND NOT (
		      breaker_state = 'half_open'
		      AND breaker_probe_delivery_id IS DISTINCT FROM $3::uuid
		  )
		RETURNING id
	`, webhookID, s.breakerConfig.InitialCooldown.Milliseconds(), deliveryID).Scan(&updated)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("record breaker success: %w", err)
	}
	slog.Default().Info("webhook circuit breaker closed", "webhook_id", webhookID)
	return true, nil
}

func (s *RedisStore) recordBreakerFailurePG(ctx context.Context, tx *sql.Tx, webhookID string, reason string) (bool, error) {
	if s.breakerConfig.FailureThreshold == 0 {
		return false, nil
	}

	bKey := redisBreakerKey(webhookID)
	var state string
	var failureCount int
	var cooldownMs int64

	data, err := s.rdb.HGetAll(ctx, bKey).Result()
	if err == nil && len(data) > 0 {
		state = data["state"]
		if v, ok := data["failure_count"]; ok {
			failureCount, _ = strconv.Atoi(v)
		}
		if v, ok := data["cooldown_ms"]; ok {
			cooldownMs, _ = strconv.ParseInt(v, 10, 64)
		}
		_ = data["probe_delivery_id"]
	}

	pgState := state
	if pgState == "" {
		pgState = BreakerStateClosed
	}

	failureCount++

	var open bool
	var newCooldownMs int64

	switch pgState {
	case BreakerStateHalfOpen:
		newCooldownMs = nextBreakerCooldownRedis(time.Duration(cooldownMs)*time.Millisecond, s.breakerConfig).Milliseconds()
		open = true
	case BreakerStateClosed:
		if failureCount >= s.breakerConfig.FailureThreshold {
			newCooldownMs = s.breakerConfig.InitialCooldown.Milliseconds()
			open = true
		}
	}

	if open {
		nextProbe := time.Now().Add(time.Duration(newCooldownMs) * time.Millisecond)
		s.rdb.HSet(ctx, bKey, map[string]interface{}{
			"state":         BreakerStateOpen,
			"failure_count": failureCount,
			"next_probe_at": nextProbe.UnixMilli(),
			"cooldown_ms":   newCooldownMs,
		})

		result, err := tx.ExecContext(ctx, `
			UPDATE webhooks
			SET breaker_state = 'open',
			    breaker_failure_count = $2,
			    breaker_opened_at = now(),
			    breaker_next_probe_at = now() + ($3 * interval '1 millisecond'),
			    breaker_cooldown_ms = $3,
			    breaker_probe_delivery_id = NULL
			WHERE id = $1::uuid
			  AND breaker_state <> 'open'
		`, webhookID, failureCount, newCooldownMs)
		if err != nil {
			return false, fmt.Errorf("open breaker: %w", err)
		}
		rows, _ := result.RowsAffected()
		return rows > 0, nil
	}

	s.rdb.HSet(ctx, bKey, "failure_count", failureCount)
	return false, nil
}

func nextBreakerCooldownRedis(current time.Duration, config BreakerConfig) time.Duration {
	if current <= 0 {
		return config.InitialCooldown
	}
	next := current * 2
	if next > config.MaxCooldown {
		return config.MaxCooldown
	}
	return next
}

// RunRetryScheduler starts a background goroutine that polls the retry sorted
// set and requeues due deliveries back into the stream.
func (s *RedisStore) RunRetryScheduler(ctx context.Context) {
	s.mu.Lock()
	if s.retryStarted {
		s.mu.Unlock()
		return
	}
	s.retryStarted = true
	s.retryStop = make(chan struct{})
	s.retryDone = make(chan struct{})
	s.mu.Unlock()

	go func() {
		defer close(s.retryDone)
		ticker := time.NewTicker(redisRetryPollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-s.retryStop:
				return
			case <-ticker.C:
				if err := s.flushRetries(ctx); err != nil {
					slog.Default().Warn("retry scheduler error", "error", err)
				}
			}
		}
	}()
}

// StopRetryScheduler stops the retry scheduler goroutine, blocking until
// it exits.
func (s *RedisStore) StopRetryScheduler() {
	s.mu.Lock()
	if !s.retryStarted {
		s.mu.Unlock()
		return
	}
	close(s.retryStop)
	s.mu.Unlock()
	<-s.retryDone
}

func (s *RedisStore) flushRetries(ctx context.Context) error {
	now := strconv.FormatInt(time.Now().UnixMilli(), 10)
	results, err := s.rdb.ZRangeByScoreWithScores(ctx, redisRetryScheduled, &redis.ZRangeBy{
		Min:   "0",
		Max:   now,
		Count: int64(redisRetryBatchSize),
	}).Result()
	if err != nil {
		return fmt.Errorf("zrangebyscore: %w", err)
	}
	if len(results) == 0 {
		return nil
	}

	for _, z := range results {
		deliveryID, ok := z.Member.(string)
		if !ok {
			continue
		}

		hashKey := redisDeliveryKey(deliveryID)
		data, err := s.rdb.HGetAll(ctx, hashKey).Result()
		if err != nil {
			return fmt.Errorf("hgetall for retry %s: %w", deliveryID, err)
		}
		if len(data) == 0 {
			s.rdb.ZRem(ctx, redisRetryScheduled, deliveryID)
			continue
		}

		fields := map[string]interface{}{
			"id":                deliveryID,
			"event_id":          data["event_id"],
			"webhook_id":        data["webhook_id"],
			"customer_id":       data["customer_id"],
			"customer_class":    data["customer_class"],
			"event_type":        data["event_type"],
			"payload":           data["payload"],
			"url":               data["url"],
			"secret":            data["secret"],
			"created_at":        data["created_at"],
			"attempt_count":     data["attempt_count"],
			"previous_delay_ms": data["delay_ms"],
			"last_error":        data["last_error"],
		}

		streamKey := redisStreamKey(data["customer_id"])
		if err := ensureConsumerGroup(ctx, s.rdb, streamKey); err != nil {
			return err
		}

		if _, err := s.rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamKey,
			ID:     deliveryID,
			Values: fields,
		}).Result(); err != nil {
			return fmt.Errorf("xadd retry %s: %w", deliveryID, err)
		}

		if err := s.rdb.ZRem(ctx, redisRetryScheduled, deliveryID).Err(); err != nil {
			return fmt.Errorf("zrem retry %s: %w", deliveryID, err)
		}
	}

	return nil
}

// ArchiveTerminalDeliveries is a no-op for the Redis store since terminal
// state is managed in the delivery hash and stream.
func (s *RedisStore) ArchiveTerminalDeliveries(ctx context.Context, retention time.Duration, limit int) (int, error) {
	return 0, nil
}

// NewRedisDeliveryID generates a UUID v4 formatted string for use as a Redis
// stream entry ID and delivery hash key.
func NewRedisDeliveryID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		panic(fmt.Sprintf("rand read: %v", err))
	}
	buf[6] = (buf[6] & 0x0f) | 0x40
	buf[8] = (buf[8] & 0x3f) | 0x80
	hex := fmt.Sprintf("%x-%x-%x-%x-%x", buf[0:4], buf[4:6], buf[6:8], buf[8:10], buf[10:])
	return hex
}
