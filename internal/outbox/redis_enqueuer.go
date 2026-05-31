package outbox

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/lvluu/webhook-notifier/internal/registry"
)

// RedisEnqueuer implements ingest.DeliveryEnqueuer by writing delivery
// records to Redis Streams and delivery hashes.
type RedisEnqueuer struct {
	rdb *redis.Client
}

// NewRedisEnqueuer returns a RedisEnqueuer that writes deliveries to Redis.
func NewRedisEnqueuer(rdb *redis.Client) *RedisEnqueuer {
	return &RedisEnqueuer{rdb: rdb}
}

// Enqueue creates delivery entries in the Redis stream and delivery hash
// for each matching webhook, then returns the delivery IDs.
func (e *RedisEnqueuer) Enqueue(ctx context.Context, eventID string, customerID string, hooks []registry.Webhook) ([]string, error) {
	if len(hooks) == 0 {
		return nil, nil
	}

	streamKey := redisStreamKey(customerID)
	if err := ensureConsumerGroup(ctx, e.rdb, streamKey); err != nil {
		return nil, err
	}

	ids := make([]string, 0, len(hooks))
	now := time.Now()
	nowMs := now.UnixMilli()

	pipe := e.rdb.Pipeline()

	for _, hook := range hooks {
		deliveryID := NewRedisDeliveryID()
		ids = append(ids, deliveryID)

		pipe.HSet(ctx, redisDeliveryKey(deliveryID), map[string]interface{}{
			"id":                deliveryID,
			"event_id":          eventID,
			"webhook_id":        hook.ID,
			"customer_id":       customerID,
			"customer_class":    "1",
			"event_type":        "webhook",
			"payload":           "{}",
			"url":               hook.URL,
			"secret":            hook.SecretEncrypted,
			"created_at":        strconv.FormatInt(nowMs, 10),
			"attempt_count":     "0",
			"previous_delay_ms": "0",
			"last_error":        "",
			"delay_ms":          "0",
		})

		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: streamKey,
			ID:     deliveryID,
			Values: map[string]interface{}{
				"id":                deliveryID,
				"event_id":          eventID,
				"webhook_id":        hook.ID,
				"customer_id":       customerID,
				"customer_class":    "1",
				"event_type":        "webhook",
				"payload":           "{}",
				"url":               hook.URL,
				"secret":            hook.SecretEncrypted,
				"created_at":        strconv.FormatInt(nowMs, 10),
				"attempt_count":     "0",
				"previous_delay_ms": "0",
				"last_error":        "",
			},
		})
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("pipeline enqueue: %w", err)
	}

	return ids, nil
}
