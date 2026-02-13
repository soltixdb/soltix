package queue

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisConfig represents Redis Streams configuration
type RedisConfig struct {
	URL      string // Redis URL (e.g., redis://localhost:6379)
	Password string // Optional password
	DB       int    // Database number (default: 0)
	Stream   string // Stream prefix (default: "soltix")
	Group    string // Consumer group name (default: "soltix-group")
	Consumer string // Consumer name (default: hostname)
}

// RedisQueue implements Queue interface using Redis Streams
type RedisQueue struct {
	client        *redis.Client
	config        RedisConfig
	subscriptions map[string]context.CancelFunc
	mu            sync.RWMutex
}

// newRedisQueue creates a new Redis Streams queue instance
func newRedisQueue(cfg RedisConfig) (*RedisQueue, error) {
	// Parse URL or use defaults
	opts, err := redis.ParseURL(cfg.URL)
	if err != nil {
		// Fallback to simple options
		opts = &redis.Options{
			Addr:     cfg.URL,
			Password: cfg.Password,
			DB:       cfg.DB,
		}
	}

	client := redis.NewClient(opts)

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	// Apply defaults
	if cfg.Stream == "" {
		cfg.Stream = "soltix"
	}
	if cfg.Group == "" {
		cfg.Group = "soltix-group"
	}
	if cfg.Consumer == "" {
		hostname, _ := os.Hostname()
		if hostname == "" {
			hostname = "consumer-1"
		}
		cfg.Consumer = hostname
	}

	return &RedisQueue{
		client:        client,
		config:        cfg,
		subscriptions: make(map[string]context.CancelFunc),
	}, nil
}

// streamName converts a subject to a Redis stream name
func (q *RedisQueue) streamName(subject string) string {
	return fmt.Sprintf("%s:%s", q.config.Stream, subject)
}

// Publish publishes a message to a Redis stream
func (q *RedisQueue) Publish(ctx context.Context, subject string, data []byte) error {
	stream := q.streamName(subject)

	_, err := q.client.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		ID:     "*", // Auto-generate ID
		Values: map[string]interface{}{
			"data": data,
		},
	}).Result()
	if err != nil {
		return fmt.Errorf("failed to publish to Redis stream %s: %w", stream, err)
	}

	return nil
}

// PublishBatch publishes multiple messages using Redis pipeline
func (q *RedisQueue) PublishBatch(ctx context.Context, messages []BatchMessage) (int, error) {
	if len(messages) == 0 {
		return 0, nil
	}

	pipe := q.client.Pipeline()

	for _, msg := range messages {
		stream := q.streamName(msg.Subject)
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: stream,
			ID:     "*",
			Values: map[string]interface{}{
				"data": msg.Data,
			},
		})
	}

	cmds, err := pipe.Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to execute batch publish: %w", err)
	}

	// Count successful publishes
	successCount := 0
	for _, cmd := range cmds {
		if cmd.Err() == nil {
			successCount++
		}
	}

	return successCount, nil
}

// Subscribe subscribes to a Redis stream with consumer group
func (q *RedisQueue) Subscribe(subject string, handler MessageHandler) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, exists := q.subscriptions[subject]; exists {
		return fmt.Errorf("already subscribed to subject: %s", subject)
	}

	stream := q.streamName(subject)
	ctx, cancel := context.WithCancel(context.Background())

	// Create consumer group if not exists
	err := q.client.XGroupCreateMkStream(ctx, stream, q.config.Group, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		cancel()
		return fmt.Errorf("failed to create consumer group: %w", err)
	}

	// Start reading messages in background
	go q.readStream(ctx, stream, handler)

	q.subscriptions[subject] = cancel
	return nil
}

// readStream continuously reads messages from a Redis stream
func (q *RedisQueue) readStream(ctx context.Context, stream string, handler MessageHandler) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Read new messages
		streams, err := q.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    q.config.Group,
			Consumer: q.config.Consumer,
			Streams:  []string{stream, ">"},
			Count:    100,
			Block:    5 * time.Second,
		}).Result()
		if err != nil {
			if err == redis.Nil {
				continue // No messages, continue polling
			}
			// Log error and continue
			continue
		}

		for _, s := range streams {
			for _, msg := range s.Messages {
				data, ok := msg.Values["data"].(string)
				if !ok {
					// ACK invalid message
					q.client.XAck(ctx, stream, q.config.Group, msg.ID)
					continue
				}

				// Process message
				if err := handler([]byte(data)); err != nil {
					// On error, don't ACK - message will be redelivered
					continue
				}

				// ACK successful processing
				q.client.XAck(ctx, stream, q.config.Group, msg.ID)
			}
		}
	}
}

// Unsubscribe unsubscribes from a subject
func (q *RedisQueue) Unsubscribe(subject string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	cancel, exists := q.subscriptions[subject]
	if !exists {
		return fmt.Errorf("not subscribed to subject: %s", subject)
	}

	cancel()
	delete(q.subscriptions, subject)
	return nil
}

// Close closes the Redis connection
func (q *RedisQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Cancel all subscriptions
	for subject, cancel := range q.subscriptions {
		cancel()
		delete(q.subscriptions, subject)
	}

	return q.client.Close()
}
