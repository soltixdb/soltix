package subscriber

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/soltixdb/soltix/internal/logging"
)

var redisLog = logging.Global().With("component", "subscriber.redis")

// RedisSubscriber implements Subscriber for Redis Streams
type RedisSubscriber struct {
	client        *redis.Client
	streamPrefix  string // Stream prefix to match queue publisher
	consumerGroup string
	consumerID    string
	subscriptions map[string]context.CancelFunc
	mu            sync.RWMutex
}

// NewRedisSubscriber creates a new Redis Streams subscriber
func NewRedisSubscriber(addr, password string, db int, streamPrefix, consumerGroup, consumerID string) (*RedisSubscriber, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		PoolSize:     10,
		MinIdleConns: 2,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	// Apply defaults
	if streamPrefix == "" {
		streamPrefix = "soltix" // Default to match queue publisher
	}

	return &RedisSubscriber{
		client:        client,
		streamPrefix:  streamPrefix,
		consumerGroup: consumerGroup,
		consumerID:    consumerID,
		subscriptions: make(map[string]context.CancelFunc),
	}, nil
}

// Subscribe subscribes to a stream with the given handler
func (s *RedisSubscriber) Subscribe(ctx context.Context, subject string, handler MessageHandler) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	streamName := s.streamName(subject)

	if _, exists := s.subscriptions[streamName]; exists {
		return fmt.Errorf("already subscribed to stream: %s", streamName)
	}

	// Create consumer group if it doesn't exist
	err := s.client.XGroupCreateMkStream(ctx, streamName, s.consumerGroup, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return fmt.Errorf("failed to create consumer group: %w", err)
	}

	// Create cancellable context for this subscription
	subCtx, cancel := context.WithCancel(ctx)
	s.subscriptions[streamName] = cancel

	// Start consuming in a goroutine
	go s.consume(subCtx, streamName, subject, handler)

	redisLog.Info("Subscribed to Redis stream", "stream", streamName, "group", s.consumerGroup, "consumer", s.consumerID)
	return nil
}

// consume reads messages from the stream and processes them
func (s *RedisSubscriber) consume(ctx context.Context, streamName, subject string, handler MessageHandler) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Read new messages
		streams, err := s.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    s.consumerGroup,
			Consumer: s.consumerID,
			Streams:  []string{streamName, ">"},
			Count:    100,
			Block:    time.Second,
		}).Result()
		if err != nil {
			if err == redis.Nil || ctx.Err() != nil {
				continue
			}
			redisLog.Error("Failed to read from stream", "stream", streamName, "error", err)
			time.Sleep(time.Second)
			continue
		}

		for _, stream := range streams {
			for _, message := range stream.Messages {
				data, ok := message.Values["data"].(string)
				if !ok {
					redisLog.Warn("Invalid message format", "stream", streamName, "id", message.ID)
					s.client.XAck(ctx, streamName, s.consumerGroup, message.ID)
					continue
				}

				if err := handler(ctx, subject, []byte(data)); err != nil {
					redisLog.Error("Failed to handle message", "stream", streamName, "id", message.ID, "error", err)
					// Don't ACK - message will be redelivered
					continue
				}

				// ACK the message
				if err := s.client.XAck(ctx, streamName, s.consumerGroup, message.ID).Err(); err != nil {
					redisLog.Error("Failed to ACK message", "stream", streamName, "id", message.ID, "error", err)
				}
			}
		}
	}
}

// streamName converts a subject to a Redis stream name
// Format: {streamPrefix}:{subject} to match queue publisher
func (s *RedisSubscriber) streamName(subject string) string {
	return fmt.Sprintf("%s:%s", s.streamPrefix, subject)
}

// Unsubscribe unsubscribes from a stream
func (s *RedisSubscriber) Unsubscribe(subject string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	streamName := s.streamName(subject)
	cancel, exists := s.subscriptions[streamName]
	if !exists {
		return fmt.Errorf("not subscribed to stream: %s", streamName)
	}

	cancel()
	delete(s.subscriptions, streamName)
	redisLog.Info("Unsubscribed from Redis stream", "stream", streamName)
	return nil
}

// Close closes all subscriptions and the connection
func (s *RedisSubscriber) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for streamName, cancel := range s.subscriptions {
		cancel()
		redisLog.Debug("Cancelled subscription", "stream", streamName)
	}
	s.subscriptions = make(map[string]context.CancelFunc)

	if err := s.client.Close(); err != nil {
		return fmt.Errorf("failed to close Redis client: %w", err)
	}

	redisLog.Info("Redis subscriber closed")
	return nil
}
