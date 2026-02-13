package queue

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Test helper: check if Redis is available
func isRedisAvailable() bool {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	return client.Ping(ctx).Err() == nil
}

// Test helper: get Redis URL from env or default
func getRedisURL() string {
	if url := os.Getenv("REDIS_URL"); url != "" {
		return url
	}
	return "redis://localhost:6379"
}

// Test helper: cleanup Redis stream
func cleanupRedisStream(t *testing.T, client *redis.Client, stream string) {
	ctx := context.Background()
	client.Del(ctx, stream)
}

func TestNewRedisQueue(t *testing.T) {
	if !isRedisAvailable() {
		t.Skip("Redis not available, skipping test")
	}

	cfg := RedisConfig{
		URL:    getRedisURL(),
		Stream: "test-soltix",
		Group:  "test-group",
	}

	q, err := NewRedisQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create Redis queue: %v", err)
	}
	defer func() { _ = q.Close() }()

	if q.client == nil {
		t.Fatal("Redis client should not be nil")
	}
}

func TestNewRedisQueue_InvalidURL(t *testing.T) {
	cfg := RedisConfig{
		URL: "invalid-redis-url:9999",
	}

	_, err := NewRedisQueue(cfg)
	if err == nil {
		t.Fatal("Expected error for invalid Redis URL")
	}
}

func TestNewRedisQueue_Defaults(t *testing.T) {
	if !isRedisAvailable() {
		t.Skip("Redis not available, skipping test")
	}

	cfg := RedisConfig{
		URL: getRedisURL(),
		// Leave Stream, Group, Consumer empty to test defaults
	}

	q, err := NewRedisQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create Redis queue: %v", err)
	}
	defer func() { _ = q.Close() }()

	if q.config.Stream != "soltix" {
		t.Errorf("Expected default stream 'soltix', got '%s'", q.config.Stream)
	}
	if q.config.Group != "soltix-group" {
		t.Errorf("Expected default group 'soltix-group', got '%s'", q.config.Group)
	}
	if q.config.Consumer == "" {
		t.Error("Consumer should have a default value")
	}
}

func TestRedisQueue_Publish(t *testing.T) {
	if !isRedisAvailable() {
		t.Skip("Redis not available, skipping test")
	}

	cfg := RedisConfig{
		URL:    getRedisURL(),
		Stream: "test-publish",
	}

	q, err := NewRedisQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create Redis queue: %v", err)
	}
	defer func() { _ = q.Close() }()

	// Cleanup
	defer cleanupRedisStream(t, q.client, q.streamName("test.subject"))

	ctx := context.Background()
	err = q.Publish(ctx, "test.subject", []byte("test message"))
	if err != nil {
		t.Fatalf("Failed to publish: %v", err)
	}

	// Verify message was added to stream
	stream := q.streamName("test.subject")
	msgs, err := q.client.XRange(ctx, stream, "-", "+").Result()
	if err != nil {
		t.Fatalf("Failed to read stream: %v", err)
	}

	if len(msgs) != 1 {
		t.Errorf("Expected 1 message in stream, got %d", len(msgs))
	}
}

func TestRedisQueue_PublishBatch(t *testing.T) {
	if !isRedisAvailable() {
		t.Skip("Redis not available, skipping test")
	}

	cfg := RedisConfig{
		URL:    getRedisURL(),
		Stream: "test-batch",
	}

	q, err := NewRedisQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create Redis queue: %v", err)
	}
	defer func() { _ = q.Close() }()

	// Cleanup
	defer cleanupRedisStream(t, q.client, q.streamName("batch.1"))
	defer cleanupRedisStream(t, q.client, q.streamName("batch.2"))

	messages := []BatchMessage{
		{Subject: "batch.1", Data: []byte("msg1")},
		{Subject: "batch.1", Data: []byte("msg2")},
		{Subject: "batch.2", Data: []byte("msg3")},
	}

	ctx := context.Background()
	count, err := q.PublishBatch(ctx, messages)
	if err != nil {
		t.Fatalf("Failed to batch publish: %v", err)
	}

	if count != 3 {
		t.Errorf("Expected 3 messages published, got %d", count)
	}
}

func TestRedisQueue_PublishBatch_Empty(t *testing.T) {
	if !isRedisAvailable() {
		t.Skip("Redis not available, skipping test")
	}

	cfg := RedisConfig{
		URL:    getRedisURL(),
		Stream: "test-empty-batch",
	}

	q, err := NewRedisQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create Redis queue: %v", err)
	}
	defer func() { _ = q.Close() }()

	ctx := context.Background()
	count, err := q.PublishBatch(ctx, []BatchMessage{})
	if err != nil {
		t.Fatalf("Empty batch should not error: %v", err)
	}

	if count != 0 {
		t.Errorf("Expected 0 messages published, got %d", count)
	}
}

func TestRedisQueue_Subscribe(t *testing.T) {
	if !isRedisAvailable() {
		t.Skip("Redis not available, skipping test")
	}

	cfg := RedisConfig{
		URL:      getRedisURL(),
		Stream:   "test-subscribe",
		Group:    fmt.Sprintf("test-group-%d", time.Now().UnixNano()),
		Consumer: "test-consumer",
	}

	q, err := NewRedisQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create Redis queue: %v", err)
	}
	defer func() { _ = q.Close() }()

	subject := "sub.test"
	stream := q.streamName(subject)
	defer cleanupRedisStream(t, q.client, stream)

	var received []byte
	var wg sync.WaitGroup
	wg.Add(1)

	err = q.Subscribe(subject, func(data []byte) error {
		received = data
		wg.Done()
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	// Give subscriber time to start
	time.Sleep(100 * time.Millisecond)

	// Publish message
	ctx := context.Background()
	err = q.Publish(ctx, subject, []byte("hello redis"))
	if err != nil {
		t.Fatalf("Failed to publish: %v", err)
	}

	// Wait with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for message")
	}

	if string(received) != "hello redis" {
		t.Errorf("Expected 'hello redis', got '%s'", received)
	}
}

func TestRedisQueue_Subscribe_MultipleMessages(t *testing.T) {
	if !isRedisAvailable() {
		t.Skip("Redis not available, skipping test")
	}

	cfg := RedisConfig{
		URL:      getRedisURL(),
		Stream:   "test-multi",
		Group:    fmt.Sprintf("test-group-%d", time.Now().UnixNano()),
		Consumer: "test-consumer",
	}

	q, err := NewRedisQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create Redis queue: %v", err)
	}
	defer func() { _ = q.Close() }()

	subject := "multi.test"
	stream := q.streamName(subject)
	defer cleanupRedisStream(t, q.client, stream)

	var messageCount int32
	expectedCount := 10

	err = q.Subscribe(subject, func(data []byte) error {
		atomic.AddInt32(&messageCount, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Publish multiple messages
	ctx := context.Background()
	for i := 0; i < expectedCount; i++ {
		err = q.Publish(ctx, subject, []byte(fmt.Sprintf("msg-%d", i)))
		if err != nil {
			t.Fatalf("Failed to publish message %d: %v", i, err)
		}
	}

	// Wait for all messages
	timeout := time.After(15 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			t.Fatalf("Timeout: received %d of %d messages", atomic.LoadInt32(&messageCount), expectedCount)
		case <-ticker.C:
			if int(atomic.LoadInt32(&messageCount)) >= expectedCount {
				return
			}
		}
	}
}

func TestRedisQueue_Subscribe_DoubleSubscribe(t *testing.T) {
	if !isRedisAvailable() {
		t.Skip("Redis not available, skipping test")
	}

	cfg := RedisConfig{
		URL:    getRedisURL(),
		Stream: "test-double-sub",
		Group:  fmt.Sprintf("test-group-%d", time.Now().UnixNano()),
	}

	q, err := NewRedisQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create Redis queue: %v", err)
	}
	defer func() { _ = q.Close() }()

	subject := "double.sub"

	// First subscribe
	err = q.Subscribe(subject, func(data []byte) error {
		return nil
	})
	if err != nil {
		t.Fatalf("First subscribe failed: %v", err)
	}

	// Second subscribe should fail
	err = q.Subscribe(subject, func(data []byte) error {
		return nil
	})
	if err == nil {
		t.Fatal("Expected error for double subscribe")
	}
}

func TestRedisQueue_Unsubscribe(t *testing.T) {
	if !isRedisAvailable() {
		t.Skip("Redis not available, skipping test")
	}

	cfg := RedisConfig{
		URL:    getRedisURL(),
		Stream: "test-unsub",
		Group:  fmt.Sprintf("test-group-%d", time.Now().UnixNano()),
	}

	q, err := NewRedisQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create Redis queue: %v", err)
	}
	defer func() { _ = q.Close() }()

	subject := "unsub.test"

	err = q.Subscribe(subject, func(data []byte) error {
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	err = q.Unsubscribe(subject)
	if err != nil {
		t.Fatalf("Failed to unsubscribe: %v", err)
	}

	// Double unsubscribe should error
	err = q.Unsubscribe(subject)
	if err == nil {
		t.Fatal("Expected error for double unsubscribe")
	}
}

func TestRedisQueue_Unsubscribe_NotSubscribed(t *testing.T) {
	if !isRedisAvailable() {
		t.Skip("Redis not available, skipping test")
	}

	cfg := RedisConfig{
		URL:    getRedisURL(),
		Stream: "test-unsub-not",
	}

	q, err := NewRedisQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create Redis queue: %v", err)
	}
	defer func() { _ = q.Close() }()

	err = q.Unsubscribe("not.subscribed")
	if err == nil {
		t.Fatal("Expected error for unsubscribing from non-subscribed subject")
	}
}

func TestRedisQueue_Close(t *testing.T) {
	if !isRedisAvailable() {
		t.Skip("Redis not available, skipping test")
	}

	cfg := RedisConfig{
		URL:    getRedisURL(),
		Stream: "test-close",
		Group:  fmt.Sprintf("test-group-%d", time.Now().UnixNano()),
	}

	q, err := NewRedisQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create Redis queue: %v", err)
	}

	// Subscribe to something
	err = q.Subscribe("close.test", func(data []byte) error {
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	// Close should clean up subscriptions
	err = q.Close()
	if err != nil {
		t.Fatalf("Failed to close: %v", err)
	}

	// Verify subscriptions are cleared
	if len(q.subscriptions) != 0 {
		t.Error("Subscriptions should be empty after close")
	}
}

func TestRedisQueue_StreamName(t *testing.T) {
	q := &RedisQueue{
		config: RedisConfig{
			Stream: "myprefix",
		},
	}

	tests := []struct {
		subject  string
		expected string
	}{
		{"test", "myprefix:test"},
		{"metrics.cpu", "myprefix:metrics.cpu"},
		{"a.b.c", "myprefix:a.b.c"},
	}

	for _, tt := range tests {
		result := q.streamName(tt.subject)
		if result != tt.expected {
			t.Errorf("streamName(%s) = %s, expected %s", tt.subject, result, tt.expected)
		}
	}
}

func TestRedisQueue_ConcurrentPublish(t *testing.T) {
	if !isRedisAvailable() {
		t.Skip("Redis not available, skipping test")
	}

	cfg := RedisConfig{
		URL:    getRedisURL(),
		Stream: "test-concurrent",
	}

	q, err := NewRedisQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create Redis queue: %v", err)
	}
	defer func() { _ = q.Close() }()

	subject := "concurrent.test"
	stream := q.streamName(subject)
	defer cleanupRedisStream(t, q.client, stream)

	ctx := context.Background()
	numGoroutines := 10
	messagesPerGoroutine := 100

	var wg sync.WaitGroup
	var errCount int32

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < messagesPerGoroutine; j++ {
				err := q.Publish(ctx, subject, []byte(fmt.Sprintf("worker-%d-msg-%d", workerID, j)))
				if err != nil {
					atomic.AddInt32(&errCount, 1)
				}
			}
		}(i)
	}

	wg.Wait()

	if errCount > 0 {
		t.Errorf("Had %d errors during concurrent publish", errCount)
	}

	// Verify message count
	msgs, err := q.client.XLen(ctx, stream).Result()
	if err != nil {
		t.Fatalf("Failed to get stream length: %v", err)
	}

	expected := int64(numGoroutines * messagesPerGoroutine)
	if msgs != expected {
		t.Errorf("Expected %d messages in stream, got %d", expected, msgs)
	}
}

func TestRedisConfig_Defaults(t *testing.T) {
	cfg := RedisConfig{}

	if cfg.DB != 0 {
		t.Errorf("Default DB should be 0, got %d", cfg.DB)
	}
	if cfg.Stream != "" {
		t.Error("Stream should be empty before initialization")
	}
}
