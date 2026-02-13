package queue

import (
	"context"
	"os"
	"testing"
	"time"
)

// Test helper: check if Kafka is available
func isKafkaAvailable() bool {
	brokers := getKafkaBrokers()
	if len(brokers) == 0 {
		return false
	}
	// Simple check - try to create a queue
	// In real scenario, you'd try to connect
	return os.Getenv("KAFKA_TEST") == "1"
}

// Test helper: get Kafka brokers from env or default
func getKafkaBrokers() []string {
	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		return []string{brokers}
	}
	return []string{"localhost:9092"}
}

func TestNewKafkaQueue(t *testing.T) {
	cfg := KafkaConfig{
		Brokers: []string{"localhost:9092"},
		GroupID: "test-group",
	}

	q, err := NewKafkaQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create Kafka queue: %v", err)
	}
	defer func() { _ = q.Close() }()

	if q == nil {
		t.Fatal("Kafka queue should not be nil")
	}
}

func TestNewKafkaQueue_NoBrokers(t *testing.T) {
	cfg := KafkaConfig{
		Brokers: []string{},
	}

	_, err := NewKafkaQueue(cfg)
	if err == nil {
		t.Fatal("Expected error when no brokers configured")
	}
}

func TestNewKafkaQueue_NilBrokers(t *testing.T) {
	cfg := KafkaConfig{
		Brokers: nil,
	}

	_, err := NewKafkaQueue(cfg)
	if err == nil {
		t.Fatal("Expected error when brokers is nil")
	}
}

func TestNewKafkaQueue_DefaultGroupID(t *testing.T) {
	cfg := KafkaConfig{
		Brokers: []string{"localhost:9092"},
		// GroupID is empty
	}

	q, err := NewKafkaQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create Kafka queue: %v", err)
	}
	defer func() { _ = q.Close() }()

	if q.config.GroupID != "soltix-group" {
		t.Errorf("Expected default GroupID 'soltix-group', got '%s'", q.config.GroupID)
	}
}

func TestKafkaQueue_Publish(t *testing.T) {
	if !isKafkaAvailable() {
		t.Skip("Kafka not available, skipping test")
	}

	cfg := KafkaConfig{
		Brokers: getKafkaBrokers(),
	}

	q, err := NewKafkaQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create Kafka queue: %v", err)
	}
	defer func() { _ = q.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = q.Publish(ctx, "test.topic", []byte("test message"))
	if err != nil {
		t.Fatalf("Failed to publish: %v", err)
	}
}

func TestKafkaQueue_GetOrCreateWriter(t *testing.T) {
	cfg := KafkaConfig{
		Brokers: []string{"localhost:9092"},
	}

	q, err := NewKafkaQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create Kafka queue: %v", err)
	}
	defer func() { _ = q.Close() }()

	// First call creates writer
	w1 := q.getOrCreateWriter("topic1")
	if w1 == nil {
		t.Fatal("Writer should not be nil")
	}

	// Second call returns same writer
	w2 := q.getOrCreateWriter("topic1")
	if w1 != w2 {
		t.Error("Should return same writer for same topic")
	}

	// Different topic gets different writer
	w3 := q.getOrCreateWriter("topic2")
	if w1 == w3 {
		t.Error("Different topics should have different writers")
	}

	if len(q.writers) != 2 {
		t.Errorf("Expected 2 writers, got %d", len(q.writers))
	}
}

func TestKafkaQueue_PublishBatch(t *testing.T) {
	if !isKafkaAvailable() {
		t.Skip("Kafka not available, skipping test")
	}

	cfg := KafkaConfig{
		Brokers: getKafkaBrokers(),
	}

	q, err := NewKafkaQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create Kafka queue: %v", err)
	}
	defer func() { _ = q.Close() }()

	messages := []BatchMessage{
		{Subject: "topic1", Data: []byte("msg1")},
		{Subject: "topic1", Data: []byte("msg2")},
		{Subject: "topic2", Data: []byte("msg3")},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	count, err := q.PublishBatch(ctx, messages)
	if err != nil {
		t.Fatalf("Failed to publish batch: %v", err)
	}
	if count != 3 {
		t.Errorf("Expected 3 messages published, got %d", count)
	}
}

func TestKafkaQueue_PublishBatch_Empty(t *testing.T) {
	cfg := KafkaConfig{
		Brokers: []string{"localhost:9092"},
	}

	q, err := NewKafkaQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create Kafka queue: %v", err)
	}
	defer func() { _ = q.Close() }()

	ctx := context.Background()
	count, err := q.PublishBatch(ctx, []BatchMessage{})
	if err != nil {
		t.Fatalf("Empty batch should not error: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0 messages, got %d", count)
	}
}

func TestKafkaQueue_Subscribe(t *testing.T) {
	if !isKafkaAvailable() {
		t.Skip("Kafka not available, skipping test")
	}

	cfg := KafkaConfig{
		Brokers: getKafkaBrokers(),
		GroupID: "test-group",
	}

	q, err := NewKafkaQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create Kafka queue: %v", err)
	}
	defer func() { _ = q.Close() }()

	err = q.Subscribe("test.topic", func(data []byte) error {
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	// Verify subscription was added
	q.mu.RLock()
	_, exists := q.subscriptions["test.topic"]
	_, readerExists := q.readers["test.topic"]
	q.mu.RUnlock()

	if !exists {
		t.Error("Subscription should exist")
	}
	if !readerExists {
		t.Error("Reader should exist")
	}
}

func TestKafkaQueue_Subscribe_DoubleSubscribe(t *testing.T) {
	if !isKafkaAvailable() {
		t.Skip("Kafka not available, skipping test")
	}

	cfg := KafkaConfig{
		Brokers: getKafkaBrokers(),
	}

	q, err := NewKafkaQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create Kafka queue: %v", err)
	}
	defer func() { _ = q.Close() }()

	err = q.Subscribe("test.topic", func(data []byte) error {
		return nil
	})
	if err != nil {
		t.Fatalf("First subscribe failed: %v", err)
	}

	err = q.Subscribe("test.topic", func(data []byte) error {
		return nil
	})
	if err == nil {
		t.Fatal("Expected error for double subscribe")
	}
}

func TestKafkaQueue_Unsubscribe_NotSubscribed(t *testing.T) {
	cfg := KafkaConfig{
		Brokers: []string{"localhost:9092"},
	}

	q, err := NewKafkaQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create Kafka queue: %v", err)
	}
	defer func() { _ = q.Close() }()

	err = q.Unsubscribe("not.subscribed")
	if err == nil {
		t.Fatal("Expected error for unsubscribing from non-subscribed topic")
	}
}

func TestKafkaQueue_Close(t *testing.T) {
	cfg := KafkaConfig{
		Brokers: []string{"localhost:9092"},
	}

	q, err := NewKafkaQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create Kafka queue: %v", err)
	}

	// Add a fake subscription to test cleanup
	q.mu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	q.subscriptions["test.topic"] = cancel
	q.mu.Unlock()

	// Verify subscription exists
	if len(q.subscriptions) != 1 {
		t.Error("Expected 1 subscription before close")
	}

	// Close should clean up
	err = q.Close()
	if err != nil {
		t.Fatalf("Failed to close: %v", err)
	}

	// Verify subscriptions cleared
	if len(q.subscriptions) != 0 {
		t.Error("Subscriptions should be empty after close")
	}

	// Verify context was cancelled
	select {
	case <-ctx.Done():
		// Success - context was cancelled
	default:
		t.Error("Context should be cancelled after close")
	}
}

func TestKafkaQueue_Close_Empty(t *testing.T) {
	cfg := KafkaConfig{
		Brokers: []string{"localhost:9092"},
	}

	q, err := NewKafkaQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create Kafka queue: %v", err)
	}

	// Close empty queue should work
	err = q.Close()
	if err != nil {
		t.Fatalf("Failed to close empty queue: %v", err)
	}
}

func TestKafkaQueue_MultipleBrokers(t *testing.T) {
	cfg := KafkaConfig{
		Brokers: []string{"broker1:9092", "broker2:9092", "broker3:9092"},
		GroupID: "multi-broker-group",
	}

	q, err := NewKafkaQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create Kafka queue: %v", err)
	}
	defer func() { _ = q.Close() }()

	if len(q.config.Brokers) != 3 {
		t.Errorf("Expected 3 brokers, got %d", len(q.config.Brokers))
	}
}

func TestKafkaConfig_Validation(t *testing.T) {
	tests := []struct {
		name        string
		cfg         KafkaConfig
		expectError bool
	}{
		{
			name: "valid config",
			cfg: KafkaConfig{
				Brokers: []string{"localhost:9092"},
				GroupID: "test-group",
			},
			expectError: false,
		},
		{
			name: "empty brokers",
			cfg: KafkaConfig{
				Brokers: []string{},
				GroupID: "test-group",
			},
			expectError: true,
		},
		{
			name: "nil brokers",
			cfg: KafkaConfig{
				Brokers: nil,
				GroupID: "test-group",
			},
			expectError: true,
		},
		{
			name: "empty group id with default",
			cfg: KafkaConfig{
				Brokers: []string{"localhost:9092"},
				GroupID: "",
			},
			expectError: false, // Should use default
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewKafkaQueue(tt.cfg)
			if tt.expectError && err == nil {
				t.Error("Expected error but got nil")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

func TestKafkaQueue_ConcurrentClose(t *testing.T) {
	cfg := KafkaConfig{
		Brokers: []string{"localhost:9092"},
	}

	q, err := NewKafkaQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create Kafka queue: %v", err)
	}

	// Add some subscriptions
	for i := 0; i < 5; i++ {
		q.mu.Lock()
		_, cancel := context.WithCancel(context.Background())
		q.subscriptions[string(rune('a'+i))] = cancel
		q.mu.Unlock()
	}

	// Close should handle all subscriptions
	err = q.Close()
	if err != nil {
		t.Fatalf("Failed to close: %v", err)
	}

	if len(q.subscriptions) != 0 {
		t.Error("All subscriptions should be cleaned up")
	}
}

func TestKafkaQueue_ConfigDefaults(t *testing.T) {
	cfg := KafkaConfig{
		Brokers: []string{"localhost:9092"},
	}

	q, err := NewKafkaQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create Kafka queue: %v", err)
	}
	defer func() { _ = q.Close() }()

	// Check all defaults are applied
	if q.config.GroupID != "soltix-group" {
		t.Errorf("Expected GroupID 'soltix-group', got '%s'", q.config.GroupID)
	}
	if q.config.BatchSize != 100 {
		t.Errorf("Expected BatchSize 100, got %d", q.config.BatchSize)
	}
	if q.config.BatchTimeout != 10*time.Millisecond {
		t.Errorf("Expected BatchTimeout 10ms, got %v", q.config.BatchTimeout)
	}
	if q.config.MaxRetries != 3 {
		t.Errorf("Expected MaxRetries 3, got %d", q.config.MaxRetries)
	}
	if q.config.RetryBackoff != 100*time.Millisecond {
		t.Errorf("Expected RetryBackoff 100ms, got %v", q.config.RetryBackoff)
	}
	if q.config.CommitRetries != 3 {
		t.Errorf("Expected CommitRetries 3, got %d", q.config.CommitRetries)
	}
}

func TestKafkaQueue_Stats(t *testing.T) {
	cfg := KafkaConfig{
		Brokers: []string{"localhost:9092"},
	}

	q, err := NewKafkaQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create Kafka queue: %v", err)
	}
	defer func() { _ = q.Close() }()

	// Stats for non-existent topic should return empty stats
	stats := q.Stats("non.existent")
	if stats.Topic != "" {
		t.Error("Stats for non-existent topic should be empty")
	}

	// Create a writer by calling getOrCreateWriter
	q.getOrCreateWriter("test.topic")

	// Now stats should return the writer's stats
	stats = q.Stats("test.topic")
	if stats.Topic != "test.topic" {
		t.Errorf("Expected topic 'test.topic', got '%s'", stats.Topic)
	}
}

func TestKafkaQueue_ReaderStats(t *testing.T) {
	cfg := KafkaConfig{
		Brokers: []string{"localhost:9092"},
	}

	q, err := NewKafkaQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create Kafka queue: %v", err)
	}
	defer func() { _ = q.Close() }()

	// Stats for non-existent topic should return empty stats
	stats := q.ReaderStats("non.existent")
	if stats.Topic != "" {
		t.Error("ReaderStats for non-existent topic should be empty")
	}
}

func TestKafkaQueue_Close_WithWriters(t *testing.T) {
	cfg := KafkaConfig{
		Brokers: []string{"localhost:9092"},
	}

	q, err := NewKafkaQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create Kafka queue: %v", err)
	}

	// Create some writers
	q.getOrCreateWriter("topic1")
	q.getOrCreateWriter("topic2")
	q.getOrCreateWriter("topic3")

	if len(q.writers) != 3 {
		t.Errorf("Expected 3 writers, got %d", len(q.writers))
	}

	// Close should clean up all writers
	err = q.Close()
	if err != nil {
		t.Fatalf("Failed to close: %v", err)
	}

	if len(q.writers) != 0 {
		t.Error("Writers should be empty after close")
	}
}

// Benchmark tests for Kafka
func BenchmarkKafkaQueue_Create(b *testing.B) {
	cfg := KafkaConfig{
		Brokers: []string{"localhost:9092"},
		GroupID: "bench-group",
	}

	for i := 0; i < b.N; i++ {
		q, err := NewKafkaQueue(cfg)
		if err != nil {
			b.Fatalf("Failed to create queue: %v", err)
		}
		_ = q.Close()
	}
}

func BenchmarkKafkaQueue_GetOrCreateWriter(b *testing.B) {
	cfg := KafkaConfig{
		Brokers: []string{"localhost:9092"},
	}

	q, err := NewKafkaQueue(cfg)
	if err != nil {
		b.Fatalf("Failed to create queue: %v", err)
	}
	defer func() { _ = q.Close() }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.getOrCreateWriter("bench.topic")
	}
}
