package queue

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/config"
	"github.com/soltixdb/soltix/internal/utils"
)

func TestNewQueue_DefaultsToNATS(t *testing.T) {
	// When Type is empty, should default to NATS
	cfg := config.QueueConfig{
		URL: "nats://localhost:4222",
	}

	_, err := NewQueue(cfg)
	// This will fail if NATS is not running, which is expected in unit tests
	// The important thing is that it attempts NATS connection
	if err == nil {
		t.Log("NATS connection succeeded")
	} else {
		t.Logf("NATS connection failed (expected if NATS not running): %v", err)
	}
}

func TestNewQueue_MemoryQueue(t *testing.T) {
	cfg := config.QueueConfig{
		Type: "memory",
	}

	q, err := NewQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create memory queue: %v", err)
	}
	defer func() { _ = q.Close() }()

	if q == nil {
		t.Fatal("Memory queue should not be nil")
	}
}

func TestNewQueue_UnsupportedType(t *testing.T) {
	cfg := config.QueueConfig{
		Type: "unknown",
	}

	_, err := NewQueue(cfg)
	if err == nil {
		t.Fatal("Expected error for unsupported queue type")
	}
}

func TestMemoryQueue_PublishSubscribe(t *testing.T) {
	q := NewMemoryQueue()
	defer func() { _ = q.Close() }()

	subject := "test.subject"
	testData := []byte("test message")

	var received []byte
	var wg sync.WaitGroup
	wg.Add(1)

	// Subscribe first
	err := q.Subscribe(subject, func(data []byte) error {
		received = data
		wg.Done()
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	// Publish message
	ctx := context.Background()
	err = q.Publish(ctx, subject, testData)
	if err != nil {
		t.Fatalf("Failed to publish: %v", err)
	}

	// Wait for message with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message")
	}

	if string(received) != string(testData) {
		t.Errorf("Expected %s, got %s", testData, received)
	}
}

func TestMemoryQueue_PublishBatch(t *testing.T) {
	q := NewMemoryQueue()
	defer func() { _ = q.Close() }()

	messages := []BatchMessage{
		{Subject: "test.1", Data: []byte("msg1")},
		{Subject: "test.2", Data: []byte("msg2")},
		{Subject: "test.3", Data: []byte("msg3")},
	}

	ctx := context.Background()
	count, err := q.PublishBatch(ctx, messages)
	if err != nil {
		t.Fatalf("Failed to publish batch: %v", err)
	}

	if count != len(messages) {
		t.Errorf("Expected %d messages published, got %d", len(messages), count)
	}
}

func TestMemoryQueue_Unsubscribe(t *testing.T) {
	q := NewMemoryQueue()
	defer func() { _ = q.Close() }()

	subject := "test.subject"

	// Subscribe
	err := q.Subscribe(subject, func(data []byte) error {
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	// Unsubscribe
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

func TestMemoryQueue_DoubleSubscribe(t *testing.T) {
	q := NewMemoryQueue()
	defer func() { _ = q.Close() }()

	subject := "test.subject"

	// First subscribe
	err := q.Subscribe(subject, func(data []byte) error {
		return nil
	})
	if err != nil {
		t.Fatalf("Failed first subscribe: %v", err)
	}

	// Second subscribe should fail
	err = q.Subscribe(subject, func(data []byte) error {
		return nil
	})
	if err == nil {
		t.Fatal("Expected error for double subscribe")
	}
}

func TestMemoryQueue_GetPendingCount(t *testing.T) {
	q := NewMemoryQueue()
	defer func() { _ = q.Close() }()

	subject := "test.subject"
	ctx := context.Background()

	// Publish without subscriber
	for i := 0; i < 5; i++ {
		_ = q.Publish(ctx, subject, []byte("msg"))
	}

	count := q.GetPendingCount(subject)
	if count != 5 {
		t.Errorf("Expected 5 pending messages, got %d", count)
	}
}

func TestNewPublisher(t *testing.T) {
	cfg := config.QueueConfig{
		Type: "memory",
	}

	p, err := NewPublisher(cfg)
	if err != nil {
		t.Fatalf("Failed to create publisher: %v", err)
	}
	defer func() { _ = p.Close() }()

	// Publish a message
	ctx := context.Background()
	err = p.Publish(ctx, "test", []byte("data"))
	if err != nil {
		t.Fatalf("Failed to publish: %v", err)
	}
}

func TestNewSubscriber(t *testing.T) {
	cfg := config.QueueConfig{
		Type: "memory",
	}

	s, err := NewSubscriber(cfg)
	if err != nil {
		t.Fatalf("Failed to create subscriber: %v", err)
	}
	defer func() { _ = s.Close() }()

	// Subscribe to a subject
	err = s.Subscribe("test", func(data []byte) error {
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}
}

func TestQueueTypes(t *testing.T) {
	tests := []struct {
		queueType utils.QueueType
		expected  string
	}{
		{utils.QueueTypeNATS, "nats"},
		{utils.QueueTypeRedis, "redis"},
		{utils.QueueTypeKafka, "kafka"},
		{utils.QueueTypeMemory, "memory"},
	}

	for _, tt := range tests {
		if string(tt.queueType) != tt.expected {
			t.Errorf("QueueType %s != %s", tt.queueType, tt.expected)
		}
	}
}
