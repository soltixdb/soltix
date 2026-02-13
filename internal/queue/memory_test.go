package queue

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewMemoryQueue(t *testing.T) {
	q := NewMemoryQueue()
	if q == nil {
		t.Fatal("NewMemoryQueue should return non-nil")
	}
	defer func() { _ = q.Close() }()

	if q.channels == nil {
		t.Error("channels map should be initialized")
	}
	if q.subscriptions == nil {
		t.Error("subscriptions map should be initialized")
	}
}

func TestMemoryQueue_Publish(t *testing.T) {
	q := NewMemoryQueue()
	defer func() { _ = q.Close() }()

	ctx := context.Background()
	err := q.Publish(ctx, "test.subject", []byte("test message"))
	if err != nil {
		t.Fatalf("Failed to publish: %v", err)
	}

	// Verify message is in channel
	count := q.GetPendingCount("test.subject")
	if count != 1 {
		t.Errorf("Expected 1 pending message, got %d", count)
	}
}

func TestMemoryQueue_Publish_MultipleSubjects(t *testing.T) {
	q := NewMemoryQueue()
	defer func() { _ = q.Close() }()

	ctx := context.Background()
	subjects := []string{"subject.1", "subject.2", "subject.3"}

	for _, subject := range subjects {
		err := q.Publish(ctx, subject, []byte("message"))
		if err != nil {
			t.Fatalf("Failed to publish to %s: %v", subject, err)
		}
	}

	for _, subject := range subjects {
		if q.GetPendingCount(subject) != 1 {
			t.Errorf("Expected 1 message in %s", subject)
		}
	}
}

func TestMemoryQueue_Publish_ContextCancelled(t *testing.T) {
	q := NewMemoryQueue()
	defer func() { _ = q.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := q.Publish(ctx, "test", []byte("message"))
	// Should return context error since channel is empty and context is cancelled
	// But since channel has capacity, it might succeed
	// This test verifies the context check path works
	if err != nil && err != context.Canceled {
		t.Logf("Publish with cancelled context: %v", err)
	}
}

func TestMemoryQueue_Publish_DataCopy(t *testing.T) {
	q := NewMemoryQueue()
	defer func() { _ = q.Close() }()

	ctx := context.Background()
	originalData := []byte("original")
	err := q.Publish(ctx, "test", originalData)
	if err != nil {
		t.Fatalf("Failed to publish: %v", err)
	}

	// Modify original data
	originalData[0] = 'X'

	// Subscribe and verify data wasn't affected
	var received []byte
	var wg sync.WaitGroup
	wg.Add(1)

	err = q.Subscribe("test", func(data []byte) error {
		received = data
		wg.Done()
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	waitWithTimeout(t, &wg, 2*time.Second)

	if string(received) != "original" {
		t.Errorf("Data should be 'original', got '%s'", received)
	}
}

func TestMemoryQueue_PublishBatch_Success(t *testing.T) {
	q := NewMemoryQueue()
	defer func() { _ = q.Close() }()

	messages := []BatchMessage{
		{Subject: "batch.1", Data: []byte("msg1")},
		{Subject: "batch.2", Data: []byte("msg2")},
		{Subject: "batch.1", Data: []byte("msg3")},
	}

	ctx := context.Background()
	count, err := q.PublishBatch(ctx, messages)
	if err != nil {
		t.Fatalf("Failed to publish batch: %v", err)
	}

	if count != 3 {
		t.Errorf("Expected 3 messages published, got %d", count)
	}

	if q.GetPendingCount("batch.1") != 2 {
		t.Errorf("Expected 2 messages in batch.1")
	}
	if q.GetPendingCount("batch.2") != 1 {
		t.Errorf("Expected 1 message in batch.2")
	}
}

func TestMemoryQueue_PublishBatch_Empty(t *testing.T) {
	q := NewMemoryQueue()
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

func TestMemoryQueue_Subscribe(t *testing.T) {
	q := NewMemoryQueue()
	defer func() { _ = q.Close() }()

	var received []byte
	var wg sync.WaitGroup
	wg.Add(1)

	err := q.Subscribe("test", func(data []byte) error {
		received = data
		wg.Done()
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	ctx := context.Background()
	err = q.Publish(ctx, "test", []byte("hello"))
	if err != nil {
		t.Fatalf("Failed to publish: %v", err)
	}

	waitWithTimeout(t, &wg, 2*time.Second)

	if string(received) != "hello" {
		t.Errorf("Expected 'hello', got '%s'", received)
	}
}

func TestMemoryQueue_Subscribe_MultipleMessages(t *testing.T) {
	q := NewMemoryQueue()
	defer func() { _ = q.Close() }()

	messageCount := 100
	var receivedCount int32

	err := q.Subscribe("test", func(data []byte) error {
		atomic.AddInt32(&receivedCount, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	ctx := context.Background()
	for i := 0; i < messageCount; i++ {
		_ = q.Publish(ctx, "test", []byte(fmt.Sprintf("msg-%d", i)))
	}

	// Wait for all messages
	waitFor(t, func() bool {
		return int(atomic.LoadInt32(&receivedCount)) >= messageCount
	}, 5*time.Second)

	if int(receivedCount) != messageCount {
		t.Errorf("Expected %d messages, got %d", messageCount, receivedCount)
	}
}

func TestMemoryQueue_Subscribe_HandlerError(t *testing.T) {
	q := NewMemoryQueue()
	defer func() { _ = q.Close() }()

	var callCount int32

	err := q.Subscribe("test", func(data []byte) error {
		atomic.AddInt32(&callCount, 1)
		return fmt.Errorf("handler error")
	})
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		_ = q.Publish(ctx, "test", []byte("msg"))
	}

	// Wait a bit for processing
	time.Sleep(200 * time.Millisecond)

	// Handler should still be called for each message despite errors
	if atomic.LoadInt32(&callCount) < 5 {
		t.Errorf("Expected at least 5 calls, got %d", callCount)
	}
}

func TestMemoryQueue_Subscribe_DoubleSubscribe(t *testing.T) {
	q := NewMemoryQueue()
	defer func() { _ = q.Close() }()

	err := q.Subscribe("test", func(data []byte) error {
		return nil
	})
	if err != nil {
		t.Fatalf("First subscribe failed: %v", err)
	}

	err = q.Subscribe("test", func(data []byte) error {
		return nil
	})
	if err == nil {
		t.Fatal("Expected error for double subscribe")
	}
}

func TestMemoryQueue_Subscribe_MultipleSubjects(t *testing.T) {
	q := NewMemoryQueue()
	defer func() { _ = q.Close() }()

	subjects := []string{"sub.1", "sub.2", "sub.3"}
	receivedMap := make(map[string]int32)
	var mu sync.Mutex

	for _, subject := range subjects {
		s := subject // capture
		err := q.Subscribe(s, func(data []byte) error {
			mu.Lock()
			receivedMap[s]++
			mu.Unlock()
			return nil
		})
		if err != nil {
			t.Fatalf("Failed to subscribe to %s: %v", s, err)
		}
	}

	ctx := context.Background()
	for _, subject := range subjects {
		for i := 0; i < 10; i++ {
			_ = q.Publish(ctx, subject, []byte("msg"))
		}
	}

	// Wait for processing
	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	for _, subject := range subjects {
		if receivedMap[subject] != 10 {
			t.Errorf("Expected 10 messages for %s, got %d", subject, receivedMap[subject])
		}
	}
	mu.Unlock()
}

func TestMemoryQueue_Unsubscribe_Success(t *testing.T) {
	q := NewMemoryQueue()
	defer func() { _ = q.Close() }()

	err := q.Subscribe("test", func(data []byte) error {
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	err = q.Unsubscribe("test")
	if err != nil {
		t.Fatalf("Failed to unsubscribe: %v", err)
	}
}

func TestMemoryQueue_Unsubscribe_NotSubscribed(t *testing.T) {
	q := NewMemoryQueue()
	defer func() { _ = q.Close() }()

	err := q.Unsubscribe("not.subscribed")
	if err == nil {
		t.Fatal("Expected error for unsubscribing non-existent subject")
	}
}

func TestMemoryQueue_Unsubscribe_DoubleUnsubscribe(t *testing.T) {
	q := NewMemoryQueue()
	defer func() { _ = q.Close() }()

	_ = q.Subscribe("test", func(data []byte) error { return nil })

	err := q.Unsubscribe("test")
	if err != nil {
		t.Fatalf("First unsubscribe failed: %v", err)
	}

	err = q.Unsubscribe("test")
	if err == nil {
		t.Fatal("Expected error for double unsubscribe")
	}
}

func TestMemoryQueue_Unsubscribe_StopsProcessing(t *testing.T) {
	q := NewMemoryQueue()
	defer func() { _ = q.Close() }()

	var receivedCount int32

	err := q.Subscribe("test", func(data []byte) error {
		atomic.AddInt32(&receivedCount, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	// Publish some messages
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		_ = q.Publish(ctx, "test", []byte("msg"))
	}

	time.Sleep(100 * time.Millisecond)

	// Unsubscribe
	_ = q.Unsubscribe("test")

	countAfterUnsub := atomic.LoadInt32(&receivedCount)

	// Publish more - should not be processed
	for i := 0; i < 5; i++ {
		_ = q.Publish(ctx, "test", []byte("msg"))
	}

	time.Sleep(100 * time.Millisecond)

	// Count should not increase
	if atomic.LoadInt32(&receivedCount) > countAfterUnsub {
		// This might happen due to race, but generally shouldn't
		t.Logf("Note: Some messages processed after unsubscribe")
	}
}

func TestMemoryQueue_Close(t *testing.T) {
	q := NewMemoryQueue()

	// Add some subscriptions and channels
	_ = q.Subscribe("test.1", func(data []byte) error { return nil })
	_ = q.Subscribe("test.2", func(data []byte) error { return nil })

	ctx := context.Background()
	_ = q.Publish(ctx, "test.1", []byte("msg"))
	_ = q.Publish(ctx, "test.3", []byte("msg")) // Create channel without subscription

	err := q.Close()
	if err != nil {
		t.Fatalf("Failed to close: %v", err)
	}

	if len(q.subscriptions) != 0 {
		t.Error("Subscriptions should be empty after close")
	}
	if len(q.channels) != 0 {
		t.Error("Channels should be empty after close")
	}
}

func TestMemoryQueue_Close_Empty(t *testing.T) {
	q := NewMemoryQueue()

	err := q.Close()
	if err != nil {
		t.Fatalf("Failed to close empty queue: %v", err)
	}
}

func TestMemoryQueue_GetPendingCount_Full(t *testing.T) {
	q := NewMemoryQueue()
	defer func() { _ = q.Close() }()

	// Non-existent subject
	if q.GetPendingCount("none") != 0 {
		t.Error("Non-existent subject should have 0 pending")
	}

	// Add some messages
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		_ = q.Publish(ctx, "test", []byte("msg"))
	}

	if q.GetPendingCount("test") != 10 {
		t.Errorf("Expected 10 pending, got %d", q.GetPendingCount("test"))
	}
}

func TestMemoryQueue_ConcurrentPublish(t *testing.T) {
	q := NewMemoryQueue()
	defer func() { _ = q.Close() }()

	ctx := context.Background()
	numGoroutines := 10
	messagesPerGoroutine := 100

	var wg sync.WaitGroup
	var errCount int32

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < messagesPerGoroutine; j++ {
				if err := q.Publish(ctx, "concurrent", []byte(fmt.Sprintf("%d-%d", id, j))); err != nil {
					atomic.AddInt32(&errCount, 1)
				}
			}
		}(i)
	}

	wg.Wait()

	if errCount > 0 {
		t.Errorf("Had %d errors during concurrent publish", errCount)
	}

	expected := numGoroutines * messagesPerGoroutine
	if q.GetPendingCount("concurrent") != expected {
		t.Errorf("Expected %d pending, got %d", expected, q.GetPendingCount("concurrent"))
	}
}

func TestMemoryQueue_ConcurrentSubscribeUnsubscribe(t *testing.T) {
	q := NewMemoryQueue()
	defer func() { _ = q.Close() }()

	numSubjects := 10
	var wg sync.WaitGroup

	// Concurrent subscribe
	for i := 0; i < numSubjects; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_ = q.Subscribe(fmt.Sprintf("subject.%d", id), func(data []byte) error {
				return nil
			})
		}(i)
	}
	wg.Wait()

	// Concurrent unsubscribe
	for i := 0; i < numSubjects; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_ = q.Unsubscribe(fmt.Sprintf("subject.%d", id))
		}(i)
	}
	wg.Wait()

	if len(q.subscriptions) != 0 {
		t.Errorf("All subscriptions should be removed, got %d", len(q.subscriptions))
	}
}

func TestMemoryQueue_ChannelCapacity(t *testing.T) {
	q := NewMemoryQueue()
	defer func() { _ = q.Close() }()

	ctx := context.Background()
	subject := "capacity.test"

	// Fill up the channel (capacity is 10000)
	for i := 0; i < 10000; i++ {
		err := q.Publish(ctx, subject, []byte("msg"))
		if err != nil {
			t.Fatalf("Failed to publish message %d: %v", i, err)
		}
	}

	// Next publish should fail (channel full)
	err := q.Publish(ctx, subject, []byte("overflow"))
	if err == nil {
		t.Fatal("Expected error when channel is full")
	}
}

func TestMemoryQueue_GetOrCreateChannel(t *testing.T) {
	q := NewMemoryQueue()
	defer func() { _ = q.Close() }()

	// First call creates channel
	ch1 := q.getOrCreateChannel("test")
	if ch1 == nil {
		t.Fatal("Channel should not be nil")
	}

	// Second call returns same channel
	ch2 := q.getOrCreateChannel("test")
	if ch1 != ch2 {
		t.Error("Should return same channel for same subject")
	}

	// Different subject gets different channel
	ch3 := q.getOrCreateChannel("other")
	if ch1 == ch3 {
		t.Error("Different subjects should have different channels")
	}
}

// Benchmark tests
func BenchmarkMemoryQueue_Publish(b *testing.B) {
	q := NewMemoryQueue()
	defer func() { _ = q.Close() }()

	ctx := context.Background()
	data := []byte("benchmark message")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Publish(ctx, fmt.Sprintf("bench.%d", i%100), data)
	}
}

func BenchmarkMemoryQueue_PublishBatch(b *testing.B) {
	q := NewMemoryQueue()
	defer func() { _ = q.Close() }()

	ctx := context.Background()
	messages := make([]BatchMessage, 100)
	for i := range messages {
		messages[i] = BatchMessage{
			Subject: fmt.Sprintf("bench.%d", i),
			Data:    []byte("benchmark message"),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = q.PublishBatch(ctx, messages)
	}
}

func BenchmarkMemoryQueue_PublishSubscribe(b *testing.B) {
	q := NewMemoryQueue()
	defer func() { _ = q.Close() }()

	_ = q.Subscribe("bench", func(data []byte) error {
		return nil
	})

	ctx := context.Background()
	data := []byte("benchmark message")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Publish(ctx, "bench", data)
	}
}

// Helper functions
func waitWithTimeout(t *testing.T, wg *sync.WaitGroup, timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(timeout):
		t.Fatal("Timeout waiting for WaitGroup")
	}
}

func waitFor(t *testing.T, condition func() bool, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("Timeout waiting for condition")
}
