package subscriber

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMemorySubscriber_New(t *testing.T) {
	sub, err := NewMemorySubscriber()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = sub.Close() }()

	if sub == nil {
		t.Fatal("expected non-nil subscriber")
	}
}

func TestMemorySubscriber_Subscribe(t *testing.T) {
	sub, err := NewMemorySubscriber()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = sub.Close() }()

	ctx := context.Background()
	var received atomic.Int32

	err = sub.Subscribe(ctx, "test.subject", func(ctx context.Context, subject string, data []byte) error {
		received.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	PublishToMemory("test.subject", []byte("test message"))
	time.Sleep(100 * time.Millisecond)

	if received.Load() != 1 {
		t.Errorf("expected 1 message received, got %d", received.Load())
	}
}

func TestMemorySubscriber_SubscribeDuplicate(t *testing.T) {
	sub, err := NewMemorySubscriber()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = sub.Close() }()

	ctx := context.Background()
	handler := func(ctx context.Context, subject string, data []byte) error {
		return nil
	}

	err = sub.Subscribe(ctx, "test.subject.dup", handler)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = sub.Subscribe(ctx, "test.subject.dup", handler)
	if err == nil {
		t.Fatal("expected error for duplicate subscription")
	}
}

func TestMemorySubscriber_Unsubscribe(t *testing.T) {
	sub, err := NewMemorySubscriber()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = sub.Close() }()

	ctx := context.Background()
	err = sub.Subscribe(ctx, "test.subject.unsub", func(ctx context.Context, subject string, data []byte) error {
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = sub.Unsubscribe("test.subject.unsub")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = sub.Unsubscribe("test.subject.unsub")
	if err == nil {
		t.Fatal("expected error for unsubscribing non-existent subject")
	}
}

func TestMemorySubscriber_ConcurrentPublish(t *testing.T) {
	sub, err := NewMemorySubscriber()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = sub.Close() }()

	ctx := context.Background()
	var received atomic.Int32

	err = sub.Subscribe(ctx, "concurrent.subject", func(ctx context.Context, subject string, data []byte) error {
		received.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			PublishToMemory("concurrent.subject", []byte("test"))
		}()
	}
	wg.Wait()
	time.Sleep(200 * time.Millisecond)

	if received.Load() != 100 {
		t.Errorf("expected 100 messages, got %d", received.Load())
	}
}

func TestMemorySubscriber_Close(t *testing.T) {
	sub, err := NewMemorySubscriber()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()
	_ = sub.Subscribe(ctx, "test1.close", func(ctx context.Context, subject string, data []byte) error { return nil })
	_ = sub.Subscribe(ctx, "test2.close", func(ctx context.Context, subject string, data []byte) error { return nil })

	err = sub.Close()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sub.mu.RLock()
	count := len(sub.subscriptions)
	sub.mu.RUnlock()

	if count != 0 {
		t.Errorf("expected 0 subscriptions after close, got %d", count)
	}
}

func TestMemorySubscriber_MultipleSubjects(t *testing.T) {
	sub, err := NewMemorySubscriber()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = sub.Close() }()

	ctx := context.Background()
	var count1, count2 atomic.Int32

	err = sub.Subscribe(ctx, "subject.1", func(ctx context.Context, subject string, data []byte) error {
		count1.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = sub.Subscribe(ctx, "subject.2", func(ctx context.Context, subject string, data []byte) error {
		count2.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	PublishToMemory("subject.1", []byte("msg1"))
	PublishToMemory("subject.2", []byte("msg2"))
	PublishToMemory("subject.1", []byte("msg3"))
	time.Sleep(100 * time.Millisecond)

	if count1.Load() != 2 {
		t.Errorf("expected 2 messages for subject.1, got %d", count1.Load())
	}
	if count2.Load() != 1 {
		t.Errorf("expected 1 message for subject.2, got %d", count2.Load())
	}
}

func TestMemorySubscriber_MessageContent(t *testing.T) {
	sub, err := NewMemorySubscriber()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = sub.Close() }()

	ctx := context.Background()
	var receivedSubject string
	var receivedData []byte
	done := make(chan struct{})

	err = sub.Subscribe(ctx, "content.test", func(ctx context.Context, subject string, data []byte) error {
		receivedSubject = subject
		receivedData = data
		close(done)
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expectedData := []byte("hello world")
	PublishToMemory("content.test", expectedData)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for message")
	}

	if receivedSubject != "content.test" {
		t.Errorf("expected subject=content.test, got %s", receivedSubject)
	}
	if string(receivedData) != string(expectedData) {
		t.Errorf("expected data=%s, got %s", expectedData, receivedData)
	}
}

func TestMemorySubscriber_HandlerError(t *testing.T) {
	sub, err := NewMemorySubscriber()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = sub.Close() }()

	ctx := context.Background()
	var callCount atomic.Int32

	err = sub.Subscribe(ctx, "error.test", func(ctx context.Context, subject string, data []byte) error {
		callCount.Add(1)
		return fmt.Errorf("simulated error")
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	PublishToMemory("error.test", []byte("test"))
	time.Sleep(50 * time.Millisecond)

	if callCount.Load() != 1 {
		t.Errorf("expected handler to be called once, got %d", callCount.Load())
	}
}

func TestMemorySubscriber_ContextCancellation(t *testing.T) {
	sub, err := NewMemorySubscriber()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = sub.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	var callCount atomic.Int32

	err = sub.Subscribe(ctx, "cancel.test", func(ctx context.Context, subject string, data []byte) error {
		callCount.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	PublishToMemory("cancel.test", []byte("test1"))
	time.Sleep(50 * time.Millisecond)

	cancel()
	time.Sleep(50 * time.Millisecond)

	initialCount := callCount.Load()
	PublishToMemory("cancel.test", []byte("test2"))
	time.Sleep(50 * time.Millisecond)

	if callCount.Load() > initialCount+1 {
		t.Errorf("messages processed after context cancellation")
	}
}

func BenchmarkMemorySubscriber_Publish(b *testing.B) {
	sub, _ := NewMemorySubscriber()
	defer func() { _ = sub.Close() }()

	ctx := context.Background()
	_ = sub.Subscribe(ctx, "bench.subject", func(ctx context.Context, subject string, data []byte) error {
		return nil
	})

	data := []byte("benchmark message")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		PublishToMemory("bench.subject", data)
	}
}

func BenchmarkMemorySubscriber_ConcurrentPublish(b *testing.B) {
	sub, _ := NewMemorySubscriber()
	defer func() { _ = sub.Close() }()

	ctx := context.Background()
	_ = sub.Subscribe(ctx, "bench.concurrent", func(ctx context.Context, subject string, data []byte) error {
		return nil
	})

	data := []byte("benchmark message")
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			PublishToMemory("bench.concurrent", data)
		}
	})
}

// Additional comprehensive tests

func TestMemorySubscriber_PublishToNonExistentSubject(t *testing.T) {
	sub, err := NewMemorySubscriber()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = sub.Close() }()

	// Publishing to non-existent subject should not panic
	PublishToMemory("non.existent.subject", []byte("test"))
	time.Sleep(50 * time.Millisecond)
}

func TestMemorySubscriber_EmptySubject(t *testing.T) {
	sub, err := NewMemorySubscriber()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = sub.Close() }()

	ctx := context.Background()
	var received atomic.Int32

	err = sub.Subscribe(ctx, "", func(ctx context.Context, subject string, data []byte) error {
		received.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	PublishToMemory("", []byte("test"))
	time.Sleep(50 * time.Millisecond)

	if received.Load() != 1 {
		t.Errorf("expected 1 message, got %d", received.Load())
	}
}

func TestMemorySubscriber_NilData(t *testing.T) {
	sub, err := NewMemorySubscriber()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = sub.Close() }()

	ctx := context.Background()
	var receivedData []byte
	done := make(chan struct{})

	err = sub.Subscribe(ctx, "nil.data", func(ctx context.Context, subject string, data []byte) error {
		receivedData = data
		close(done)
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	PublishToMemory("nil.data", nil)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for message")
	}

	if receivedData != nil {
		t.Errorf("expected nil data, got %v", receivedData)
	}
}

func TestMemorySubscriber_EmptyData(t *testing.T) {
	sub, err := NewMemorySubscriber()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = sub.Close() }()

	ctx := context.Background()
	var receivedData []byte
	done := make(chan struct{})

	err = sub.Subscribe(ctx, "empty.data", func(ctx context.Context, subject string, data []byte) error {
		receivedData = data
		close(done)
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	PublishToMemory("empty.data", []byte{})

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for message")
	}

	if len(receivedData) != 0 {
		t.Errorf("expected empty data, got %v", receivedData)
	}
}

func TestMemorySubscriber_LargeData(t *testing.T) {
	sub, err := NewMemorySubscriber()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = sub.Close() }()

	ctx := context.Background()
	var receivedSize int
	done := make(chan struct{})

	err = sub.Subscribe(ctx, "large.data", func(ctx context.Context, subject string, data []byte) error {
		receivedSize = len(data)
		close(done)
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	largeData := make([]byte, 1024*1024) // 1MB
	PublishToMemory("large.data", largeData)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for message")
	}

	if receivedSize != 1024*1024 {
		t.Errorf("expected data size=1048576, got %d", receivedSize)
	}
}

func TestMemorySubscriber_ConcurrentSubscribeUnsubscribe(t *testing.T) {
	sub, err := NewMemorySubscriber()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = sub.Close() }()

	ctx := context.Background()
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			subject := fmt.Sprintf("concurrent.%d", idx)
			_ = sub.Subscribe(ctx, subject, func(ctx context.Context, subject string, data []byte) error {
				return nil
			})
			time.Sleep(10 * time.Millisecond)
			_ = sub.Unsubscribe(subject)
		}(i)
	}

	wg.Wait()
}

func TestMemorySubscriber_MultipleSubscribers(t *testing.T) {
	sub1, _ := NewMemorySubscriber()
	sub2, _ := NewMemorySubscriber()
	defer func() {
		_ = sub1.Close()
		_ = sub2.Close()
	}()

	ctx := context.Background()
	var count1, count2 atomic.Int32

	_ = sub1.Subscribe(ctx, "shared.subject", func(ctx context.Context, subject string, data []byte) error {
		count1.Add(1)
		return nil
	})

	_ = sub2.Subscribe(ctx, "shared.subject", func(ctx context.Context, subject string, data []byte) error {
		count2.Add(1)
		return nil
	})

	PublishToMemory("shared.subject", []byte("test"))
	time.Sleep(100 * time.Millisecond)

	if count1.Load() != 1 {
		t.Errorf("expected 1 message for sub1, got %d", count1.Load())
	}
	if count2.Load() != 1 {
		t.Errorf("expected 1 message for sub2, got %d", count2.Load())
	}
}

func TestMemorySubscriber_CloseIdempotent(t *testing.T) {
	sub, err := NewMemorySubscriber()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = sub.Close()
	if err != nil {
		t.Fatalf("first close error: %v", err)
	}

	err = sub.Close()
	if err != nil {
		t.Fatalf("second close error: %v", err)
	}
}

func TestMemorySubscriber_UnsubscribeNonExistent(t *testing.T) {
	sub, err := NewMemorySubscriber()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = sub.Close() }()

	err = sub.Unsubscribe("non.existent")
	if err == nil {
		t.Fatal("expected error for unsubscribing non-existent subject")
	}
}

func TestMemorySubscriber_ChannelFull(t *testing.T) {
	sub, err := NewMemorySubscriber()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = sub.Close() }()

	ctx := context.Background()
	blockCh := make(chan struct{})
	var received atomic.Int32

	err = sub.Subscribe(ctx, "full.channel", func(ctx context.Context, subject string, data []byte) error {
		received.Add(1)
		<-blockCh // Block handler
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Fill the channel (capacity is 1000)
	for i := 0; i < 1100; i++ {
		PublishToMemory("full.channel", []byte("test"))
	}

	time.Sleep(50 * time.Millisecond)
	close(blockCh)
	time.Sleep(100 * time.Millisecond)

	// Some messages should be received (at least the buffered ones)
	if received.Load() < 1 {
		t.Errorf("expected at least 1 message received, got %d", received.Load())
	}
}

func TestMemorySubscriber_SpecialSubjectCharacters(t *testing.T) {
	sub, err := NewMemorySubscriber()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = sub.Close() }()

	ctx := context.Background()
	var received atomic.Int32

	specialSubject := "test.subject-with_special!@#chars"
	err = sub.Subscribe(ctx, specialSubject, func(ctx context.Context, subject string, data []byte) error {
		received.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	PublishToMemory(specialSubject, []byte("test"))
	time.Sleep(50 * time.Millisecond)

	if received.Load() != 1 {
		t.Errorf("expected 1 message, got %d", received.Load())
	}
}

func TestMemorySubscriber_UnsubscribeWhileProcessing(t *testing.T) {
	sub, err := NewMemorySubscriber()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = sub.Close() }()

	ctx := context.Background()
	var processing atomic.Bool
	var received atomic.Int32

	err = sub.Subscribe(ctx, "unsub.while.processing", func(ctx context.Context, subject string, data []byte) error {
		processing.Store(true)
		time.Sleep(100 * time.Millisecond)
		received.Add(1)
		processing.Store(false)
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	PublishToMemory("unsub.while.processing", []byte("test1"))
	time.Sleep(10 * time.Millisecond) // Let processing start

	err = sub.Unsubscribe("unsub.while.processing")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	time.Sleep(150 * time.Millisecond)

	// The message that started processing should complete
	if received.Load() < 1 {
		t.Errorf("expected at least 1 message processed, got %d", received.Load())
	}
}

func TestPublishToMemory_GlobalBroker(t *testing.T) {
	broker := getMemoryBroker()
	if broker == nil {
		t.Fatal("expected non-nil global broker")
	}

	broker2 := getMemoryBroker()
	if broker != broker2 {
		t.Error("expected same broker instance (singleton)")
	}
}
