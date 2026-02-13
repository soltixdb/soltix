package queue

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// setupTestNATS creates an embedded NATS server for testing
func setupTestNATS(t *testing.T) (*server.Server, string, func()) {
	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1, // Random port
		JetStream: true,
		StoreDir:  t.TempDir(),
	}

	ns, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("Failed to create NATS server: %v", err)
	}

	go ns.Start()

	// Wait for server to be ready
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS server not ready")
	}

	url := ns.ClientURL()

	cleanup := func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	}

	return ns, url, cleanup
}

func TestNewNATSQueue(t *testing.T) {
	_, url, cleanup := setupTestNATS(t)
	defer cleanup()

	queue, err := NewNATSQueue(url)
	if err != nil {
		t.Fatalf("Failed to create NATS queue: %v", err)
	}
	defer func() { _ = queue.Close() }()

	if queue == nil {
		t.Fatal("Expected queue to be created")
	}

	if queue.conn == nil {
		t.Error("Expected connection to be initialized")
	}

	if queue.js == nil {
		t.Error("Expected JetStream context to be initialized")
	}

	if queue.subscriptions == nil {
		t.Error("Expected subscriptions map to be initialized")
	}
}

func TestNewNATSQueue_InvalidURL(t *testing.T) {
	queue, err := NewNATSQueue("nats://invalid-host:9999")
	if err == nil {
		if queue != nil {
			_ = queue.Close()
		}
		t.Fatal("Expected error with invalid URL")
	}
}

func TestNewNATSQueueWithConn(t *testing.T) {
	_, url, cleanup := setupTestNATS(t)
	defer cleanup()

	// Create connection first
	conn, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer conn.Close()

	queue, err := NewNATSQueueWithConn(conn)
	if err != nil {
		t.Fatalf("Failed to create NATS queue with connection: %v", err)
	}
	defer func() { _ = queue.Close() }()

	if queue == nil {
		t.Fatal("Expected queue to be created")
	}

	if queue.conn == nil {
		t.Error("Expected connection to be set")
	}

	if queue.js == nil {
		t.Error("Expected JetStream context to be initialized")
	}
}

func TestNATSQueue_PublishAndSubscribe(t *testing.T) {
	_, url, cleanup := setupTestNATS(t)
	defer cleanup()

	queue, err := NewNATSQueue(url)
	if err != nil {
		t.Fatalf("Failed to create NATS queue: %v", err)
	}
	defer func() { _ = queue.Close() }()

	subject := "test.publish.subscribe"
	testData := []byte("test message")

	// Create a channel to receive messages
	received := make(chan []byte, 1)
	handler := func(data []byte) error {
		received <- data
		return nil
	}

	// Subscribe first
	err = queue.Subscribe(subject, handler)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	// Wait a bit for subscription to be ready
	time.Sleep(200 * time.Millisecond)

	// Publish message
	ctx := context.Background()
	err = queue.Publish(ctx, subject, testData)
	if err != nil {
		t.Fatalf("Failed to publish: %v", err)
	}

	// Wait for message
	select {
	case data := <-received:
		if string(data) != string(testData) {
			t.Errorf("Expected data %q, got %q", testData, data)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message")
	}
}

func TestNATSQueue_PublishMultipleMessages(t *testing.T) {
	_, url, cleanup := setupTestNATS(t)
	defer cleanup()

	queue, err := NewNATSQueue(url)
	if err != nil {
		t.Fatalf("Failed to create NATS queue: %v", err)
	}
	defer func() { _ = queue.Close() }()

	subject := "test.multiple.messages"
	messageCount := 10

	// Create a counter for received messages
	var receivedCount atomic.Int32
	var mu sync.Mutex
	receivedMessages := make([]string, 0, messageCount)

	handler := func(data []byte) error {
		mu.Lock()
		receivedMessages = append(receivedMessages, string(data))
		mu.Unlock()
		receivedCount.Add(1)
		return nil
	}

	// Subscribe
	err = queue.Subscribe(subject, handler)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	// Wait for subscription
	time.Sleep(100 * time.Millisecond)

	// Publish multiple messages
	ctx := context.Background()
	for i := 0; i < messageCount; i++ {
		data := []byte(fmt.Sprintf("message-%d", i))
		err = queue.Publish(ctx, subject, data)
		if err != nil {
			t.Fatalf("Failed to publish message %d: %v", i, err)
		}
	}

	// Wait for all messages
	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if receivedCount.Load() >= int32(messageCount) {
				// Success
				mu.Lock()
				if len(receivedMessages) != messageCount {
					t.Errorf("Expected %d messages, got %d", messageCount, len(receivedMessages))
				}
				mu.Unlock()
				return
			}
		case <-timeout:
			t.Fatalf("Timeout: only received %d out of %d messages", receivedCount.Load(), messageCount)
		}
	}
}

func TestNATSQueue_SubscribeAlreadySubscribed(t *testing.T) {
	_, url, cleanup := setupTestNATS(t)
	defer cleanup()

	queue, err := NewNATSQueue(url)
	if err != nil {
		t.Fatalf("Failed to create NATS queue: %v", err)
	}
	defer func() { _ = queue.Close() }()

	subject := "test.duplicate.subscribe"
	handler := func(data []byte) error {
		return nil
	}

	// First subscription should succeed
	err = queue.Subscribe(subject, handler)
	if err != nil {
		t.Fatalf("Failed to subscribe first time: %v", err)
	}

	// Second subscription should fail
	err = queue.Subscribe(subject, handler)
	if err == nil {
		t.Error("Expected error when subscribing to same subject twice")
	}
}

func TestNATSQueue_MessageHandlerError(t *testing.T) {
	_, url, cleanup := setupTestNATS(t)
	defer cleanup()

	queue, err := NewNATSQueue(url)
	if err != nil {
		t.Fatalf("Failed to create NATS queue: %v", err)
	}
	defer func() { _ = queue.Close() }()

	subject := "test.handler.error"
	testData := []byte("test message")

	// Track how many times handler is called
	var callCount atomic.Int32
	handler := func(data []byte) error {
		count := callCount.Add(1)
		// Fail first 2 times, succeed on 3rd
		if count < 3 {
			return fmt.Errorf("simulated error")
		}
		return nil
	}

	// Subscribe
	err = queue.Subscribe(subject, handler)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	// Wait for subscription
	time.Sleep(100 * time.Millisecond)

	// Publish message
	ctx := context.Background()
	err = queue.Publish(ctx, subject, testData)
	if err != nil {
		t.Fatalf("Failed to publish: %v", err)
	}

	// Wait for retries
	time.Sleep(3 * time.Second)

	// Should be called 3 times (original + 2 retries)
	if callCount.Load() < 3 {
		t.Errorf("Expected at least 3 handler calls (with retries), got %d", callCount.Load())
	}
}

func TestNATSQueue_Unsubscribe(t *testing.T) {
	_, url, cleanup := setupTestNATS(t)
	defer cleanup()

	queue, err := NewNATSQueue(url)
	if err != nil {
		t.Fatalf("Failed to create NATS queue: %v", err)
	}
	defer func() { _ = queue.Close() }()

	subject := "test.unsubscribe"
	handler := func(data []byte) error {
		return nil
	}

	// Subscribe
	err = queue.Subscribe(subject, handler)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	// Verify subscription exists
	queue.mu.RLock()
	_, exists := queue.subscriptions[subject]
	queue.mu.RUnlock()
	if !exists {
		t.Fatal("Expected subscription to exist")
	}

	// Unsubscribe
	err = queue.Unsubscribe(subject)
	if err != nil {
		t.Fatalf("Failed to unsubscribe: %v", err)
	}

	// Verify subscription is removed
	queue.mu.RLock()
	_, exists = queue.subscriptions[subject]
	queue.mu.RUnlock()
	if exists {
		t.Error("Expected subscription to be removed")
	}
}

func TestNATSQueue_UnsubscribeNotSubscribed(t *testing.T) {
	_, url, cleanup := setupTestNATS(t)
	defer cleanup()

	queue, err := NewNATSQueue(url)
	if err != nil {
		t.Fatalf("Failed to create NATS queue: %v", err)
	}
	defer func() { _ = queue.Close() }()

	// Try to unsubscribe from non-existent subscription
	err = queue.Unsubscribe("nonexistent.subject")
	if err == nil {
		t.Error("Expected error when unsubscribing from non-existent subject")
	}
}

func TestNATSQueue_MultipleSubscribers(t *testing.T) {
	_, url, cleanup := setupTestNATS(t)
	defer cleanup()

	queue, err := NewNATSQueue(url)
	if err != nil {
		t.Fatalf("Failed to create NATS queue: %v", err)
	}
	defer func() { _ = queue.Close() }()

	subject1 := "test.multi.subject1"
	subject2 := "test.multi.subject2"

	received1 := make(chan []byte, 1)
	received2 := make(chan []byte, 1)

	handler1 := func(data []byte) error {
		received1 <- data
		return nil
	}

	handler2 := func(data []byte) error {
		received2 <- data
		return nil
	}

	// Subscribe to both subjects
	err = queue.Subscribe(subject1, handler1)
	if err != nil {
		t.Fatalf("Failed to subscribe to subject1: %v", err)
	}

	err = queue.Subscribe(subject2, handler2)
	if err != nil {
		t.Fatalf("Failed to subscribe to subject2: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Publish to both subjects
	ctx := context.Background()
	data1 := []byte("message1")
	data2 := []byte("message2")

	_ = queue.Publish(ctx, subject1, data1)
	_ = queue.Publish(ctx, subject2, data2)

	// Verify both received their messages
	select {
	case msg := <-received1:
		if string(msg) != string(data1) {
			t.Errorf("Subject1: expected %q, got %q", data1, msg)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message on subject1")
	}

	select {
	case msg := <-received2:
		if string(msg) != string(data2) {
			t.Errorf("Subject2: expected %q, got %q", data2, msg)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message on subject2")
	}
}

func TestNATSQueue_Close(t *testing.T) {
	_, url, cleanup := setupTestNATS(t)
	defer cleanup()

	queue, err := NewNATSQueue(url)
	if err != nil {
		t.Fatalf("Failed to create NATS queue: %v", err)
	}

	subject := "test.close"
	handler := func(data []byte) error {
		return nil
	}

	// Subscribe to a subject
	err = queue.Subscribe(subject, handler)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	// Close should unsubscribe and close connection
	err = queue.Close()
	if err != nil {
		t.Errorf("Failed to close queue: %v", err)
	}

	// Verify subscriptions are cleaned up
	queue.mu.RLock()
	subCount := len(queue.subscriptions)
	queue.mu.RUnlock()

	if subCount != 0 {
		t.Errorf("Expected 0 subscriptions after close, got %d", subCount)
	}

	// Verify connection is closed
	if !queue.conn.IsClosed() {
		t.Error("Expected connection to be closed")
	}
}

func TestNATSQueue_GetNATSConn(t *testing.T) {
	_, url, cleanup := setupTestNATS(t)
	defer cleanup()

	queue, err := NewNATSQueue(url)
	if err != nil {
		t.Fatalf("Failed to create NATS queue: %v", err)
	}
	defer func() { _ = queue.Close() }()

	conn := queue.GetNATSConn()
	if conn == nil {
		t.Fatal("Expected non-nil connection")
	}

	if conn != queue.conn {
		t.Error("Expected returned connection to match internal connection")
	}
}

func TestNATSQueue_ConcurrentPublish(t *testing.T) {
	_, url, cleanup := setupTestNATS(t)
	defer cleanup()

	queue, err := NewNATSQueue(url)
	if err != nil {
		t.Fatalf("Failed to create NATS queue: %v", err)
	}
	defer func() { _ = queue.Close() }()

	subject := "test.concurrent.publish"
	messageCount := 50
	goroutines := 5

	var receivedCount atomic.Int32
	handler := func(data []byte) error {
		receivedCount.Add(1)
		return nil
	}

	// Subscribe
	err = queue.Subscribe(subject, handler)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Publish from multiple goroutines concurrently
	var wg sync.WaitGroup
	ctx := context.Background()

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < messageCount; i++ {
				data := []byte(fmt.Sprintf("goroutine-%d-message-%d", id, i))
				_ = queue.Publish(ctx, subject, data)
			}
		}(g)
	}

	wg.Wait()

	// Wait for all messages to be received
	expectedTotal := int32(messageCount * goroutines)
	timeout := time.After(10 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if receivedCount.Load() >= expectedTotal {
				return
			}
		case <-timeout:
			t.Fatalf("Timeout: received %d out of %d messages", receivedCount.Load(), expectedTotal)
		}
	}
}

func TestNATSQueue_WildcardSubscription(t *testing.T) {
	_, url, cleanup := setupTestNATS(t)
	defer cleanup()

	queue, err := NewNATSQueue(url)
	if err != nil {
		t.Fatalf("Failed to create NATS queue: %v", err)
	}
	defer func() { _ = queue.Close() }()

	// Subscribe to wildcard pattern
	wildcardSubject := "test.wildcard.>"
	var receivedCount atomic.Int32

	handler := func(data []byte) error {
		receivedCount.Add(1)
		return nil
	}

	err = queue.Subscribe(wildcardSubject, handler)
	if err != nil {
		t.Fatalf("Failed to subscribe to wildcard: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Publish to multiple subjects matching the wildcard
	ctx := context.Background()
	subjects := []string{
		"test.wildcard.foo",
		"test.wildcard.bar",
		"test.wildcard.baz.qux",
	}

	for _, subject := range subjects {
		err = queue.Publish(ctx, subject, []byte("test"))
		if err != nil {
			t.Fatalf("Failed to publish to %s: %v", subject, err)
		}
	}

	// Wait for messages
	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	expectedCount := int32(len(subjects))
	for {
		select {
		case <-ticker.C:
			if receivedCount.Load() >= expectedCount {
				return
			}
		case <-timeout:
			t.Fatalf("Timeout: received %d out of %d messages", receivedCount.Load(), expectedCount)
		}
	}
}

func TestNATSQueue_PublishBatch(t *testing.T) {
	_, url, cleanup := setupTestNATS(t)
	defer cleanup()

	queue, err := NewNATSQueue(url)
	if err != nil {
		t.Fatalf("Failed to create NATS queue: %v", err)
	}
	defer func() { _ = queue.Close() }()

	subject := "test.batch.publish"
	var receivedCount atomic.Int32
	var mu sync.Mutex
	receivedMessages := make([]string, 0)

	handler := func(data []byte) error {
		mu.Lock()
		receivedMessages = append(receivedMessages, string(data))
		mu.Unlock()
		receivedCount.Add(1)
		return nil
	}

	err = queue.Subscribe(subject, handler)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Create batch messages
	messageCount := 100
	messages := make([]BatchMessage, messageCount)
	for i := 0; i < messageCount; i++ {
		messages[i] = BatchMessage{
			Subject: subject,
			Data:    []byte(fmt.Sprintf("message_%d", i)),
		}
	}

	// Publish batch
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	publishedCount, err := queue.PublishBatch(ctx, messages)
	if err != nil {
		t.Fatalf("Failed to publish batch: %v", err)
	}

	if publishedCount != messageCount {
		t.Errorf("Expected %d published, got %d", messageCount, publishedCount)
	}

	// Wait for all messages to be received
	timeout := time.After(10 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if receivedCount.Load() >= int32(messageCount) {
				mu.Lock()
				if len(receivedMessages) != messageCount {
					t.Errorf("Expected %d messages, got %d", messageCount, len(receivedMessages))
				}
				mu.Unlock()
				return
			}
		case <-timeout:
			t.Fatalf("Timeout: received %d out of %d messages", receivedCount.Load(), messageCount)
		}
	}
}

func TestNATSQueue_PublishBatch_EmptyBatch(t *testing.T) {
	_, url, cleanup := setupTestNATS(t)
	defer cleanup()

	queue, err := NewNATSQueue(url)
	if err != nil {
		t.Fatalf("Failed to create NATS queue: %v", err)
	}
	defer func() { _ = queue.Close() }()

	// Publish empty batch
	ctx := context.Background()
	publishedCount, err := queue.PublishBatch(ctx, []BatchMessage{})
	if err != nil {
		t.Fatalf("Expected no error for empty batch, got: %v", err)
	}
	if publishedCount != 0 {
		t.Errorf("Expected 0 published for empty batch, got %d", publishedCount)
	}
}

func TestNATSQueue_PublishBatch_MultipleSubjects(t *testing.T) {
	_, url, cleanup := setupTestNATS(t)
	defer cleanup()

	queue, err := NewNATSQueue(url)
	if err != nil {
		t.Fatalf("Failed to create NATS queue: %v", err)
	}
	defer func() { _ = queue.Close() }()

	// Create streams for different subjects
	subjects := []string{
		"test.batch.node.1",
		"test.batch.node.2",
		"test.batch.node.3",
	}

	var totalReceived atomic.Int32
	perSubjectCount := make(map[string]*atomic.Int32)
	var mu sync.Mutex

	for _, subj := range subjects {
		perSubjectCount[subj] = &atomic.Int32{}
		localSubj := subj
		handler := func(data []byte) error {
			mu.Lock()
			perSubjectCount[localSubj].Add(1)
			mu.Unlock()
			totalReceived.Add(1)
			return nil
		}
		err = queue.Subscribe(localSubj, handler)
		if err != nil {
			t.Fatalf("Failed to subscribe to %s: %v", localSubj, err)
		}
	}

	time.Sleep(100 * time.Millisecond)

	// Create batch messages to different subjects (simulating multi-node writes)
	messagesPerSubject := 50
	messages := make([]BatchMessage, 0, len(subjects)*messagesPerSubject)
	for _, subj := range subjects {
		for i := 0; i < messagesPerSubject; i++ {
			messages = append(messages, BatchMessage{
				Subject: subj,
				Data:    []byte(fmt.Sprintf("msg_%d_to_%s", i, subj)),
			})
		}
	}

	// Publish batch
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	expectedTotal := len(subjects) * messagesPerSubject
	publishedCount, err := queue.PublishBatch(ctx, messages)
	if err != nil {
		t.Fatalf("Failed to publish batch: %v", err)
	}

	if publishedCount != expectedTotal {
		t.Errorf("Expected %d published, got %d", expectedTotal, publishedCount)
	}

	// Wait for all messages
	timeout := time.After(10 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if totalReceived.Load() >= int32(expectedTotal) {
				// Verify distribution across subjects
				for subj, count := range perSubjectCount {
					if count.Load() != int32(messagesPerSubject) {
						t.Errorf("Subject %s: expected %d, got %d", subj, messagesPerSubject, count.Load())
					}
				}
				return
			}
		case <-timeout:
			t.Fatalf("Timeout: received %d out of %d messages", totalReceived.Load(), expectedTotal)
		}
	}
}

func TestNATSQueue_PublishBatch_LargeBatch(t *testing.T) {
	_, url, cleanup := setupTestNATS(t)
	defer cleanup()

	queue, err := NewNATSQueue(url)
	if err != nil {
		t.Fatalf("Failed to create NATS queue: %v", err)
	}
	defer func() { _ = queue.Close() }()

	subject := "test.batch.large"
	var receivedCount atomic.Int32

	handler := func(data []byte) error {
		receivedCount.Add(1)
		return nil
	}

	err = queue.Subscribe(subject, handler)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Create large batch (1000 messages - simulating typical batch write)
	messageCount := 1000
	messages := make([]BatchMessage, messageCount)
	for i := 0; i < messageCount; i++ {
		// Simulate realistic message size (~200 bytes)
		data := []byte(fmt.Sprintf(`{"database":"db","collection":"coll","shard_id":"shard_%d","time":"2026-01-01T00:00:00Z","id":"device_%d","fields":{"temp":%.2f}}`, i%10, i, float64(i)*0.1))
		messages[i] = BatchMessage{
			Subject: subject,
			Data:    data,
		}
	}

	// Measure publish time
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	start := time.Now()
	publishedCount, err := queue.PublishBatch(ctx, messages)
	publishDuration := time.Since(start)

	if err != nil {
		t.Fatalf("Failed to publish large batch: %v", err)
	}

	if publishedCount != messageCount {
		t.Errorf("Expected %d published, got %d", messageCount, publishedCount)
	}

	t.Logf("Published %d messages in %v (%.2f msgs/sec)", messageCount, publishDuration, float64(messageCount)/publishDuration.Seconds())

	// Wait for messages
	timeout := time.After(30 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if receivedCount.Load() >= int32(messageCount) {
				t.Logf("Received all %d messages", messageCount)
				return
			}
		case <-timeout:
			t.Fatalf("Timeout: received %d out of %d messages", receivedCount.Load(), messageCount)
		}
	}
}
