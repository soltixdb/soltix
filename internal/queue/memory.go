package queue

import (
	"context"
	"fmt"
	"sync"
)

// MemoryQueue implements Queue interface using in-memory channels
// This is useful for testing and development without external dependencies
type MemoryQueue struct {
	channels      map[string]chan []byte
	subscriptions map[string]context.CancelFunc
	mu            sync.RWMutex
}

// newMemoryQueue creates a new in-memory queue instance
func newMemoryQueue() *MemoryQueue {
	return &MemoryQueue{
		channels:      make(map[string]chan []byte),
		subscriptions: make(map[string]context.CancelFunc),
	}
}

// getOrCreateChannel returns existing channel or creates new one
func (q *MemoryQueue) getOrCreateChannel(subject string) chan []byte {
	q.mu.Lock()
	defer q.mu.Unlock()

	if ch, exists := q.channels[subject]; exists {
		return ch
	}

	// Create buffered channel with capacity 10000
	ch := make(chan []byte, 10000)
	q.channels[subject] = ch
	return ch
}

// Publish publishes a message to an in-memory channel
func (q *MemoryQueue) Publish(ctx context.Context, subject string, data []byte) error {
	ch := q.getOrCreateChannel(subject)

	// Make a copy of data to avoid race conditions
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	select {
	case ch <- dataCopy:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return fmt.Errorf("channel full for subject: %s", subject)
	}
}

// PublishBatch publishes multiple messages
func (q *MemoryQueue) PublishBatch(ctx context.Context, messages []BatchMessage) (int, error) {
	successCount := 0

	for _, msg := range messages {
		if err := q.Publish(ctx, msg.Subject, msg.Data); err != nil {
			continue
		}
		successCount++
	}

	return successCount, nil
}

// Subscribe subscribes to an in-memory channel
func (q *MemoryQueue) Subscribe(subject string, handler MessageHandler) error {
	q.mu.Lock()
	if _, exists := q.subscriptions[subject]; exists {
		q.mu.Unlock()
		return fmt.Errorf("already subscribed to subject: %s", subject)
	}
	q.mu.Unlock()

	ch := q.getOrCreateChannel(subject)
	ctx, cancel := context.WithCancel(context.Background())

	q.mu.Lock()
	q.subscriptions[subject] = cancel
	q.mu.Unlock()

	// Start consuming messages in background
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case data := <-ch:
				if err := handler(data); err != nil {
					// In memory queue, we just log and continue
					// Could implement retry logic if needed
					continue
				}
			}
		}
	}()

	return nil
}

// Unsubscribe unsubscribes from a channel
func (q *MemoryQueue) Unsubscribe(subject string) error {
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

// Close closes all channels and subscriptions
func (q *MemoryQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Cancel all subscriptions
	for subject, cancel := range q.subscriptions {
		cancel()
		delete(q.subscriptions, subject)
	}

	// Close all channels
	for subject, ch := range q.channels {
		close(ch)
		delete(q.channels, subject)
	}

	return nil
}

// GetPendingCount returns the number of pending messages for a subject (for testing)
func (q *MemoryQueue) GetPendingCount(subject string) int {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if ch, exists := q.channels[subject]; exists {
		return len(ch)
	}
	return 0
}
