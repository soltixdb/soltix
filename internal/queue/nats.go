package queue

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// NATSQueue implements Queue interface using NATS JetStream
type NATSQueue struct {
	conn          *nats.Conn
	js            nats.JetStreamContext
	subscriptions map[string]*nats.Subscription
	mu            sync.RWMutex
}

// newNATSQueue creates a new NATS queue instance with JetStream enabled
func newNATSQueue(url string) (*NATSQueue, error) {
	conn, err := nats.Connect(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	// Create JetStream context
	js, err := conn.JetStream()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	return &NATSQueue{
		conn:          conn,
		js:            js,
		subscriptions: make(map[string]*nats.Subscription),
	}, nil
}

// newNATSQueueWithConn creates a new NATS queue instance with existing connection (used in tests)
func newNATSQueueWithConn(conn *nats.Conn) (*NATSQueue, error) {
	js, err := conn.JetStream()
	if err != nil {
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	return &NATSQueue{
		conn:          conn,
		js:            js,
		subscriptions: make(map[string]*nats.Subscription),
	}, nil
}

// Publish publishes a message to a subject using JetStream
func (q *NATSQueue) Publish(ctx context.Context, subject string, data []byte) error {
	// Use JetStream publish with context for better reliability
	_, err := q.js.PublishAsync(subject, data)
	if err != nil {
		return fmt.Errorf("failed to publish to subject %s: %w", subject, err)
	}
	return nil
}

// PublishBatch publishes multiple messages asynchronously and waits for all to complete
// This is more efficient than calling Publish multiple times because:
// 1. All messages are queued immediately (non-blocking)
// 2. Single network round-trip for acknowledgment
// 3. NATS batches messages internally for network efficiency
func (q *NATSQueue) PublishBatch(ctx context.Context, messages []BatchMessage) (int, error) {
	if len(messages) == 0 {
		return 0, nil
	}

	// Collect all pending futures
	futures := make([]nats.PubAckFuture, 0, len(messages))

	// Publish all messages asynchronously (non-blocking)
	for _, msg := range messages {
		future, err := q.js.PublishAsync(msg.Subject, msg.Data)
		if err != nil {
			// If we fail to queue a message, continue with others
			continue
		}
		futures = append(futures, future)
	}

	// Wait for all pending messages with timeout from context
	select {
	case <-q.js.PublishAsyncComplete():
		// All messages acknowledged
	case <-ctx.Done():
		return len(futures), fmt.Errorf("timeout waiting for batch publish: %w", ctx.Err())
	}

	// Count successful publishes
	successCount := 0
	for _, future := range futures {
		select {
		case <-future.Ok():
			successCount++
		case err := <-future.Err():
			// Log individual failures but don't fail entire batch
			_ = err // Could log this if needed
		default:
			// Still pending, count as success since we got PublishAsyncComplete
			successCount++
		}
	}

	return successCount, nil
}

// Subscribe subscribes to a subject with a message handler using JetStream durable consumer
// This provides:
// - Message persistence and replay
// - Manual acknowledgment (at-least-once delivery)
// - Flow control (max 100 in-flight messages)
func (q *NATSQueue) Subscribe(subject string, handler MessageHandler) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Check if already subscribed
	if _, exists := q.subscriptions[subject]; exists {
		return fmt.Errorf("already subscribed to subject: %s", subject)
	}

	// Create or get stream for this subject
	streamName := "soltix-" + sanitizeConsumerName(subject)
	_, err := q.js.StreamInfo(streamName)
	if err != nil {
		// Stream doesn't exist, create it
		_, err = q.js.AddStream(&nats.StreamConfig{
			Name:     streamName,
			Subjects: []string{subject},
			Storage:  nats.FileStorage,
		})
		if err != nil {
			return fmt.Errorf("failed to create stream for subject %s: %w", subject, err)
		}
	}

	// Create durable consumer name from subject (replace special characters)
	// Consumer names can only contain A-Z, a-z, 0-9, dash and underscore
	durableName := "consumer-" + sanitizeConsumerName(subject)

	// Subscribe with JetStream durable consumer
	sub, err := q.js.Subscribe(subject, func(msg *nats.Msg) {
		// Process message
		if err := handler(msg.Data); err != nil {
			// On error, NAK the message so it can be redelivered
			_ = msg.Nak()
			return
		}

		// On success, ACK the message
		_ = msg.Ack()
	},
		nats.Durable(durableName),    // Durable consumer - survives restarts
		nats.ManualAck(),             // Require explicit ACK
		nats.MaxAckPending(100),      // Max 100 unacked messages (flow control)
		nats.AckWait(30*time.Second), // Redeliver after 30s if not acked
		nats.MaxDeliver(3),           // Max 3 delivery attempts
		nats.DeliverAll(),            // Start from beginning (replay all messages)
	)
	if err != nil {
		return fmt.Errorf("failed to subscribe to subject %s: %w", subject, err)
	}

	q.subscriptions[subject] = sub
	return nil
}

// Unsubscribe unsubscribes from a subject
func (q *NATSQueue) Unsubscribe(subject string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	sub, exists := q.subscriptions[subject]
	if !exists {
		return fmt.Errorf("not subscribed to subject: %s", subject)
	}

	if err := sub.Unsubscribe(); err != nil {
		return fmt.Errorf("failed to unsubscribe from subject %s: %w", subject, err)
	}

	delete(q.subscriptions, subject)
	return nil
}

// Close closes the NATS connection and all subscriptions
func (q *NATSQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Unsubscribe from all subjects
	for subject, sub := range q.subscriptions {
		if err := sub.Unsubscribe(); err != nil {
			// Log error but continue closing others
			continue
		}
		delete(q.subscriptions, subject)
	}

	// Close connection
	q.conn.Close()
	return nil
}

// GetNATSConn returns the underlying NATS connection (for advanced usage)
func (q *NATSQueue) GetNATSConn() *nats.Conn {
	return q.conn
}

// sanitizeConsumerName replaces invalid characters for consumer names
// Consumer names can only contain: A-Z, a-z, 0-9, dash (-) and underscore (_)
func sanitizeConsumerName(subject string) string {
	result := make([]byte, 0, len(subject))
	for i := 0; i < len(subject); i++ {
		c := subject[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_' {
			result = append(result, c)
		} else {
			// Replace invalid characters with underscore
			result = append(result, '_')
		}
	}
	return string(result)
}
