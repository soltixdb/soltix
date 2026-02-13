package queue

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// KafkaConfig represents Apache Kafka configuration
type KafkaConfig struct {
	Brokers       []string      // Kafka broker addresses
	GroupID       string        // Consumer group ID
	BatchSize     int           // Batch size for producer (default: 100)
	BatchTimeout  time.Duration // Batch timeout for producer (default: 10ms)
	RequiredAcks  int           // Required acks: 0=none, 1=leader, -1=all (default: 1)
	Async         bool          // Async writes (default: false)
	MaxRetries    int           // Max retries on failure (default: 3)
	RetryBackoff  time.Duration // Backoff between retries (default: 100ms)
	CommitRetries int           // Consumer commit retries (default: 3)
}

// KafkaQueue implements Queue interface using Apache Kafka
type KafkaQueue struct {
	config        KafkaConfig
	writers       map[string]*kafka.Writer
	readers       map[string]*kafka.Reader
	subscriptions map[string]context.CancelFunc
	mu            sync.RWMutex
}

// newKafkaQueue creates a new Kafka queue instance
func newKafkaQueue(cfg KafkaConfig) (*KafkaQueue, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka brokers not configured")
	}

	// Apply defaults
	if cfg.GroupID == "" {
		cfg.GroupID = "soltix-group"
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 100
	}
	if cfg.BatchTimeout == 0 {
		cfg.BatchTimeout = 10 * time.Millisecond
	}
	if cfg.RequiredAcks == 0 {
		cfg.RequiredAcks = int(kafka.RequireOne)
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 3
	}
	if cfg.RetryBackoff == 0 {
		cfg.RetryBackoff = 100 * time.Millisecond
	}
	if cfg.CommitRetries == 0 {
		cfg.CommitRetries = 3
	}

	return &KafkaQueue{
		config:        cfg,
		writers:       make(map[string]*kafka.Writer),
		readers:       make(map[string]*kafka.Reader),
		subscriptions: make(map[string]context.CancelFunc),
	}, nil
}

// getOrCreateWriter returns existing writer or creates a new one for the topic
func (q *KafkaQueue) getOrCreateWriter(topic string) *kafka.Writer {
	q.mu.Lock()
	defer q.mu.Unlock()

	if writer, exists := q.writers[topic]; exists {
		return writer
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(q.config.Brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    q.config.BatchSize,
		BatchTimeout: q.config.BatchTimeout,
		RequiredAcks: kafka.RequiredAcks(q.config.RequiredAcks),
		Async:        q.config.Async,
		MaxAttempts:  q.config.MaxRetries,
	}

	q.writers[topic] = writer
	return writer
}

// Publish publishes a message to a Kafka topic
func (q *KafkaQueue) Publish(ctx context.Context, subject string, data []byte) error {
	writer := q.getOrCreateWriter(subject)

	msg := kafka.Message{
		Value: data,
		Time:  time.Now(),
	}

	err := writer.WriteMessages(ctx, msg)
	if err != nil {
		return fmt.Errorf("failed to publish to kafka topic %s: %w", subject, err)
	}

	return nil
}

// PublishBatch publishes multiple messages to Kafka topics
func (q *KafkaQueue) PublishBatch(ctx context.Context, messages []BatchMessage) (int, error) {
	if len(messages) == 0 {
		return 0, nil
	}

	// Group messages by topic
	topicMessages := make(map[string][]kafka.Message)
	for _, msg := range messages {
		kafkaMsg := kafka.Message{
			Value: msg.Data,
			Time:  time.Now(),
		}
		topicMessages[msg.Subject] = append(topicMessages[msg.Subject], kafkaMsg)
	}

	successCount := 0
	var lastErr error

	// Write to each topic
	for topic, msgs := range topicMessages {
		writer := q.getOrCreateWriter(topic)
		err := writer.WriteMessages(ctx, msgs...)
		if err != nil {
			lastErr = err
			continue
		}
		successCount += len(msgs)
	}

	if lastErr != nil && successCount == 0 {
		return 0, fmt.Errorf("failed to publish batch: %w", lastErr)
	}

	return successCount, nil
}

// Subscribe subscribes to a Kafka topic with consumer group
func (q *KafkaQueue) Subscribe(subject string, handler MessageHandler) error {
	q.mu.Lock()
	if _, exists := q.subscriptions[subject]; exists {
		q.mu.Unlock()
		return fmt.Errorf("already subscribed to topic: %s", subject)
	}
	q.mu.Unlock()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        q.config.Brokers,
		GroupID:        q.config.GroupID,
		Topic:          subject,
		MinBytes:       1,    // 1 byte minimum
		MaxBytes:       10e6, // 10MB max
		MaxWait:        1 * time.Second,
		CommitInterval: time.Second, // Auto-commit every second
	})

	ctx, cancel := context.WithCancel(context.Background())

	q.mu.Lock()
	q.readers[subject] = reader
	q.subscriptions[subject] = cancel
	q.mu.Unlock()

	// Start consuming in background
	go q.consumeMessages(ctx, reader, handler)

	return nil
}

// consumeMessages reads messages from Kafka in a loop
func (q *KafkaQueue) consumeMessages(ctx context.Context, reader *kafka.Reader, handler MessageHandler) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Read message with context
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return // Context cancelled
			}
			// Log error and continue
			continue
		}

		// Process message
		if err := handler(msg.Value); err != nil {
			// On error, don't commit - message will be redelivered
			continue
		}

		// Commit with retries
		for i := 0; i < q.config.CommitRetries; i++ {
			if err := reader.CommitMessages(ctx, msg); err == nil {
				break
			}
			if ctx.Err() != nil {
				return
			}
			time.Sleep(q.config.RetryBackoff)
		}
	}
}

// Unsubscribe unsubscribes from a Kafka topic
func (q *KafkaQueue) Unsubscribe(subject string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	cancel, exists := q.subscriptions[subject]
	if !exists {
		return fmt.Errorf("not subscribed to topic: %s", subject)
	}

	// Cancel the consumer goroutine
	cancel()

	// Close the reader
	if reader, ok := q.readers[subject]; ok {
		_ = reader.Close()
		delete(q.readers, subject)
	}

	delete(q.subscriptions, subject)
	return nil
}

// Close closes all Kafka connections
func (q *KafkaQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	var lastErr error

	// Cancel all subscriptions and close readers
	for subject, cancel := range q.subscriptions {
		cancel()
		if reader, ok := q.readers[subject]; ok {
			if err := reader.Close(); err != nil {
				lastErr = err
			}
		}
		delete(q.subscriptions, subject)
		delete(q.readers, subject)
	}

	// Close all writers
	for topic, writer := range q.writers {
		if err := writer.Close(); err != nil {
			lastErr = err
		}
		delete(q.writers, topic)
	}

	return lastErr
}

// Stats returns writer stats for a topic (for monitoring)
func (q *KafkaQueue) Stats(topic string) kafka.WriterStats {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if writer, exists := q.writers[topic]; exists {
		return writer.Stats()
	}
	return kafka.WriterStats{}
}

// ReaderStats returns reader stats for a topic (for monitoring)
func (q *KafkaQueue) ReaderStats(topic string) kafka.ReaderStats {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if reader, exists := q.readers[topic]; exists {
		return reader.Stats()
	}
	return kafka.ReaderStats{}
}
