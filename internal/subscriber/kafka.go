package subscriber

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/soltixdb/soltix/internal/logging"
)

var kafkaLog = logging.Global().With("component", "subscriber.kafka")

// KafkaSubscriber implements Subscriber for Kafka
type KafkaSubscriber struct {
	brokers       []string
	consumerGroup string
	readers       map[string]*kafka.Reader
	cancels       map[string]context.CancelFunc
	mu            sync.RWMutex
}

// NewKafkaSubscriber creates a new Kafka subscriber
func NewKafkaSubscriber(brokers []string, consumerGroup string) (*KafkaSubscriber, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("at least one broker is required")
	}

	return &KafkaSubscriber{
		brokers:       brokers,
		consumerGroup: consumerGroup,
		readers:       make(map[string]*kafka.Reader),
		cancels:       make(map[string]context.CancelFunc),
	}, nil
}

// Subscribe subscribes to a topic with the given handler
func (s *KafkaSubscriber) Subscribe(ctx context.Context, subject string, handler MessageHandler) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	topic := s.topicName(subject)

	if _, exists := s.readers[topic]; exists {
		return fmt.Errorf("already subscribed to topic: %s", topic)
	}

	// Create a new reader for this topic with better configuration
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:               s.brokers,
		GroupID:               s.consumerGroup,
		Topic:                 topic,
		MinBytes:              1,
		MaxBytes:              10e6, // 10MB
		MaxWait:               3 * time.Second,
		CommitInterval:        time.Second,
		StartOffset:           kafka.LastOffset,
		HeartbeatInterval:     3 * time.Second,
		SessionTimeout:        30 * time.Second,
		RebalanceTimeout:      60 * time.Second,
		RetentionTime:         24 * time.Hour,
		WatchPartitionChanges: true,
		ErrorLogger:           kafka.LoggerFunc(func(msg string, args ...interface{}) { kafkaLog.Debug(fmt.Sprintf(msg, args...)) }),
	})

	s.readers[topic] = reader

	// Create cancellable context for this subscription
	subCtx, cancel := context.WithCancel(ctx)
	s.cancels[topic] = cancel

	// Start consuming in a goroutine
	go s.consume(subCtx, reader, subject, handler)

	kafkaLog.Info("Subscribed to Kafka topic", "topic", topic, "group", s.consumerGroup)
	return nil
}

// consume reads messages from the topic and processes them
func (s *KafkaSubscriber) consume(ctx context.Context, reader *kafka.Reader, subject string, handler MessageHandler) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			kafkaLog.Error("Failed to fetch message", "topic", reader.Config().Topic, "error", err)
			time.Sleep(time.Second)
			continue
		}

		if err := handler(ctx, subject, msg.Value); err != nil {
			kafkaLog.Error("Failed to handle message", "topic", reader.Config().Topic, "offset", msg.Offset, "error", err)
			// Don't commit - message will be reprocessed
			continue
		}

		// Commit the message
		if err := reader.CommitMessages(ctx, msg); err != nil {
			kafkaLog.Error("Failed to commit message", "topic", reader.Config().Topic, "offset", msg.Offset, "error", err)
		}
	}
}

// topicName converts a subject to a Kafka topic name
func (s *KafkaSubscriber) topicName(subject string) string {
	return subject
}

// Unsubscribe unsubscribes from a topic
func (s *KafkaSubscriber) Unsubscribe(subject string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	topic := s.topicName(subject)

	cancel, exists := s.cancels[topic]
	if !exists {
		return fmt.Errorf("not subscribed to topic: %s", topic)
	}

	cancel()
	delete(s.cancels, topic)

	reader, exists := s.readers[topic]
	if exists {
		if err := reader.Close(); err != nil {
			kafkaLog.Warn("Failed to close reader", "topic", topic, "error", err)
		}
		delete(s.readers, topic)
	}

	kafkaLog.Info("Unsubscribed from Kafka topic", "topic", topic)
	return nil
}

// Close closes all readers and subscriptions
func (s *KafkaSubscriber) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Cancel all contexts first
	for topic, cancel := range s.cancels {
		cancel()
		kafkaLog.Debug("Cancelled subscription", "topic", topic)
	}
	s.cancels = make(map[string]context.CancelFunc)

	// Close all readers
	var lastErr error
	for topic, reader := range s.readers {
		if err := reader.Close(); err != nil {
			kafkaLog.Warn("Failed to close reader", "topic", topic, "error", err)
			lastErr = err
		}
	}
	s.readers = make(map[string]*kafka.Reader)

	kafkaLog.Info("Kafka subscriber closed")
	return lastErr
}
