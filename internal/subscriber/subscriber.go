package subscriber

import (
	"context"
)

// MessageHandler is a function that processes incoming messages
type MessageHandler func(ctx context.Context, subject string, data []byte) error

// Subscriber defines the interface for message subscription
type Subscriber interface {
	// Subscribe subscribes to a subject/topic with the given handler
	Subscribe(ctx context.Context, subject string, handler MessageHandler) error

	// Unsubscribe unsubscribes from a subject/topic
	Unsubscribe(subject string) error

	// Close closes the subscriber and releases resources
	Close() error
}

// Config holds common subscriber configuration
type Config struct {
	// NodeID is the unique identifier for this subscriber node
	NodeID string

	// ConsumerGroup is the consumer group name for group-based consumption
	ConsumerGroup string

	// MaxRetries is the maximum number of retries for failed messages
	MaxRetries int

	// BatchSize is the number of messages to fetch in a batch (where applicable)
	BatchSize int
}

// DefaultConfig returns a Config with default values
func DefaultConfig() Config {
	return Config{
		MaxRetries: 3,
		BatchSize:  100,
	}
}
