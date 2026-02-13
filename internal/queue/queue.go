package queue

import "context"

// Publisher publishes messages to a queue
type Publisher interface {
	// Publish publishes a message to a subject/topic
	Publish(ctx context.Context, subject string, data []byte) error

	// PublishBatch publishes multiple messages asynchronously and waits for all to complete
	// Returns the number of successfully published messages and any error
	PublishBatch(ctx context.Context, messages []BatchMessage) (int, error)

	// Close closes the connection
	Close() error
}

// BatchMessage represents a message for batch publishing
type BatchMessage struct {
	Subject string
	Data    []byte
}

// Subscriber subscribes to messages from a queue
type Subscriber interface {
	// Subscribe subscribes to a subject/topic with a handler
	Subscribe(subject string, handler MessageHandler) error

	// Unsubscribe unsubscribes from a subject/topic
	Unsubscribe(subject string) error

	// Close closes the connection
	Close() error
}

// MessageHandler handles incoming messages
type MessageHandler func(data []byte) error

// Queue combines Publisher and Subscriber interfaces
type Queue interface {
	Publisher
	Subscriber
}
