package subscriber

import (
	"context"
	"fmt"
	"sync"

	"github.com/soltixdb/soltix/internal/logging"
)

var memoryLog = logging.Global().With("component", "subscriber.memory")

// memorySubscription represents an active subscription
type memorySubscription struct {
	handler MessageHandler
	ctx     context.Context
	cancel  context.CancelFunc
	ch      chan memoryMessage
}

type memoryMessage struct {
	subject string
	data    []byte
}

// MemorySubscriber implements Subscriber for in-memory queue
type MemorySubscriber struct {
	subscriptions map[string]*memorySubscription
	mu            sync.RWMutex
}

// memoryBroker is a global in-memory message broker for testing
var (
	memBroker     *memoryBroker
	memBrokerOnce sync.Once
)

type memoryBroker struct {
	subscribers map[string][]*memorySubscription
	mu          sync.RWMutex
}

func getMemoryBroker() *memoryBroker {
	memBrokerOnce.Do(func() {
		memBroker = &memoryBroker{
			subscribers: make(map[string][]*memorySubscription),
		}
	})
	return memBroker
}

// PublishToMemory publishes a message to all memory subscribers (for testing)
func PublishToMemory(subject string, data []byte) {
	b := getMemoryBroker()
	b.mu.RLock()
	subs := b.subscribers[subject]
	b.mu.RUnlock()

	for _, sub := range subs {
		select {
		case sub.ch <- memoryMessage{subject: subject, data: data}:
		default:
			memoryLog.Warn("Subscriber channel full, dropping message", "subject", subject)
		}
	}
}

// NewMemorySubscriber creates a new in-memory subscriber
func NewMemorySubscriber() (*MemorySubscriber, error) {
	return &MemorySubscriber{
		subscriptions: make(map[string]*memorySubscription),
	}, nil
}

// Subscribe subscribes to a subject with the given handler
func (s *MemorySubscriber) Subscribe(ctx context.Context, subject string, handler MessageHandler) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.subscriptions[subject]; exists {
		return fmt.Errorf("already subscribed to subject: %s", subject)
	}

	subCtx, cancel := context.WithCancel(ctx)
	sub := &memorySubscription{
		handler: handler,
		ctx:     subCtx,
		cancel:  cancel,
		ch:      make(chan memoryMessage, 1000),
	}

	s.subscriptions[subject] = sub

	// Register with global broker
	b := getMemoryBroker()
	b.mu.Lock()
	b.subscribers[subject] = append(b.subscribers[subject], sub)
	b.mu.Unlock()

	// Start consuming in a goroutine
	go s.consume(sub, subject)

	memoryLog.Info("Subscribed to in-memory subject", "subject", subject)
	return nil
}

// consume reads messages and processes them
func (s *MemorySubscriber) consume(sub *memorySubscription, subject string) {
	for {
		select {
		case <-sub.ctx.Done():
			return
		case msg := <-sub.ch:
			if err := sub.handler(sub.ctx, msg.subject, msg.data); err != nil {
				memoryLog.Error("Failed to handle message", "subject", msg.subject, "error", err)
			}
		}
	}
}

// Unsubscribe unsubscribes from a subject
func (s *MemorySubscriber) Unsubscribe(subject string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	sub, exists := s.subscriptions[subject]
	if !exists {
		return fmt.Errorf("not subscribed to subject: %s", subject)
	}

	sub.cancel()
	delete(s.subscriptions, subject)

	// Unregister from global broker
	b := getMemoryBroker()
	b.mu.Lock()
	subs := b.subscribers[subject]
	for i, bs := range subs {
		if bs == sub {
			b.subscribers[subject] = append(subs[:i], subs[i+1:]...)
			break
		}
	}
	b.mu.Unlock()

	memoryLog.Info("Unsubscribed from in-memory subject", "subject", subject)
	return nil
}

// Close closes all subscriptions
func (s *MemorySubscriber) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	b := getMemoryBroker()

	for subject, sub := range s.subscriptions {
		sub.cancel()

		// Unregister from global broker
		b.mu.Lock()
		subs := b.subscribers[subject]
		for i, bs := range subs {
			if bs == sub {
				b.subscribers[subject] = append(subs[:i], subs[i+1:]...)
				break
			}
		}
		b.mu.Unlock()
	}
	s.subscriptions = make(map[string]*memorySubscription)

	memoryLog.Info("Memory subscriber closed")
	return nil
}
