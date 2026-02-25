package subscriber

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/soltixdb/soltix/internal/logging"
)

var natsLog = logging.Global().With("component", "subscriber.nats")

// NATSSubscriber implements Subscriber for NATS JetStream
type NATSSubscriber struct {
	conn          *nats.Conn
	js            nats.JetStreamContext
	nodeID        string
	consumerGroup string
	subscriptions map[string]*nats.Subscription
	mu            sync.RWMutex
}

// NewNATSSubscriber creates a new NATS subscriber
func NewNATSSubscriber(url, nodeID, consumerGroup string) (*NATSSubscriber, error) {
	opts := []nats.Option{
		nats.Name(fmt.Sprintf("storage-subscriber-%s", nodeID)),
		nats.ReconnectWait(time.Second),
		nats.MaxReconnects(-1),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			if err != nil {
				natsLog.Warn("NATS disconnected", "error", err)
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			natsLog.Info("NATS reconnected", "url", nc.ConnectedUrl())
		}),
	}

	conn, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := conn.JetStream()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	return &NATSSubscriber{
		conn:          conn,
		js:            js,
		nodeID:        nodeID,
		consumerGroup: consumerGroup,
		subscriptions: make(map[string]*nats.Subscription),
	}, nil
}

// Subscribe subscribes to a subject with the given handler
func (s *NATSSubscriber) Subscribe(ctx context.Context, subject string, handler MessageHandler) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.subscriptions[subject]; exists {
		return fmt.Errorf("already subscribed to subject: %s", subject)
	}

	// Ensure stream exists
	if err := s.ensureStream(subject); err != nil {
		return err
	}

	// Create durable consumer name - must be unique per subject
	// Sanitize subject to create valid consumer name (no dots allowed)
	sanitizedSubject := strings.ReplaceAll(subject, ".", "_")
	sanitizedSubject = strings.ReplaceAll(sanitizedSubject, "*", "all")
	durableName := fmt.Sprintf("%s-%s-%s", s.consumerGroup, s.nodeID, sanitizedSubject)

	// Determine delivery policy based on subject pattern
	// Admin/broadcast subjects: DeliverNew (only new messages, no replay)
	// Node-specific subjects: DeliverAll (replay all messages for reliability)
	isAdminSubject := strings.Contains(subject, ".admin.")

	// Track message count for debugging (atomic to prevent race condition)
	var msgCount uint64

	// Build subscription options
	subOpts := []nats.SubOpt{
		nats.Durable(durableName),
		nats.ManualAck(),
		nats.MaxAckPending(100),        // Flow control - max pending messages
		nats.AckWait(30 * time.Second), // Timeout before redeliver
		nats.MaxDeliver(3),             // Max redelivery attempts
	}

	// Admin subjects use DeliverNew (broadcast, no replay)
	// Regular subjects use DeliverAll (node-specific, replay for reliability)
	if isAdminSubject {
		subOpts = append(subOpts, nats.DeliverNew())
	} else {
		subOpts = append(subOpts, nats.DeliverAll())
	}

	sub, err := s.js.Subscribe(subject, func(msg *nats.Msg) {
		// Increment message counter atomically
		currentCount := atomic.AddUint64(&msgCount, 1)

		// Check context before processing
		if ctx.Err() != nil {
			natsLog.Error("Context cancelled, skipping message",
				"subject", msg.Subject,
				"msg_count", currentCount,
				"ctx_err", ctx.Err())
			_ = msg.Nak()
			return
		}

		if err := handler(ctx, msg.Subject, msg.Data); err != nil {
			natsLog.Error("Failed to handle message",
				"subject", msg.Subject,
				"msg_count", currentCount,
				"error", err,
				"data_preview", string(msg.Data[:min(100, len(msg.Data))]))
			_ = msg.Nak()
			return
		}
		_ = msg.Ack()
	}, subOpts...)
	if err != nil {
		return fmt.Errorf("failed to subscribe to %s: %w", subject, err)
	}

	s.subscriptions[subject] = sub
	natsLog.Info("Subscribed to subject",
		"subject", subject,
		"durable", durableName,
		"is_admin", isAdminSubject)
	return nil
}

// ensureStream ensures the stream exists for the subject
func (s *NATSSubscriber) ensureStream(subject string) error {
	streamName := s.getStreamName(subject)

	// First, check if there's already a stream that contains this subject
	// by looking up the stream for this subject
	streamForSubject, err := s.js.StreamNameBySubject(subject)
	if err == nil && streamForSubject != "" {
		return nil // Stream already exists for this subject
	}

	// Check if the exact stream exists
	_, err = s.js.StreamInfo(streamName)
	if err == nil {
		return nil // Stream exists
	}

	// Determine stream retention policy based on subject pattern
	// Admin/broadcast subjects (*.admin.*) should use LimitsPolicy (fanout to all consumers)
	// Node-specific subjects should use WorkQueuePolicy (only one consumer gets the message)
	retention := nats.WorkQueuePolicy
	if strings.Contains(subject, ".admin.") {
		retention = nats.LimitsPolicy
	}

	// Create stream if it doesn't exist
	_, err = s.js.AddStream(&nats.StreamConfig{
		Name:      streamName,
		Subjects:  []string{subject},
		Retention: retention,
		MaxAge:    24 * time.Hour,
		Storage:   nats.FileStorage,
		Replicas:  1,
	})
	if err != nil && err != nats.ErrStreamNameAlreadyInUse {
		natsLog.Error("Failed to create stream", "stream", streamName, "retention", retention, "error", err)
		return fmt.Errorf("failed to create stream %s: %w", streamName, err)
	}

	natsLog.Info("Stream ensured", "stream", streamName, "subject", subject, "retention", retention)
	return nil
}

// getStreamName returns the stream name for a subject
// NATS stream names cannot contain dots, so we replace them with underscores
func (s *NATSSubscriber) getStreamName(subject string) string {
	// Replace dots and dashes with underscores for valid NATS stream name
	sanitized := strings.ReplaceAll(subject, ".", "_")
	sanitized = strings.ReplaceAll(sanitized, "-", "_")
	return fmt.Sprintf("STREAM_%s", sanitized)
}

// Unsubscribe unsubscribes from a subject
func (s *NATSSubscriber) Unsubscribe(subject string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	sub, exists := s.subscriptions[subject]
	if !exists {
		return fmt.Errorf("not subscribed to subject: %s", subject)
	}

	if err := sub.Unsubscribe(); err != nil {
		return fmt.Errorf("failed to unsubscribe from %s: %w", subject, err)
	}

	delete(s.subscriptions, subject)
	natsLog.Info("Unsubscribed from subject", "subject", subject)
	return nil
}

// Close closes all subscriptions and the connection
func (s *NATSSubscriber) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for subject, sub := range s.subscriptions {
		if err := sub.Unsubscribe(); err != nil {
			natsLog.Warn("Failed to unsubscribe", "subject", subject, "error", err)
		}
	}
	s.subscriptions = make(map[string]*nats.Subscription)

	s.conn.Close()
	natsLog.Info("NATS subscriber closed")
	return nil
}
