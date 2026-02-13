package subscriber

import (
	"context"
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestKafkaSubscriber_New(t *testing.T) {
	sub, err := NewKafkaSubscriber([]string{"localhost:9092"}, "test-group")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = sub.Close() }()

	if sub == nil {
		t.Fatal("expected non-nil subscriber")
	}

	if sub.consumerGroup != "test-group" {
		t.Errorf("expected consumerGroup=test-group, got %s", sub.consumerGroup)
	}
}

func TestKafkaSubscriber_New_NoBrokers(t *testing.T) {
	_, err := NewKafkaSubscriber([]string{}, "test-group")
	if err == nil {
		t.Fatal("expected error for empty brokers")
	}
}

func TestKafkaSubscriber_TopicName(t *testing.T) {
	s := &KafkaSubscriber{}

	tests := []struct {
		subject  string
		expected string
	}{
		{"data.write", "data.write"},
		{"flush.trigger", "flush.trigger"},
		{"test_topic", "test_topic"},
	}

	for _, tt := range tests {
		result := s.topicName(tt.subject)
		if result != tt.expected {
			t.Errorf("topicName(%s) = %s, expected %s", tt.subject, result, tt.expected)
		}
	}
}

func TestKafkaSubscriber_SubscribeDuplicate(t *testing.T) {
	sub, err := NewKafkaSubscriber([]string{"localhost:9092"}, "test-group")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sub.mu.Lock()
	dummyReader := &kafka.Reader{}
	sub.readers["test.topic"] = dummyReader
	sub.mu.Unlock()

	ctx := context.Background()
	err = sub.Subscribe(ctx, "test.topic", func(ctx context.Context, subject string, data []byte) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected error for duplicate subscription")
	}

	sub.mu.Lock()
	delete(sub.readers, "test.topic")
	sub.mu.Unlock()
	_ = sub.Close()
}

func TestKafkaSubscriber_UnsubscribeNonExistent(t *testing.T) {
	sub, err := NewKafkaSubscriber([]string{"localhost:9092"}, "test-group")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = sub.Close() }()

	err = sub.Unsubscribe("non.existent.topic")
	if err == nil {
		t.Fatal("expected error for non-existent subscription")
	}
}

func TestKafkaSubscriber_Close(t *testing.T) {
	sub, err := NewKafkaSubscriber([]string{"localhost:9092"}, "test-group")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = sub.Close()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sub.mu.RLock()
	readersCount := len(sub.readers)
	cancelsCount := len(sub.cancels)
	sub.mu.RUnlock()

	if readersCount != 0 {
		t.Errorf("expected 0 readers after close, got %d", readersCount)
	}
	if cancelsCount != 0 {
		t.Errorf("expected 0 cancels after close, got %d", cancelsCount)
	}
}

func TestKafkaSubscriber_TopicName_SpecialChars(t *testing.T) {
	s := &KafkaSubscriber{}

	tests := []struct {
		subject  string
		expected string
	}{
		{"soltix.write.node.123", "soltix.write.node.123"},
		{"admin-flush", "admin-flush"},
		{"test_with_underscore", "test_with_underscore"},
	}

	for _, tt := range tests {
		result := s.topicName(tt.subject)
		if result != tt.expected {
			t.Errorf("topicName(%s) = %s, expected %s", tt.subject, result, tt.expected)
		}
	}
}

func TestKafkaSubscriber_MultipleBrokers(t *testing.T) {
	brokers := []string{"broker1:9092", "broker2:9092", "broker3:9092"}
	sub, err := NewKafkaSubscriber(brokers, "multi-broker-group")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = sub.Close() }()

	if len(sub.brokers) != 3 {
		t.Errorf("expected 3 brokers, got %d", len(sub.brokers))
	}

	if sub.consumerGroup != "multi-broker-group" {
		t.Errorf("expected consumerGroup=multi-broker-group, got %s", sub.consumerGroup)
	}
}

// Additional comprehensive tests

func TestKafkaSubscriber_New_NilBrokers(t *testing.T) {
	_, err := NewKafkaSubscriber(nil, "test-group")
	if err == nil {
		t.Fatal("expected error for nil brokers")
	}
}

func TestKafkaSubscriber_New_EmptyConsumerGroup(t *testing.T) {
	sub, err := NewKafkaSubscriber([]string{"localhost:9092"}, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = sub.Close() }()

	if sub.consumerGroup != "" {
		t.Errorf("expected empty consumerGroup, got %s", sub.consumerGroup)
	}
}

func TestKafkaSubscriber_TopicName_EmptySubject(t *testing.T) {
	s := &KafkaSubscriber{}
	result := s.topicName("")
	if result != "" {
		t.Errorf("expected empty topic name, got %s", result)
	}
}

func TestKafkaSubscriber_TopicName_UnicodeSubject(t *testing.T) {
	s := &KafkaSubscriber{}
	subject := "测试.主题"
	result := s.topicName(subject)
	if result != subject {
		t.Errorf("expected %s, got %s", subject, result)
	}
}

func TestKafkaSubscriber_CloseEmpty(t *testing.T) {
	sub, err := NewKafkaSubscriber([]string{"localhost:9092"}, "test-group")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = sub.Close()
	if err != nil {
		t.Fatalf("unexpected error on empty close: %v", err)
	}
}

func TestKafkaSubscriber_CloseIdempotent(t *testing.T) {
	sub, err := NewKafkaSubscriber([]string{"localhost:9092"}, "test-group")
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

func TestKafkaSubscriber_BrokersField(t *testing.T) {
	brokers := []string{"broker1:9092", "broker2:9093"}
	sub, err := NewKafkaSubscriber(brokers, "test-group")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = sub.Close() }()

	if sub.brokers[0] != "broker1:9092" {
		t.Errorf("expected first broker=broker1:9092, got %s", sub.brokers[0])
	}
	if sub.brokers[1] != "broker2:9093" {
		t.Errorf("expected second broker=broker2:9093, got %s", sub.brokers[1])
	}
}

func TestKafkaSubscriber_InitialState(t *testing.T) {
	sub, err := NewKafkaSubscriber([]string{"localhost:9092"}, "test-group")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = sub.Close() }()

	sub.mu.RLock()
	readersCount := len(sub.readers)
	cancelsCount := len(sub.cancels)
	sub.mu.RUnlock()

	if readersCount != 0 {
		t.Errorf("expected 0 initial readers, got %d", readersCount)
	}
	if cancelsCount != 0 {
		t.Errorf("expected 0 initial cancels, got %d", cancelsCount)
	}
}

func TestKafkaSubscriber_ConsumerGroupField(t *testing.T) {
	sub, err := NewKafkaSubscriber([]string{"localhost:9092"}, "my-consumer-group")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = sub.Close() }()

	if sub.consumerGroup != "my-consumer-group" {
		t.Errorf("expected consumerGroup=my-consumer-group, got %s", sub.consumerGroup)
	}
}

func TestKafkaSubscriber_TopicName_VeryLongSubject(t *testing.T) {
	s := &KafkaSubscriber{}
	longSubject := string(make([]byte, 1000))
	result := s.topicName(longSubject)
	if len(result) != 1000 {
		t.Errorf("expected topic name length=1000, got %d", len(result))
	}
}

func TestKafkaSubscriber_SingleBroker(t *testing.T) {
	sub, err := NewKafkaSubscriber([]string{"single-broker:9092"}, "test-group")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = sub.Close() }()

	if len(sub.brokers) != 1 {
		t.Errorf("expected 1 broker, got %d", len(sub.brokers))
	}
	if sub.brokers[0] != "single-broker:9092" {
		t.Errorf("expected broker=single-broker:9092, got %s", sub.brokers[0])
	}
}

func TestKafkaSubscriber_TopicName_WithDots(t *testing.T) {
	s := &KafkaSubscriber{}
	subject := "data.write.node.123"
	result := s.topicName(subject)
	if result != subject {
		t.Errorf("expected %s, got %s", subject, result)
	}
}

func TestKafkaSubscriber_TopicName_WithDashes(t *testing.T) {
	s := &KafkaSubscriber{}
	subject := "data-write-node-123"
	result := s.topicName(subject)
	if result != subject {
		t.Errorf("expected %s, got %s", subject, result)
	}
}

func TestKafkaSubscriber_TopicName_WithUnderscores(t *testing.T) {
	s := &KafkaSubscriber{}
	subject := "data_write_node_123"
	result := s.topicName(subject)
	if result != subject {
		t.Errorf("expected %s, got %s", subject, result)
	}
}

func TestKafkaSubscriber_TopicName_MixedSpecialChars(t *testing.T) {
	s := &KafkaSubscriber{}
	subject := "data.write-node_123"
	result := s.topicName(subject)
	if result != subject {
		t.Errorf("expected %s, got %s", subject, result)
	}
}

func TestKafkaSubscriber_New_ConsumerGroupWithSpecialChars(t *testing.T) {
	sub, err := NewKafkaSubscriber([]string{"localhost:9092"}, "group-with-dash_and_underscore")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = sub.Close() }()

	if sub.consumerGroup != "group-with-dash_and_underscore" {
		t.Errorf("unexpected consumerGroup: %s", sub.consumerGroup)
	}
}

func TestKafkaSubscriber_New_BrokerWithIPAddress(t *testing.T) {
	sub, err := NewKafkaSubscriber([]string{"192.168.1.100:9092"}, "test-group")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = sub.Close() }()

	if sub.brokers[0] != "192.168.1.100:9092" {
		t.Errorf("expected broker=192.168.1.100:9092, got %s", sub.brokers[0])
	}
}

func TestKafkaSubscriber_New_BrokerWithCustomPort(t *testing.T) {
	sub, err := NewKafkaSubscriber([]string{"localhost:19092"}, "test-group")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = sub.Close() }()

	if sub.brokers[0] != "localhost:19092" {
		t.Errorf("expected broker=localhost:19092, got %s", sub.brokers[0])
	}
}
