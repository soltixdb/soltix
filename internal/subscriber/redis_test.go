package subscriber

import (
	"context"
	"testing"
)

func TestRedisSubscriber_StreamName(t *testing.T) {
	s := &RedisSubscriber{streamPrefix: "soltix"}

	tests := []struct {
		subject  string
		expected string
	}{
		{"data.write", "soltix:data.write"},
		{"flush.trigger", "soltix:flush.trigger"},
		{"test", "soltix:test"},
	}

	for _, tt := range tests {
		result := s.streamName(tt.subject)
		if result != tt.expected {
			t.Errorf("streamName(%s) = %s, expected %s", tt.subject, result, tt.expected)
		}
	}
}

func TestRedisSubscriber_New_InvalidAddr(t *testing.T) {
	_, err := NewRedisSubscriber("invalid:6379", "", 0, "soltix", "test-group", "consumer1")
	if err == nil {
		t.Fatal("expected error for invalid address")
	}
}

func TestRedisSubscriber_StreamName_SpecialChars(t *testing.T) {
	s := &RedisSubscriber{streamPrefix: "soltix"}

	tests := []struct {
		subject  string
		expected string
	}{
		{"soltix.write.node.123", "soltix:soltix.write.node.123"},
		{"admin.flush", "soltix:admin.flush"},
		{"test-with-dash", "soltix:test-with-dash"},
	}

	for _, tt := range tests {
		result := s.streamName(tt.subject)
		if result != tt.expected {
			t.Errorf("streamName(%s) = %s, expected %s", tt.subject, result, tt.expected)
		}
	}
}

// Additional comprehensive tests

func TestRedisSubscriber_StreamName_EmptyPrefix(t *testing.T) {
	s := &RedisSubscriber{streamPrefix: ""}

	tests := []struct {
		subject  string
		expected string
	}{
		{"data.write", ":data.write"},
		{"flush.trigger", ":flush.trigger"},
	}

	for _, tt := range tests {
		result := s.streamName(tt.subject)
		if result != tt.expected {
			t.Errorf("streamName(%s) = %s, expected %s", tt.subject, result, tt.expected)
		}
	}
}

func TestRedisSubscriber_StreamName_EmptySubject(t *testing.T) {
	s := &RedisSubscriber{streamPrefix: "soltix"}
	result := s.streamName("")
	expected := "soltix:"
	if result != expected {
		t.Errorf("expected %s, got %s", expected, result)
	}
}

func TestRedisSubscriber_StreamName_LongPrefix(t *testing.T) {
	longPrefix := string(make([]byte, 1000))
	s := &RedisSubscriber{streamPrefix: longPrefix}
	result := s.streamName("test")
	if len(result) != 1005 { // 1000 + ":" + 4
		t.Errorf("expected length=1005, got %d", len(result))
	}
}

func TestRedisSubscriber_StreamName_UnicodeSubject(t *testing.T) {
	s := &RedisSubscriber{streamPrefix: "soltix"}
	subject := "测试.主题"
	result := s.streamName(subject)
	expected := "soltix:测试.主题"
	if result != expected {
		t.Errorf("expected %s, got %s", expected, result)
	}
}

func TestRedisSubscriber_New_EmptyAddr(t *testing.T) {
	sub, err := NewRedisSubscriber("", "", 0, "soltix", "test-group", "consumer1")
	if err != nil {
		// Expected - connection failed
		return
	}
	// If successful (uses default), clean up
	_ = sub.Close()
}

func TestRedisSubscriber_New_InvalidPort(t *testing.T) {
	_, err := NewRedisSubscriber("localhost:99999", "", 0, "soltix", "test-group", "consumer1")
	if err == nil {
		t.Fatal("expected error for invalid port")
	}
}

func TestRedisSubscriber_New_WithPassword(t *testing.T) {
	_, err := NewRedisSubscriber("invalid:6379", "secret-password", 0, "soltix", "test-group", "consumer1")
	if err == nil {
		t.Fatal("expected error for invalid address")
	}
}

func TestRedisSubscriber_New_WithDB(t *testing.T) {
	_, err := NewRedisSubscriber("invalid:6379", "", 5, "soltix", "test-group", "consumer1")
	if err == nil {
		t.Fatal("expected error for invalid address")
	}
}

func TestRedisSubscriber_New_EmptyStreamPrefix(t *testing.T) {
	_, err := NewRedisSubscriber("invalid:6379", "", 0, "", "test-group", "consumer1")
	if err == nil {
		t.Fatal("expected error for invalid address")
	}
}

func TestRedisSubscriber_New_EmptyConsumerGroup(t *testing.T) {
	_, err := NewRedisSubscriber("invalid:6379", "", 0, "soltix", "", "consumer1")
	if err == nil {
		t.Fatal("expected error for invalid address")
	}
}

func TestRedisSubscriber_New_EmptyConsumerID(t *testing.T) {
	_, err := NewRedisSubscriber("invalid:6379", "", 0, "soltix", "test-group", "")
	if err == nil {
		t.Fatal("expected error for invalid address")
	}
}

func TestRedisSubscriber_StreamName_WithUnderscores(t *testing.T) {
	s := &RedisSubscriber{streamPrefix: "soltix"}
	subject := "data_write_node_123"
	result := s.streamName(subject)
	expected := "soltix:data_write_node_123"
	if result != expected {
		t.Errorf("expected %s, got %s", expected, result)
	}
}

func TestRedisSubscriber_StreamName_MixedSpecialChars(t *testing.T) {
	s := &RedisSubscriber{streamPrefix: "custom-prefix"}
	subject := "data.write-node_123"
	result := s.streamName(subject)
	expected := "custom-prefix:data.write-node_123"
	if result != expected {
		t.Errorf("expected %s, got %s", expected, result)
	}
}

func TestRedisSubscriber_StreamName_MultipleColons(t *testing.T) {
	s := &RedisSubscriber{streamPrefix: "prefix:with:colons"}
	subject := "test:subject"
	result := s.streamName(subject)
	expected := "prefix:with:colons:test:subject"
	if result != expected {
		t.Errorf("expected %s, got %s", expected, result)
	}
}

func TestRedisSubscriber_New_AddressWithHTTP(t *testing.T) {
	_, err := NewRedisSubscriber("http://localhost:6379", "", 0, "soltix", "test-group", "consumer1")
	if err == nil {
		t.Fatal("expected error for HTTP address")
	}
}

func TestRedisSubscriber_New_IPv6Address(t *testing.T) {
	sub, err := NewRedisSubscriber("[::1]:6379", "", 0, "soltix", "test-group", "consumer1")
	if err != nil {
		// Expected - connection failed
		return
	}
	// If successful, clean up
	_ = sub.Close()
}

func TestRedisSubscriber_StreamName_VeryLongSubject(t *testing.T) {
	s := &RedisSubscriber{streamPrefix: "soltix"}
	longSubject := string(make([]byte, 10000))
	result := s.streamName(longSubject)
	if len(result) != 10007 { // "soltix:" + 10000
		t.Errorf("expected length=10007, got %d", len(result))
	}
}

func TestRedisSubscriber_New_DBNegative(t *testing.T) {
	_, err := NewRedisSubscriber("invalid:6379", "", -1, "soltix", "test-group", "consumer1")
	if err == nil {
		t.Fatal("expected error for invalid address")
	}
}

func TestRedisSubscriber_New_DBLarge(t *testing.T) {
	_, err := NewRedisSubscriber("invalid:6379", "", 15, "soltix", "test-group", "consumer1")
	if err == nil {
		t.Fatal("expected error for invalid address")
	}
}

func TestRedisSubscriber_StreamName_OnlyDots(t *testing.T) {
	s := &RedisSubscriber{streamPrefix: "soltix"}
	subject := "....."
	result := s.streamName(subject)
	expected := "soltix:....."
	if result != expected {
		t.Errorf("expected %s, got %s", expected, result)
	}
}

func TestRedisSubscriber_StreamName_OnlyDashes(t *testing.T) {
	s := &RedisSubscriber{streamPrefix: "soltix"}
	subject := "-----"
	result := s.streamName(subject)
	expected := "soltix:-----"
	if result != expected {
		t.Errorf("expected %s, got %s", expected, result)
	}
}

func TestRedisSubscriber_UnsubscribeNonExistent(t *testing.T) {
	// Create a subscriber struct directly to test Unsubscribe without connection
	s := &RedisSubscriber{
		streamPrefix:  "soltix",
		subscriptions: make(map[string]context.CancelFunc),
	}

	err := s.Unsubscribe("non.existent.stream")
	if err == nil {
		t.Fatal("expected error for non-existent stream")
	}
}
