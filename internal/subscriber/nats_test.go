package subscriber

import (
	"strings"
	"testing"

	"github.com/nats-io/nats.go"
)

func TestNATSSubscriber_New_InvalidURL(t *testing.T) {
	_, err := NewNATSSubscriber("nats://invalid:4222", "node1", "test-group")
	if err == nil {
		t.Fatal("expected error for invalid URL")
	}
}

func TestNATSSubscriber_GetStreamName(t *testing.T) {
	s := &NATSSubscriber{}

	tests := []struct {
		subject  string
		expected string
	}{
		{"data.write", "STREAM_data_write"},
		{"flush.trigger", "STREAM_flush_trigger"},
		{"test", "STREAM_test"},
	}

	for _, tt := range tests {
		result := s.getStreamName(tt.subject)
		if result != tt.expected {
			t.Errorf("getStreamName(%s) = %s, expected %s", tt.subject, result, tt.expected)
		}
	}
}

func TestNATSSubscriber_GetStreamName_SpecialChars(t *testing.T) {
	s := &NATSSubscriber{}

	tests := []struct {
		subject  string
		expected string
	}{
		{"soltix.write.node.123", "STREAM_soltix_write_node_123"},
		{"admin.flush", "STREAM_admin_flush"},
		{"test-with-dash", "STREAM_test_with_dash"},
	}

	for _, tt := range tests {
		result := s.getStreamName(tt.subject)
		if result != tt.expected {
			t.Errorf("getStreamName(%s) = %s, expected %s", tt.subject, result, tt.expected)
		}
	}
}

// Additional comprehensive tests

func TestNATSSubscriber_GetStreamName_EmptySubject(t *testing.T) {
	s := &NATSSubscriber{}
	result := s.getStreamName("")
	expected := "STREAM_"
	if result != expected {
		t.Errorf("expected %s, got %s", expected, result)
	}
}

func TestNATSSubscriber_GetStreamName_OnlyDots(t *testing.T) {
	s := &NATSSubscriber{}
	subject := "....."
	result := s.getStreamName(subject)
	expected := "STREAM______"
	if result != expected {
		t.Errorf("expected %s, got %s", expected, result)
	}
}

func TestNATSSubscriber_GetStreamName_OnlyDashes(t *testing.T) {
	s := &NATSSubscriber{}
	subject := "-----"
	result := s.getStreamName(subject)
	expected := "STREAM______" // 5 dashes become 5 underscores + STREAM_ prefix
	if result != expected {
		t.Errorf("expected %s, got %s", expected, result)
	}
}

func TestNATSSubscriber_GetStreamName_MixedSpecialChars(t *testing.T) {
	s := &NATSSubscriber{}
	subject := "data.write-node_123"
	result := s.getStreamName(subject)
	expected := "STREAM_data_write_node_123"
	if result != expected {
		t.Errorf("expected %s, got %s", expected, result)
	}
}

func TestNATSSubscriber_GetStreamName_Underscores(t *testing.T) {
	s := &NATSSubscriber{}
	subject := "data_write_node"
	result := s.getStreamName(subject)
	expected := "STREAM_data_write_node"
	if result != expected {
		t.Errorf("expected %s, got %s", expected, result)
	}
}

func TestNATSSubscriber_GetStreamName_VeryLongSubject(t *testing.T) {
	s := &NATSSubscriber{}
	longSubject := string(make([]byte, 1000))
	result := s.getStreamName(longSubject)
	if len(result) != 1007 { // "STREAM_" + 1000
		t.Errorf("expected length=1007, got %d", len(result))
	}
}

func TestNATSSubscriber_GetStreamName_UnicodeSubject(t *testing.T) {
	s := &NATSSubscriber{}
	subject := "测试.主题"
	result := s.getStreamName(subject)
	// Unicode characters are preserved, only dots and dashes are replaced
	if !strings.HasPrefix(result, "STREAM_") {
		t.Errorf("expected result to start with STREAM_, got %s", result)
	}
}

func TestNATSSubscriber_New_EmptyURL(t *testing.T) {
	sub, err := NewNATSSubscriber("", "node1", "test-group")
	if err != nil {
		// Expected - connection failed
		return
	}
	// If successful (uses default URL), clean up
	_ = sub.Close()
}

func TestNATSSubscriber_New_InvalidProtocol(t *testing.T) {
	sub, err := NewNATSSubscriber("http://localhost:4222", "node1", "test-group")
	if err != nil {
		// Expected - connection failed
		return
	}
	// If successful, clean up
	_ = sub.Close()
}

func TestNATSSubscriber_New_EmptyNodeID(t *testing.T) {
	_, err := NewNATSSubscriber("nats://invalid:4222", "", "test-group")
	if err == nil {
		t.Fatal("expected error for invalid URL")
	}
}

func TestNATSSubscriber_New_EmptyConsumerGroup(t *testing.T) {
	_, err := NewNATSSubscriber("nats://invalid:4222", "node1", "")
	if err == nil {
		t.Fatal("expected error for invalid URL")
	}
}

func TestNATSSubscriber_GetStreamName_MultipleDots(t *testing.T) {
	s := &NATSSubscriber{}
	subject := "level1.level2.level3.level4"
	result := s.getStreamName(subject)
	expected := "STREAM_level1_level2_level3_level4"
	if result != expected {
		t.Errorf("expected %s, got %s", expected, result)
	}
}

func TestNATSSubscriber_GetStreamName_MultipleDashes(t *testing.T) {
	s := &NATSSubscriber{}
	subject := "level1-level2-level3-level4"
	result := s.getStreamName(subject)
	expected := "STREAM_level1_level2_level3_level4"
	if result != expected {
		t.Errorf("expected %s, got %s", expected, result)
	}
}

func TestNATSSubscriber_GetStreamName_LeadingTrailingSpecialChars(t *testing.T) {
	s := &NATSSubscriber{}

	tests := []struct {
		subject  string
		expected string
	}{
		{".leading.dot", "STREAM__leading_dot"},
		{"trailing.dot.", "STREAM_trailing_dot_"},
		{"-leading-dash", "STREAM__leading_dash"},
		{"trailing-dash-", "STREAM_trailing_dash_"},
	}

	for _, tt := range tests {
		result := s.getStreamName(tt.subject)
		if result != tt.expected {
			t.Errorf("getStreamName(%s) = %s, expected %s", tt.subject, result, tt.expected)
		}
	}
}

func TestNATSSubscriber_GetStreamName_SingleCharacter(t *testing.T) {
	s := &NATSSubscriber{}

	tests := []struct {
		subject  string
		expected string
	}{
		{"a", "STREAM_a"},
		{".", "STREAM__"},
		{"-", "STREAM__"},
		{"_", "STREAM__"},
	}

	for _, tt := range tests {
		result := s.getStreamName(tt.subject)
		if result != tt.expected {
			t.Errorf("getStreamName(%s) = %s, expected %s", tt.subject, result, tt.expected)
		}
	}
}

func TestNATSSubscriber_GetStreamName_Numbers(t *testing.T) {
	s := &NATSSubscriber{}
	subject := "data.123.456"
	result := s.getStreamName(subject)
	expected := "STREAM_data_123_456"
	if result != expected {
		t.Errorf("expected %s, got %s", expected, result)
	}
}

func TestNATSSubscriber_New_LocalhostURL(t *testing.T) {
	_, err := NewNATSSubscriber("nats://localhost:14222", "node1", "test-group")
	if err == nil {
		t.Fatal("expected error for unreachable localhost")
	}
}

func TestNATSSubscriber_New_IPAddressURL(t *testing.T) {
	_, err := NewNATSSubscriber("nats://127.0.0.1:14222", "node1", "test-group")
	if err == nil {
		t.Fatal("expected error for unreachable IP")
	}
}

func TestNATSSubscriber_GetStreamName_AlphanumericOnly(t *testing.T) {
	s := &NATSSubscriber{}
	subject := "abc123XYZ"
	result := s.getStreamName(subject)
	expected := "STREAM_abc123XYZ"
	if result != expected {
		t.Errorf("expected %s, got %s", expected, result)
	}
}

func TestNATSSubscriber_GetStreamName_Consistency(t *testing.T) {
	s := &NATSSubscriber{}
	subject := "test.subject-name"

	result1 := s.getStreamName(subject)
	result2 := s.getStreamName(subject)

	if result1 != result2 {
		t.Errorf("getStreamName should be deterministic, got %s and %s", result1, result2)
	}
}

func TestNATSSubscriber_New_CustomPort(t *testing.T) {
	_, err := NewNATSSubscriber("nats://localhost:14333", "node1", "test-group")
	if err == nil {
		t.Fatal("expected error for unreachable custom port")
	}
}

func TestNATSSubscriber_UnsubscribeNonExistent(t *testing.T) {
	// Create a subscriber struct directly to test Unsubscribe without connection
	s := &NATSSubscriber{
		subscriptions: make(map[string]*nats.Subscription),
	}

	err := s.Unsubscribe("non.existent.subject")
	if err == nil {
		t.Fatal("expected error for non-existent subject")
	}
}
