package subscriber

import (
	"context"
	"errors"
	"testing"
)

// Test Config struct fields
func TestConfig_Fields(t *testing.T) {
	cfg := Config{
		NodeID:        "node-123",
		ConsumerGroup: "test-group",
		MaxRetries:    5,
		BatchSize:     200,
	}

	if cfg.NodeID != "node-123" {
		t.Errorf("Expected NodeID=node-123, got %s", cfg.NodeID)
	}
	if cfg.ConsumerGroup != "test-group" {
		t.Errorf("Expected ConsumerGroup=test-group, got %s", cfg.ConsumerGroup)
	}
	if cfg.MaxRetries != 5 {
		t.Errorf("Expected MaxRetries=5, got %d", cfg.MaxRetries)
	}
	if cfg.BatchSize != 200 {
		t.Errorf("Expected BatchSize=200, got %d", cfg.BatchSize)
	}
}

// Test Config with zero values
func TestConfig_ZeroValues(t *testing.T) {
	cfg := Config{}

	if cfg.NodeID != "" {
		t.Errorf("Expected empty NodeID, got %s", cfg.NodeID)
	}
	if cfg.ConsumerGroup != "" {
		t.Errorf("Expected empty ConsumerGroup, got %s", cfg.ConsumerGroup)
	}
	if cfg.MaxRetries != 0 {
		t.Errorf("Expected MaxRetries=0, got %d", cfg.MaxRetries)
	}
	if cfg.BatchSize != 0 {
		t.Errorf("Expected BatchSize=0, got %d", cfg.BatchSize)
	}
}

// Test Config with negative values
func TestConfig_NegativeValues(t *testing.T) {
	cfg := Config{
		MaxRetries: -1,
		BatchSize:  -100,
	}

	if cfg.MaxRetries != -1 {
		t.Errorf("Expected MaxRetries=-1, got %d", cfg.MaxRetries)
	}
	if cfg.BatchSize != -100 {
		t.Errorf("Expected BatchSize=-100, got %d", cfg.BatchSize)
	}
}

// Test Config with large values
func TestConfig_LargeValues(t *testing.T) {
	cfg := Config{
		MaxRetries: 1000000,
		BatchSize:  1000000,
	}

	if cfg.MaxRetries != 1000000 {
		t.Errorf("Expected MaxRetries=1000000, got %d", cfg.MaxRetries)
	}
	if cfg.BatchSize != 1000000 {
		t.Errorf("Expected BatchSize=1000000, got %d", cfg.BatchSize)
	}
}

// Test DefaultConfig returns correct defaults
func TestDefaultConfig_Values(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.MaxRetries != 3 {
		t.Errorf("Expected MaxRetries=3, got %d", cfg.MaxRetries)
	}
	if cfg.BatchSize != 100 {
		t.Errorf("Expected BatchSize=100, got %d", cfg.BatchSize)
	}
	if cfg.NodeID != "" {
		t.Errorf("Expected empty NodeID, got %s", cfg.NodeID)
	}
	if cfg.ConsumerGroup != "" {
		t.Errorf("Expected empty ConsumerGroup, got %s", cfg.ConsumerGroup)
	}
}

// Test DefaultConfig returns new instance each time
func TestDefaultConfig_NewInstance(t *testing.T) {
	cfg1 := DefaultConfig()
	cfg2 := DefaultConfig()

	cfg1.MaxRetries = 10

	if cfg2.MaxRetries == 10 {
		t.Error("DefaultConfig should return independent instances")
	}
}

// Test MessageHandler function signature
func TestMessageHandler_Success(t *testing.T) {
	called := false
	var handler MessageHandler = func(ctx context.Context, subject string, data []byte) error {
		called = true
		if subject != "test.subject" {
			t.Errorf("Expected subject=test.subject, got %s", subject)
		}
		if string(data) != "test data" {
			t.Errorf("Expected data='test data', got %s", string(data))
		}
		return nil
	}

	ctx := context.Background()
	err := handler(ctx, "test.subject", []byte("test data"))
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if !called {
		t.Error("Handler was not called")
	}
}

// Test MessageHandler with error
func TestMessageHandler_Error(t *testing.T) {
	expectedErr := errors.New("handler error")
	var handler MessageHandler = func(ctx context.Context, subject string, data []byte) error {
		return expectedErr
	}

	ctx := context.Background()
	err := handler(ctx, "test.subject", []byte("test data"))
	if err != expectedErr {
		t.Errorf("Expected error=%v, got %v", expectedErr, err)
	}
}

// Test MessageHandler with cancelled context
func TestMessageHandler_CancelledContext(t *testing.T) {
	var handler MessageHandler = func(ctx context.Context, subject string, data []byte) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := handler(ctx, "test.subject", []byte("test data"))
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled error, got %v", err)
	}
}

// Test MessageHandler with nil data
func TestMessageHandler_NilData(t *testing.T) {
	var receivedData []byte
	var handler MessageHandler = func(ctx context.Context, subject string, data []byte) error {
		receivedData = data
		return nil
	}

	ctx := context.Background()
	err := handler(ctx, "test.subject", nil)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if receivedData != nil {
		t.Errorf("Expected nil data, got %v", receivedData)
	}
}

// Test MessageHandler with empty data
func TestMessageHandler_EmptyData(t *testing.T) {
	var receivedData []byte
	var handler MessageHandler = func(ctx context.Context, subject string, data []byte) error {
		receivedData = data
		return nil
	}

	ctx := context.Background()
	err := handler(ctx, "test.subject", []byte{})
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if len(receivedData) != 0 {
		t.Errorf("Expected empty data, got %v", receivedData)
	}
}

// Test MessageHandler with empty subject
func TestMessageHandler_EmptySubject(t *testing.T) {
	var receivedSubject string
	var handler MessageHandler = func(ctx context.Context, subject string, data []byte) error {
		receivedSubject = subject
		return nil
	}

	ctx := context.Background()
	err := handler(ctx, "", []byte("test data"))
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if receivedSubject != "" {
		t.Errorf("Expected empty subject, got %s", receivedSubject)
	}
}

// Test Config with special characters in strings
func TestConfig_SpecialCharacters(t *testing.T) {
	cfg := Config{
		NodeID:        "node-123_test!@#",
		ConsumerGroup: "group:with:colons",
	}

	if cfg.NodeID != "node-123_test!@#" {
		t.Errorf("Expected NodeID with special chars, got %s", cfg.NodeID)
	}
	if cfg.ConsumerGroup != "group:with:colons" {
		t.Errorf("Expected ConsumerGroup with colons, got %s", cfg.ConsumerGroup)
	}
}

// Test Config with unicode characters
func TestConfig_UnicodeCharacters(t *testing.T) {
	cfg := Config{
		NodeID:        "节点-123",
		ConsumerGroup: "组-测试",
	}

	if cfg.NodeID != "节点-123" {
		t.Errorf("Expected NodeID with unicode, got %s", cfg.NodeID)
	}
	if cfg.ConsumerGroup != "组-测试" {
		t.Errorf("Expected ConsumerGroup with unicode, got %s", cfg.ConsumerGroup)
	}
}

// Test Config with very long strings
func TestConfig_LongStrings(t *testing.T) {
	longString := string(make([]byte, 10000))
	cfg := Config{
		NodeID:        longString,
		ConsumerGroup: longString,
	}

	if len(cfg.NodeID) != 10000 {
		t.Errorf("Expected NodeID length=10000, got %d", len(cfg.NodeID))
	}
	if len(cfg.ConsumerGroup) != 10000 {
		t.Errorf("Expected ConsumerGroup length=10000, got %d", len(cfg.ConsumerGroup))
	}
}

// Test MessageHandler with large data
func TestMessageHandler_LargeData(t *testing.T) {
	largeData := make([]byte, 1024*1024) // 1MB
	var receivedSize int
	var handler MessageHandler = func(ctx context.Context, subject string, data []byte) error {
		receivedSize = len(data)
		return nil
	}

	ctx := context.Background()
	err := handler(ctx, "test.subject", largeData)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if receivedSize != 1024*1024 {
		t.Errorf("Expected data size=1048576, got %d", receivedSize)
	}
}
