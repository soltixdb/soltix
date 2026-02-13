package subscriber

import (
	"testing"

	"github.com/soltixdb/soltix/internal/config"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.MaxRetries != 3 {
		t.Errorf("expected MaxRetries=3, got %d", cfg.MaxRetries)
	}

	if cfg.BatchSize != 100 {
		t.Errorf("expected BatchSize=100, got %d", cfg.BatchSize)
	}
}

func TestConfig_DefaultValues(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.NodeID != "" {
		t.Errorf("expected empty NodeID, got %s", cfg.NodeID)
	}

	if cfg.ConsumerGroup != "" {
		t.Errorf("expected empty ConsumerGroup, got %s", cfg.ConsumerGroup)
	}

	if cfg.MaxRetries != 3 {
		t.Errorf("expected MaxRetries=3, got %d", cfg.MaxRetries)
	}

	if cfg.BatchSize != 100 {
		t.Errorf("expected BatchSize=100, got %d", cfg.BatchSize)
	}
}

func TestConfig_CustomValues(t *testing.T) {
	cfg := Config{
		NodeID:        "node-123",
		ConsumerGroup: "my-group",
		MaxRetries:    5,
		BatchSize:     200,
	}

	if cfg.NodeID != "node-123" {
		t.Errorf("expected NodeID=node-123, got %s", cfg.NodeID)
	}

	if cfg.ConsumerGroup != "my-group" {
		t.Errorf("expected ConsumerGroup=my-group, got %s", cfg.ConsumerGroup)
	}

	if cfg.MaxRetries != 5 {
		t.Errorf("expected MaxRetries=5, got %d", cfg.MaxRetries)
	}

	if cfg.BatchSize != 200 {
		t.Errorf("expected BatchSize=200, got %d", cfg.BatchSize)
	}
}

func TestNewSubscriber_Memory(t *testing.T) {
	cfg := config.QueueConfig{
		Type: "memory",
	}
	subCfg := DefaultConfig()

	sub, err := NewSubscriber(cfg, subCfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = sub.Close() }()

	_, ok := sub.(*MemorySubscriber)
	if !ok {
		t.Error("expected MemorySubscriber type")
	}
}

func TestNewSubscriber_DefaultToNATS(t *testing.T) {
	cfg := config.QueueConfig{
		Type: "",
		URL:  "nats://invalid:4222",
	}
	subCfg := DefaultConfig()

	_, err := NewSubscriber(cfg, subCfg)
	if err == nil {
		t.Fatal("expected error for invalid NATS URL")
	}
}

func TestNewSubscriber_UnsupportedType(t *testing.T) {
	cfg := config.QueueConfig{
		Type: "unsupported",
	}
	subCfg := DefaultConfig()

	_, err := NewSubscriber(cfg, subCfg)
	if err == nil {
		t.Fatal("expected error for unsupported queue type")
	}
}

func TestNewSubscriber_Kafka(t *testing.T) {
	cfg := config.QueueConfig{
		Type:         "kafka",
		KafkaBrokers: []string{"localhost:9092"},
	}
	subCfg := Config{
		ConsumerGroup: "test-group",
	}

	sub, err := NewSubscriber(cfg, subCfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = sub.Close() }()

	_, ok := sub.(*KafkaSubscriber)
	if !ok {
		t.Error("expected KafkaSubscriber type")
	}
}

func TestNewSubscriber_MemoryUpperCase(t *testing.T) {
	cfg := config.QueueConfig{
		Type: "MEMORY",
	}
	subCfg := DefaultConfig()

	sub, err := NewSubscriber(cfg, subCfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = sub.Close() }()

	_, ok := sub.(*MemorySubscriber)
	if !ok {
		t.Error("expected MemorySubscriber type")
	}
}

func TestNewSubscriber_KafkaMixedCase(t *testing.T) {
	cfg := config.QueueConfig{
		Type:         "Kafka",
		KafkaBrokers: []string{"localhost:9092"},
	}
	subCfg := Config{
		ConsumerGroup: "test-group",
	}

	sub, err := NewSubscriber(cfg, subCfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = sub.Close() }()

	_, ok := sub.(*KafkaSubscriber)
	if !ok {
		t.Error("expected KafkaSubscriber type")
	}
}

func TestNewSubscriber_RedisInvalidURL(t *testing.T) {
	cfg := config.QueueConfig{
		Type: "redis",
		URL:  "invalid:6379",
	}
	subCfg := Config{
		NodeID:        "node1",
		ConsumerGroup: "test-group",
	}

	_, err := NewSubscriber(cfg, subCfg)
	if err == nil {
		t.Fatal("expected error for invalid Redis URL")
	}
}

func TestNewSubscriber_NATSExplicit(t *testing.T) {
	cfg := config.QueueConfig{
		Type: "nats",
		URL:  "nats://invalid:4222",
	}
	subCfg := DefaultConfig()

	_, err := NewSubscriber(cfg, subCfg)
	if err == nil {
		t.Fatal("expected error for invalid NATS URL")
	}
}

func TestNewSubscriber_RedisWithDefaults(t *testing.T) {
	cfg := config.QueueConfig{
		Type: "redis",
		URL:  "",
	}
	subCfg := Config{
		NodeID:        "node1",
		ConsumerGroup: "test-group",
	}

	sub, err := NewSubscriber(cfg, subCfg)
	if err != nil {
		// Expected - localhost:6379 not available
		return
	}
	// If successful (rare), clean up
	_ = sub.Close()
}

func TestNewSubscriber_RedisWithStreamPrefix(t *testing.T) {
	cfg := config.QueueConfig{
		Type:        "redis",
		URL:         "invalid:6379",
		RedisStream: "custom-prefix",
	}
	subCfg := Config{
		NodeID:        "node1",
		ConsumerGroup: "test-group",
	}

	_, err := NewSubscriber(cfg, subCfg)
	if err == nil {
		t.Fatal("expected error for invalid Redis address")
	}
}

func TestNewSubscriber_RedisEmptyStreamPrefix(t *testing.T) {
	cfg := config.QueueConfig{
		Type:        "redis",
		URL:         "invalid:6379",
		RedisStream: "",
	}
	subCfg := Config{
		NodeID:        "node1",
		ConsumerGroup: "test-group",
	}

	_, err := NewSubscriber(cfg, subCfg)
	if err == nil {
		t.Fatal("expected error for invalid Redis address")
	}
}

func TestNewSubscriber_RedisWithPassword(t *testing.T) {
	cfg := config.QueueConfig{
		Type:     "redis",
		URL:      "invalid:6379",
		Password: "secret",
	}
	subCfg := Config{
		NodeID:        "node1",
		ConsumerGroup: "test-group",
	}

	_, err := NewSubscriber(cfg, subCfg)
	if err == nil {
		t.Fatal("expected error for invalid Redis address")
	}
}

func TestNewSubscriber_RedisWithDB(t *testing.T) {
	cfg := config.QueueConfig{
		Type:    "redis",
		URL:     "invalid:6379",
		RedisDB: 5,
	}
	subCfg := Config{
		NodeID:        "node1",
		ConsumerGroup: "test-group",
	}

	_, err := NewSubscriber(cfg, subCfg)
	if err == nil {
		t.Fatal("expected error for invalid Redis address")
	}
}

func TestNewSubscriber_KafkaEmptyBrokers(t *testing.T) {
	cfg := config.QueueConfig{
		Type:         "kafka",
		KafkaBrokers: []string{},
	}
	subCfg := Config{
		ConsumerGroup: "test-group",
	}

	_, err := NewSubscriber(cfg, subCfg)
	if err == nil {
		t.Fatal("expected error for empty Kafka brokers")
	}
}

func TestNewSubscriber_KafkaMultipleBrokers(t *testing.T) {
	cfg := config.QueueConfig{
		Type:         "kafka",
		KafkaBrokers: []string{"localhost:9092", "localhost:9093", "localhost:9094"},
	}
	subCfg := Config{
		ConsumerGroup: "test-group",
	}

	sub, err := NewSubscriber(cfg, subCfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = sub.Close() }()

	kafkaSub, ok := sub.(*KafkaSubscriber)
	if !ok {
		t.Fatal("expected KafkaSubscriber type")
	}
	if len(kafkaSub.brokers) != 3 {
		t.Errorf("expected 3 brokers, got %d", len(kafkaSub.brokers))
	}
}

func TestNewSubscriber_EmptyQueueType(t *testing.T) {
	cfg := config.QueueConfig{
		Type: "",
		URL:  "nats://invalid:4222",
	}
	subCfg := DefaultConfig()

	_, err := NewSubscriber(cfg, subCfg)
	if err == nil {
		t.Fatal("expected error for invalid NATS URL (defaults to NATS)")
	}
}

func TestNewSubscriber_WhitespaceQueueType(t *testing.T) {
	cfg := config.QueueConfig{
		Type: "  ",
		URL:  "nats://invalid:4222",
	}
	subCfg := DefaultConfig()

	_, err := NewSubscriber(cfg, subCfg)
	if err == nil {
		t.Fatal("expected error for invalid NATS URL")
	}
}

func TestNewSubscriber_SpecialCharQueueType(t *testing.T) {
	cfg := config.QueueConfig{
		Type: "special!@#",
	}
	subCfg := DefaultConfig()

	_, err := NewSubscriber(cfg, subCfg)
	if err == nil {
		t.Fatal("expected error for unsupported queue type")
	}
}

func TestNewSubscriber_NATSUpperCase(t *testing.T) {
	cfg := config.QueueConfig{
		Type: "NATS",
		URL:  "nats://invalid:4222",
	}
	subCfg := DefaultConfig()

	_, err := NewSubscriber(cfg, subCfg)
	if err == nil {
		t.Fatal("expected error for invalid NATS URL")
	}
}

func TestNewSubscriber_RedisUpperCase(t *testing.T) {
	cfg := config.QueueConfig{
		Type: "REDIS",
		URL:  "invalid:6379",
	}
	subCfg := Config{
		NodeID:        "node1",
		ConsumerGroup: "test-group",
	}

	_, err := NewSubscriber(cfg, subCfg)
	if err == nil {
		t.Fatal("expected error for invalid Redis address")
	}
}

func TestNewSubscriber_ConfigNodeIDEmpty(t *testing.T) {
	cfg := config.QueueConfig{
		Type: "memory",
	}
	subCfg := Config{
		NodeID:        "",
		ConsumerGroup: "test-group",
	}

	sub, err := NewSubscriber(cfg, subCfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = sub.Close() }()

	_, ok := sub.(*MemorySubscriber)
	if !ok {
		t.Error("expected MemorySubscriber type")
	}
}

func TestNewSubscriber_ConfigConsumerGroupEmpty(t *testing.T) {
	cfg := config.QueueConfig{
		Type: "memory",
	}
	subCfg := Config{
		NodeID:        "node1",
		ConsumerGroup: "",
	}

	sub, err := NewSubscriber(cfg, subCfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = sub.Close() }()

	_, ok := sub.(*MemorySubscriber)
	if !ok {
		t.Error("expected MemorySubscriber type")
	}
}

func TestNewSubscriber_ConfigZeroValues(t *testing.T) {
	cfg := config.QueueConfig{
		Type: "memory",
	}
	subCfg := Config{
		MaxRetries: 0,
		BatchSize:  0,
	}

	sub, err := NewSubscriber(cfg, subCfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = sub.Close() }()

	_, ok := sub.(*MemorySubscriber)
	if !ok {
		t.Error("expected MemorySubscriber type")
	}
}
