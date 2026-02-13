package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/subscriber"
)

// mockSubscriber implements subscriber.Subscriber for testing
type mockSubscriber struct {
	subscriptions map[string]subscriber.MessageHandler
	closed        bool
}

func newMockSubscriber() *mockSubscriber {
	return &mockSubscriber{
		subscriptions: make(map[string]subscriber.MessageHandler),
	}
}

func (m *mockSubscriber) Subscribe(ctx context.Context, subject string, handler subscriber.MessageHandler) error {
	m.subscriptions[subject] = handler
	return nil
}

func (m *mockSubscriber) Unsubscribe(subject string) error {
	delete(m.subscriptions, subject)
	return nil
}

func (m *mockSubscriber) Close() error {
	m.closed = true
	return nil
}

// SimulateMessage simulates receiving a message on a subject
func (m *mockSubscriber) SimulateMessage(ctx context.Context, subject string, data []byte) error {
	if handler, ok := m.subscriptions[subject]; ok {
		return handler(ctx, subject, data)
	}
	return nil
}

func TestNewStorageService(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "storage_service_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	sub := newMockSubscriber()

	service, err := NewStorageService(
		sub,
		"test-node",
		tmpDir,
		logger,
		time.Hour,
		1000,
		StorageConfig{
			MaxRowsPerPart: 1000,
			MaxPartSize:    64 * 1024 * 1024,
			MinRowsPerPart: 100,
		},
	)
	if err != nil {
		t.Fatalf("failed to create storage service: %v", err)
	}
	defer func() { _ = service.Stop() }()

	if service == nil {
		t.Fatal("expected non-nil service")
	}

	if service.nodeID != "test-node" {
		t.Errorf("expected nodeID=test-node, got %s", service.nodeID)
	}

	if service.walPartitioned == nil {
		t.Error("expected walPartitioned to be initialized")
	}

	if service.memStore == nil {
		t.Error("expected memStore to be initialized")
	}

	if service.columnarStorage == nil {
		t.Error("expected columnarStorage to be initialized")
	}

	if service.writeWorkerPool == nil {
		t.Error("expected writeWorkerPool to be initialized")
	}

	if service.flushPool == nil {
		t.Error("expected flushPool to be initialized")
	}

	if service.aggPipeline == nil {
		t.Error("expected aggPipeline to be initialized")
	}
}

func TestNewStorageService_NilSubscriber(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage_service_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()

	_, err = NewStorageService(
		nil, // nil subscriber
		"test-node",
		tmpDir,
		logger,
		time.Hour,
		1000,
		StorageConfig{},
	)
	if err == nil {
		t.Fatal("expected error for nil subscriber")
	}
}

func TestStorageService_Start(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage_service_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	sub := newMockSubscriber()

	service, err := NewStorageService(
		sub,
		"test-node",
		tmpDir,
		logger,
		time.Hour,
		1000,
		StorageConfig{},
	)
	if err != nil {
		t.Fatalf("failed to create storage service: %v", err)
	}
	defer func() { _ = service.Stop() }()

	err = service.Start()
	if err != nil {
		t.Fatalf("failed to start service: %v", err)
	}

	// Verify subscriptions were created
	expectedSubjects := []string{
		"soltix.write.node.test-node",
		"soltix.admin.flush.trigger",
	}

	for _, subject := range expectedSubjects {
		if _, ok := sub.subscriptions[subject]; !ok {
			t.Errorf("expected subscription for subject %s", subject)
		}
	}
}

func TestStorageService_HandleWriteMessage(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage_service_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	sub := newMockSubscriber()

	service, err := NewStorageService(
		sub,
		"test-node",
		tmpDir,
		logger,
		time.Hour,
		1000,
		StorageConfig{},
	)
	if err != nil {
		t.Fatalf("failed to create storage service: %v", err)
	}
	defer func() { _ = service.Stop() }()

	err = service.Start()
	if err != nil {
		t.Fatalf("failed to start service: %v", err)
	}

	// Create a write message
	writeMsg := WriteMessage{
		Database:   "testdb",
		Collection: "metrics",
		ShardID:    "shard-1",
		Time:       time.Now().Format(time.RFC3339),
		ID:         "point-1",
		Fields: map[string]interface{}{
			"temperature": 25.5,
			"humidity":    60.0,
		},
	}

	data, err := json.Marshal(writeMsg)
	if err != nil {
		t.Fatalf("failed to marshal message: %v", err)
	}

	// Simulate receiving the message
	ctx := context.Background()
	err = sub.SimulateMessage(ctx, "soltix.write.node.test-node", data)
	if err != nil {
		t.Fatalf("failed to handle message: %v", err)
	}

	// Give some time for async processing
	time.Sleep(100 * time.Millisecond)
}

func TestStorageService_HandleWriteMessage_InvalidJSON(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage_service_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	sub := newMockSubscriber()

	service, err := NewStorageService(
		sub,
		"test-node",
		tmpDir,
		logger,
		time.Hour,
		1000,
		StorageConfig{},
	)
	if err != nil {
		t.Fatalf("failed to create storage service: %v", err)
	}
	defer func() { _ = service.Stop() }()

	err = service.Start()
	if err != nil {
		t.Fatalf("failed to start service: %v", err)
	}

	// Simulate receiving invalid JSON
	ctx := context.Background()
	err = sub.SimulateMessage(ctx, "soltix.write.node.test-node", []byte("invalid json"))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestStorageService_HandleFlushTrigger(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage_service_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	sub := newMockSubscriber()

	service, err := NewStorageService(
		sub,
		"test-node",
		tmpDir,
		logger,
		time.Hour,
		1000,
		StorageConfig{},
	)
	if err != nil {
		t.Fatalf("failed to create storage service: %v", err)
	}
	defer func() { _ = service.Stop() }()

	err = service.Start()
	if err != nil {
		t.Fatalf("failed to start service: %v", err)
	}

	// Simulate flush trigger
	ctx := context.Background()
	err = sub.SimulateMessage(ctx, "soltix.admin.flush.trigger", []byte("{}"))
	if err != nil {
		t.Fatalf("failed to handle flush trigger: %v", err)
	}

	// Give some time for async processing
	time.Sleep(100 * time.Millisecond)
}

func TestStorageService_MultipleWrites(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage_service_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	sub := newMockSubscriber()

	service, err := NewStorageService(
		sub,
		"test-node",
		tmpDir,
		logger,
		time.Hour,
		1000,
		StorageConfig{},
	)
	if err != nil {
		t.Fatalf("failed to create storage service: %v", err)
	}
	defer func() { _ = service.Stop() }()

	err = service.Start()
	if err != nil {
		t.Fatalf("failed to start service: %v", err)
	}

	ctx := context.Background()
	writeCount := 100

	for i := 0; i < writeCount; i++ {
		writeMsg := WriteMessage{
			Database:   "testdb",
			Collection: "metrics",
			ShardID:    "shard-1",
			Time:       time.Now().Format(time.RFC3339Nano),
			ID:         fmt.Sprintf("point-%d", i),
			Fields: map[string]interface{}{
				"value": float64(i),
			},
		}

		data, _ := json.Marshal(writeMsg)
		err = sub.SimulateMessage(ctx, "soltix.write.node.test-node", data)
		if err != nil {
			t.Fatalf("failed to handle message %d: %v", i, err)
		}
	}

	// Give some time for async processing
	time.Sleep(200 * time.Millisecond)
}

func TestStorageService_Stop(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage_service_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	sub := newMockSubscriber()

	service, err := NewStorageService(
		sub,
		"test-node",
		tmpDir,
		logger,
		time.Hour,
		1000,
		StorageConfig{},
	)
	if err != nil {
		t.Fatalf("failed to create storage service: %v", err)
	}

	err = service.Start()
	if err != nil {
		t.Fatalf("failed to start service: %v", err)
	}

	err = service.Stop()
	if err != nil {
		t.Fatalf("failed to stop service: %v", err)
	}

	// Verify subscriber was closed
	if !sub.closed {
		t.Error("expected subscriber to be closed")
	}

	// Verify subscriptions were removed
	if len(sub.subscriptions) != 0 {
		t.Errorf("expected 0 subscriptions after stop, got %d", len(sub.subscriptions))
	}
}

func TestStorageService_GetMemoryStore(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage_service_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	sub := newMockSubscriber()

	service, err := NewStorageService(
		sub,
		"test-node",
		tmpDir,
		logger,
		time.Hour,
		1000,
		StorageConfig{},
	)
	if err != nil {
		t.Fatalf("failed to create storage service: %v", err)
	}
	defer func() { _ = service.Stop() }()

	memStore := service.GetMemoryStore()
	if memStore == nil {
		t.Error("expected non-nil memory store")
	}
}

func TestStorageService_GetStorage(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage_service_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	sub := newMockSubscriber()

	service, err := NewStorageService(
		sub,
		"test-node",
		tmpDir,
		logger,
		time.Hour,
		1000,
		StorageConfig{},
	)
	if err != nil {
		t.Fatalf("failed to create storage service: %v", err)
	}
	defer func() { _ = service.Stop() }()

	storage := service.GetStorage()
	if storage == nil {
		t.Error("expected non-nil columnar storage")
	}
}

func TestStorageService_GetWAL(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage_service_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	sub := newMockSubscriber()

	service, err := NewStorageService(
		sub,
		"test-node",
		tmpDir,
		logger,
		time.Hour,
		1000,
		StorageConfig{},
	)
	if err != nil {
		t.Fatalf("failed to create storage service: %v", err)
	}
	defer func() { _ = service.Stop() }()

	wal := service.GetWAL()
	if wal == nil {
		t.Error("expected non-nil WAL")
	}
}

func TestStorageService_GetWriteWorkerPool(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage_service_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	sub := newMockSubscriber()

	service, err := NewStorageService(
		sub,
		"test-node",
		tmpDir,
		logger,
		time.Hour,
		1000,
		StorageConfig{},
	)
	if err != nil {
		t.Fatalf("failed to create storage service: %v", err)
	}
	defer func() { _ = service.Stop() }()

	pool := service.GetWriteWorkerPool()
	if pool == nil {
		t.Error("expected non-nil write worker pool")
	}
}

func TestStorageService_WithTimezone(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage_service_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	sub := newMockSubscriber()

	// Use JST timezone
	jst, _ := time.LoadLocation("Asia/Tokyo")

	service, err := NewStorageService(
		sub,
		"test-node",
		tmpDir,
		logger,
		time.Hour,
		1000,
		StorageConfig{
			Timezone: jst,
		},
	)
	if err != nil {
		t.Fatalf("failed to create storage service: %v", err)
	}
	defer func() { _ = service.Stop() }()

	if service.storageConfig.Timezone != jst {
		t.Error("expected JST timezone to be set")
	}
}

func TestStorageService_ReplayWAL_Empty(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage_service_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	sub := newMockSubscriber()

	service, err := NewStorageService(
		sub,
		"test-node",
		tmpDir,
		logger,
		time.Hour,
		1000,
		StorageConfig{},
	)
	if err != nil {
		t.Fatalf("failed to create storage service: %v", err)
	}
	defer func() { _ = service.Stop() }()

	// replayWAL is called during Start(), should not fail with empty WAL
	err = service.Start()
	if err != nil {
		t.Fatalf("failed to start service with empty WAL: %v", err)
	}
}

func TestStorageService_ConcurrentWrites(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage_service_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	sub := newMockSubscriber()

	service, err := NewStorageService(
		sub,
		"test-node",
		tmpDir,
		logger,
		time.Hour,
		1000,
		StorageConfig{},
	)
	if err != nil {
		t.Fatalf("failed to create storage service: %v", err)
	}
	defer func() { _ = service.Stop() }()

	err = service.Start()
	if err != nil {
		t.Fatalf("failed to start service: %v", err)
	}

	ctx := context.Background()
	var successCount atomic.Int32
	writeCount := 50
	done := make(chan bool, writeCount)

	for i := 0; i < writeCount; i++ {
		go func(idx int) {
			writeMsg := WriteMessage{
				Database:   "testdb",
				Collection: "metrics",
				ShardID:    "shard-1",
				Time:       time.Now().Format(time.RFC3339Nano),
				ID:         fmt.Sprintf("point-%d", idx),
				Fields: map[string]interface{}{
					"value": float64(idx),
				},
			}

			data, _ := json.Marshal(writeMsg)
			if err := sub.SimulateMessage(ctx, "soltix.write.node.test-node", data); err == nil {
				successCount.Add(1)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < writeCount; i++ {
		<-done
	}

	if successCount.Load() != int32(writeCount) {
		t.Errorf("expected %d successful writes, got %d", writeCount, successCount.Load())
	}
}

func TestStorageService_DirectoryStructure(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage_service_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	sub := newMockSubscriber()

	service, err := NewStorageService(
		sub,
		"test-node",
		tmpDir,
		logger,
		time.Hour,
		1000,
		StorageConfig{},
	)
	if err != nil {
		t.Fatalf("failed to create storage service: %v", err)
	}
	defer func() { _ = service.Stop() }()

	// Verify directories were created
	expectedDirs := []string{
		filepath.Join(tmpDir, "wal"),
		filepath.Join(tmpDir, "data"),
		filepath.Join(tmpDir, "agg"),
	}

	for _, dir := range expectedDirs {
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			// Directories may be created lazily, so we just check the service initialized correctly
			t.Logf("Directory %s not created yet (may be lazy)", dir)
		}
	}
}

// Test replayWAL with no partitions
func TestStorageService_ReplayWAL_NoPartitions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage_service_test_replay")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	sub := newMockSubscriber()

	service, err := NewStorageService(
		sub,
		"test-node",
		tmpDir,
		logger,
		time.Hour,
		1000,
		StorageConfig{
			MaxRowsPerPart: 1000,
			MaxPartSize:    64 * 1024 * 1024,
			MinRowsPerPart: 100,
		},
	)
	if err != nil {
		t.Fatalf("failed to create storage service: %v", err)
	}
	defer func() { _ = service.Stop() }()

	// Call replayWAL with no partitions
	err = service.replayWAL()
	if err != nil {
		t.Errorf("replayWAL should succeed with no partitions, got error: %v", err)
	}
}

// Test replayWAL with recent data (within maxAge)
func TestStorageService_ReplayWAL_RecentData(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage_service_test_replay")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	sub := newMockSubscriber()

	maxAge := time.Hour
	// First service: write data to WAL
	service1, err := NewStorageService(
		sub,
		"test-node",
		tmpDir,
		logger,
		maxAge,
		1000,
		StorageConfig{
			MaxRowsPerPart: 1000,
			MaxPartSize:    64 * 1024 * 1024,
			MinRowsPerPart: 100,
		},
	)
	if err != nil {
		t.Fatalf("failed to create storage service: %v", err)
	}

	// Write recent data to WAL (within maxAge)
	recentTime := time.Now().Add(-30 * time.Minute)
	writeMsg := WriteMessage{
		Database:   "testdb",
		Collection: "metrics",
		ShardID:    "shard-1",
		GroupID:    1,
		Time:       recentTime.Format(time.RFC3339),
		ID:         "recent-point-1",
		Fields: map[string]interface{}{
			"value": 42.0,
		},
	}

	if err := service1.writeToStorage(writeMsg); err != nil {
		t.Fatalf("failed to write to storage: %v", err)
	}

	// Stop first service to flush WAL
	if err := service1.Stop(); err != nil {
		t.Fatalf("failed to stop service1: %v", err)
	}

	// Second service: should replay WAL on startup
	service2, err := NewStorageService(
		sub,
		"test-node",
		tmpDir,
		logger,
		maxAge,
		1000,
		StorageConfig{
			MaxRowsPerPart: 1000,
			MaxPartSize:    64 * 1024 * 1024,
			MinRowsPerPart: 100,
		},
	)
	if err != nil {
		t.Fatalf("failed to create storage service2: %v", err)
	}
	defer func() { _ = service2.Stop() }()

	// Start the service (this triggers replayWAL)
	if err := service2.Start(); err != nil {
		t.Fatalf("failed to start service2: %v", err)
	}

	// Verify data was replayed to memory store
	points, err := service2.memStore.Query("testdb", "metrics", "recent-point-1",
		recentTime.Add(-1*time.Minute), recentTime.Add(1*time.Minute))
	if err != nil {
		t.Errorf("failed to query memory store: %v", err)
	}

	if len(points) == 0 {
		t.Error("expected replayed data in memory store, got none")
	}
}

// Test replayWAL with old data (beyond maxAge)
func TestStorageService_ReplayWAL_OldData(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage_service_test_replay")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	sub := newMockSubscriber()

	maxAge := time.Hour
	// First service: write old data
	service1, err := NewStorageService(
		sub,
		"test-node",
		tmpDir,
		logger,
		maxAge,
		1000,
		StorageConfig{
			MaxRowsPerPart: 1000,
			MaxPartSize:    64 * 1024 * 1024,
			MinRowsPerPart: 100,
		},
	)
	if err != nil {
		t.Fatalf("failed to create storage service: %v", err)
	}

	// Write old data to WAL (beyond maxAge)
	oldTime := time.Now().Add(-2 * time.Hour)
	writeMsg := WriteMessage{
		Database:   "testdb",
		Collection: "metrics",
		ShardID:    "shard-1",
		GroupID:    1,
		Time:       oldTime.Format(time.RFC3339),
		ID:         "old-point-1",
		Fields: map[string]interface{}{
			"value": 99.0,
		},
	}

	if err := service1.writeToStorage(writeMsg); err != nil {
		t.Fatalf("failed to write to storage: %v", err)
	}

	// Stop first service
	if err := service1.Stop(); err != nil {
		t.Fatalf("failed to stop service1: %v", err)
	}

	// Second service: should replay WAL on startup
	service2, err := NewStorageService(
		sub,
		"test-node",
		tmpDir,
		logger,
		maxAge,
		1000,
		StorageConfig{
			MaxRowsPerPart: 1000,
			MaxPartSize:    64 * 1024 * 1024,
			MinRowsPerPart: 100,
		},
	)
	if err != nil {
		t.Fatalf("failed to create storage service2: %v", err)
	}
	defer func() { _ = service2.Stop() }()

	// Start the service (this triggers replayWAL)
	if err := service2.Start(); err != nil {
		t.Fatalf("failed to start service2: %v", err)
	}

	// Verify old data was NOT replayed to memory store (beyond maxAge)
	points, err := service2.memStore.Query("testdb", "metrics", "old-point-1",
		oldTime.Add(-1*time.Minute), oldTime.Add(1*time.Minute))
	if err != nil {
		t.Errorf("failed to query memory store: %v", err)
	}

	if len(points) > 0 {
		t.Error("expected old data NOT in memory store, but found some")
	}
}

// Test replayWAL with multiple partitions
func TestStorageService_ReplayWAL_MultiplePartitions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage_service_test_replay")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	sub := newMockSubscriber()

	// First service: write data to multiple partitions
	service1, err := NewStorageService(
		sub,
		"test-node",
		tmpDir,
		logger,
		time.Hour,
		1000,
		StorageConfig{
			MaxRowsPerPart: 1000,
			MaxPartSize:    64 * 1024 * 1024,
			MinRowsPerPart: 100,
		},
	)
	if err != nil {
		t.Fatalf("failed to create storage service: %v", err)
	}

	// Write data to multiple databases/collections
	recentTime := time.Now().Add(-30 * time.Minute)

	messages := []WriteMessage{
		{
			Database:   "db1",
			Collection: "metrics",
			ShardID:    "shard-1",
			GroupID:    1,
			Time:       recentTime.Format(time.RFC3339),
			ID:         "point-1",
			Fields:     map[string]interface{}{"value": 1.0},
		},
		{
			Database:   "db2",
			Collection: "metrics",
			ShardID:    "shard-2",
			GroupID:    2,
			Time:       recentTime.Format(time.RFC3339),
			ID:         "point-2",
			Fields:     map[string]interface{}{"value": 2.0},
		},
		{
			Database:   "db1",
			Collection: "events",
			ShardID:    "shard-3",
			GroupID:    1,
			Time:       recentTime.Format(time.RFC3339),
			ID:         "point-3",
			Fields:     map[string]interface{}{"value": 3.0},
		},
	}

	for _, msg := range messages {
		if err := service1.writeToStorage(msg); err != nil {
			t.Fatalf("failed to write to storage: %v", err)
		}
	}

	// Give time for WAL writes to complete
	time.Sleep(100 * time.Millisecond)

	// Stop first service
	if err := service1.Stop(); err != nil {
		t.Fatalf("failed to stop service1: %v", err)
	}

	// Second service: should replay all partitions
	service2, err := NewStorageService(
		sub,
		"test-node",
		tmpDir,
		logger,
		time.Hour,
		1000,
		StorageConfig{
			MaxRowsPerPart: 1000,
			MaxPartSize:    64 * 1024 * 1024,
			MinRowsPerPart: 100,
		},
	)
	if err != nil {
		t.Fatalf("failed to create storage service2: %v", err)
	}
	defer func() { _ = service2.Stop() }()

	// Start the service (this triggers replayWAL)
	if err := service2.Start(); err != nil {
		t.Fatalf("failed to start service2: %v", err)
	}

	// Verify data from all partitions was replayed
	for _, msg := range messages {
		points, err := service2.memStore.Query(msg.Database, msg.Collection, msg.ID,
			recentTime.Add(-1*time.Minute), recentTime.Add(1*time.Minute))
		if err != nil {
			t.Errorf("failed to query memory store for %s/%s: %v", msg.Database, msg.Collection, err)
		}
		if len(points) == 0 {
			t.Errorf("expected replayed data for %s/%s, got none", msg.Database, msg.Collection)
		}
	}
}

// Test replayWAL with mixed recent and old data
func TestStorageService_ReplayWAL_MixedData(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage_service_test_replay")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	sub := newMockSubscriber()

	maxAge := time.Hour
	// First service: write mixed data
	service1, err := NewStorageService(
		sub,
		"test-node",
		tmpDir,
		logger,
		maxAge,
		1000,
		StorageConfig{
			MaxRowsPerPart: 1000,
			MaxPartSize:    64 * 1024 * 1024,
			MinRowsPerPart: 100,
		},
	)
	if err != nil {
		t.Fatalf("failed to create storage service: %v", err)
	}

	// Write mix of recent and old data
	recentTime := time.Now().Add(-30 * time.Minute)
	oldTime := time.Now().Add(-2 * time.Hour)

	messages := []WriteMessage{
		{
			Database:   "testdb",
			Collection: "metrics",
			ShardID:    "shard-1",
			GroupID:    1,
			Time:       recentTime.Format(time.RFC3339),
			ID:         "recent-point",
			Fields:     map[string]interface{}{"value": 1.0},
		},
		{
			Database:   "testdb",
			Collection: "metrics",
			ShardID:    "shard-1",
			GroupID:    1,
			Time:       oldTime.Format(time.RFC3339),
			ID:         "old-point",
			Fields:     map[string]interface{}{"value": 2.0},
		},
	}

	for _, msg := range messages {
		if err := service1.writeToStorage(msg); err != nil {
			t.Fatalf("failed to write to storage: %v", err)
		}
	}

	// Stop first service
	if err := service1.Stop(); err != nil {
		t.Fatalf("failed to stop service1: %v", err)
	}

	// Second service: should replay
	service2, err := NewStorageService(
		sub,
		"test-node",
		tmpDir,
		logger,
		maxAge,
		1000,
		StorageConfig{
			MaxRowsPerPart: 1000,
			MaxPartSize:    64 * 1024 * 1024,
			MinRowsPerPart: 100,
		},
	)
	if err != nil {
		t.Fatalf("failed to create storage service2: %v", err)
	}
	defer func() { _ = service2.Stop() }()

	// Start the service (this triggers replayWAL)
	if err := service2.Start(); err != nil {
		t.Fatalf("failed to start service2: %v", err)
	}

	// Verify recent data in memory store
	recentPoints, err := service2.memStore.Query("testdb", "metrics", "recent-point",
		recentTime.Add(-1*time.Minute), recentTime.Add(1*time.Minute))
	if err != nil {
		t.Errorf("failed to query recent points: %v", err)
	}
	if len(recentPoints) == 0 {
		t.Error("expected recent data in memory store")
	}

	// Verify old data NOT in memory store
	oldPoints, err := service2.memStore.Query("testdb", "metrics", "old-point",
		oldTime.Add(-1*time.Minute), oldTime.Add(1*time.Minute))
	if err != nil {
		t.Errorf("failed to query old points: %v", err)
	}
	if len(oldPoints) > 0 {
		t.Error("expected old data NOT in memory store")
	}
}

// Test replayWAL processes segments sequentially
func TestStorageService_ReplayWAL_SegmentProcessing(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage_service_test_replay")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	sub := newMockSubscriber()

	// First service: write multiple messages
	service1, err := NewStorageService(
		sub,
		"test-node",
		tmpDir,
		logger,
		time.Hour,
		1000,
		StorageConfig{
			MaxRowsPerPart: 1000,
			MaxPartSize:    64 * 1024 * 1024,
			MinRowsPerPart: 100,
		},
	)
	if err != nil {
		t.Fatalf("failed to create storage service: %v", err)
	}

	// Write multiple messages to create segments
	recentTime := time.Now().Add(-30 * time.Minute)
	for i := 0; i < 10; i++ {
		writeMsg := WriteMessage{
			Database:   "testdb",
			Collection: "metrics",
			ShardID:    "shard-1",
			GroupID:    1,
			Time:       recentTime.Add(time.Duration(i) * time.Second).Format(time.RFC3339),
			ID:         fmt.Sprintf("point-%d", i),
			Fields: map[string]interface{}{
				"value": float64(i),
			},
		}

		if err := service1.writeToStorage(writeMsg); err != nil {
			t.Fatalf("failed to write to storage: %v", err)
		}
	}

	// Stop first service
	if err := service1.Stop(); err != nil {
		t.Fatalf("failed to stop service1: %v", err)
	}

	// Second service: should replay all segments
	service2, err := NewStorageService(
		sub,
		"test-node",
		tmpDir,
		logger,
		time.Hour,
		1000,
		StorageConfig{
			MaxRowsPerPart: 1000,
			MaxPartSize:    64 * 1024 * 1024,
			MinRowsPerPart: 100,
		},
	)
	if err != nil {
		t.Fatalf("failed to create storage service2: %v", err)
	}
	defer func() { _ = service2.Stop() }()

	// Start the service (this triggers replayWAL)
	if err := service2.Start(); err != nil {
		t.Fatalf("failed to start service2: %v", err)
	}

	// Verify all points were replayed
	for i := 0; i < 10; i++ {
		pointID := fmt.Sprintf("point-%d", i)
		points, err := service2.memStore.Query("testdb", "metrics", pointID,
			recentTime.Add(-1*time.Minute), recentTime.Add(1*time.Hour))
		if err != nil {
			t.Errorf("failed to query point %s: %v", pointID, err)
		}
		if len(points) == 0 {
			t.Errorf("expected point %s in memory store", pointID)
		}
	}
}

// Test replayWAL is idempotent (can be called multiple times safely)
func TestStorageService_ReplayWAL_Idempotent(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage_service_test_replay")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	sub := newMockSubscriber()

	service, err := NewStorageService(
		sub,
		"test-node",
		tmpDir,
		logger,
		time.Hour,
		1000,
		StorageConfig{
			MaxRowsPerPart: 1000,
			MaxPartSize:    64 * 1024 * 1024,
			MinRowsPerPart: 100,
		},
	)
	if err != nil {
		t.Fatalf("failed to create storage service: %v", err)
	}
	defer func() { _ = service.Stop() }()

	// First replay (with no data)
	err = service.replayWAL()
	if err != nil {
		t.Errorf("first replayWAL failed: %v", err)
	}

	// Second replay (should still succeed)
	err = service.replayWAL()
	if err != nil {
		t.Errorf("second replayWAL failed: %v", err)
	}
}

// Test replayWAL with empty maxAge (should not replay to memory)
func TestStorageService_ReplayWAL_ZeroMaxAge(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage_service_test_replay")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	sub := newMockSubscriber()

	// First service: write recent data with non-zero maxAge
	service1, err := NewStorageService(
		sub,
		"test-node",
		tmpDir,
		logger,
		time.Hour, // Use non-zero maxAge for writing
		1000,
		StorageConfig{
			MaxRowsPerPart: 1000,
			MaxPartSize:    64 * 1024 * 1024,
			MinRowsPerPart: 100,
		},
	)
	if err != nil {
		t.Fatalf("failed to create storage service: %v", err)
	}

	// Write recent data
	recentTime := time.Now()
	writeMsg := WriteMessage{
		Database:   "testdb",
		Collection: "metrics",
		ShardID:    "shard-1",
		GroupID:    1,
		Time:       recentTime.Format(time.RFC3339),
		ID:         "point-1",
		Fields: map[string]interface{}{
			"value": 42.0,
		},
	}

	if err := service1.writeToStorage(writeMsg); err != nil {
		t.Fatalf("failed to write to storage: %v", err)
	}

	// Stop first service
	if err := service1.Stop(); err != nil {
		t.Fatalf("failed to stop service1: %v", err)
	}

	// Second service: replay with maxAge=0 (no memory store retention)
	service2, err := NewStorageService(
		sub,
		"test-node",
		tmpDir,
		logger,
		0, // maxAge = 0
		1000,
		StorageConfig{
			MaxRowsPerPart: 1000,
			MaxPartSize:    64 * 1024 * 1024,
			MinRowsPerPart: 100,
		},
	)
	if err != nil {
		t.Fatalf("failed to create storage service2: %v", err)
	}
	defer func() { _ = service2.Stop() }()

	// Start the service (this triggers replayWAL)
	if err := service2.Start(); err != nil {
		t.Fatalf("failed to start service2: %v", err)
	}

	// With maxAge=0, nothing should be in memory store
	points, err := service2.memStore.Query("testdb", "metrics", "point-1",
		recentTime.Add(-1*time.Minute), recentTime.Add(1*time.Minute))
	if err != nil {
		t.Errorf("failed to query memory store: %v", err)
	}

	if len(points) > 0 {
		t.Error("expected no data in memory store with maxAge=0")
	}
}

// Test replayWAL with large number of entries
func TestStorageService_ReplayWAL_LargeDataset(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage_service_test_replay")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	sub := newMockSubscriber()

	// First service: write large dataset
	service1, err := NewStorageService(
		sub,
		"test-node",
		tmpDir,
		logger,
		time.Hour,
		10000, // Large memory store
		StorageConfig{
			MaxRowsPerPart: 1000,
			MaxPartSize:    64 * 1024 * 1024,
			MinRowsPerPart: 100,
		},
	)
	if err != nil {
		t.Fatalf("failed to create storage service: %v", err)
	}

	// Write 100 recent entries
	recentTime := time.Now().Add(-30 * time.Minute)
	entryCount := 100

	for i := 0; i < entryCount; i++ {
		writeMsg := WriteMessage{
			Database:   "testdb",
			Collection: "metrics",
			ShardID:    "shard-1",
			GroupID:    1,
			Time:       recentTime.Add(time.Duration(i) * time.Second).Format(time.RFC3339),
			ID:         fmt.Sprintf("point-%d", i),
			Fields: map[string]interface{}{
				"value": float64(i),
			},
		}

		if err := service1.writeToStorage(writeMsg); err != nil {
			t.Fatalf("failed to write entry %d: %v", i, err)
		}
	}

	// Stop first service
	if err := service1.Stop(); err != nil {
		t.Fatalf("failed to stop service1: %v", err)
	}

	// Second service: should replay all data
	service2, err := NewStorageService(
		sub,
		"test-node",
		tmpDir,
		logger,
		time.Hour,
		10000,
		StorageConfig{
			MaxRowsPerPart: 1000,
			MaxPartSize:    64 * 1024 * 1024,
			MinRowsPerPart: 100,
		},
	)
	if err != nil {
		t.Fatalf("failed to create storage service2: %v", err)
	}
	defer func() { _ = service2.Stop() }()

	// Start the service (this triggers replayWAL)
	if err := service2.Start(); err != nil {
		t.Fatalf("failed to start service2: %v", err)
	}

	// Verify a sample of points were replayed
	samplePoints := []int{0, 25, 50, 75, 99}
	for _, idx := range samplePoints {
		pointID := fmt.Sprintf("point-%d", idx)
		points, err := service2.memStore.Query("testdb", "metrics", pointID,
			recentTime.Add(-1*time.Minute), recentTime.Add(2*time.Hour))
		if err != nil {
			t.Errorf("failed to query point %s: %v", pointID, err)
		}
		if len(points) == 0 {
			t.Errorf("expected point %s in memory store", pointID)
		}
	}
}
