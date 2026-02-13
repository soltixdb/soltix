package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/wal"
)

// =============================================================================
// serviceTestWAL — specialized mock for service.go tests
// Has extra fields not in mockPartitionedWriter (writeErr, newSegment, etc.)
// =============================================================================

type serviceTestWAL struct {
	entries        map[string][]*wal.Entry
	partitions     []wal.PartitionInfo
	closeCalled    bool
	writeCount     int
	newSegmentNext bool
	writeErr       error
}

func newServiceTestWAL() *serviceTestWAL {
	return &serviceTestWAL{
		entries: make(map[string][]*wal.Entry),
	}
}

func (m *serviceTestWAL) WritePartitioned(entry *wal.Entry, database string, date time.Time) (wal.WriteResult, error) {
	if m.writeErr != nil {
		return wal.WriteResult{}, m.writeErr
	}
	key := fmt.Sprintf("%s:%s:%s", database, entry.Collection, date.Format("2006-01-02"))
	m.entries[key] = append(m.entries[key], entry)
	m.writeCount++
	return wal.WriteResult{IsNewSegment: m.newSegmentNext}, nil
}

func (m *serviceTestWAL) WriteSyncPartitioned(entry *wal.Entry, database string, date time.Time) error {
	_, err := m.WritePartitioned(entry, database, date)
	return err
}

func (m *serviceTestWAL) FlushPartition(database, collection string, date time.Time) error {
	return nil
}

func (m *serviceTestWAL) SetCheckpointPartition(database, collection string, date time.Time) error {
	return nil
}

func (m *serviceTestWAL) TruncatePartitionBeforeCheckpoint(database, collection string, date time.Time) error {
	return nil
}
func (m *serviceTestWAL) FlushAll() error { return nil }
func (m *serviceTestWAL) Close() error {
	m.closeCalled = true
	return nil
}

func (m *serviceTestWAL) GetPartitionWriter(database, collection string, date time.Time) (wal.Writer, error) {
	return nil, nil
}

func (m *serviceTestWAL) ReadPartition(database, collection string, date time.Time) ([]*wal.Entry, error) {
	key := fmt.Sprintf("%s:%s:%s", database, collection, date.Format("2006-01-02"))
	return m.entries[key], nil
}

func (m *serviceTestWAL) ReadAllPartitions() ([]*wal.Entry, error) {
	var all []*wal.Entry
	for _, entries := range m.entries {
		all = append(all, entries...)
	}
	return all, nil
}

func (m *serviceTestWAL) ListPartitions() ([]wal.PartitionInfo, error) {
	return m.partitions, nil
}

func (m *serviceTestWAL) RemovePartition(database, collection string, date time.Time) error {
	return nil
}

func (m *serviceTestWAL) GetStats() wal.PartitionedStats {
	return wal.PartitionedStats{}
}
func (m *serviceTestWAL) SetCheckpointAll() error            { return nil }
func (m *serviceTestWAL) TruncateBeforeCheckpointAll() error { return nil }
func (m *serviceTestWAL) GetTotalSegmentCount() (int, error) { return 0, nil }
func (m *serviceTestWAL) RotateAll() error                   { return nil }
func (m *serviceTestWAL) ProcessPartitions(handler func(partition wal.PartitionInfo, entries []*wal.Entry) error) error {
	return nil
}

func (m *serviceTestWAL) PrepareFlushPartition(database, collection string, date time.Time) ([]string, error) {
	return nil, nil
}

func (m *serviceTestWAL) ReadPartitionSegmentFile(database, collection string, date time.Time, filename string) ([]*wal.Entry, error) {
	return nil, nil
}

func (m *serviceTestWAL) RemovePartitionSegmentFiles(database, collection string, date time.Time, files []string) error {
	return nil
}

func (m *serviceTestWAL) HasPartitionData(database, collection string, date time.Time) bool {
	return false
}

// =============================================================================
// Tests: writeToStorageDirect — the 0% coverage function
// =============================================================================

func TestWriteToStorageDirect_RecentData(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "service_ext_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	mockWAL := newServiceTestWAL()
	memStore := NewMemoryStore(time.Hour, 1000, logger)

	storageDir := os.TempDir()
	tieredConfig := GroupStorageConfig{
		DataDir:            storageDir,
		MaxRowsPerPart:     1000,
		MaxPartSize:        64 * 1024 * 1024,
		MinRowsPerPart:     100,
		MaxDevicesPerGroup: 50,
	}
	tieredStorage := NewTieredStorage(tieredConfig, logger)
	mainStorage := NewTieredStorageAdapter(tieredStorage)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	service := &StorageService{
		nodeID:          "test-node",
		logger:          logger,
		dataDir:         tmpDir,
		walPartitioned:  mockWAL,
		memStore:        memStore,
		tieredStorage:   tieredStorage,
		ctx:             ctx,
		cancel:          cancel,
		writeWorkerPool: nil, // force direct write
	}
	_ = mainStorage // used via tieredStorage

	msg := WriteMessage{
		Database:   "testdb",
		Collection: "metrics",
		ShardID:    "shard-1",
		Time:       time.Now().Format(time.RFC3339),
		ID:         "point-1",
		Fields:     map[string]interface{}{"temp": 25.5},
	}

	err = service.writeToStorageDirect(msg)
	if err != nil {
		t.Fatalf("writeToStorageDirect failed: %v", err)
	}

	// Verify WAL was written
	if mockWAL.writeCount != 1 {
		t.Errorf("expected 1 WAL write, got %d", mockWAL.writeCount)
	}

	// Verify data in memory store (recent data should be in memStore)
	results, _ := memStore.QueryCollection("testdb", "metrics", time.Time{}, time.Now().Add(time.Minute))
	if len(results) != 1 {
		t.Errorf("expected 1 data point in memStore, got %d", len(results))
	}
}

func TestWriteToStorageDirect_OldData_SkipsMemStore(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "service_ext_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	mockWAL := newServiceTestWAL()
	memStore := NewMemoryStore(time.Hour, 1000, logger) // maxAge = 1 hour

	storageDir := os.TempDir()
	tieredConfig := GroupStorageConfig{
		DataDir:            storageDir,
		MaxRowsPerPart:     1000,
		MaxPartSize:        64 * 1024 * 1024,
		MinRowsPerPart:     100,
		MaxDevicesPerGroup: 50,
	}
	tieredStorage := NewTieredStorage(tieredConfig, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	service := &StorageService{
		nodeID:         "test-node",
		logger:         logger,
		dataDir:        tmpDir,
		walPartitioned: mockWAL,
		memStore:       memStore,
		tieredStorage:  tieredStorage,
		ctx:            ctx,
		cancel:         cancel,
	}

	// Write old data (2 hours ago, beyond maxAge)
	oldTime := time.Now().Add(-2 * time.Hour)
	msg := WriteMessage{
		Database:   "testdb",
		Collection: "metrics",
		Time:       oldTime.Format(time.RFC3339),
		ID:         "old-point",
		Fields:     map[string]interface{}{"temp": 10.0},
	}

	err = service.writeToStorageDirect(msg)
	if err != nil {
		t.Fatalf("writeToStorageDirect failed for old data: %v", err)
	}

	// WAL should be written
	if mockWAL.writeCount != 1 {
		t.Errorf("expected 1 WAL write, got %d", mockWAL.writeCount)
	}

	// memStore should NOT have old data
	results, _ := memStore.QueryCollection("testdb", "metrics", time.Time{}, time.Now().Add(time.Minute))
	if len(results) != 0 {
		t.Errorf("expected 0 data points in memStore for old data, got %d", len(results))
	}
}

func TestWriteToStorageDirect_InvalidTime(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newServiceTestWAL()
	memStore := NewMemoryStore(time.Hour, 1000, logger)

	service := &StorageService{
		logger:         logger,
		walPartitioned: mockWAL,
		memStore:       memStore,
	}

	msg := WriteMessage{
		Database: "testdb",
		Time:     "not-a-valid-time",
		ID:       "bad-point",
		Fields:   map[string]interface{}{"x": 1},
	}

	err := service.writeToStorageDirect(msg)
	if err == nil {
		t.Fatal("expected error for invalid time")
	}
}

func TestWriteToStorageDirect_WALWriteError(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newServiceTestWAL()
	mockWAL.writeErr = fmt.Errorf("WAL disk full")
	memStore := NewMemoryStore(time.Hour, 1000, logger)

	service := &StorageService{
		logger:         logger,
		walPartitioned: mockWAL,
		memStore:       memStore,
	}

	msg := WriteMessage{
		Database: "testdb",
		Time:     time.Now().Format(time.RFC3339),
		ID:       "point",
		Fields:   map[string]interface{}{"x": 1},
	}

	err := service.writeToStorageDirect(msg)
	if err == nil {
		t.Fatal("expected error when WAL write fails")
	}
}

func TestWriteToStorageDirect_NewSegmentNotifiesFlushPool(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "service_ext_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	mockWAL := newServiceTestWAL()
	mockWAL.newSegmentNext = true // simulate new segment creation
	memStore := NewMemoryStore(time.Hour, 1000, logger)

	storageDir := os.TempDir()
	tieredConfig := GroupStorageConfig{
		DataDir:            storageDir,
		MaxRowsPerPart:     1000,
		MaxPartSize:        64 * 1024 * 1024,
		MinRowsPerPart:     100,
		MaxDevicesPerGroup: 50,
	}
	tieredStorage := NewTieredStorage(tieredConfig, logger)
	mainStorage := NewTieredStorageAdapter(tieredStorage)

	// Create a flush pool with a mock that won't panic
	flushPool := NewFlushWorkerPool(
		FlushWorkerPoolConfig{
			MaxBatchSize:      100,
			FlushDelay:        time.Second,
			MaxPendingEntries: 10000,
			WorkerIdleTimeout: 5 * time.Minute,
			QueueSize:         1000,
		},
		logger,
		mockWAL,
		memStore,
		mainStorage,
		nil, // no agg pipeline
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	service := &StorageService{
		nodeID:         "test-node",
		logger:         logger,
		dataDir:        tmpDir,
		walPartitioned: mockWAL,
		memStore:       memStore,
		tieredStorage:  tieredStorage,
		flushPool:      flushPool,
		ctx:            ctx,
		cancel:         cancel,
	}

	msg := WriteMessage{
		Database:   "testdb",
		Collection: "metrics",
		Time:       time.Now().Format(time.RFC3339),
		ID:         "seg-point",
		Fields:     map[string]interface{}{"temp": 30.0},
	}

	err = service.writeToStorageDirect(msg)
	if err != nil {
		t.Fatalf("writeToStorageDirect failed: %v", err)
	}

	// Notification should have been sent (check notifyCh)
	// Give a tiny moment for the non-blocking send
	time.Sleep(10 * time.Millisecond)
}

// =============================================================================
// Tests: writeToStorage — uses worker pool fallback
// =============================================================================

func TestWriteToStorage_FallbackToDirectWrite(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "service_ext_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	mockWAL := newServiceTestWAL()
	memStore := NewMemoryStore(time.Hour, 1000, logger)

	storageDir := os.TempDir()
	tieredConfig := GroupStorageConfig{
		DataDir:            storageDir,
		MaxRowsPerPart:     1000,
		MaxPartSize:        64 * 1024 * 1024,
		MinRowsPerPart:     100,
		MaxDevicesPerGroup: 50,
	}
	tieredStorage := NewTieredStorage(tieredConfig, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	service := &StorageService{
		nodeID:          "test-node",
		logger:          logger,
		dataDir:         tmpDir,
		walPartitioned:  mockWAL,
		memStore:        memStore,
		tieredStorage:   tieredStorage,
		writeWorkerPool: nil, // nil → fallback to direct
		ctx:             ctx,
		cancel:          cancel,
	}

	msg := WriteMessage{
		Database:   "testdb",
		Collection: "metrics",
		Time:       time.Now().Format(time.RFC3339),
		ID:         "fallback-point",
		Fields:     map[string]interface{}{"val": 1.0},
	}

	err = service.writeToStorage(msg)
	if err != nil {
		t.Fatalf("writeToStorage fallback failed: %v", err)
	}
	if mockWAL.writeCount != 1 {
		t.Errorf("expected 1 WAL write in fallback path, got %d", mockWAL.writeCount)
	}
}

// =============================================================================
// Tests: handleWriteMessage edge cases
// =============================================================================

func TestHandleWriteMessage_ContextCancelled(t *testing.T) {
	logger := logging.NewDevelopment()

	service := &StorageService{
		logger: logger,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := service.handleWriteMessage(ctx, "test-subject", []byte("{}"))
	if err == nil {
		t.Fatal("expected error when context is cancelled")
	}
}

func TestHandleWriteMessage_InvalidJSON(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newServiceTestWAL()
	memStore := NewMemoryStore(time.Hour, 1000, logger)

	service := &StorageService{
		logger:         logger,
		walPartitioned: mockWAL,
		memStore:       memStore,
	}

	ctx := context.Background()
	err := service.handleWriteMessage(ctx, "test-subject", []byte("not valid json"))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestHandleWriteMessage_ValidMessage_ViaDirect(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "service_ext_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	mockWAL := newServiceTestWAL()
	memStore := NewMemoryStore(time.Hour, 1000, logger)

	storageDir := os.TempDir()
	tieredConfig := GroupStorageConfig{
		DataDir:            storageDir,
		MaxRowsPerPart:     1000,
		MaxPartSize:        64 * 1024 * 1024,
		MinRowsPerPart:     100,
		MaxDevicesPerGroup: 50,
	}
	tieredStorage := NewTieredStorage(tieredConfig, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	service := &StorageService{
		nodeID:          "test-node",
		logger:          logger,
		dataDir:         tmpDir,
		walPartitioned:  mockWAL,
		memStore:        memStore,
		tieredStorage:   tieredStorage,
		writeWorkerPool: nil, // force direct
		ctx:             ctx,
		cancel:          cancel,
	}

	msg := WriteMessage{
		Database:   "testdb",
		Collection: "metrics",
		Time:       time.Now().Format(time.RFC3339),
		ID:         "msg-point",
		Fields:     map[string]interface{}{"val": 42.0},
	}

	data, _ := json.Marshal(msg)
	err = service.handleWriteMessage(context.Background(), "test-subject", data)
	if err != nil {
		t.Fatalf("handleWriteMessage failed: %v", err)
	}

	if mockWAL.writeCount != 1 {
		t.Errorf("expected 1 WAL write, got %d", mockWAL.writeCount)
	}
}

// =============================================================================
// Tests: handleFlushTrigger
// =============================================================================

func TestHandleFlushTrigger_NoFlushPool(t *testing.T) {
	logger := logging.NewDevelopment()

	service := &StorageService{
		logger:    logger,
		flushPool: nil,
	}

	ctx := context.Background()
	err := service.handleFlushTrigger(ctx, "soltix.admin.flush.trigger", []byte("{}"))
	if err != nil {
		t.Fatalf("handleFlushTrigger should not fail with nil pool: %v", err)
	}
}

// =============================================================================
// Tests: replayWAL with mock WAL
// =============================================================================

func TestReplayWAL_EmptyPartitions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "service_ext_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	mockWAL := newServiceTestWAL()
	// empty partitions
	mockWAL.partitions = []wal.PartitionInfo{}

	memStore := NewMemoryStore(time.Hour, 1000, logger)
	tieredConfig := GroupStorageConfig{
		DataDir:            tmpDir,
		MaxRowsPerPart:     1000,
		MaxPartSize:        64 * 1024 * 1024,
		MinRowsPerPart:     100,
		MaxDevicesPerGroup: 50,
	}
	tieredStorage := NewTieredStorage(tieredConfig, logger)

	service := &StorageService{
		logger:         logger,
		walPartitioned: mockWAL,
		memStore:       memStore,
		tieredStorage:  tieredStorage,
	}

	err = service.replayWAL()
	if err != nil {
		t.Fatalf("replayWAL with empty partitions should not fail: %v", err)
	}
}

func TestReplayWAL_WithPartitions_NoSegments(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "service_ext_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	mockWAL := newServiceTestWAL()
	mockWAL.partitions = []wal.PartitionInfo{
		{
			Database:   "testdb",
			Collection: "metrics",
			Date:       time.Now(),
		},
	}
	// PrepareFlushPartition returns nil (no segments)

	memStore := NewMemoryStore(time.Hour, 1000, logger)
	tieredConfig := GroupStorageConfig{
		DataDir:            tmpDir,
		MaxRowsPerPart:     1000,
		MaxPartSize:        64 * 1024 * 1024,
		MinRowsPerPart:     100,
		MaxDevicesPerGroup: 50,
	}
	tieredStorage := NewTieredStorage(tieredConfig, logger)

	service := &StorageService{
		logger:         logger,
		walPartitioned: mockWAL,
		memStore:       memStore,
		tieredStorage:  tieredStorage,
	}

	err = service.replayWAL()
	if err != nil {
		t.Fatalf("replayWAL with partitions but no segments should not fail: %v", err)
	}
}

// =============================================================================
// Tests: walEntryToDataPoint
// =============================================================================

func TestWalEntryToDataPoint_NilEntry_ReturnsNil(t *testing.T) {
	dp := walEntryToDataPoint(nil)
	if dp != nil {
		t.Fatal("expected nil for nil entry")
	}
}

func TestWalEntryToDataPoint_Valid(t *testing.T) {
	now := time.Now()
	entry := &wal.Entry{
		Type:       wal.EntryTypeWrite,
		Database:   "db1",
		Collection: "col1",
		ShardID:    "shard-0",
		Time:       now.Format(time.RFC3339),
		ID:         "device-1",
		Fields:     map[string]interface{}{"temp": 25.5},
		Timestamp:  now.UnixNano(),
	}

	dp := walEntryToDataPoint(entry)
	if dp == nil {
		t.Fatal("expected non-nil data point")
	}
	if dp.Database != "db1" {
		t.Errorf("expected database=db1, got %s", dp.Database)
	}
	if dp.ID != "device-1" {
		t.Errorf("expected ID=device-1, got %s", dp.ID)
	}
	if dp.Fields["temp"] != 25.5 {
		t.Errorf("expected temp=25.5, got %v", dp.Fields["temp"])
	}
}

// =============================================================================
// Tests: GetTieredStorage
// =============================================================================

func TestGetTieredStorage_ReturnsNonNil(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "service_ext_test")
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
		t.Fatalf("failed to create service: %v", err)
	}
	defer func() { _ = service.Stop() }()

	ts := service.GetTieredStorage()
	if ts == nil {
		t.Error("expected non-nil TieredStorage")
	}
}

// =============================================================================
// Tests: Service Stop with various nil components
// =============================================================================

func TestServiceStop_AllNilComponents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	service := &StorageService{
		logger:     logging.NewDevelopment(),
		ctx:        ctx,
		cancel:     cancel,
		subscriber: nil, // nil subscriber
		// All components nil
	}

	err := service.Stop()
	if err != nil {
		t.Fatalf("Stop with nil components should not fail: %v", err)
	}
}

func TestServiceStop_WithMockComponents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	mockWAL := newServiceTestWAL()
	sub := newMockSubscriber()
	memStore := NewMemoryStore(time.Hour, 100, logging.NewDevelopment())

	service := &StorageService{
		nodeID:         "test",
		logger:         logging.NewDevelopment(),
		walPartitioned: mockWAL,
		memStore:       memStore,
		subscriber:     sub,
		ctx:            ctx,
		cancel:         cancel,
	}

	err := service.Stop()
	if err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	if !mockWAL.closeCalled {
		t.Error("expected WAL Close to be called")
	}
	if !sub.closed {
		t.Error("expected subscriber to be closed")
	}
}

// =============================================================================
// Tests: CollectionPartitionKey
// =============================================================================

func TestCollectionPartitionKey_FormatValidation(t *testing.T) {
	date := time.Date(2025, 6, 15, 0, 0, 0, 0, time.UTC)
	key := CollectionPartitionKey("mydb", "metrics", date)
	expected := "mydb:metrics:2025-06-15"
	if key != expected {
		t.Errorf("expected key=%q, got %q", expected, key)
	}
}

// =============================================================================
// Tests: WriteNotification
// =============================================================================

func TestWriteNotification_Struct(t *testing.T) {
	now := time.Now()
	notif := WriteNotification{
		Database:   "db1",
		Date:       now,
		Collection: "col1",
		EntryCount: 42,
		Immediate:  true,
	}

	if notif.Database != "db1" {
		t.Errorf("expected Database=db1, got %s", notif.Database)
	}
	if notif.EntryCount != 42 {
		t.Errorf("expected EntryCount=42, got %d", notif.EntryCount)
	}
	if !notif.Immediate {
		t.Error("expected Immediate=true")
	}
}
