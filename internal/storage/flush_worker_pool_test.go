package storage

import (
	"sync"
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/wal"
)

// =============================================================================
// Mock wal.PartitionedWriter for testing
// =============================================================================

type mockPartitionedWriter struct {
	entries         map[string][]*wal.Entry // key: "db:collection:date"
	mu              sync.Mutex
	preparedFiles   map[string][]string // key: "db:collection:date" -> segment files
	hasDataMap      map[string]bool
	flushCalled     int
	closeCalled     int
	removeCalled    int
	checkpointCalls int
}

func newMockPartitionedWriter() *mockPartitionedWriter {
	return &mockPartitionedWriter{
		entries:       make(map[string][]*wal.Entry),
		preparedFiles: make(map[string][]string),
		hasDataMap:    make(map[string]bool),
	}
}

func (m *mockPartitionedWriter) WritePartitioned(entry *wal.Entry, database string, date time.Time) (wal.WriteResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := database + ":" + entry.Collection + ":" + date.Format("2006-01-02")
	m.entries[key] = append(m.entries[key], entry)
	return wal.WriteResult{IsNewSegment: false}, nil
}

func (m *mockPartitionedWriter) WriteSyncPartitioned(entry *wal.Entry, database string, date time.Time) error {
	_, err := m.WritePartitioned(entry, database, date)
	return err
}

func (m *mockPartitionedWriter) FlushPartition(database, collection string, date time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flushCalled++
	return nil
}

func (m *mockPartitionedWriter) SetCheckpointPartition(database, collection string, date time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.checkpointCalls++
	return nil
}

func (m *mockPartitionedWriter) TruncatePartitionBeforeCheckpoint(database, collection string, date time.Time) error {
	return nil
}

func (m *mockPartitionedWriter) FlushAll() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flushCalled++
	return nil
}

func (m *mockPartitionedWriter) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closeCalled++
	return nil
}

func (m *mockPartitionedWriter) GetPartitionWriter(database, collection string, date time.Time) (wal.Writer, error) {
	return nil, nil
}

func (m *mockPartitionedWriter) ReadPartition(database, collection string, date time.Time) ([]*wal.Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := database + ":" + collection + ":" + date.Format("2006-01-02")
	return m.entries[key], nil
}

func (m *mockPartitionedWriter) ReadAllPartitions() ([]*wal.Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var all []*wal.Entry
	for _, entries := range m.entries {
		all = append(all, entries...)
	}
	return all, nil
}

func (m *mockPartitionedWriter) ListPartitions() ([]wal.PartitionInfo, error) {
	return nil, nil
}

func (m *mockPartitionedWriter) RemovePartition(database, collection string, date time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := database + ":" + collection + ":" + date.Format("2006-01-02")
	delete(m.entries, key)
	m.removeCalled++
	return nil
}

func (m *mockPartitionedWriter) GetStats() wal.PartitionedStats {
	return wal.PartitionedStats{PartitionCount: len(m.entries)}
}

func (m *mockPartitionedWriter) SetCheckpointAll() error {
	return nil
}

func (m *mockPartitionedWriter) TruncateBeforeCheckpointAll() error {
	return nil
}

func (m *mockPartitionedWriter) GetTotalSegmentCount() (int, error) {
	return 0, nil
}

func (m *mockPartitionedWriter) RotateAll() error {
	return nil
}

func (m *mockPartitionedWriter) ProcessPartitions(handler func(partition wal.PartitionInfo, entries []*wal.Entry) error) error {
	return nil
}

func (m *mockPartitionedWriter) PrepareFlushPartition(database, collection string, date time.Time) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := database + ":" + collection + ":" + date.Format("2006-01-02")
	if files, ok := m.preparedFiles[key]; ok {
		return files, nil
	}
	return nil, nil
}

func (m *mockPartitionedWriter) ReadPartitionSegmentFile(database, collection string, date time.Time, filename string) ([]*wal.Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := database + ":" + collection + ":" + date.Format("2006-01-02")
	return m.entries[key], nil
}

func (m *mockPartitionedWriter) RemovePartitionSegmentFiles(database, collection string, date time.Time, files []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.removeCalled++
	return nil
}

func (m *mockPartitionedWriter) HasPartitionData(database, collection string, date time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := database + ":" + collection + ":" + date.Format("2006-01-02")
	return m.hasDataMap[key]
}

// SetTestData sets up test data for the mock
func (m *mockPartitionedWriter) SetTestData(database, collection string, date time.Time, entries []*wal.Entry, files []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := database + ":" + collection + ":" + date.Format("2006-01-02")
	m.entries[key] = entries
	m.preparedFiles[key] = files
	m.hasDataMap[key] = len(entries) > 0
}

// =============================================================================
// Mock MainStorage for FlushWorkerPool testing
// =============================================================================

type flushMockMainStorage struct {
	writtenBatches [][]*DataPoint
	mu             sync.Mutex
	queryResult    []*DataPoint
}

func newFlushMockMainStorage() *flushMockMainStorage {
	return &flushMockMainStorage{
		writtenBatches: make([][]*DataPoint, 0),
	}
}

func (m *flushMockMainStorage) WriteBatch(points []*DataPoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writtenBatches = append(m.writtenBatches, points)
	return nil
}

func (m *flushMockMainStorage) Query(database, collection string, deviceIDs []string, startTime, endTime time.Time, fields []string) ([]*DataPoint, error) {
	return m.queryResult, nil
}

func (m *flushMockMainStorage) SetTimezone(tz *time.Location) {
	// no-op for mock
}

func (m *flushMockMainStorage) GetWrittenBatches() [][]*DataPoint {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.writtenBatches
}

func TestDefaultFlushWorkerPoolConfig(t *testing.T) {
	config := DefaultFlushWorkerPoolConfig()

	if config.MaxBatchSize != 100 {
		t.Errorf("MaxBatchSize = %d, expected 100", config.MaxBatchSize)
	}
	if config.FlushDelay != 1*time.Second {
		t.Errorf("FlushDelay = %v, expected 1s", config.FlushDelay)
	}
	if config.MaxPendingEntries != 10000 {
		t.Errorf("MaxPendingEntries = %d, expected 10000", config.MaxPendingEntries)
	}
	if config.WorkerIdleTimeout != 5*time.Minute {
		t.Errorf("WorkerIdleTimeout = %v, expected 5m", config.WorkerIdleTimeout)
	}
	if config.QueueSize != 1000 {
		t.Errorf("QueueSize = %d, expected 1000", config.QueueSize)
	}
}

func TestFlushWorkerPoolConfig(t *testing.T) {
	config := FlushWorkerPoolConfig{
		MaxBatchSize:      200,
		FlushDelay:        2 * time.Second,
		MaxPendingEntries: 5000,
		WorkerIdleTimeout: 3 * time.Minute,
		QueueSize:         500,
	}

	if config.MaxBatchSize != 200 {
		t.Errorf("MaxBatchSize = %d, expected 200", config.MaxBatchSize)
	}
	if config.FlushDelay != 2*time.Second {
		t.Errorf("FlushDelay = %v, expected 2s", config.FlushDelay)
	}
	if config.MaxPendingEntries != 5000 {
		t.Errorf("MaxPendingEntries = %d, expected 5000", config.MaxPendingEntries)
	}
}

func TestFlushWorkerConfig(t *testing.T) {
	now := time.Now()
	config := FlushWorkerConfig{
		Key:               "testdb:metrics:2024-05-15",
		Database:          "testdb",
		Collection:        "metrics",
		Date:              now,
		MaxBatchSize:      100,
		FlushDelay:        time.Second,
		MaxPendingEntries: 10000,
		QueueSize:         1000,
	}

	if config.Key != "testdb:metrics:2024-05-15" {
		t.Errorf("Key = %q, expected %q", config.Key, "testdb:metrics:2024-05-15")
	}
	if config.Database != "testdb" {
		t.Errorf("Database = %q, expected %q", config.Database, "testdb")
	}
	if config.Collection != "metrics" {
		t.Errorf("Collection = %q, expected %q", config.Collection, "metrics")
	}
	if config.MaxBatchSize != 100 {
		t.Errorf("MaxBatchSize = %d, expected 100", config.MaxBatchSize)
	}
}

func TestWalEntryToDataPoint_Nil(t *testing.T) {
	result := walEntryToDataPoint(nil)
	if result != nil {
		t.Errorf("Expected nil, got %v", result)
	}
}

// Note: Full FlushWorkerPool tests require mocking wal.PartitionedWriter
// which is more complex. These tests cover the configuration and basic structures.

func TestFlushWorkerPool_GetStatsEmpty(t *testing.T) {
	logger := logging.NewDevelopment()

	// Create minimal pool for testing stats
	pool := &FlushWorkerPool{
		config:   DefaultFlushWorkerPoolConfig(),
		logger:   logger,
		workers:  make(map[string]*FlushWorker),
		notifyCh: make(chan WriteNotification, 100),
	}

	stats := pool.GetStats()

	workerCount, ok := stats["worker_count"].(int)
	if !ok || workerCount != 0 {
		t.Errorf("worker_count = %v, expected 0", stats["worker_count"])
	}

	pendingNotifs, ok := stats["pending_notifs"].(int)
	if !ok || pendingNotifs != 0 {
		t.Errorf("pending_notifs = %v, expected 0", stats["pending_notifs"])
	}
}

func TestFlushWorker_GetStats(t *testing.T) {
	logger := logging.NewDevelopment()
	now := time.Now()

	worker := &FlushWorker{
		config: FlushWorkerConfig{
			Key:        "testdb:metrics:2024-05-15",
			Database:   "testdb",
			Collection: "metrics",
			Date:       now,
			QueueSize:  100,
		},
		logger:          logger,
		notifyCh:        make(chan WriteNotification, 100),
		lastActive:      now,
		lastFlush:       now.Add(-time.Minute),
		totalFlushCount: 10,
		totalFlushedOps: 500,
	}

	stats := worker.GetStats()

	if stats["partition"] != "testdb:metrics:2024-05-15" {
		t.Errorf("partition = %v, expected %q", stats["partition"], "testdb:metrics:2024-05-15")
	}
	if stats["pending_count"].(int) != 0 {
		t.Errorf("pending_count = %v, expected 0", stats["pending_count"])
	}
	if stats["total_flush_count"].(int64) != 10 {
		t.Errorf("total_flush_count = %v, expected 10", stats["total_flush_count"])
	}
	if stats["total_flushed_ops"].(int64) != 500 {
		t.Errorf("total_flushed_ops = %v, expected 500", stats["total_flushed_ops"])
	}
}

func TestFlushWorker_LastActive(t *testing.T) {
	logger := logging.NewDevelopment()
	now := time.Now()

	worker := &FlushWorker{
		config: FlushWorkerConfig{
			Key: "test",
		},
		logger:     logger,
		notifyCh:   make(chan WriteNotification, 10),
		lastActive: now,
	}

	lastActive := worker.LastActive()
	if !lastActive.Equal(now) {
		t.Errorf("LastActive() = %v, expected %v", lastActive, now)
	}
}

func TestFlushWorker_TriggerFlush_NoPending(t *testing.T) {
	logger := logging.NewDevelopment()

	worker := &FlushWorker{
		config: FlushWorkerConfig{
			Key: "test",
		},
		logger:       logger,
		notifyCh:     make(chan WriteNotification, 10),
		triggerCh:    make(chan struct{}, 1),
		pendingCount: 0,
	}

	// Should not panic when no pending - uses channel now
	worker.TriggerFlush()

	// Verify trigger was sent to channel
	select {
	case <-worker.triggerCh:
		// Trigger was received as expected
	default:
		t.Error("Expected trigger to be sent to channel")
	}
}

func TestFlushWorker_TriggerFlush_NonBlocking(t *testing.T) {
	logger := logging.NewDevelopment()

	worker := &FlushWorker{
		config: FlushWorkerConfig{
			Key: "test",
		},
		logger:       logger,
		notifyCh:     make(chan WriteNotification, 10),
		triggerCh:    make(chan struct{}, 1),
		pendingCount: 5,
	}

	// First trigger should succeed
	worker.TriggerFlush()

	// Second trigger should be safely skipped (non-blocking)
	// This should not block or panic
	worker.TriggerFlush()
	worker.TriggerFlush()
	worker.TriggerFlush()

	// Verify only one trigger is in channel
	select {
	case <-worker.triggerCh:
		// Got first trigger
	default:
		t.Error("Expected at least one trigger in channel")
	}

	// Channel should now be empty
	select {
	case <-worker.triggerCh:
		t.Error("Expected channel to be empty after consuming one trigger")
	default:
		// Channel is empty as expected
	}
}

func TestFlushWorker_TriggerFlush_ConcurrentSafe(t *testing.T) {
	logger := logging.NewDevelopment()

	worker := &FlushWorker{
		config: FlushWorkerConfig{
			Key: "test",
		},
		logger:       logger,
		notifyCh:     make(chan WriteNotification, 10),
		triggerCh:    make(chan struct{}, 1),
		pendingCount: 10,
	}

	// Test concurrent TriggerFlush calls - should not panic or deadlock
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker.TriggerFlush()
		}()
	}

	// Wait with timeout to detect deadlock
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All goroutines completed successfully
	case <-time.After(2 * time.Second):
		t.Error("Concurrent TriggerFlush calls caused deadlock")
	}
}

// =============================================================================
// New tests with mocks
// =============================================================================

func TestWalEntryToDataPoint_ValidEntry(t *testing.T) {
	now := time.Now().UnixNano()
	entry := &wal.Entry{
		Type:       wal.EntryTypeWrite,
		Database:   "testdb",
		Collection: "metrics",
		ID:         "device1",
		Fields: map[string]interface{}{
			"temp":     25.5,
			"humidity": 60.0,
		},
		Timestamp: now,
	}

	result := walEntryToDataPoint(entry)

	if result == nil {
		t.Fatal("Expected non-nil result")
		return
	}
	if result.ID != "device1" {
		t.Errorf("ID = %q, expected %q", result.ID, "device1")
	}
	if result.Database != "testdb" {
		t.Errorf("Database = %q, expected %q", result.Database, "testdb")
	}
	if result.Collection != "metrics" {
		t.Errorf("Collection = %q, expected %q", result.Collection, "metrics")
	}
	if result.Fields["temp"] != 25.5 {
		t.Errorf("Fields[temp] = %v, expected 25.5", result.Fields["temp"])
	}
}

func TestNewFlushWorkerPool(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 10000, logger)
	mockStorage := newFlushMockMainStorage()

	pool := NewFlushWorkerPool(
		DefaultFlushWorkerPoolConfig(),
		logger,
		mockWAL,
		memStore,
		mockStorage,
		nil, // no aggregation pipeline for test
	)

	if pool == nil {
		t.Fatal("Expected non-nil pool")
		return
	}
	if pool.config.MaxBatchSize != 100 {
		t.Errorf("config.MaxBatchSize = %d, expected 100", pool.config.MaxBatchSize)
	}
	if pool.workers == nil {
		t.Error("workers map should be initialized")
	}
	if pool.notifyCh == nil {
		t.Error("notifyCh should be initialized")
	}

	_ = memStore.Close()
}

func TestFlushWorkerPool_StartStop(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 10000, logger)
	mockStorage := newFlushMockMainStorage()

	pool := NewFlushWorkerPool(
		FlushWorkerPoolConfig{
			MaxBatchSize:      10,
			FlushDelay:        100 * time.Millisecond,
			MaxPendingEntries: 100,
			WorkerIdleTimeout: 1 * time.Second,
			QueueSize:         10,
		},
		logger,
		mockWAL,
		memStore,
		mockStorage,
		nil,
	)

	// Start the pool
	pool.Start()

	// Let it run for a bit
	time.Sleep(50 * time.Millisecond)

	// Stop the pool
	pool.Stop()

	_ = memStore.Close()
}

func TestFlushWorkerPool_Notify(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 10000, logger)
	mockStorage := newFlushMockMainStorage()

	pool := NewFlushWorkerPool(
		FlushWorkerPoolConfig{
			MaxBatchSize:      10,
			FlushDelay:        100 * time.Millisecond,
			MaxPendingEntries: 100,
			WorkerIdleTimeout: 1 * time.Second,
			QueueSize:         100,
		},
		logger,
		mockWAL,
		memStore,
		mockStorage,
		nil,
	)

	pool.Start()

	// Send notification
	notif := WriteNotification{
		Database:   "testdb",
		Collection: "metrics",
		Date:       time.Now(),
		EntryCount: 10,
		Immediate:  false,
	}
	pool.Notify(notif)

	// Allow time for processing
	time.Sleep(50 * time.Millisecond)

	// Check stats
	stats := pool.GetStats()
	workerCount := stats["worker_count"].(int)
	if workerCount != 1 {
		t.Errorf("worker_count = %d, expected 1", workerCount)
	}

	pool.Stop()
	_ = memStore.Close()
}

func TestFlushWorkerPool_SetAggregationPipeline(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 10000, logger)
	mockStorage := newFlushMockMainStorage()

	pool := NewFlushWorkerPool(
		DefaultFlushWorkerPoolConfig(),
		logger,
		mockWAL,
		memStore,
		mockStorage,
		nil,
	)

	if pool.aggPipe != nil {
		t.Error("Expected nil aggPipe initially")
	}

	// Set pipeline (nil for testing)
	pool.SetAggregationPipeline(nil)

	_ = memStore.Close()
}

func TestFlushWorkerPool_FlushAll(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 10000, logger)
	mockStorage := newFlushMockMainStorage()

	pool := NewFlushWorkerPool(
		FlushWorkerPoolConfig{
			MaxBatchSize:      10,
			FlushDelay:        100 * time.Millisecond,
			MaxPendingEntries: 100,
			WorkerIdleTimeout: 1 * time.Second,
			QueueSize:         100,
		},
		logger,
		mockWAL,
		memStore,
		mockStorage,
		nil,
	)

	pool.Start()

	// Add a notification to create a worker
	pool.Notify(WriteNotification{
		Database:   "testdb",
		Collection: "metrics",
		Date:       time.Now(),
		EntryCount: 10,
	})

	time.Sleep(50 * time.Millisecond)

	// FlushAll
	pool.FlushAll()

	pool.Stop()
	_ = memStore.Close()
}

func TestFlushWorkerPool_CleanupIdleWorkers(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 10000, logger)
	mockStorage := newFlushMockMainStorage()

	pool := NewFlushWorkerPool(
		FlushWorkerPoolConfig{
			MaxBatchSize:      10,
			FlushDelay:        50 * time.Millisecond,
			MaxPendingEntries: 100,
			WorkerIdleTimeout: 50 * time.Millisecond, // Very short for testing
			QueueSize:         100,
		},
		logger,
		mockWAL,
		memStore,
		mockStorage,
		nil,
	)

	pool.Start()

	// Add notification to create worker
	pool.Notify(WriteNotification{
		Database:   "testdb",
		Collection: "metrics",
		Date:       time.Now(),
		EntryCount: 10,
	})

	time.Sleep(30 * time.Millisecond)

	// Verify worker exists
	stats := pool.GetStats()
	if stats["worker_count"].(int) != 1 {
		t.Errorf("Expected 1 worker, got %d", stats["worker_count"].(int))
	}

	// Wait for worker to become idle and get cleaned up
	time.Sleep(200 * time.Millisecond)

	// Manually trigger cleanup
	pool.cleanupIdleWorkers()

	// Check worker was removed
	stats = pool.GetStats()
	if stats["worker_count"].(int) != 0 {
		t.Errorf("Expected 0 workers after cleanup, got %d", stats["worker_count"].(int))
	}

	pool.Stop()
	_ = memStore.Close()
}

func TestNewFlushWorker(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 10000, logger)
	mockStorage := newFlushMockMainStorage()

	now := time.Now()
	config := FlushWorkerConfig{
		Key:               "testdb:metrics:2024-05-15",
		Database:          "testdb",
		Collection:        "metrics",
		Date:              now,
		MaxBatchSize:      100,
		FlushDelay:        time.Second,
		MaxPendingEntries: 1000,
		QueueSize:         100,
	}

	worker := NewFlushWorker(config, logger, mockWAL, memStore, mockStorage, nil)

	if worker == nil {
		t.Fatal("Expected non-nil worker")
		return
	}
	if worker.config.Key != "testdb:metrics:2024-05-15" {
		t.Errorf("config.Key = %q, expected %q", worker.config.Key, "testdb:metrics:2024-05-15")
	}
	if worker.config.Database != "testdb" {
		t.Errorf("config.Database = %q, expected %q", worker.config.Database, "testdb")
	}
	if worker.notifyCh == nil {
		t.Error("notifyCh should be initialized")
	}

	_ = memStore.Close()
}

func TestFlushWorker_StartStop(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 10000, logger)
	mockStorage := newFlushMockMainStorage()

	worker := NewFlushWorker(
		FlushWorkerConfig{
			Key:               "testdb:metrics:2024-05-15",
			Database:          "testdb",
			Collection:        "metrics",
			Date:              time.Now(),
			MaxBatchSize:      10,
			FlushDelay:        50 * time.Millisecond,
			MaxPendingEntries: 100,
			QueueSize:         10,
		},
		logger,
		mockWAL,
		memStore,
		mockStorage,
		nil,
	)

	worker.Start()
	time.Sleep(20 * time.Millisecond)
	worker.Stop()

	_ = memStore.Close()
}

func TestFlushWorker_Notify(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 10000, logger)
	mockStorage := newFlushMockMainStorage()

	worker := NewFlushWorker(
		FlushWorkerConfig{
			Key:               "testdb:metrics:2024-05-15",
			Database:          "testdb",
			Collection:        "metrics",
			Date:              time.Now(),
			MaxBatchSize:      10,
			FlushDelay:        100 * time.Millisecond,
			MaxPendingEntries: 100,
			QueueSize:         10,
		},
		logger,
		mockWAL,
		memStore,
		mockStorage,
		nil,
	)

	worker.Start()

	// Send notification
	worker.Notify(WriteNotification{
		Database:   "testdb",
		Collection: "metrics",
		Date:       time.Now(),
		EntryCount: 5,
	})

	time.Sleep(20 * time.Millisecond)

	// Check last active was updated
	lastActive := worker.LastActive()
	if time.Since(lastActive) > time.Second {
		t.Error("LastActive should be recent")
	}

	worker.Stop()
	_ = memStore.Close()
}

func TestFlushWorker_ImmediateFlush(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 10000, logger)
	mockStorage := newFlushMockMainStorage()

	now := time.Now()
	date := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)

	// Set up test data in mock WAL
	mockWAL.SetTestData("testdb", "metrics", date, []*wal.Entry{
		{
			Type:       wal.EntryTypeWrite,
			Database:   "testdb",
			Collection: "metrics",
			ID:         "device1",
			Fields:     map[string]interface{}{"temp": 25.0},
			Timestamp:  now.UnixNano(),
		},
	}, []string{"wal-001.log"})

	worker := NewFlushWorker(
		FlushWorkerConfig{
			Key:               "testdb:metrics:" + date.Format("2006-01-02"),
			Database:          "testdb",
			Collection:        "metrics",
			Date:              date,
			MaxBatchSize:      10,
			FlushDelay:        time.Second,
			MaxPendingEntries: 100,
			QueueSize:         10,
		},
		logger,
		mockWAL,
		memStore,
		mockStorage,
		nil,
	)

	worker.Start()

	// Send immediate notification
	worker.Notify(WriteNotification{
		Database:   "testdb",
		Collection: "metrics",
		Date:       date,
		EntryCount: 1,
		Immediate:  true, // Force immediate flush
	})

	time.Sleep(100 * time.Millisecond)

	worker.Stop()
	_ = memStore.Close()
}

func TestFlushWorkerPool_GetOrCreateWorker(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 10000, logger)
	mockStorage := newFlushMockMainStorage()

	pool := NewFlushWorkerPool(
		DefaultFlushWorkerPoolConfig(),
		logger,
		mockWAL,
		memStore,
		mockStorage,
		nil,
	)

	now := time.Now()

	// Create first worker
	worker1 := pool.getOrCreateWorker("testdb", "metrics", now)
	if worker1 == nil {
		t.Fatal("Expected non-nil worker")
	}

	// Get same worker
	worker2 := pool.getOrCreateWorker("testdb", "metrics", now)
	if worker1 != worker2 {
		t.Error("Expected same worker for same partition")
	}

	// Create different worker
	worker3 := pool.getOrCreateWorker("otherdb", "metrics", now)
	if worker1 == worker3 {
		t.Error("Expected different worker for different database")
	}

	// Check stats
	stats := pool.GetStats()
	if stats["worker_count"].(int) != 2 {
		t.Errorf("worker_count = %d, expected 2", stats["worker_count"].(int))
	}

	// Stop workers
	worker1.Stop()
	worker3.Stop()
	_ = memStore.Close()
}

func TestFlushWorker_QueueFull(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 10000, logger)
	mockStorage := newFlushMockMainStorage()

	worker := NewFlushWorker(
		FlushWorkerConfig{
			Key:               "testdb:metrics:2024-05-15",
			Database:          "testdb",
			Collection:        "metrics",
			Date:              time.Now(),
			MaxBatchSize:      10,
			FlushDelay:        time.Second,
			MaxPendingEntries: 100,
			QueueSize:         2, // Very small queue
		},
		logger,
		mockWAL,
		memStore,
		mockStorage,
		nil,
	)

	// Don't start worker - let queue fill up

	// Fill queue
	worker.Notify(WriteNotification{EntryCount: 1})
	worker.Notify(WriteNotification{EntryCount: 1})

	// Third notification should be dropped (queue full)
	worker.Notify(WriteNotification{EntryCount: 1})

	_ = memStore.Close()
}

func TestMockPartitionedWriter(t *testing.T) {
	mock := newMockPartitionedWriter()

	// Test WritePartitioned
	entry := &wal.Entry{
		Database:   "testdb",
		Collection: "metrics",
		ID:         "device1",
	}
	now := time.Now()
	result, err := mock.WritePartitioned(entry, "testdb", now)
	if err != nil {
		t.Errorf("WritePartitioned error: %v", err)
	}
	if result.IsNewSegment {
		t.Error("Expected IsNewSegment=false")
	}

	// Test ReadPartition
	entries, err := mock.ReadPartition("testdb", "metrics", now)
	if err != nil {
		t.Errorf("ReadPartition error: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("Expected 1 entry, got %d", len(entries))
	}

	// Test FlushPartition
	if err := mock.FlushPartition("testdb", "metrics", now); err != nil {
		t.Errorf("FlushPartition error: %v", err)
	}
	if mock.flushCalled != 1 {
		t.Errorf("flushCalled = %d, expected 1", mock.flushCalled)
	}

	// Test GetStats
	stats := mock.GetStats()
	if stats.PartitionCount != 1 {
		t.Errorf("PartitionCount = %d, expected 1", stats.PartitionCount)
	}

	// Test Close
	if err := mock.Close(); err != nil {
		t.Errorf("Close error: %v", err)
	}
	if mock.closeCalled != 1 {
		t.Errorf("closeCalled = %d, expected 1", mock.closeCalled)
	}
}

func TestFlushMockMainStorage(t *testing.T) {
	mock := newFlushMockMainStorage()

	// Test WriteBatch
	points := []*DataPoint{
		{ID: "device1", Fields: map[string]interface{}{"temp": 25.0}},
		{ID: "device2", Fields: map[string]interface{}{"temp": 26.0}},
	}

	if err := mock.WriteBatch(points); err != nil {
		t.Errorf("WriteBatch error: %v", err)
	}

	batches := mock.GetWrittenBatches()
	if len(batches) != 1 {
		t.Errorf("Expected 1 batch, got %d", len(batches))
	}
	if len(batches[0]) != 2 {
		t.Errorf("Expected 2 points in batch, got %d", len(batches[0]))
	}
}
