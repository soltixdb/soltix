package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/wal"
)

// =============================================================================
// WriteWorkerPool Submit — covers Submit branches (currently 60%)
// =============================================================================

func TestWriteWorkerPool_Submit_ValidMessage(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 1000, logger)

	pool := NewWriteWorkerPool(logger, mockWAL, memStore, nil)
	pool.Start()
	defer pool.Stop()

	msg := WriteMessage{
		Database:   "db1",
		Collection: "metrics",
		Time:       time.Now().Format(time.RFC3339),
		ID:         "dev-1",
		Fields:     map[string]interface{}{"temp": 25.5},
	}

	err := pool.Submit(msg)
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	// Give worker time to process
	time.Sleep(50 * time.Millisecond)
}

func TestWriteWorkerPool_Submit_BadTimeParseFails(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 1000, logger)

	pool := NewWriteWorkerPool(logger, mockWAL, memStore, nil)
	pool.Start()
	defer pool.Stop()

	msg := WriteMessage{
		Database:   "db1",
		Collection: "metrics",
		Time:       "not-a-time",
		ID:         "dev-1",
		Fields:     map[string]interface{}{"temp": 1.0},
	}

	err := pool.Submit(msg)
	if err == nil {
		t.Fatal("expected error for invalid time")
	}
}

func TestWriteWorkerPool_Submit_MultiplePartitions(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 1000, logger)

	pool := NewWriteWorkerPool(logger, mockWAL, memStore, nil)
	pool.Start()
	defer pool.Stop()

	// Submit to different partitions (different databases, dates)
	for i := 0; i < 10; i++ {
		msg := WriteMessage{
			Database:   fmt.Sprintf("db%d", i%3),
			Collection: "metrics",
			Time:       time.Now().Add(time.Duration(-i) * time.Hour).Format(time.RFC3339),
			ID:         fmt.Sprintf("dev-%d", i),
			Fields:     map[string]interface{}{"val": float64(i)},
		}
		if err := pool.Submit(msg); err != nil {
			t.Fatalf("Submit %d failed: %v", i, err)
		}
	}

	time.Sleep(100 * time.Millisecond)
}

func TestWriteWorkerPool_Submit_ReuseWorker(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 1000, logger)

	pool := NewWriteWorkerPool(logger, mockWAL, memStore, nil)
	pool.Start()
	defer pool.Stop()

	now := time.Now()
	for i := 0; i < 5; i++ {
		msg := WriteMessage{
			Database:   "db1",
			Collection: "metrics",
			Time:       now.Format(time.RFC3339),
			ID:         fmt.Sprintf("dev-%d", i),
			Fields:     map[string]interface{}{"val": float64(i)},
		}
		if err := pool.Submit(msg); err != nil {
			t.Fatalf("Submit %d failed: %v", i, err)
		}
	}

	time.Sleep(50 * time.Millisecond)

	// Should have only 1 worker for the same partition
	stats := pool.Stats()
	wc, ok := stats["worker_count"].(int)
	if !ok {
		t.Fatal("could not get worker_count from stats")
	}
	if wc != 1 {
		t.Errorf("expected 1 worker for same partition, got %d", wc)
	}
}

// =============================================================================
// WriteWorkerPool SubmitSync — covers SubmitSync (currently 25%)
// =============================================================================

func TestWriteWorkerPool_SubmitSync_ValidMessage(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 1000, logger)

	pool := NewWriteWorkerPool(logger, mockWAL, memStore, nil)
	pool.Start()
	defer pool.Stop()

	msg := WriteMessage{
		Database:   "db1",
		Collection: "metrics",
		Time:       time.Now().Format(time.RFC3339),
		ID:         "dev-sync-1",
		Fields:     map[string]interface{}{"temp": 30.0},
	}

	err := pool.SubmitSync(msg)
	if err != nil {
		t.Fatalf("SubmitSync failed: %v", err)
	}
}

func TestWriteWorkerPool_SubmitSync_BadTimeParseFails(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 1000, logger)

	pool := NewWriteWorkerPool(logger, mockWAL, memStore, nil)
	pool.Start()
	defer pool.Stop()

	msg := WriteMessage{
		Database:   "db1",
		Collection: "metrics",
		Time:       "invalid",
		ID:         "dev-sync-1",
		Fields:     map[string]interface{}{"temp": 30.0},
	}

	err := pool.SubmitSync(msg)
	if err == nil {
		t.Fatal("expected error for invalid time in SubmitSync")
	}
}

func TestWriteWorkerPool_SubmitSync_MultipleSync(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 1000, logger)

	pool := NewWriteWorkerPool(logger, mockWAL, memStore, nil)
	pool.Start()
	defer pool.Stop()

	for i := 0; i < 10; i++ {
		msg := WriteMessage{
			Database:   "db1",
			Collection: "metrics",
			Time:       time.Now().Format(time.RFC3339),
			ID:         fmt.Sprintf("sync-dev-%d", i),
			Fields:     map[string]interface{}{"val": float64(i)},
		}
		if err := pool.SubmitSync(msg); err != nil {
			t.Fatalf("SubmitSync %d failed: %v", i, err)
		}
	}
}

// =============================================================================
// WriteWorkerPool Stop — covers Stop with workers (currently 83.3%)
// =============================================================================

func TestWriteWorkerPool_StopDrainsWorkers(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 1000, logger)

	pool := NewWriteWorkerPool(logger, mockWAL, memStore, nil)
	pool.Start()

	// Submit some messages to create workers
	for i := 0; i < 5; i++ {
		msg := WriteMessage{
			Database:   fmt.Sprintf("db%d", i),
			Collection: "metrics",
			Time:       time.Now().Format(time.RFC3339),
			ID:         fmt.Sprintf("dev-%d", i),
			Fields:     map[string]interface{}{"val": float64(i)},
		}
		_ = pool.Submit(msg)
	}

	time.Sleep(50 * time.Millisecond)

	// Stop should drain all
	pool.Stop()
}

// =============================================================================
// WriteWorkerPool Stats
// =============================================================================

func TestWriteWorkerPool_Stats_Empty(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 1000, logger)

	pool := NewWriteWorkerPool(logger, mockWAL, memStore, nil)
	pool.Start()
	defer pool.Stop()

	stats := pool.Stats()
	if stats["worker_count"].(int) != 0 {
		t.Errorf("expected 0 workers, got %v", stats["worker_count"])
	}
}

func TestWriteWorkerPool_Stats_WithWorkers(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 1000, logger)

	pool := NewWriteWorkerPool(logger, mockWAL, memStore, nil)
	pool.Start()
	defer pool.Stop()

	// Create 3 different partition workers
	for i := 0; i < 3; i++ {
		msg := WriteMessage{
			Database:   fmt.Sprintf("db%d", i),
			Collection: "metrics",
			Time:       time.Now().Format(time.RFC3339),
			ID:         fmt.Sprintf("dev-%d", i),
			Fields:     map[string]interface{}{"val": float64(i)},
		}
		_ = pool.Submit(msg)
	}

	time.Sleep(50 * time.Millisecond)

	stats := pool.Stats()
	wc := stats["worker_count"].(int)
	if wc != 3 {
		t.Errorf("expected 3 workers, got %d", wc)
	}

	workers, ok := stats["workers"].([]map[string]interface{})
	if !ok || len(workers) != 3 {
		t.Errorf("expected 3 worker stats entries, got %d", len(workers))
	}
}

// =============================================================================
// WriteWorker processTask — covers processTask (currently 66.7%)
// =============================================================================

func TestWriteWorker_ProcessTask_Success(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 1000, logger)

	worker := &WriteWorker{
		key:        "db1:metrics:2025-01-01",
		database:   "db1",
		collection: "metrics",
		date:       "2025-01-01",
		dateParsed: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		dataCh:     make(chan *WriteTask, 100),
		logger:     logger,
		wal:        mockWAL,
		memStore:   memStore,
		memMaxAge:  time.Hour,
		lastActive: time.Now(),
		stopCh:     make(chan struct{}),
	}

	task := &WriteTask{
		Entry: &wal.Entry{
			Type:       wal.EntryTypeWrite,
			Database:   "db1",
			Collection: "metrics",
			Time:       time.Now().Format(time.RFC3339),
			ID:         "dev-1",
			Fields:     map[string]interface{}{"temp": 25.5},
			Timestamp:  time.Now().UnixNano(),
		},
		DataPoint: &DataPoint{
			Database:   "db1",
			Collection: "metrics",
			Time:       time.Now(),
			ID:         "dev-1",
			Fields:     map[string]interface{}{"temp": 25.5},
			InsertedAt: time.Now(),
		},
	}

	worker.processTask(task)
	// No error expected — check WAL got the entry
}

func TestWriteWorker_ProcessTask_WithResultCh(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 1000, logger)

	worker := &WriteWorker{
		key:        "db1:metrics:2025-01-01",
		database:   "db1",
		collection: "metrics",
		date:       "2025-01-01",
		dateParsed: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		dataCh:     make(chan *WriteTask, 100),
		logger:     logger,
		wal:        mockWAL,
		memStore:   memStore,
		memMaxAge:  time.Hour,
		lastActive: time.Now(),
		stopCh:     make(chan struct{}),
	}

	resultCh := make(chan error, 1)
	task := &WriteTask{
		Entry: &wal.Entry{
			Type:       wal.EntryTypeWrite,
			Database:   "db1",
			Collection: "metrics",
			Time:       time.Now().Format(time.RFC3339),
			ID:         "dev-sync",
			Fields:     map[string]interface{}{"temp": 25.5},
			Timestamp:  time.Now().UnixNano(),
		},
		DataPoint: &DataPoint{
			Database:   "db1",
			Collection: "metrics",
			Time:       time.Now(),
			ID:         "dev-sync",
			Fields:     map[string]interface{}{"temp": 25.5},
			InsertedAt: time.Now(),
		},
		ResultCh: resultCh,
	}

	worker.processTask(task)

	// Should receive result
	select {
	case err := <-resultCh:
		if err != nil {
			t.Fatalf("expected nil error from result channel, got: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for result")
	}
}

// =============================================================================
// WriteWorker processWrite — covers processWrite (currently 76.9%)
// covers: old data skips memStore, new segment notifies flush pool
// =============================================================================

func TestWriteWorker_ProcessWrite_OldData_SkipsMemStore(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 1000, logger)

	worker := &WriteWorker{
		key:        "db1:metrics:2023-01-01",
		database:   "db1",
		collection: "metrics",
		date:       "2023-01-01",
		dateParsed: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
		dataCh:     make(chan *WriteTask, 100),
		logger:     logger,
		wal:        mockWAL,
		memStore:   memStore,
		memMaxAge:  time.Hour,
		lastActive: time.Now(),
		stopCh:     make(chan struct{}),
	}

	oldTime := time.Now().Add(-2 * time.Hour)
	task := &WriteTask{
		Entry: &wal.Entry{
			Type:       wal.EntryTypeWrite,
			Database:   "db1",
			Collection: "metrics",
			Time:       oldTime.Format(time.RFC3339),
			ID:         "dev-old",
			Fields:     map[string]interface{}{"temp": 10.0},
			Timestamp:  oldTime.UnixNano(),
		},
		DataPoint: &DataPoint{
			Database:   "db1",
			Collection: "metrics",
			Time:       oldTime,
			ID:         "dev-old",
			Fields:     map[string]interface{}{"temp": 10.0},
			InsertedAt: time.Now(),
		},
	}

	err := worker.processWrite(task)
	if err != nil {
		t.Fatalf("processWrite failed: %v", err)
	}

	// memStore should be empty (old data)
	results, _ := memStore.QueryCollection("db1", "metrics", time.Time{}, time.Now().Add(time.Minute))
	if len(results) != 0 {
		t.Errorf("expected 0 data points in memStore for old data, got %d", len(results))
	}
}

func TestWriteWorker_ProcessWrite_RecentData_InMemStore(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 1000, logger)

	now := time.Now()
	worker := &WriteWorker{
		key:        "db1:metrics:" + now.Format("2006-01-02"),
		database:   "db1",
		collection: "metrics",
		date:       now.Format("2006-01-02"),
		dateParsed: now.Truncate(24 * time.Hour),
		dataCh:     make(chan *WriteTask, 100),
		logger:     logger,
		wal:        mockWAL,
		memStore:   memStore,
		memMaxAge:  time.Hour,
		lastActive: time.Now(),
		stopCh:     make(chan struct{}),
	}

	task := &WriteTask{
		Entry: &wal.Entry{
			Type:       wal.EntryTypeWrite,
			Database:   "db1",
			Collection: "metrics",
			Time:       now.Format(time.RFC3339),
			ID:         "dev-recent",
			Fields:     map[string]interface{}{"temp": 25.5},
			Timestamp:  now.UnixNano(),
		},
		DataPoint: &DataPoint{
			Database:   "db1",
			Collection: "metrics",
			Time:       now,
			ID:         "dev-recent",
			Fields:     map[string]interface{}{"temp": 25.5},
			InsertedAt: time.Now(),
		},
	}

	err := worker.processWrite(task)
	if err != nil {
		t.Fatalf("processWrite failed: %v", err)
	}

	// memStore should have data
	results, _ := memStore.QueryCollection("db1", "metrics", time.Time{}, time.Now().Add(time.Minute))
	if len(results) != 1 {
		t.Errorf("expected 1 data point in memStore, got %d", len(results))
	}
}

// =============================================================================
// WriteWorker touch / getLastActive
// =============================================================================

func TestWriteWorker_Touch_UpdatesLastActive(t *testing.T) {
	worker := &WriteWorker{
		lastActive: time.Now().Add(-time.Hour),
	}

	before := worker.getLastActive()
	worker.touch()
	after := worker.getLastActive()

	if !after.After(before) {
		t.Error("expected lastActive to be updated after touch()")
	}
}

// =============================================================================
// WriteWorker Stop — double stop
// =============================================================================

func TestWriteWorker_DoubleStop(t *testing.T) {
	worker := &WriteWorker{
		key:    "test",
		stopCh: make(chan struct{}),
	}
	worker.Stop()
	worker.Stop() // should not panic
}

// =============================================================================
// WriteWorker drain — covers drain()
// =============================================================================

func TestWriteWorker_Drain_EmptyChannel(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 1000, logger)

	worker := &WriteWorker{
		key:        "db1:metrics:2025-01-01",
		database:   "db1",
		collection: "metrics",
		date:       "2025-01-01",
		dateParsed: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		dataCh:     make(chan *WriteTask, 100),
		logger:     logger,
		wal:        mockWAL,
		memStore:   memStore,
		memMaxAge:  time.Hour,
		lastActive: time.Now(),
		stopCh:     make(chan struct{}),
	}

	// drain on empty channel should return immediately
	worker.drain()
}

func TestWriteWorker_Drain_WithPendingTasks(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 1000, logger)

	worker := &WriteWorker{
		key:        "db1:metrics:2025-01-01",
		database:   "db1",
		collection: "metrics",
		date:       "2025-01-01",
		dateParsed: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		dataCh:     make(chan *WriteTask, 100),
		logger:     logger,
		wal:        mockWAL,
		memStore:   memStore,
		memMaxAge:  time.Hour,
		lastActive: time.Now(),
		stopCh:     make(chan struct{}),
	}

	// Put 3 tasks in the channel
	for i := 0; i < 3; i++ {
		worker.dataCh <- &WriteTask{
			Entry: &wal.Entry{
				Type:       wal.EntryTypeWrite,
				Database:   "db1",
				Collection: "metrics",
				Time:       time.Now().Format(time.RFC3339),
				ID:         fmt.Sprintf("drain-%d", i),
				Fields:     map[string]interface{}{"val": float64(i)},
				Timestamp:  time.Now().UnixNano(),
			},
			DataPoint: &DataPoint{
				Database:   "db1",
				Collection: "metrics",
				Time:       time.Now(),
				ID:         fmt.Sprintf("drain-%d", i),
				Fields:     map[string]interface{}{"val": float64(i)},
				InsertedAt: time.Now(),
			},
		}
	}

	worker.drain()

	if len(worker.dataCh) != 0 {
		t.Errorf("expected channel to be empty after drain, got %d", len(worker.dataCh))
	}
}

// =============================================================================
// WriteWorkerPool doCleanup — covers doCleanup path
// =============================================================================

func TestWriteWorkerPool_DoCleanup_RemovesIdleWorkers(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 1000, logger)

	pool := NewWriteWorkerPool(logger, mockWAL, memStore, nil)
	pool.idleTimeout = 10 * time.Millisecond // very short for testing
	pool.Start()
	defer pool.Stop()

	// Submit to create a worker
	msg := WriteMessage{
		Database:   "db1",
		Collection: "metrics",
		Time:       time.Now().Format(time.RFC3339),
		ID:         "dev-idle",
		Fields:     map[string]interface{}{"val": 1.0},
	}
	_ = pool.Submit(msg)

	time.Sleep(50 * time.Millisecond)

	// Worker should exist
	pool.workersMu.RLock()
	initialCount := len(pool.workers)
	pool.workersMu.RUnlock()
	if initialCount == 0 {
		t.Fatal("expected at least 1 worker")
	}

	// Wait for idle timeout
	time.Sleep(50 * time.Millisecond)

	// Force cleanup
	pool.doCleanup()

	pool.workersMu.RLock()
	afterCount := len(pool.workers)
	pool.workersMu.RUnlock()

	if afterCount >= initialCount {
		t.Errorf("expected cleanup to remove idle workers, before=%d after=%d", initialCount, afterCount)
	}
}

// =============================================================================
// WriteWorkerPool with FlushPool notification
// =============================================================================

func TestWriteWorkerPool_WithFlushPoolNotification(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 1000, logger)

	// Set up a real flush pool so the notification path gets covered
	mainStorage := newFlushMockMainStorage()
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
		nil,
	)

	pool := NewWriteWorkerPool(logger, mockWAL, memStore, flushPool)
	pool.Start()
	defer pool.Stop()

	// Submit message — processWrite will be called
	msg := WriteMessage{
		Database:   "db1",
		Collection: "metrics",
		Time:       time.Now().Format(time.RFC3339),
		ID:         "dev-notif",
		Fields:     map[string]interface{}{"temp": 20.0},
	}

	err := pool.Submit(msg)
	if err != nil {
		t.Fatalf("Submit with flush pool failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
}
