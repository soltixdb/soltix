package storage

import (
	"sync"
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/wal"
)

// =============================================================================
// FlushWorker flush() — currently 69.8% coverage
// =============================================================================

func TestFlushWorker_Flush_WithSegments(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 1000, logger)
	mockStorage := newFlushMockMainStorage()

	date := time.Date(2025, 6, 15, 0, 0, 0, 0, time.UTC)

	// Setup test data: WAL has entries and segments
	now := time.Now()
	entries := []*wal.Entry{
		{
			Type:       wal.EntryTypeWrite,
			Database:   "db1",
			Collection: "metrics",
			Time:       now.Format(time.RFC3339),
			ID:         "dev-1",
			Fields:     map[string]interface{}{"temp": 25.5},
			Timestamp:  now.UnixNano(),
		},
		{
			Type:       wal.EntryTypeWrite,
			Database:   "db1",
			Collection: "metrics",
			Time:       now.Add(time.Second).Format(time.RFC3339),
			ID:         "dev-2",
			Fields:     map[string]interface{}{"temp": 30.0},
			Timestamp:  now.Add(time.Second).UnixNano(),
		},
	}

	mockWAL.SetTestData("db1", "metrics", date, entries, []string{"segment-001.log"})

	worker := NewFlushWorker(FlushWorkerConfig{
		Key:               "db1:metrics:2025-06-15",
		Database:          "db1",
		Collection:        "metrics",
		Date:              date,
		MaxBatchSize:      100,
		FlushDelay:        time.Second,
		MaxPendingEntries: 10000,
		QueueSize:         1000,
	}, logger, mockWAL, memStore, mockStorage, nil)

	// Manually set pending count
	worker.pendingCount = 2

	worker.flush()

	// Verify storage got the data
	mockStorage.mu.Lock()
	batchCount := len(mockStorage.writtenBatches)
	mockStorage.mu.Unlock()

	if batchCount != 1 {
		t.Errorf("expected 1 batch written to storage, got %d", batchCount)
	}

	// Verify pendingCount was reset
	worker.mu.RLock()
	pc := worker.pendingCount
	fc := worker.totalFlushCount
	worker.mu.RUnlock()

	if pc != 0 {
		t.Errorf("expected pendingCount=0 after flush, got %d", pc)
	}
	if fc != 1 {
		t.Errorf("expected totalFlushCount=1, got %d", fc)
	}
}

func TestFlushWorker_Flush_NoSegments(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 1000, logger)
	mockStorage := newFlushMockMainStorage()

	date := time.Date(2025, 6, 15, 0, 0, 0, 0, time.UTC)
	// No prepared files, so PrepareFlushPartition returns nil

	worker := NewFlushWorker(FlushWorkerConfig{
		Key:               "db1:metrics:2025-06-15",
		Database:          "db1",
		Collection:        "metrics",
		Date:              date,
		MaxBatchSize:      100,
		FlushDelay:        time.Second,
		MaxPendingEntries: 10000,
		QueueSize:         1000,
	}, logger, mockWAL, memStore, mockStorage, nil)

	worker.flush()

	// No storage writes expected
	mockStorage.mu.Lock()
	batchCount := len(mockStorage.writtenBatches)
	mockStorage.mu.Unlock()

	if batchCount != 0 {
		t.Errorf("expected 0 batches written, got %d", batchCount)
	}
}

func TestFlushWorker_Flush_EmptyEntries(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 1000, logger)
	mockStorage := newFlushMockMainStorage()

	date := time.Date(2025, 6, 15, 0, 0, 0, 0, time.UTC)

	// Segments exist but have no entries
	mockWAL.SetTestData("db1", "metrics", date, nil, []string{"segment-empty.log"})
	// Override entries to be empty after PrepareFlush returns the files
	mockWAL.mu.Lock()
	key := "db1:metrics:2025-06-15"
	mockWAL.entries[key] = nil // empty entries
	// But keep preparedFiles so PrepareFlush returns segment files
	mockWAL.mu.Unlock()

	worker := NewFlushWorker(FlushWorkerConfig{
		Key:               "db1:metrics:2025-06-15",
		Database:          "db1",
		Collection:        "metrics",
		Date:              date,
		MaxBatchSize:      100,
		FlushDelay:        time.Second,
		MaxPendingEntries: 10000,
		QueueSize:         1000,
	}, logger, mockWAL, memStore, mockStorage, nil)

	worker.flush()

	// No writes to storage
	mockStorage.mu.Lock()
	batchCount := len(mockStorage.writtenBatches)
	mockStorage.mu.Unlock()

	if batchCount != 0 {
		t.Errorf("expected 0 batches for empty entries, got %d", batchCount)
	}
}

// =============================================================================
// FlushWorkerPool Notify — currently 20% for pool-level Notify
// =============================================================================

func TestFlushWorkerPool_Notify_QueuesNotification(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 1000, logger)
	mockStorage := newFlushMockMainStorage()

	pool := NewFlushWorkerPool(
		FlushWorkerPoolConfig{
			MaxBatchSize:      100,
			FlushDelay:        time.Second,
			MaxPendingEntries: 10000,
			WorkerIdleTimeout: 5 * time.Minute,
			QueueSize:         1000,
		},
		logger, mockWAL, memStore, mockStorage, nil,
	)

	pool.Start()
	defer pool.Stop()

	// Send a notification
	pool.Notify(WriteNotification{
		Database:   "db1",
		Date:       time.Now(),
		Collection: "metrics",
		EntryCount: 1,
		Immediate:  false,
	})

	// Give the dispatch loop time to route
	time.Sleep(50 * time.Millisecond)

	// Check that a worker was created
	pool.workersMu.RLock()
	wc := len(pool.workers)
	pool.workersMu.RUnlock()

	if wc != 1 {
		t.Errorf("expected 1 worker after notification, got %d", wc)
	}
}

func TestFlushWorkerPool_Notify_ImmediateFlush(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 1000, logger)
	mockStorage := newFlushMockMainStorage()

	pool := NewFlushWorkerPool(
		FlushWorkerPoolConfig{
			MaxBatchSize:      100,
			FlushDelay:        time.Second,
			MaxPendingEntries: 10000,
			WorkerIdleTimeout: 5 * time.Minute,
			QueueSize:         1000,
		},
		logger, mockWAL, memStore, mockStorage, nil,
	)

	pool.Start()
	defer pool.Stop()

	// Send an immediate notification (e.g., historical data)
	pool.Notify(WriteNotification{
		Database:   "db1",
		Date:       time.Now(),
		Collection: "metrics",
		EntryCount: 1,
		Immediate:  true,
	})

	time.Sleep(100 * time.Millisecond)
}

func TestFlushWorkerPool_Notify_MultipleNotifications(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 1000, logger)
	mockStorage := newFlushMockMainStorage()

	pool := NewFlushWorkerPool(
		FlushWorkerPoolConfig{
			MaxBatchSize:      100,
			FlushDelay:        time.Second,
			MaxPendingEntries: 10000,
			WorkerIdleTimeout: 5 * time.Minute,
			QueueSize:         1000,
		},
		logger, mockWAL, memStore, mockStorage, nil,
	)

	pool.Start()
	defer pool.Stop()

	// Send notifications to different partitions
	for i := 0; i < 5; i++ {
		pool.Notify(WriteNotification{
			Database:   "db1",
			Date:       time.Now().AddDate(0, 0, -i),
			Collection: "metrics",
			EntryCount: 1,
		})
	}

	time.Sleep(100 * time.Millisecond)
}

// =============================================================================
// FlushWorker Notify — currently 73.7%
// =============================================================================

func TestFlushWorker_Notify_UpdatesLastActive(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 1000, logger)
	mockStorage := newFlushMockMainStorage()

	date := time.Now()
	worker := NewFlushWorker(FlushWorkerConfig{
		Key:               "db1:metrics:" + date.Format("2006-01-02"),
		Database:          "db1",
		Collection:        "metrics",
		Date:              date,
		MaxBatchSize:      100,
		FlushDelay:        time.Second,
		MaxPendingEntries: 10000,
		QueueSize:         100,
	}, logger, mockWAL, memStore, mockStorage, nil)

	worker.Start()
	defer worker.Stop()

	before := worker.LastActive()

	time.Sleep(10 * time.Millisecond)

	worker.Notify(WriteNotification{
		Database:   "db1",
		Collection: "metrics",
		Date:       date,
		EntryCount: 1,
	})

	time.Sleep(50 * time.Millisecond)

	after := worker.LastActive()
	if !after.After(before) {
		t.Error("expected lastActive to be updated after Notify")
	}
}

// =============================================================================
// FlushWorker run() — covers the timer-based flush path (87.2%)
// =============================================================================

func TestFlushWorker_Run_TimerBasedFlush(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 1000, logger)
	mockStorage := newFlushMockMainStorage()

	date := time.Now()
	worker := NewFlushWorker(FlushWorkerConfig{
		Key:               "db1:metrics:" + date.Format("2006-01-02"),
		Database:          "db1",
		Collection:        "metrics",
		Date:              date,
		MaxBatchSize:      100,
		FlushDelay:        50 * time.Millisecond, // Very short delay for test
		MaxPendingEntries: 10000,
		QueueSize:         100,
	}, logger, mockWAL, memStore, mockStorage, nil)

	worker.Start()

	// Send a non-immediate notification to start the timer
	worker.Notify(WriteNotification{
		Database:   "db1",
		Collection: "metrics",
		Date:       date,
		EntryCount: 1,
	})

	// Wait for timer-based flush to trigger (50ms delay + some margin)
	time.Sleep(200 * time.Millisecond)

	worker.Stop()
}

func TestFlushWorker_Run_CountBasedImmediateFlush(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 1000, logger)
	mockStorage := newFlushMockMainStorage()

	date := time.Now()
	worker := NewFlushWorker(FlushWorkerConfig{
		Key:               "db1:metrics:" + date.Format("2006-01-02"),
		Database:          "db1",
		Collection:        "metrics",
		Date:              date,
		MaxBatchSize:      100,
		FlushDelay:        10 * time.Second,
		MaxPendingEntries: 3, // Very low threshold
		QueueSize:         100,
	}, logger, mockWAL, memStore, mockStorage, nil)

	worker.Start()

	// Send enough notifications to exceed MaxPendingEntries
	for i := 0; i < 5; i++ {
		worker.Notify(WriteNotification{
			Database:   "db1",
			Collection: "metrics",
			Date:       date,
			EntryCount: 1,
		})
	}

	time.Sleep(100 * time.Millisecond)
	worker.Stop()
}

func TestFlushWorker_Run_ManualTrigger(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 1000, logger)
	mockStorage := newFlushMockMainStorage()

	date := time.Now()
	worker := NewFlushWorker(FlushWorkerConfig{
		Key:               "db1:metrics:" + date.Format("2006-01-02"),
		Database:          "db1",
		Collection:        "metrics",
		Date:              date,
		MaxBatchSize:      100,
		FlushDelay:        10 * time.Second,
		MaxPendingEntries: 10000,
		QueueSize:         100,
	}, logger, mockWAL, memStore, mockStorage, nil)

	worker.Start()

	// Send a notification to create pending data
	worker.Notify(WriteNotification{
		Database:   "db1",
		Collection: "metrics",
		Date:       date,
		EntryCount: 1,
	})

	time.Sleep(50 * time.Millisecond)

	// Manually trigger flush
	worker.TriggerFlush()

	time.Sleep(50 * time.Millisecond)
	worker.Stop()
}

// =============================================================================
// FlushWorkerPool FlushAll
// =============================================================================

func TestFlushWorkerPool_FlushAll_WithWorkers(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 1000, logger)
	mockStorage := newFlushMockMainStorage()

	pool := NewFlushWorkerPool(
		FlushWorkerPoolConfig{
			MaxBatchSize:      100,
			FlushDelay:        time.Second,
			MaxPendingEntries: 10000,
			WorkerIdleTimeout: 5 * time.Minute,
			QueueSize:         1000,
		},
		logger, mockWAL, memStore, mockStorage, nil,
	)

	pool.Start()
	defer pool.Stop()

	// Create workers via notifications
	pool.Notify(WriteNotification{
		Database:   "db1",
		Date:       time.Now(),
		Collection: "metrics",
		EntryCount: 1,
	})
	pool.Notify(WriteNotification{
		Database:   "db2",
		Date:       time.Now(),
		Collection: "logs",
		EntryCount: 1,
	})

	time.Sleep(50 * time.Millisecond)

	pool.FlushAll()

	time.Sleep(50 * time.Millisecond)
}

// =============================================================================
// FlushWorkerPool GetStats
// =============================================================================

func TestFlushWorkerPool_GetStats_WithActiveWorkers(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 1000, logger)
	mockStorage := newFlushMockMainStorage()

	pool := NewFlushWorkerPool(
		FlushWorkerPoolConfig{
			MaxBatchSize:      100,
			FlushDelay:        time.Second,
			MaxPendingEntries: 10000,
			WorkerIdleTimeout: 5 * time.Minute,
			QueueSize:         1000,
		},
		logger, mockWAL, memStore, mockStorage, nil,
	)

	pool.Start()
	defer pool.Stop()

	pool.Notify(WriteNotification{
		Database:   "db1",
		Date:       time.Now(),
		Collection: "metrics",
		EntryCount: 5,
	})

	time.Sleep(50 * time.Millisecond)

	stats := pool.GetStats()
	wc := stats["worker_count"].(int)
	if wc != 1 {
		t.Errorf("expected worker_count=1, got %d", wc)
	}
}

// =============================================================================
// FlushWorkerPool cleanupIdleWorkers
// =============================================================================

func TestFlushWorkerPool_CleanupIdleWorkers_RemovesExpired(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 1000, logger)
	mockStorage := newFlushMockMainStorage()

	pool := NewFlushWorkerPool(
		FlushWorkerPoolConfig{
			MaxBatchSize:      100,
			FlushDelay:        time.Second,
			MaxPendingEntries: 10000,
			WorkerIdleTimeout: 10 * time.Millisecond, // very short
			QueueSize:         1000,
		},
		logger, mockWAL, memStore, mockStorage, nil,
	)

	pool.Start()
	defer pool.Stop()

	pool.Notify(WriteNotification{
		Database:   "db1",
		Date:       time.Now(),
		Collection: "metrics",
		EntryCount: 1,
	})

	time.Sleep(50 * time.Millisecond)

	pool.workersMu.RLock()
	before := len(pool.workers)
	pool.workersMu.RUnlock()

	if before == 0 {
		t.Fatal("expected at least 1 worker before cleanup")
	}

	// Wait for idle timeout
	time.Sleep(50 * time.Millisecond)

	pool.cleanupIdleWorkers()

	pool.workersMu.RLock()
	after := len(pool.workers)
	pool.workersMu.RUnlock()

	if after >= before {
		t.Errorf("expected idle workers to be removed, before=%d after=%d", before, after)
	}
}

// =============================================================================
// FlushWorkerPool dispatchLoop — concurrent notifications
// =============================================================================

func TestFlushWorkerPool_DispatchLoop_ConcurrentNotifications(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 1000, logger)
	mockStorage := newFlushMockMainStorage()

	pool := NewFlushWorkerPool(
		FlushWorkerPoolConfig{
			MaxBatchSize:      100,
			FlushDelay:        time.Second,
			MaxPendingEntries: 10000,
			WorkerIdleTimeout: 5 * time.Minute,
			QueueSize:         1000,
		},
		logger, mockWAL, memStore, mockStorage, nil,
	)

	pool.Start()
	defer pool.Stop()

	// Send many notifications concurrently
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			pool.Notify(WriteNotification{
				Database:   "db1",
				Date:       time.Now(),
				Collection: "metrics",
				EntryCount: 1,
			})
		}(i)
	}

	wg.Wait()
	time.Sleep(100 * time.Millisecond)
}

// =============================================================================
// FlushWorker GetStats — covers stats collection
// =============================================================================

func TestFlushWorker_GetStats_AfterFlush(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 1000, logger)
	mockStorage := newFlushMockMainStorage()

	date := time.Date(2025, 6, 15, 0, 0, 0, 0, time.UTC)
	now := time.Now()
	entries := []*wal.Entry{
		{
			Type:       wal.EntryTypeWrite,
			Database:   "db1",
			Collection: "metrics",
			Time:       now.Format(time.RFC3339),
			ID:         "dev-1",
			Fields:     map[string]interface{}{"val": 1.0},
			Timestamp:  now.UnixNano(),
		},
	}
	mockWAL.SetTestData("db1", "metrics", date, entries, []string{"seg.log"})

	worker := NewFlushWorker(FlushWorkerConfig{
		Key:               "db1:metrics:2025-06-15",
		Database:          "db1",
		Collection:        "metrics",
		Date:              date,
		MaxBatchSize:      100,
		FlushDelay:        time.Second,
		MaxPendingEntries: 10000,
		QueueSize:         100,
	}, logger, mockWAL, memStore, mockStorage, nil)

	worker.pendingCount = 1
	worker.flush()

	stats := worker.GetStats()
	if stats["total_flush_count"].(int64) != 1 {
		t.Errorf("expected total_flush_count=1, got %v", stats["total_flush_count"])
	}
	if stats["total_flushed_ops"].(int64) != 1 {
		t.Errorf("expected total_flushed_ops=1, got %v", stats["total_flushed_ops"])
	}
	if stats["pending_count"].(int) != 0 {
		t.Errorf("expected pending_count=0 after flush, got %v", stats["pending_count"])
	}
}

// =============================================================================
// FlushWorkerPool Notify — channel full path (backpressure)
// =============================================================================

func TestFlushWorkerPool_Notify_ChannelFullBackpressure(t *testing.T) {
	logger := logging.NewDevelopment()
	mockWAL := newMockPartitionedWriter()
	memStore := NewMemoryStore(time.Hour, 1000, logger)
	mockStorage := newFlushMockMainStorage()

	pool := NewFlushWorkerPool(
		FlushWorkerPoolConfig{
			MaxBatchSize:      100,
			FlushDelay:        time.Second,
			MaxPendingEntries: 10000,
			WorkerIdleTimeout: 5 * time.Minute,
			QueueSize:         1, // Very small queue to trigger backpressure
		},
		logger, mockWAL, memStore, mockStorage, nil,
	)

	// Don't start the pool — so the dispatch loop doesn't drain the notifyCh
	// This way the channel will fill up

	// Fill the global channel
	for i := 0; i < cap(pool.notifyCh); i++ {
		pool.notifyCh <- WriteNotification{
			Database:   "db1",
			Date:       time.Now(),
			Collection: "metrics",
			EntryCount: 1,
		}
	}

	// Now the pool is started so backpressure retry can work
	pool.Start()
	defer pool.Stop()

	// This notification should trigger backpressure path
	pool.Notify(WriteNotification{
		Database:   "db1",
		Date:       time.Now(),
		Collection: "metrics",
		EntryCount: 1,
	})

	time.Sleep(100 * time.Millisecond)
}
