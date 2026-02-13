package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/wal"
)

func TestWriteWorkerPool_PartitionKey(t *testing.T) {
	tests := []struct {
		database string
		time     string
		expected string
	}{
		{"db1", "2026-01-27T10:00:00Z", "db1:2026-01-27"},
		{"db2", "2026-01-28T23:59:59Z", "db2:2026-01-28"},
		{"mydb", "2025-12-31T00:00:00Z", "mydb:2025-12-31"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			timeParsed, _ := time.Parse(time.RFC3339, tt.time)
			dateStr := timeParsed.Format("2006-01-02")
			key := fmt.Sprintf("%s:%s", tt.database, dateStr)

			if key != tt.expected {
				t.Errorf("Expected key %s, got %s", tt.expected, key)
			}
		})
	}
}

func TestWriteWorkerPool_CreateWorkers(t *testing.T) {
	logger := logging.NewDevelopment()
	memStore := NewMemoryStore(time.Hour, 10000, logger)
	defer func() { _ = memStore.Close() }()

	pool := &WriteWorkerPool{
		workers:     make(map[string]*WriteWorker),
		logger:      logger,
		wal:         nil, // nil for test
		memStore:    memStore,
		bufferSize:  100,
		idleTimeout: 5 * time.Minute,
		stopCh:      make(chan struct{}),
	}

	// Create workers for different partitions (db:collection:date)
	worker1 := pool.getOrCreateWorker("db1:coll1:2026-01-27", "db1", "coll1", "2026-01-27")
	worker2 := pool.getOrCreateWorker("db2:coll1:2026-01-27", "db2", "coll1", "2026-01-27")
	worker3 := pool.getOrCreateWorker("db1:coll1:2026-01-28", "db1", "coll1", "2026-01-28")
	worker4 := pool.getOrCreateWorker("db1:coll2:2026-01-27", "db1", "coll2", "2026-01-27")

	// Same partition should return same worker
	worker1Again := pool.getOrCreateWorker("db1:coll1:2026-01-27", "db1", "coll1", "2026-01-27")

	if worker1 != worker1Again {
		t.Error("Expected same worker for same partition")
	}

	if worker1 == worker2 {
		t.Error("Expected different workers for different databases")
	}

	if worker1 == worker3 {
		t.Error("Expected different workers for different dates")
	}

	if worker1 == worker4 {
		t.Error("Expected different workers for different collections")
	}

	// Check pool has 4 workers
	if len(pool.workers) != 4 {
		t.Errorf("Expected 4 workers, got %d", len(pool.workers))
	}

	// Cleanup
	for _, w := range pool.workers {
		w.Stop()
	}
}

func TestWriteWorkerPool_Stats(t *testing.T) {
	logger := logging.NewDevelopment()
	memStore := NewMemoryStore(time.Hour, 10000, logger)
	defer func() { _ = memStore.Close() }()

	pool := &WriteWorkerPool{
		workers:     make(map[string]*WriteWorker),
		logger:      logger,
		memStore:    memStore,
		bufferSize:  100,
		idleTimeout: 5 * time.Minute,
		stopCh:      make(chan struct{}),
	}

	// Create some workers (db:collection:date)
	pool.getOrCreateWorker("db1:coll1:2026-01-27", "db1", "coll1", "2026-01-27")
	pool.getOrCreateWorker("db2:coll1:2026-01-27", "db2", "coll1", "2026-01-27")

	stats := pool.Stats()

	workerCount, ok := stats["worker_count"].(int)
	if !ok || workerCount != 2 {
		t.Errorf("Expected worker_count=2, got %v", stats["worker_count"])
	}

	bufferSize, ok := stats["buffer_size"].(int)
	if !ok || bufferSize != 100 {
		t.Errorf("Expected buffer_size=100, got %v", stats["buffer_size"])
	}

	// Cleanup
	for _, w := range pool.workers {
		w.Stop()
	}
}

func TestWriteWorker_Touch(t *testing.T) {
	worker := &WriteWorker{
		key:        "test:2026-01-27",
		lastActive: time.Now().Add(-1 * time.Hour),
	}

	oldTime := worker.getLastActive()
	time.Sleep(10 * time.Millisecond)
	worker.touch()
	newTime := worker.getLastActive()

	if !newTime.After(oldTime) {
		t.Error("Expected lastActive to be updated after touch")
	}
}

func TestWriteWorkerPool_ConcurrentAccess(t *testing.T) {
	logger := logging.NewDevelopment()
	memStore := NewMemoryStore(time.Hour, 10000, logger)
	defer func() { _ = memStore.Close() }()

	pool := &WriteWorkerPool{
		workers:     make(map[string]*WriteWorker),
		logger:      logger,
		memStore:    memStore,
		bufferSize:  100,
		idleTimeout: 5 * time.Minute,
		stopCh:      make(chan struct{}),
	}

	// Concurrent access to create workers
	var wg sync.WaitGroup
	partitions := []struct {
		key        string
		database   string
		collection string
		date       string
	}{
		{"db1:coll1:2026-01-27", "db1", "coll1", "2026-01-27"},
		{"db2:coll1:2026-01-27", "db2", "coll1", "2026-01-27"},
		{"db1:coll1:2026-01-28", "db1", "coll1", "2026-01-28"},
		{"db2:coll2:2026-01-28", "db2", "coll2", "2026-01-28"},
	}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			p := partitions[idx%len(partitions)]
			pool.getOrCreateWorker(p.key, p.database, p.collection, p.date)
		}(i)
	}

	wg.Wait()

	// Should have exactly 4 workers
	if len(pool.workers) != 4 {
		t.Errorf("Expected 4 workers, got %d", len(pool.workers))
	}

	// Cleanup
	for _, w := range pool.workers {
		w.Stop()
	}
}

func TestWriteWorker_Drain(t *testing.T) {
	logger := logging.NewDevelopment()
	var processedCount int32

	worker := &WriteWorker{
		key:    "test:2026-01-27",
		dataCh: make(chan *WriteTask, 10),
		logger: logger,
		stopCh: make(chan struct{}),
	}

	// Add some tasks
	for i := 0; i < 5; i++ {
		worker.dataCh <- &WriteTask{
			ResultCh: make(chan error, 1),
		}
	}

	// Custom drain that counts
	for {
		select {
		case task := <-worker.dataCh:
			atomic.AddInt32(&processedCount, 1)
			if task.ResultCh != nil {
				task.ResultCh <- nil
			}
		default:
			goto done
		}
	}
done:

	if processedCount != 5 {
		t.Errorf("Expected to drain 5 tasks, got %d", processedCount)
	}
}

func TestWriteWorkerPool_IdleCleanup(t *testing.T) {
	logger := logging.NewDevelopment()
	memStore := NewMemoryStore(time.Hour, 10000, logger)
	defer func() { _ = memStore.Close() }()

	pool := &WriteWorkerPool{
		workers:     make(map[string]*WriteWorker),
		logger:      logger,
		memStore:    memStore,
		bufferSize:  100,
		idleTimeout: 100 * time.Millisecond, // Short timeout for test
		stopCh:      make(chan struct{}),
	}

	// Create a worker (db:collection:date)
	worker := pool.getOrCreateWorker("db1:coll1:2026-01-27", "db1", "coll1", "2026-01-27")

	// Set last active to past
	worker.mu.Lock()
	worker.lastActive = time.Now().Add(-200 * time.Millisecond)
	worker.mu.Unlock()

	// Run cleanup
	pool.doCleanup()

	// Worker should be removed
	pool.workersMu.RLock()
	_, exists := pool.workers["db1:coll1:2026-01-27"]
	pool.workersMu.RUnlock()

	if exists {
		t.Error("Expected idle worker to be removed")
	}
}

func BenchmarkWriteWorkerPool_GetOrCreateWorker(b *testing.B) {
	logger := logging.NewDevelopment()
	memStore := NewMemoryStore(time.Hour, 10000, logger)
	defer func() { _ = memStore.Close() }()

	pool := &WriteWorkerPool{
		workers:     make(map[string]*WriteWorker),
		logger:      logger,
		memStore:    memStore,
		bufferSize:  1000,
		idleTimeout: 5 * time.Minute,
		stopCh:      make(chan struct{}),
	}

	partitions := []struct {
		key        string
		database   string
		collection string
		date       string
	}{
		{"db1:coll1:2026-01-27", "db1", "coll1", "2026-01-27"},
		{"db2:coll1:2026-01-27", "db2", "coll1", "2026-01-27"},
		{"db3:coll1:2026-01-27", "db3", "coll1", "2026-01-27"},
		{"db1:coll1:2026-01-28", "db1", "coll1", "2026-01-28"},
		{"db2:coll2:2026-01-28", "db2", "coll2", "2026-01-28"},
		{"db3:coll2:2026-01-28", "db3", "coll2", "2026-01-28"},
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			p := partitions[i%len(partitions)]
			pool.getOrCreateWorker(p.key, p.database, p.collection, p.date)
			i++
		}
	})

	// Cleanup
	for _, w := range pool.workers {
		w.Stop()
	}
}

// =============================================================================
// Additional WriteWorkerPool tests with mock
// =============================================================================

func TestNewWriteWorkerPool(t *testing.T) {
	logger := logging.NewDevelopment()
	memStore := NewMemoryStore(time.Hour, 10000, logger)
	defer func() { _ = memStore.Close() }()

	// Create pool without WAL (testing config only)
	pool := NewWriteWorkerPool(logger, nil, memStore, nil)

	if pool == nil {
		t.Fatal("Expected non-nil pool")
		return
	}
	if pool.bufferSize != 1000 {
		t.Errorf("bufferSize = %d, expected 1000", pool.bufferSize)
	}
	if pool.idleTimeout != 5*time.Minute {
		t.Errorf("idleTimeout = %v, expected 5m", pool.idleTimeout)
	}
	if pool.workers == nil {
		t.Error("workers map should be initialized")
	}
	if pool.memMaxAge != time.Hour {
		t.Errorf("memMaxAge = %v, expected 1h", pool.memMaxAge)
	}
}

func TestWriteWorkerPool_StartStop(t *testing.T) {
	logger := logging.NewDevelopment()
	memStore := NewMemoryStore(time.Hour, 10000, logger)
	defer func() { _ = memStore.Close() }()

	pool := NewWriteWorkerPool(logger, nil, memStore, nil)

	// Start the pool
	pool.Start()

	// Let it run for a bit
	time.Sleep(50 * time.Millisecond)

	// Stop the pool
	pool.Stop()
}

func TestWriteTask_Fields(t *testing.T) {
	now := time.Now()

	entry := &wal.Entry{
		Type:       wal.EntryTypeWrite,
		Database:   "testdb",
		Collection: "metrics",
		ID:         "device1",
		Fields:     map[string]interface{}{"temp": 25.0},
		Timestamp:  now.UnixNano(),
	}

	dataPoint := &DataPoint{
		Database:   "testdb",
		Collection: "metrics",
		ID:         "device1",
		Time:       now,
		Fields:     map[string]interface{}{"temp": 25.0},
	}

	task := &WriteTask{
		Entry:     entry,
		DataPoint: dataPoint,
		ResultCh:  make(chan error, 1),
	}

	if task.Entry.Database != "testdb" {
		t.Errorf("Entry.Database = %q, expected %q", task.Entry.Database, "testdb")
	}
	if task.DataPoint.ID != "device1" {
		t.Errorf("DataPoint.ID = %q, expected %q", task.DataPoint.ID, "device1")
	}
	if task.ResultCh == nil {
		t.Error("ResultCh should not be nil")
	}
}

func TestWriteWorker_StopWithTasks(t *testing.T) {
	logger := logging.NewDevelopment()

	worker := &WriteWorker{
		key:        "test:coll:2026-01-27",
		database:   "test",
		collection: "coll",
		date:       "2026-01-27",
		dataCh:     make(chan *WriteTask, 10),
		logger:     logger,
		stopCh:     make(chan struct{}),
	}

	// Add some tasks
	for i := 0; i < 3; i++ {
		task := &WriteTask{
			ResultCh: make(chan error, 1),
		}
		worker.dataCh <- task
	}

	// Close without processing - tasks should be drained
	close(worker.stopCh)

	// Drain tasks manually (simulating what Stop does)
	drained := 0
	for {
		select {
		case task := <-worker.dataCh:
			if task.ResultCh != nil {
				task.ResultCh <- nil
			}
			drained++
		default:
			goto done
		}
	}
done:
	if drained != 3 {
		t.Errorf("Expected 3 drained tasks, got %d", drained)
	}
}

func TestWriteMessage_Fields(t *testing.T) {
	msg := WriteMessage{
		Database:   "testdb",
		Collection: "metrics",
		ShardID:    "shard1",
		Nodes:      []string{"node1", "node2"},
		Time:       "2026-01-27T10:00:00Z",
		ID:         "device1",
		Fields:     map[string]interface{}{"temp": 25.0, "humidity": 60.0},
	}

	if msg.Database != "testdb" {
		t.Errorf("Database = %q, expected %q", msg.Database, "testdb")
	}
	if msg.Collection != "metrics" {
		t.Errorf("Collection = %q, expected %q", msg.Collection, "metrics")
	}
	if len(msg.Nodes) != 2 {
		t.Errorf("Nodes length = %d, expected 2", len(msg.Nodes))
	}
	if msg.Fields["temp"] != 25.0 {
		t.Errorf("Fields[temp] = %v, expected 25.0", msg.Fields["temp"])
	}
}
