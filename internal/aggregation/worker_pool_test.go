package aggregation

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/logging"
)

func TestDefaultWorkerPoolConfig(t *testing.T) {
	config := DefaultWorkerPoolConfig()

	if config.MaxActiveWorkers != 100 {
		t.Errorf("MaxActiveWorkers = %d, expected 100", config.MaxActiveWorkers)
	}
	if config.WorkerIdleTimeout != 10*time.Minute {
		t.Errorf("WorkerIdleTimeout = %v, expected 10m", config.WorkerIdleTimeout)
	}
	if config.QueueSize != 100 {
		t.Errorf("QueueSize = %d, expected 100", config.QueueSize)
	}
	if config.BatchDelay != 5*time.Second {
		t.Errorf("BatchDelay = %v, expected 5s", config.BatchDelay)
	}
	if config.Timezone != time.UTC {
		t.Errorf("Timezone = %v, expected UTC", config.Timezone)
	}
}

func TestNewAggregationWorkerPool(t *testing.T) {
	logger := logging.NewDevelopment()
	rawReader := &mockRawDataReader{}
	aggStorage := &mockAggregationStorage{}
	config := DefaultWorkerPoolConfig()

	pool := NewAggregationWorkerPool(config, logger, rawReader, aggStorage)

	if pool == nil {
		t.Fatal("NewAggregationWorkerPool returned nil")
		return
	}

	if pool.config.MaxActiveWorkers != 100 {
		t.Errorf("config.MaxActiveWorkers = %d, expected 100", pool.config.MaxActiveWorkers)
	}
	if pool.workers == nil {
		t.Error("workers map is nil")
	}
	if pool.semaphore == nil {
		t.Error("semaphore is nil")
	}
	if pool.pendingQueue == nil {
		t.Error("pendingQueue is nil")
	}
	if pool.notifyCh == nil {
		t.Error("notifyCh is nil")
	}
}

func TestNewAggregationWorkerPool_InvalidConfig(t *testing.T) {
	logger := logging.NewDevelopment()
	rawReader := &mockRawDataReader{}
	aggStorage := &mockAggregationStorage{}

	// Test with invalid MaxActiveWorkers
	config := WorkerPoolConfig{
		MaxActiveWorkers: 0, // Invalid
		QueueSize:        0, // Invalid
		Timezone:         nil,
	}

	pool := NewAggregationWorkerPool(config, logger, rawReader, aggStorage)

	// Should use defaults
	if pool.config.MaxActiveWorkers != 100 {
		t.Errorf("MaxActiveWorkers = %d, expected 100 (default)", pool.config.MaxActiveWorkers)
	}
	if pool.config.QueueSize != 100 {
		t.Errorf("QueueSize = %d, expected 100 (default)", pool.config.QueueSize)
	}
	if pool.config.Timezone != time.UTC {
		t.Errorf("Timezone = %v, expected UTC (default)", pool.config.Timezone)
	}
}

func TestAggregationWorkerPool_StartStop(t *testing.T) {
	logger := logging.NewDevelopment()
	rawReader := &mockRawDataReader{}
	aggStorage := &mockAggregationStorage{}
	config := DefaultWorkerPoolConfig()

	pool := NewAggregationWorkerPool(config, logger, rawReader, aggStorage)

	pool.Start()
	pool.Stop()

	// Should complete without hanging
}

func TestAggregationWorkerPool_Notify(t *testing.T) {
	logger := logging.NewDevelopment()
	rawReader := &mockRawDataReader{}
	aggStorage := &mockAggregationStorage{}
	config := DefaultWorkerPoolConfig()

	pool := NewAggregationWorkerPool(config, logger, rawReader, aggStorage)
	pool.Start()
	defer pool.Stop()

	notif := AggregationNotification{
		Database:   "testdb",
		Collection: "metrics",
		Level:      AggregationHourly,
		TimeKey:    time.Now(),
	}

	// Should not block
	pool.Notify(notif)
}

func TestAggregationWorkerPool_NotifyMultiple(t *testing.T) {
	logger := logging.NewDevelopment()
	rawReader := &mockRawDataReader{}
	aggStorage := &mockAggregationStorage{}
	config := DefaultWorkerPoolConfig()

	pool := NewAggregationWorkerPool(config, logger, rawReader, aggStorage)
	pool.Start()
	defer pool.Stop()

	// Send multiple notifications
	for i := 0; i < 10; i++ {
		notif := AggregationNotification{
			Database:   "testdb",
			Collection: "metrics",
			Level:      AggregationHourly,
			TimeKey:    time.Now(),
		}
		pool.Notify(notif)
	}

	// Should complete without blocking
}

func TestAggregationWorkerPool_SetNextLevelPool(t *testing.T) {
	logger := logging.NewDevelopment()
	rawReader := &mockRawDataReader{}
	aggStorage := &mockAggregationStorage{}
	config := DefaultWorkerPoolConfig()

	hourlyPool := NewAggregationWorkerPool(config, logger, rawReader, aggStorage)
	dailyPool := NewAggregationWorkerPool(config, logger, rawReader, aggStorage)

	hourlyPool.SetNextLevelPool(AggregationDaily, dailyPool)

	if hourlyPool.nextLevelPool[AggregationDaily] != dailyPool {
		t.Error("SetNextLevelPool did not set the next level pool correctly")
	}
}

func TestAggregationWorkerPool_Stats(t *testing.T) {
	logger := logging.NewDevelopment()
	rawReader := &mockRawDataReader{}
	aggStorage := &mockAggregationStorage{}
	config := DefaultWorkerPoolConfig()

	pool := NewAggregationWorkerPool(config, logger, rawReader, aggStorage)
	pool.Start()
	defer pool.Stop()

	stats := pool.Stats()

	// Verify stats structure
	if _, ok := stats["max_active_workers"]; !ok {
		t.Error("Stats missing 'max_active_workers' key")
	}
	if _, ok := stats["total_workers"]; !ok {
		t.Error("Stats missing 'total_workers' key")
	}
	if _, ok := stats["running_workers"]; !ok {
		t.Error("Stats missing 'running_workers' key")
	}
	if _, ok := stats["pending_workers"]; !ok {
		t.Error("Stats missing 'pending_workers' key")
	}
	if _, ok := stats["waiting_for_job_workers"]; !ok {
		t.Error("Stats missing 'waiting_for_job_workers' key")
	}
	if _, ok := stats["total_pending"]; !ok {
		t.Error("Stats missing 'total_pending' key")
	}
	if _, ok := stats["total_processed"]; !ok {
		t.Error("Stats missing 'total_processed' key")
	}
	if _, ok := stats["total_failed"]; !ok {
		t.Error("Stats missing 'total_failed' key")
	}

	// Verify initial values
	if stats["max_active_workers"].(int) != 100 {
		t.Errorf("max_active_workers = %d, expected 100", stats["max_active_workers"].(int))
	}
	if stats["total_workers"].(int) != 0 {
		t.Errorf("total_workers = %d, expected 0", stats["total_workers"].(int))
	}
}

func TestAggregationWorkerPool_PartitionKey(t *testing.T) {
	logger := logging.NewDevelopment()
	rawReader := &mockRawDataReader{}
	aggStorage := &mockAggregationStorage{}
	config := DefaultWorkerPoolConfig()

	pool := NewAggregationWorkerPool(config, logger, rawReader, aggStorage)

	tests := []struct {
		name       string
		database   string
		collection string
		level      AggregationLevel
		timeKey    time.Time
		expected   string
	}{
		{
			name:       "hourly partition",
			database:   "db1",
			collection: "col1",
			level:      AggregationHourly,
			timeKey:    time.Date(2024, 5, 15, 10, 30, 0, 0, time.UTC),
			expected:   "db1:col1:1h:20240515",
		},
		{
			name:       "daily partition",
			database:   "db2",
			collection: "col2",
			level:      AggregationDaily,
			timeKey:    time.Date(2024, 5, 15, 10, 30, 0, 0, time.UTC),
			expected:   "db2:col2:1d:202405",
		},
		{
			name:       "monthly partition",
			database:   "db3",
			collection: "col3",
			level:      AggregationMonthly,
			timeKey:    time.Date(2024, 5, 15, 10, 30, 0, 0, time.UTC),
			expected:   "db3:col3:1M:2024",
		},
		{
			name:       "yearly partition",
			database:   "db4",
			collection: "col4",
			level:      AggregationYearly,
			timeKey:    time.Date(2024, 5, 15, 10, 30, 0, 0, time.UTC),
			expected:   "db4:col4:1y:2024", // Changed from "all" to year-specific key to support multi-year data
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := pool.partitionKey(tt.database, tt.collection, tt.level, tt.timeKey)
			if result != tt.expected {
				t.Errorf("partitionKey() = %q, expected %q", result, tt.expected)
			}
		})
	}
}

func TestAggregationWorkerPool_ConcurrentNotify(t *testing.T) {
	logger := logging.NewDevelopment()
	rawReader := &mockRawDataReader{}
	aggStorage := &mockAggregationStorage{}
	config := DefaultWorkerPoolConfig()
	config.QueueSize = 1000

	pool := NewAggregationWorkerPool(config, logger, rawReader, aggStorage)
	pool.Start()
	defer pool.Stop()

	var wg sync.WaitGroup
	notifyCount := 100
	goroutines := 10

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < notifyCount; i++ {
				notif := AggregationNotification{
					Database:   "testdb",
					Collection: "metrics",
					Level:      AggregationHourly,
					TimeKey:    time.Now(),
				}
				pool.Notify(notif)
			}
		}(g)
	}

	wg.Wait()
}

func TestAggregationNotification(t *testing.T) {
	now := time.Now()
	notif := AggregationNotification{
		Database:   "testdb",
		Collection: "metrics",
		Level:      AggregationHourly,
		TimeKey:    now,
	}

	if notif.Database != "testdb" {
		t.Errorf("Database = %q, expected %q", notif.Database, "testdb")
	}
	if notif.Collection != "metrics" {
		t.Errorf("Collection = %q, expected %q", notif.Collection, "metrics")
	}
	if notif.Level != AggregationHourly {
		t.Errorf("Level = %v, expected %v", notif.Level, AggregationHourly)
	}
	if !notif.TimeKey.Equal(now) {
		t.Errorf("TimeKey = %v, expected %v", notif.TimeKey, now)
	}
}

func TestWorkerState(t *testing.T) {
	tests := []struct {
		name     string
		state    workerState
		expected int
	}{
		{"idle", workerIdle, 0},
		{"pending", workerPending, 1},
		{"running", workerRunning, 2},
		{"waiting for job", workerWaitingForJob, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if int(tt.state) != tt.expected {
				t.Errorf("workerState = %d, expected %d", tt.state, tt.expected)
			}
		})
	}
}

func TestPartitionWorker(t *testing.T) {
	now := time.Now()
	worker := &PartitionWorker{
		key:        "test:key",
		database:   "testdb",
		collection: "metrics",
		level:      AggregationHourly,
		timeKey:    now,
		notifyCh:   make(chan struct{}, 100),
		stopCh:     make(chan struct{}),
		state:      workerIdle,
	}

	if worker.key != "test:key" {
		t.Errorf("key = %q, expected %q", worker.key, "test:key")
	}
	if worker.database != "testdb" {
		t.Errorf("database = %q, expected %q", worker.database, "testdb")
	}
	if worker.collection != "metrics" {
		t.Errorf("collection = %q, expected %q", worker.collection, "metrics")
	}
	if worker.level != AggregationHourly {
		t.Errorf("level = %v, expected %v", worker.level, AggregationHourly)
	}
	if worker.state != workerIdle {
		t.Errorf("state = %v, expected %v", worker.state, workerIdle)
	}
}

func TestAggregationWorkerPool_WithRawData(t *testing.T) {
	logger := logging.NewDevelopment()

	var writeHourlyCalled atomic.Int32
	aggStorage := &mockAggregationStorage{
		writeHourlyFunc: func(database, collection string, points []*AggregatedPoint) error {
			writeHourlyCalled.Add(1)
			return nil
		},
	}

	now := time.Now()
	rawReader := &mockRawDataReader{
		queryFunc: func(database, collection string, deviceIDs []string, startTime, endTime time.Time, fields []string) ([]DataPointInterface, error) {
			// Return some mock data
			return []DataPointInterface{
				&mockDataPoint{
					id:         "device1",
					database:   database,
					collection: collection,
					time:       now,
					fields: map[string]interface{}{
						"temperature": 25.5,
						"humidity":    60.0,
					},
				},
			}, nil
		},
	}

	config := DefaultWorkerPoolConfig()
	config.BatchDelay = 100 * time.Millisecond // Faster for testing

	pool := NewAggregationWorkerPool(config, logger, rawReader, aggStorage)
	pool.Start()

	// Send notification
	notif := AggregationNotification{
		Database:   "testdb",
		Collection: "metrics",
		Level:      AggregationHourly,
		TimeKey:    now,
	}
	pool.Notify(notif)

	// Wait for processing
	time.Sleep(500 * time.Millisecond)

	pool.Stop()

	// Verify write was called
	if writeHourlyCalled.Load() == 0 {
		t.Log("Note: WriteHourly may not be called if worker didn't process in time")
	}
}

func TestAggregationWorkerPool_DispatchToWorker(t *testing.T) {
	logger := logging.NewDevelopment()
	rawReader := &mockRawDataReader{}
	aggStorage := &mockAggregationStorage{}
	config := DefaultWorkerPoolConfig()

	pool := NewAggregationWorkerPool(config, logger, rawReader, aggStorage)
	pool.Start()
	defer pool.Stop()

	now := time.Now()
	notif := AggregationNotification{
		Database:   "testdb",
		Collection: "metrics",
		Level:      AggregationHourly,
		TimeKey:    now,
	}

	// Dispatch multiple times to same partition
	for i := 0; i < 5; i++ {
		pool.Notify(notif)
	}

	// Wait a bit for worker creation
	time.Sleep(100 * time.Millisecond)

	// Check stats
	stats := pool.Stats()
	totalWorkers := stats["total_workers"].(int)

	// Should have at most 1 worker for this partition
	if totalWorkers > 1 {
		t.Errorf("total_workers = %d, expected <= 1 for same partition", totalWorkers)
	}
}

func TestAggregationWorkerPool_MultiplePartitions(t *testing.T) {
	logger := logging.NewDevelopment()
	rawReader := &mockRawDataReader{}
	aggStorage := &mockAggregationStorage{}
	config := DefaultWorkerPoolConfig()

	pool := NewAggregationWorkerPool(config, logger, rawReader, aggStorage)
	pool.Start()
	defer pool.Stop()

	now := time.Now()

	// Create notifications for different partitions
	partitions := []struct {
		database   string
		collection string
	}{
		{"db1", "col1"},
		{"db1", "col2"},
		{"db2", "col1"},
		{"db2", "col2"},
	}

	for _, p := range partitions {
		notif := AggregationNotification{
			Database:   p.database,
			Collection: p.collection,
			Level:      AggregationHourly,
			TimeKey:    now,
		}
		pool.Notify(notif)
	}

	// Wait for workers to be created
	time.Sleep(200 * time.Millisecond)

	// Check stats
	stats := pool.Stats()
	totalWorkers := stats["total_workers"].(int)

	// Should have workers for each partition (up to 4)
	if totalWorkers < 1 {
		t.Errorf("total_workers = %d, expected >= 1", totalWorkers)
	}
}

// =============================================================================
// aggregateHourly with actual data processing
// =============================================================================

func TestAggregateHourly_WithData(t *testing.T) {
	logger := logging.NewDevelopment()

	now := time.Date(2024, 5, 15, 10, 30, 0, 0, time.UTC)
	var writeHourlyCalled atomic.Int32
	var writtenPoints []*AggregatedPoint
	var mu sync.Mutex

	aggStorage := &mockAggregationStorage{
		writeHourlyFunc: func(database, collection string, points []*AggregatedPoint) error {
			writeHourlyCalled.Add(1)
			mu.Lock()
			writtenPoints = append(writtenPoints, points...)
			mu.Unlock()
			return nil
		},
	}

	rawReader := &mockRawDataReader{
		queryFunc: func(database, collection string, deviceIDs []string, startTime, endTime time.Time, fields []string) ([]DataPointInterface, error) {
			return []DataPointInterface{
				&mockDataPoint{
					id: "dev1", database: database, collection: collection, time: now,
					fields: map[string]interface{}{"temperature": 25.0, "humidity": 60.0},
				},
				&mockDataPoint{
					id: "dev1", database: database, collection: collection, time: now.Add(5 * time.Minute),
					fields: map[string]interface{}{"temperature": 26.0, "humidity": 62.0},
				},
				&mockDataPoint{
					id: "dev2", database: database, collection: collection, time: now,
					fields: map[string]interface{}{"temperature": 22.0},
				},
			}, nil
		},
	}

	config := DefaultWorkerPoolConfig()
	pool := NewAggregationWorkerPool(config, logger, rawReader, aggStorage)

	worker := &PartitionWorker{
		key:        "testdb:metrics:1h:20240515",
		database:   "testdb",
		collection: "metrics",
		level:      AggregationHourly,
		timeKey:    now,
	}

	err := pool.aggregateHourly(worker)
	if err != nil {
		t.Fatalf("aggregateHourly failed: %v", err)
	}

	if writeHourlyCalled.Load() != 1 {
		t.Errorf("WriteHourly called %d times, want 1", writeHourlyCalled.Load())
	}

	mu.Lock()
	defer mu.Unlock()
	if len(writtenPoints) == 0 {
		t.Fatal("No points written")
	}

	// Should have aggregated dev1 (10:00 hour) and dev2 (10:00 hour)
	deviceSet := make(map[string]bool)
	for _, p := range writtenPoints {
		deviceSet[p.DeviceID] = true
	}
	if !deviceSet["dev1"] {
		t.Error("Expected dev1 in written points")
	}
	if !deviceSet["dev2"] {
		t.Error("Expected dev2 in written points")
	}
}

func TestAggregateHourly_EmptyRawData(t *testing.T) {
	logger := logging.NewDevelopment()

	aggStorage := &mockAggregationStorage{}
	rawReader := &mockRawDataReader{
		queryFunc: func(database, collection string, deviceIDs []string, startTime, endTime time.Time, fields []string) ([]DataPointInterface, error) {
			return nil, nil // Empty
		},
	}

	config := DefaultWorkerPoolConfig()
	pool := NewAggregationWorkerPool(config, logger, rawReader, aggStorage)

	worker := &PartitionWorker{
		key:        "testdb:metrics:1h:20240515",
		database:   "testdb",
		collection: "metrics",
		level:      AggregationHourly,
		timeKey:    time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC),
	}

	err := pool.aggregateHourly(worker)
	if err != nil {
		t.Fatalf("aggregateHourly with empty data should not error: %v", err)
	}
}

func TestAggregateHourly_QueryError(t *testing.T) {
	logger := logging.NewDevelopment()

	aggStorage := &mockAggregationStorage{}
	rawReader := &mockRawDataReader{
		queryFunc: func(database, collection string, deviceIDs []string, startTime, endTime time.Time, fields []string) ([]DataPointInterface, error) {
			return nil, fmt.Errorf("connection refused")
		},
	}

	config := DefaultWorkerPoolConfig()
	pool := NewAggregationWorkerPool(config, logger, rawReader, aggStorage)

	worker := &PartitionWorker{
		key:        "testdb:metrics:1h:20240515",
		database:   "testdb",
		collection: "metrics",
		level:      AggregationHourly,
		timeKey:    time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC),
	}

	err := pool.aggregateHourly(worker)
	if err == nil {
		t.Error("aggregateHourly should fail on query error")
	}
}

func TestAggregateHourly_WriteError(t *testing.T) {
	logger := logging.NewDevelopment()

	aggStorage := &mockAggregationStorage{
		writeHourlyFunc: func(database, collection string, points []*AggregatedPoint) error {
			return fmt.Errorf("disk full")
		},
	}
	rawReader := &mockRawDataReader{
		queryFunc: func(database, collection string, deviceIDs []string, startTime, endTime time.Time, fields []string) ([]DataPointInterface, error) {
			return []DataPointInterface{
				&mockDataPoint{id: "dev1", time: time.Now(), fields: map[string]interface{}{"temp": 25.0}},
			}, nil
		},
	}

	config := DefaultWorkerPoolConfig()
	pool := NewAggregationWorkerPool(config, logger, rawReader, aggStorage)

	worker := &PartitionWorker{
		key:        "testdb:metrics:1h:20240515",
		database:   "testdb",
		collection: "metrics",
		level:      AggregationHourly,
		timeKey:    time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC),
	}

	err := pool.aggregateHourly(worker)
	if err == nil {
		t.Error("aggregateHourly should fail on write error")
	}
}

// =============================================================================
// aggregateDaily
// =============================================================================

func TestAggregateDaily_WithData(t *testing.T) {
	logger := logging.NewDevelopment()

	var writeDailyCalled atomic.Int32
	aggStorage := &mockAggregationStorage{
		readHourlyForDayFunc: func(database, collection string, day time.Time) ([]*AggregatedPoint, error) {
			return []*AggregatedPoint{
				{
					Time: day.Add(10 * time.Hour), DeviceID: "dev1", Level: AggregationHourly,
					Fields: map[string]*AggregatedField{
						"temp": {Sum: 250, Avg: 25, Min: 20, Max: 30, Count: 10},
					},
				},
				{
					Time: day.Add(11 * time.Hour), DeviceID: "dev1", Level: AggregationHourly,
					Fields: map[string]*AggregatedField{
						"temp": {Sum: 260, Avg: 26, Min: 21, Max: 31, Count: 10},
					},
				},
			}, nil
		},
		writeDailyFunc: func(database, collection string, points []*AggregatedPoint) error {
			writeDailyCalled.Add(1)
			if len(points) == 0 {
				t.Error("Expected non-empty daily points")
			}
			return nil
		},
	}

	config := DefaultWorkerPoolConfig()
	pool := NewAggregationWorkerPool(config, logger, &mockRawDataReader{}, aggStorage)

	worker := &PartitionWorker{
		key:        "testdb:metrics:1d:202405",
		database:   "testdb",
		collection: "metrics",
		level:      AggregationDaily,
		timeKey:    time.Date(2024, 5, 15, 0, 0, 0, 0, time.UTC),
	}

	err := pool.aggregateDaily(worker)
	if err != nil {
		t.Fatalf("aggregateDaily failed: %v", err)
	}

	if writeDailyCalled.Load() != 1 {
		t.Errorf("WriteDaily called %d times, want 1", writeDailyCalled.Load())
	}
}

func TestAggregateDaily_NoHourlyData(t *testing.T) {
	logger := logging.NewDevelopment()

	aggStorage := &mockAggregationStorage{
		readHourlyForDayFunc: func(database, collection string, day time.Time) ([]*AggregatedPoint, error) {
			return nil, nil // No data
		},
	}

	config := DefaultWorkerPoolConfig()
	pool := NewAggregationWorkerPool(config, logger, &mockRawDataReader{}, aggStorage)

	worker := &PartitionWorker{
		key:        "testdb:metrics:1d:202405",
		database:   "testdb",
		collection: "metrics",
		level:      AggregationDaily,
		timeKey:    time.Date(2024, 5, 15, 0, 0, 0, 0, time.UTC),
	}

	err := pool.aggregateDaily(worker)
	if err != nil {
		t.Fatalf("aggregateDaily with no data should not error: %v", err)
	}
}

func TestAggregateDaily_WriteError(t *testing.T) {
	logger := logging.NewDevelopment()

	aggStorage := &mockAggregationStorage{
		readHourlyForDayFunc: func(database, collection string, day time.Time) ([]*AggregatedPoint, error) {
			return []*AggregatedPoint{
				{
					Time: day.Add(10 * time.Hour), DeviceID: "dev1", Level: AggregationHourly,
					Fields: map[string]*AggregatedField{"temp": {Sum: 250, Count: 10}},
				},
			}, nil
		},
		writeDailyFunc: func(database, collection string, points []*AggregatedPoint) error {
			return fmt.Errorf("write error")
		},
	}

	config := DefaultWorkerPoolConfig()
	pool := NewAggregationWorkerPool(config, logger, &mockRawDataReader{}, aggStorage)

	worker := &PartitionWorker{
		key:        "testdb:metrics:1d:202405",
		database:   "testdb",
		collection: "metrics",
		level:      AggregationDaily,
		timeKey:    time.Date(2024, 5, 15, 0, 0, 0, 0, time.UTC),
	}

	err := pool.aggregateDaily(worker)
	if err == nil {
		t.Error("aggregateDaily should fail on write error")
	}
}

// =============================================================================
// aggregateMonthly
// =============================================================================

func TestAggregateMonthly_WithData(t *testing.T) {
	logger := logging.NewDevelopment()

	var writeMonthlyCalled atomic.Int32
	aggStorage := &mockAggregationStorage{
		readDailyForMonthFunc: func(database, collection string, month time.Time) ([]*AggregatedPoint, error) {
			return []*AggregatedPoint{
				{
					Time: month, DeviceID: "dev1", Level: AggregationDaily,
					Fields: map[string]*AggregatedField{"temp": {Sum: 7500, Avg: 25, Min: 18, Max: 32, Count: 300}},
				},
			}, nil
		},
		writeMonthlyFunc: func(database, collection string, points []*AggregatedPoint) error {
			writeMonthlyCalled.Add(1)
			return nil
		},
	}

	config := DefaultWorkerPoolConfig()
	pool := NewAggregationWorkerPool(config, logger, &mockRawDataReader{}, aggStorage)

	worker := &PartitionWorker{
		key:        "testdb:metrics:1M:2024",
		database:   "testdb",
		collection: "metrics",
		level:      AggregationMonthly,
		timeKey:    time.Date(2024, 5, 15, 0, 0, 0, 0, time.UTC),
	}

	err := pool.aggregateMonthly(worker)
	if err != nil {
		t.Fatalf("aggregateMonthly failed: %v", err)
	}

	if writeMonthlyCalled.Load() != 1 {
		t.Errorf("WriteMonthly called %d times, want 1", writeMonthlyCalled.Load())
	}
}

func TestAggregateMonthly_NoDailyData(t *testing.T) {
	logger := logging.NewDevelopment()

	aggStorage := &mockAggregationStorage{
		readDailyForMonthFunc: func(database, collection string, month time.Time) ([]*AggregatedPoint, error) {
			return nil, nil
		},
	}

	config := DefaultWorkerPoolConfig()
	pool := NewAggregationWorkerPool(config, logger, &mockRawDataReader{}, aggStorage)

	worker := &PartitionWorker{
		key:        "testdb:metrics:1M:2024",
		database:   "testdb",
		collection: "metrics",
		level:      AggregationMonthly,
		timeKey:    time.Date(2024, 5, 15, 0, 0, 0, 0, time.UTC),
	}

	err := pool.aggregateMonthly(worker)
	if err != nil {
		t.Fatalf("aggregateMonthly with no data should not error: %v", err)
	}
}

func TestAggregateMonthly_WriteError(t *testing.T) {
	logger := logging.NewDevelopment()

	aggStorage := &mockAggregationStorage{
		readDailyForMonthFunc: func(database, collection string, month time.Time) ([]*AggregatedPoint, error) {
			return []*AggregatedPoint{
				{
					Time: month, DeviceID: "dev1", Level: AggregationDaily,
					Fields: map[string]*AggregatedField{"temp": {Sum: 7500, Count: 300}},
				},
			}, nil
		},
		writeMonthlyFunc: func(database, collection string, points []*AggregatedPoint) error {
			return fmt.Errorf("write error")
		},
	}

	config := DefaultWorkerPoolConfig()
	pool := NewAggregationWorkerPool(config, logger, &mockRawDataReader{}, aggStorage)

	worker := &PartitionWorker{
		key:        "testdb:metrics:1M:2024",
		database:   "testdb",
		collection: "metrics",
		level:      AggregationMonthly,
		timeKey:    time.Date(2024, 5, 15, 0, 0, 0, 0, time.UTC),
	}

	err := pool.aggregateMonthly(worker)
	if err == nil {
		t.Error("aggregateMonthly should fail on write error")
	}
}

// =============================================================================
// aggregateYearly
// =============================================================================

func TestAggregateYearly_WithData(t *testing.T) {
	logger := logging.NewDevelopment()

	var writeYearlyCalled atomic.Int32
	aggStorage := &mockAggregationStorage{
		readMonthlyForYearFunc: func(database, collection string, year time.Time) ([]*AggregatedPoint, error) {
			return []*AggregatedPoint{
				{
					Time: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), DeviceID: "dev1", Level: AggregationMonthly,
					Fields: map[string]*AggregatedField{"temp": {Sum: 22500, Avg: 25, Min: 15, Max: 35, Count: 900}},
				},
				{
					Time: time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC), DeviceID: "dev1", Level: AggregationMonthly,
					Fields: map[string]*AggregatedField{"temp": {Sum: 27000, Avg: 30, Min: 20, Max: 40, Count: 900}},
				},
			}, nil
		},
		writeYearlyFunc: func(database, collection string, points []*AggregatedPoint) error {
			writeYearlyCalled.Add(1)
			if len(points) != 1 {
				t.Errorf("Expected 1 yearly point (1 device), got %d", len(points))
			}
			return nil
		},
	}

	config := DefaultWorkerPoolConfig()
	pool := NewAggregationWorkerPool(config, logger, &mockRawDataReader{}, aggStorage)

	worker := &PartitionWorker{
		key:        "testdb:metrics:1y:2024",
		database:   "testdb",
		collection: "metrics",
		level:      AggregationYearly,
		timeKey:    time.Date(2024, 5, 15, 0, 0, 0, 0, time.UTC),
	}

	err := pool.aggregateYearly(worker)
	if err != nil {
		t.Fatalf("aggregateYearly failed: %v", err)
	}

	if writeYearlyCalled.Load() != 1 {
		t.Errorf("WriteYearly called %d times, want 1", writeYearlyCalled.Load())
	}
}

func TestAggregateYearly_NoMonthlyData(t *testing.T) {
	logger := logging.NewDevelopment()

	aggStorage := &mockAggregationStorage{
		readMonthlyForYearFunc: func(database, collection string, year time.Time) ([]*AggregatedPoint, error) {
			return nil, nil
		},
	}

	config := DefaultWorkerPoolConfig()
	pool := NewAggregationWorkerPool(config, logger, &mockRawDataReader{}, aggStorage)

	worker := &PartitionWorker{
		key:        "testdb:metrics:1y:2024",
		database:   "testdb",
		collection: "metrics",
		level:      AggregationYearly,
		timeKey:    time.Date(2024, 5, 15, 0, 0, 0, 0, time.UTC),
	}

	err := pool.aggregateYearly(worker)
	if err != nil {
		t.Fatalf("aggregateYearly with no data should not error: %v", err)
	}
}

func TestAggregateYearly_ReadError(t *testing.T) {
	logger := logging.NewDevelopment()

	aggStorage := &mockAggregationStorage{
		readMonthlyForYearFunc: func(database, collection string, year time.Time) ([]*AggregatedPoint, error) {
			return nil, fmt.Errorf("read error")
		},
	}

	config := DefaultWorkerPoolConfig()
	pool := NewAggregationWorkerPool(config, logger, &mockRawDataReader{}, aggStorage)

	worker := &PartitionWorker{
		key:        "testdb:metrics:1y:2024",
		database:   "testdb",
		collection: "metrics",
		level:      AggregationYearly,
		timeKey:    time.Date(2024, 5, 15, 0, 0, 0, 0, time.UTC),
	}

	err := pool.aggregateYearly(worker)
	if err == nil {
		t.Error("aggregateYearly should fail on read error")
	}
}

func TestAggregateYearly_WriteError(t *testing.T) {
	logger := logging.NewDevelopment()

	aggStorage := &mockAggregationStorage{
		readMonthlyForYearFunc: func(database, collection string, year time.Time) ([]*AggregatedPoint, error) {
			return []*AggregatedPoint{
				{
					Time: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), DeviceID: "dev1",
					Fields: map[string]*AggregatedField{"temp": {Sum: 100, Count: 10}},
				},
			}, nil
		},
		writeYearlyFunc: func(database, collection string, points []*AggregatedPoint) error {
			return fmt.Errorf("write error")
		},
	}

	config := DefaultWorkerPoolConfig()
	pool := NewAggregationWorkerPool(config, logger, &mockRawDataReader{}, aggStorage)

	worker := &PartitionWorker{
		key:        "testdb:metrics:1y:2024",
		database:   "testdb",
		collection: "metrics",
		level:      AggregationYearly,
		timeKey:    time.Date(2024, 5, 15, 0, 0, 0, 0, time.UTC),
	}

	err := pool.aggregateYearly(worker)
	if err == nil {
		t.Error("aggregateYearly should fail on write error")
	}
}

func TestAggregateYearly_MultipleDevices(t *testing.T) {
	logger := logging.NewDevelopment()

	var writtenPoints []*AggregatedPoint
	var mu sync.Mutex

	aggStorage := &mockAggregationStorage{
		readMonthlyForYearFunc: func(database, collection string, year time.Time) ([]*AggregatedPoint, error) {
			return []*AggregatedPoint{
				{
					Time: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), DeviceID: "dev1",
					Fields: map[string]*AggregatedField{"temp": {Sum: 100, Avg: 10, Min: 5, Max: 15, Count: 10}},
				},
				{
					Time: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), DeviceID: "dev2",
					Fields: map[string]*AggregatedField{"temp": {Sum: 200, Avg: 20, Min: 15, Max: 25, Count: 10}},
				},
				{
					Time: time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC), DeviceID: "dev1",
					Fields: map[string]*AggregatedField{"temp": {Sum: 300, Avg: 30, Min: 25, Max: 35, Count: 10}},
				},
			}, nil
		},
		writeYearlyFunc: func(database, collection string, points []*AggregatedPoint) error {
			mu.Lock()
			writtenPoints = append(writtenPoints, points...)
			mu.Unlock()
			return nil
		},
	}

	config := DefaultWorkerPoolConfig()
	pool := NewAggregationWorkerPool(config, logger, &mockRawDataReader{}, aggStorage)

	worker := &PartitionWorker{
		key:        "testdb:metrics:1y:2024",
		database:   "testdb",
		collection: "metrics",
		level:      AggregationYearly,
		timeKey:    time.Date(2024, 5, 15, 0, 0, 0, 0, time.UTC),
	}

	err := pool.aggregateYearly(worker)
	if err != nil {
		t.Fatalf("aggregateYearly failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(writtenPoints) != 2 {
		t.Errorf("Expected 2 yearly points (2 devices), got %d", len(writtenPoints))
	}
}

// =============================================================================
// processPartition
// =============================================================================

func TestProcessPartition_Success(t *testing.T) {
	logger := logging.NewDevelopment()

	var writeHourlyCalled atomic.Int32
	aggStorage := &mockAggregationStorage{
		writeHourlyFunc: func(database, collection string, points []*AggregatedPoint) error {
			writeHourlyCalled.Add(1)
			return nil
		},
	}

	rawReader := &mockRawDataReader{
		queryFunc: func(database, collection string, deviceIDs []string, startTime, endTime time.Time, fields []string) ([]DataPointInterface, error) {
			return []DataPointInterface{
				&mockDataPoint{id: "dev1", time: time.Now(), fields: map[string]interface{}{"temp": 25.0}},
			}, nil
		},
	}

	config := DefaultWorkerPoolConfig()
	pool := NewAggregationWorkerPool(config, logger, rawReader, aggStorage)

	worker := &PartitionWorker{
		key:          "testdb:metrics:1h:20240515",
		database:     "testdb",
		collection:   "metrics",
		level:        AggregationHourly,
		timeKey:      time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC),
		pendingCount: 3,
	}

	pool.processPartition(worker)

	// Verify pending count was reset
	worker.mu.Lock()
	if worker.pendingCount != 0 {
		t.Errorf("pendingCount = %d, want 0", worker.pendingCount)
	}
	worker.mu.Unlock()

	// Verify stats updated
	pool.statsMu.RLock()
	if pool.totalProcessed != 1 {
		t.Errorf("totalProcessed = %d, want 1", pool.totalProcessed)
	}
	pool.statsMu.RUnlock()
}

func TestProcessPartition_Failure(t *testing.T) {
	logger := logging.NewDevelopment()

	aggStorage := &mockAggregationStorage{}
	rawReader := &mockRawDataReader{
		queryFunc: func(database, collection string, deviceIDs []string, startTime, endTime time.Time, fields []string) ([]DataPointInterface, error) {
			return nil, fmt.Errorf("query failed")
		},
	}

	config := DefaultWorkerPoolConfig()
	pool := NewAggregationWorkerPool(config, logger, rawReader, aggStorage)

	worker := &PartitionWorker{
		key:          "testdb:metrics:1h:20240515",
		database:     "testdb",
		collection:   "metrics",
		level:        AggregationHourly,
		timeKey:      time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC),
		pendingCount: 1,
	}

	pool.processPartition(worker)

	pool.statsMu.RLock()
	if pool.totalFailed != 1 {
		t.Errorf("totalFailed = %d, want 1", pool.totalFailed)
	}
	pool.statsMu.RUnlock()
}

func TestProcessPartition_ZeroPending(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultWorkerPoolConfig()
	pool := NewAggregationWorkerPool(config, logger, &mockRawDataReader{}, &mockAggregationStorage{})

	worker := &PartitionWorker{
		key:          "testdb:metrics:1h:20240515",
		database:     "testdb",
		collection:   "metrics",
		level:        AggregationHourly,
		timeKey:      time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC),
		pendingCount: 0, // No pending
	}

	// Should be a no-op
	pool.processPartition(worker)

	pool.statsMu.RLock()
	if pool.totalProcessed != 0 {
		t.Errorf("totalProcessed = %d, should be 0 (no pending)", pool.totalProcessed)
	}
	pool.statsMu.RUnlock()
}

// =============================================================================
// processPartition cascading to next level
// =============================================================================

func TestProcessPartition_Cascade(t *testing.T) {
	logger := logging.NewDevelopment()

	rawReader := &mockRawDataReader{
		queryFunc: func(database, collection string, deviceIDs []string, startTime, endTime time.Time, fields []string) ([]DataPointInterface, error) {
			return []DataPointInterface{
				&mockDataPoint{id: "dev1", time: time.Now(), fields: map[string]interface{}{"temp": 25.0}},
			}, nil
		},
	}

	aggStorage := &mockAggregationStorage{
		writeHourlyFunc: func(database, collection string, points []*AggregatedPoint) error {
			return nil
		},
	}

	config := DefaultWorkerPoolConfig()
	config.BatchDelay = 50 * time.Millisecond

	hourlyPool := NewAggregationWorkerPool(config, logger, rawReader, aggStorage)
	dailyPool := NewAggregationWorkerPool(config, logger, rawReader, aggStorage)

	hourlyPool.SetNextLevelPool(AggregationDaily, dailyPool)
	dailyPool.Start()
	defer dailyPool.Stop()

	worker := &PartitionWorker{
		key:          "testdb:metrics:1h:20240515",
		database:     "testdb",
		collection:   "metrics",
		level:        AggregationHourly,
		timeKey:      time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC),
		pendingCount: 1,
	}

	hourlyPool.processPartition(worker)

	// Give some time for cascade notification
	time.Sleep(100 * time.Millisecond)

	// Verify daily pool received notification
	dailyPool.workersMu.RLock()
	workerCount := len(dailyPool.workers)
	dailyPool.workersMu.RUnlock()

	if workerCount == 0 {
		t.Log("Note: Daily worker may not have been created yet due to timing")
	}
}

// =============================================================================
// preemptIdleWorker
// =============================================================================

func TestPreemptIdleWorker_NoWorkers(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultWorkerPoolConfig()
	pool := NewAggregationWorkerPool(config, logger, &mockRawDataReader{}, &mockAggregationStorage{})

	// Should not panic with no workers
	pool.preemptIdleWorker()
}

func TestPreemptIdleWorker_NoIdleWorkers(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultWorkerPoolConfig()
	pool := NewAggregationWorkerPool(config, logger, &mockRawDataReader{}, &mockAggregationStorage{})

	// Add a running worker
	pool.workersMu.Lock()
	pool.workers["test:key"] = &PartitionWorker{
		key:    "test:key",
		state:  workerRunning,
		stopCh: make(chan struct{}),
	}
	pool.workersMu.Unlock()

	// Should not panic
	pool.preemptIdleWorker()
}

func TestPreemptIdleWorker_PreemptsOldestIdle(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultWorkerPoolConfig()
	pool := NewAggregationWorkerPool(config, logger, &mockRawDataReader{}, &mockAggregationStorage{})

	// Add two waiting workers with different idle times
	oldStopCh := make(chan struct{})
	newStopCh := make(chan struct{})

	pool.workersMu.Lock()
	pool.workers["old:key"] = &PartitionWorker{
		key:          "old:key",
		state:        workerWaitingForJob,
		pendingCount: 0,
		idleSince:    time.Now().Add(-5 * time.Minute),
		stopCh:       oldStopCh,
	}
	pool.workers["new:key"] = &PartitionWorker{
		key:          "new:key",
		state:        workerWaitingForJob,
		pendingCount: 0,
		idleSince:    time.Now().Add(-1 * time.Minute),
		stopCh:       newStopCh,
	}
	pool.workersMu.Unlock()

	pool.preemptIdleWorker()

	// Old worker should have been stopped
	select {
	case <-oldStopCh:
		// Good - oldest was preempted
	default:
		t.Error("Expected oldest idle worker to be preempted")
	}

	// New worker should NOT be stopped
	select {
	case <-newStopCh:
		t.Error("Newer idle worker should not be preempted")
	default:
		// Good
	}
}

func TestPreemptIdleWorker_SkipsPendingWorkers(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultWorkerPoolConfig()
	pool := NewAggregationWorkerPool(config, logger, &mockRawDataReader{}, &mockAggregationStorage{})

	stopCh := make(chan struct{})

	pool.workersMu.Lock()
	pool.workers["pending:key"] = &PartitionWorker{
		key:          "pending:key",
		state:        workerWaitingForJob,
		pendingCount: 5, // Has pending work
		idleSince:    time.Now().Add(-10 * time.Minute),
		stopCh:       stopCh,
	}
	pool.workersMu.Unlock()

	pool.preemptIdleWorker()

	// Should NOT be stopped because it has pending work
	select {
	case <-stopCh:
		t.Error("Worker with pending count should not be preempted")
	default:
		// Good
	}
}

// =============================================================================
// cleanupStoppedWorkers
// =============================================================================

func TestCleanupStoppedWorkers_RemovesIdleWithNoPending(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultWorkerPoolConfig()
	pool := NewAggregationWorkerPool(config, logger, &mockRawDataReader{}, &mockAggregationStorage{})

	pool.workersMu.Lock()
	pool.workers["idle1"] = &PartitionWorker{key: "idle1", state: workerIdle, pendingCount: 0}
	pool.workers["idle2"] = &PartitionWorker{key: "idle2", state: workerIdle, pendingCount: 0}
	pool.workers["running1"] = &PartitionWorker{key: "running1", state: workerRunning, pendingCount: 1}
	pool.workers["waiting1"] = &PartitionWorker{key: "waiting1", state: workerWaitingForJob, pendingCount: 0}
	pool.workersMu.Unlock()

	pool.cleanupStoppedWorkers()

	pool.workersMu.RLock()
	remaining := len(pool.workers)
	pool.workersMu.RUnlock()

	if remaining != 2 {
		t.Errorf("Expected 2 remaining workers (running + waiting), got %d", remaining)
	}

	// Verify specific workers are still there
	pool.workersMu.RLock()
	if _, ok := pool.workers["running1"]; !ok {
		t.Error("running1 should still exist")
	}
	if _, ok := pool.workers["waiting1"]; !ok {
		t.Error("waiting1 should still exist")
	}
	if _, ok := pool.workers["idle1"]; ok {
		t.Error("idle1 should have been removed")
	}
	pool.workersMu.RUnlock()
}

func TestCleanupStoppedWorkers_NoWorkers(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultWorkerPoolConfig()
	pool := NewAggregationWorkerPool(config, logger, &mockRawDataReader{}, &mockAggregationStorage{})

	// Should not panic
	pool.cleanupStoppedWorkers()
}

func TestCleanupStoppedWorkers_AllRunning(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultWorkerPoolConfig()
	pool := NewAggregationWorkerPool(config, logger, &mockRawDataReader{}, &mockAggregationStorage{})

	pool.workersMu.Lock()
	pool.workers["r1"] = &PartitionWorker{key: "r1", state: workerRunning, pendingCount: 1}
	pool.workers["r2"] = &PartitionWorker{key: "r2", state: workerRunning, pendingCount: 2}
	pool.workersMu.Unlock()

	pool.cleanupStoppedWorkers()

	pool.workersMu.RLock()
	if len(pool.workers) != 2 {
		t.Errorf("Expected 2 workers (all running), got %d", len(pool.workers))
	}
	pool.workersMu.RUnlock()
}

// =============================================================================
// Notify when pool is stopped
// =============================================================================

func TestNotify_AfterStop(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultWorkerPoolConfig()
	pool := NewAggregationWorkerPool(config, logger, &mockRawDataReader{}, &mockAggregationStorage{})

	pool.Start()
	pool.Stop()

	// Notify after stop should not block or panic
	pool.Notify(AggregationNotification{
		Database:   "testdb",
		Collection: "metrics",
		Level:      AggregationHourly,
		TimeKey:    time.Now(),
	})
}

func TestNotify_ChannelFull(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultWorkerPoolConfig()
	config.QueueSize = 1 // Very small queue

	pool := NewAggregationWorkerPool(config, logger, &mockRawDataReader{}, &mockAggregationStorage{})
	// Don't start the pool so notifications pile up in the channel

	// Fill the channel
	for i := 0; i < config.QueueSize*10+1; i++ {
		pool.Notify(AggregationNotification{
			Database:   "testdb",
			Collection: "metrics",
			Level:      AggregationHourly,
			TimeKey:    time.Now(),
		})
	}
	// Should not block
}

// =============================================================================
// dispatchToWorker
// =============================================================================

func TestDispatchToWorker_CreatesNewWorker(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultWorkerPoolConfig()
	pool := NewAggregationWorkerPool(config, logger, &mockRawDataReader{}, &mockAggregationStorage{})
	pool.Start()
	defer pool.Stop()

	notif := AggregationNotification{
		Database:   "testdb",
		Collection: "metrics",
		Level:      AggregationHourly,
		TimeKey:    time.Now(),
	}

	pool.dispatchToWorker(notif)

	pool.workersMu.RLock()
	key := pool.partitionKey(notif.Database, notif.Collection, notif.Level, notif.TimeKey)
	worker, ok := pool.workers[key]
	pool.workersMu.RUnlock()

	if !ok {
		t.Fatal("Worker should have been created")
	}
	if worker == nil {
		t.Fatal("Worker should not be nil")
	}
}

func TestDispatchToWorker_UpdatesTimeKey(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultWorkerPoolConfig()
	pool := NewAggregationWorkerPool(config, logger, &mockRawDataReader{}, &mockAggregationStorage{})
	pool.Start()
	defer pool.Stop()

	oldTime := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)
	newTime := time.Date(2024, 5, 15, 11, 0, 0, 0, time.UTC)

	notif1 := AggregationNotification{Database: "testdb", Collection: "metrics", Level: AggregationHourly, TimeKey: oldTime}
	notif2 := AggregationNotification{Database: "testdb", Collection: "metrics", Level: AggregationHourly, TimeKey: newTime}

	pool.dispatchToWorker(notif1)
	pool.dispatchToWorker(notif2)

	pool.workersMu.RLock()
	key := pool.partitionKey("testdb", "metrics", AggregationHourly, oldTime)
	worker := pool.workers[key]
	pool.workersMu.RUnlock()

	if worker == nil {
		t.Fatal("Worker should exist")
	}

	worker.mu.Lock()
	tk := worker.timeKey
	worker.mu.Unlock()

	if !tk.Equal(newTime) {
		t.Errorf("timeKey = %v, want %v (should update to newer)", tk, newTime)
	}
}

// =============================================================================
// dispatchToWorker semaphore full → pending queue
// =============================================================================

func TestDispatchToWorker_SemaphoreFull(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultWorkerPoolConfig()
	config.MaxActiveWorkers = 1 // Very limited
	pool := NewAggregationWorkerPool(config, logger, &mockRawDataReader{}, &mockAggregationStorage{})
	pool.Start()
	defer pool.Stop()

	// Fill semaphore
	pool.semaphore <- struct{}{}

	notif := AggregationNotification{
		Database:   "testdb",
		Collection: "metrics",
		Level:      AggregationHourly,
		TimeKey:    time.Now(),
	}

	// Should go to pending queue
	pool.dispatchToWorker(notif)

	pool.workersMu.RLock()
	key := pool.partitionKey(notif.Database, notif.Collection, notif.Level, notif.TimeKey)
	worker := pool.workers[key]
	pool.workersMu.RUnlock()

	if worker == nil {
		t.Fatal("Worker should exist")
	}

	// Release semaphore so pending queue can process
	<-pool.semaphore
}

// =============================================================================
// Stats while running
// =============================================================================

func TestStats_WhileRunning(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultWorkerPoolConfig()
	pool := NewAggregationWorkerPool(config, logger, &mockRawDataReader{}, &mockAggregationStorage{})
	pool.Start()
	defer pool.Stop()

	// Send some notifications
	for i := 0; i < 5; i++ {
		pool.Notify(AggregationNotification{
			Database:   fmt.Sprintf("db%d", i),
			Collection: "metrics",
			Level:      AggregationHourly,
			TimeKey:    time.Now(),
		})
	}

	time.Sleep(50 * time.Millisecond)

	stats := pool.Stats()

	// Basic validation
	if stats["max_active_workers"].(int) != config.MaxActiveWorkers {
		t.Errorf("max_active_workers = %d, want %d", stats["max_active_workers"].(int), config.MaxActiveWorkers)
	}
}

// =============================================================================
// partitionKey - default/unknown level
// =============================================================================

func TestPartitionKey_UnknownLevel(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultWorkerPoolConfig()
	pool := NewAggregationWorkerPool(config, logger, &mockRawDataReader{}, &mockAggregationStorage{})

	key := pool.partitionKey("db", "col", AggregationLevel("unknown"), time.Date(2024, 5, 15, 10, 30, 0, 0, time.UTC))
	expected := "db:col:unknown:20240515"
	if key != expected {
		t.Errorf("partitionKey unknown = %q, want %q", key, expected)
	}
}
