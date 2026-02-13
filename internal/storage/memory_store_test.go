package storage

import (
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/logging"
)

func TestNewMemoryStore(t *testing.T) {
	logger := logging.NewDevelopment()
	maxAge := 1 * time.Hour
	maxSize := 10000

	ms := NewMemoryStore(maxAge, maxSize, logger)

	if ms == nil {
		t.Fatal("Expected non-nil MemoryStore")
		return
	}

	if ms.maxAge != maxAge {
		t.Errorf("Expected maxAge %v, got %v", maxAge, ms.maxAge)
	}

	if ms.maxSize != maxSize {
		t.Errorf("Expected maxSize %d, got %d", maxSize, ms.maxSize)
	}

	if ms.Count() != 0 {
		t.Errorf("Expected initial count 0, got %d", ms.Count())
	}

	_ = ms.Close()
}

func TestMemoryStore_Write(t *testing.T) {
	logger := logging.NewDevelopment()
	ms := NewMemoryStore(1*time.Hour, 10000, logger)
	defer func() { _ = ms.Close() }()

	now := time.Now()
	dp := &DataPoint{
		Database:   "testdb",
		Collection: "testcoll",
		ShardID:    "shard1",
		Time:       now,
		ID:         "device1",
		Fields:     map[string]interface{}{"temp": 25.5, "humidity": 60.0},
		InsertedAt: now,
	}

	err := ms.Write(dp)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if ms.Count() != 1 {
		t.Errorf("Expected count 1, got %d", ms.Count())
	}

	if ms.Size() == 0 {
		t.Error("Expected non-zero size")
	}

	// Verify data structure
	if !ms.hasDevice("testdb", "testcoll", "device1") {
		t.Error("Device not created")
	}
}

func TestMemoryStore_WriteMultiple(t *testing.T) {
	logger := logging.NewDevelopment()
	ms := NewMemoryStore(1*time.Hour, 10000, logger)
	defer func() { _ = ms.Close() }()

	now := time.Now()

	// Write 10 points for same device
	for i := 0; i < 10; i++ {
		dp := &DataPoint{
			Database:   "testdb",
			Collection: "testcoll",
			ShardID:    "shard1",
			Time:       now.Add(time.Duration(i) * time.Second),
			ID:         "device1",
			Fields:     map[string]interface{}{"temp": float64(20 + i)},
			InsertedAt: now,
		}

		err := ms.Write(dp)
		if err != nil {
			t.Fatalf("Write %d failed: %v", i, err)
		}
	}

	if ms.Count() != 10 {
		t.Errorf("Expected count 10, got %d", ms.Count())
	}
}

func TestMemoryStore_WriteMultipleDevices(t *testing.T) {
	logger := logging.NewDevelopment()
	ms := NewMemoryStore(1*time.Hour, 10000, logger)
	defer func() { _ = ms.Close() }()

	now := time.Now()

	// Write data for 3 devices
	for i := 0; i < 3; i++ {
		dp := &DataPoint{
			Database:   "testdb",
			Collection: "testcoll",
			ShardID:    "shard1",
			Time:       now,
			ID:         "device" + string(rune('1'+i)),
			Fields:     map[string]interface{}{"temp": float64(20 + i)},
			InsertedAt: now,
		}

		err := ms.Write(dp)
		if err != nil {
			t.Fatalf("Write device %d failed: %v", i, err)
		}
	}

	if ms.Count() != 3 {
		t.Errorf("Expected count 3, got %d", ms.Count())
	}

	deviceIDs := ms.GetDeviceIDs("testdb", "testcoll")
	if len(deviceIDs) != 3 {
		t.Errorf("Expected 3 device IDs, got %d", len(deviceIDs))
	}
}

func TestMemoryStore_GetUnFlushed(t *testing.T) {
	logger := logging.NewDevelopment()
	ms := NewMemoryStore(1*time.Hour, 10000, logger)
	defer func() { _ = ms.Close() }()

	now := time.Now()

	// Write 5 points
	for i := 0; i < 5; i++ {
		dp := &DataPoint{
			Database:   "testdb",
			Collection: "testcoll",
			ShardID:    "shard1",
			Time:       now.Add(time.Duration(i) * time.Second),
			ID:         "device1",
			Fields:     map[string]interface{}{"temp": float64(20 + i)},
			InsertedAt: now,
		}

		_ = ms.Write(dp)
	}

	// Get unflushed
	unflushed, err := ms.GetUnFlushed()
	if err != nil {
		t.Fatalf("GetUnFlushed failed: %v", err)
	}

	if len(unflushed) != 5 {
		t.Errorf("Expected 5 unflushed points, got %d", len(unflushed))
	}

	// All should be marked as Flushing
	for i, dp := range unflushed {
		if dp.FlushStatus != FlushStatusFlushing {
			t.Errorf("Point %d: expected FlushStatusFlushing, got %d", i, dp.FlushStatus)
		}
	}

	// Second call should return 0 (already marked as flushing)
	unflushed2, _ := ms.GetUnFlushed()
	if len(unflushed2) != 0 {
		t.Errorf("Expected 0 unflushed on second call, got %d", len(unflushed2))
	}
}

func TestMemoryStore_MarkFlushed(t *testing.T) {
	logger := logging.NewDevelopment()
	ms := NewMemoryStore(1*time.Hour, 10000, logger)
	defer func() { _ = ms.Close() }()

	now := time.Now()

	// Write points
	for i := 0; i < 3; i++ {
		dp := &DataPoint{
			Database:   "testdb",
			Collection: "testcoll",
			ShardID:    "shard1",
			Time:       now.Add(time.Duration(i) * time.Second),
			ID:         "device1",
			Fields:     map[string]interface{}{"temp": float64(20 + i)},
			InsertedAt: now,
		}
		_ = ms.Write(dp)
	}

	// Mark as flushing
	unflushed, _ := ms.GetUnFlushed()
	if len(unflushed) != 3 {
		t.Fatalf("Expected 3 unflushed, got %d", len(unflushed))
	}

	// Mark as flushed
	err := ms.MarkFlushed()
	if err != nil {
		t.Fatalf("MarkFlushed failed: %v", err)
	}

	// Verify status changed
	deviceSlice := ms.getDeviceSlice("testdb", "testcoll", "device1")
	if deviceSlice == nil {
		t.Fatal("Device slice not found")
	}
	for i := 0; i < 3; i++ {
		point := deviceSlice.GetAt(i)
		if point.FlushStatus != FlushStatusFlushed {
			t.Errorf("Point %d: expected FlushStatusFlushed, got %d", i, point.FlushStatus)
		}
	}
}

func TestMemoryStore_Query(t *testing.T) {
	logger := logging.NewDevelopment()
	ms := NewMemoryStore(1*time.Hour, 10000, logger)
	defer func() { _ = ms.Close() }()

	now := time.Now()

	// Write 10 points
	for i := 0; i < 10; i++ {
		dp := &DataPoint{
			Database:   "testdb",
			Collection: "testcoll",
			ShardID:    "shard1",
			Time:       now.Add(time.Duration(i) * time.Second),
			ID:         "device1",
			Fields:     map[string]interface{}{"temp": float64(20 + i)},
			InsertedAt: now,
		}
		_ = ms.Write(dp)
	}

	tests := []struct {
		name      string
		database  string
		coll      string
		deviceID  string
		start     time.Time
		end       time.Time
		wantCount int
	}{
		{
			name:      "Query all",
			database:  "testdb",
			coll:      "testcoll",
			deviceID:  "device1",
			start:     now,
			end:       now.Add(10 * time.Second),
			wantCount: 10,
		},
		{
			name:      "Query middle range",
			database:  "testdb",
			coll:      "testcoll",
			deviceID:  "device1",
			start:     now.Add(3 * time.Second),
			end:       now.Add(6 * time.Second),
			wantCount: 4,
		},
		{
			name:      "Query non-existent database",
			database:  "wrongdb",
			coll:      "testcoll",
			deviceID:  "device1",
			start:     now,
			end:       now.Add(10 * time.Second),
			wantCount: 0,
		},
		{
			name:      "Query non-existent collection",
			database:  "testdb",
			coll:      "wrongcoll",
			deviceID:  "device1",
			start:     now,
			end:       now.Add(10 * time.Second),
			wantCount: 0,
		},
		{
			name:      "Query non-existent device",
			database:  "testdb",
			coll:      "testcoll",
			deviceID:  "device999",
			start:     now,
			end:       now.Add(10 * time.Second),
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ms.Query(tt.database, tt.coll, tt.deviceID, tt.start, tt.end)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			if len(result) != tt.wantCount {
				t.Errorf("Expected %d results, got %d", tt.wantCount, len(result))
			}
		})
	}
}

func TestMemoryStore_QueryCollection(t *testing.T) {
	logger := logging.NewDevelopment()
	ms := NewMemoryStore(1*time.Hour, 10000, logger)
	defer func() { _ = ms.Close() }()

	now := time.Now()

	// Write data for 3 devices
	for i := 0; i < 3; i++ {
		for j := 0; j < 5; j++ {
			dp := &DataPoint{
				Database:   "testdb",
				Collection: "testcoll",
				ShardID:    "shard1",
				Time:       now.Add(time.Duration(j) * time.Second),
				ID:         "device" + string(rune('1'+i)),
				Fields:     map[string]interface{}{"temp": float64(20 + j)},
				InsertedAt: now,
			}
			_ = ms.Write(dp)
		}
	}

	// Query entire collection (all devices)
	result, err := ms.QueryCollection("testdb", "testcoll", now, now.Add(10*time.Second))
	if err != nil {
		t.Fatalf("QueryCollection failed: %v", err)
	}

	if len(result) != 15 {
		t.Errorf("Expected 15 results (3 devices × 5 points), got %d", len(result))
	}
}

func TestMemoryStore_GetStats(t *testing.T) {
	logger := logging.NewDevelopment()
	ms := NewMemoryStore(1*time.Hour, 10000, logger)
	defer func() { _ = ms.Close() }()

	now := time.Now()

	// Write data for 2 databases, 2 collections each, 2 devices each
	for db := 0; db < 2; db++ {
		for coll := 0; coll < 2; coll++ {
			for dev := 0; dev < 2; dev++ {
				dp := &DataPoint{
					Database:   "db" + string(rune('1'+db)),
					Collection: "coll" + string(rune('1'+coll)),
					ShardID:    "shard1",
					Time:       now,
					ID:         "device" + string(rune('1'+dev)),
					Fields:     map[string]interface{}{"temp": 20.0},
					InsertedAt: now,
				}
				_ = ms.Write(dp)
			}
		}
	}

	stats := ms.GetStats()

	if stats["total_count"].(int64) != 8 {
		t.Errorf("Expected total_count 8, got %v", stats["total_count"])
	}

	if stats["database_count"].(int) != 2 {
		t.Errorf("Expected database_count 2, got %v", stats["database_count"])
	}

	if stats["collection_count"].(int) != 4 {
		t.Errorf("Expected collection_count 4, got %v", stats["collection_count"])
	}

	if stats["device_count"].(int) != 8 {
		t.Errorf("Expected device_count 8, got %v", stats["device_count"])
	}
}

func TestMemoryStore_QueryIter(t *testing.T) {
	logger := logging.NewDevelopment()
	ms := NewMemoryStore(1*time.Hour, 10000, logger)
	defer func() { _ = ms.Close() }()

	now := time.Now()

	// Write data for 3 devices, 5 points each
	for i := 0; i < 3; i++ {
		for j := 0; j < 5; j++ {
			dp := &DataPoint{
				Database:   "testdb",
				Collection: "testcoll",
				ShardID:    "shard1",
				Time:       now.Add(time.Duration(j) * time.Second),
				ID:         "device" + string(rune('1'+i)),
				Fields:     map[string]interface{}{"temp": float64(20 + j)},
				InsertedAt: now,
			}
			_ = ms.Write(dp)
		}
	}

	// Test QueryIter all devices — should return same count as Query
	queryResult, _ := ms.QueryCollection("testdb", "testcoll", now, now.Add(10*time.Second))

	iterCount := 0
	next := ms.QueryIter("testdb", "testcoll", "", now, now.Add(10*time.Second))
	for _, ok := next(); ok; _, ok = next() {
		iterCount++
	}

	if iterCount != len(queryResult) {
		t.Errorf("QueryIter returned %d points, Query returned %d", iterCount, len(queryResult))
	}

	// Test QueryIter specific device
	iterCount = 0
	next = ms.QueryIter("testdb", "testcoll", "device1", now, now.Add(10*time.Second))
	for _, ok := next(); ok; _, ok = next() {
		iterCount++
	}
	if iterCount != 5 {
		t.Errorf("QueryIter(device1) returned %d, expected 5", iterCount)
	}

	// Test QueryIter non-existent
	next = ms.QueryIter("testdb", "testcoll", "nonexistent", now, now.Add(10*time.Second))
	dp, ok := next()
	if ok || dp != nil {
		t.Error("Expected empty iterator for non-existent device")
	}

	// Test QueryIter non-existent database
	next = ms.QueryIter("wrongdb", "testcoll", "", now, now.Add(10*time.Second))
	dp, ok = next()
	if ok || dp != nil {
		t.Error("Expected empty iterator for non-existent database")
	}
}

func TestMemoryStore_Cleanup(t *testing.T) {
	logger := logging.NewDevelopment()
	maxAge := 100 * time.Millisecond
	ms := NewMemoryStore(maxAge, 10000, logger)
	defer func() { _ = ms.Close() }()

	now := time.Now()

	// Write old point
	oldDp := &DataPoint{
		Database:   "testdb",
		Collection: "testcoll",
		ShardID:    "shard1",
		Time:       now.Add(-200 * time.Millisecond),
		ID:         "device1",
		Fields:     map[string]interface{}{"temp": 20.0},
		InsertedAt: now.Add(-200 * time.Millisecond),
	}
	_ = ms.Write(oldDp)

	// Write new point
	newDp := &DataPoint{
		Database:   "testdb",
		Collection: "testcoll",
		ShardID:    "shard1",
		Time:       now,
		ID:         "device1",
		Fields:     map[string]interface{}{"temp": 25.0},
		InsertedAt: now,
	}
	_ = ms.Write(newDp)

	if ms.Count() != 2 {
		t.Errorf("Expected count 2 before cleanup, got %d", ms.Count())
	}

	// Simulate flush cycle: GetUnFlushed marks as Flushing, then MarkFlushed marks as Flushed
	// This is required because cleanup only removes flushed data to prevent data loss
	_, _ = ms.GetUnFlushed() // Marks New -> Flushing
	_ = ms.MarkFlushed()     // Marks Flushing -> Flushed

	// Trigger cleanup manually
	ms.cleanup()

	if ms.Count() != 1 {
		t.Errorf("Expected count 1 after cleanup (only old flushed point removed), got %d", ms.Count())
	}
}

func TestMemoryStore_EvictOldest(t *testing.T) {
	logger := logging.NewDevelopment()
	maxSize := 5
	ms := NewMemoryStore(1*time.Hour, maxSize, logger)
	defer func() { _ = ms.Close() }()

	now := time.Now()

	// Write 10 points (exceeds maxSize)
	for i := 0; i < 10; i++ {
		dp := &DataPoint{
			Database:   "testdb",
			Collection: "testcoll",
			ShardID:    "shard1",
			Time:       now.Add(time.Duration(i) * time.Second),
			ID:         "device1",
			Fields:     map[string]interface{}{"temp": float64(20 + i)},
			InsertedAt: now,
		}
		_ = ms.Write(dp)
	}

	// Mark first 10 points as flushed (so they can be evicted)
	// Get all unflushed and mark them as flushing, then mark all as flushed
	_, _ = ms.GetUnFlushed()
	_ = ms.MarkFlushed()

	// Trigger eviction manually by writing more data
	for i := 10; i < 12; i++ {
		dp := &DataPoint{
			Database:   "testdb",
			Collection: "testcoll",
			ShardID:    "shard1",
			Time:       now.Add(time.Duration(i) * time.Second),
			ID:         "device1",
			Fields:     map[string]interface{}{"temp": float64(20 + i)},
			InsertedAt: now,
		}
		_ = ms.Write(dp)
	}

	// Wait for background eviction to complete (async operation)
	time.Sleep(100 * time.Millisecond)

	// Should have evicted oldest flushed points to stay at maxSize
	count := ms.Count()
	if count != int64(maxSize) {
		t.Errorf("Expected count %d after eviction, got %d", maxSize, count)
	}

	// Verify correct points remain
	result, _ := ms.Query("testdb", "testcoll", "device1", now, now.Add(20*time.Second))
	if len(result) != maxSize {
		t.Errorf("Expected %d points remaining, got %d", maxSize, len(result))
	}

	// Should have newest 5 points (7-11)
	if len(result) > 0 && !result[0].Time.Equal(now.Add(7*time.Second)) {
		t.Errorf("Expected oldest remaining point to be at t=7s, got %v", result[0].Time)
	}
}

func TestMemoryStore_GetDeviceIDs(t *testing.T) {
	logger := logging.NewDevelopment()
	ms := NewMemoryStore(1*time.Hour, 10000, logger)
	defer func() { _ = ms.Close() }()

	now := time.Now()

	// Write data for 3 devices
	deviceNames := []string{"device1", "device2", "device3"}
	for _, name := range deviceNames {
		dp := &DataPoint{
			Database:   "testdb",
			Collection: "testcoll",
			ShardID:    "shard1",
			Time:       now,
			ID:         name,
			Fields:     map[string]interface{}{"temp": 20.0},
			InsertedAt: now,
		}
		_ = ms.Write(dp)
	}

	deviceIDs := ms.GetDeviceIDs("testdb", "testcoll")

	if len(deviceIDs) != 3 {
		t.Errorf("Expected 3 device IDs, got %d", len(deviceIDs))
	}

	// Check all devices present
	deviceMap := make(map[string]bool)
	for _, id := range deviceIDs {
		deviceMap[id] = true
	}

	for _, name := range deviceNames {
		if !deviceMap[name] {
			t.Errorf("Expected to find device %s", name)
		}
	}

	// Test non-existent database
	ids := ms.GetDeviceIDs("wrongdb", "testcoll")
	if ids != nil {
		t.Error("Expected nil for non-existent database")
	}

	// Test non-existent collection
	ids = ms.GetDeviceIDs("testdb", "wrongcoll")
	if ids != nil {
		t.Error("Expected nil for non-existent collection")
	}
}

func TestMemoryStore_Close(t *testing.T) {
	logger := logging.NewDevelopment()
	ms := NewMemoryStore(1*time.Hour, 10000, logger)

	now := time.Now()
	dp := &DataPoint{
		Database:   "testdb",
		Collection: "testcoll",
		ShardID:    "shard1",
		Time:       now,
		ID:         "device1",
		Fields:     map[string]interface{}{"temp": 20.0},
		InsertedAt: now,
	}
	_ = ms.Write(dp)

	err := ms.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Verify cleanup and eviction goroutines stopped
	select {
	case <-ms.cleanupDone:
		// Expected - channel closed
	case <-time.After(100 * time.Millisecond):
		t.Error("Cleanup goroutine did not stop")
	}

	select {
	case <-ms.evictionDone:
		// Expected - channel closed
	case <-time.After(100 * time.Millisecond):
		t.Error("Eviction goroutine did not stop")
	}
}

func TestDataPoint_estimateSize(t *testing.T) {
	dp := &DataPoint{
		Database:    "testdb",
		Collection:  "testcoll",
		ShardID:     "shard1",
		Time:        time.Now(),
		ID:          "device1",
		Fields:      map[string]interface{}{"temp": 20.0, "humidity": 60.0},
		InsertedAt:  time.Now(),
		FlushStatus: FlushStatusNew,
	}

	size := dp.estimateSize()

	if size == 0 {
		t.Error("Expected non-zero estimated size")
	}

	// Size should be reasonable (not too large or too small)
	if size < 100 || size > 10000 {
		t.Errorf("Unexpected size estimate: %d bytes", size)
	}
}

func TestMemoryStore_QueryDevice(t *testing.T) {
	logger := logging.NewDevelopment()
	ms := NewMemoryStore(1*time.Hour, 10000, logger)
	defer func() { _ = ms.Close() }()

	now := time.Now()

	// Write some data points
	for i := 0; i < 5; i++ {
		dp := &DataPoint{
			Database:   "testdb",
			Collection: "testcoll",
			ShardID:    "shard1",
			Time:       now.Add(time.Duration(i) * time.Second),
			ID:         "device1",
			Fields:     map[string]interface{}{"temp": float64(20 + i)},
			InsertedAt: now,
		}
		_ = ms.Write(dp)
	}

	// Query device
	startTime := now.Add(-time.Minute)
	endTime := now.Add(time.Minute)
	result, err := ms.QueryDevice("testdb", "testcoll", "device1", startTime, endTime)
	if err != nil {
		t.Fatalf("QueryDevice failed: %v", err)
	}

	if len(result) != 5 {
		t.Errorf("Expected 5 results, got %d", len(result))
	}
}

func TestMemoryStore_GetMaxAge(t *testing.T) {
	logger := logging.NewDevelopment()
	maxAge := 2 * time.Hour
	ms := NewMemoryStore(maxAge, 10000, logger)
	defer func() { _ = ms.Close() }()

	result := ms.GetMaxAge()
	if result != maxAge {
		t.Errorf("GetMaxAge() = %v, expected %v", result, maxAge)
	}
}

func TestSortDataPoints(t *testing.T) {
	now := time.Now()
	points := []*DataPoint{
		{Time: now.Add(2 * time.Second), ID: "device1"},
		{Time: now, ID: "device1"},
		{Time: now.Add(1 * time.Second), ID: "device1"},
		{Time: now.Add(-1 * time.Second), ID: "device1"},
	}

	SortDataPoints(points)

	// Verify sorted order
	for i := 1; i < len(points); i++ {
		if points[i].Time.Before(points[i-1].Time) {
			t.Errorf("Points not sorted: %v should come before %v", points[i-1].Time, points[i].Time)
		}
	}

	// Verify first and last
	if !points[0].Time.Before(points[len(points)-1].Time) {
		t.Error("First point should be before last point")
	}
}

func TestSortDataPoints_Empty(t *testing.T) {
	points := []*DataPoint{}
	// Should not panic
	SortDataPoints(points)

	if len(points) != 0 {
		t.Error("Empty slice should remain empty")
	}
}

func TestSortDataPoints_Single(t *testing.T) {
	now := time.Now()
	points := []*DataPoint{
		{Time: now, ID: "device1"},
	}

	SortDataPoints(points)

	if len(points) != 1 {
		t.Error("Single element slice should have 1 element")
	}
}
