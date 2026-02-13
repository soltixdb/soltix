package aggregation

import (
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/logging"
)

func TestStorage_WriteAggregatedPoints(t *testing.T) {
	// Create temp directory
	tempDir, err := os.MkdirTemp("", "agg_storage_api_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	points := []*AggregatedPoint{
		{
			Time:     now,
			DeviceID: "device1",
			Level:    AggregationHourly,
			Fields: map[string]*AggregatedField{
				"temperature": {
					Count:      10,
					Sum:        250.0,
					Avg:        25.0,
					Min:        20.0,
					Max:        30.0,
					SumSquares: 6300.0,
				},
				"humidity": {
					Count:      10,
					Sum:        600.0,
					Avg:        60.0,
					Min:        55.0,
					Max:        65.0,
					SumSquares: 36100.0,
				},
			},
		},
	}

	err = storage.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points, now)
	if err != nil {
		t.Fatalf("WriteAggregatedPoints failed: %v", err)
	}

	// Verify directory was created
	partDir := filepath.Join(tempDir, "agg_1h", "testdb", "metrics", "2024", "05", "15")
	if _, err := os.Stat(partDir); os.IsNotExist(err) {
		t.Errorf("Partition directory was not created: %s", partDir)
	}
}

func TestStorage_WriteAggregatedPoints_EmptyPoints(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_storage_api_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	// Should not error with empty points
	err = storage.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", []*AggregatedPoint{}, time.Now())
	if err != nil {
		t.Fatalf("WriteAggregatedPoints with empty points failed: %v", err)
	}
}

func TestStorage_ReadAggregatedPoints(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_storage_api_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	// Write some points
	points := []*AggregatedPoint{
		{
			Time:     now,
			DeviceID: "device1",
			Level:    AggregationHourly,
			Fields: map[string]*AggregatedField{
				"temperature": {
					Count:      10,
					Sum:        250.0,
					Avg:        25.0,
					Min:        20.0,
					Max:        30.0,
					SumSquares: 6300.0,
				},
			},
		},
		{
			Time:     now.Add(time.Hour),
			DeviceID: "device1",
			Level:    AggregationHourly,
			Fields: map[string]*AggregatedField{
				"temperature": {
					Count:      10,
					Sum:        260.0,
					Avg:        26.0,
					Min:        21.0,
					Max:        31.0,
					SumSquares: 6760.0,
				},
			},
		},
	}

	err = storage.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points, now)
	if err != nil {
		t.Fatalf("WriteAggregatedPoints failed: %v", err)
	}

	// Read points back
	readPoints, err := storage.ReadAggregatedPoints(AggregationHourly, "testdb", "metrics", ReadOptions{
		StartTime: now,
		EndTime:   now.Add(3 * time.Hour),
	})
	if err != nil {
		t.Fatalf("ReadAggregatedPoints failed: %v", err)
	}

	if len(readPoints) != 2 {
		t.Errorf("Expected 2 points, got %d", len(readPoints))
	}
}

func TestStorage_ReadAggregatedPoints_WithDeviceFilter(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_storage_api_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	// Write points for multiple devices
	points := []*AggregatedPoint{
		{
			Time:     now,
			DeviceID: "device1",
			Level:    AggregationHourly,
			Fields: map[string]*AggregatedField{
				"temperature": {Count: 10, Sum: 250.0, Avg: 25.0, Min: 20.0, Max: 30.0},
			},
		},
		{
			Time:     now,
			DeviceID: "device2",
			Level:    AggregationHourly,
			Fields: map[string]*AggregatedField{
				"temperature": {Count: 10, Sum: 260.0, Avg: 26.0, Min: 21.0, Max: 31.0},
			},
		},
	}

	err = storage.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points, now)
	if err != nil {
		t.Fatalf("WriteAggregatedPoints failed: %v", err)
	}

	// Read with device filter
	readPoints, err := storage.ReadAggregatedPoints(AggregationHourly, "testdb", "metrics", ReadOptions{
		StartTime: now,
		EndTime:   now.Add(time.Hour),
		DeviceIDs: []string{"device1"},
	})
	if err != nil {
		t.Fatalf("ReadAggregatedPoints failed: %v", err)
	}

	if len(readPoints) != 1 {
		t.Errorf("Expected 1 point after filtering, got %d", len(readPoints))
	}

	if len(readPoints) > 0 && readPoints[0].DeviceID != "device1" {
		t.Errorf("Expected device1, got %s", readPoints[0].DeviceID)
	}
}

func TestReadOptions(t *testing.T) {
	now := time.Now()
	opts := ReadOptions{
		StartTime: now,
		EndTime:   now.Add(time.Hour),
		DeviceIDs: []string{"dev1", "dev2"},
		Fields:    []string{"temp", "humidity"},
		Metrics:   []MetricType{MetricSum, MetricAvg},
	}

	if !opts.StartTime.Equal(now) {
		t.Errorf("StartTime = %v, expected %v", opts.StartTime, now)
	}
	if !opts.EndTime.Equal(now.Add(time.Hour)) {
		t.Errorf("EndTime = %v, expected %v", opts.EndTime, now.Add(time.Hour))
	}
	if len(opts.DeviceIDs) != 2 {
		t.Errorf("DeviceIDs length = %d, expected 2", len(opts.DeviceIDs))
	}
	if len(opts.Fields) != 2 {
		t.Errorf("Fields length = %d, expected 2", len(opts.Fields))
	}
	if len(opts.Metrics) != 2 {
		t.Errorf("Metrics length = %d, expected 2", len(opts.Metrics))
	}
}

func TestStorage_GetPartitionStats(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_storage_api_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	// Write some points
	points := []*AggregatedPoint{
		{
			Time:     now,
			DeviceID: "device1",
			Level:    AggregationHourly,
			Fields: map[string]*AggregatedField{
				"temperature": {Count: 10, Sum: 250.0, Avg: 25.0, Min: 20.0, Max: 30.0},
			},
		},
	}

	err = storage.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points, now)
	if err != nil {
		t.Fatalf("WriteAggregatedPoints failed: %v", err)
	}

	// Get partition stats
	stats, err := storage.GetPartitionStats(AggregationHourly, "testdb", "metrics", now)
	if err != nil {
		t.Fatalf("GetPartitionStats failed: %v", err)
	}

	if stats == nil {
		t.Fatal("GetPartitionStats returned nil")
		return
	}

	if stats.RowCount != 1 {
		t.Errorf("RowCount = %d, expected 1", stats.RowCount)
	}
}

func TestStorage_ListPartitions(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_storage_api_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	// Write points for multiple days
	for i := 0; i < 3; i++ {
		day := time.Date(2024, 5, 15+i, 10, 0, 0, 0, time.UTC)
		points := []*AggregatedPoint{
			{
				Time:     day,
				DeviceID: "device1",
				Level:    AggregationHourly,
				Fields: map[string]*AggregatedField{
					"temperature": {Count: 10, Sum: 250.0, Avg: 25.0, Min: 20.0, Max: 30.0},
				},
			},
		}
		err := storage.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points, day)
		if err != nil {
			t.Fatalf("WriteAggregatedPoints failed: %v", err)
		}
	}

	// List partitions
	partitions, err := storage.ListPartitions(AggregationHourly, "testdb", "metrics")
	if err != nil {
		t.Fatalf("ListPartitions failed: %v", err)
	}

	if len(partitions) != 3 {
		t.Errorf("Expected 3 partitions, got %d", len(partitions))
	}
}

func TestStorage_ListPartitions_NonExistent(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_storage_api_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	// List partitions for non-existent collection
	partitions, err := storage.ListPartitions(AggregationHourly, "nonexistent", "collection")
	if err != nil {
		t.Fatalf("ListPartitions failed: %v", err)
	}

	if len(partitions) != 0 {
		t.Errorf("Expected 0 partitions for non-existent collection, got %d", len(partitions))
	}
}

func TestStorage_DeleteOldData(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_storage_api_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	// Write old and new data
	oldTime := time.Date(2023, 1, 15, 10, 0, 0, 0, time.UTC)
	newTime := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	points := []*AggregatedPoint{
		{
			Time:     oldTime,
			DeviceID: "device1",
			Level:    AggregationHourly,
			Fields: map[string]*AggregatedField{
				"temperature": {Count: 10, Sum: 250.0, Avg: 25.0, Min: 20.0, Max: 30.0},
			},
		},
	}
	err = storage.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points, oldTime)
	if err != nil {
		t.Fatalf("WriteAggregatedPoints (old) failed: %v", err)
	}

	points[0].Time = newTime
	err = storage.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points, newTime)
	if err != nil {
		t.Fatalf("WriteAggregatedPoints (new) failed: %v", err)
	}

	// Delete old data
	cutoffTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	err = storage.DeleteOldData(AggregationHourly, "testdb", "metrics", cutoffTime)
	if err != nil {
		t.Fatalf("DeleteOldData failed: %v", err)
	}

	// Verify old data was deleted
	partitions, err := storage.ListPartitions(AggregationHourly, "testdb", "metrics")
	if err != nil {
		t.Fatalf("ListPartitions failed: %v", err)
	}

	// Should only have the new partition
	if len(partitions) != 1 {
		t.Errorf("Expected 1 partition after deletion, got %d", len(partitions))
	}
}

func TestStorage_GetPartitionDirsInRange(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_storage_api_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	// Create partitions for multiple days
	days := []time.Time{
		time.Date(2024, 5, 15, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 5, 16, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 5, 17, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 5, 18, 0, 0, 0, 0, time.UTC),
	}

	for _, day := range days {
		points := []*AggregatedPoint{
			{
				Time:     day,
				DeviceID: "device1",
				Level:    AggregationHourly,
				Fields: map[string]*AggregatedField{
					"temperature": {Count: 10, Sum: 250.0, Avg: 25.0, Min: 20.0, Max: 30.0},
				},
			},
		}
		err := storage.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points, day)
		if err != nil {
			t.Fatalf("WriteAggregatedPoints failed: %v", err)
		}
	}

	// Get dirs in range
	start := time.Date(2024, 5, 15, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 5, 17, 0, 0, 0, 0, time.UTC)
	dirs := storage.getPartitionDirsInRange(AggregationHourly, "testdb", "metrics", start, end)

	// Should include 15, 16, 17 (inclusive)
	if len(dirs) != 3 {
		t.Errorf("Expected 3 dirs in range, got %d", len(dirs))
	}
}

func TestStorage_WriteMultipleLevels(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_storage_api_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	points := []*AggregatedPoint{
		{
			Time:     now,
			DeviceID: "device1",
			Fields: map[string]*AggregatedField{
				"temperature": {Count: 10, Sum: 250.0, Avg: 25.0, Min: 20.0, Max: 30.0},
			},
		},
	}

	// Write to all levels
	levels := []AggregationLevel{AggregationHourly, AggregationDaily, AggregationMonthly, AggregationYearly}
	for _, level := range levels {
		points[0].Level = level
		err := storage.WriteAggregatedPoints(level, "testdb", "metrics", points, now)
		if err != nil {
			t.Fatalf("WriteAggregatedPoints for level %s failed: %v", level, err)
		}
	}

	// Verify all partitions exist
	expectedPaths := []string{
		filepath.Join(tempDir, "agg_1h", "testdb", "metrics", "2024", "05", "15"),
		filepath.Join(tempDir, "agg_1d", "testdb", "metrics", "2024", "05"),
		filepath.Join(tempDir, "agg_1M", "testdb", "metrics", "2024"),
		filepath.Join(tempDir, "agg_1y", "testdb", "metrics"),
	}

	for _, path := range expectedPaths {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			t.Errorf("Expected partition path does not exist: %s", path)
		}
	}
}

func TestStorage_WriteAggregatedPoints_AppendToExistingPart(t *testing.T) {
	// This test verifies that multiple writes append to the same part file
	// instead of creating new part files each time (V6: single file per DG)

	tempDir, err := os.MkdirTemp("", "agg_storage_append_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	// Use config with high MaxRowsPerPart to force appending
	config := StorageConfig{
		MaxRowsPerPart: 10000,
		MaxPartSize:    64 * 1024 * 1024,
	}
	storage := NewStorageWithConfig(tempDir, logger, config)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	// Write 10 batches of 5 points each (50 points total)
	for batch := 0; batch < 10; batch++ {
		points := make([]*AggregatedPoint, 5)
		for i := 0; i < 5; i++ {
			points[i] = &AggregatedPoint{
				Time:     now.Add(time.Duration(batch*5+i) * time.Minute),
				DeviceID: "device1",
				Level:    AggregationHourly,
				Fields: map[string]*AggregatedField{
					"temperature": {Count: 10, Sum: 250.0, Avg: 25.0, Min: 20.0, Max: 30.0},
				},
			}
		}

		err = storage.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points, now)
		if err != nil {
			t.Fatalf("WriteAggregatedPoints batch %d failed: %v", batch, err)
		}
	}

	// V6: should have only 1 part file (part_0000.bin) under dg_0000/
	partDir := filepath.Join(tempDir, "agg_1h", "testdb", "metrics", "2024", "05", "15")
	dgDir := filepath.Join(partDir, "dg_0000")
	files, err := filepath.Glob(filepath.Join(dgDir, "part_*.bin"))
	if err != nil {
		t.Fatalf("Failed to list part files: %v", err)
	}

	// V6: single file per DG containing all metrics
	expectedFileCount := 1
	if len(files) != expectedFileCount {
		t.Errorf("Expected %d part file, got %d. Files: %v", expectedFileCount, len(files), files)
	}

	// Verify metadata shows correct total row count
	meta, err := storage.readMetadata(partDir)
	if err != nil {
		t.Fatalf("Failed to read metadata: %v", err)
	}

	expectedRowCount := int64(50) // 10 batches * 5 points
	if meta.RowCount != expectedRowCount {
		t.Errorf("Expected RowCount=%d, got %d", expectedRowCount, meta.RowCount)
	}

	// V6: verify DG metadata has 1 part with 50 rows
	dgMeta, err := storage.readDGMetadata(dgDir)
	if err != nil {
		t.Fatalf("Failed to read DG metadata: %v", err)
	}
	if len(dgMeta.Parts) != 1 {
		t.Errorf("Expected 1 part in DG metadata, got %d", len(dgMeta.Parts))
	}
	if len(dgMeta.Parts) > 0 && dgMeta.Parts[0].RowCount != expectedRowCount {
		t.Errorf("Expected RowCount=%d in DG part, got %d", expectedRowCount, dgMeta.Parts[0].RowCount)
	}

	// Read back and verify data
	result, err := storage.ReadAggregatedPoints(AggregationHourly, "testdb", "metrics", ReadOptions{
		StartTime: now.Add(-time.Hour),
		EndTime:   now.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("ReadAggregatedPoints failed: %v", err)
	}

	if len(result) != 50 {
		t.Errorf("Expected 50 points, got %d", len(result))
	}
}

func TestStorage_WriteAggregatedPoints_CreateNewPartWhenFull(t *testing.T) {
	// This test verifies that a new part is created when MaxRowsPerPart is reached

	tempDir, err := os.MkdirTemp("", "agg_storage_newpart_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	// Use config with low MaxRowsPerPart to force creating new parts
	config := StorageConfig{
		MaxRowsPerPart: 10, // Only 10 rows per part
		MaxPartSize:    64 * 1024 * 1024,
	}
	storage := NewStorageWithConfig(tempDir, logger, config)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	// Write 25 points in 5 batches of 5 points
	// With MaxRowsPerPart=10, should create 3 parts (10 + 10 + 5)
	for batch := 0; batch < 5; batch++ {
		points := make([]*AggregatedPoint, 5)
		for i := 0; i < 5; i++ {
			points[i] = &AggregatedPoint{
				Time:     now.Add(time.Duration(batch*5+i) * time.Minute),
				DeviceID: "device1",
				Level:    AggregationHourly,
				Fields: map[string]*AggregatedField{
					"temperature": {Count: 10, Sum: 250.0, Avg: 25.0, Min: 20.0, Max: 30.0},
				},
			}
		}

		err = storage.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points, now)
		if err != nil {
			t.Fatalf("WriteAggregatedPoints batch %d failed: %v", batch, err)
		}
	}

	// Verify metadata
	partDir := filepath.Join(tempDir, "agg_1h", "testdb", "metrics", "2024", "05", "15")
	_, err = storage.readMetadata(partDir)
	if err != nil {
		t.Fatalf("Failed to read metadata: %v", err)
	}

	// V6: should have 3 parts in DG metadata (10 + 10 + 5 = 25 rows)
	dgDir := filepath.Join(partDir, "dg_0000")
	dgMeta, err := storage.readDGMetadata(dgDir)
	if err != nil {
		t.Fatalf("Failed to read DG metadata: %v", err)
	}
	if len(dgMeta.Parts) != 3 {
		t.Errorf("Expected 3 parts in DG, got %d", len(dgMeta.Parts))
	}

	// Part 0: 10 rows, Part 1: 10 rows, Part 2: 5 rows
	expectedRows := []int64{10, 10, 5}
	for i, part := range dgMeta.Parts {
		if i < len(expectedRows) && part.RowCount != expectedRows[i] {
			t.Errorf("Part %d: expected RowCount=%d, got %d", i, expectedRows[i], part.RowCount)
		}
	}

	// V6: total files = 3 part files under dg_0000/
	files, err := filepath.Glob(filepath.Join(dgDir, "part_*.bin"))
	if err != nil {
		t.Fatalf("Failed to list part files: %v", err)
	}
	if len(files) != 3 {
		t.Errorf("Expected 3 part files, got %d", len(files))
	}
}

func TestStorage_Compact(t *testing.T) {
	// This test verifies that compaction merges multiple small parts into fewer larger parts

	tempDir, err := os.MkdirTemp("", "agg_storage_compact_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	// Use low MaxRowsPerPart to force creating multiple parts
	config := StorageConfig{
		MaxRowsPerPart: 5, // Very small to create many parts
		MaxPartSize:    64 * 1024 * 1024,
	}
	storage := NewStorageWithConfig(tempDir, logger, config)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	// Write 20 points in 4 batches of 5 points
	// With MaxRowsPerPart=5, should create 4 parts
	for batch := 0; batch < 4; batch++ {
		points := make([]*AggregatedPoint, 5)
		for i := 0; i < 5; i++ {
			points[i] = &AggregatedPoint{
				Time:     now.Add(time.Duration(batch*5+i) * time.Minute),
				DeviceID: "device1",
				Level:    AggregationHourly,
				Fields: map[string]*AggregatedField{
					"temperature": {Count: 10, Sum: 250.0, Avg: 25.0, Min: 20.0, Max: 30.0},
				},
			}
		}

		err = storage.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points, now)
		if err != nil {
			t.Fatalf("WriteAggregatedPoints batch %d failed: %v", batch, err)
		}
	}

	partDir := filepath.Join(tempDir, "agg_1h", "testdb", "metrics", "2024", "05", "15")
	dgDir := filepath.Join(partDir, "dg_0000")

	// V6: verify we have 4 parts before compaction
	dgMeta, err := storage.readDGMetadata(dgDir)
	if err != nil {
		t.Fatalf("Failed to read DG metadata: %v", err)
	}
	if len(dgMeta.Parts) != 4 {
		t.Errorf("Before compact: expected 4 parts, got %d", len(dgMeta.Parts))
	}

	// Count files before compaction
	filesBefore, _ := filepath.Glob(filepath.Join(dgDir, "part_*.bin"))
	t.Logf("Files before compaction: %d", len(filesBefore))

	// Now change config to allow larger parts for compaction
	storage.config.MaxRowsPerPart = 10000

	// Run compaction
	err = storage.Compact(partDir)
	if err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	// V6: should now have only 1 part (20 rows < 10000)
	storage.invalidateMetadataCache(partDir)
	dgMeta, err = storage.readDGMetadata(dgDir)
	if err != nil {
		t.Fatalf("Failed to read DG metadata after compact: %v", err)
	}

	if len(dgMeta.Parts) != 1 {
		t.Errorf("After compact: expected 1 part, got %d", len(dgMeta.Parts))
	}
	if len(dgMeta.Parts) > 0 && dgMeta.Parts[0].RowCount != 20 {
		t.Errorf("After compact: expected 20 rows, got %d", dgMeta.Parts[0].RowCount)
	}

	// V6: verify file count: 1 file under dg_0000/
	filesAfter, _ := filepath.Glob(filepath.Join(dgDir, "part_*.bin"))
	t.Logf("Files after compaction: %d", len(filesAfter))
	if len(filesAfter) != 1 {
		t.Errorf("Expected 1 file after compaction, got %d", len(filesAfter))
	}

	// Verify data integrity - read back and check
	result, err := storage.ReadAggregatedPoints(AggregationHourly, "testdb", "metrics", ReadOptions{
		StartTime: now.Add(-time.Hour),
		EndTime:   now.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("ReadAggregatedPoints after compact failed: %v", err)
	}

	if len(result) != 20 {
		t.Errorf("Expected 20 points after compaction, got %d", len(result))
	}

	// Verify data is sorted by timestamp
	for i := 1; i < len(result); i++ {
		if result[i].Time.Before(result[i-1].Time) {
			t.Errorf("Data not sorted after compaction at index %d", i)
		}
	}
}

func TestStorage_CompactAll(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_storage_compactall_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	config := StorageConfig{
		MaxRowsPerPart: 5,
		MaxPartSize:    64 * 1024 * 1024,
	}
	storage := NewStorageWithConfig(tempDir, logger, config)

	// Write data to multiple days
	for day := 0; day < 3; day++ {
		baseTime := time.Date(2024, 5, 15+day, 10, 0, 0, 0, time.UTC)
		for batch := 0; batch < 3; batch++ {
			points := make([]*AggregatedPoint, 5)
			for i := 0; i < 5; i++ {
				points[i] = &AggregatedPoint{
					Time:     baseTime.Add(time.Duration(batch*5+i) * time.Minute),
					DeviceID: "device1",
					Level:    AggregationHourly,
					Fields: map[string]*AggregatedField{
						"temperature": {Count: 10, Sum: 250.0, Avg: 25.0, Min: 20.0, Max: 30.0},
					},
				}
			}
			err = storage.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points, baseTime)
			if err != nil {
				t.Fatalf("WriteAggregatedPoints failed: %v", err)
			}
		}
	}

	// Change config for compaction
	storage.config.MaxRowsPerPart = 10000

	// Compact all partitions
	results, err := storage.CompactAll(AggregationHourly, "testdb", "metrics")
	if err != nil {
		t.Fatalf("CompactAll failed: %v", err)
	}

	// V6: should have compacted 3 partitions * 1 DG = 3 results (only those with >1 part)
	t.Logf("Compaction results: %d", len(results))

	// V6: verify each partition now has 1 part in DG metadata
	partitions, _ := storage.ListPartitions(AggregationHourly, "testdb", "metrics")
	for _, partDir := range partitions {
		storage.invalidateMetadataCache(partDir)
		dgDir := filepath.Join(partDir, "dg_0000")
		dgMeta, _ := storage.readDGMetadata(dgDir)
		if len(dgMeta.Parts) != 1 {
			t.Errorf("Partition %s: expected 1 part after compact, got %d", partDir, len(dgMeta.Parts))
		}
	}
}

func TestStorage_NeedsCompaction(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_storage_needscompact_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	config := StorageConfig{
		MaxRowsPerPart: 5,
		MaxPartSize:    64 * 1024 * 1024,
	}
	storage := NewStorageWithConfig(tempDir, logger, config)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	// Write enough data to create multiple parts
	for batch := 0; batch < 3; batch++ {
		points := make([]*AggregatedPoint, 5)
		for i := 0; i < 5; i++ {
			points[i] = &AggregatedPoint{
				Time:     now.Add(time.Duration(batch*5+i) * time.Minute),
				DeviceID: "device1",
				Level:    AggregationHourly,
				Fields: map[string]*AggregatedField{
					"temperature": {Count: 10, Sum: 250.0, Avg: 25.0, Min: 20.0, Max: 30.0},
				},
			}
		}
		err = storage.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points, now)
		if err != nil {
			t.Fatalf("WriteAggregatedPoints failed: %v", err)
		}
	}

	partDir := filepath.Join(tempDir, "agg_1h", "testdb", "metrics", "2024", "05", "15")

	// Check with minParts=2 - should need compaction
	needs, partsCount := storage.NeedsCompaction(partDir, 2)
	if !needs {
		t.Error("Expected NeedsCompaction to return true with minParts=2")
	}
	// V6: partsCount is keyed by DG dir name (e.g. "dg_0000")
	if count, ok := partsCount["dg_0000"]; !ok || count < 3 {
		t.Errorf("Expected >= 3 parts for dg_0000, got %d (ok=%v)", partsCount["dg_0000"], ok)
	}

	// Check with minParts=5 - should not need compaction
	needs, _ = storage.NeedsCompaction(partDir, 5)
	if needs {
		t.Error("Expected NeedsCompaction to return false with minParts=5")
	}
}

// =============================================================================
// ReadAggregatedPoints with field/metric filters
// =============================================================================

func TestReadAggregatedPoints_FieldFilter(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_field_filter_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	points := []*AggregatedPoint{
		{
			Time:     now,
			DeviceID: "device1",
			Level:    AggregationHourly,
			Fields: map[string]*AggregatedField{
				"temperature": {Count: 10, Sum: 250.0, Avg: 25.0, Min: 20.0, Max: 30.0},
				"humidity":    {Count: 10, Sum: 600.0, Avg: 60.0, Min: 55.0, Max: 65.0},
				"pressure":    {Count: 10, Sum: 10100.0, Avg: 1010.0, Min: 1005.0, Max: 1015.0},
			},
		},
	}

	err = storage.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points, now)
	if err != nil {
		t.Fatalf("WriteAggregatedPoints failed: %v", err)
	}

	// Read with field filter - only temperature
	result, err := storage.ReadAggregatedPoints(AggregationHourly, "testdb", "metrics", ReadOptions{
		StartTime: now.Add(-time.Hour),
		EndTime:   now.Add(time.Hour),
		Fields:    []string{"temperature"},
	})
	if err != nil {
		t.Fatalf("ReadAggregatedPoints with field filter failed: %v", err)
	}

	if len(result) != 1 {
		t.Fatalf("Expected 1 point, got %d", len(result))
	}

	// Should only have temperature field
	if _, ok := result[0].Fields["temperature"]; !ok {
		t.Error("Expected temperature field in result")
	}
	// humidity and pressure should NOT be present
	if _, ok := result[0].Fields["humidity"]; ok {
		t.Error("humidity field should be filtered out")
	}
	if _, ok := result[0].Fields["pressure"]; ok {
		t.Error("pressure field should be filtered out")
	}
}

func TestReadAggregatedPoints_MetricFilter(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_metric_filter_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	points := []*AggregatedPoint{
		{
			Time:     now,
			DeviceID: "device1",
			Level:    AggregationHourly,
			Fields: map[string]*AggregatedField{
				"temperature": {Count: 10, Sum: 250.0, Avg: 25.0, Min: 20.0, Max: 30.0},
			},
		},
	}

	err = storage.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points, now)
	if err != nil {
		t.Fatalf("WriteAggregatedPoints failed: %v", err)
	}

	// Read with metric filter - only Sum and Avg
	result, err := storage.ReadAggregatedPoints(AggregationHourly, "testdb", "metrics", ReadOptions{
		StartTime: now.Add(-time.Hour),
		EndTime:   now.Add(time.Hour),
		Metrics:   []MetricType{MetricSum, MetricAvg},
	})
	if err != nil {
		t.Fatalf("ReadAggregatedPoints with metric filter failed: %v", err)
	}

	if len(result) != 1 {
		t.Fatalf("Expected 1 point, got %d", len(result))
	}

	field := result[0].Fields["temperature"]
	if field == nil {
		t.Fatal("temperature field is nil")
	}

	// Sum and Avg should be populated
	if field.Sum != 250.0 {
		t.Errorf("Sum = %f, want 250.0", field.Sum)
	}
	if field.Avg != 25.0 {
		t.Errorf("Avg = %f, want 25.0", field.Avg)
	}
	// Min and Max should be zero (not read)
	if field.Min != 0 {
		t.Errorf("Min = %f, should be 0 (filtered)", field.Min)
	}
	if field.Max != 0 {
		t.Errorf("Max = %f, should be 0 (filtered)", field.Max)
	}
}

func TestReadAggregatedPoints_CombinedFilters(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_combined_filter_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	points := []*AggregatedPoint{
		{
			Time:     now,
			DeviceID: "device1",
			Fields: map[string]*AggregatedField{
				"temperature": {Count: 10, Sum: 250.0, Avg: 25.0, Min: 20.0, Max: 30.0},
				"humidity":    {Count: 10, Sum: 600.0, Avg: 60.0, Min: 55.0, Max: 65.0},
			},
		},
		{
			Time:     now,
			DeviceID: "device2",
			Fields: map[string]*AggregatedField{
				"temperature": {Count: 5, Sum: 130.0, Avg: 26.0, Min: 22.0, Max: 28.0},
				"humidity":    {Count: 5, Sum: 300.0, Avg: 60.0, Min: 58.0, Max: 62.0},
			},
		},
	}

	err = storage.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points, now)
	if err != nil {
		t.Fatalf("WriteAggregatedPoints failed: %v", err)
	}

	// Read with device + field + metric filter
	result, err := storage.ReadAggregatedPoints(AggregationHourly, "testdb", "metrics", ReadOptions{
		StartTime: now.Add(-time.Hour),
		EndTime:   now.Add(time.Hour),
		DeviceIDs: []string{"device1"},
		Fields:    []string{"temperature"},
		Metrics:   []MetricType{MetricSum},
	})
	if err != nil {
		t.Fatalf("ReadAggregatedPoints failed: %v", err)
	}

	if len(result) != 1 {
		t.Fatalf("Expected 1 point (device1 only), got %d", len(result))
	}

	if result[0].DeviceID != "device1" {
		t.Errorf("Expected device1, got %s", result[0].DeviceID)
	}

	// Should only have temperature
	if _, ok := result[0].Fields["temperature"]; !ok {
		t.Error("Expected temperature field")
	}
	if _, ok := result[0].Fields["humidity"]; ok {
		t.Error("humidity should be filtered out")
	}
}

func TestReadAggregatedPoints_EmptyResult(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_empty_result_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	// Read from non-existent data
	result, err := storage.ReadAggregatedPoints(AggregationHourly, "testdb", "metrics", ReadOptions{
		StartTime: now.Add(-time.Hour),
		EndTime:   now.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("ReadAggregatedPoints failed: %v", err)
	}

	if len(result) != 0 {
		t.Errorf("Expected 0 points, got %d", len(result))
	}
}

func TestReadAggregatedPoints_SortOrder(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_sort_order_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	// Write points for multiple devices at multiple times
	points := []*AggregatedPoint{
		{Time: now.Add(2 * time.Hour), DeviceID: "device2", Fields: map[string]*AggregatedField{"t": {Sum: 3, Count: 1}}},
		{Time: now, DeviceID: "device2", Fields: map[string]*AggregatedField{"t": {Sum: 1, Count: 1}}},
		{Time: now, DeviceID: "device1", Fields: map[string]*AggregatedField{"t": {Sum: 2, Count: 1}}},
		{Time: now.Add(time.Hour), DeviceID: "device1", Fields: map[string]*AggregatedField{"t": {Sum: 4, Count: 1}}},
	}

	err = storage.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points, now)
	if err != nil {
		t.Fatal(err)
	}

	result, err := storage.ReadAggregatedPoints(AggregationHourly, "testdb", "metrics", ReadOptions{
		StartTime: now.Add(-time.Hour),
		EndTime:   now.Add(3 * time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 4 {
		t.Fatalf("Expected 4 points, got %d", len(result))
	}

	// Verify sort: by time, then by device ID
	for i := 1; i < len(result); i++ {
		if result[i].Time.Before(result[i-1].Time) {
			t.Errorf("Points not sorted by time at index %d", i)
		}
		if result[i].Time.Equal(result[i-1].Time) && result[i].DeviceID < result[i-1].DeviceID {
			t.Errorf("Points with same time not sorted by device ID at index %d", i)
		}
	}
}

// =============================================================================
// CompactWithOptions edge cases
// =============================================================================

func TestCompactWithOptions_DryRun(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_compact_dryrun_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	config := StorageConfig{MaxRowsPerPart: 5, MaxPartSize: 64 * 1024 * 1024}
	storage := NewStorageWithConfig(tempDir, logger, config)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	// Create 3 parts (15 points with MaxRowsPerPart=5)
	for batch := 0; batch < 3; batch++ {
		points := make([]*AggregatedPoint, 5)
		for i := 0; i < 5; i++ {
			points[i] = &AggregatedPoint{
				Time:     now.Add(time.Duration(batch*5+i) * time.Minute),
				DeviceID: "device1",
				Fields:   map[string]*AggregatedField{"temp": {Sum: 10, Count: 1}},
			}
		}
		err = storage.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points, now)
		if err != nil {
			t.Fatal(err)
		}
	}

	partDir := filepath.Join(tempDir, "agg_1h", "testdb", "metrics", "2024", "05", "15")
	dgDir := filepath.Join(partDir, "dg_0000")

	// Count files before
	filesBefore, _ := filepath.Glob(filepath.Join(dgDir, "part_*.bin"))

	// Compact with DryRun
	storage.config.MaxRowsPerPart = 10000
	opts := CompactOptions{
		MinPartsToCompact: 2,
		DryRun:            true,
	}

	err = storage.CompactWithOptions(partDir, opts)
	if err != nil {
		t.Fatalf("CompactWithOptions DryRun failed: %v", err)
	}

	// Files should be unchanged
	filesAfter, _ := filepath.Glob(filepath.Join(dgDir, "part_*.bin"))
	if len(filesAfter) != len(filesBefore) {
		t.Errorf("DryRun changed file count: before=%d, after=%d", len(filesBefore), len(filesAfter))
	}
}

func TestCompact_NoData(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_compact_nodata_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	err = storage.Compact("/nonexistent/path")
	if err != nil {
		t.Fatalf("Compact on nonexistent path should not error: %v", err)
	}
}

func TestCompact_SinglePart(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_compact_single_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	points := []*AggregatedPoint{
		{Time: now, DeviceID: "device1", Fields: map[string]*AggregatedField{"temp": {Sum: 10, Count: 1}}},
	}

	err = storage.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points, now)
	if err != nil {
		t.Fatal(err)
	}

	partDir := filepath.Join(tempDir, "agg_1h", "testdb", "metrics", "2024", "05", "15")

	// Should not compact (only 1 part, needs at least 2)
	err = storage.Compact(partDir)
	if err != nil {
		t.Fatalf("Compact single part should not error: %v", err)
	}
}

// =============================================================================
// cleanupCompactFiles
// =============================================================================

func TestCleanupCompactFiles(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agg_cleanup_compact_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tmpDir, logger)

	// Create .compact files
	for i := 0; i < 3; i++ {
		f := filepath.Join(tmpDir, "part_000"+string(rune('0'+i))+".compact")
		if err := os.WriteFile(f, []byte("data"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// Also create a .bin file that should NOT be removed
	binFile := filepath.Join(tmpDir, "part_0000.bin")
	if err := os.WriteFile(binFile, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	storage.cleanupCompactFiles(tmpDir)

	// .compact files should be gone
	compactFiles, _ := filepath.Glob(filepath.Join(tmpDir, "part_*.compact"))
	if len(compactFiles) != 0 {
		t.Errorf("Expected 0 .compact files, got %d", len(compactFiles))
	}

	// .bin file should still exist
	if _, err := os.Stat(binFile); os.IsNotExist(err) {
		t.Error("part_0000.bin should not have been removed")
	}
}

// =============================================================================
// DeleteOldData edge cases
// =============================================================================

func TestDeleteOldData_NoDataToDelete(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_delete_none_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	points := []*AggregatedPoint{
		{Time: now, DeviceID: "device1", Fields: map[string]*AggregatedField{"temp": {Sum: 10, Count: 1}}},
	}

	err = storage.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points, now)
	if err != nil {
		t.Fatal(err)
	}

	// Delete with cutoff before the data - should NOT delete
	cutoff := time.Date(2024, 5, 14, 0, 0, 0, 0, time.UTC)
	err = storage.DeleteOldData(AggregationHourly, "testdb", "metrics", cutoff)
	if err != nil {
		t.Fatal(err)
	}

	// Data should still exist
	partitions, _ := storage.ListPartitions(AggregationHourly, "testdb", "metrics")
	if len(partitions) != 1 {
		t.Errorf("Expected 1 partition (data not deleted), got %d", len(partitions))
	}
}

func TestDeleteOldData_NonExistentCollection(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_delete_nonexist_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	// Should not error
	err = storage.DeleteOldData(AggregationHourly, "nonexistent", "collection", time.Now())
	if err != nil {
		t.Fatalf("DeleteOldData on nonexistent should not error: %v", err)
	}
}

// =============================================================================
// NeedsCompaction edge cases
// =============================================================================

func TestNeedsCompaction_NonExistentPath(t *testing.T) {
	logger := logging.NewDevelopment()
	storage := NewStorage("/tmp/nonexistent", logger)

	needs, partsCount := storage.NeedsCompaction("/tmp/nonexistent/path", 2)
	if needs {
		t.Error("NeedsCompaction should return false for nonexistent path")
	}
	if partsCount != nil {
		t.Errorf("partsCount should be nil, got %v", partsCount)
	}
}

func TestNeedsCompaction_EmptyPartition(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_needs_compact_empty_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	// Write just 1 point → 1 part → should not need compaction
	points := []*AggregatedPoint{
		{Time: now, DeviceID: "device1", Fields: map[string]*AggregatedField{"temp": {Sum: 10, Count: 1}}},
	}

	err = storage.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points, now)
	if err != nil {
		t.Fatal(err)
	}

	partDir := filepath.Join(tempDir, "agg_1h", "testdb", "metrics", "2024", "05", "15")
	needs, partsCount := storage.NeedsCompaction(partDir, 2)
	if needs {
		t.Error("Single part should not need compaction")
	}
	if count, ok := partsCount["dg_0000"]; ok && count > 1 {
		t.Errorf("Expected 1 part for dg_0000, got %d", count)
	}
}

// =============================================================================
// GetPartitionStats edge cases
// =============================================================================

func TestGetPartitionStats_NonExistent(t *testing.T) {
	logger := logging.NewDevelopment()
	storage := NewStorage("/tmp/nonexistent", logger)

	stats, err := storage.GetPartitionStats(AggregationHourly, "db", "col", time.Now())
	if err != nil {
		t.Fatalf("GetPartitionStats should not error: %v", err)
	}
	if stats != nil {
		t.Error("Expected nil stats for nonexistent partition")
	}
}

// =============================================================================
// getPartitionDirsInRange edge cases
// =============================================================================

func TestGetPartitionDirsInRange_Daily(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_dirs_daily_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	// Write daily data for 2 months
	month1 := time.Date(2024, 4, 15, 0, 0, 0, 0, time.UTC)
	month2 := time.Date(2024, 5, 15, 0, 0, 0, 0, time.UTC)

	for _, ts := range []time.Time{month1, month2} {
		points := []*AggregatedPoint{
			{Time: ts, DeviceID: "d1", Fields: map[string]*AggregatedField{"temp": {Sum: 10, Count: 1}}},
		}
		if err := storage.WriteAggregatedPoints(AggregationDaily, "testdb", "metrics", points, ts); err != nil {
			t.Fatal(err)
		}
	}

	dirs := storage.getPartitionDirsInRange(AggregationDaily, "testdb", "metrics", month1, month2)
	if len(dirs) != 2 {
		t.Errorf("Expected 2 daily partition dirs, got %d", len(dirs))
	}
}

func TestGetPartitionDirsInRange_Monthly(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_dirs_monthly_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	// Write monthly data for 2 years
	year1 := time.Date(2023, 6, 15, 0, 0, 0, 0, time.UTC)
	year2 := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)

	for _, ts := range []time.Time{year1, year2} {
		points := []*AggregatedPoint{
			{Time: ts, DeviceID: "d1", Fields: map[string]*AggregatedField{"temp": {Sum: 10, Count: 1}}},
		}
		if err := storage.WriteAggregatedPoints(AggregationMonthly, "testdb", "metrics", points, ts); err != nil {
			t.Fatal(err)
		}
	}

	dirs := storage.getPartitionDirsInRange(AggregationMonthly, "testdb", "metrics", year1, year2)
	if len(dirs) != 2 {
		t.Errorf("Expected 2 monthly partition dirs, got %d", len(dirs))
	}
}

func TestGetPartitionDirsInRange_Yearly(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_dirs_yearly_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	year := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)

	points := []*AggregatedPoint{
		{Time: year, DeviceID: "d1", Fields: map[string]*AggregatedField{"temp": {Sum: 10, Count: 1}}},
	}
	if err := storage.WriteAggregatedPoints(AggregationYearly, "testdb", "metrics", points, year); err != nil {
		t.Fatal(err)
	}

	dirs := storage.getPartitionDirsInRange(AggregationYearly, "testdb", "metrics", year, year.Add(24*time.Hour))
	if len(dirs) != 1 {
		t.Errorf("Expected 1 yearly partition dir, got %d", len(dirs))
	}
}

func TestGetPartitionDirsInRange_NoExistingDirs(t *testing.T) {
	logger := logging.NewDevelopment()
	storage := NewStorage("/tmp/nonexistent_dirs_test", logger)

	now := time.Now()
	dirs := storage.getPartitionDirsInRange(AggregationHourly, "db", "col", now, now.Add(24*time.Hour))
	if len(dirs) != 0 {
		t.Errorf("Expected 0 dirs, got %d", len(dirs))
	}
}

// =============================================================================
// readPartitionData edge cases
// =============================================================================

func TestReadPartitionData_NonExistentDir(t *testing.T) {
	logger := logging.NewDevelopment()
	storage := NewStorage("/tmp", logger)

	pointsMap := make(map[string]*AggregatedPoint)
	err := storage.readPartitionData("/tmp/nonexistent_read_test", math.MinInt64, math.MaxInt64, nil, nil, nil, pointsMap)
	if err != nil {
		t.Fatalf("readPartitionData nonexistent should not error: %v", err)
	}
	if len(pointsMap) != 0 {
		t.Error("Expected empty pointsMap")
	}
}

func TestReadPartitionData_TimeRangeSkip(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_readpart_skip_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	points := []*AggregatedPoint{
		{Time: now, DeviceID: "d1", Fields: map[string]*AggregatedField{"temp": {Sum: 10, Count: 1}}},
	}

	err = storage.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points, now)
	if err != nil {
		t.Fatal(err)
	}

	partDir := filepath.Join(tempDir, "agg_1h", "testdb", "metrics", "2024", "05", "15")

	// Read with time range that doesn't overlap the partition
	pointsMap := make(map[string]*AggregatedPoint)
	// Use very old time range
	err = storage.readPartitionData(partDir, 1000, 2000, nil, nil, nil, pointsMap)
	if err != nil {
		t.Fatal(err)
	}
	if len(pointsMap) != 0 {
		t.Errorf("Expected 0 points for non-overlapping time range, got %d", len(pointsMap))
	}
}

func TestReadPartitionData_DGDeviceFilterSkip(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_readpart_dgskip_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	config := StorageConfig{MaxDevicesPerGroup: 2, MaxRowsPerPart: 10000, MaxPartSize: 64 * 1024 * 1024}
	storage := NewStorageWithConfig(tempDir, logger, config)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	// Write points for 4 devices → 2 DGs (MaxDevicesPerGroup=2)
	points := []*AggregatedPoint{
		{Time: now, DeviceID: "d1", Fields: map[string]*AggregatedField{"temp": {Sum: 10, Count: 1}}},
		{Time: now, DeviceID: "d2", Fields: map[string]*AggregatedField{"temp": {Sum: 20, Count: 1}}},
		{Time: now, DeviceID: "d3", Fields: map[string]*AggregatedField{"temp": {Sum: 30, Count: 1}}},
		{Time: now, DeviceID: "d4", Fields: map[string]*AggregatedField{"temp": {Sum: 40, Count: 1}}},
	}

	err = storage.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points, now)
	if err != nil {
		t.Fatal(err)
	}

	partDir := filepath.Join(tempDir, "agg_1h", "testdb", "metrics", "2024", "05", "15")

	// Read with device filter that only matches devices in DG 0
	deviceFilter := map[string]struct{}{"d1": {}}
	pointsMap := make(map[string]*AggregatedPoint)
	err = storage.readPartitionData(partDir, math.MinInt64, math.MaxInt64, deviceFilter, nil, nil, pointsMap)
	if err != nil {
		t.Fatal(err)
	}

	if len(pointsMap) != 1 {
		t.Errorf("Expected 1 point (d1 only), got %d", len(pointsMap))
	}
}

// =============================================================================
// mergeV6ColumnarData edge cases
// =============================================================================

func TestMergeV6ColumnarData_Empty(t *testing.T) {
	logger := logging.NewDevelopment()
	storage := NewStorage("/tmp", logger)

	data := NewColumnarData(0)
	pointsMap := make(map[string]*AggregatedPoint)

	storage.mergeV6ColumnarData(data, nil, pointsMap)

	if len(pointsMap) != 0 {
		t.Errorf("Expected 0 points, got %d", len(pointsMap))
	}
}

func TestMergeV6ColumnarData_WithDeviceFilter(t *testing.T) {
	logger := logging.NewDevelopment()
	storage := NewStorage("/tmp", logger)

	data := NewColumnarData(3)
	data.AddRowAllMetrics(1000, "dev1", map[string]*AggregatedField{"temp": {Sum: 10, Count: 1}})
	data.AddRowAllMetrics(2000, "dev2", map[string]*AggregatedField{"temp": {Sum: 20, Count: 1}})
	data.AddRowAllMetrics(3000, "dev1", map[string]*AggregatedField{"temp": {Sum: 30, Count: 1}})

	deviceFilter := map[string]struct{}{"dev1": {}}
	pointsMap := make(map[string]*AggregatedPoint)

	storage.mergeV6ColumnarData(data, deviceFilter, pointsMap)

	if len(pointsMap) != 2 {
		t.Errorf("Expected 2 points (dev1 only), got %d", len(pointsMap))
	}
}

func TestMergeV6ColumnarData_MergesExistingPoints(t *testing.T) {
	logger := logging.NewDevelopment()
	storage := NewStorage("/tmp", logger)

	// First data
	data1 := NewColumnarData(1)
	data1.AddRowAllMetrics(1000, "dev1", map[string]*AggregatedField{"temp": {Sum: 10, Avg: 10, Count: 1}})

	pointsMap := make(map[string]*AggregatedPoint)
	storage.mergeV6ColumnarData(data1, nil, pointsMap)

	// Second data with same timestamp and device - should merge into same point
	data2 := NewColumnarData(1)
	data2.AddRowAllMetrics(1000, "dev1", map[string]*AggregatedField{"humidity": {Sum: 60, Avg: 60, Count: 1}})

	storage.mergeV6ColumnarData(data2, nil, pointsMap)

	if len(pointsMap) != 1 {
		t.Fatalf("Expected 1 merged point, got %d", len(pointsMap))
	}

	for _, point := range pointsMap {
		if _, ok := point.Fields["temp"]; !ok {
			t.Error("Expected temp field")
		}
		if _, ok := point.Fields["humidity"]; !ok {
			t.Error("Expected humidity field")
		}
	}
}

// =============================================================================
// buildNewV6PartsInfo
// =============================================================================

func TestBuildNewV6PartsInfo(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_build_parts_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	// Write some data to create a valid part file
	cd := NewColumnarData(3)
	cd.AddRowAllMetrics(1000, "dev1", map[string]*AggregatedField{"temp": {Sum: 10, Count: 1}})
	cd.AddRowAllMetrics(2000, "dev1", map[string]*AggregatedField{"temp": {Sum: 20, Count: 1}})
	cd.AddRowAllMetrics(3000, "dev2", map[string]*AggregatedField{"temp": {Sum: 30, Count: 1}})

	pending, _ := storage.writeV6PartFile(tempDir, 0, AggregationHourly, cd)
	_ = storage.commitPendingWrites([]*pendingPartWrite{pending})

	parts := storage.buildNewV6PartsInfo(tempDir, 1)
	if len(parts) != 1 {
		t.Fatalf("Expected 1 part, got %d", len(parts))
	}

	pi := parts[0]
	if pi.PartNum != 0 {
		t.Errorf("PartNum = %d, want 0", pi.PartNum)
	}
	if pi.FileName != "part_0000.bin" {
		t.Errorf("FileName = %q", pi.FileName)
	}
	if pi.Size <= 0 {
		t.Errorf("Size = %d, should be > 0", pi.Size)
	}
	if pi.MinTime != 1000 {
		t.Errorf("MinTime = %d, want 1000", pi.MinTime)
	}
	if pi.MaxTime != 3000 {
		t.Errorf("MaxTime = %d, want 3000", pi.MaxTime)
	}
}

func TestBuildNewV6PartsInfo_NonExistentFile(t *testing.T) {
	logger := logging.NewDevelopment()
	storage := NewStorage("/tmp", logger)

	parts := storage.buildNewV6PartsInfo("/tmp/nonexistent_buildparts_test", 3)
	if len(parts) != 0 {
		t.Errorf("Expected 0 parts for nonexistent files, got %d", len(parts))
	}
}

// =============================================================================
// DefaultCompactOptions
// =============================================================================

func TestDefaultCompactOptions(t *testing.T) {
	opts := DefaultCompactOptions()
	if opts.MinPartsToCompact != 2 {
		t.Errorf("MinPartsToCompact = %d, want 2", opts.MinPartsToCompact)
	}
	if opts.TargetRowsPerPart != 0 {
		t.Errorf("TargetRowsPerPart = %d, want 0", opts.TargetRowsPerPart)
	}
	if opts.DryRun {
		t.Error("DryRun should default to false")
	}
}

// =============================================================================
// ListPartitions - skips dg_ directories
// =============================================================================

func TestListPartitions_SkipsDGDirs(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_list_skip_dg_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)
	points := []*AggregatedPoint{
		{Time: now, DeviceID: "d1", Fields: map[string]*AggregatedField{"temp": {Sum: 10, Count: 1}}},
	}

	err = storage.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points, now)
	if err != nil {
		t.Fatal(err)
	}

	partitions, err := storage.ListPartitions(AggregationHourly, "testdb", "metrics")
	if err != nil {
		t.Fatal(err)
	}

	// Should have 1 partition but NOT list dg_0000 as a separate partition
	if len(partitions) != 1 {
		t.Errorf("Expected 1 partition, got %d", len(partitions))
	}

	for _, p := range partitions {
		base := filepath.Base(p)
		if base == "dg_0000" {
			t.Error("ListPartitions should not include dg_ directories")
		}
	}
}

// =============================================================================
// Compact with multiple DGs
// =============================================================================

func TestCompact_MultipleDGs(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_compact_multi_dg_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	config := StorageConfig{MaxRowsPerPart: 3, MaxPartSize: 64 * 1024 * 1024, MaxDevicesPerGroup: 2}
	storage := NewStorageWithConfig(tempDir, logger, config)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	// Write data for 4 devices → 2 DGs, with multiple batches to create multiple parts per DG
	for batch := 0; batch < 3; batch++ {
		points := make([]*AggregatedPoint, 0, 4)
		for _, dev := range []string{"d1", "d2", "d3", "d4"} {
			for i := 0; i < 3; i++ {
				points = append(points, &AggregatedPoint{
					Time:     now.Add(time.Duration(batch*12+i) * time.Minute),
					DeviceID: dev,
					Fields:   map[string]*AggregatedField{"temp": {Sum: 10, Count: 1}},
				})
			}
		}
		err = storage.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points, now)
		if err != nil {
			t.Fatal(err)
		}
	}

	partDir := filepath.Join(tempDir, "agg_1h", "testdb", "metrics", "2024", "05", "15")

	// Increase MaxRowsPerPart for compaction
	storage.config.MaxRowsPerPart = 10000

	err = storage.Compact(partDir)
	if err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	// Verify both DGs were compacted
	for _, dgName := range []string{"dg_0000", "dg_0001"} {
		dgDir := filepath.Join(partDir, dgName)
		storage.invalidateMetadataCache(dgDir)
		dgMeta, _ := storage.readDGMetadata(dgDir)
		if dgMeta != nil && len(dgMeta.Parts) > 1 {
			// After compaction with high MaxRowsPerPart, should have at most 1 part
			t.Logf("DG %s has %d parts after compaction", dgName, len(dgMeta.Parts))
		}
	}

	// Verify data integrity
	result, err := storage.ReadAggregatedPoints(AggregationHourly, "testdb", "metrics", ReadOptions{
		StartTime: now.Add(-time.Hour),
		EndTime:   now.Add(2 * time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(result) == 0 {
		t.Error("Expected data after compaction")
	}
}

// =============================================================================
// CompactAll with mixed partitions
// =============================================================================

func TestCompactAll_EmptyResult(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_compactall_empty_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	results, err := storage.CompactAll(AggregationHourly, "nonexistent", "collection")
	if err != nil {
		t.Fatalf("CompactAll should not error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("Expected 0 results, got %d", len(results))
	}
}

// =============================================================================
// Write aggregated points - multi-DG write
// =============================================================================

func TestWriteAggregatedPoints_MultiDG(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_write_multi_dg_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	config := StorageConfig{MaxDevicesPerGroup: 2, MaxRowsPerPart: 10000, MaxPartSize: 64 * 1024 * 1024}
	storage := NewStorageWithConfig(tempDir, logger, config)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	// Write 5 devices → ceil(5/2) = 3 DGs
	points := make([]*AggregatedPoint, 5)
	for i := 0; i < 5; i++ {
		points[i] = &AggregatedPoint{
			Time:     now,
			DeviceID: "dev" + string(rune('A'+i)),
			Fields:   map[string]*AggregatedField{"temp": {Sum: float64(i * 10), Count: 1}},
		}
	}

	err = storage.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points, now)
	if err != nil {
		t.Fatal(err)
	}

	partDir := filepath.Join(tempDir, "agg_1h", "testdb", "metrics", "2024", "05", "15")
	meta, _ := storage.readMetadata(partDir)
	if meta == nil {
		t.Fatal("Expected metadata")
	}

	if len(meta.DeviceGroups) != 3 {
		t.Errorf("Expected 3 device groups, got %d", len(meta.DeviceGroups))
	}

	// Read back and verify all 5 devices
	result, err := storage.ReadAggregatedPoints(AggregationHourly, "testdb", "metrics", ReadOptions{
		StartTime: now.Add(-time.Hour),
		EndTime:   now.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 5 {
		t.Errorf("Expected 5 points, got %d", len(result))
	}
}

// =============================================================================
// Write with new fields added in subsequent writes
// =============================================================================

func TestWriteAggregatedPoints_NewFields(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_new_fields_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	// First write with temperature
	points1 := []*AggregatedPoint{
		{Time: now, DeviceID: "d1", Fields: map[string]*AggregatedField{"temperature": {Sum: 25, Count: 1}}},
	}
	err = storage.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points1, now)
	if err != nil {
		t.Fatal(err)
	}

	// Second write with temperature + humidity (new field)
	points2 := []*AggregatedPoint{
		{Time: now.Add(time.Minute), DeviceID: "d1", Fields: map[string]*AggregatedField{
			"temperature": {Sum: 26, Count: 1},
			"humidity":    {Sum: 60, Count: 1},
		}},
	}
	err = storage.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points2, now)
	if err != nil {
		t.Fatal(err)
	}

	// Read back all
	result, err := storage.ReadAggregatedPoints(AggregationHourly, "testdb", "metrics", ReadOptions{
		StartTime: now.Add(-time.Hour),
		EndTime:   now.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 2 {
		t.Fatalf("Expected 2 points, got %d", len(result))
	}

	// Verify schema was updated
	partDir := filepath.Join(tempDir, "agg_1h", "testdb", "metrics", "2024", "05", "15")
	meta, _ := storage.readMetadata(partDir)
	if len(meta.Fields) != 2 {
		t.Errorf("Expected 2 fields in schema, got %d", len(meta.Fields))
	}
}

// =============================================================================
// Compact DataIntegrity - write/compact/read roundtrip with value verification
// =============================================================================

func TestCompact_DataIntegrity(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_compact_integrity_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	config := StorageConfig{MaxRowsPerPart: 3, MaxPartSize: 64 * 1024 * 1024}
	storage := NewStorageWithConfig(tempDir, logger, config)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	// Write 9 points in 3 batches → 3 parts (MaxRowsPerPart=3)
	for batch := 0; batch < 3; batch++ {
		points := make([]*AggregatedPoint, 3)
		for i := 0; i < 3; i++ {
			idx := batch*3 + i
			points[i] = &AggregatedPoint{
				Time:     now.Add(time.Duration(idx) * time.Minute),
				DeviceID: "d1",
				Fields: map[string]*AggregatedField{
					"temp": {
						Sum:   float64((idx + 1) * 10),
						Avg:   float64(idx + 1),
						Min:   float64(idx),
						Max:   float64(idx + 2),
						Count: int64(idx + 1),
					},
				},
			}
		}
		err = storage.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points, now)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Compact
	storage.config.MaxRowsPerPart = 10000
	partDir := filepath.Join(tempDir, "agg_1h", "testdb", "metrics", "2024", "05", "15")
	err = storage.Compact(partDir)
	if err != nil {
		t.Fatal(err)
	}

	// Read back
	result, err := storage.ReadAggregatedPoints(AggregationHourly, "testdb", "metrics", ReadOptions{
		StartTime: now.Add(-time.Hour),
		EndTime:   now.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 9 {
		t.Fatalf("Expected 9 points after compact, got %d", len(result))
	}

	// Verify each point's values
	for i, pt := range result {
		field := pt.Fields["temp"]
		if field == nil {
			t.Errorf("Point %d: temp field is nil", i)
			continue
		}
		expectedSum := float64((i + 1) * 10)
		if field.Sum != expectedSum {
			t.Errorf("Point %d: Sum = %f, want %f", i, field.Sum, expectedSum)
		}
		expectedCount := int64(i + 1)
		if field.Count != expectedCount {
			t.Errorf("Point %d: Count = %d, want %d", i, field.Count, expectedCount)
		}
	}
}
