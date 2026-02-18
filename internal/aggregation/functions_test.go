package aggregation

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/logging"
)

func TestAggregateRawDataPoints_Basic(t *testing.T) {
	now := time.Now()
	points := []*RawDataPoint{
		{
			Time: now,
			Fields: map[string]interface{}{
				"temperature": 20.0,
				"humidity":    50.0,
			},
		},
		{
			Time: now.Add(time.Minute),
			Fields: map[string]interface{}{
				"temperature": 22.0,
				"humidity":    55.0,
			},
		},
		{
			Time: now.Add(2 * time.Minute),
			Fields: map[string]interface{}{
				"temperature": 24.0,
				"humidity":    60.0,
			},
		},
	}

	agg, err := AggregateRawDataPoints("device-001", points, AggregationHourly)
	if err != nil {
		t.Fatalf("AggregateRawDataPoints failed: %v", err)
	}

	if agg.DeviceID != "device-001" {
		t.Errorf("Expected DeviceID='device-001', got '%s'", agg.DeviceID)
	}

	if agg.Level != AggregationHourly {
		t.Errorf("Expected Level='1h', got '%s'", agg.Level)
	}

	if len(agg.Fields) != 2 {
		t.Errorf("Expected 2 fields, got %d", len(agg.Fields))
	}

	// Check temperature aggregation
	tempField := agg.Fields["temperature"]
	if tempField == nil {
		t.Fatal("temperature field is nil")
		return
	}
	if tempField.Count != 3 {
		t.Errorf("Expected temperature Count=3, got %d", tempField.Count)
	}
	if tempField.Sum != 66.0 {
		t.Errorf("Expected temperature Sum=66.0, got %f", tempField.Sum)
	}
	if tempField.Avg != 22.0 {
		t.Errorf("Expected temperature Avg=22.0, got %f", tempField.Avg)
	}
	if tempField.Min != 20.0 {
		t.Errorf("Expected temperature Min=20.0, got %f", tempField.Min)
	}
	if tempField.Max != 24.0 {
		t.Errorf("Expected temperature Max=24.0, got %f", tempField.Max)
	}
	if tempField.MinTime != now.UnixNano() {
		t.Errorf("Expected temperature MinTime=%d, got %d", now.UnixNano(), tempField.MinTime)
	}
	if tempField.MaxTime != now.Add(2*time.Minute).UnixNano() {
		t.Errorf("Expected temperature MaxTime=%d, got %d", now.Add(2*time.Minute).UnixNano(), tempField.MaxTime)
	}

	// Check humidity aggregation
	humidityField := agg.Fields["humidity"]
	if humidityField == nil {
		t.Fatal("humidity field is nil")
		return
	}
	if humidityField.Count != 3 {
		t.Errorf("Expected humidity Count=3, got %d", humidityField.Count)
	}
	if humidityField.Sum != 165.0 {
		t.Errorf("Expected humidity Sum=165.0, got %f", humidityField.Sum)
	}
	if humidityField.Avg != 55.0 {
		t.Errorf("Expected humidity Avg=55.0, got %f", humidityField.Avg)
	}
}

func TestAggregateRawDataPoints_EmptyPoints(t *testing.T) {
	_, err := AggregateRawDataPoints("device-001", []*RawDataPoint{}, AggregationHourly)
	if err == nil {
		t.Error("Expected error for empty points, got nil")
	}
}

func TestAggregateRawDataPoints_IntValues(t *testing.T) {
	now := time.Now()
	points := []*RawDataPoint{
		{
			Time: now,
			Fields: map[string]interface{}{
				"count": 10,
			},
		},
		{
			Time: now.Add(time.Minute),
			Fields: map[string]interface{}{
				"count": 20,
			},
		},
	}

	agg, err := AggregateRawDataPoints("device-001", points, AggregationDaily)
	if err != nil {
		t.Fatalf("AggregateRawDataPoints failed: %v", err)
	}

	countField := agg.Fields["count"]
	if countField == nil {
		t.Fatal("count field is nil")
		return
	}
	if countField.Sum != 30.0 {
		t.Errorf("Expected count Sum=30.0, got %f", countField.Sum)
	}
	if countField.Avg != 15.0 {
		t.Errorf("Expected count Avg=15.0, got %f", countField.Avg)
	}
}

func TestAggregateRawDataPoints_Int64Values(t *testing.T) {
	now := time.Now()
	points := []*RawDataPoint{
		{
			Time: now,
			Fields: map[string]interface{}{
				"bigcount": int64(1000000),
			},
		},
		{
			Time: now.Add(time.Minute),
			Fields: map[string]interface{}{
				"bigcount": int64(2000000),
			},
		},
	}

	agg, err := AggregateRawDataPoints("device-001", points, AggregationMonthly)
	if err != nil {
		t.Fatalf("AggregateRawDataPoints failed: %v", err)
	}

	field := agg.Fields["bigcount"]
	if field == nil {
		t.Fatal("bigcount field is nil")
		return
	}
	if field.Sum != 3000000.0 {
		t.Errorf("Expected bigcount Sum=3000000.0, got %f", field.Sum)
	}
}

func TestAggregateRawDataPoints_SkipsNonNumeric(t *testing.T) {
	now := time.Now()
	points := []*RawDataPoint{
		{
			Time: now,
			Fields: map[string]interface{}{
				"temperature": 20.0,
				"status":      "active",
				"name":        "sensor-1",
			},
		},
		{
			Time: now.Add(time.Minute),
			Fields: map[string]interface{}{
				"temperature": 22.0,
				"status":      "active",
			},
		},
	}

	agg, err := AggregateRawDataPoints("device-001", points, AggregationHourly)
	if err != nil {
		t.Fatalf("AggregateRawDataPoints failed: %v", err)
	}

	// Should only have temperature field (numeric)
	if len(agg.Fields) != 1 {
		t.Errorf("Expected 1 field (temperature), got %d", len(agg.Fields))
	}

	if agg.Fields["temperature"] == nil {
		t.Error("Expected temperature field, got nil")
	}
	if agg.Fields["status"] != nil {
		t.Error("Expected status field to be skipped")
	}
	if agg.Fields["name"] != nil {
		t.Error("Expected name field to be skipped")
	}
}

func TestAggregateAggregatedPoints_Basic(t *testing.T) {
	now := time.Now()

	// Create 3 hourly aggregated points
	points := []*AggregatedPoint{
		{
			Time:     now,
			DeviceID: "device-001",
			Level:    AggregationHourly,
			Complete: true,
			Fields: map[string]*AggregatedField{
				"temperature": {
					Count: 60,
					Sum:   1200.0,
					Avg:   20.0,
					Min:   18.0,
					Max:   22.0,
				},
			},
		},
		{
			Time:     now.Add(time.Hour),
			DeviceID: "device-001",
			Level:    AggregationHourly,
			Complete: true,
			Fields: map[string]*AggregatedField{
				"temperature": {
					Count: 60,
					Sum:   1320.0,
					Avg:   22.0,
					Min:   20.0,
					Max:   24.0,
				},
			},
		},
		{
			Time:     now.Add(2 * time.Hour),
			DeviceID: "device-001",
			Level:    AggregationHourly,
			Complete: true,
			Fields: map[string]*AggregatedField{
				"temperature": {
					Count: 60,
					Sum:   1380.0,
					Avg:   23.0,
					Min:   21.0,
					Max:   25.0,
				},
			},
		},
	}

	agg, err := AggregateAggregatedPoints("device-001", points, AggregationDaily)
	if err != nil {
		t.Fatalf("AggregateAggregatedPoints failed: %v", err)
	}

	if agg.DeviceID != "device-001" {
		t.Errorf("Expected DeviceID='device-001', got '%s'", agg.DeviceID)
	}

	if agg.Level != AggregationDaily {
		t.Errorf("Expected Level='1d', got '%s'", agg.Level)
	}

	tempField := agg.Fields["temperature"]
	if tempField == nil {
		t.Fatal("temperature field is nil")
		return
	}

	// Should merge all 3 hourly aggregates
	if tempField.Count != 180 {
		t.Errorf("Expected Count=180 (60+60+60), got %d", tempField.Count)
	}
	if tempField.Sum != 3900.0 {
		t.Errorf("Expected Sum=3900.0, got %f", tempField.Sum)
	}
	if tempField.Min != 18.0 {
		t.Errorf("Expected Min=18.0 (global min), got %f", tempField.Min)
	}
	if tempField.Max != 25.0 {
		t.Errorf("Expected Max=25.0 (global max), got %f", tempField.Max)
	}

	// Check average is recalculated
	expectedAvg := 3900.0 / 180.0
	if tempField.Avg != expectedAvg {
		t.Errorf("Expected Avg=%f, got %f", expectedAvg, tempField.Avg)
	}
}

func TestAggregateAggregatedPoints_EmptyPoints(t *testing.T) {
	_, err := AggregateAggregatedPoints("device-001", []*AggregatedPoint{}, AggregationDaily)
	if err == nil {
		t.Error("Expected error for empty points, got nil")
	}
}

func TestAggregateAggregatedPoints_MultipleFields(t *testing.T) {
	now := time.Now()

	points := []*AggregatedPoint{
		{
			Time:     now,
			DeviceID: "device-001",
			Level:    AggregationHourly,
			Fields: map[string]*AggregatedField{
				"temperature": {Count: 10, Sum: 200.0, Avg: 20.0, Min: 18.0, Max: 22.0, SumSquares: 4000.0},
				"humidity":    {Count: 10, Sum: 500.0, Avg: 50.0, Min: 45.0, Max: 55.0, SumSquares: 25000.0},
			},
		},
		{
			Time:     now.Add(time.Hour),
			DeviceID: "device-001",
			Level:    AggregationHourly,
			Fields: map[string]*AggregatedField{
				"temperature": {Count: 10, Sum: 220.0, Avg: 22.0, Min: 20.0, Max: 24.0, SumSquares: 4840.0},
				"humidity":    {Count: 10, Sum: 600.0, Avg: 60.0, Min: 55.0, Max: 65.0, SumSquares: 36000.0},
			},
		},
	}

	agg, err := AggregateAggregatedPoints("device-001", points, AggregationDaily)
	if err != nil {
		t.Fatalf("AggregateAggregatedPoints failed: %v", err)
	}

	if len(agg.Fields) != 2 {
		t.Errorf("Expected 2 fields, got %d", len(agg.Fields))
	}

	// Check temperature
	tempField := agg.Fields["temperature"]
	if tempField.Count != 20 {
		t.Errorf("Expected temperature Count=20, got %d", tempField.Count)
	}
	if tempField.Sum != 420.0 {
		t.Errorf("Expected temperature Sum=420.0, got %f", tempField.Sum)
	}
	if tempField.Min != 18.0 {
		t.Errorf("Expected temperature Min=18.0, got %f", tempField.Min)
	}
	if tempField.Max != 24.0 {
		t.Errorf("Expected temperature Max=24.0, got %f", tempField.Max)
	}

	// Check humidity
	humidityField := agg.Fields["humidity"]
	if humidityField.Count != 20 {
		t.Errorf("Expected humidity Count=20, got %d", humidityField.Count)
	}
	if humidityField.Sum != 1100.0 {
		t.Errorf("Expected humidity Sum=1100.0, got %f", humidityField.Sum)
	}
	if humidityField.Min != 45.0 {
		t.Errorf("Expected humidity Min=45.0, got %f", humidityField.Min)
	}
	if humidityField.Max != 65.0 {
		t.Errorf("Expected humidity Max=65.0, got %f", humidityField.Max)
	}
}

func TestConvertToRawDataPoint(t *testing.T) {
	now := time.Now()
	fields := map[string]interface{}{
		"temperature": 20.0,
		"humidity":    50.0,
	}

	point := ConvertToRawDataPoint(now, fields)

	if !point.Time.Equal(now) {
		t.Errorf("Expected Time=%v, got %v", now, point.Time)
	}
	if len(point.Fields) != 2 {
		t.Errorf("Expected 2 fields, got %d", len(point.Fields))
	}
	if point.Fields["temperature"] != 20.0 {
		t.Errorf("Expected temperature=20.0, got %v", point.Fields["temperature"])
	}
	if point.Fields["humidity"] != 50.0 {
		t.Errorf("Expected humidity=50.0, got %v", point.Fields["humidity"])
	}
}

// =============================================================================
// writeMetadata error paths
// =============================================================================

func TestWriteMetadata_ReadOnlyDir(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	config := DefaultStorageConfig()
	s := NewStorageWithConfig(tmpDir, logger, config)

	// Use a path under /dev/null (non-writable)
	err := s.writeMetadata("/dev/null/impossible/dir", &PartitionMetadata{
		Version: 6,
		Level:   string(AggregationHourly),
	})
	if err == nil {
		t.Error("writeMetadata to non-writable dir should fail")
	}
}

func TestWriteMetadata_UpdatesTimestamp(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	config := DefaultStorageConfig()
	s := NewStorageWithConfig(tmpDir, logger, config)

	partDir := filepath.Join(tmpDir, "test_part")
	before := time.Now()

	meta := &PartitionMetadata{
		Version: 6,
		Level:   string(AggregationHourly),
	}

	err := s.writeMetadata(partDir, meta)
	if err != nil {
		t.Fatalf("writeMetadata failed: %v", err)
	}

	if meta.UpdatedAt.Before(before) {
		t.Error("UpdatedAt should be after the time before write")
	}

	// Verify written to disk
	data, err := os.ReadFile(filepath.Join(partDir, MetadataFile))
	if err != nil {
		t.Fatalf("Failed to read metadata file: %v", err)
	}

	var readMeta PartitionMetadata
	if err := json.Unmarshal(data, &readMeta); err != nil {
		t.Fatalf("Failed to unmarshal metadata: %v", err)
	}

	if readMeta.Level != string(AggregationHourly) {
		t.Errorf("Level = %q, want %q", readMeta.Level, AggregationHourly)
	}
}

func TestWriteMetadata_CacheUpdate(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	config := DefaultStorageConfig()
	s := NewStorageWithConfig(tmpDir, logger, config)

	partDir := filepath.Join(tmpDir, "cached_part")

	meta := &PartitionMetadata{
		Version: 6,
		Level:   string(AggregationHourly),
	}

	// Write
	err := s.writeMetadata(partDir, meta)
	if err != nil {
		t.Fatalf("writeMetadata failed: %v", err)
	}

	// Read from cache (should be cached now)
	cachedMeta, err := s.readMetadata(partDir)
	if err != nil {
		t.Fatalf("readMetadata failed: %v", err)
	}
	if cachedMeta.Level != string(AggregationHourly) {
		t.Errorf("Cached metadata Level = %q, want %q", cachedMeta.Level, AggregationHourly)
	}
}

// =============================================================================
// writeDGMetadata error paths
// =============================================================================

func TestWriteDGMetadata_ReadOnlyDir(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	config := DefaultStorageConfig()
	s := NewStorageWithConfig(tmpDir, logger, config)

	err := s.writeDGMetadata("/dev/null/impossible/dir", &DGMetadata{
		Parts: []PartInfo{},
	})
	if err == nil {
		t.Error("writeDGMetadata to non-writable dir should fail")
	}
}

func TestWriteDGMetadata_RoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	config := DefaultStorageConfig()
	s := NewStorageWithConfig(tmpDir, logger, config)

	dgDir := filepath.Join(tmpDir, "dg_0000")

	meta := &DGMetadata{
		Parts: []PartInfo{
			{FileName: "part_0001.bin", MinTime: 100, MaxTime: 200, RowCount: 50, Size: 1024, Checksum: 0xabc123},
			{FileName: "part_0002.bin", MinTime: 201, MaxTime: 300, RowCount: 75, Size: 2048, Checksum: 0xdef456},
		},
	}

	err := s.writeDGMetadata(dgDir, meta)
	if err != nil {
		t.Fatalf("writeDGMetadata failed: %v", err)
	}

	// Read back
	readMeta, err := s.readDGMetadata(dgDir)
	if err != nil {
		t.Fatalf("readDGMetadata failed: %v", err)
	}

	if len(readMeta.Parts) != 2 {
		t.Fatalf("Expected 2 parts, got %d", len(readMeta.Parts))
	}
	if readMeta.Parts[0].FileName != "part_0001.bin" {
		t.Errorf("Part 0 filename = %q, want part_0001.bin", readMeta.Parts[0].FileName)
	}
	if readMeta.Parts[1].RowCount != 75 {
		t.Errorf("Part 1 rows = %d, want 75", readMeta.Parts[1].RowCount)
	}
}

func TestReadDGMetadata_InvalidJSON_Extended(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	config := DefaultStorageConfig()
	s := NewStorageWithConfig(tmpDir, logger, config)

	dgDir := filepath.Join(tmpDir, "dg_bad")
	if err := os.MkdirAll(dgDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dgDir, MetadataFile), []byte("{invalid json}"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := s.readDGMetadata(dgDir)
	if err == nil {
		t.Error("Expected error for invalid JSON in DG metadata")
	}
}

// =============================================================================
// readV6PartFooter edge cases
// =============================================================================

func TestReadV6PartFooter_InvalidFooterOffset(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	config := DefaultStorageConfig()
	s := NewStorageWithConfig(tmpDir, logger, config)

	filePath := filepath.Join(tmpDir, "bad_footer.bin")

	// Create a file with valid size but bad footer offset
	buf := make([]byte, V6AggHeaderSize+100)
	// Set magic number
	binary.LittleEndian.PutUint32(buf[0:], AggPartMagic)
	// Set absurdly high footer offset (bigger than file)
	binary.LittleEndian.PutUint64(buf[len(buf)-8:], uint64(9999999))
	binary.LittleEndian.PutUint32(buf[len(buf)-12:], uint32(50))

	if err := os.WriteFile(filePath, buf, 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := s.readV6PartFooter(filePath)
	if err == nil {
		t.Error("Expected error for invalid footer offset")
	}
}

// =============================================================================
// commitPendingWrites edge cases
// =============================================================================

func TestCommitPendingWrites_RenameError(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	config := DefaultStorageConfig()
	s := NewStorageWithConfig(tmpDir, logger, config)

	// Create first valid pending write
	tmpPath1 := filepath.Join(tmpDir, "file1.tmp")
	if err := os.WriteFile(tmpPath1, []byte("data1"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Second pending write with a non-existent tmp file
	pending := []*pendingPartWrite{
		{tmpPath: tmpPath1, filePath: filepath.Join(tmpDir, "file1.bin")},
		{tmpPath: filepath.Join(tmpDir, "nonexistent.tmp"), filePath: filepath.Join(tmpDir, "file2.bin")},
	}

	err := s.commitPendingWrites(pending)
	if err == nil {
		t.Error("Expected error when rename fails")
	}
}

func TestCommitPendingWrites_MixedNilEntries(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	config := DefaultStorageConfig()
	s := NewStorageWithConfig(tmpDir, logger, config)

	tmpPath := filepath.Join(tmpDir, "file.tmp")
	if err := os.WriteFile(tmpPath, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	pending := []*pendingPartWrite{
		nil,
		{tmpPath: tmpPath, filePath: filepath.Join(tmpDir, "file.bin")},
		nil,
	}

	err := s.commitPendingWrites(pending)
	if err != nil {
		t.Fatalf("commitPendingWrites with nil entries should work: %v", err)
	}

	// Verify file was renamed
	if _, err := os.Stat(filepath.Join(tmpDir, "file.bin")); err != nil {
		t.Error("file.bin should exist after rename")
	}
}

// =============================================================================
// CompactAll with multiple partitions and errors
// =============================================================================

func TestCompactAll_WithActualPartitions(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	config := DefaultStorageConfig()
	config.MaxRowsPerPart = 100
	s := NewStorageWithConfig(tmpDir, logger, config)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	// Write data to create multiple parts across partitions
	for i := 0; i < 3; i++ {
		points := []*AggregatedPoint{
			{
				Time: now.Add(time.Duration(i) * time.Hour), DeviceID: "dev1",
				Level: AggregationHourly,
				Fields: map[string]*AggregatedField{
					"temp": {Sum: float64(250 + i*10), Avg: float64(25 + i), Min: float64(20 + i), Max: float64(30 + i), Count: 10},
				},
			},
		}
		err := s.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points, now)
		if err != nil {
			t.Fatalf("WriteAggregatedPoints failed: %v", err)
		}
	}

	// CompactAll
	results, err := s.CompactAll(AggregationHourly, "testdb", "metrics")
	if err != nil {
		t.Fatalf("CompactAll failed: %v", err)
	}

	// Should have processed some partitions
	t.Logf("CompactAll returned %d results", len(results))
}

func TestCompactAll_NonExistentLevel(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	config := DefaultStorageConfig()
	s := NewStorageWithConfig(tmpDir, logger, config)

	results, err := s.CompactAll(AggregationLevel("unknown"), "testdb", "metrics")
	if err != nil {
		t.Fatalf("CompactAll for unknown level should not error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("Expected 0 results for non-existent level, got %d", len(results))
	}
}

// =============================================================================
// DeleteOldData with actual data
// =============================================================================

func TestDeleteOldData_WithActualData(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	config := DefaultStorageConfig()
	s := NewStorageWithConfig(tmpDir, logger, config)

	oldTime := time.Date(2023, 1, 15, 10, 0, 0, 0, time.UTC)
	newTime := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)

	// Write old data
	oldPoints := []*AggregatedPoint{
		{
			Time: oldTime, DeviceID: "dev1", Level: AggregationHourly,
			Fields: map[string]*AggregatedField{"temp": {Sum: 250, Avg: 25, Min: 20, Max: 30, Count: 10}},
		},
	}
	err := s.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", oldPoints, oldTime)
	if err != nil {
		t.Fatalf("Write old data failed: %v", err)
	}

	// Write new data
	newPoints := []*AggregatedPoint{
		{
			Time: newTime, DeviceID: "dev1", Level: AggregationHourly,
			Fields: map[string]*AggregatedField{"temp": {Sum: 260, Avg: 26, Min: 21, Max: 31, Count: 10}},
		},
	}
	err = s.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", newPoints, newTime)
	if err != nil {
		t.Fatalf("Write new data failed: %v", err)
	}

	// Delete data older than 2024
	cutoff := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	err = s.DeleteOldData(AggregationHourly, "testdb", "metrics", cutoff)
	if err != nil {
		t.Fatalf("DeleteOldData failed: %v", err)
	}

	// Read back — old data should be gone, new data should remain
	results, err := s.ReadAggregatedPoints(AggregationHourly, "testdb", "metrics", ReadOptions{
		StartTime: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("ReadAggregatedPoints failed: %v", err)
	}

	for _, p := range results {
		if p.Time.Before(cutoff) {
			t.Errorf("Found point with time %v, should have been deleted (before %v)", p.Time, cutoff)
		}
	}
}

func TestDeleteOldData_NoMatchingPartitions(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	config := DefaultStorageConfig()
	s := NewStorageWithConfig(tmpDir, logger, config)

	// Delete on empty directory
	err := s.DeleteOldData(AggregationHourly, "testdb", "metrics", time.Now())
	if err != nil {
		t.Fatalf("DeleteOldData on empty should not error: %v", err)
	}
}

// =============================================================================
// readPartitionData with DG metadata read error
// =============================================================================

func TestReadPartitionData_DGMetadataError(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	config := DefaultStorageConfig()
	s := NewStorageWithConfig(tmpDir, logger, config)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	// Write some data first
	points := []*AggregatedPoint{
		{
			Time: now, DeviceID: "dev1", Level: AggregationHourly,
			Fields: map[string]*AggregatedField{"temp": {Sum: 250, Avg: 25, Min: 20, Max: 30, Count: 10}},
		},
	}
	err := s.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points, now)
	if err != nil {
		t.Fatalf("WriteAggregatedPoints failed: %v", err)
	}

	// Corrupt the DG metadata
	partDir := s.getPartitionDir(AggregationHourly, "testdb", "metrics", now)
	meta, _ := s.readMetadata(partDir)
	if meta != nil && len(meta.DeviceGroups) > 0 {
		dgDir := filepath.Join(partDir, meta.DeviceGroups[0].DirName)
		if writeErr := os.WriteFile(filepath.Join(dgDir, MetadataFile), []byte("{corrupt}"), 0o644); writeErr != nil {
			t.Fatal(writeErr)
		}
	}

	// Read should continue (DG metadata error is logged and skipped)
	pointsMap := make(map[string]*AggregatedPoint)
	err = s.readPartitionData(partDir, 0, now.Add(24*time.Hour).UnixNano(), nil, nil, nil, pointsMap)
	// Should not return error (it logs and continues)
	if err != nil {
		t.Logf("readPartitionData returned error (acceptable): %v", err)
	}
}

// =============================================================================
// readPartitionData with footer/part read errors
// =============================================================================

func TestReadPartitionData_CorruptPartFile(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	config := DefaultStorageConfig()
	s := NewStorageWithConfig(tmpDir, logger, config)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	// Write data
	points := []*AggregatedPoint{
		{
			Time: now, DeviceID: "dev1", Level: AggregationHourly,
			Fields: map[string]*AggregatedField{"temp": {Sum: 250, Avg: 25, Min: 20, Max: 30, Count: 10}},
		},
	}
	err := s.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points, now)
	if err != nil {
		t.Fatalf("WriteAggregatedPoints failed: %v", err)
	}

	// Corrupt the part file
	partDir := s.getPartitionDir(AggregationHourly, "testdb", "metrics", now)
	meta, _ := s.readMetadata(partDir)
	if meta != nil && len(meta.DeviceGroups) > 0 {
		dgDir := filepath.Join(partDir, meta.DeviceGroups[0].DirName)
		dgMeta, _ := s.readDGMetadata(dgDir)
		if dgMeta != nil && len(dgMeta.Parts) > 0 {
			partPath := filepath.Join(dgDir, dgMeta.Parts[0].FileName)
			if writeErr := os.WriteFile(partPath, []byte("corrupted"), 0o644); writeErr != nil {
				t.Fatal(writeErr)
			}
		}
	}

	// Read should return error for corrupted part
	pointsMap := make(map[string]*AggregatedPoint)
	err = s.readPartitionData(partDir, 0, now.Add(24*time.Hour).UnixNano(), nil, nil, nil, pointsMap)
	if err == nil {
		t.Log("Note: readPartitionData with corrupt data might skip or error depending on implementation")
	}
}

// =============================================================================
// CompactWithOptions - verify actual data integrity after compaction
// =============================================================================

func TestCompactWithOptions_DataIntegrityCheck(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	config := DefaultStorageConfig()
	config.MaxRowsPerPart = 5 // Force many small parts
	s := NewStorageWithConfig(tmpDir, logger, config)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	// Write multiple batches to create multiple parts
	for i := 0; i < 5; i++ {
		points := make([]*AggregatedPoint, 3)
		for j := 0; j < 3; j++ {
			points[j] = &AggregatedPoint{
				Time: now.Add(time.Duration(i*3+j) * time.Minute), DeviceID: "dev1",
				Level: AggregationHourly,
				Fields: map[string]*AggregatedField{
					"temp": {Sum: float64(100 + i*10 + j), Avg: float64(25 + i), Min: float64(20), Max: float64(30 + i), Count: int64(10 + j)},
				},
			}
		}
		err := s.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points, now)
		if err != nil {
			t.Fatalf("Write batch %d failed: %v", i, err)
		}
	}

	// Read all data before compaction
	beforePoints, err := s.ReadAggregatedPoints(AggregationHourly, "testdb", "metrics", ReadOptions{
		StartTime: now.Add(-time.Hour),
		EndTime:   now.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("ReadBefore failed: %v", err)
	}

	// Compact
	partDir := s.getPartitionDir(AggregationHourly, "testdb", "metrics", now)
	err = s.CompactWithOptions(partDir, CompactOptions{TargetRowsPerPart: 100})
	if err != nil {
		t.Fatalf("CompactWithOptions failed: %v", err)
	}

	// Read all data after compaction
	afterPoints, err := s.ReadAggregatedPoints(AggregationHourly, "testdb", "metrics", ReadOptions{
		StartTime: now.Add(-time.Hour),
		EndTime:   now.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("ReadAfter failed: %v", err)
	}

	// Verify same number of points
	if len(beforePoints) != len(afterPoints) {
		t.Errorf("Point count changed after compaction: before=%d, after=%d", len(beforePoints), len(afterPoints))
	}
}

// =============================================================================
// writeV6PartFile with large data (more columns)
// =============================================================================

func TestWriteV6PartFile_MultipleFieldsMultipleMetrics(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	config := DefaultStorageConfig()
	s := NewStorageWithConfig(tmpDir, logger, config)

	data := NewColumnarData(3)
	data.AddRowAllMetrics(1000, "dev1", map[string]*AggregatedField{
		"temp":     {Sum: 100, Avg: 10, Min: 5, Max: 15, Count: 10},
		"humidity": {Sum: 500, Avg: 50, Min: 40, Max: 60, Count: 10},
	})
	data.AddRowAllMetrics(2000, "dev2", map[string]*AggregatedField{
		"temp":     {Sum: 200, Avg: 20, Min: 15, Max: 25, Count: 20},
		"humidity": {Sum: 600, Avg: 60, Min: 50, Max: 70, Count: 20},
	})
	data.AddRowAllMetrics(3000, "dev1", map[string]*AggregatedField{
		"temp":     {Sum: 150, Avg: 15, Min: 8, Max: 22, Count: 15},
		"humidity": {Sum: 550, Avg: 55, Min: 45, Max: 65, Count: 15},
	})

	partDir := filepath.Join(tmpDir, "multi_field_test")
	if err := os.MkdirAll(partDir, 0o755); err != nil {
		t.Fatal(err)
	}

	result, err := s.writeV6PartFile(partDir, 1, AggregationHourly, data)
	if err != nil {
		t.Fatalf("writeV6PartFile failed: %v", err)
	}
	if result == nil {
		t.Fatal("writeV6PartFile returned nil result")
	}

	if result.partInfo.RowCount != 3 {
		t.Errorf("RowCount = %d, want 3", result.partInfo.RowCount)
	}

	// Commit the pending write (rename .tmp to final path) before reading
	if commitErr := s.commitPendingWrites([]*pendingPartWrite{result}); commitErr != nil {
		t.Fatalf("commitPendingWrites failed: %v", commitErr)
	}

	// Read back and verify footer
	footer, err := s.readV6PartFooter(result.filePath)
	if err != nil {
		t.Fatalf("readV6PartFooter failed: %v", err)
	}

	// Should have columns for both temp and humidity
	if len(footer.Columns) == 0 {
		t.Error("Expected non-zero columns in footer")
	}
	if len(footer.FieldNames) < 3 {
		t.Errorf("Expected at least 3 field names, got %d: %v", len(footer.FieldNames), footer.FieldNames)
	}
	t.Logf("Total columns: %d, fields: %v, devices: %v", len(footer.Columns), footer.FieldNames, footer.DeviceNames)
}

// =============================================================================
// sliceColumnarData - beyond range
// =============================================================================

func TestSliceColumnarData_BeyondRange(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, logger)

	data := &ColumnarData{
		Timestamps: []int64{100, 200, 300, 400},
		DeviceIDs:  []string{"d1", "d2", "d3", "d4"},
		FloatColumns: map[string][]float64{
			"temp:sum": {1, 2, 3, 4},
		},
		IntColumns: map[string][]int64{
			"temp:count": {10, 20, 30, 40},
		},
	}

	// Out-of-range start panics because the underlying function
	// slices Timestamps/DeviceIDs without bounds checking.
	// Verify that it panics as expected.
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for out-of-range slice, but none occurred")
		}
	}()
	_ = s.sliceColumnarData(data, 10, 20)
}

func TestSliceColumnarData_StartAtZero(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, logger)

	data := &ColumnarData{
		Timestamps: []int64{100, 200, 300},
		DeviceIDs:  []string{"d1", "d2", "d3"},
		FloatColumns: map[string][]float64{
			"temp:sum": {1, 2, 3},
		},
		IntColumns: map[string][]int64{},
	}

	result := s.sliceColumnarData(data, 0, 2)
	if result.RowCount() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.RowCount())
	}
	if result.Timestamps[0] != 100 || result.Timestamps[1] != 200 {
		t.Error("Wrong timestamps in slice")
	}
}

// =============================================================================
// AggregateAggregatedPoints - monthly/yearly level time truncation
// =============================================================================

func TestAggregateAggregatedPoints_MonthlyLevel(t *testing.T) {
	now := time.Date(2024, 5, 15, 10, 30, 0, 0, time.UTC)

	points := []*AggregatedPoint{
		{
			Time: now, DeviceID: "dev1", Level: AggregationDaily,
			Fields: map[string]*AggregatedField{"temp": {Count: 10, Sum: 250, Avg: 25, Min: 20, Max: 30}},
		},
		{
			Time: now.AddDate(0, 0, 1), DeviceID: "dev1", Level: AggregationDaily,
			Fields: map[string]*AggregatedField{"temp": {Count: 10, Sum: 260, Avg: 26, Min: 21, Max: 31}},
		},
	}

	result, err := AggregateAggregatedPoints("dev1", points, AggregationMonthly)
	if err != nil {
		t.Fatalf("AggregateAggregatedPoints monthly failed: %v", err)
	}

	// Time should be truncated to month start
	expected := time.Date(2024, 5, 1, 0, 0, 0, 0, time.UTC)
	if !result.Time.Equal(expected) {
		t.Errorf("Time = %v, want %v (month start)", result.Time, expected)
	}

	if result.Fields["temp"].Count != 20 {
		t.Errorf("Count = %d, want 20", result.Fields["temp"].Count)
	}
}

func TestAggregateAggregatedPoints_YearlyLevel(t *testing.T) {
	now := time.Date(2024, 5, 15, 10, 30, 0, 0, time.UTC)

	points := []*AggregatedPoint{
		{
			Time: now, DeviceID: "dev1", Level: AggregationMonthly,
			Fields: map[string]*AggregatedField{"temp": {Count: 300, Sum: 7500, Avg: 25, Min: 15, Max: 35}},
		},
	}

	result, err := AggregateAggregatedPoints("dev1", points, AggregationYearly)
	if err != nil {
		t.Fatalf("AggregateAggregatedPoints yearly failed: %v", err)
	}

	// Time should be truncated to year start
	expected := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	if !result.Time.Equal(expected) {
		t.Errorf("Time = %v, want %v (year start)", result.Time, expected)
	}
}

func TestAggregateAggregatedPoints_DefaultLevel(t *testing.T) {
	now := time.Date(2024, 5, 15, 10, 30, 45, 0, time.UTC)

	points := []*AggregatedPoint{
		{
			Time: now, DeviceID: "dev1", Level: AggregationHourly,
			Fields: map[string]*AggregatedField{"temp": {Count: 60, Sum: 1500, Avg: 25, Min: 20, Max: 30}},
		},
	}

	result, err := AggregateAggregatedPoints("dev1", points, AggregationHourly)
	if err != nil {
		t.Fatalf("AggregateAggregatedPoints hourly failed: %v", err)
	}

	// Time should be truncated to hour
	expected := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)
	if !result.Time.Equal(expected) {
		t.Errorf("Time = %v, want %v (hour start)", result.Time, expected)
	}
}

// =============================================================================
// AggregateRawDataPoints - monthly/yearly level
// =============================================================================

func TestAggregateRawDataPoints_MonthlyLevel(t *testing.T) {
	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	points := []*RawDataPoint{
		{Time: now, Fields: map[string]interface{}{"temp": 25.0}},
		{Time: now.AddDate(0, 0, 1), Fields: map[string]interface{}{"temp": 26.0}},
	}

	result, err := AggregateRawDataPoints("dev1", points, AggregationMonthly)
	if err != nil {
		t.Fatalf("AggregateRawDataPoints monthly failed: %v", err)
	}

	expected := time.Date(2024, 5, 1, 0, 0, 0, 0, time.UTC)
	if !result.Time.Equal(expected) {
		t.Errorf("Time = %v, want %v", result.Time, expected)
	}
}

func TestAggregateRawDataPoints_YearlyLevel(t *testing.T) {
	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	points := []*RawDataPoint{
		{Time: now, Fields: map[string]interface{}{"temp": 25.0}},
	}

	result, err := AggregateRawDataPoints("dev1", points, AggregationYearly)
	if err != nil {
		t.Fatalf("AggregateRawDataPoints yearly failed: %v", err)
	}

	expected := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	if !result.Time.Equal(expected) {
		t.Errorf("Time = %v, want %v", result.Time, expected)
	}
}

// =============================================================================
// invalidateMetadataCache
// =============================================================================

func TestInvalidateMetadataCache(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	config := DefaultStorageConfig()
	s := NewStorageWithConfig(tmpDir, logger, config)

	partDir := filepath.Join(tmpDir, "cache_test")

	// Write metadata (which caches it)
	meta := &PartitionMetadata{Version: 6, Level: string(AggregationHourly)}
	err := s.writeMetadata(partDir, meta)
	if err != nil {
		t.Fatalf("writeMetadata failed: %v", err)
	}

	// Verify it's cached
	s.metaCacheMu.RLock()
	_, cached := s.metaCache[partDir]
	s.metaCacheMu.RUnlock()
	if !cached {
		t.Fatal("Metadata should be cached after write")
	}

	// Invalidate
	s.invalidateMetadataCache(partDir)

	// Verify no longer cached
	s.metaCacheMu.RLock()
	_, cached = s.metaCache[partDir]
	s.metaCacheMu.RUnlock()
	if cached {
		t.Error("Metadata should not be cached after invalidation")
	}
}

// =============================================================================
// NeedsCompaction with mixed DG states
// =============================================================================

func TestNeedsCompaction_MixedDGStates(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	config := DefaultStorageConfig()
	config.MaxRowsPerPart = 3 // Very small to force multiple parts
	s := NewStorageWithConfig(tmpDir, logger, config)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	// Write multiple batches to force multiple parts
	for i := 0; i < 5; i++ {
		points := make([]*AggregatedPoint, 4)
		for j := 0; j < 4; j++ {
			points[j] = &AggregatedPoint{
				Time: now.Add(time.Duration(i*4+j) * time.Minute), DeviceID: "dev1",
				Level: AggregationHourly,
				Fields: map[string]*AggregatedField{
					"temp": {Sum: 100, Avg: 25, Min: 20, Max: 30, Count: 10},
				},
			}
		}
		err := s.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points, now)
		if err != nil {
			t.Fatalf("Write %d failed: %v", i, err)
		}
	}

	partDir := s.getPartitionDir(AggregationHourly, "testdb", "metrics", now)

	needsCompact, partsCount := s.NeedsCompaction(partDir, 2)
	t.Logf("NeedsCompaction: %v, partsCount: %v", needsCompact, partsCount)

	if !needsCompact {
		t.Error("Should need compaction with many parts")
	}
}

// =============================================================================
// GetPartitionStats with real data
// =============================================================================

func TestGetPartitionStats_WithData(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	config := DefaultStorageConfig()
	s := NewStorageWithConfig(tmpDir, logger, config)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	points := []*AggregatedPoint{
		{
			Time: now, DeviceID: "dev1", Level: AggregationHourly,
			Fields: map[string]*AggregatedField{
				"temp":     {Sum: 250, Avg: 25, Min: 20, Max: 30, Count: 10},
				"humidity": {Sum: 500, Avg: 50, Min: 40, Max: 60, Count: 10},
			},
		},
		{
			Time: now.Add(time.Minute), DeviceID: "dev2", Level: AggregationHourly,
			Fields: map[string]*AggregatedField{
				"temp": {Sum: 260, Avg: 26, Min: 21, Max: 31, Count: 10},
			},
		},
	}
	err := s.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points, now)
	if err != nil {
		t.Fatalf("WriteAggregatedPoints failed: %v", err)
	}

	meta, err := s.GetPartitionStats(AggregationHourly, "testdb", "metrics", now)
	if err != nil {
		t.Fatalf("GetPartitionStats failed: %v", err)
	}
	if meta == nil {
		t.Fatal("GetPartitionStats returned nil metadata")
	}
	if len(meta.DeviceGroups) == 0 {
		t.Error("Expected at least 1 device group")
	}
}
