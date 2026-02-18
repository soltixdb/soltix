package aggregation

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/logging"
)

func TestStorage_WriteAndRead(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "columnar_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tmpDir, logger)

	// Create test data
	testTime := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	points := []*AggregatedPoint{
		{
			Time:     testTime,
			DeviceID: "device1",
			Level:    AggregationHourly,
			Complete: true,
			Fields: map[string]*AggregatedField{
				"temperature": {Count: 10, Sum: 250.0, Avg: 25.0, Min: 20.0, Max: 30.0},
				"humidity":    {Count: 10, Sum: 600.0, Avg: 60.0, Min: 50.0, Max: 70.0},
			},
		},
		{
			Time:     testTime,
			DeviceID: "device2",
			Level:    AggregationHourly,
			Complete: true,
			Fields: map[string]*AggregatedField{
				"temperature": {Count: 5, Sum: 100.0, Avg: 20.0, Min: 15.0, Max: 25.0},
				"humidity":    {Count: 5, Sum: 250.0, Avg: 50.0, Min: 40.0, Max: 60.0},
			},
		},
	}

	// Write hourly data
	err = storage.WriteHourly("testdb", "testcol", points)
	if err != nil {
		t.Fatalf("Failed to write hourly data: %v", err)
	}

	// Read back the data
	readPoints, err := storage.ReadHourlyForDay("testdb", "testcol", testTime)
	if err != nil {
		t.Fatalf("Failed to read hourly data: %v", err)
	}

	if len(readPoints) != 2 {
		t.Errorf("Expected 2 points, got %d", len(readPoints))
	}

	// Verify data integrity
	for _, point := range readPoints {
		if point.DeviceID == "device1" {
			tempField := point.Fields["temperature"]
			if tempField == nil {
				t.Error("Expected temperature field for device1")
				continue
			}
			if tempField.Sum != 250.0 {
				t.Errorf("Expected Sum=250.0, got %f", tempField.Sum)
			}
			if tempField.Avg != 25.0 {
				t.Errorf("Expected Avg=25.0, got %f", tempField.Avg)
			}
			if tempField.Min != 20.0 {
				t.Errorf("Expected Min=20.0, got %f", tempField.Min)
			}
			if tempField.Max != 30.0 {
				t.Errorf("Expected Max=30.0, got %f", tempField.Max)
			}
			if tempField.Count != 10 {
				t.Errorf("Expected Count=10, got %d", tempField.Count)
			}
		}
	}
}

func TestStorage_DailyAggregation(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "columnar_daily_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tmpDir, logger)

	// Create daily test data
	testTime := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	points := []*AggregatedPoint{
		{
			Time:     testTime,
			DeviceID: "sensor1",
			Level:    AggregationDaily,
			Complete: true,
			Fields: map[string]*AggregatedField{
				"power": {Count: 24, Sum: 1000.0, Avg: 41.67, Min: 10.0, Max: 100.0},
			},
		},
	}

	// Write daily data
	err = storage.WriteDaily("testdb", "testcol", points)
	if err != nil {
		t.Fatalf("Failed to write daily data: %v", err)
	}

	// Read back
	readPoints, err := storage.ReadDailyForMonth("testdb", "testcol", testTime)
	if err != nil {
		t.Fatalf("Failed to read daily data: %v", err)
	}

	if len(readPoints) != 1 {
		t.Errorf("Expected 1 point, got %d", len(readPoints))
	}

	if len(readPoints) > 0 {
		point := readPoints[0]
		if point.DeviceID != "sensor1" {
			t.Errorf("Expected deviceID=sensor1, got %s", point.DeviceID)
		}
		if point.Fields["power"].Sum != 1000.0 {
			t.Errorf("Expected Sum=1000.0, got %f", point.Fields["power"].Sum)
		}
	}
}

func TestStorage_MultiplePartitions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "columnar_multi_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tmpDir, logger)

	// Write data for multiple days
	for day := 1; day <= 3; day++ {
		testTime := time.Date(2024, 1, day, 10, 0, 0, 0, time.UTC)
		points := []*AggregatedPoint{
			{
				Time:     testTime,
				DeviceID: "device1",
				Level:    AggregationHourly,
				Complete: true,
				Fields: map[string]*AggregatedField{
					"value": {Count: int64(day * 10), Sum: float64(day * 100), Avg: 10.0, Min: 1.0, Max: float64(day * 10)},
				},
			},
		}
		err = storage.WriteHourly("testdb", "testcol", points)
		if err != nil {
			t.Fatalf("Failed to write day %d: %v", day, err)
		}
	}

	// Read data across time range
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(2024, 1, 4, 0, 0, 0, 0, time.UTC)

	readPoints, err := storage.ReadAggregatedPoints(AggregationHourly, "testdb", "testcol", ReadOptions{
		StartTime: startTime,
		EndTime:   endTime,
	})
	if err != nil {
		t.Fatalf("Failed to read data: %v", err)
	}

	if len(readPoints) != 3 {
		t.Errorf("Expected 3 points (one per day), got %d", len(readPoints))
	}
}

func TestStorage_DeviceFilter(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "columnar_filter_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tmpDir, logger)

	testTime := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	points := []*AggregatedPoint{
		{Time: testTime, DeviceID: "device1", Level: AggregationHourly, Fields: map[string]*AggregatedField{"value": {Count: 1, Sum: 10.0}}},
		{Time: testTime, DeviceID: "device2", Level: AggregationHourly, Fields: map[string]*AggregatedField{"value": {Count: 1, Sum: 20.0}}},
		{Time: testTime, DeviceID: "device3", Level: AggregationHourly, Fields: map[string]*AggregatedField{"value": {Count: 1, Sum: 30.0}}},
	}

	err = storage.WriteHourly("testdb", "testcol", points)
	if err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	// Read with device filter
	readPoints, err := storage.ReadAggregatedPoints(AggregationHourly, "testdb", "testcol", ReadOptions{
		StartTime: testTime.Add(-time.Hour),
		EndTime:   testTime.Add(time.Hour),
		DeviceIDs: []string{"device1", "device3"},
	})
	if err != nil {
		t.Fatalf("Failed to read with filter: %v", err)
	}

	if len(readPoints) != 2 {
		t.Errorf("Expected 2 filtered points, got %d", len(readPoints))
	}

	// Verify filtered devices
	deviceIDs := make(map[string]bool)
	for _, p := range readPoints {
		deviceIDs[p.DeviceID] = true
	}
	if !deviceIDs["device1"] || !deviceIDs["device3"] {
		t.Error("Expected device1 and device3 in results")
	}
	if deviceIDs["device2"] {
		t.Error("device2 should not be in filtered results")
	}
}

func TestStorage_MetadataCache(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "columnar_cache_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tmpDir, logger)

	testTime := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	points := []*AggregatedPoint{
		{Time: testTime, DeviceID: "device1", Level: AggregationHourly, Fields: map[string]*AggregatedField{"value": {Count: 1, Sum: 10.0}}},
	}

	// Write data
	err = storage.WriteHourly("testdb", "testcol", points)
	if err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	// Read twice - second read should use cache
	for i := 0; i < 2; i++ {
		readPoints, err := storage.ReadHourlyForDay("testdb", "testcol", testTime)
		if err != nil {
			t.Fatalf("Read %d failed: %v", i, err)
		}
		if len(readPoints) != 1 {
			t.Errorf("Read %d: Expected 1 point, got %d", i, len(readPoints))
		}
	}
}

func TestStorage_EmptyQuery(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "columnar_empty_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tmpDir, logger)

	// Query non-existent data
	readPoints, err := storage.ReadHourlyForDay("testdb", "testcol", time.Now())
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}

	if len(readPoints) != 0 {
		t.Errorf("Expected 0 points for non-existent data, got %d", len(readPoints))
	}
}

func TestStorage_InterfaceCompliance(t *testing.T) {
	// Ensure Storage implements AggregationStorage
	var _ AggregationStorage = (*Storage)(nil)
}

// =============================================================================
// MetricType conversions
// =============================================================================

func TestMetricTypeToIndex(t *testing.T) {
	tests := []struct {
		metric MetricType
		want   uint8
	}{
		{MetricSum, 0},
		{MetricAvg, 1},
		{MetricMin, 2},
		{MetricMax, 3},
		{MetricCount, 4},
		{MetricMinTime, 5},
		{MetricMaxTime, 6},
		{MetricType("unknown"), 0}, // default
		{MetricType(""), 0},        // empty
	}
	for _, tt := range tests {
		got := MetricTypeToIndex(tt.metric)
		if got != tt.want {
			t.Errorf("MetricTypeToIndex(%q) = %d, want %d", tt.metric, got, tt.want)
		}
	}
}

func TestMetricTypeFromIndex(t *testing.T) {
	tests := []struct {
		idx  uint8
		want MetricType
	}{
		{0, MetricSum},
		{1, MetricAvg},
		{2, MetricMin},
		{3, MetricMax},
		{4, MetricCount},
		{5, MetricMinTime},
		{6, MetricMaxTime},
		{255, MetricSum}, // out of range
	}
	for _, tt := range tests {
		got := MetricTypeFromIndex(tt.idx)
		if got != tt.want {
			t.Errorf("MetricTypeFromIndex(%d) = %q, want %q", tt.idx, got, tt.want)
		}
	}
}

func TestMetricTypeRoundTrip(t *testing.T) {
	for _, m := range AllMetricTypes() {
		idx := MetricTypeToIndex(m)
		back := MetricTypeFromIndex(idx)
		if back != m {
			t.Errorf("RoundTrip(%q) -> %d -> %q", m, idx, back)
		}
	}
}

// =============================================================================
// V6 key helpers
// =============================================================================

func TestV6FloatKey(t *testing.T) {
	key := v6FloatKey(MetricSum, "temperature")
	metric, field := v6ParseFloatKey(key)
	if metric != MetricSum || field != "temperature" {
		t.Errorf("v6FloatKey roundtrip: got metric=%q field=%q", metric, field)
	}
}

func TestV6IntKey(t *testing.T) {
	key := v6IntKey("temperature")
	metric, field := v6ParseIntKey(key)
	if metric != MetricCount {
		t.Errorf("v6IntKey roundtrip metric: got %q", metric)
	}
	if field != "temperature" {
		t.Errorf("v6IntKey roundtrip: got field=%q", field)
	}
}

func TestV6ParseFloatKey_NoSeparator(t *testing.T) {
	// Key without null byte separator
	metric, field := v6ParseFloatKey("noseparator")
	if metric != "" || field != "noseparator" {
		t.Errorf("v6ParseFloatKey no separator: metric=%q field=%q", metric, field)
	}
}

func TestV6ParseIntKey_NoSeparator(t *testing.T) {
	metric, field := v6ParseIntKey("noseparator")
	if metric != MetricCount {
		t.Errorf("v6ParseIntKey no separator metric: %q", metric)
	}
	if field != "noseparator" {
		t.Errorf("v6ParseIntKey no separator: field=%q", field)
	}
}

func TestV6ParseFloatKey_EmptyMetric(t *testing.T) {
	// Key like "\x00fieldName" - empty metric
	key := "\x00temperature"
	metric, field := v6ParseFloatKey(key)
	if metric != "" || field != "temperature" {
		t.Errorf("v6ParseFloatKey empty metric: metric=%q field=%q", metric, field)
	}
}

func TestV6FloatKey_AllMetrics(t *testing.T) {
	for _, m := range []MetricType{MetricSum, MetricAvg, MetricMin, MetricMax} {
		key := v6FloatKey(m, "field1")
		gotMetric, gotField := v6ParseFloatKey(key)
		if gotMetric != m {
			t.Errorf("metric roundtrip %q: got %q", m, gotMetric)
		}
		if gotField != "field1" {
			t.Errorf("field roundtrip for %q: got %q", m, gotField)
		}
	}
}

// =============================================================================
// getPartitionDir
// =============================================================================

func TestGetPartitionDir_AllLevels(t *testing.T) {
	logger := logging.NewDevelopment()
	storage := NewStorage("/base", logger)

	ts := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		level AggregationLevel
		want  string
	}{
		{AggregationHourly, filepath.Join("/base", "agg_1h", "db", "col", "2024", "05", "15")},
		{AggregationDaily, filepath.Join("/base", "agg_1d", "db", "col", "2024", "05")},
		{AggregationMonthly, filepath.Join("/base", "agg_1M", "db", "col", "2024")},
		{AggregationYearly, filepath.Join("/base", "agg_1y", "db", "col")},
		{AggregationLevel("unknown"), filepath.Join("/base", "agg_1h", "db", "col", "2024", "05", "15")}, // default
	}

	for _, tt := range tests {
		got := storage.getPartitionDir(tt.level, "db", "col", ts)
		if got != tt.want {
			t.Errorf("getPartitionDir(%q) = %q, want %q", tt.level, got, tt.want)
		}
	}
}

func TestGetPartitionDir_WithTimezone(t *testing.T) {
	logger := logging.NewDevelopment()
	storage := NewStorage("/base", logger)

	tokyo, _ := time.LoadLocation("Asia/Tokyo")
	storage.SetTimezone(tokyo)

	// 2024-05-15 23:00 UTC = 2024-05-16 08:00 JST
	ts := time.Date(2024, 5, 15, 23, 0, 0, 0, time.UTC)

	got := storage.getPartitionDir(AggregationHourly, "db", "col", ts)
	// Should use JST (day 16, not 15)
	want := filepath.Join("/base", "agg_1h", "db", "col", "2024", "05", "16")
	if got != want {
		t.Errorf("getPartitionDir with timezone: got %q, want %q", got, want)
	}
}

// =============================================================================
// assignDeviceGroups
// =============================================================================

func TestAssignDeviceGroups_Basic(t *testing.T) {
	logger := logging.NewDevelopment()
	storage := NewStorageWithConfig("/tmp", logger, StorageConfig{
		MaxDevicesPerGroup: 3,
		MaxRowsPerPart:     10000,
		MaxPartSize:        64 * 1024 * 1024,
	})

	meta := &PartitionMetadata{
		DeviceGroupMap: make(map[string]int),
	}

	// Add 7 devices, should create 3 groups (3 + 3 + 1)
	devices := []string{"d1", "d2", "d3", "d4", "d5", "d6", "d7"}
	storage.assignDeviceGroups(meta, devices)

	if len(meta.DeviceGroups) != 3 {
		t.Errorf("Expected 3 device groups, got %d", len(meta.DeviceGroups))
	}

	// Verify each device is assigned
	for _, d := range devices {
		if _, ok := meta.DeviceGroupMap[d]; !ok {
			t.Errorf("Device %s not assigned to any group", d)
		}
	}

	// Verify group sizes
	if len(meta.DeviceGroups[0].DeviceNames) != 3 {
		t.Errorf("Group 0: expected 3 devices, got %d", len(meta.DeviceGroups[0].DeviceNames))
	}
	if len(meta.DeviceGroups[1].DeviceNames) != 3 {
		t.Errorf("Group 1: expected 3 devices, got %d", len(meta.DeviceGroups[1].DeviceNames))
	}
	if len(meta.DeviceGroups[2].DeviceNames) != 1 {
		t.Errorf("Group 2: expected 1 device, got %d", len(meta.DeviceGroups[2].DeviceNames))
	}
}

func TestAssignDeviceGroups_PreservesExisting(t *testing.T) {
	logger := logging.NewDevelopment()
	storage := NewStorageWithConfig("/tmp", logger, StorageConfig{
		MaxDevicesPerGroup: 3,
		MaxRowsPerPart:     10000,
		MaxPartSize:        64 * 1024 * 1024,
	})

	meta := &PartitionMetadata{
		DeviceGroups: []DeviceGroupInfo{
			{DirName: "dg_0000", GroupIndex: 0, DeviceNames: []string{"d1", "d2"}},
		},
		DeviceGroupMap: map[string]int{"d1": 0, "d2": 0},
	}

	// Add new devices - d3 should fill group 0, d4 in new group
	storage.assignDeviceGroups(meta, []string{"d1", "d2", "d3", "d4"})

	if len(meta.DeviceGroups) != 2 {
		t.Errorf("Expected 2 groups, got %d", len(meta.DeviceGroups))
	}
	if meta.DeviceGroupMap["d3"] != 0 {
		t.Errorf("d3 should be in group 0, got group %d", meta.DeviceGroupMap["d3"])
	}
	if meta.DeviceGroupMap["d4"] != 1 {
		t.Errorf("d4 should be in group 1, got group %d", meta.DeviceGroupMap["d4"])
	}
}

func TestAssignDeviceGroups_NoNewDevices(t *testing.T) {
	logger := logging.NewDevelopment()
	storage := NewStorage("/tmp", logger)

	meta := &PartitionMetadata{
		DeviceGroups: []DeviceGroupInfo{
			{DirName: "dg_0000", GroupIndex: 0, DeviceNames: []string{"d1"}},
		},
		DeviceGroupMap: map[string]int{"d1": 0},
	}

	// Same devices - should not change
	storage.assignDeviceGroups(meta, []string{"d1"})

	if len(meta.DeviceGroups) != 1 {
		t.Errorf("Expected 1 group, got %d", len(meta.DeviceGroups))
	}
}

func TestAssignDeviceGroups_ZeroMaxDevices(t *testing.T) {
	logger := logging.NewDevelopment()
	storage := NewStorageWithConfig("/tmp", logger, StorageConfig{
		MaxDevicesPerGroup: 0, // Should use default
		MaxRowsPerPart:     10000,
		MaxPartSize:        64 * 1024 * 1024,
	})

	meta := &PartitionMetadata{
		DeviceGroupMap: make(map[string]int),
	}

	storage.assignDeviceGroups(meta, []string{"d1", "d2"})

	if len(meta.DeviceGroups) != 1 {
		t.Errorf("Expected 1 group (default max), got %d", len(meta.DeviceGroups))
	}
}

func TestAssignDeviceGroups_NilMap(t *testing.T) {
	logger := logging.NewDevelopment()
	storage := NewStorage("/tmp", logger)

	meta := &PartitionMetadata{
		DeviceGroupMap: nil, // nil map
	}

	storage.assignDeviceGroups(meta, []string{"d1"})

	if meta.DeviceGroupMap == nil {
		t.Error("DeviceGroupMap should be initialized")
	}
	if _, ok := meta.DeviceGroupMap["d1"]; !ok {
		t.Error("d1 should be assigned")
	}
}

// =============================================================================
// collectFieldNames / collectDeviceIDs
// =============================================================================

func TestCollectFieldNames(t *testing.T) {
	points := []*AggregatedPoint{
		{Fields: map[string]*AggregatedField{"b": {}, "a": {}}},
		{Fields: map[string]*AggregatedField{"c": {}, "a": {}}},
	}

	names := collectFieldNames(points)
	if len(names) != 3 {
		t.Fatalf("Expected 3 fields, got %d", len(names))
	}
	// Should be sorted
	if names[0] != "a" || names[1] != "b" || names[2] != "c" {
		t.Errorf("Expected sorted [a, b, c], got %v", names)
	}
}

func TestCollectDeviceIDs(t *testing.T) {
	points := []*AggregatedPoint{
		{DeviceID: "z"},
		{DeviceID: "a"},
		{DeviceID: "z"}, // duplicate
	}

	ids := collectDeviceIDs(points)
	if len(ids) != 2 {
		t.Fatalf("Expected 2 unique IDs, got %d", len(ids))
	}
	if ids[0] != "a" || ids[1] != "z" {
		t.Errorf("Expected sorted [a, z], got %v", ids)
	}
}

// =============================================================================
// Metadata I/O
// =============================================================================

func TestReadMetadata_NonExistent(t *testing.T) {
	logger := logging.NewDevelopment()
	storage := NewStorage("/tmp/nonexistent_agg_test", logger)

	meta, err := storage.readMetadata("/tmp/nonexistent_agg_test/does_not_exist")
	if err != nil {
		t.Fatalf("readMetadata non-existent should not error: %v", err)
	}
	if meta != nil {
		t.Error("readMetadata non-existent should return nil")
	}
}

func TestReadMetadata_InvalidJSON(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agg_meta_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	// Write invalid JSON
	metaPath := filepath.Join(tmpDir, MetadataFile)
	if err := os.WriteFile(metaPath, []byte("not json"), 0o644); err != nil {
		t.Fatal(err)
	}

	logger := logging.NewDevelopment()
	storage := NewStorage(tmpDir, logger)

	_, err = storage.readMetadata(tmpDir)
	if err == nil {
		t.Error("readMetadata with invalid JSON should return error")
	}
}

func TestWriteMetadata_ReadBack(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agg_meta_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tmpDir, logger)

	meta := &PartitionMetadata{
		Version:  6,
		Level:    "1h",
		RowCount: 42,
		Fields:   []FieldMeta{{Name: "temp", Type: "float64", Position: 0}},
		DeviceGroups: []DeviceGroupInfo{
			{DirName: "dg_0000", GroupIndex: 0, DeviceNames: []string{"d1"}},
		},
		DeviceGroupMap: map[string]int{"d1": 0},
	}

	if err := storage.writeMetadata(tmpDir, meta); err != nil {
		t.Fatalf("writeMetadata failed: %v", err)
	}

	// Invalidate cache to force disk read
	storage.invalidateMetadataCache(tmpDir)

	readMeta, err := storage.readMetadata(tmpDir)
	if err != nil {
		t.Fatalf("readMetadata failed: %v", err)
	}
	if readMeta.RowCount != 42 {
		t.Errorf("RowCount = %d, want 42", readMeta.RowCount)
	}
	if readMeta.Version != 6 {
		t.Errorf("Version = %d, want 6", readMeta.Version)
	}
	if len(readMeta.Fields) != 1 {
		t.Errorf("Fields length = %d, want 1", len(readMeta.Fields))
	}
	if readMeta.DeviceGroupMap["d1"] != 0 {
		t.Errorf("DeviceGroupMap d1 = %d, want 0", readMeta.DeviceGroupMap["d1"])
	}
}

func TestReadMetadata_CacheHit(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agg_meta_cache_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tmpDir, logger)

	meta := &PartitionMetadata{Version: 6, RowCount: 10}
	if err := storage.writeMetadata(tmpDir, meta); err != nil {
		t.Fatal(err)
	}

	// First read populates cache
	m1, _ := storage.readMetadata(tmpDir)
	// Delete the file - second read should still work from cache
	if err := os.Remove(filepath.Join(tmpDir, MetadataFile)); err != nil {
		t.Fatal(err)
	}
	m2, _ := storage.readMetadata(tmpDir)

	if m1 != m2 {
		t.Error("Expected same pointer from cache")
	}
}

// =============================================================================
// DG Metadata I/O
// =============================================================================

func TestReadDGMetadata_NonExistent(t *testing.T) {
	logger := logging.NewDevelopment()
	storage := NewStorage("/tmp/nonexistent_agg_test", logger)

	meta, err := storage.readDGMetadata("/tmp/nonexistent_agg_test/dg_0000")
	if err != nil {
		t.Fatalf("readDGMetadata non-existent should not error: %v", err)
	}
	if meta != nil {
		t.Error("readDGMetadata non-existent should return nil")
	}
}

func TestReadDGMetadata_InvalidJSON(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agg_dgmeta_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	// Write invalid JSON
	metaPath := filepath.Join(tmpDir, MetadataFile)
	if err := os.WriteFile(metaPath, []byte("{invalid"), 0o644); err != nil {
		t.Fatal(err)
	}

	logger := logging.NewDevelopment()
	storage := NewStorage(tmpDir, logger)

	_, err = storage.readDGMetadata(tmpDir)
	if err == nil {
		t.Error("readDGMetadata with invalid JSON should return error")
	}
}

func TestWriteDGMetadata_ReadBack(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agg_dgmeta_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tmpDir, logger)

	dgMeta := &DGMetadata{
		Version:     6,
		GroupIndex:  0,
		DirName:     "dg_0000",
		DeviceNames: []string{"d1", "d2"},
		Parts: []PartInfo{
			{PartNum: 0, FileName: "part_0000.bin", RowCount: 100},
		},
		RowCount: 100,
	}

	if err := storage.writeDGMetadata(tmpDir, dgMeta); err != nil {
		t.Fatalf("writeDGMetadata failed: %v", err)
	}

	readMeta, err := storage.readDGMetadata(tmpDir)
	if err != nil {
		t.Fatalf("readDGMetadata failed: %v", err)
	}
	if readMeta.Version != 6 {
		t.Errorf("Version = %d, want 6", readMeta.Version)
	}
	if len(readMeta.Parts) != 1 {
		t.Errorf("Parts length = %d, want 1", len(readMeta.Parts))
	}
	if readMeta.RowCount != 100 {
		t.Errorf("RowCount = %d, want 100", readMeta.RowCount)
	}
}

// =============================================================================
// ColumnarData operations
// =============================================================================

func TestColumnarData_AddRowAllMetrics(t *testing.T) {
	cd := NewColumnarData(2)

	fields := map[string]*AggregatedField{
		"temp": {Sum: 100, Avg: 25, Min: 20, Max: 30, Count: 4},
	}

	cd.AddRowAllMetrics(1000, "dev1", fields)
	cd.AddRowAllMetrics(2000, "dev2", fields)

	if cd.RowCount() != 2 {
		t.Errorf("RowCount = %d, want 2", cd.RowCount())
	}
	if len(cd.FloatColumns) != 4 { // sum, avg, min, max
		t.Errorf("FloatColumns = %d, want 4", len(cd.FloatColumns))
	}
	if len(cd.IntColumns) != 3 { // count, min_time, max_time
		t.Errorf("IntColumns = %d, want 3", len(cd.IntColumns))
	}
}

func TestColumnarData_MultipleFields(t *testing.T) {
	cd := NewColumnarData(1)

	fields := map[string]*AggregatedField{
		"temp":     {Sum: 100, Avg: 25, Min: 20, Max: 30, Count: 4},
		"humidity": {Sum: 200, Avg: 50, Min: 40, Max: 60, Count: 4},
	}

	cd.AddRowAllMetrics(1000, "dev1", fields)

	// Should have 8 float columns (4 metrics * 2 fields) and 6 int columns (3 metrics * 2 fields)
	if len(cd.FloatColumns) != 8 {
		t.Errorf("FloatColumns = %d, want 8", len(cd.FloatColumns))
	}
	if len(cd.IntColumns) != 6 {
		t.Errorf("IntColumns = %d, want 6", len(cd.IntColumns))
	}
}

// =============================================================================
// Footer encode/decode
// =============================================================================

func TestEncodeDecodeV6AggFooter_RoundTrip(t *testing.T) {
	logger := logging.NewDevelopment()
	storage := NewStorage("/tmp", logger)

	footer := &V6AggPartFooter{
		DeviceNames:    []string{"device1", "device2"},
		FieldNames:     []string{"_time", "temperature", "humidity"},
		RowCountPerDev: []uint32{10, 5},
		Columns: []V6AggColumnEntry{
			{DeviceIdx: 0, FieldIdx: 0, MetricIdx: 0xFF, Offset: 64, Size: 100, RowCount: 10, ColumnType: 0},
			{DeviceIdx: 0, FieldIdx: 1, MetricIdx: 0, Offset: 164, Size: 200, RowCount: 10, ColumnType: 1},
			{DeviceIdx: 1, FieldIdx: 0, MetricIdx: 0xFF, Offset: 364, Size: 50, RowCount: 5, ColumnType: 0},
		},
	}

	encoded := storage.encodeV6AggFooter(footer)
	decoded, err := storage.decodeV6AggFooter(encoded)
	if err != nil {
		t.Fatalf("decodeV6AggFooter failed: %v", err)
	}

	if len(decoded.DeviceNames) != 2 {
		t.Errorf("DeviceNames length = %d, want 2", len(decoded.DeviceNames))
	}
	if decoded.DeviceNames[0] != "device1" || decoded.DeviceNames[1] != "device2" {
		t.Errorf("DeviceNames = %v", decoded.DeviceNames)
	}

	if len(decoded.FieldNames) != 3 {
		t.Errorf("FieldNames length = %d, want 3", len(decoded.FieldNames))
	}

	if len(decoded.Columns) != 3 {
		t.Errorf("Columns length = %d, want 3", len(decoded.Columns))
	}

	// Verify column entry values
	col := decoded.Columns[1]
	if col.DeviceIdx != 0 || col.FieldIdx != 1 || col.MetricIdx != 0 || col.Offset != 164 || col.Size != 200 || col.RowCount != 10 || col.ColumnType != 1 {
		t.Errorf("Column entry mismatch: %+v", col)
	}

	if decoded.RowCountPerDev[0] != 10 || decoded.RowCountPerDev[1] != 5 {
		t.Errorf("RowCountPerDev = %v", decoded.RowCountPerDev)
	}
}

func TestDecodeV6AggFooter_TooShort(t *testing.T) {
	logger := logging.NewDevelopment()
	storage := NewStorage("/tmp", logger)

	_, err := storage.decodeV6AggFooter([]byte{1, 2, 3})
	if err == nil {
		t.Error("Expected error for too-short footer data")
	}
}

func TestDecodeV6AggFooter_TruncatedDeviceName(t *testing.T) {
	logger := logging.NewDevelopment()
	storage := NewStorage("/tmp", logger)

	// Encode device count = 1, then truncated data
	data := make([]byte, 8)
	data[0] = 1  // 1 device
	data[4] = 10 // name length = 10 (but no actual name data)
	// only 8 bytes total, not enough for 10-byte name

	_, err := storage.decodeV6AggFooter(data)
	if err == nil {
		t.Error("Expected error for truncated device name")
	}
}

func TestDecodeV6AggFooter_EmptyFooter(t *testing.T) {
	logger := logging.NewDevelopment()
	storage := NewStorage("/tmp", logger)

	footer := &V6AggPartFooter{
		DeviceNames:    []string{},
		FieldNames:     []string{},
		RowCountPerDev: []uint32{},
		Columns:        []V6AggColumnEntry{},
	}

	encoded := storage.encodeV6AggFooter(footer)
	decoded, err := storage.decodeV6AggFooter(encoded)
	if err != nil {
		t.Fatalf("decodeV6AggFooter empty failed: %v", err)
	}
	if len(decoded.DeviceNames) != 0 || len(decoded.FieldNames) != 0 || len(decoded.Columns) != 0 {
		t.Error("Expected empty decoded footer")
	}
}

// =============================================================================
// V6 Part file write + read roundtrip
// =============================================================================

func TestWriteReadV6PartFile_RoundTrip(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agg_part_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tmpDir, logger)

	cd := NewColumnarData(3)
	cd.AddRowAllMetrics(1000000000, "dev1", map[string]*AggregatedField{
		"temp": {Sum: 100, Avg: 25, Min: 20, Max: 30, Count: 4},
	})
	cd.AddRowAllMetrics(2000000000, "dev1", map[string]*AggregatedField{
		"temp": {Sum: 200, Avg: 50, Min: 40, Max: 60, Count: 8},
	})
	cd.AddRowAllMetrics(3000000000, "dev2", map[string]*AggregatedField{
		"temp": {Sum: 150, Avg: 37.5, Min: 30, Max: 45, Count: 4},
	})

	pending, wErr := storage.writeV6PartFile(tmpDir, 0, AggregationHourly, cd)
	if wErr != nil {
		t.Fatalf("writeV6PartFile failed: %v", wErr)
	}
	if pending == nil {
		t.Fatal("pending should not be nil")
	}

	// Commit the write
	if err := storage.commitPendingWrites([]*pendingPartWrite{pending}); err != nil {
		t.Fatalf("commitPendingWrites failed: %v", err)
	}

	// Read back
	footer, fErr := storage.readV6PartFooter(pending.filePath)
	if fErr != nil {
		t.Fatalf("readV6PartFooter failed: %v", fErr)
	}
	if footer == nil {
		t.Fatal("footer should not be nil")
	}

	if len(footer.DeviceNames) != 2 {
		t.Errorf("DeviceNames = %d, want 2", len(footer.DeviceNames))
	}

	data, rErr := storage.readV6PartFileData(pending.filePath, footer, nil, nil, nil, math.MinInt64, math.MaxInt64)
	if rErr != nil {
		t.Fatalf("readV6PartFileData failed: %v", rErr)
	}

	if data.RowCount() != 3 {
		t.Errorf("RowCount = %d, want 3", data.RowCount())
	}

	// Verify timestamps
	if data.Timestamps[0] != 1000000000 || data.Timestamps[1] != 2000000000 || data.Timestamps[2] != 3000000000 {
		t.Errorf("Timestamps mismatch: %v", data.Timestamps)
	}
}

func TestWriteV6PartFile_EmptyData(t *testing.T) {
	logger := logging.NewDevelopment()
	storage := NewStorage("/tmp", logger)

	cd := NewColumnarData(0)
	pending, err := storage.writeV6PartFile("/tmp", 0, AggregationHourly, cd)
	if err != nil {
		t.Fatalf("writeV6PartFile empty should not error: %v", err)
	}
	if pending != nil {
		t.Error("pending should be nil for empty data")
	}
}

func TestReadV6PartFooter_NonExistent(t *testing.T) {
	logger := logging.NewDevelopment()
	storage := NewStorage("/tmp", logger)

	footer, err := storage.readV6PartFooter("/tmp/nonexistent_part.bin")
	if err != nil {
		t.Fatalf("readV6PartFooter non-existent should not error: %v", err)
	}
	if footer != nil {
		t.Error("footer should be nil for non-existent file")
	}
}

func TestReadV6PartFooter_TooSmall(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agg_part_small_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	// Create a file that's too small
	smallFile := filepath.Join(tmpDir, "small.bin")
	if err := os.WriteFile(smallFile, make([]byte, 10), 0o644); err != nil {
		t.Fatal(err)
	}

	logger := logging.NewDevelopment()
	storage := NewStorage(tmpDir, logger)

	_, err = storage.readV6PartFooter(smallFile)
	if err == nil {
		t.Error("readV6PartFooter too-small should return error")
	}
}

// =============================================================================
// Read with time filtering
// =============================================================================

func TestReadV6PartFileData_TimeFilter(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agg_timefilter_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tmpDir, logger)

	cd := NewColumnarData(5)
	for i := int64(1); i <= 5; i++ {
		cd.AddRowAllMetrics(i*1000, "dev1", map[string]*AggregatedField{
			"temp": {Sum: float64(i * 10), Avg: float64(i), Min: float64(i), Max: float64(i * 2), Count: 1},
		})
	}

	pending, _ := storage.writeV6PartFile(tmpDir, 0, AggregationHourly, cd)
	_ = storage.commitPendingWrites([]*pendingPartWrite{pending})

	footer, _ := storage.readV6PartFooter(pending.filePath)

	// Read only timestamps 2000-4000 (inclusive start, exclusive end)
	data, err := storage.readV6PartFileData(pending.filePath, footer, nil, nil, nil, 2000, 4000)
	if err != nil {
		t.Fatalf("readV6PartFileData with time filter failed: %v", err)
	}

	if data.RowCount() != 2 { // ts 2000, 3000
		t.Errorf("RowCount = %d, want 2 (timestamps 2000, 3000)", data.RowCount())
	}
}

func TestReadV6PartFileData_DeviceFilter(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agg_devfilter_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tmpDir, logger)

	cd := NewColumnarData(4)
	cd.AddRowAllMetrics(1000, "dev1", map[string]*AggregatedField{"temp": {Sum: 10, Count: 1}})
	cd.AddRowAllMetrics(2000, "dev2", map[string]*AggregatedField{"temp": {Sum: 20, Count: 1}})
	cd.AddRowAllMetrics(3000, "dev3", map[string]*AggregatedField{"temp": {Sum: 30, Count: 1}})
	cd.AddRowAllMetrics(4000, "dev1", map[string]*AggregatedField{"temp": {Sum: 40, Count: 1}})

	pending, _ := storage.writeV6PartFile(tmpDir, 0, AggregationHourly, cd)
	_ = storage.commitPendingWrites([]*pendingPartWrite{pending})

	footer, _ := storage.readV6PartFooter(pending.filePath)

	// Filter only dev1
	deviceFilter := map[string]struct{}{"dev1": {}}
	data, err := storage.readV6PartFileData(pending.filePath, footer, deviceFilter, nil, nil, math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("readV6PartFileData with device filter failed: %v", err)
	}

	if data.RowCount() != 2 { // dev1 has 2 rows
		t.Errorf("RowCount = %d, want 2", data.RowCount())
	}
}

func TestReadV6PartFileData_NoMatchingDevice(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agg_devfilter_none_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tmpDir, logger)

	cd := NewColumnarData(1)
	cd.AddRowAllMetrics(1000, "dev1", map[string]*AggregatedField{"temp": {Sum: 10, Count: 1}})

	pending, _ := storage.writeV6PartFile(tmpDir, 0, AggregationHourly, cd)
	_ = storage.commitPendingWrites([]*pendingPartWrite{pending})

	footer, _ := storage.readV6PartFooter(pending.filePath)

	// Filter for a device that doesn't exist
	deviceFilter := map[string]struct{}{"nonexistent": {}}
	data, err := storage.readV6PartFileData(pending.filePath, footer, deviceFilter, nil, nil, math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("readV6PartFileData failed: %v", err)
	}

	if data.RowCount() != 0 {
		t.Errorf("RowCount = %d, want 0", data.RowCount())
	}
}

func TestReadV6PartFileData_NoMatchingTime(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agg_timefilter_none_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tmpDir, logger)

	cd := NewColumnarData(1)
	cd.AddRowAllMetrics(1000, "dev1", map[string]*AggregatedField{"temp": {Sum: 10, Count: 1}})

	pending, _ := storage.writeV6PartFile(tmpDir, 0, AggregationHourly, cd)
	_ = storage.commitPendingWrites([]*pendingPartWrite{pending})

	footer, _ := storage.readV6PartFooter(pending.filePath)

	// Time range that doesn't match any rows
	data, err := storage.readV6PartFileData(pending.filePath, footer, nil, nil, nil, 5000, 6000)
	if err != nil {
		t.Fatalf("readV6PartFileData failed: %v", err)
	}

	if data.RowCount() != 0 {
		t.Errorf("RowCount = %d, want 0", data.RowCount())
	}
}

// =============================================================================
// mergeColumnarDataForWrite
// =============================================================================

func TestMergeColumnarDataForWrite(t *testing.T) {
	logger := logging.NewDevelopment()
	storage := NewStorage("/tmp", logger)

	existing := NewColumnarData(2)
	existing.AddRowAllMetrics(1000, "dev1", map[string]*AggregatedField{
		"temp": {Sum: 10, Avg: 5, Min: 1, Max: 9, Count: 2},
	})
	existing.AddRowAllMetrics(2000, "dev1", map[string]*AggregatedField{
		"temp": {Sum: 20, Avg: 10, Min: 5, Max: 15, Count: 2},
	})

	newData := NewColumnarData(1)
	newData.AddRowAllMetrics(3000, "dev2", map[string]*AggregatedField{
		"temp": {Sum: 30, Avg: 15, Min: 10, Max: 20, Count: 2},
	})

	merged := storage.mergeColumnarDataForWrite(existing, newData)

	if merged.RowCount() != 3 {
		t.Errorf("RowCount = %d, want 3", merged.RowCount())
	}
	if merged.Timestamps[0] != 1000 || merged.Timestamps[2] != 3000 {
		t.Errorf("Timestamps = %v", merged.Timestamps)
	}
	if merged.DeviceIDs[0] != "dev1" || merged.DeviceIDs[2] != "dev2" {
		t.Errorf("DeviceIDs = %v", merged.DeviceIDs)
	}
}

func TestMergeColumnarDataForWrite_DifferentFields(t *testing.T) {
	logger := logging.NewDevelopment()
	storage := NewStorage("/tmp", logger)

	existing := NewColumnarData(1)
	existing.AddRowAllMetrics(1000, "dev1", map[string]*AggregatedField{
		"temp": {Sum: 10, Count: 1},
	})

	newData := NewColumnarData(1)
	newData.AddRowAllMetrics(2000, "dev1", map[string]*AggregatedField{
		"humidity": {Sum: 60, Count: 1},
	})

	merged := storage.mergeColumnarDataForWrite(existing, newData)

	if merged.RowCount() != 2 {
		t.Errorf("RowCount = %d, want 2", merged.RowCount())
	}

	// Both temp and humidity columns should exist with proper zero-padding
	sumTempKey := v6FloatKey(MetricSum, "temp")
	sumHumKey := v6FloatKey(MetricSum, "humidity")

	if len(merged.FloatColumns[sumTempKey]) != 2 {
		t.Errorf("temp column length = %d, want 2", len(merged.FloatColumns[sumTempKey]))
	}
	if len(merged.FloatColumns[sumHumKey]) != 2 {
		t.Errorf("humidity column length = %d, want 2", len(merged.FloatColumns[sumHumKey]))
	}
}

// =============================================================================
// sortColumnarDataByTimestamp
// =============================================================================

func TestSortColumnarDataByTimestamp(t *testing.T) {
	logger := logging.NewDevelopment()
	storage := NewStorage("/tmp", logger)

	cd := NewColumnarData(3)
	cd.AddRowAllMetrics(3000, "dev1", map[string]*AggregatedField{"temp": {Sum: 30, Count: 3}})
	cd.AddRowAllMetrics(1000, "dev2", map[string]*AggregatedField{"temp": {Sum: 10, Count: 1}})
	cd.AddRowAllMetrics(2000, "dev1", map[string]*AggregatedField{"temp": {Sum: 20, Count: 2}})

	storage.sortColumnarDataByTimestamp(cd)

	if cd.Timestamps[0] != 1000 || cd.Timestamps[1] != 2000 || cd.Timestamps[2] != 3000 {
		t.Errorf("Timestamps not sorted: %v", cd.Timestamps)
	}
	if cd.DeviceIDs[0] != "dev2" || cd.DeviceIDs[1] != "dev1" || cd.DeviceIDs[2] != "dev1" {
		t.Errorf("DeviceIDs not reordered: %v", cd.DeviceIDs)
	}

	// Verify float columns are reordered
	sumKey := v6FloatKey(MetricSum, "temp")
	vals := cd.FloatColumns[sumKey]
	if vals[0] != 10 || vals[1] != 20 || vals[2] != 30 {
		t.Errorf("Float values not reordered: %v", vals)
	}

	// Verify int columns are reordered
	countKey := v6IntKey("temp")
	ivals := cd.IntColumns[countKey]
	if ivals[0] != 1 || ivals[1] != 2 || ivals[2] != 3 {
		t.Errorf("Int values not reordered: %v", ivals)
	}
}

func TestSortColumnarDataByTimestamp_SingleRow(t *testing.T) {
	logger := logging.NewDevelopment()
	storage := NewStorage("/tmp", logger)

	cd := NewColumnarData(1)
	cd.AddRowAllMetrics(1000, "dev1", map[string]*AggregatedField{"temp": {Sum: 10, Count: 1}})

	// Should not panic
	storage.sortColumnarDataByTimestamp(cd)

	if cd.Timestamps[0] != 1000 {
		t.Errorf("Timestamp changed: %d", cd.Timestamps[0])
	}
}

func TestSortColumnarDataByTimestamp_Empty(t *testing.T) {
	logger := logging.NewDevelopment()
	storage := NewStorage("/tmp", logger)

	cd := NewColumnarData(0)

	// Should not panic
	storage.sortColumnarDataByTimestamp(cd)
}

// =============================================================================
// sliceColumnarData
// =============================================================================

func TestSliceColumnarData(t *testing.T) {
	logger := logging.NewDevelopment()
	storage := NewStorage("/tmp", logger)

	cd := NewColumnarData(5)
	for i := int64(0); i < 5; i++ {
		cd.AddRowAllMetrics(i*1000, "dev1", map[string]*AggregatedField{
			"temp": {Sum: float64(i * 10), Count: i + 1},
		})
	}

	sliced := storage.sliceColumnarData(cd, 1, 4)

	if sliced.RowCount() != 3 {
		t.Errorf("RowCount = %d, want 3", sliced.RowCount())
	}
	if sliced.Timestamps[0] != 1000 || sliced.Timestamps[2] != 3000 {
		t.Errorf("Timestamps = %v", sliced.Timestamps)
	}

	sumKey := v6FloatKey(MetricSum, "temp")
	if sliced.FloatColumns[sumKey][0] != 10 || sliced.FloatColumns[sumKey][2] != 30 {
		t.Errorf("Float values = %v", sliced.FloatColumns[sumKey])
	}

	countKey := v6IntKey("temp")
	if sliced.IntColumns[countKey][0] != 2 || sliced.IntColumns[countKey][2] != 4 {
		t.Errorf("Int values = %v", sliced.IntColumns[countKey])
	}
}

func TestSliceColumnarData_FullRange(t *testing.T) {
	logger := logging.NewDevelopment()
	storage := NewStorage("/tmp", logger)

	cd := NewColumnarData(2)
	cd.AddRowAllMetrics(1000, "dev1", map[string]*AggregatedField{"temp": {Sum: 10, Count: 1}})
	cd.AddRowAllMetrics(2000, "dev2", map[string]*AggregatedField{"temp": {Sum: 20, Count: 2}})

	sliced := storage.sliceColumnarData(cd, 0, 2)
	if sliced.RowCount() != 2 {
		t.Errorf("RowCount = %d, want 2", sliced.RowCount())
	}
}

// =============================================================================
// commitPendingWrites / cleanupPendingWrites
// =============================================================================

func TestCommitPendingWrites_WithNilEntry(t *testing.T) {
	logger := logging.NewDevelopment()
	storage := NewStorage("/tmp", logger)

	// Should handle nil entries gracefully
	err := storage.commitPendingWrites([]*pendingPartWrite{nil, nil})
	if err != nil {
		t.Errorf("commitPendingWrites with nil entries should not error: %v", err)
	}
}

func TestCommitPendingWrites_Success(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agg_commit_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tmpDir, logger)

	// Create a temp file
	tmpFile := filepath.Join(tmpDir, "test.tmp")
	finalFile := filepath.Join(tmpDir, "test.bin")
	if err := os.WriteFile(tmpFile, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	pending := &pendingPartWrite{
		tmpPath:  tmpFile,
		filePath: finalFile,
	}

	if err := storage.commitPendingWrites([]*pendingPartWrite{pending}); err != nil {
		t.Fatalf("commitPendingWrites failed: %v", err)
	}

	// Verify final file exists
	if _, err := os.Stat(finalFile); os.IsNotExist(err) {
		t.Error("Final file should exist after commit")
	}
	// Verify tmp file no longer exists
	if _, err := os.Stat(tmpFile); !os.IsNotExist(err) {
		t.Error("Tmp file should be removed after commit")
	}
}

func TestCleanupPendingWrites(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agg_cleanup_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tmpDir, logger)

	// Create temp files
	tmpFile1 := filepath.Join(tmpDir, "test1.tmp")
	tmpFile2 := filepath.Join(tmpDir, "test2.tmp")
	if err := os.WriteFile(tmpFile1, []byte("data1"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tmpFile2, []byte("data2"), 0o644); err != nil {
		t.Fatal(err)
	}

	pending := []*pendingPartWrite{
		{tmpPath: tmpFile1, filePath: filepath.Join(tmpDir, "test1.bin")},
		nil, // nil entry should be handled
		{tmpPath: tmpFile2, filePath: filepath.Join(tmpDir, "test2.bin")},
	}

	storage.cleanupPendingWrites(pending)

	// Verify tmp files are removed
	if _, err := os.Stat(tmpFile1); !os.IsNotExist(err) {
		t.Error("tmpFile1 should be removed")
	}
	if _, err := os.Stat(tmpFile2); !os.IsNotExist(err) {
		t.Error("tmpFile2 should be removed")
	}
}

// =============================================================================
// getPartFileName / getDeviceGroupDir
// =============================================================================

func TestGetPartFileName(t *testing.T) {
	logger := logging.NewDevelopment()
	storage := NewStorage("/tmp", logger)

	if got := storage.getPartFileName(0); got != "part_0000.bin" {
		t.Errorf("getPartFileName(0) = %q", got)
	}
	if got := storage.getPartFileName(9999); got != "part_9999.bin" {
		t.Errorf("getPartFileName(9999) = %q", got)
	}
}

func TestGetDeviceGroupDir(t *testing.T) {
	if got := getDeviceGroupDir(0); got != "dg_0000" {
		t.Errorf("getDeviceGroupDir(0) = %q", got)
	}
	if got := getDeviceGroupDir(42); got != "dg_0042" {
		t.Errorf("getDeviceGroupDir(42) = %q", got)
	}
}

// =============================================================================
// DefaultStorageConfig
// =============================================================================

func TestDefaultStorageConfig(t *testing.T) {
	cfg := DefaultStorageConfig()
	if cfg.MaxRowsPerPart != 10000 {
		t.Errorf("MaxRowsPerPart = %d, want 10000", cfg.MaxRowsPerPart)
	}
	if cfg.MaxPartSize != 64*1024*1024 {
		t.Errorf("MaxPartSize = %d, want 64MB", cfg.MaxPartSize)
	}
	if cfg.MaxDevicesPerGroup != DefaultMaxDevicesPerGroup {
		t.Errorf("MaxDevicesPerGroup = %d, want %d", cfg.MaxDevicesPerGroup, DefaultMaxDevicesPerGroup)
	}
}

// =============================================================================
// Constants
// =============================================================================

func TestV6Constants(t *testing.T) {
	if AggPartMagic != 0x41474750 {
		t.Errorf("AggPartMagic = 0x%X, want 0x41474750", AggPartMagic)
	}
	if AggPartVersion != 6 {
		t.Errorf("AggPartVersion = %d, want 6", AggPartVersion)
	}
	if V6AggHeaderSize != 64 {
		t.Errorf("V6AggHeaderSize = %d, want 64", V6AggHeaderSize)
	}
	if MetadataFile != "_metadata.idx" {
		t.Errorf("MetadataFile = %q, want _metadata.idx", MetadataFile)
	}
}

// =============================================================================
// PartInfo JSON serialization
// =============================================================================

func TestPartInfoJSON(t *testing.T) {
	pi := PartInfo{
		PartNum:  1,
		FileName: "part_0001.bin",
		RowCount: 500,
		MinTime:  1000,
		MaxTime:  5000,
		Size:     1024,
		Checksum: 12345,
	}

	data, err := json.Marshal(pi)
	if err != nil {
		t.Fatal(err)
	}

	var decoded PartInfo
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}

	if decoded != pi {
		t.Errorf("PartInfo roundtrip failed: %+v != %+v", decoded, pi)
	}
}

// =============================================================================
// PartitionMetadata JSON serialization
// =============================================================================

func TestPartitionMetadataJSON(t *testing.T) {
	meta := PartitionMetadata{
		Version:        6,
		Level:          "1h",
		RowCount:       100,
		DeviceCount:    5,
		MinTime:        1000,
		MaxTime:        9000,
		Fields:         []FieldMeta{{Name: "temp", Type: "float64", Position: 0}},
		DeviceGroups:   []DeviceGroupInfo{{DirName: "dg_0000", GroupIndex: 0, DeviceNames: []string{"d1"}}},
		DeviceGroupMap: map[string]int{"d1": 0},
	}

	data, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}

	var decoded PartitionMetadata
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}

	if decoded.Version != meta.Version || decoded.RowCount != meta.RowCount {
		t.Errorf("PartitionMetadata roundtrip failed")
	}
}

// =============================================================================
// V6AggPartFooter structure
// =============================================================================

func TestV6AggColumnEntry_MetricIdx0xFF(t *testing.T) {
	// 0xFF is used for _time column
	entry := V6AggColumnEntry{
		DeviceIdx:  0,
		FieldIdx:   0,
		MetricIdx:  0xFF,
		Offset:     64,
		Size:       100,
		RowCount:   10,
		ColumnType: 0,
	}

	if entry.MetricIdx != 0xFF {
		t.Errorf("MetricIdx = %d, want 0xFF", entry.MetricIdx)
	}
}

// =============================================================================
// Write with PartInfo verification
// =============================================================================

func TestWriteV6PartFile_PartInfo(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agg_partinfo_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tmpDir, logger)

	cd := NewColumnarData(2)
	cd.AddRowAllMetrics(1000, "dev1", map[string]*AggregatedField{
		"temp": {Sum: 10, Avg: 5, Min: 1, Max: 9, Count: 2},
	})
	cd.AddRowAllMetrics(5000, "dev1", map[string]*AggregatedField{
		"temp": {Sum: 50, Avg: 25, Min: 10, Max: 40, Count: 2},
	})

	pending, err := storage.writeV6PartFile(tmpDir, 7, AggregationHourly, cd)
	if err != nil {
		t.Fatalf("writeV6PartFile failed: %v", err)
	}

	pi := pending.partInfo
	if pi.PartNum != 7 {
		t.Errorf("PartNum = %d, want 7", pi.PartNum)
	}
	if pi.FileName != "part_0007.bin" {
		t.Errorf("FileName = %q", pi.FileName)
	}
	if pi.RowCount != 2 {
		t.Errorf("RowCount = %d, want 2", pi.RowCount)
	}
	if pi.MinTime != 1000 {
		t.Errorf("MinTime = %d, want 1000", pi.MinTime)
	}
	if pi.MaxTime != 5000 {
		t.Errorf("MaxTime = %d, want 5000", pi.MaxTime)
	}
	if pi.Size <= 0 {
		t.Errorf("Size = %d, should be > 0", pi.Size)
	}
}
