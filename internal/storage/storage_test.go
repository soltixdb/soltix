package storage

import (
	"os"
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/compression"
	"github.com/soltixdb/soltix/internal/logging"
)

func TestNewStorage(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "multipart_storage_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, compression.Snappy, logger)

	if storage == nil {
		t.Fatal("NewStorage returned nil")
		return
	}
	if storage.dataDir != tempDir {
		t.Errorf("dataDir = %q, expected %q", storage.dataDir, tempDir)
	}
	if storage.timezone != time.UTC {
		t.Errorf("timezone = %v, expected UTC", storage.timezone)
	}
}

func TestNewStorageWithConfig(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "multipart_storage_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorageWithConfig(tempDir, compression.Snappy, logger, 5000, 32*1024*1024, 500, 25)

	if storage == nil {
		t.Fatal("NewStorageWithConfig returned nil")
		return
	}
	if storage.maxRowsPerPart != 5000 {
		t.Errorf("maxRowsPerPart = %d, expected 5000", storage.maxRowsPerPart)
	}
	if storage.maxPartSize != 32*1024*1024 {
		t.Errorf("maxPartSize = %d, expected %d", storage.maxPartSize, 32*1024*1024)
	}
	if storage.minRowsPerPart != 500 {
		t.Errorf("minRowsPerPart = %d, expected 500", storage.minRowsPerPart)
	}
	if storage.maxDevicesPerGroup != 25 {
		t.Errorf("maxDevicesPerGroup = %d, expected 25", storage.maxDevicesPerGroup)
	}
}

func TestNewStorageWithConfig_Defaults(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "multipart_storage_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	// Pass zero values to trigger defaults
	storage := NewStorageWithConfig(tempDir, compression.Snappy, logger, 0, 0, 0, 0)

	if storage.maxRowsPerPart != DefaultMaxRowsPerPart {
		t.Errorf("maxRowsPerPart = %d, expected %d", storage.maxRowsPerPart, DefaultMaxRowsPerPart)
	}
	if storage.maxPartSize != DefaultMaxPartSize {
		t.Errorf("maxPartSize = %d, expected %d", storage.maxPartSize, DefaultMaxPartSize)
	}
	if storage.minRowsPerPart != DefaultMinRowsPerPart {
		t.Errorf("minRowsPerPart = %d, expected %d", storage.minRowsPerPart, DefaultMinRowsPerPart)
	}
	if storage.maxDevicesPerGroup != DefaultMaxDevicesPerGroup {
		t.Errorf("maxDevicesPerGroup = %d, expected %d", storage.maxDevicesPerGroup, DefaultMaxDevicesPerGroup)
	}
}

func TestStorage_SetTimezone(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "multipart_storage_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, compression.Snappy, logger)

	// Default is UTC
	if storage.timezone != time.UTC {
		t.Errorf("Default timezone = %v, expected UTC", storage.timezone)
	}

	// Set to different timezone
	tokyo, _ := time.LoadLocation("Asia/Tokyo")
	storage.SetTimezone(tokyo)

	if storage.timezone != tokyo {
		t.Errorf("timezone = %v, expected Asia/Tokyo", storage.timezone)
	}

	// Setting nil should not change timezone
	storage.SetTimezone(nil)
	if storage.timezone != tokyo {
		t.Errorf("timezone should remain Asia/Tokyo after setting nil, got %v", storage.timezone)
	}
}

func TestStorage_SetRowGroupSize(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "multipart_storage_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, compression.Snappy, logger)

	// Default value
	if storage.rowGroupSize != DefaultRowGroupSize {
		t.Errorf("Default rowGroupSize = %d, expected %d", storage.rowGroupSize, DefaultRowGroupSize)
	}

	// Set new value
	storage.SetRowGroupSize(5000)
	if storage.rowGroupSize != 5000 {
		t.Errorf("rowGroupSize = %d, expected 5000", storage.rowGroupSize)
	}

	// Setting zero or negative should not change
	storage.SetRowGroupSize(0)
	if storage.rowGroupSize != 5000 {
		t.Errorf("rowGroupSize should remain 5000 after setting 0, got %d", storage.rowGroupSize)
	}

	storage.SetRowGroupSize(-1)
	if storage.rowGroupSize != 5000 {
		t.Errorf("rowGroupSize should remain 5000 after setting -1, got %d", storage.rowGroupSize)
	}
}

func TestStorage_WriteBatchEmpty(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "multipart_storage_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, compression.Snappy, logger)

	// Write empty batch should succeed
	err = storage.WriteBatch([]*DataPoint{})
	if err != nil {
		t.Fatalf("WriteBatch with empty entries failed: %v", err)
	}
}

func TestStorage_WriteBatch(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "multipart_storage_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, compression.Snappy, logger)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	entries := []*DataPoint{
		{
			ID:         "device1",
			Database:   "testdb",
			Collection: "metrics",
			Time:       now,
			Fields: map[string]interface{}{
				"temperature": 25.5,
				"humidity":    60.0,
			},
		},
		{
			ID:         "device2",
			Database:   "testdb",
			Collection: "metrics",
			Time:       now.Add(time.Minute),
			Fields: map[string]interface{}{
				"temperature": 26.0,
				"humidity":    55.0,
			},
		},
	}

	err = storage.WriteBatch(entries)
	if err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}
}

func TestStorage_Query(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "multipart_storage_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, compression.Snappy, logger)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	// Write some data first
	entries := []*DataPoint{
		{
			ID:         "device1",
			Database:   "testdb",
			Collection: "metrics",
			Time:       now,
			Fields: map[string]interface{}{
				"temperature": 25.5,
			},
		},
		{
			ID:         "device1",
			Database:   "testdb",
			Collection: "metrics",
			Time:       now.Add(time.Hour),
			Fields: map[string]interface{}{
				"temperature": 26.0,
			},
		},
	}

	err = storage.WriteBatch(entries)
	if err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	// Query data back
	result, err := storage.Query("testdb", "metrics", nil, now, now.Add(2*time.Hour), nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(result) != 2 {
		t.Errorf("Query returned %d results, expected 2", len(result))
	}
}

func TestStorage_QueryWithDeviceFilter(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "multipart_storage_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, compression.Snappy, logger)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	// Write data for multiple devices
	entries := []*DataPoint{
		{
			ID:         "device1",
			Database:   "testdb",
			Collection: "metrics",
			Time:       now,
			Fields:     map[string]interface{}{"temp": 25.0},
		},
		{
			ID:         "device2",
			Database:   "testdb",
			Collection: "metrics",
			Time:       now,
			Fields:     map[string]interface{}{"temp": 26.0},
		},
	}

	err = storage.WriteBatch(entries)
	if err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	// Query with device filter
	result, err := storage.Query("testdb", "metrics", []string{"device1"}, now, now.Add(time.Hour), nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(result) != 1 {
		t.Errorf("Query returned %d results, expected 1", len(result))
	}
	if len(result) > 0 && result[0].ID != "device1" {
		t.Errorf("Expected device1, got %s", result[0].ID)
	}
}

func TestStorage_QueryNonExistent(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "multipart_storage_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, compression.Snappy, logger)

	// Query non-existent data
	result, err := storage.Query("nonexistent", "collection", nil, time.Now(), time.Now().Add(time.Hour), nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(result) != 0 {
		t.Errorf("Query returned %d results, expected 0", len(result))
	}
}

func TestStorageConstants(t *testing.T) {
	// Test constants are defined correctly
	if MetadataIndexMagic != 0x58444953 {
		t.Errorf("MetadataIndexMagic = %x, expected %x", MetadataIndexMagic, 0x58444953)
	}
	if StorageVersion != 6 {
		t.Errorf("StorageVersion = %d, expected 6", StorageVersion)
	}
	if DefaultMaxRowsPerPart != 100_000 {
		t.Errorf("DefaultMaxRowsPerPart = %d, expected 100000", DefaultMaxRowsPerPart)
	}
	if DefaultMaxPartSize != 64*1024*1024 {
		t.Errorf("DefaultMaxPartSize = %d, expected %d", DefaultMaxPartSize, 64*1024*1024)
	}
	if DefaultMinRowsPerPart != 1000 {
		t.Errorf("DefaultMinRowsPerPart = %d, expected 1000", DefaultMinRowsPerPart)
	}
	if DefaultRowGroupSize != 10_000 {
		t.Errorf("DefaultRowGroupSize = %d, expected 10000", DefaultRowGroupSize)
	}
	if DefaultMaxDevicesPerGroup != 50 {
		t.Errorf("DefaultMaxDevicesPerGroup = %d, expected 50", DefaultMaxDevicesPerGroup)
	}
	if DeviceGroupMetadataMagic != 0x58474453 {
		t.Errorf("DeviceGroupMetadataMagic = %x, expected %x", DeviceGroupMetadataMagic, 0x58474453)
	}
}

func TestMetadataIndex(t *testing.T) {
	metadata := MetadataIndex{
		Magic:        MetadataIndexMagic,
		Version:      StorageVersion,
		Compression:  compression.Snappy,
		TotalRows:    10000,
		GroupCount:   2,
		DeviceCount:  5,
		FieldCount:   3,
		MinTimestamp: 1000000,
		MaxTimestamp: 2000000,
		FieldNames:   []string{"humidity", "pressure", "temp"},
		FieldTypes:   []compression.ColumnType{compression.ColumnTypeFloat64, compression.ColumnTypeFloat64, compression.ColumnTypeFloat64},
		DeviceNames:  []string{"dev1", "dev2", "dev3", "dev4", "dev5"},
		DeviceGroups: []DeviceGroupManifest{
			{
				DirName:     "dg_0000",
				GroupIndex:  0,
				DeviceCount: 3,
				PartCount:   1,
				RowCount:    6000,
				DeviceNames: []string{"dev1", "dev2", "dev3"},
			},
			{
				DirName:     "dg_0001",
				GroupIndex:  1,
				DeviceCount: 2,
				PartCount:   1,
				RowCount:    4000,
				DeviceNames: []string{"dev4", "dev5"},
			},
		},
		DeviceGroupMap: map[string]int{
			"dev1": 0,
			"dev2": 0,
			"dev3": 0,
			"dev4": 1,
			"dev5": 1,
		},
	}

	if metadata.Magic != MetadataIndexMagic {
		t.Errorf("Magic = %x, expected %x", metadata.Magic, MetadataIndexMagic)
	}
	if metadata.Version != 6 {
		t.Errorf("Version = %d, expected 6", metadata.Version)
	}
	if metadata.TotalRows != 10000 {
		t.Errorf("TotalRows = %d, expected 10000", metadata.TotalRows)
	}
	if metadata.GroupCount != 2 {
		t.Errorf("GroupCount = %d, expected 2", metadata.GroupCount)
	}
	if len(metadata.FieldNames) != 3 {
		t.Errorf("FieldNames length = %d, expected 3", len(metadata.FieldNames))
	}
	if len(metadata.DeviceGroups) != 2 {
		t.Errorf("DeviceGroups length = %d, expected 2", len(metadata.DeviceGroups))
	}
	if len(metadata.DeviceGroupMap) != 5 {
		t.Errorf("DeviceGroupMap length = %d, expected 5", len(metadata.DeviceGroupMap))
	}
	if metadata.DeviceGroupMap["dev1"] != 0 {
		t.Errorf("DeviceGroupMap[dev1] = %d, expected 0", metadata.DeviceGroupMap["dev1"])
	}
	if metadata.DeviceGroupMap["dev5"] != 1 {
		t.Errorf("DeviceGroupMap[dev5] = %d, expected 1", metadata.DeviceGroupMap["dev5"])
	}
}

func TestQueryWithFieldProjection(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "multipart_query_proj")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, compression.Snappy, logger)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	entries := []*DataPoint{
		{
			ID:         "device1",
			Database:   "testdb",
			Collection: "metrics",
			Time:       now,
			Fields: map[string]interface{}{
				"temp":     25.5,
				"humidity": 60.0,
				"pressure": 1013.0,
			},
		},
	}

	err = storage.WriteBatch(entries)
	if err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	// Query only "temp" field
	result, err := storage.Query("testdb", "metrics", nil, now, now.Add(time.Hour), []string{"temp"})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(result) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(result))
	}

	if _, ok := result[0].Fields["temp"]; !ok {
		t.Error("Expected 'temp' field in result")
	}
}
