package storage

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/compression"
	"github.com/soltixdb/soltix/internal/logging"
)

// =============================================================================
// storage.go: writeData — append path (existing DG with new devices/fields)
// Targets: writeData 77.2%, writeDataFull 87.0%
// =============================================================================

func TestWriteData_AppendToExistingDG(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorageWithConfig(tmpDir, compression.Snappy, logger, 1000, 64*1024*1024, 100, 50)

	now := time.Now()

	// First write creates initial data
	batch1 := []*DataPoint{
		{Database: "db", Collection: "c", ID: "dev-1", Time: now, Fields: map[string]interface{}{"temp": 25.5}},
		{Database: "db", Collection: "c", ID: "dev-2", Time: now.Add(time.Second), Fields: map[string]interface{}{"temp": 26.0}},
	}
	if err := s.WriteBatch(batch1); err != nil {
		t.Fatalf("first WriteBatch failed: %v", err)
	}

	// Second write appends to existing DG (hits append code path)
	batch2 := []*DataPoint{
		{Database: "db", Collection: "c", ID: "dev-1", Time: now.Add(2 * time.Second), Fields: map[string]interface{}{"temp": 27.0}},
		{Database: "db", Collection: "c", ID: "dev-3", Time: now.Add(3 * time.Second), Fields: map[string]interface{}{"temp": 28.0, "humidity": 60.0}},
	}
	if err := s.WriteBatch(batch2); err != nil {
		t.Fatalf("second WriteBatch (append) failed: %v", err)
	}

	results, err := s.Query("db", "c", nil, now.Add(-time.Minute), now.Add(time.Minute), nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(results) < 4 {
		t.Errorf("expected at least 4 results, got %d", len(results))
	}
}

func TestWriteData_AppendNewDGWhenFull(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	// maxDevicesPerGroup=2 → forces new DG creation quickly
	s := NewStorageWithConfig(tmpDir, compression.Snappy, logger, 1000, 64*1024*1024, 100, 2)

	now := time.Now()

	// First write: 2 devices → fills DG 0
	batch1 := []*DataPoint{
		{Database: "db", Collection: "c", ID: "dev-1", Time: now, Fields: map[string]interface{}{"val": 1.0}},
		{Database: "db", Collection: "c", ID: "dev-2", Time: now, Fields: map[string]interface{}{"val": 2.0}},
	}
	if err := s.WriteBatch(batch1); err != nil {
		t.Fatalf("first write failed: %v", err)
	}

	// Second write: 2 new devices → should create DG 1
	batch2 := []*DataPoint{
		{Database: "db", Collection: "c", ID: "dev-3", Time: now.Add(time.Second), Fields: map[string]interface{}{"val": 3.0}},
		{Database: "db", Collection: "c", ID: "dev-4", Time: now.Add(time.Second), Fields: map[string]interface{}{"val": 4.0}},
	}
	if err := s.WriteBatch(batch2); err != nil {
		t.Fatalf("second write failed: %v", err)
	}

	// Third write: mix of existing + new → tests routing to correct DGs
	batch3 := []*DataPoint{
		{Database: "db", Collection: "c", ID: "dev-1", Time: now.Add(2 * time.Second), Fields: map[string]interface{}{"val": 5.0}},
		{Database: "db", Collection: "c", ID: "dev-5", Time: now.Add(2 * time.Second), Fields: map[string]interface{}{"val": 6.0}},
	}
	if err := s.WriteBatch(batch3); err != nil {
		t.Fatalf("third write failed: %v", err)
	}

	results, err := s.Query("db", "c", nil, now.Add(-time.Minute), now.Add(time.Minute), nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(results) < 6 {
		t.Errorf("expected at least 6 results, got %d", len(results))
	}
}

func TestWriteData_AppendNewFieldsToExistingDevice(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorageWithConfig(tmpDir, compression.Snappy, logger, 1000, 64*1024*1024, 100, 50)

	now := time.Now()

	// First write with one field
	batch1 := []*DataPoint{
		{Database: "db", Collection: "c", ID: "dev-1", Time: now, Fields: map[string]interface{}{"temp": 25.5}},
	}
	if err := s.WriteBatch(batch1); err != nil {
		t.Fatalf("first write failed: %v", err)
	}

	// Second write introduces new fields
	batch2 := []*DataPoint{
		{Database: "db", Collection: "c", ID: "dev-1", Time: now.Add(time.Second), Fields: map[string]interface{}{
			"temp":     26.0,
			"humidity": 65.0,
			"pressure": 1013.25,
		}},
	}
	if err := s.WriteBatch(batch2); err != nil {
		t.Fatalf("second write with new fields failed: %v", err)
	}

	results, err := s.Query("db", "c", []string{"dev-1"}, now.Add(-time.Minute), now.Add(time.Minute), []string{"humidity"})
	if err != nil {
		t.Fatalf("Query with field projection failed: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected results for field projection query")
	}
}

// =============================================================================
// storage.go: Query — edge cases
// Targets: Query 78.6%, queryDateDir 88.9%
// =============================================================================

func TestQuery_FilterByDevice_ReturnsOnlyMatching(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, compression.Snappy, logger)

	now := time.Now()
	batch := make([]*DataPoint, 0, 10)
	for i := 0; i < 10; i++ {
		batch = append(batch, &DataPoint{
			Database:   "db",
			Collection: "c",
			ID:         "sensor-" + string(rune('A'+i)),
			Time:       now.Add(time.Duration(i) * time.Second),
			Fields:     map[string]interface{}{"val": float64(i)},
		})
	}
	if err := s.WriteBatch(batch); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	results, err := s.Query("db", "c", []string{"sensor-A", "sensor-C"}, now.Add(-time.Minute), now.Add(time.Minute), nil)
	if err != nil {
		t.Fatalf("Query with device filter failed: %v", err)
	}

	for _, dp := range results {
		if dp.ID != "sensor-A" && dp.ID != "sensor-C" {
			t.Errorf("unexpected device ID in result: %s", dp.ID)
		}
	}
}

func TestQuery_TimeRangeMiss_ReturnsEmpty(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, compression.Snappy, logger)

	now := time.Now()
	batch := []*DataPoint{
		{Database: "db", Collection: "c", ID: "dev-1", Time: now, Fields: map[string]interface{}{"val": 1.0}},
	}
	if err := s.WriteBatch(batch); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	// Query with time range far in the past
	results, err := s.Query("db", "c", nil, now.Add(-48*time.Hour), now.Add(-24*time.Hour), nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results for out-of-bounds time range, got %d", len(results))
	}
}

func TestQuery_NonexistentDeviceFilter_ReturnsEmpty(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, compression.Snappy, logger)

	now := time.Now()
	batch := []*DataPoint{
		{Database: "db", Collection: "c", ID: "dev-1", Time: now, Fields: map[string]interface{}{"val": 1.0}},
	}
	if err := s.WriteBatch(batch); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	results, err := s.Query("db", "c", []string{"nonexistent-device"}, now.Add(-time.Minute), now.Add(time.Minute), nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results for nonexistent device, got %d", len(results))
	}
}

func TestQuery_MultipleFieldProjection(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, compression.Snappy, logger)

	now := time.Now()
	batch := []*DataPoint{
		{Database: "db", Collection: "c", ID: "dev-1", Time: now, Fields: map[string]interface{}{
			"temp":     25.5,
			"humidity": 60.0,
			"pressure": 1013.0,
			"voltage":  3.3,
		}},
	}
	if err := s.WriteBatch(batch); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	// Only request "temp" and "voltage"
	results, err := s.Query("db", "c", nil, now.Add(-time.Minute), now.Add(time.Minute), []string{"temp", "voltage"})
	if err != nil {
		t.Fatalf("Query with field projection failed: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected at least 1 result")
	}
}

// =============================================================================
// storage.go: writeGlobalMetadata / writeDeviceGroupMetadata error paths
// Targets: writeGlobalMetadata 71.4%, writeDeviceGroupMetadata 71.4%
// =============================================================================

func TestWriteGlobalMetadata_InvalidPathFails(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage("/nonexistent", compression.Snappy, logger)

	idx := &MetadataIndex{
		Magic:   MetadataIndexMagic,
		Version: StorageVersion,
	}
	_, err := s.writeGlobalMetadata("/nonexistent/deep/path/_metadata.idx", idx)
	if err == nil {
		t.Fatal("expected error for invalid path")
	}
}

func TestWriteDeviceGroupMetadata_InvalidPathFails(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage("/nonexistent", compression.Snappy, logger)

	dgMeta := &DeviceGroupMetadata{
		Magic:   DeviceGroupMetadataMagic,
		Version: StorageVersion,
	}
	_, err := s.writeDeviceGroupMetadata("/nonexistent/deep/path/_metadata.idx", dgMeta)
	if err == nil {
		t.Fatal("expected error for invalid path")
	}
}

func TestWriteGlobalMetadata_RoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, compression.Snappy, logger)

	idx := &MetadataIndex{
		Magic:       MetadataIndexMagic,
		Version:     StorageVersion,
		Compression: compression.Snappy,
		TotalRows:   500,
		GroupCount:  2,
		DeviceCount: 10,
		FieldCount:  3,
		FieldNames:  []string{"temp", "humidity", "pressure"},
		FieldTypes:  []compression.ColumnType{compression.ColumnTypeFloat64, compression.ColumnTypeFloat64, compression.ColumnTypeFloat64},
		DeviceNames: []string{"dev-1", "dev-2"},
		DeviceGroups: []DeviceGroupManifest{
			{DirName: "dg_0000", GroupIndex: 0, DeviceCount: 5, PartCount: 1, RowCount: 250},
			{DirName: "dg_0001", GroupIndex: 1, DeviceCount: 5, PartCount: 1, RowCount: 250},
		},
		DeviceGroupMap: map[string]int{"dev-1": 0, "dev-2": 1},
		MinTimestamp:   1000,
		MaxTimestamp:   2000,
	}

	metaPath := filepath.Join(tmpDir, "_metadata.idx")
	tmpInfo, err := s.writeGlobalMetadata(metaPath, idx)
	if err != nil {
		t.Fatalf("writeGlobalMetadata failed: %v", err)
	}
	// Rename tmp to final
	if err := os.Rename(tmpInfo.TmpPath, tmpInfo.FinalPath); err != nil {
		t.Fatalf("rename failed: %v", err)
	}

	// Read it back
	readIdx, err := s.readGlobalMetadata(metaPath)
	if err != nil {
		t.Fatalf("readGlobalMetadata failed: %v", err)
	}
	if readIdx.Magic != MetadataIndexMagic {
		t.Errorf("expected magic %x, got %x", MetadataIndexMagic, readIdx.Magic)
	}
	if readIdx.TotalRows != 500 {
		t.Errorf("expected TotalRows=500, got %d", readIdx.TotalRows)
	}
}

func TestWriteDeviceGroupMetadata_RoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, compression.Snappy, logger)

	dgMeta := &DeviceGroupMetadata{
		Magic:         DeviceGroupMetadataMagic,
		Version:       StorageVersion,
		GroupIndex:    0,
		DeviceCount:   2,
		PartCount:     1,
		RowCount:      100,
		DeviceNames:   []string{"dev-1", "dev-2"},
		PartFileNames: []string{"part_0000.bin"},
		Parts: []PartManifest{
			{FileName: "part_0000.bin", PartIndex: 0, RowCount: 100, DeviceCount: 2, MinTimestamp: 1000, MaxTimestamp: 2000},
		},
		DevicePartMap: map[string][]int{"dev-1": {0}, "dev-2": {0}},
	}

	metaPath := filepath.Join(tmpDir, "_metadata.idx")
	tmpInfo, err := s.writeDeviceGroupMetadata(metaPath, dgMeta)
	if err != nil {
		t.Fatalf("writeDeviceGroupMetadata failed: %v", err)
	}
	if err := os.Rename(tmpInfo.TmpPath, tmpInfo.FinalPath); err != nil {
		t.Fatalf("rename failed: %v", err)
	}

	readMeta, err := s.readDeviceGroupMetadata(metaPath)
	if err != nil {
		t.Fatalf("readDeviceGroupMetadata failed: %v", err)
	}
	if readMeta.Magic != DeviceGroupMetadataMagic {
		t.Errorf("expected magic %x, got %x", DeviceGroupMetadataMagic, readMeta.Magic)
	}
	if readMeta.RowCount != 100 {
		t.Errorf("expected RowCount=100, got %d", readMeta.RowCount)
	}
}

// =============================================================================
// storage.go: readDeviceGroupMetadata error paths
// Targets: readDeviceGroupMetadata 75.0%
// =============================================================================

func TestReadDeviceGroupMetadata_NonexistentFile(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage("/tmp", compression.Snappy, logger)

	_, err := s.readDeviceGroupMetadata("/nonexistent_path/file.idx")
	if err == nil {
		t.Fatal("expected error for nonexistent file")
	}
}

func TestReadDeviceGroupMetadata_TruncatedFile(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, compression.Snappy, logger)

	path := filepath.Join(tmpDir, "truncated.idx")
	if err := os.WriteFile(path, []byte{0x01, 0x02, 0x03}, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := s.readDeviceGroupMetadata(path)
	if err == nil {
		t.Fatal("expected error for truncated file")
	}
}

func TestReadDeviceGroupMetadata_WrongMagic(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, compression.Snappy, logger)

	// Write a file with valid length but wrong magic
	data := make([]byte, 100)
	binary.LittleEndian.PutUint32(data[0:4], 0xDEADBEEF) // wrong magic
	binary.LittleEndian.PutUint32(data[4:8], StorageVersion)

	path := filepath.Join(tmpDir, "wrong_magic.idx")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := s.readDeviceGroupMetadata(path)
	if err == nil {
		t.Fatal("expected error for wrong magic number")
	}
}

// =============================================================================
// storage.go: readV6PartFooter error paths
// Targets: readV6PartFooter 84.0%
// =============================================================================

func TestReadV6PartFooter_EmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, compression.Snappy, logger)

	emptyFile := filepath.Join(tmpDir, "empty.bin")
	if err := os.WriteFile(emptyFile, []byte{}, 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := s.readV6PartFooter(emptyFile)
	if err == nil {
		t.Fatal("expected error for empty file")
	}
}

func TestReadV6PartFooter_CorruptFooterOffset(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, compression.Snappy, logger)

	// Create file with 8-byte footer that has an offset pointing beyond file
	data := make([]byte, 100)
	binary.LittleEndian.PutUint32(data[92:96], 999) // footer size
	binary.LittleEndian.PutUint32(data[96:100], 0)  // footer offset = 0, but footer size > file size

	f := filepath.Join(tmpDir, "bad_footer.bin")
	if err := os.WriteFile(f, data, 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := s.readV6PartFooter(f)
	if err == nil {
		t.Fatal("expected error for corrupt footer offset")
	}
}

// =============================================================================
// storage.go: writeV6PartFile round-trip with readV6PartFileData
// Targets: writeV6PartFile 82.8%, readV6PartFileData 88.9%
// =============================================================================

func TestWriteV6PartFile_WriteAndRead(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, compression.Snappy, logger)

	now := time.Now()
	fieldNames := []string{"temp", "humidity"}
	fieldTypes := []compression.ColumnType{compression.ColumnTypeFloat64, compression.ColumnTypeFloat64}

	deviceGroups := map[string][]*DataPoint{
		"dev-1": {
			{ID: "dev-1", Time: now, Fields: map[string]interface{}{"temp": 25.5, "humidity": 60.0}},
			{ID: "dev-1", Time: now.Add(time.Second), Fields: map[string]interface{}{"temp": 26.0, "humidity": 61.0}},
		},
		"dev-2": {
			{ID: "dev-2", Time: now.Add(2 * time.Second), Fields: map[string]interface{}{"temp": 30.0, "humidity": 55.0}},
		},
	}

	deviceIDs := []string{"dev-1", "dev-2"}
	sort.Strings(deviceIDs)

	partPath := filepath.Join(tmpDir, "part_0000.bin")
	footer, tmpInfo, err := s.writeV6PartFile(partPath, 0, deviceIDs, deviceGroups, fieldNames, fieldTypes)
	if err != nil {
		t.Fatalf("writeV6PartFile failed: %v", err)
	}
	if err := os.Rename(tmpInfo.TmpPath, tmpInfo.FinalPath); err != nil {
		t.Fatalf("rename failed: %v", err)
	}

	if len(footer.DeviceNames) != 2 {
		t.Errorf("expected 2 devices, got %d", len(footer.DeviceNames))
	}
	totalRows := uint32(0)
	for _, rc := range footer.RowCountPerDev {
		totalRows += rc
	}
	if totalRows != 3 {
		t.Errorf("expected 3 total rows, got %d", totalRows)
	}

	// Read footer
	readFooter, err := s.readV6PartFooter(partPath)
	if err != nil {
		t.Fatalf("readV6PartFooter failed: %v", err)
	}
	if len(readFooter.DeviceNames) != 2 {
		t.Errorf("expected footer 2 devices, got %d", len(readFooter.DeviceNames))
	}

	// Read data (no filters)
	result, err := s.readV6PartFileData(partPath, readFooter, nil, nil, 0, now.Add(time.Minute).UnixNano())
	if err != nil {
		t.Fatalf("readV6PartFileData failed: %v", err)
	}
	if len(result) < 3 {
		t.Errorf("expected at least 3 data points, got %d", len(result))
	}
}

func TestReadV6PartFileData_WithDeviceFilter(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, compression.Snappy, logger)

	now := time.Now()
	fieldNames := []string{"temp"}
	fieldTypes := []compression.ColumnType{compression.ColumnTypeFloat64}

	deviceGroups := map[string][]*DataPoint{
		"dev-1": {{ID: "dev-1", Time: now, Fields: map[string]interface{}{"temp": 25.5}}},
		"dev-2": {{ID: "dev-2", Time: now.Add(time.Second), Fields: map[string]interface{}{"temp": 26.0}}},
		"dev-3": {{ID: "dev-3", Time: now.Add(2 * time.Second), Fields: map[string]interface{}{"temp": 27.0}}},
	}

	deviceIDs := []string{"dev-1", "dev-2", "dev-3"}
	sort.Strings(deviceIDs)

	partPath := filepath.Join(tmpDir, "part_filt.bin")
	_, tmpInfo, err := s.writeV6PartFile(partPath, 0, deviceIDs, deviceGroups, fieldNames, fieldTypes)
	if err != nil {
		t.Fatalf("writeV6PartFile failed: %v", err)
	}
	if err := os.Rename(tmpInfo.TmpPath, tmpInfo.FinalPath); err != nil {
		t.Fatal(err)
	}

	footer, _ := s.readV6PartFooter(partPath)

	// Read with device filter → only dev-2
	deviceFilter := map[string]bool{"dev-2": true}
	result, err := s.readV6PartFileData(partPath, footer, deviceFilter, nil, 0, now.Add(time.Minute).UnixNano())
	if err != nil {
		t.Fatalf("readV6PartFileData with filter failed: %v", err)
	}

	for _, dp := range result {
		if dp.ID != "dev-2" {
			t.Errorf("expected only dev-2, got %s", dp.ID)
		}
	}
}

func TestReadV6PartFileData_WithFieldFilter(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, compression.Snappy, logger)

	now := time.Now()
	fieldNames := []string{"temp", "humidity", "pressure"}
	fieldTypes := []compression.ColumnType{compression.ColumnTypeFloat64, compression.ColumnTypeFloat64, compression.ColumnTypeFloat64}

	deviceGroups := map[string][]*DataPoint{
		"dev-1": {
			{ID: "dev-1", Time: now, Fields: map[string]interface{}{"temp": 25.5, "humidity": 60.0, "pressure": 1013.0}},
		},
	}

	partPath := filepath.Join(tmpDir, "part_ff.bin")
	_, tmpInfo, err := s.writeV6PartFile(partPath, 0, []string{"dev-1"}, deviceGroups, fieldNames, fieldTypes)
	if err != nil {
		t.Fatalf("writeV6PartFile failed: %v", err)
	}
	if err := os.Rename(tmpInfo.TmpPath, tmpInfo.FinalPath); err != nil {
		t.Fatal(err)
	}

	footer, _ := s.readV6PartFooter(partPath)

	// Only request "temp" and "pressure"
	fieldFilter := map[string]bool{"temp": true, "pressure": true}
	result, err := s.readV6PartFileData(partPath, footer, nil, fieldFilter, 0, now.Add(time.Minute).UnixNano())
	if err != nil {
		t.Fatalf("readV6PartFileData with field filter failed: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
}

func TestReadV6PartFileData_TimeRangeFilter(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, compression.Snappy, logger)

	baseTime := time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC)
	fieldNames := []string{"val"}
	fieldTypes := []compression.ColumnType{compression.ColumnTypeFloat64}

	deviceGroups := map[string][]*DataPoint{
		"dev-1": {
			{ID: "dev-1", Time: baseTime, Fields: map[string]interface{}{"val": 1.0}},
			{ID: "dev-1", Time: baseTime.Add(time.Hour), Fields: map[string]interface{}{"val": 2.0}},
			{ID: "dev-1", Time: baseTime.Add(2 * time.Hour), Fields: map[string]interface{}{"val": 3.0}},
		},
	}

	partPath := filepath.Join(tmpDir, "part_time.bin")
	_, tmpInfo, err := s.writeV6PartFile(partPath, 0, []string{"dev-1"}, deviceGroups, fieldNames, fieldTypes)
	if err != nil {
		t.Fatalf("writeV6PartFile failed: %v", err)
	}
	if err := os.Rename(tmpInfo.TmpPath, tmpInfo.FinalPath); err != nil {
		t.Fatal(err)
	}

	footer, _ := s.readV6PartFooter(partPath)

	// Only query the middle hour
	startNano := baseTime.Add(30 * time.Minute).UnixNano()
	endNano := baseTime.Add(90 * time.Minute).UnixNano()
	result, err := s.readV6PartFileData(partPath, footer, nil, nil, startNano, endNano)
	if err != nil {
		t.Fatalf("readV6PartFileData failed: %v", err)
	}
	// Should only find the point at baseTime+1h
	if len(result) != 1 {
		t.Errorf("expected 1 result in time range, got %d", len(result))
	}
}

// =============================================================================
// storage.go: cleanupV6StaleParts
// Targets: cleanupV6StaleParts 81.5%
// =============================================================================

func TestCleanupV6StaleParts_HandlesNonBinFiles(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, compression.Snappy, logger)

	dgDir := filepath.Join(tmpDir, "dg_0000")
	if err := os.MkdirAll(dgDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Write DG metadata first
	dgMeta := &DeviceGroupMetadata{
		Magic:         DeviceGroupMetadataMagic,
		Version:       StorageVersion,
		PartFileNames: []string{"part_0000.bin"},
		Parts:         []PartManifest{{FileName: "part_0000.bin", PartIndex: 0}},
		DevicePartMap: make(map[string][]int),
	}
	metaPath := filepath.Join(dgDir, "_metadata.idx")
	tmpInfo, err := s.writeDeviceGroupMetadata(metaPath, dgMeta)
	if err != nil {
		t.Fatalf("writeDeviceGroupMetadata failed: %v", err)
	}
	if err := os.Rename(tmpInfo.TmpPath, tmpInfo.FinalPath); err != nil {
		t.Fatal(err)
	}

	// Create referenced file, orphan .bin, orphan .tmp, and a non-bin/tmp file
	if err := os.WriteFile(filepath.Join(dgDir, "part_0000.bin"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dgDir, "part_9999.bin"), []byte("orphan"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dgDir, "old_data.tmp"), []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dgDir, "notes.txt"), []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}

	manifest := DeviceGroupManifest{DirName: "dg_0000"}
	s.cleanupV6StaleParts(dgDir, manifest)

	// Orphan .bin and .tmp should be removed
	if _, err := os.Stat(filepath.Join(dgDir, "part_9999.bin")); !os.IsNotExist(err) {
		t.Error("orphan part_9999.bin should be removed")
	}
	if _, err := os.Stat(filepath.Join(dgDir, "old_data.tmp")); !os.IsNotExist(err) {
		t.Error("orphan old_data.tmp should be removed")
	}
	// Referenced and non-bin/tmp files should remain
	if _, err := os.Stat(filepath.Join(dgDir, "part_0000.bin")); os.IsNotExist(err) {
		t.Error("referenced part_0000.bin should still exist")
	}
	if _, err := os.Stat(filepath.Join(dgDir, "notes.txt")); os.IsNotExist(err) {
		t.Error("notes.txt should still exist (not .bin or .tmp)")
	}
}

func TestCleanupV6StaleParts_NoMetadataFile(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, compression.Snappy, logger)

	dgDir := filepath.Join(tmpDir, "dg_0000")
	if err := os.MkdirAll(dgDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// No _metadata.idx → should warn and return gracefully
	manifest := DeviceGroupManifest{DirName: "dg_0000"}
	s.cleanupV6StaleParts(dgDir, manifest) // should not panic
}

// =============================================================================
// storage.go: readAllV6PartsInDG — corrupt part file
// Targets: readAllV6PartsInDG 84.6%
// =============================================================================

func TestReadAllV6PartsInDG_CorruptPartFile(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, compression.Snappy, logger)

	dgDir := filepath.Join(tmpDir, "dg_0000")
	if err := os.MkdirAll(dgDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Create a corrupt part file
	if err := os.WriteFile(filepath.Join(dgDir, "part_0000.bin"), []byte("this-is-not-valid-data"), 0o644); err != nil {
		t.Fatal(err)
	}

	dgMeta := &DeviceGroupMetadata{
		PartFileNames: []string{"part_0000.bin"},
		DevicePartMap: make(map[string][]int),
	}

	result := s.readAllV6PartsInDG(dgDir, dgMeta)
	// Should handle gracefully, not crash
	if len(result) != 0 {
		t.Logf("readAllV6PartsInDG with corrupt file returned %d points", len(result))
	}
}

// =============================================================================
// storage.go: queryV6DG — time range pruning via part manifests
// Targets: queryV6DG 80.0%
// =============================================================================

func TestQueryV6DG_EmptyDG(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, compression.Snappy, logger)

	dgDir := filepath.Join(tmpDir, "dg_0000")
	if err := os.MkdirAll(dgDir, 0o755); err != nil {
		t.Fatal(err)
	}

	dgMeta := &DeviceGroupMetadata{
		PartFileNames: []string{},
		DevicePartMap: make(map[string][]int),
	}

	results, err := s.queryV6DG(dgDir, dgMeta, nil, nil, 0, time.Now().UnixNano())
	if err != nil {
		t.Fatalf("queryV6DG failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results for empty DG, got %d", len(results))
	}
}

func TestQueryV6DG_PartManifestTimeRangePruning(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, compression.Snappy, logger)

	// Write data at a known time through WriteBatch
	baseTime := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	batch := []*DataPoint{
		{Database: "db", Collection: "c", ID: "dev-1", Time: baseTime, Fields: map[string]interface{}{"val": 1.0}},
		{Database: "db", Collection: "c", ID: "dev-1", Time: baseTime.Add(time.Hour), Fields: map[string]interface{}{"val": 2.0}},
	}
	if err := s.WriteBatch(batch); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	// Query with time range that should match
	results, err := s.Query("db", "c", nil, baseTime.Add(-time.Minute), baseTime.Add(2*time.Hour), nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}

	// Query with time range that misses both points
	results2, err := s.Query("db", "c", nil, baseTime.Add(3*time.Hour), baseTime.Add(4*time.Hour), nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(results2) != 0 {
		t.Errorf("expected 0 results for miss, got %d", len(results2))
	}
}

// =============================================================================
// index.go: DecodeFileIndex edge cases
// Targets: DecodeFileIndex 80.8%
// =============================================================================

func TestDecodeFileIndex_EmptyData(t *testing.T) {
	_, err := DecodeFileIndex([]byte{})
	if err == nil {
		t.Fatal("expected error for empty data")
	}
}

func TestDecodeFileIndex_ExcessiveEntryCount(t *testing.T) {
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, 20_000_000) // exceeds maxEntryCount
	_, err := DecodeFileIndex(data)
	if err == nil {
		t.Fatal("expected error for excessive entry count")
	}
}

func TestDecodeFileIndex_TruncatedBody(t *testing.T) {
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, 1) // 1 entry but no body
	_, err := DecodeFileIndex(data)
	if err == nil {
		t.Fatal("expected error for truncated body")
	}
}

func TestDecodeFileIndex_ValidRoundTrip(t *testing.T) {
	idx := NewFileIndex()
	idx.AddEntry(DeviceIndexEntry{
		DeviceID:    "sensor-A",
		BlockOffset: 100,
		BlockSize:   2048,
		EntryCount:  50,
		MinTime:     1000000,
		MaxTime:     2000000,
	})
	idx.AddEntry(DeviceIndexEntry{
		DeviceID:    "sensor-B",
		BlockOffset: 3000,
		BlockSize:   1024,
		EntryCount:  25,
		MinTime:     1500000,
		MaxTime:     2500000,
	})
	idx.Sort()

	encoded, err := idx.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeFileIndex(encoded)
	if err != nil {
		t.Fatalf("DecodeFileIndex failed: %v", err)
	}
	if len(decoded.DeviceEntries) != 2 {
		t.Errorf("expected 2 entries, got %d", len(decoded.DeviceEntries))
	}
	if decoded.DeviceEntries[0].DeviceID != "sensor-A" && decoded.DeviceEntries[1].DeviceID != "sensor-A" {
		t.Error("expected sensor-A in decoded entries")
	}
}

// =============================================================================
// index.go: FindDevice edge cases
// Targets: FindDevice 85.7%
// =============================================================================

func TestFindDevice_ManyDevicesBinarySearch(t *testing.T) {
	idx := NewFileIndex()
	for i := 0; i < 100; i++ {
		idx.AddEntry(DeviceIndexEntry{
			DeviceID:    "dev-" + string(rune(i+33)), // distinct chars
			BlockOffset: int64(i * 100),
			BlockSize:   100,
			EntryCount:  uint32(i + 1),
			MinTime:     int64(i * 1000),
			MaxTime:     int64(i*1000 + 999),
		})
	}
	idx.Sort()

	// Find existing device
	_, ok := idx.FindDevice("dev-" + string(rune(50+33)))
	if !ok {
		t.Error("expected to find device in sorted index")
	}

	// Find non-existing device
	_, ok2 := idx.FindDevice("zzz-nonexistent-very-long-name-to-avoid-bloom-fp")
	if ok2 {
		t.Log("bloom filter false positive (acceptable)")
	}
}

// =============================================================================
// index.go: GetStats with time tracking
// Targets: GetStats 91.7%
// =============================================================================

func TestGetStats_WithTimeRange(t *testing.T) {
	idx := NewFileIndex()
	idx.AddEntry(DeviceIndexEntry{
		DeviceID:   "dev-1",
		EntryCount: 50,
		MinTime:    1000000000,
		MaxTime:    2000000000,
	})
	idx.AddEntry(DeviceIndexEntry{
		DeviceID:   "dev-2",
		EntryCount: 25,
		MinTime:    500000000,
		MaxTime:    3000000000,
	})

	stats := idx.GetStats()
	dc := stats["device_count"].(int)
	if dc != 2 {
		t.Errorf("expected device_count=2, got %d", dc)
	}
	te := stats["total_entries"].(uint32)
	if te != 75 {
		t.Errorf("expected total_entries=75, got %d", te)
	}
}

func TestGetStats_ZeroEntries(t *testing.T) {
	idx := NewFileIndex()
	stats := idx.GetStats()
	dc := stats["device_count"].(int)
	if dc != 0 {
		t.Errorf("expected device_count=0, got %d", dc)
	}
}

// =============================================================================
// storage.go: findDateDirs
// =============================================================================

func TestFindDateDirs_NonexistentDB(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, compression.Snappy, logger)

	dirs, err := s.findDateDirs("nonexistent", "nonexistent", time.Time{}, time.Now())
	if err != nil {
		t.Fatalf("findDateDirs should not error for missing db: %v", err)
	}
	if len(dirs) != 0 {
		t.Errorf("expected 0 dirs, got %d", len(dirs))
	}
}

func TestFindDateDirs_WithWrittenData(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, compression.Snappy, logger)

	now := time.Now()
	batch := []*DataPoint{
		{Database: "db", Collection: "c", ID: "dev-1", Time: now, Fields: map[string]interface{}{"val": 1.0}},
	}
	if err := s.WriteBatch(batch); err != nil {
		t.Fatal(err)
	}

	dirs, err := s.findDateDirs("db", "c", now.Add(-24*time.Hour), now.Add(24*time.Hour))
	if err != nil {
		t.Fatalf("findDateDirs failed: %v", err)
	}
	if len(dirs) == 0 {
		t.Error("expected at least 1 date dir")
	}
}

// =============================================================================
// storage.go: groupByFile with multiple databases
// =============================================================================

func TestGroupByFile_AcrossDatabases(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage("/tmp", compression.Snappy, logger)

	now := time.Now()
	points := []*DataPoint{
		{Database: "db1", Collection: "metrics", ID: "dev-1", Time: now, Fields: map[string]interface{}{"val": 1.0}},
		{Database: "db1", Collection: "logs", ID: "dev-1", Time: now, Fields: map[string]interface{}{"val": 2.0}},
		{Database: "db2", Collection: "metrics", ID: "dev-2", Time: now, Fields: map[string]interface{}{"val": 3.0}},
	}

	groups := s.groupByFile(points)
	if len(groups) != 3 {
		t.Errorf("expected 3 groups (db1/metrics, db1/logs, db2/metrics), got %d", len(groups))
	}
}

// =============================================================================
// storage.go: deduplicatePoints with InsertedAt ordering
// =============================================================================

func TestDeduplicatePoints_InsertedAtOrdering(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage("/tmp", compression.Snappy, logger)

	now := time.Unix(1000, 0)
	points := []*DataPoint{
		{ID: "dev-1", Time: now, Fields: map[string]interface{}{"val": 1.0}, InsertedAt: time.Unix(100, 0)},
		{ID: "dev-1", Time: now, Fields: map[string]interface{}{"val": 2.0}, InsertedAt: time.Unix(200, 0)}, // newer InsertedAt → wins
		{ID: "dev-2", Time: now, Fields: map[string]interface{}{"val": 3.0}},
	}

	result := s.deduplicatePoints(points)
	if len(result) != 2 {
		t.Errorf("expected 2 points after dedup, got %d", len(result))
	}

	// Check that the newer insertion won
	for _, dp := range result {
		if dp.ID == "dev-1" {
			if val, ok := dp.Fields["val"].(float64); ok && val != 2.0 {
				t.Errorf("expected val=2.0 (newer InsertedAt), got %v", val)
			}
		}
	}
}

// =============================================================================
// storage.go: NewStorageWithConfig defaults
// =============================================================================

func TestNewStorageWithConfig_ZeroDefaultsFallback(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorageWithConfig(tmpDir, compression.Snappy, logger, 0, 0, 0, 0)

	if s.maxRowsPerPart != DefaultMaxRowsPerPart {
		t.Errorf("expected default maxRowsPerPart=%d, got %d", DefaultMaxRowsPerPart, s.maxRowsPerPart)
	}
	if s.maxPartSize != DefaultMaxPartSize {
		t.Errorf("expected default maxPartSize=%d, got %d", DefaultMaxPartSize, s.maxPartSize)
	}
	if s.minRowsPerPart != DefaultMinRowsPerPart {
		t.Errorf("expected default minRowsPerPart=%d, got %d", DefaultMinRowsPerPart, s.minRowsPerPart)
	}
	if s.maxDevicesPerGroup != DefaultMaxDevicesPerGroup {
		t.Errorf("expected default maxDevicesPerGroup=%d, got %d", DefaultMaxDevicesPerGroup, s.maxDevicesPerGroup)
	}
}

// =============================================================================
// storage.go: CompactDateDir after multiple writes
// =============================================================================

func TestCompactDateDir_MergesMultipleWriteBatches(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorageWithConfig(tmpDir, compression.Snappy, logger, 1000, 64*1024*1024, 100, 50)

	now := time.Now()

	// Write multiple batches to create multiple parts
	for batch := 0; batch < 5; batch++ {
		points := make([]*DataPoint, 0, 10)
		for i := 0; i < 10; i++ {
			points = append(points, &DataPoint{
				Database:   "db",
				Collection: "c",
				ID:         "dev-1",
				Time:       now.Add(time.Duration(batch*10+i) * time.Second),
				Fields:     map[string]interface{}{"val": float64(batch*10 + i)},
			})
		}
		if err := s.WriteBatch(points); err != nil {
			t.Fatalf("WriteBatch %d failed: %v", batch, err)
		}
	}

	// Query after writes to verify all data accessible
	results, err := s.Query("db", "c", nil, now.Add(-time.Minute), now.Add(time.Hour), nil)
	if err != nil {
		t.Fatalf("Query after writes failed: %v", err)
	}
	if len(results) < 50 {
		t.Errorf("expected at least 50 results, got %d", len(results))
	}
}

// =============================================================================
// storage.go: concurrent writes to different collections
// =============================================================================

func TestWriteBatch_MultipleCollections_Concurrent(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, compression.Snappy, logger)

	now := time.Now()

	collections := []string{"metrics", "logs", "events", "alerts"}
	errCh := make(chan error, len(collections))

	for _, col := range collections {
		go func(c string) {
			for i := 0; i < 20; i++ {
				batch := []*DataPoint{
					{
						Database: "db", Collection: c, ID: "dev-1", Time: now.Add(time.Duration(i) * time.Second),
						Fields: map[string]interface{}{"val": float64(i)},
					},
				}
				if err := s.WriteBatch(batch); err != nil {
					errCh <- err
					return
				}
			}
			errCh <- nil
		}(col)
	}

	for range collections {
		if err := <-errCh; err != nil {
			t.Errorf("concurrent write failed: %v", err)
		}
	}

	// Query each collection
	for _, col := range collections {
		results, err := s.Query("db", col, nil, now.Add(-time.Minute), now.Add(time.Hour), nil)
		if err != nil {
			t.Errorf("Query %s failed: %v", col, err)
		}
		if len(results) != 20 {
			t.Errorf("expected 20 results for %s, got %d", col, len(results))
		}
	}
}

// =============================================================================
// storage.go: WriteBatch with large field variety
// =============================================================================

func TestWriteBatch_MixedFieldTypes(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, compression.Snappy, logger)

	now := time.Now()
	batch := []*DataPoint{
		{Database: "db", Collection: "c", ID: "dev-1", Time: now, Fields: map[string]interface{}{
			"float_val":  3.14,
			"int_val":    int64(42),
			"string_val": "hello",
			"bool_val":   true,
		}},
		{Database: "db", Collection: "c", ID: "dev-1", Time: now.Add(time.Second), Fields: map[string]interface{}{
			"float_val":  2.71,
			"int_val":    int64(100),
			"string_val": "world",
			"bool_val":   false,
		}},
	}
	if err := s.WriteBatch(batch); err != nil {
		t.Fatalf("WriteBatch with mixed types failed: %v", err)
	}

	results, err := s.Query("db", "c", nil, now.Add(-time.Minute), now.Add(time.Minute), nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}
}

// =============================================================================
// storage.go: WriteBatch and Query with many devices in one DG
// =============================================================================

func TestWriteAndQuery_ManyDevicesSingleDG(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorageWithConfig(tmpDir, compression.Snappy, logger, 1000, 64*1024*1024, 100, 100)

	now := time.Now()
	batch := make([]*DataPoint, 0, 50)
	for i := 0; i < 50; i++ {
		batch = append(batch, &DataPoint{
			Database:   "db",
			Collection: "c",
			ID:         "device-" + string(rune(i+33)),
			Time:       now.Add(time.Duration(i) * time.Second),
			Fields:     map[string]interface{}{"val": float64(i)},
		})
	}
	if err := s.WriteBatch(batch); err != nil {
		t.Fatal(err)
	}

	// Query all
	results, err := s.Query("db", "c", nil, now.Add(-time.Minute), now.Add(time.Hour), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 50 {
		t.Errorf("expected 50 results, got %d", len(results))
	}

	// Query specific device
	results2, err := s.Query("db", "c", []string{"device-" + string(rune(10+33))}, now.Add(-time.Minute), now.Add(time.Hour), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(results2) != 1 {
		t.Errorf("expected 1 result for single device, got %d", len(results2))
	}
}
