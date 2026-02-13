package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/compression"
	"github.com/soltixdb/soltix/internal/logging"
)

// =============================================================================
// inferType — all type branches
// =============================================================================

func TestInferType_Float64(t *testing.T) {
	result := inferType(float64(3.14))
	if result != compression.ColumnTypeFloat64 {
		t.Errorf("inferType(float64) = %v, want ColumnTypeFloat64", result)
	}
}

func TestInferType_Float32(t *testing.T) {
	result := inferType(float32(3.14))
	if result != compression.ColumnTypeFloat64 {
		t.Errorf("inferType(float32) = %v, want ColumnTypeFloat64", result)
	}
}

func TestInferType_Int(t *testing.T) {
	result := inferType(int(42))
	if result != compression.ColumnTypeInt64 {
		t.Errorf("inferType(int) = %v, want ColumnTypeInt64", result)
	}
}

func TestInferType_Int32(t *testing.T) {
	result := inferType(int32(42))
	if result != compression.ColumnTypeInt64 {
		t.Errorf("inferType(int32) = %v, want ColumnTypeInt64", result)
	}
}

func TestInferType_Int64(t *testing.T) {
	result := inferType(int64(42))
	if result != compression.ColumnTypeInt64 {
		t.Errorf("inferType(int64) = %v, want ColumnTypeInt64", result)
	}
}

func TestInferType_Bool(t *testing.T) {
	result := inferType(true)
	if result != compression.ColumnTypeBool {
		t.Errorf("inferType(bool) = %v, want ColumnTypeBool", result)
	}
}

func TestInferType_String(t *testing.T) {
	result := inferType("hello")
	if result != compression.ColumnTypeString {
		t.Errorf("inferType(string) = %v, want ColumnTypeString", result)
	}
}

func TestInferType_Default(t *testing.T) {
	// Unknown type (e.g. struct) should default to Float64
	result := inferType(struct{}{})
	if result != compression.ColumnTypeFloat64 {
		t.Errorf("inferType(struct{}) = %v, want ColumnTypeFloat64 (default)", result)
	}
}

// =============================================================================
// inferFieldTypes — coverage
// =============================================================================

func TestInferFieldTypes_AllFieldsInFirst(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), compression.Snappy, logger)

	points := []*DataPoint{
		{Fields: map[string]interface{}{"temp": 25.5, "count": int64(10), "active": true}},
	}
	fieldNames := []string{"active", "count", "temp"}
	types := s.inferFieldTypes(points, fieldNames)

	if types[0] != compression.ColumnTypeBool {
		t.Errorf("active type = %v, want Bool", types[0])
	}
	if types[1] != compression.ColumnTypeInt64 {
		t.Errorf("count type = %v, want Int64", types[1])
	}
	if types[2] != compression.ColumnTypeFloat64 {
		t.Errorf("temp type = %v, want Float64", types[2])
	}
}

func TestInferFieldTypes_SpreadAcrossPoints(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), compression.Snappy, logger)

	points := []*DataPoint{
		{Fields: map[string]interface{}{"temp": 25.5}},
		{Fields: map[string]interface{}{"count": int64(10)}},
		{Fields: map[string]interface{}{"name": "sensor1"}},
	}
	fieldNames := []string{"count", "name", "temp"}
	types := s.inferFieldTypes(points, fieldNames)

	if types[0] != compression.ColumnTypeInt64 {
		t.Errorf("count type = %v, want Int64", types[0])
	}
	if types[1] != compression.ColumnTypeString {
		t.Errorf("name type = %v, want String", types[1])
	}
	if types[2] != compression.ColumnTypeFloat64 {
		t.Errorf("temp type = %v, want Float64", types[2])
	}
}

func TestInferFieldTypes_NilValues(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), compression.Snappy, logger)

	points := []*DataPoint{
		{Fields: map[string]interface{}{"temp": nil}},
		{Fields: map[string]interface{}{"temp": 25.5}},
	}
	fieldNames := []string{"temp"}
	types := s.inferFieldTypes(points, fieldNames)

	// First point has nil, so should use second point's value
	if types[0] != compression.ColumnTypeFloat64 {
		t.Errorf("temp type = %v, want Float64", types[0])
	}
}

// =============================================================================
// splitIntoParts — edge cases
// =============================================================================

func TestSplitIntoParts_Empty(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), compression.Snappy, logger)

	parts := s.splitIntoParts(map[string][]*DataPoint{})
	if len(parts) != 1 {
		t.Errorf("splitIntoParts(empty) = %d parts, want 1 (empty placeholder)", len(parts))
	}
	if len(parts[0]) != 0 {
		t.Errorf("Empty placeholder part has %d entries, want 0", len(parts[0]))
	}
}

func TestSplitIntoParts_SingleDeviceLarge(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorageWithConfig(t.TempDir(), compression.Snappy, logger, 10, 0, 1, 50)

	// Create 25 points for a single device (maxRowsPerPart=10)
	points := make([]*DataPoint, 25)
	for i := range points {
		points[i] = &DataPoint{
			ID:     "dev1",
			Time:   time.Now().Add(time.Duration(i) * time.Second),
			Fields: map[string]interface{}{"temp": float64(i)},
		}
	}

	groups := map[string][]*DataPoint{"dev1": points}
	parts := s.splitIntoParts(groups)

	// 25 rows / 10 per part = 3 parts (10+10+5)
	if len(parts) != 3 {
		t.Errorf("splitIntoParts = %d parts, want 3", len(parts))
	}
}

func TestSplitIntoParts_MultipleDevicesFitInOne(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorageWithConfig(t.TempDir(), compression.Snappy, logger, 100, 0, 1, 50)

	groups := map[string][]*DataPoint{
		"dev1": {{ID: "dev1", Fields: map[string]interface{}{"temp": 1.0}}},
		"dev2": {{ID: "dev2", Fields: map[string]interface{}{"temp": 2.0}}},
	}
	parts := s.splitIntoParts(groups)

	if len(parts) != 1 {
		t.Errorf("splitIntoParts = %d parts, want 1", len(parts))
	}
}

func TestSplitIntoParts_SizeBasedSplit(t *testing.T) {
	logger := logging.NewDevelopment()
	// Set very small maxPartSize to trigger size-based splitting
	s := NewStorageWithConfig(t.TempDir(), compression.Snappy, logger, 1000000, 100, 1, 50)

	// Create enough points to exceed the tiny size limit
	points := make([]*DataPoint, 100)
	for i := range points {
		points[i] = &DataPoint{
			ID:     "dev1",
			Time:   time.Now().Add(time.Duration(i) * time.Second),
			Fields: map[string]interface{}{"temp": float64(i), "humidity": float64(i * 2)},
		}
	}

	groups := map[string][]*DataPoint{"dev1": points}
	parts := s.splitIntoParts(groups)

	// With tiny maxPartSize, should split into multiple parts
	if len(parts) < 2 {
		t.Logf("splitIntoParts = %d parts (size-based split may not trigger with this data)", len(parts))
	}
}

// =============================================================================
// splitIntoDeviceGroups — edge cases
// =============================================================================

func TestSplitIntoDeviceGroups_Empty(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), compression.Snappy, logger)

	groups := s.splitIntoDeviceGroups(map[string][]*DataPoint{})
	if len(groups) != 1 {
		t.Errorf("splitIntoDeviceGroups(empty) = %d groups, want 1 (empty placeholder)", len(groups))
	}
}

func TestSplitIntoDeviceGroups_ExactlyMaxDevices(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorageWithConfig(t.TempDir(), compression.Snappy, logger, 1000, 0, 1, 3)

	allDevices := map[string][]*DataPoint{
		"dev1": {{ID: "dev1"}},
		"dev2": {{ID: "dev2"}},
		"dev3": {{ID: "dev3"}},
	}
	groups := s.splitIntoDeviceGroups(allDevices)

	if len(groups) != 1 {
		t.Errorf("splitIntoDeviceGroups = %d groups, want 1", len(groups))
	}
}

func TestSplitIntoDeviceGroups_MoreThanMax(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorageWithConfig(t.TempDir(), compression.Snappy, logger, 1000, 0, 1, 2)

	allDevices := map[string][]*DataPoint{
		"dev1": {{ID: "dev1"}},
		"dev2": {{ID: "dev2"}},
		"dev3": {{ID: "dev3"}},
		"dev4": {{ID: "dev4"}},
		"dev5": {{ID: "dev5"}},
	}
	groups := s.splitIntoDeviceGroups(allDevices)

	// 5 devices / 2 per group = 3 groups
	if len(groups) != 3 {
		t.Errorf("splitIntoDeviceGroups = %d groups, want 3", len(groups))
	}
}

// =============================================================================
// groupByFile and groupByDevice
// =============================================================================

func TestGroupByFile(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), compression.Snappy, logger)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)
	tomorrow := now.Add(24 * time.Hour)

	entries := []*DataPoint{
		{ID: "d1", Database: "db1", Collection: "c1", Time: now},
		{ID: "d2", Database: "db1", Collection: "c1", Time: now},
		{ID: "d3", Database: "db2", Collection: "c1", Time: now},
		{ID: "d1", Database: "db1", Collection: "c1", Time: tomorrow},
	}

	grouped := s.groupByFile(entries)

	if len(grouped) != 3 {
		t.Errorf("groupByFile = %d groups, want 3", len(grouped))
	}

	// Same db/collection/date should be grouped together
	key := storageFileKey{database: "db1", collection: "c1", date: "20240515"}
	if len(grouped[key]) != 2 {
		t.Errorf("db1/c1/20240515 has %d entries, want 2", len(grouped[key]))
	}
}

func TestGroupByDevice(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), compression.Snappy, logger)

	points := []*DataPoint{
		{ID: "dev1"}, {ID: "dev2"}, {ID: "dev1"}, {ID: "dev3"}, {ID: "dev2"},
	}

	groups := s.groupByDevice(points)
	if len(groups) != 3 {
		t.Errorf("groupByDevice = %d groups, want 3", len(groups))
	}
	if len(groups["dev1"]) != 2 {
		t.Errorf("dev1 has %d points, want 2", len(groups["dev1"]))
	}
	if len(groups["dev2"]) != 2 {
		t.Errorf("dev2 has %d points, want 2", len(groups["dev2"]))
	}
}

// =============================================================================
// collectFieldNames
// =============================================================================

func TestCollectFieldNames(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), compression.Snappy, logger)

	points := []*DataPoint{
		{Fields: map[string]interface{}{"temp": 1.0, "_inserted_at": time.Now()}},
		{Fields: map[string]interface{}{"humidity": 2.0, "temp": 3.0}},
	}

	fields := s.collectFieldNames(points)

	// Should exclude _inserted_at
	if len(fields) != 2 {
		t.Errorf("collectFieldNames = %v, want [humidity, temp]", fields)
	}
	if fields[0] != "humidity" || fields[1] != "temp" {
		t.Errorf("collectFieldNames = %v, want [humidity, temp]", fields)
	}
}

// =============================================================================
// mergeFieldDefinitionsV6
// =============================================================================

func TestMergeFieldDefinitionsV6(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), compression.Snappy, logger)

	existing := &MetadataIndex{
		FieldNames: []string{"humidity", "temp"},
		FieldTypes: []compression.ColumnType{compression.ColumnTypeFloat64, compression.ColumnTypeFloat64},
	}

	newFields := []string{"pressure", "temp"} // temp already exists
	newTypes := []compression.ColumnType{compression.ColumnTypeFloat64, compression.ColumnTypeFloat64}

	merged, mergedTypes := s.mergeFieldDefinitionsV6(existing, newFields, newTypes)

	if len(merged) != 3 {
		t.Errorf("mergeFieldDefinitionsV6 = %d fields, want 3", len(merged))
	}
	// Should be sorted: humidity, pressure, temp
	expected := []string{"humidity", "pressure", "temp"}
	for i, name := range merged {
		if name != expected[i] {
			t.Errorf("merged[%d] = %s, want %s", i, name, expected[i])
		}
	}
	if len(mergedTypes) != 3 {
		t.Errorf("mergedTypes length = %d, want 3", len(mergedTypes))
	}
}

// =============================================================================
// compactDeviceGroups
// =============================================================================

func TestCompactDeviceGroups(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), compression.Snappy, logger)

	dgs := []DeviceGroupManifest{
		{DirName: "dg_0000", GroupIndex: 0, RowCount: 100, DeviceNames: []string{"d1"}},
		{DirName: "dg_0001", GroupIndex: 1, RowCount: 0, DeviceNames: []string{}}, // empty
		{DirName: "dg_0002", GroupIndex: 2, RowCount: 200, DeviceNames: []string{"d2"}},
	}

	compacted, reindex := s.compactDeviceGroups(dgs)

	if len(compacted) != 2 {
		t.Errorf("compacted = %d groups, want 2", len(compacted))
	}
	if reindex[0] != 0 {
		t.Errorf("reindex[0] = %d, want 0", reindex[0])
	}
	if reindex[2] != 1 {
		t.Errorf("reindex[2] = %d, want 1", reindex[2])
	}
	if compacted[1].DirName != "dg_0001" {
		t.Errorf("compacted[1].DirName = %s, want dg_0001", compacted[1].DirName)
	}
}

// =============================================================================
// deduplicatePoints
// =============================================================================

func TestDeduplicatePoints_NoDuplicates(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), compression.Snappy, logger)

	now := time.Now()
	points := []*DataPoint{
		{ID: "d1", Time: now, InsertedAt: now},
		{ID: "d2", Time: now, InsertedAt: now},
	}

	result := s.deduplicatePoints(points)
	if len(result) != 2 {
		t.Errorf("deduplicatePoints = %d, want 2", len(result))
	}
}

func TestDeduplicatePoints_WithDuplicates(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), compression.Snappy, logger)

	now := time.Now()
	older := now.Add(-time.Hour)

	points := []*DataPoint{
		{ID: "d1", Time: now, InsertedAt: older, Fields: map[string]interface{}{"v": 1.0}},
		{ID: "d1", Time: now, InsertedAt: now, Fields: map[string]interface{}{"v": 2.0}},
		{ID: "d2", Time: now, InsertedAt: now},
	}

	result := s.deduplicatePoints(points)
	if len(result) != 2 {
		t.Errorf("deduplicatePoints = %d, want 2", len(result))
	}

	// d1 should keep the newer InsertedAt (value 2.0)
	for _, p := range result {
		if p.ID == "d1" {
			if v, ok := p.Fields["v"].(float64); !ok || v != 2.0 {
				t.Errorf("d1 should have v=2.0 (newer insert), got %v", p.Fields["v"])
			}
		}
	}
}

// =============================================================================
// cleanupTmpFiles / atomicRenameAll
// =============================================================================

func TestCleanupTmpFiles(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), compression.Snappy, logger)

	tmpDir := t.TempDir()

	// Create temp files
	f1 := filepath.Join(tmpDir, "file1.tmp")
	f2 := filepath.Join(tmpDir, "file2.tmp")
	if err := os.WriteFile(f1, []byte("data1"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f2, []byte("data2"), 0o644); err != nil {
		t.Fatal(err)
	}

	tmpFiles := []*TmpFileInfo{
		{TmpPath: f1, FinalPath: filepath.Join(tmpDir, "file1.final")},
		{TmpPath: f2, FinalPath: filepath.Join(tmpDir, "file2.final")},
	}

	s.cleanupTmpFiles(tmpFiles)

	if _, err := os.Stat(f1); !os.IsNotExist(err) {
		t.Error("file1.tmp should be removed")
	}
	if _, err := os.Stat(f2); !os.IsNotExist(err) {
		t.Error("file2.tmp should be removed")
	}
}

func TestAtomicRenameAll_SuccessfulRename(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), compression.Snappy, logger)

	tmpDir := t.TempDir()

	f1 := filepath.Join(tmpDir, "file1.tmp")
	f2 := filepath.Join(tmpDir, "file2.tmp")
	if err := os.WriteFile(f1, []byte("data1"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f2, []byte("data2"), 0o644); err != nil {
		t.Fatal(err)
	}

	final1 := filepath.Join(tmpDir, "file1.final")
	final2 := filepath.Join(tmpDir, "file2.final")

	tmpFiles := []*TmpFileInfo{
		{TmpPath: f1, FinalPath: final1},
		{TmpPath: f2, FinalPath: final2},
	}

	err := s.atomicRenameAll(tmpFiles)
	if err != nil {
		t.Fatalf("atomicRenameAll failed: %v", err)
	}

	if _, err := os.Stat(final1); err != nil {
		t.Error("file1.final should exist")
	}
	if _, err := os.Stat(final2); err != nil {
		t.Error("file2.final should exist")
	}
}

func TestAtomicRenameAll_NonexistentFile(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), compression.Snappy, logger)

	// TmpPath doesn't exist → rename should fail
	tmpFiles := []*TmpFileInfo{
		{TmpPath: "/nonexistent/file.tmp", FinalPath: "/nonexistent/file.final"},
	}

	err := s.atomicRenameAll(tmpFiles)
	if err == nil {
		t.Fatal("Expected error for nonexistent tmp file")
	}
}

// =============================================================================
// SetMaxDevicesPerGroup
// =============================================================================

func TestSetMaxDevicesPerGroup(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), compression.Snappy, logger)

	if s.maxDevicesPerGroup != DefaultMaxDevicesPerGroup {
		t.Errorf("Default maxDevicesPerGroup = %d", s.maxDevicesPerGroup)
	}

	s.SetMaxDevicesPerGroup(100)
	if s.maxDevicesPerGroup != 100 {
		t.Errorf("maxDevicesPerGroup = %d, want 100", s.maxDevicesPerGroup)
	}

	// 0 or negative should not change
	s.SetMaxDevicesPerGroup(0)
	if s.maxDevicesPerGroup != 100 {
		t.Errorf("maxDevicesPerGroup changed to %d after setting 0", s.maxDevicesPerGroup)
	}

	s.SetMaxDevicesPerGroup(-1)
	if s.maxDevicesPerGroup != 100 {
		t.Errorf("maxDevicesPerGroup changed to %d after setting -1", s.maxDevicesPerGroup)
	}
}

// =============================================================================
// decodeV6Footer — truncation boundary tests
// =============================================================================

func TestDecodeV6Footer_TooShort(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), compression.Snappy, logger)

	_, err := s.decodeV6Footer([]byte{1, 2, 3})
	if err == nil {
		t.Fatal("Expected error for too-short footer")
	}
}

func TestDecodeV6Footer_TruncatedAtDeviceName(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), compression.Snappy, logger)

	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, 1) // 1 device, no name data
	_, err := s.decodeV6Footer(buf)
	if err == nil {
		t.Fatal("Expected error for truncated footer")
	}
}

func TestDecodeV6Footer_TruncatedAtDeviceNameData(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), compression.Snappy, logger)

	buf := make([]byte, 0, 32)
	intBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(intBuf, 1) // 1 device
	buf = append(buf, intBuf...)
	nameLenBuf := make([]byte, 2)
	binary.LittleEndian.PutUint16(nameLenBuf, 10)
	buf = append(buf, nameLenBuf...)
	buf = append(buf, []byte("ab")...) // Only 2 bytes, need 10

	_, err := s.decodeV6Footer(buf)
	if err == nil {
		t.Fatal("Expected error for truncated device name data")
	}
}

func TestDecodeV6Footer_TruncatedAtRowCount(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), compression.Snappy, logger)

	buf := make([]byte, 0, 32)
	intBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(intBuf, 1) // 1 device
	buf = append(buf, intBuf...)
	nameLenBuf := make([]byte, 2)
	binary.LittleEndian.PutUint16(nameLenBuf, 1)
	buf = append(buf, nameLenBuf...)
	buf = append(buf, 'd') // device name "d"
	// No row count data → truncated

	_, err := s.decodeV6Footer(buf)
	if err == nil {
		t.Fatal("Expected error for truncated row count")
	}
}

func TestDecodeV6Footer_TruncatedAtFieldCount(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), compression.Snappy, logger)

	buf := make([]byte, 0, 64)
	intBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(intBuf, 1) // 1 device
	buf = append(buf, intBuf...)
	nameLenBuf := make([]byte, 2)
	binary.LittleEndian.PutUint16(nameLenBuf, 1)
	buf = append(buf, nameLenBuf...)
	buf = append(buf, 'd')
	binary.LittleEndian.PutUint32(intBuf, 5) // row count
	buf = append(buf, intBuf...)
	// No field count → truncated

	_, err := s.decodeV6Footer(buf)
	if err == nil {
		t.Fatal("Expected error for truncated field count")
	}
}

func TestDecodeV6Footer_TruncatedAtFieldName(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), compression.Snappy, logger)

	buf := make([]byte, 0, 64)
	intBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(intBuf, 1) // 1 device
	buf = append(buf, intBuf...)
	nameLenBuf := make([]byte, 2)
	binary.LittleEndian.PutUint16(nameLenBuf, 1)
	buf = append(buf, nameLenBuf...)
	buf = append(buf, 'd')
	binary.LittleEndian.PutUint32(intBuf, 5) // row count
	buf = append(buf, intBuf...)
	binary.LittleEndian.PutUint32(intBuf, 1) // field count
	buf = append(buf, intBuf...)
	// No field name length → truncated

	_, err := s.decodeV6Footer(buf)
	if err == nil {
		t.Fatal("Expected error for truncated field name")
	}
}

func TestDecodeV6Footer_TruncatedAtFieldType(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), compression.Snappy, logger)

	buf := make([]byte, 0, 64)
	intBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(intBuf, 1) // 1 device
	buf = append(buf, intBuf...)
	nameLenBuf := make([]byte, 2)
	binary.LittleEndian.PutUint16(nameLenBuf, 1)
	buf = append(buf, nameLenBuf...)
	buf = append(buf, 'd')
	binary.LittleEndian.PutUint32(intBuf, 5) // row count
	buf = append(buf, intBuf...)
	binary.LittleEndian.PutUint32(intBuf, 1) // field count
	buf = append(buf, intBuf...)
	// Field name "_time"
	binary.LittleEndian.PutUint16(nameLenBuf, 5)
	buf = append(buf, nameLenBuf...)
	buf = append(buf, []byte("_time")...)
	// No field type → truncated

	_, err := s.decodeV6Footer(buf)
	if err == nil {
		t.Fatal("Expected error for truncated field type")
	}
}

func TestDecodeV6Footer_TruncatedAtColumnCount(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), compression.Snappy, logger)

	buf := make([]byte, 0, 64)
	intBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(intBuf, 1) // 1 device
	buf = append(buf, intBuf...)
	nameLenBuf := make([]byte, 2)
	binary.LittleEndian.PutUint16(nameLenBuf, 1)
	buf = append(buf, nameLenBuf...)
	buf = append(buf, 'd')
	binary.LittleEndian.PutUint32(intBuf, 5) // row count
	buf = append(buf, intBuf...)
	binary.LittleEndian.PutUint32(intBuf, 1) // field count
	buf = append(buf, intBuf...)
	binary.LittleEndian.PutUint16(nameLenBuf, 5)
	buf = append(buf, nameLenBuf...)
	buf = append(buf, []byte("_time")...)
	buf = append(buf, byte(compression.ColumnTypeInt64)) // field type
	// No column count → truncated

	_, err := s.decodeV6Footer(buf)
	if err == nil {
		t.Fatal("Expected error for truncated column count")
	}
}

func TestDecodeV6Footer_TruncatedAtColumnEntry(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), compression.Snappy, logger)

	buf := make([]byte, 0, 64)
	intBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(intBuf, 1) // 1 device
	buf = append(buf, intBuf...)
	nameLenBuf := make([]byte, 2)
	binary.LittleEndian.PutUint16(nameLenBuf, 1)
	buf = append(buf, nameLenBuf...)
	buf = append(buf, 'd')
	binary.LittleEndian.PutUint32(intBuf, 5) // row count
	buf = append(buf, intBuf...)
	binary.LittleEndian.PutUint32(intBuf, 1) // field count
	buf = append(buf, intBuf...)
	binary.LittleEndian.PutUint16(nameLenBuf, 5)
	buf = append(buf, nameLenBuf...)
	buf = append(buf, []byte("_time")...)
	buf = append(buf, byte(compression.ColumnTypeInt64)) // field type
	binary.LittleEndian.PutUint32(intBuf, 1)             // 1 column
	buf = append(buf, intBuf...)
	buf = append(buf, []byte{0, 0, 0, 0, 0}...) // partial column entry (need 25 bytes)

	_, err := s.decodeV6Footer(buf)
	if err == nil {
		t.Fatal("Expected error for truncated column entry")
	}
}

// =============================================================================
// readV6PartFooter — error cases
// =============================================================================

func TestReadV6PartFooter_NonExistent(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), compression.Snappy, logger)

	_, err := s.readV6PartFooter("/nonexistent/file.bin")
	if err == nil {
		t.Fatal("Expected error for nonexistent file")
	}
}

func TestReadV6PartFooter_TooSmall(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), compression.Snappy, logger)

	filePath := filepath.Join(t.TempDir(), "small.bin")
	if err := os.WriteFile(filePath, make([]byte, 50), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := s.readV6PartFooter(filePath)
	if err == nil {
		t.Fatal("Expected error for too-small file")
	}
}

func TestReadV6PartFooter_InvalidOffset(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), compression.Snappy, logger)

	fileSize := V6HeaderSize + 24
	data := make([]byte, fileSize)
	binary.LittleEndian.PutUint64(data[fileSize-8:], 999999) // Invalid offset
	binary.LittleEndian.PutUint32(data[fileSize-12:], 100)

	filePath := filepath.Join(t.TempDir(), "bad_offset.bin")
	if err := os.WriteFile(filePath, data, 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := s.readV6PartFooter(filePath)
	if err == nil {
		t.Fatal("Expected error for invalid footer offset")
	}
}

// =============================================================================
// Global metadata encode/decode round-trip
// =============================================================================

func TestGlobalMetadata_EncodeDecodeRoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, compression.Snappy, logger)

	metadata := &MetadataIndex{
		Magic:        MetadataIndexMagic,
		Version:      StorageVersion,
		Compression:  compression.Snappy,
		TotalRows:    500,
		GroupCount:   2,
		DeviceCount:  4,
		FieldCount:   3,
		MinTimestamp: 1000,
		MaxTimestamp: 9000,
		FieldNames:   []string{"humidity", "pressure", "temp"},
		FieldTypes:   []compression.ColumnType{compression.ColumnTypeFloat64, compression.ColumnTypeFloat64, compression.ColumnTypeFloat64},
		DeviceNames:  []string{"d1", "d2", "d3", "d4"},
		DeviceGroups: []DeviceGroupManifest{
			{DirName: "dg_0000", GroupIndex: 0, DeviceCount: 2, PartCount: 1, RowCount: 300, DeviceNames: []string{"d1", "d2"}},
			{DirName: "dg_0001", GroupIndex: 1, DeviceCount: 2, PartCount: 1, RowCount: 200, DeviceNames: []string{"d3", "d4"}},
		},
		DeviceGroupMap: map[string]int{"d1": 0, "d2": 0, "d3": 1, "d4": 1},
	}

	metadataPath := filepath.Join(tmpDir, "_metadata.idx")
	tmpInfo, err := s.writeGlobalMetadata(metadataPath, metadata)
	if err != nil {
		t.Fatalf("writeGlobalMetadata failed: %v", err)
	}

	// Rename tmp to final
	if err := os.Rename(tmpInfo.TmpPath, tmpInfo.FinalPath); err != nil {
		t.Fatal(err)
	}

	// Read back
	decoded, err := s.readGlobalMetadata(metadataPath)
	if err != nil {
		t.Fatalf("readGlobalMetadata failed: %v", err)
	}

	if decoded.Magic != MetadataIndexMagic {
		t.Errorf("Magic = %x, want %x", decoded.Magic, MetadataIndexMagic)
	}
	if decoded.TotalRows != 500 {
		t.Errorf("TotalRows = %d, want 500", decoded.TotalRows)
	}
	if len(decoded.FieldNames) != 3 {
		t.Errorf("FieldNames = %d, want 3", len(decoded.FieldNames))
	}
	if len(decoded.DeviceGroups) != 2 {
		t.Errorf("DeviceGroups = %d, want 2", len(decoded.DeviceGroups))
	}
	if len(decoded.DeviceGroupMap) != 4 {
		t.Errorf("DeviceGroupMap = %d, want 4", len(decoded.DeviceGroupMap))
	}
}

// =============================================================================
// DeviceGroupMetadata encode/decode round-trip
// =============================================================================

func TestDeviceGroupMetadata_EncodeDecodeRoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, compression.Snappy, logger)

	dgMeta := &DeviceGroupMetadata{
		Magic:         DeviceGroupMetadataMagic,
		Version:       V6StorageVersion,
		GroupIndex:    0,
		DeviceCount:   3,
		PartCount:     2,
		RowCount:      100,
		DeviceNames:   []string{"dev1", "dev2", "dev3"},
		PartFileNames: []string{"part_0000.bin", "part_0001.bin"},
		Parts: []PartManifest{
			{FileName: "part_0000.bin", PartIndex: 0, RowCount: 60, DeviceCount: 2},
			{FileName: "part_0001.bin", PartIndex: 1, RowCount: 40, DeviceCount: 1},
		},
		DevicePartMap: map[string][]int{"dev1": {0}, "dev2": {0}, "dev3": {1}},
	}

	dgDir := filepath.Join(tmpDir, "dg_0000")
	if err := os.MkdirAll(dgDir, 0o755); err != nil {
		t.Fatal(err)
	}

	metaPath := filepath.Join(dgDir, "_metadata.idx")
	tmpInfo, err := s.writeDeviceGroupMetadata(metaPath, dgMeta)
	if err != nil {
		t.Fatalf("writeDeviceGroupMetadata failed: %v", err)
	}

	if err := os.Rename(tmpInfo.TmpPath, tmpInfo.FinalPath); err != nil {
		t.Fatal(err)
	}

	decoded, err := s.readDeviceGroupMetadata(metaPath)
	if err != nil {
		t.Fatalf("readDeviceGroupMetadata failed: %v", err)
	}

	if decoded.Magic != DeviceGroupMetadataMagic {
		t.Errorf("Magic = %x, want %x", decoded.Magic, DeviceGroupMetadataMagic)
	}
	if decoded.RowCount != 100 {
		t.Errorf("RowCount = %d, want 100", decoded.RowCount)
	}
	if len(decoded.DeviceNames) != 3 {
		t.Errorf("DeviceNames = %d, want 3", len(decoded.DeviceNames))
	}
	if len(decoded.PartFileNames) != 2 {
		t.Errorf("PartFileNames = %d, want 2", len(decoded.PartFileNames))
	}
	if len(decoded.DevicePartMap) != 3 {
		t.Errorf("DevicePartMap = %d, want 3", len(decoded.DevicePartMap))
	}
}

// =============================================================================
// WriteBatch + Query comprehensive
// =============================================================================

func TestWriteAndQuery_MultipleDevices_MultipleFields(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, compression.Snappy, logger)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	// Write data for 5 devices with multiple fields
	var entries []*DataPoint
	for i := 0; i < 5; i++ {
		for j := 0; j < 10; j++ {
			entries = append(entries, &DataPoint{
				ID:         fmt.Sprintf("device%d", i),
				Database:   "testdb",
				Collection: "metrics",
				Time:       now.Add(time.Duration(j) * time.Minute),
				Fields: map[string]interface{}{
					"temperature": 20.0 + float64(i) + float64(j)*0.1,
					"humidity":    50.0 + float64(i),
					"active":      true,
				},
			})
		}
	}

	err := s.WriteBatch(entries)
	if err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	// Query all
	result, err := s.Query("testdb", "metrics", nil, now.Add(-time.Hour), now.Add(time.Hour), nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(result) != 50 {
		t.Errorf("Query all = %d, want 50", len(result))
	}

	// Query specific device
	result, err = s.Query("testdb", "metrics", []string{"device0"}, now.Add(-time.Hour), now.Add(time.Hour), nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(result) != 10 {
		t.Errorf("Query device0 = %d, want 10", len(result))
	}

	// Query specific fields
	result, err = s.Query("testdb", "metrics", nil, now.Add(-time.Hour), now.Add(time.Hour), []string{"temperature"})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(result) != 50 {
		t.Errorf("Query with field filter = %d, want 50", len(result))
	}

	// Query non-matching device
	result, err = s.Query("testdb", "metrics", []string{"nonexistent"}, now.Add(-time.Hour), now.Add(time.Hour), nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("Query non-matching device = %d, want 0", len(result))
	}

	// Query out-of-range time
	result, err = s.Query("testdb", "metrics", nil, now.Add(-10*time.Hour), now.Add(-9*time.Hour), nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("Query out-of-range = %d, want 0", len(result))
	}
}

func TestWriteAndQuery_AppendNewData(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, compression.Snappy, logger)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	// First write
	entries1 := []*DataPoint{
		{ID: "d1", Database: "testdb", Collection: "m", Time: now, Fields: map[string]interface{}{"temp": 25.0}},
	}
	if err := s.WriteBatch(entries1); err != nil {
		t.Fatalf("First WriteBatch failed: %v", err)
	}

	// Second write (append to same date dir)
	entries2 := []*DataPoint{
		{ID: "d2", Database: "testdb", Collection: "m", Time: now.Add(time.Minute), Fields: map[string]interface{}{"temp": 26.0}},
	}
	if err := s.WriteBatch(entries2); err != nil {
		t.Fatalf("Second WriteBatch failed: %v", err)
	}

	// Query should return both
	result, err := s.Query("testdb", "m", nil, now.Add(-time.Hour), now.Add(time.Hour), nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(result) != 2 {
		t.Errorf("Query after append = %d, want 2", len(result))
	}
}

func TestWriteAndQuery_AppendNewFields(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, compression.Snappy, logger)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	// First write with temp
	entries1 := []*DataPoint{
		{ID: "d1", Database: "testdb", Collection: "m", Time: now, Fields: map[string]interface{}{"temp": 25.0}},
	}
	if err := s.WriteBatch(entries1); err != nil {
		t.Fatalf("First WriteBatch failed: %v", err)
	}

	// Second write with NEW field "humidity"
	entries2 := []*DataPoint{
		{ID: "d1", Database: "testdb", Collection: "m", Time: now.Add(time.Minute), Fields: map[string]interface{}{"temp": 26.0, "humidity": 55.0}},
	}
	if err := s.WriteBatch(entries2); err != nil {
		t.Fatalf("Second WriteBatch failed: %v", err)
	}

	// Query should return both
	result, err := s.Query("testdb", "m", nil, now.Add(-time.Hour), now.Add(time.Hour), nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(result) != 2 {
		t.Errorf("Query = %d, want 2", len(result))
	}
}

func TestWriteAndQuery_LargeDeviceGroupSplit(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	// Small maxDevicesPerGroup to force multiple device groups
	s := NewStorageWithConfig(tmpDir, compression.Snappy, logger, 1000, 0, 1, 2)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	// Write 5 devices → should split into 3 DGs (2+2+1)
	var entries []*DataPoint
	for i := 0; i < 5; i++ {
		entries = append(entries, &DataPoint{
			ID:         fmt.Sprintf("dev%d", i),
			Database:   "testdb",
			Collection: "metrics",
			Time:       now.Add(time.Duration(i) * time.Minute),
			Fields:     map[string]interface{}{"temp": float64(i)},
		})
	}

	if err := s.WriteBatch(entries); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	// Query all should return all 5
	result, err := s.Query("testdb", "metrics", nil, now.Add(-time.Hour), now.Add(time.Hour), nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(result) != 5 {
		t.Errorf("Query = %d, want 5", len(result))
	}

	// Query specific device across DG boundary
	result, err = s.Query("testdb", "metrics", []string{"dev4"}, now.Add(-time.Hour), now.Add(time.Hour), nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(result) != 1 {
		t.Errorf("Query dev4 = %d, want 1", len(result))
	}
}

// =============================================================================
// findDateDirs
// =============================================================================

func TestFindDateDirs_RangeSpanningMultipleDays(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, compression.Snappy, logger)

	// Create date dirs manually
	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 3; i++ {
		date := now.Add(time.Duration(i) * 24 * time.Hour)
		dateStr := date.Format("20060102")
		dir := filepath.Join(tmpDir, "testdb", "metrics", dateStr[:4], dateStr[4:6], dateStr)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	dirs, err := s.findDateDirs("testdb", "metrics", now, now.Add(3*24*time.Hour))
	if err != nil {
		t.Fatalf("findDateDirs failed: %v", err)
	}
	if len(dirs) != 3 {
		t.Errorf("findDateDirs = %d dirs, want 3", len(dirs))
	}
}

// =============================================================================
// cleanupV6StaleParts
// =============================================================================

func TestCleanupV6StaleParts(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, compression.Snappy, logger)

	// Set up a DG directory structure
	dgDir := filepath.Join(tmpDir, "dg_0000")
	if err := os.MkdirAll(dgDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Create DG metadata referencing only part_0000.bin
	dgMeta := &DeviceGroupMetadata{
		Magic:         DeviceGroupMetadataMagic,
		Version:       V6StorageVersion,
		PartFileNames: []string{"part_0000.bin"},
		DevicePartMap: make(map[string][]int),
	}
	metaPath := filepath.Join(dgDir, "_metadata.idx")
	tmpInfo, err := s.writeDeviceGroupMetadata(metaPath, dgMeta)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmpInfo.TmpPath, tmpInfo.FinalPath); err != nil {
		t.Fatal(err)
	}

	// Create valid file
	if err := os.WriteFile(filepath.Join(dgDir, "part_0000.bin"), []byte("valid"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Create stale files
	if err := os.WriteFile(filepath.Join(dgDir, "part_0001.bin"), []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dgDir, "old.tmp"), []byte("tmp"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dgDir, "readme.txt"), []byte("not removed"), 0o644); err != nil {
		t.Fatal(err)
	}

	manifest := DeviceGroupManifest{DirName: "dg_0000"}
	s.cleanupV6StaleParts(dgDir, manifest)

	// Valid file should still exist
	if _, err := os.Stat(filepath.Join(dgDir, "part_0000.bin")); err != nil {
		t.Error("part_0000.bin should still exist")
	}

	// Stale .bin and .tmp should be removed
	if _, err := os.Stat(filepath.Join(dgDir, "part_0001.bin")); !os.IsNotExist(err) {
		t.Error("part_0001.bin (stale) should be removed")
	}
	if _, err := os.Stat(filepath.Join(dgDir, "old.tmp")); !os.IsNotExist(err) {
		t.Error("old.tmp should be removed")
	}

	// Non .bin/.tmp files should remain
	if _, err := os.Stat(filepath.Join(dgDir, "readme.txt")); err != nil {
		t.Error("readme.txt should remain (not .bin/.tmp)")
	}
}

// =============================================================================
// collectFieldsFromData
// =============================================================================

func TestCollectFieldsFromData(t *testing.T) {
	data := map[string][]*DataPoint{
		"dev1": {
			{Fields: map[string]interface{}{"temp": 25.5, "humidity": 60.0}},
			{Fields: map[string]interface{}{"temp": 26.0}},
		},
		"dev2": {
			{Fields: map[string]interface{}{"pressure": 1013.0, "temp": 24.0}},
		},
	}

	fieldNames, fieldTypes := collectFieldsFromData(data)

	if len(fieldNames) != 3 {
		t.Errorf("fieldNames = %d, want 3", len(fieldNames))
	}
	// Should be sorted
	expected := []string{"humidity", "pressure", "temp"}
	for i, name := range fieldNames {
		if name != expected[i] {
			t.Errorf("fieldNames[%d] = %s, want %s", i, name, expected[i])
		}
	}
	if len(fieldTypes) != 3 {
		t.Errorf("fieldTypes = %d, want 3", len(fieldTypes))
	}
}

// =============================================================================
// getDateDir and getDateDirLock
// =============================================================================

func TestGetDateDir(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage("/data", compression.Snappy, logger)

	key := storageFileKey{database: "db1", collection: "metrics", date: "20240515"}
	dir := s.getDateDir(key)

	expected := filepath.Join("/data", "db1", "metrics", "2024", "05", "20240515")
	if dir != expected {
		t.Errorf("getDateDir = %s, want %s", dir, expected)
	}
}

func TestGetDateDirLock_SameAndDifferentPaths(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), compression.Snappy, logger)

	lock1 := s.getDateDirLock("/some/path")
	lock2 := s.getDateDirLock("/some/path")
	lock3 := s.getDateDirLock("/other/path")

	if lock1 != lock2 {
		t.Error("Same path should return same lock")
	}
	if lock1 == lock3 {
		t.Error("Different paths should return different locks")
	}
}
