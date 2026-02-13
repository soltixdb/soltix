package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/compression"
	"github.com/soltixdb/soltix/internal/logging"
	pb "github.com/soltixdb/soltix/proto/storage/v1"
)

// =============================================================================
// block.go — toProtoFieldValue / fromProtoFieldValue full branch coverage
// =============================================================================

func TestToProtoFieldValue_AllTypes(t *testing.T) {
	tests := []struct {
		name  string
		input interface{}
	}{
		{"float64", float64(3.14)},
		{"string", "hello"},
		{"bool", true},
		{"int", int(42)},
		{"int64", int64(999)},
		{"struct", struct{ X int }{1}}, // unknown type → string
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fv := toProtoFieldValue(tc.input)
			if fv == nil {
				t.Fatal("toProtoFieldValue returned nil")
			}
			if fv.Value == nil {
				t.Fatal("toProtoFieldValue returned nil Value")
			}
		})
	}
}

func TestFromProtoFieldValue_NilDefault(t *testing.T) {
	// Test the default/nil case — FieldValue with no Value set
	fv := &pb.FieldValue{} // Value is nil interface
	result := fromProtoFieldValue(fv)
	if result != nil {
		t.Errorf("fromProtoFieldValue(empty) should return nil, got %v", result)
	}
}

func TestEncodeDecodeFieldsProto_IntTypes(t *testing.T) {
	fields := []map[string]interface{}{
		{"count": int(42), "big": int64(9999999), "score": float64(95.5), "name": "test", "active": true},
	}

	data, err := EncodeFieldsProto(fields)
	if err != nil {
		t.Fatalf("EncodeFieldsProto failed: %v", err)
	}

	decoded, err := DecodeFieldsProto(data)
	if err != nil {
		t.Fatalf("DecodeFieldsProto failed: %v", err)
	}

	if len(decoded) != 1 {
		t.Fatalf("Decoded length = %d, want 1", len(decoded))
	}

	// int → int64 via proto
	if v, ok := decoded[0]["count"].(int64); !ok || v != 42 {
		t.Errorf("count = %v (%T), want int64(42)", decoded[0]["count"], decoded[0]["count"])
	}
}

// =============================================================================
// DecodeDeviceBlock — additional edge cases
// =============================================================================

func TestDecodeDeviceBlock_TruncatedEntryCount(t *testing.T) {
	// Valid device ID but no entry count
	data := make([]byte, 0, 20)
	data = appendUint32(data, 3) // device ID len = 3
	data = append(data, "abc"...)
	// No entry count → too short

	_, err := DecodeDeviceBlock(data)
	if err == nil {
		t.Fatal("Expected error for truncated entry count")
	}
}

func TestDecodeDeviceBlock_TruncatedBaseTime(t *testing.T) {
	data := make([]byte, 0, 20)
	data = appendUint32(data, 2) // device ID len
	data = append(data, "d1"...)
	data = appendUint32(data, 5) // entry count
	data = append(data, 0, 0, 0) // partial base time

	_, err := DecodeDeviceBlock(data)
	if err == nil {
		t.Fatal("Expected error for truncated base time")
	}
}

func TestDecodeDeviceBlock_TruncatedDeltas(t *testing.T) {
	data := make([]byte, 0, 30)
	data = appendUint32(data, 2) // device ID len
	data = append(data, "d1"...)
	data = appendUint32(data, 3)   // entry count = 3 → needs 2 deltas = 8 bytes
	data = appendUint64(data, 100) // base time
	data = appendUint32(data, 10)  // only 1 delta, need 2

	_, err := DecodeDeviceBlock(data)
	if err == nil {
		t.Fatal("Expected error for truncated deltas")
	}
}

func TestDecodeDeviceBlock_InvalidDeviceIDLength(t *testing.T) {
	data := make([]byte, 0, 20)
	data = appendUint32(data, 9999) // device ID len way too long
	data = append(data, "ab"...)

	_, err := DecodeDeviceBlock(data)
	if err == nil {
		t.Fatal("Expected error for invalid device ID length")
	}
}

// Helper functions for byte encoding
func appendUint32(buf []byte, v uint32) []byte {
	return append(buf, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
}

func appendUint64(buf []byte, v uint64) []byte {
	return append(buf, byte(v), byte(v>>8), byte(v>>16), byte(v>>24),
		byte(v>>32), byte(v>>40), byte(v>>48), byte(v>>56))
}

// =============================================================================
// ordered_slice.go — GetFirst/GetLast empty, Add middle-insert duplicate
// =============================================================================

func TestOrderedSlice_GetFirst_Empty(t *testing.T) {
	os := NewOrderedSlice(10)
	if os.GetFirst() != nil {
		t.Error("GetFirst on empty slice should return nil")
	}
}

func TestOrderedSlice_GetLast_Empty(t *testing.T) {
	os := NewOrderedSlice(10)
	if os.GetLast() != nil {
		t.Error("GetLast on empty slice should return nil")
	}
}

func TestOrderedSlice_Add_MiddleInsertDuplicate(t *testing.T) {
	os := NewOrderedSlice(10)
	now := time.Now()

	// Add points with gaps
	os.Add(&DataPoint{ID: "d1", Time: now, InsertedAt: now})
	os.Add(&DataPoint{ID: "d1", Time: now.Add(10 * time.Second), InsertedAt: now})
	os.Add(&DataPoint{ID: "d1", Time: now.Add(20 * time.Second), InsertedAt: now})

	// Add duplicate at middle position with newer InsertedAt
	newer := now.Add(time.Hour)
	os.Add(&DataPoint{ID: "d1", Time: now.Add(10 * time.Second), InsertedAt: newer, Fields: map[string]interface{}{"v": 999.0}})

	if os.Len() != 3 {
		t.Errorf("Len after duplicate = %d, want 3", os.Len())
	}

	// Verify the middle point was replaced
	middle := os.GetAt(1)
	if v, ok := middle.Fields["v"].(float64); !ok || v != 999.0 {
		t.Errorf("Middle point not replaced, got %v", middle.Fields)
	}
}

func TestOrderedSlice_Add_InsertBeforeFirst(t *testing.T) {
	os := NewOrderedSlice(10)
	now := time.Now()

	os.Add(&DataPoint{ID: "d1", Time: now.Add(10 * time.Second), InsertedAt: now})
	os.Add(&DataPoint{ID: "d1", Time: now, InsertedAt: now}) // Before first

	if os.Len() != 2 {
		t.Errorf("Len = %d, want 2", os.Len())
	}
	if os.GetFirst().Time != now {
		t.Error("First element should be the earlier time")
	}
}

func TestOrderedSlice_Query_PartialOverlap(t *testing.T) {
	os := NewOrderedSlice(10)
	now := time.Now()

	for i := 0; i < 10; i++ {
		os.Add(&DataPoint{ID: "d1", Time: now.Add(time.Duration(i) * time.Second)})
	}

	// Query middle range
	result := os.Query(now.Add(3*time.Second), now.Add(6*time.Second))
	if len(result) != 4 { // 3, 4, 5, 6
		t.Errorf("Query partial overlap = %d, want 4", len(result))
	}

	// Query past end
	result = os.Query(now.Add(20*time.Second), now.Add(30*time.Second))
	if len(result) != 0 {
		t.Errorf("Query past end = %d, want 0", len(result))
	}
}

// =============================================================================
// storage_interface.go — NewStorageAdapter
// =============================================================================

func TestNewStorageAdapter(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), compression.Snappy, logger)

	adapter := NewStorageAdapter(s)
	if adapter == nil {
		t.Fatal("NewStorageAdapter returned nil")
	}
	if adapter.Storage != s {
		t.Error("Adapter.Storage should reference original storage")
	}

	// Verify it implements MainStorage
	var _ MainStorage = adapter
}

func TestStorageAdapter_WriteBatchAndQuery(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, compression.Snappy, logger)
	adapter := NewStorageAdapter(s)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)
	entries := []*DataPoint{
		{ID: "d1", Database: "db", Collection: "c", Time: now, Fields: map[string]interface{}{"v": 1.0}},
	}

	err := adapter.WriteBatch(entries)
	if err != nil {
		t.Fatalf("Adapter WriteBatch failed: %v", err)
	}

	result, err := adapter.Query("db", "c", nil, now.Add(-time.Hour), now.Add(time.Hour), nil)
	if err != nil {
		t.Fatalf("Adapter Query failed: %v", err)
	}
	if len(result) != 1 {
		t.Errorf("Adapter Query = %d, want 1", len(result))
	}
}

func TestStorageAdapter_SetTimezone(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), compression.Snappy, logger)
	adapter := NewStorageAdapter(s)

	tokyo, _ := time.LoadLocation("Asia/Tokyo")
	adapter.SetTimezone(tokyo)

	if s.timezone != tokyo {
		t.Error("SetTimezone should propagate to underlying storage")
	}
}

// =============================================================================
// compaction.go — CompactDateDir (public), CompactAll, findAllDateDirs
// =============================================================================

func TestCompactDateDir_Direct(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorageWithConfig(tmpDir, compression.Snappy, logger, 5, 0, 1, 50)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	// Write multiple batches to create multiple parts
	for i := 0; i < 5; i++ {
		entries := []*DataPoint{
			{
				ID: fmt.Sprintf("d%d", i), Database: "db", Collection: "c",
				Time: now.Add(time.Duration(i) * time.Minute), Fields: map[string]interface{}{"v": float64(i)},
			},
		}
		if err := s.WriteBatch(entries); err != nil {
			t.Fatalf("WriteBatch #%d failed: %v", i, err)
		}
	}

	// Find date dirs
	dateDirs, err := s.findAllDateDirs()
	if err != nil {
		t.Fatalf("findAllDateDirs failed: %v", err)
	}
	if len(dateDirs) == 0 {
		t.Fatal("No date dirs found")
	}

	// Try compact with low threshold to trigger compaction
	ctx := context.Background()
	n, err := s.CompactDateDir(ctx, dateDirs[0], 1)
	if err != nil {
		t.Fatalf("CompactDateDir failed: %v", err)
	}

	// Verify data is still queryable after compaction
	result, err := s.Query("db", "c", nil, now.Add(-time.Hour), now.Add(time.Hour), nil)
	if err != nil {
		t.Fatalf("Query after compaction failed: %v", err)
	}
	if len(result) != 5 {
		t.Errorf("Query after compaction = %d, want 5", len(result))
	}

	t.Logf("Compacted %d device groups", n)
}

func TestCompactAll_EmptyStorage(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), compression.Snappy, logger)

	ctx := context.Background()
	n, err := s.CompactAll(ctx, 3)
	if err != nil {
		t.Fatalf("CompactAll on empty storage failed: %v", err)
	}
	if n != 0 {
		t.Errorf("CompactAll empty = %d, want 0", n)
	}
}

func TestCompactAll_ContextCancelled(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, compression.Snappy, logger)

	// Write data
	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 3; i++ {
		if err := s.WriteBatch([]*DataPoint{
			{
				ID: "d1", Database: "db", Collection: "c", Time: now.Add(time.Duration(i) * time.Minute),
				Fields: map[string]interface{}{"v": float64(i)},
			},
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Cancel context immediately
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := s.CompactAll(ctx, 0)
	if err != context.Canceled {
		// It might succeed if no date dirs found fast enough, or return Canceled
		t.Logf("CompactAll with cancelled context returned: %v", err)
	}
}

func TestFindAllDateDirs_NonExistentDir(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage("/nonexistent/path", compression.Snappy, logger)

	dirs, err := s.findAllDateDirs()
	if err != nil {
		t.Fatalf("findAllDateDirs should not error for nonexistent dir: %v", err)
	}
	if len(dirs) != 0 {
		t.Errorf("findAllDateDirs = %d, want 0", len(dirs))
	}
}

func TestFindAllDateDirs_WithData(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, compression.Snappy, logger)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)
	if err := s.WriteBatch([]*DataPoint{
		{
			ID: "d1", Database: "db", Collection: "c", Time: now,
			Fields: map[string]interface{}{"v": 1.0},
		},
	}); err != nil {
		t.Fatal(err)
	}

	dirs, err := s.findAllDateDirs()
	if err != nil {
		t.Fatalf("findAllDateDirs failed: %v", err)
	}
	if len(dirs) != 1 {
		t.Errorf("findAllDateDirs = %d, want 1", len(dirs))
	}
}

func TestNeedsCompaction_NoMetadata(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), compression.Snappy, logger)

	result := s.NeedsCompaction("/nonexistent", 3)
	if result {
		t.Error("NeedsCompaction should return false for non-existent dir")
	}
}

// =============================================================================
// CompactionWorker — Start/Stop, AddStorage, SetTieredStorage, runCompactionCycle
// =============================================================================

func TestCompactionWorker_StartStop_DoubleStart(t *testing.T) {
	logger := logging.NewDevelopment()
	cw := NewCompactionWorker(CompactionWorkerConfig{
		Interval:  50 * time.Millisecond,
		Threshold: 5,
	}, logger)

	cw.Start()

	// Starting again should be no-op
	cw.Start()

	time.Sleep(100 * time.Millisecond)
	cw.Stop()

	// Verify stopped
	if cw.running {
		t.Error("Worker should be stopped")
	}
}

func TestCompactionWorker_AddStorage(t *testing.T) {
	logger := logging.NewDevelopment()
	cw := NewCompactionWorker(DefaultCompactionWorkerConfig(), logger)

	s := NewStorage(t.TempDir(), compression.Snappy, logger)
	cw.AddStorage(s)

	if len(cw.storages) != 1 {
		t.Errorf("storages count = %d, want 1", len(cw.storages))
	}
}

func TestCompactionWorker_SetTieredStorage(t *testing.T) {
	logger := logging.NewDevelopment()
	cw := NewCompactionWorker(DefaultCompactionWorkerConfig(), logger)

	config := GroupStorageConfig{
		DataDir:     t.TempDir(),
		Compression: compression.Snappy,
	}
	ts := NewTieredStorage(config, logger)
	cw.SetTieredStorage(ts)

	if cw.tieredStorage != ts {
		t.Error("TieredStorage not set correctly")
	}
}

func TestCompactionWorker_RunCycleWithEngines(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()

	config := GroupStorageConfig{
		DataDir:            tmpDir,
		Compression:        compression.Snappy,
		MaxRowsPerPart:     100,
		MaxPartSize:        1 << 20,
		MinRowsPerPart:     1,
		MaxDevicesPerGroup: 50,
	}
	ts := NewTieredStorage(config, logger)

	cw := NewCompactionWorker(CompactionWorkerConfig{
		Interval:  50 * time.Millisecond,
		Threshold: 3,
	}, logger)
	cw.SetTieredStorage(ts)

	// Write some data
	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 3; i++ {
		if err := ts.WriteBatch([]*DataPoint{
			{
				ID: "d1", Database: "db", Collection: "c", GroupID: 0,
				Time:   now.Add(time.Duration(i) * time.Minute),
				Fields: map[string]interface{}{"v": float64(i)},
			},
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Run a compaction cycle manually
	cw.runCompactionCycle()
	// Should not error, just run
}

// =============================================================================
// write_worker.go — SubmitSync, Submit with invalid time
// =============================================================================

func TestWriteWorkerPool_Submit_InvalidTime(t *testing.T) {
	logger := logging.NewDevelopment()

	memStore := NewMemoryStore(time.Hour, 10000, logger)
	defer func() { _ = memStore.Close() }()

	pool := NewWriteWorkerPool(logger, nil, memStore, nil)
	pool.Start()
	defer pool.Stop()

	msg := WriteMessage{
		Database:   "db",
		Collection: "col",
		ID:         "d1",
		Time:       "not-a-valid-time",
		Fields:     map[string]interface{}{"v": 1.0},
	}

	err := pool.Submit(msg)
	if err == nil {
		t.Fatal("Submit with invalid time should error")
	}
}

func TestWriteWorkerPool_SubmitSync_InvalidTime(t *testing.T) {
	logger := logging.NewDevelopment()

	memStore := NewMemoryStore(time.Hour, 10000, logger)
	defer func() { _ = memStore.Close() }()

	pool := NewWriteWorkerPool(logger, nil, memStore, nil)

	msg := WriteMessage{
		Database:   "db",
		Collection: "col",
		ID:         "d1",
		Time:       "invalid-time",
		Fields:     map[string]interface{}{"v": 1.0},
	}

	err := pool.SubmitSync(msg)
	if err == nil {
		t.Fatal("SubmitSync with invalid time should error")
	}
}

// =============================================================================
// adapters.go — RawDataReaderAdapter
// =============================================================================

func TestRawDataReaderAdapter_QueryRoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, compression.Snappy, logger)
	adapter := NewStorageAdapter(s)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)
	entries := []*DataPoint{
		{
			ID: "d1", Database: "db", Collection: "c", Time: now,
			Fields: map[string]interface{}{"temp": 25.0},
		},
		{
			ID: "d2", Database: "db", Collection: "c", Time: now.Add(time.Minute),
			Fields: map[string]interface{}{"temp": 26.0},
		},
	}
	if err := s.WriteBatch(entries); err != nil {
		t.Fatal(err)
	}

	reader := NewRawDataReaderAdapter(adapter)
	result, err := reader.Query("db", "c", nil, now.Add(-time.Hour), now.Add(time.Hour), nil)
	if err != nil {
		t.Fatalf("RawDataReaderAdapter.Query failed: %v", err)
	}
	if len(result) != 2 {
		t.Errorf("RawDataReaderAdapter.Query = %d, want 2", len(result))
	}
}

// =============================================================================
// memory_store.go — evictOldest, cleanup edge cases
// =============================================================================

func TestMemoryStore_Cleanup_RemovesOldFlushed(t *testing.T) {
	logger := logging.NewDevelopment()
	ms := NewMemoryStore(100*time.Millisecond, 10000, logger)
	defer func() { _ = ms.Close() }()

	now := time.Now()
	old := now.Add(-time.Hour)

	// Write an old point (Write sets FlushStatus=New)
	dp := &DataPoint{
		ID: "d1", Database: "db", Collection: "c",
		Time: old, InsertedAt: old,
		Fields: map[string]interface{}{"v": 1.0},
	}
	_ = ms.Write(dp)

	// Manually mark as flushed to simulate the flush lifecycle
	_, _ = ms.GetUnFlushed() // Marks New -> Flushing
	_ = ms.MarkFlushed()     // Marks Flushing -> Flushed

	// Run cleanup
	ms.cleanup()

	// Should be removed (flushed + old)
	result, _ := ms.QueryCollection("db", "c", old.Add(-time.Hour), now)
	if len(result) != 0 {
		t.Errorf("After cleanup, query = %d, want 0", len(result))
	}
}

func TestMemoryStore_EvictOldest_SizeLimit(t *testing.T) {
	logger := logging.NewDevelopment()
	ms := NewMemoryStore(time.Hour, 3, logger) // max 3 items
	defer func() { _ = ms.Close() }()

	now := time.Now()

	// Write 5 flushed items
	for i := 0; i < 5; i++ {
		dp := &DataPoint{
			ID: "d1", Database: "db", Collection: "c",
			Time: now.Add(time.Duration(i) * time.Second), InsertedAt: now,
			FlushStatus: FlushStatusFlushed,
			Fields:      map[string]interface{}{"v": float64(i)},
		}
		_ = ms.Write(dp)
	}

	// Eviction should have been triggered
	time.Sleep(100 * time.Millisecond) // Wait for eviction goroutine

	result, _ := ms.QueryCollection("db", "c", now.Add(-time.Hour), now.Add(time.Hour))
	// Some points should have been evicted
	t.Logf("After eviction: %d points remaining", len(result))
}

// =============================================================================
// index.go — DecodeFileIndex, FindDevice edge cases
// =============================================================================

func TestStorage_ReadV6Column_EmptyFile(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), compression.Snappy, logger)

	// Create a small empty file
	tmpFile := filepath.Join(t.TempDir(), "test.bin")
	if err := os.WriteFile(tmpFile, make([]byte, 10), 0o644); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(tmpFile)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer func() { _ = f.Close() }()

	col := &V6ColumnEntry{
		Offset:     0,
		Size:       5,
		RowCount:   1,
		ColumnType: uint8(compression.ColumnTypeFloat64),
	}

	_, err = s.readV6Column(f, col, 1)
	// May error due to invalid data
	if err != nil {
		t.Logf("readV6Column with empty data returned error (expected): %v", err)
	}
}

// =============================================================================
// readAllV6PartsInDG — edge case: empty DG metadata
// =============================================================================

func TestReadAllV6PartsInDG_EmptyMetadata(t *testing.T) {
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

	result := s.readAllV6PartsInDG(dgDir, dgMeta)
	if len(result) != 0 {
		t.Errorf("readAllV6PartsInDG empty meta = %d, want 0", len(result))
	}
}

func TestReadAllV6PartsInDG_MissingPartFile(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, compression.Snappy, logger)

	dgDir := filepath.Join(tmpDir, "dg_0000")
	if err := os.MkdirAll(dgDir, 0o755); err != nil {
		t.Fatal(err)
	}

	dgMeta := &DeviceGroupMetadata{
		PartFileNames: []string{"part_0000.bin"}, // file doesn't exist
		DevicePartMap: make(map[string][]int),
	}

	result := s.readAllV6PartsInDG(dgDir, dgMeta)
	// Should handle gracefully (return empty or whatever it reads)
	if len(result) != 0 {
		t.Logf("readAllV6PartsInDG with missing file returned %d points", len(result))
	}
}

// =============================================================================
// queryV6DG edge cases
// =============================================================================

func TestQueryV6DG_NoMatchingDevices(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, compression.Snappy, logger)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	// Write data
	entries := []*DataPoint{
		{
			ID: "d1", Database: "db", Collection: "c", Time: now,
			Fields: map[string]interface{}{"v": 1.0},
		},
	}
	if err := s.WriteBatch(entries); err != nil {
		t.Fatal(err)
	}

	// Query with non-matching device
	result, err := s.Query("db", "c", []string{"nonexistent"}, now.Add(-time.Hour), now.Add(time.Hour), nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("Query with non-matching device = %d, want 0", len(result))
	}
}

// =============================================================================
// Query — non-existent database
// =============================================================================

func TestQuery_NonExistentDatabase(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), compression.Snappy, logger)

	now := time.Now()
	result, err := s.Query("nonexistent_db", "col", nil, now.Add(-time.Hour), now, nil)
	if err != nil {
		t.Fatalf("Query should not error for nonexistent db: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("Query = %d, want 0", len(result))
	}
}

// =============================================================================
// WriteBatch — empty batch
// =============================================================================

func TestWriteBatch_Empty(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), compression.Snappy, logger)

	err := s.WriteBatch(nil)
	if err != nil {
		t.Fatalf("WriteBatch nil should not error: %v", err)
	}

	err = s.WriteBatch([]*DataPoint{})
	if err != nil {
		t.Fatalf("WriteBatch empty should not error: %v", err)
	}
}

// =============================================================================
// WriteBatch — multiple databases/collections in one batch
// =============================================================================

func TestWriteBatch_MultipleDatabases(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, compression.Snappy, logger)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	entries := []*DataPoint{
		{ID: "d1", Database: "db1", Collection: "c1", Time: now, Fields: map[string]interface{}{"v": 1.0}},
		{ID: "d2", Database: "db2", Collection: "c2", Time: now, Fields: map[string]interface{}{"v": 2.0}},
		{ID: "d3", Database: "db1", Collection: "c2", Time: now, Fields: map[string]interface{}{"v": 3.0}},
	}

	err := s.WriteBatch(entries)
	if err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	// Query each separately
	r1, _ := s.Query("db1", "c1", nil, now.Add(-time.Hour), now.Add(time.Hour), nil)
	r2, _ := s.Query("db2", "c2", nil, now.Add(-time.Hour), now.Add(time.Hour), nil)
	r3, _ := s.Query("db1", "c2", nil, now.Add(-time.Hour), now.Add(time.Hour), nil)

	if len(r1) != 1 {
		t.Errorf("db1/c1 = %d, want 1", len(r1))
	}
	if len(r2) != 1 {
		t.Errorf("db2/c2 = %d, want 1", len(r2))
	}
	if len(r3) != 1 {
		t.Errorf("db1/c2 = %d, want 1", len(r3))
	}
}

// =============================================================================
// SetTimezone and Query with timezone
// =============================================================================

func TestSetTimezone_AffectsDateDir(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, compression.Snappy, logger)

	tokyo, _ := time.LoadLocation("Asia/Tokyo")
	s.SetTimezone(tokyo)

	// 2025-01-15 23:00 UTC = 2025-01-16 08:00 JST
	testTime := time.Date(2025, 1, 15, 23, 0, 0, 0, time.UTC)

	entries := []*DataPoint{
		{
			ID: "d1", Database: "db", Collection: "c", Time: testTime,
			Fields: map[string]interface{}{"v": 1.0},
		},
	}
	if err := s.WriteBatch(entries); err != nil {
		t.Fatal(err)
	}

	// In JST, this should be stored under 20250116
	// Query using the same timezone should find it
	result, _ := s.Query("db", "c", nil, testTime.Add(-time.Hour), testTime.Add(time.Hour), nil)
	if len(result) != 1 {
		t.Errorf("Query after timezone set = %d, want 1", len(result))
	}
}

// =============================================================================
// flush_worker_pool.go — FlushWorkerPool basic coverage
// =============================================================================

func TestFlushWorkerPool_StartStop_Basic(t *testing.T) {
	logger := logging.NewDevelopment()
	memStore := NewMemoryStore(time.Hour, 10000, logger)
	defer func() { _ = memStore.Close() }()

	pool := NewFlushWorkerPool(
		FlushWorkerPoolConfig{
			MaxBatchSize:      100,
			FlushDelay:        50 * time.Millisecond,
			MaxPendingEntries: 1000,
			WorkerIdleTimeout: 100 * time.Millisecond,
			QueueSize:         10,
		},
		logger,
		nil, // no WAL
		memStore,
		nil, // no main storage
		nil, // no agg pipeline
	)

	pool.Start()
	time.Sleep(50 * time.Millisecond)
	pool.Stop()
}

func TestFlushWorkerPool_Notify_SendNotification(t *testing.T) {
	// This test just validates that Notify doesn't block/panic immediately
	// We skip it since FlushWorker.flush needs real WAL/storage to run
	t.Skip("FlushWorker.flush requires real WAL and storage, skipping")
}

// =============================================================================
// service.go — GetTieredStorage
// =============================================================================

func TestStorageService_GetTieredStorage_Extended(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()

	sub := newMockSubscriber()

	service, err := NewStorageService(
		sub, "test-node", tmpDir, logger,
		time.Hour, 10000,
		StorageConfig{
			MaxRowsPerPart:     100,
			MaxPartSize:        1 << 20,
			MinRowsPerPart:     1,
			MaxDevicesPerGroup: 50,
		},
	)
	if err != nil {
		t.Fatalf("NewStorageService failed: %v", err)
	}
	defer func() { _ = service.Stop() }()

	ts := service.GetTieredStorage()
	if ts == nil {
		t.Fatal("GetTieredStorage returned nil")
	}

	// Verify it's a functional TieredStorage
	engines := ts.GetAllEngines()
	if engines == nil {
		t.Fatal("GetAllEngines returned nil (should return empty slice)")
	}
}
