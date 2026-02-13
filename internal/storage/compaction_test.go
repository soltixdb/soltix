package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/compression"
	"github.com/soltixdb/soltix/internal/logging"
)

// ============================================================================
// Helper: create a Storage instance for tests
// ============================================================================

func newTestStorage(t *testing.T) (*Storage, string) {
	t.Helper()
	tempDir, err := os.MkdirTemp("", "compaction_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	logger := logging.NewDevelopment()
	ms := NewStorage(tempDir, compression.Snappy, logger)
	return ms, tempDir
}

func generateTestPoints(db, col string, deviceIDs []string, baseTime time.Time, countPerDevice int) []*DataPoint {
	var points []*DataPoint
	for _, id := range deviceIDs {
		for i := 0; i < countPerDevice; i++ {
			points = append(points, &DataPoint{
				ID:         id,
				Database:   db,
				Collection: col,
				Time:       baseTime.Add(time.Duration(i) * time.Second),
				Fields: map[string]interface{}{
					"temperature": 20.0 + float64(i)*0.1,
					"humidity":    50.0 + float64(i)*0.05,
				},
			})
		}
	}
	return points
}

// ============================================================================
// Tests for getDateDirLock
// ============================================================================

func TestGetDateDirLock_ReturnsSameMutex(t *testing.T) {
	ms, tempDir := newTestStorage(t)
	defer func() { _ = os.RemoveAll(tempDir) }()

	dirA := "/some/path/dateA"
	dirB := "/some/path/dateB"

	mu1 := ms.getDateDirLock(dirA)
	mu2 := ms.getDateDirLock(dirA)
	mu3 := ms.getDateDirLock(dirB)

	if mu1 != mu2 {
		t.Error("getDateDirLock should return the same mutex for the same dateDir")
	}
	if mu1 == mu3 {
		t.Error("getDateDirLock should return different mutexes for different dateDirs")
	}
}

func TestGetDateDirLock_ConcurrentAccess(t *testing.T) {
	ms, tempDir := newTestStorage(t)
	defer func() { _ = os.RemoveAll(tempDir) }()

	var wg sync.WaitGroup
	mutexes := make([]*sync.Mutex, 100)

	// Concurrently get the same lock — all should return the same pointer
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			mutexes[idx] = ms.getDateDirLock("/shared/dir")
		}(i)
	}
	wg.Wait()

	for i := 1; i < 100; i++ {
		if mutexes[i] != mutexes[0] {
			t.Fatalf("getDateDirLock returned different mutexes under concurrency at index %d", i)
		}
	}
}

// ============================================================================
// Tests for atomicRenameAll
// ============================================================================

func TestAtomicRenameAll_Success(t *testing.T) {
	ms, tempDir := newTestStorage(t)
	defer func() { _ = os.RemoveAll(tempDir) }()

	// Create temp files
	tmpFiles := make([]*TmpFileInfo, 3)
	for i := 0; i < 3; i++ {
		tmpPath := filepath.Join(tempDir, fmt.Sprintf("file_%d.tmp", i))
		finalPath := filepath.Join(tempDir, fmt.Sprintf("file_%d.bin", i))
		if err := os.WriteFile(tmpPath, []byte(fmt.Sprintf("data-%d", i)), 0o644); err != nil {
			t.Fatalf("Failed to create tmp file: %v", err)
		}
		tmpFiles[i] = &TmpFileInfo{TmpPath: tmpPath, FinalPath: finalPath}
	}

	err := ms.atomicRenameAll(tmpFiles)
	if err != nil {
		t.Fatalf("atomicRenameAll failed: %v", err)
	}

	// Verify: tmp files gone, final files exist with correct content
	for i := 0; i < 3; i++ {
		if _, err := os.Stat(tmpFiles[i].TmpPath); !os.IsNotExist(err) {
			t.Errorf("Tmp file %d should not exist after rename", i)
		}
		data, err := os.ReadFile(tmpFiles[i].FinalPath)
		if err != nil {
			t.Errorf("Final file %d not found: %v", i, err)
		}
		expected := fmt.Sprintf("data-%d", i)
		if string(data) != expected {
			t.Errorf("Final file %d content = %q, expected %q", i, string(data), expected)
		}
	}
}

func TestAtomicRenameAll_Empty(t *testing.T) {
	ms, tempDir := newTestStorage(t)
	defer func() { _ = os.RemoveAll(tempDir) }()

	err := ms.atomicRenameAll(nil)
	if err != nil {
		t.Fatalf("atomicRenameAll with nil should succeed, got: %v", err)
	}

	err = ms.atomicRenameAll([]*TmpFileInfo{})
	if err != nil {
		t.Fatalf("atomicRenameAll with empty slice should succeed, got: %v", err)
	}
}

func TestAtomicRenameAll_FailsOnMissingTmp(t *testing.T) {
	ms, tempDir := newTestStorage(t)
	defer func() { _ = os.RemoveAll(tempDir) }()

	tmpFiles := []*TmpFileInfo{
		{
			TmpPath:   filepath.Join(tempDir, "nonexistent.tmp"),
			FinalPath: filepath.Join(tempDir, "nonexistent.bin"),
		},
	}

	err := ms.atomicRenameAll(tmpFiles)
	if err == nil {
		t.Fatal("atomicRenameAll should fail when tmp file doesn't exist")
	}
}

func TestAtomicRenameAll_PartialFailureNoDoubleUnlock(t *testing.T) {
	ms, tempDir := newTestStorage(t)
	defer func() { _ = os.RemoveAll(tempDir) }()

	// First file exists, second does not → rename of second fails
	tmpPath1 := filepath.Join(tempDir, "ok.tmp")
	if err := os.WriteFile(tmpPath1, []byte("ok"), 0o644); err != nil {
		t.Fatalf("Failed to create tmp file: %v", err)
	}

	tmpFiles := []*TmpFileInfo{
		{TmpPath: tmpPath1, FinalPath: filepath.Join(tempDir, "ok.bin")},
		{TmpPath: filepath.Join(tempDir, "missing.tmp"), FinalPath: filepath.Join(tempDir, "missing.bin")},
	}

	err := ms.atomicRenameAll(tmpFiles)
	if err == nil {
		t.Fatal("Expected error for partial failure")
	}

	// Key: this should NOT panic with "Unlock of unlocked RWMutex"
	// The fact we got here without panic proves the fix works.

	// First file was renamed (best-effort, no rollback)
	if _, err := os.Stat(filepath.Join(tempDir, "ok.bin")); os.IsNotExist(err) {
		t.Error("First file should have been renamed before failure")
	}
}

// ============================================================================
// Tests for cleanupV6StaleParts
// ============================================================================

func TestCleanupStaleParts_RemovesOldFiles(t *testing.T) {
	ms, tempDir := newTestStorage(t)
	defer func() { _ = os.RemoveAll(tempDir) }()

	// Setup: write data with multiple batches to create multiple parts
	devices := []string{"dev1", "dev2", "dev3"}
	baseTime := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)

	// First write
	points1 := generateTestPoints("testdb", "metrics", devices, baseTime, 50)
	if err := ms.WriteBatch(points1); err != nil {
		t.Fatalf("WriteBatch 1 failed: %v", err)
	}

	// More writes to accumulate parts
	for i := 1; i <= 5; i++ {
		pts := generateTestPoints("testdb", "metrics", devices, baseTime.Add(time.Duration(i)*time.Hour), 50)
		if err := ms.WriteBatch(pts); err != nil {
			t.Fatalf("WriteBatch %d failed: %v", i+1, err)
		}
	}

	// Find the dateDir and DG dir
	dateDir := filepath.Join(tempDir, "testdb", "metrics", "2024", "06", "20240615")
	dgDir := filepath.Join(dateDir, "dg_0000")

	// Verify parts were accumulated
	metadataPath := filepath.Join(dateDir, "_metadata.idx")
	metadata, err := ms.readGlobalMetadata(metadataPath)
	if err != nil {
		t.Fatalf("Failed to read metadata: %v", err)
	}

	if len(metadata.DeviceGroups) == 0 {
		t.Fatal("No device groups found")
	}

	dg := metadata.DeviceGroups[0]
	if dg.PartCount <= 1 {
		t.Skipf("Only %d parts accumulated, need more for this test", dg.PartCount)
	}

	oldPartCount := dg.PartCount
	t.Logf("Parts before compaction: %d", oldPartCount)

	// Now compact — this should rewrite and then cleanupV6StaleParts
	n, err := ms.compactDateDir(context.Background(), dateDir, 1) // threshold=1 forces compaction
	if err != nil {
		t.Fatalf("compactDateDir failed: %v", err)
	}
	if n == 0 {
		t.Fatal("Expected at least 1 DG to be compacted")
	}

	// After compaction, check the DG directory for stale files (V6: parts are directly in DG dir)
	entries, err := os.ReadDir(dgDir)
	if err != nil {
		t.Fatalf("Failed to read DG dir: %v", err)
	}

	for _, entry := range entries {
		name := entry.Name()
		if name == "_metadata.idx" {
			continue
		}
		ext := filepath.Ext(name)
		if ext == ".tmp" {
			t.Errorf("Stale .tmp file not cleaned up: %s", name)
		}
	}

	// Count remaining .bin files — should be fewer or equal to old count
	var binCount int
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".bin" {
			binCount++
		}
	}
	t.Logf("DG: %d part files after compaction (was %d)", binCount, oldPartCount)
	if uint32(binCount) > oldPartCount {
		t.Errorf("DG has more part files (%d) than before compaction (%d)", binCount, oldPartCount)
	}
}

func TestCleanupStaleParts_DirectCall(t *testing.T) {
	ms, tempDir := newTestStorage(t)
	defer func() { _ = os.RemoveAll(tempDir) }()

	// Write valid data first
	devices := []string{"dev1"}
	baseTime := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)
	points := generateTestPoints("testdb", "metrics", devices, baseTime, 10)
	if err := ms.WriteBatch(points); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	// Find the real DG dir from actual write
	realDateDir := filepath.Join(tempDir, "testdb", "metrics", "2024", "06", "20240615")
	realDGDir := filepath.Join(realDateDir, "dg_0000")

	// Create stale files in the DG directory (V6: parts are directly in DG dir)
	staleFiles := []string{"part_0005.bin", "part_0006.bin", "part_0010.bin", "old_file.bin.tmp", "leftover.tmp"}
	for _, f := range staleFiles {
		path := filepath.Join(realDGDir, f)
		if err := os.WriteFile(path, []byte("stale-data"), 0o644); err != nil {
			t.Fatalf("Failed to create stale file %s: %v", f, err)
		}
	}

	// Read metadata to build manifest
	metadataPath := filepath.Join(realDateDir, "_metadata.idx")
	metadata, err := ms.readGlobalMetadata(metadataPath)
	if err != nil {
		t.Fatalf("Failed to read metadata: %v", err)
	}
	if len(metadata.DeviceGroups) == 0 {
		t.Fatal("No device groups")
	}

	manifest := metadata.DeviceGroups[0]

	// Call cleanupV6StaleParts directly
	ms.cleanupV6StaleParts(realDGDir, manifest)

	// Verify stale files are gone
	for _, f := range staleFiles {
		path := filepath.Join(realDGDir, f)
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("Stale file should have been removed: %s", f)
		}
	}

	// Verify valid files are still there
	entries, err := os.ReadDir(realDGDir)
	if err != nil {
		t.Fatal(err)
	}

	hasMetadata := false
	hasPart0 := false
	for _, entry := range entries {
		if entry.Name() == "_metadata.idx" {
			hasMetadata = true
		}
		if entry.Name() == "part_0000.bin" {
			hasPart0 = true
		}
	}
	if !hasMetadata {
		t.Error("_metadata.idx should not be removed")
	}
	if !hasPart0 {
		t.Error("part_0000.bin (valid) should not be removed")
	}
}

// ============================================================================
// Tests for concurrent write serialization (per-dateDir lock)
// ============================================================================

func TestConcurrentWritesSameDate_NoRace(t *testing.T) {
	ms, tempDir := newTestStorage(t)
	defer func() { _ = os.RemoveAll(tempDir) }()

	baseTime := time.Date(2024, 7, 20, 10, 0, 0, 0, time.UTC)

	var wg sync.WaitGroup
	errCh := make(chan error, 10)

	// Launch 10 concurrent writes to the SAME date directory
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			deviceID := fmt.Sprintf("device_%03d", idx)
			pts := generateTestPoints("testdb", "metrics", []string{deviceID}, baseTime.Add(time.Duration(idx)*time.Minute), 20)
			if err := ms.WriteBatch(pts); err != nil {
				errCh <- fmt.Errorf("WriteBatch %d failed: %w", idx, err)
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}

	// Verify all data can be read back
	result, err := ms.Query("testdb", "metrics", nil, baseTime.Add(-time.Hour), baseTime.Add(24*time.Hour), nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Should have data from all 10 devices (20 points each = 200 total)
	if len(result) != 200 {
		t.Errorf("Expected 200 data points, got %d", len(result))
	}

	// Verify all 10 devices are present
	deviceSet := make(map[string]bool)
	for _, p := range result {
		deviceSet[p.ID] = true
	}
	if len(deviceSet) != 10 {
		t.Errorf("Expected 10 devices, got %d", len(deviceSet))
	}
}

func TestConcurrentWritesDifferentDates_NoConflict(t *testing.T) {
	ms, tempDir := newTestStorage(t)
	defer func() { _ = os.RemoveAll(tempDir) }()

	var wg sync.WaitGroup
	errCh := make(chan error, 5)

	// Launch 5 concurrent writes to DIFFERENT date directories
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			baseTime := time.Date(2024, 7, 20+idx, 10, 0, 0, 0, time.UTC)
			pts := generateTestPoints("testdb", "metrics", []string{"device1"}, baseTime, 10)
			if err := ms.WriteBatch(pts); err != nil {
				errCh <- fmt.Errorf("WriteBatch day %d failed: %w", idx, err)
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}

	// Verify data from each day
	for i := 0; i < 5; i++ {
		baseTime := time.Date(2024, 7, 20+i, 0, 0, 0, 0, time.UTC)
		result, err := ms.Query("testdb", "metrics", nil, baseTime, baseTime.Add(24*time.Hour), nil)
		if err != nil {
			t.Errorf("Query for day %d failed: %v", i, err)
			continue
		}
		if len(result) != 10 {
			t.Errorf("Day %d: expected 10 results, got %d", i, len(result))
		}
	}
}

// ============================================================================
// Tests for compaction end-to-end
// ============================================================================

func TestCompactAll_NoData(t *testing.T) {
	ms, tempDir := newTestStorage(t)
	defer func() { _ = os.RemoveAll(tempDir) }()

	n, err := ms.CompactAll(context.Background(), 4)
	if err != nil {
		t.Fatalf("CompactAll on empty storage failed: %v", err)
	}
	if n != 0 {
		t.Errorf("CompactAll should return 0 on empty storage, got %d", n)
	}
}

func TestCompactAll_BelowThreshold(t *testing.T) {
	ms, tempDir := newTestStorage(t)
	defer func() { _ = os.RemoveAll(tempDir) }()

	baseTime := time.Date(2024, 8, 1, 10, 0, 0, 0, time.UTC)

	// Write only 2 batches → 2 parts → below default threshold of 4
	for i := 0; i < 2; i++ {
		pts := generateTestPoints("testdb", "metrics", []string{"dev1"}, baseTime.Add(time.Duration(i)*time.Hour), 20)
		if err := ms.WriteBatch(pts); err != nil {
			t.Fatalf("WriteBatch failed: %v", err)
		}
	}

	n, err := ms.CompactAll(context.Background(), 4)
	if err != nil {
		t.Fatalf("CompactAll failed: %v", err)
	}
	if n != 0 {
		t.Errorf("Should not compact below threshold, got %d compacted", n)
	}
}

func TestCompactAll_AboveThreshold(t *testing.T) {
	ms, tempDir := newTestStorage(t)
	defer func() { _ = os.RemoveAll(tempDir) }()

	baseTime := time.Date(2024, 8, 1, 10, 0, 0, 0, time.UTC)
	devices := []string{"dev1", "dev2"}

	// Write 6 batches → 6 parts per CG → above threshold of 4
	for i := 0; i < 6; i++ {
		pts := generateTestPoints("testdb", "metrics", devices, baseTime.Add(time.Duration(i)*time.Hour), 30)
		if err := ms.WriteBatch(pts); err != nil {
			t.Fatalf("WriteBatch %d failed: %v", i, err)
		}
	}

	// Read data before compaction
	resultBefore, err := ms.Query("testdb", "metrics", nil, baseTime.Add(-time.Hour), baseTime.Add(24*time.Hour), nil)
	if err != nil {
		t.Fatalf("Query before compaction failed: %v", err)
	}
	countBefore := len(resultBefore)
	t.Logf("Data points before compaction: %d", countBefore)

	// Verify metadata shows >4 parts
	dateDir := filepath.Join(tempDir, "testdb", "metrics", "2024", "08", "20240801")
	metaBefore, err := ms.readGlobalMetadata(filepath.Join(dateDir, "_metadata.idx"))
	if err != nil {
		t.Fatalf("Failed to read metadata before: %v", err)
	}
	if len(metaBefore.DeviceGroups) == 0 {
		t.Fatal("No device groups")
	}
	oldPartCount := metaBefore.DeviceGroups[0].PartCount
	t.Logf("Part count before: %d", oldPartCount)
	if oldPartCount <= 4 {
		t.Skipf("Only %d parts, need >4", oldPartCount)
	}

	// Compact with threshold = 4
	n, err := ms.CompactAll(context.Background(), 4)
	if err != nil {
		t.Fatalf("CompactAll failed: %v", err)
	}
	if n == 0 {
		t.Error("Expected at least 1 DG to be compacted")
	}

	// Read data after compaction — should be the same count (deduplication preserves unique points)
	resultAfter, err := ms.Query("testdb", "metrics", nil, baseTime.Add(-time.Hour), baseTime.Add(24*time.Hour), nil)
	if err != nil {
		t.Fatalf("Query after compaction failed: %v", err)
	}
	t.Logf("Data points after compaction: %d", len(resultAfter))

	// After dedup, count should be <= before (never more)
	if len(resultAfter) > countBefore {
		t.Errorf("Data points increased after compaction: %d → %d", countBefore, len(resultAfter))
	}

	// Verify part count decreased
	metaAfter, err := ms.readGlobalMetadata(filepath.Join(dateDir, "_metadata.idx"))
	if err != nil {
		t.Fatalf("Failed to read metadata after: %v", err)
	}
	newPartCount := metaAfter.DeviceGroups[0].PartCount
	t.Logf("Part count after: %d", newPartCount)
	if newPartCount >= oldPartCount {
		t.Errorf("Part count should decrease after compaction: %d → %d", oldPartCount, newPartCount)
	}

	// Verify no stale files remain on disk (V6: parts are directly in DG dir)
	dgDir := filepath.Join(dateDir, "dg_0000")
	entries, err := os.ReadDir(dgDir)
	if err != nil {
		t.Fatalf("Failed to read DG dir: %v", err)
	}
	var binCount int
	for _, entry := range entries {
		name := entry.Name()
		if filepath.Ext(name) == ".tmp" {
			t.Errorf("Stale .tmp file found after compaction: %s", name)
		}
		if filepath.Ext(name) == ".bin" {
			binCount++
		}
	}
	if uint32(binCount) != newPartCount {
		t.Errorf("DG: disk has %d .bin files but metadata says %d parts", binCount, newPartCount)
	}
}

func TestCompactAll_DataIntegrity(t *testing.T) {
	ms, tempDir := newTestStorage(t)
	defer func() { _ = os.RemoveAll(tempDir) }()

	baseTime := time.Date(2024, 9, 10, 10, 0, 0, 0, time.UTC)
	devices := []string{"sensor_a", "sensor_b"}

	// Write 8 batches with distinct timestamps to ensure no duplication
	totalExpected := 0
	for i := 0; i < 8; i++ {
		batchTime := baseTime.Add(time.Duration(i) * time.Hour)
		pts := generateTestPoints("testdb", "data", devices, batchTime, 10)
		if err := ms.WriteBatch(pts); err != nil {
			t.Fatalf("WriteBatch %d failed: %v", i, err)
		}
		totalExpected += len(pts)
	}

	t.Logf("Total points written: %d", totalExpected)

	// Query before compaction
	resultBefore, err := ms.Query("testdb", "data", nil, baseTime.Add(-time.Hour), baseTime.Add(24*time.Hour), nil)
	if err != nil {
		t.Fatalf("Query before compaction failed: %v", err)
	}

	// Compact with threshold=1 (force compaction of everything)
	n, err := ms.CompactAll(context.Background(), 1)
	if err != nil {
		t.Fatalf("CompactAll failed: %v", err)
	}
	t.Logf("Compacted %d DGs", n)

	// Query after compaction
	resultAfter, err := ms.Query("testdb", "data", nil, baseTime.Add(-time.Hour), baseTime.Add(24*time.Hour), nil)
	if err != nil {
		t.Fatalf("Query after compaction failed: %v", err)
	}

	// Data should be preserved (same count since timestamps are unique)
	if len(resultAfter) != len(resultBefore) {
		t.Errorf("Data count changed after compaction: %d → %d", len(resultBefore), len(resultAfter))
	}

	// Verify both devices still present
	deviceCountAfter := make(map[string]int)
	for _, p := range resultAfter {
		deviceCountAfter[p.ID]++
	}
	for _, dev := range devices {
		if deviceCountAfter[dev] == 0 {
			t.Errorf("Device %s missing after compaction", dev)
		}
	}
}

// ============================================================================
// Tests for CompactionWorker
// ============================================================================

func TestCompactionWorker_StartStop(t *testing.T) {
	logger := logging.NewDevelopment()
	config := CompactionWorkerConfig{
		Interval:  100 * time.Millisecond,
		Threshold: 4,
	}

	cw := NewCompactionWorker(config, logger)
	if cw == nil {
		t.Fatal("NewCompactionWorker returned nil")
	}

	cw.Start()
	// Double start should not panic
	cw.Start()

	time.Sleep(50 * time.Millisecond)

	cw.Stop()
	// Double stop should not panic
	cw.Stop()
}

func TestCompactionWorker_AddStorageAndRun(t *testing.T) {
	ms, tempDir := newTestStorage(t)
	defer func() { _ = os.RemoveAll(tempDir) }()

	baseTime := time.Date(2024, 10, 1, 10, 0, 0, 0, time.UTC)

	// Write enough data to trigger compaction
	for i := 0; i < 6; i++ {
		pts := generateTestPoints("testdb", "m", []string{"dev1"}, baseTime.Add(time.Duration(i)*time.Hour), 10)
		if err := ms.WriteBatch(pts); err != nil {
			t.Fatalf("WriteBatch failed: %v", err)
		}
	}

	logger := logging.NewDevelopment()
	config := CompactionWorkerConfig{
		Interval:  50 * time.Millisecond,
		Threshold: 2,
	}
	cw := NewCompactionWorker(config, logger)
	cw.AddStorage(ms)
	cw.Start()

	// Let the worker run a few cycles
	time.Sleep(300 * time.Millisecond)

	cw.Stop()

	// Verify compaction happened: part count should have decreased
	dateDir := filepath.Join(tempDir, "testdb", "m", "2024", "10", "20241001")
	meta, err := ms.readGlobalMetadata(filepath.Join(dateDir, "_metadata.idx"))
	if err != nil {
		t.Fatalf("Failed to read metadata: %v", err)
	}
	if len(meta.DeviceGroups) > 0 {
		partCount := meta.DeviceGroups[0].PartCount
		t.Logf("Part count after worker run: %d", partCount)
		if partCount > 2 {
			t.Errorf("Expected compaction to reduce parts to ≤2, got %d", partCount)
		}
	}
}

func TestCompactionWorker_ContextCancellation(t *testing.T) {
	logger := logging.NewDevelopment()
	config := CompactionWorkerConfig{
		Interval:  10 * time.Millisecond,
		Threshold: 4,
	}

	cw := NewCompactionWorker(config, logger)
	cw.Start()

	// Stop should cancel context and complete quickly
	done := make(chan struct{})
	go func() {
		cw.Stop()
		close(done)
	}()

	select {
	case <-done:
		// OK
	case <-time.After(5 * time.Second):
		t.Fatal("CompactionWorker.Stop() did not return within 5 seconds")
	}
}

func TestDefaultCompactionWorkerConfig(t *testing.T) {
	config := DefaultCompactionWorkerConfig()
	if config.Threshold != DefaultCompactionThreshold {
		t.Errorf("Threshold = %d, expected %d", config.Threshold, DefaultCompactionThreshold)
	}
	expectedInterval := time.Duration(DefaultCompactionInterval) * time.Second
	if config.Interval != expectedInterval {
		t.Errorf("Interval = %v, expected %v", config.Interval, expectedInterval)
	}
}

// ============================================================================
// Tests for NeedsCompaction
// ============================================================================

func TestNeedsCompaction(t *testing.T) {
	ms, tempDir := newTestStorage(t)
	defer func() { _ = os.RemoveAll(tempDir) }()

	baseTime := time.Date(2024, 11, 1, 10, 0, 0, 0, time.UTC)

	// Write 1 batch — should NOT need compaction at threshold=4
	pts := generateTestPoints("testdb", "m", []string{"dev1"}, baseTime, 20)
	if err := ms.WriteBatch(pts); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	dateDir := filepath.Join(tempDir, "testdb", "m", "2024", "11", "20241101")
	if ms.NeedsCompaction(dateDir, 4) {
		t.Error("Should not need compaction with only 1 part (threshold=4)")
	}

	// Write more batches to go above threshold
	for i := 1; i <= 5; i++ {
		pts := generateTestPoints("testdb", "m", []string{"dev1"}, baseTime.Add(time.Duration(i)*time.Hour), 20)
		if err := ms.WriteBatch(pts); err != nil {
			t.Fatalf("WriteBatch %d failed: %v", i, err)
		}
	}

	if !ms.NeedsCompaction(dateDir, 4) {
		t.Error("Should need compaction with 6 parts (threshold=4)")
	}

	// Nonexistent directory should return false
	if ms.NeedsCompaction("/nonexistent/dir", 4) {
		t.Error("NeedsCompaction should return false for nonexistent dir")
	}
}

// ============================================================================
// Tests for compaction + concurrent writes (no race/deadlock)
// ============================================================================

func TestCompactionDuringConcurrentWrites(t *testing.T) {
	ms, tempDir := newTestStorage(t)
	defer func() { _ = os.RemoveAll(tempDir) }()

	baseTime := time.Date(2024, 12, 1, 10, 0, 0, 0, time.UTC)

	// Seed with initial data
	for i := 0; i < 3; i++ {
		pts := generateTestPoints("testdb", "m", []string{"dev1"}, baseTime.Add(time.Duration(i)*time.Hour), 20)
		if err := ms.WriteBatch(pts); err != nil {
			t.Fatalf("WriteBatch failed: %v", err)
		}
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 20)

	// Concurrent writers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			pts := generateTestPoints("testdb", "m", []string{fmt.Sprintf("dev_%d", idx)},
				baseTime.Add(time.Duration(10+idx)*time.Hour), 20)
			if err := ms.WriteBatch(pts); err != nil {
				errCh <- fmt.Errorf("concurrent write %d: %w", idx, err)
			}
		}(i)
	}

	// Concurrent compaction
	wg.Add(1)
	go func() {
		defer wg.Done()
		dateDir := filepath.Join(tempDir, "testdb", "m", "2024", "12", "20241201")
		if _, err := ms.compactDateDir(context.Background(), dateDir, 1); err != nil {
			errCh <- fmt.Errorf("compaction: %w", err)
		}
	}()

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}

	// Verify data is still queryable after concurrent writes + compaction
	result, err := ms.Query("testdb", "m", nil, baseTime.Add(-time.Hour), baseTime.Add(48*time.Hour), nil)
	if err != nil {
		t.Fatalf("Query after concurrent ops failed: %v", err)
	}
	if len(result) == 0 {
		t.Error("Expected some data after concurrent writes + compaction")
	}
	t.Logf("Total data points after concurrent writes + compaction: %d", len(result))
}
