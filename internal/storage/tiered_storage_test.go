package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/compression"
	"github.com/soltixdb/soltix/internal/logging"
)

func newTestTieredStorage(t *testing.T) (*TieredStorage, string) {
	t.Helper()
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()

	config := GroupStorageConfig{
		DataDir:            tmpDir,
		Compression:        compression.Snappy,
		MaxRowsPerPart:     100,
		MaxPartSize:        1 << 20,
		MinRowsPerPart:     1,
		MaxDevicesPerGroup: 100,
		Timezone:           time.UTC,
	}

	ts := NewTieredStorage(config, logger)
	return ts, tmpDir
}

// ============================================================================
// Path Builder Tests
// ============================================================================

func TestTieredStorage_GroupDir(t *testing.T) {
	ts, tmpDir := newTestTieredStorage(t)

	tests := []struct {
		groupID  int
		expected string
	}{
		{0, filepath.Join(tmpDir, "group_0000")},
		{1, filepath.Join(tmpDir, "group_0001")},
		{42, filepath.Join(tmpDir, "group_0042")},
		{255, filepath.Join(tmpDir, "group_0255")},
		{1000, filepath.Join(tmpDir, "group_1000")},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("group_%d", tc.groupID), func(t *testing.T) {
			got := ts.GroupDir(tc.groupID)
			if got != tc.expected {
				t.Errorf("GroupDir(%d) = %s, want %s", tc.groupID, got, tc.expected)
			}
		})
	}
}

func TestTieredStorage_GroupDateDir(t *testing.T) {
	ts, tmpDir := newTestTieredStorage(t)

	testTime := time.Date(2025, 7, 15, 10, 30, 0, 0, time.UTC)
	got := ts.GroupDateDir(42, "testdb", "sensors", testTime)

	expected := filepath.Join(tmpDir, "group_0042", "testdb", "sensors", "2025", "07", "20250715")
	if got != expected {
		t.Errorf("GroupDateDir() = %s, want %s", got, expected)
	}
}

func TestTieredStorage_GroupDateDir_WithTimezone(t *testing.T) {
	ts, _ := newTestTieredStorage(t)

	// Set Asia/Tokyo timezone (UTC+9)
	tokyo, _ := time.LoadLocation("Asia/Tokyo")
	ts.SetTimezone(tokyo)

	// 2025-07-15 23:00 UTC = 2025-07-16 08:00 JST
	testTime := time.Date(2025, 7, 15, 23, 0, 0, 0, time.UTC)
	got := ts.GroupDateDir(1, "testdb", "sensors", testTime)

	// In JST, this should be July 16
	if filepath.Base(got) != "20250716" {
		t.Errorf("Expected date 20250716 (JST), got %s", filepath.Base(got))
	}
}

// ============================================================================
// ParseGroupID Tests
// ============================================================================

func TestParseGroupID(t *testing.T) {
	tests := []struct {
		input    string
		expected int
		hasError bool
	}{
		{"group_0000", 0, false},
		{"group_0001", 1, false},
		{"group_0042", 42, false},
		{"group_0255", 255, false},
		{"group_1000", 1000, false},
		{"invalid", 0, true},
		{"grp_0001", 0, true},
		{"", 0, true},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got, err := ParseGroupID(tc.input)
			if tc.hasError {
				if err == nil {
					t.Errorf("ParseGroupID(%q) expected error, got nil", tc.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseGroupID(%q) unexpected error: %v", tc.input, err)
			}
			if got != tc.expected {
				t.Errorf("ParseGroupID(%q) = %d, want %d", tc.input, got, tc.expected)
			}
		})
	}
}

// ============================================================================
// Engine Management Tests
// ============================================================================

func TestTieredStorage_GetOrCreateEngine(t *testing.T) {
	ts, tmpDir := newTestTieredStorage(t)

	// First call creates engine
	engine1 := ts.GetEngine(42)
	if engine1 == nil {
		t.Fatal("Expected non-nil engine")
	}

	// Verify directory was created
	groupDir := filepath.Join(tmpDir, "group_0042")
	if _, err := os.Stat(groupDir); os.IsNotExist(err) {
		t.Errorf("Group directory not created: %s", groupDir)
	}

	// Second call returns same engine
	engine2 := ts.GetEngine(42)
	if engine1 != engine2 {
		t.Error("Expected same engine instance on second call")
	}

	// Different group creates different engine
	engine3 := ts.GetEngine(99)
	if engine1 == engine3 {
		t.Error("Expected different engine for different group")
	}
}

// ============================================================================
// WriteBatch Tests
// ============================================================================

func TestTieredStorage_WriteBatch_EmptyEntries(t *testing.T) {
	ts, _ := newTestTieredStorage(t)

	err := ts.WriteBatch(nil)
	if err != nil {
		t.Fatalf("WriteBatch(nil) should not error: %v", err)
	}

	err = ts.WriteBatch([]*DataPoint{})
	if err != nil {
		t.Fatalf("WriteBatch([]) should not error: %v", err)
	}
}

func TestTieredStorage_WriteBatch_RoutesToCorrectGroup(t *testing.T) {
	ts, _ := newTestTieredStorage(t)

	now := time.Now()
	entries := []*DataPoint{
		{Database: "db1", Collection: "col1", ID: "dev1", GroupID: 1, Time: now, Fields: map[string]interface{}{"temp": 25.0}},
		{Database: "db1", Collection: "col1", ID: "dev2", GroupID: 1, Time: now, Fields: map[string]interface{}{"temp": 26.0}},
		{Database: "db1", Collection: "col1", ID: "dev3", GroupID: 2, Time: now, Fields: map[string]interface{}{"temp": 27.0}},
		{Database: "db1", Collection: "col1", ID: "dev4", GroupID: 3, Time: now, Fields: map[string]interface{}{"temp": 28.0}},
	}

	err := ts.WriteBatch(entries)
	if err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	// Query group 1 - should have 2 points
	points, err := ts.QueryGroup(1, "db1", "col1", []string{"dev1", "dev2"}, now.Add(-time.Minute), now.Add(time.Minute), nil)
	if err != nil {
		t.Fatalf("QueryGroup(1) failed: %v", err)
	}
	if len(points) != 2 {
		t.Errorf("Group 1 should have 2 points, got %d", len(points))
	}

	// Query group 2 - should have 1 point
	points, err = ts.QueryGroup(2, "db1", "col1", []string{"dev3"}, now.Add(-time.Minute), now.Add(time.Minute), nil)
	if err != nil {
		t.Fatalf("QueryGroup(2) failed: %v", err)
	}
	if len(points) != 1 {
		t.Errorf("Group 2 should have 1 point, got %d", len(points))
	}

	// Query group 3 - should have 1 point
	points, err = ts.QueryGroup(3, "db1", "col1", []string{"dev4"}, now.Add(-time.Minute), now.Add(time.Minute), nil)
	if err != nil {
		t.Fatalf("QueryGroup(3) failed: %v", err)
	}
	if len(points) != 1 {
		t.Errorf("Group 3 should have 1 point, got %d", len(points))
	}

	// Query group 2 for dev1 - should have 0 points (dev1 is in group 1)
	points, err = ts.QueryGroup(2, "db1", "col1", []string{"dev1"}, now.Add(-time.Minute), now.Add(time.Minute), nil)
	if err != nil {
		t.Fatalf("QueryGroup(2, dev1) failed: %v", err)
	}
	if len(points) != 0 {
		t.Errorf("Group 2 should have 0 points for dev1, got %d", len(points))
	}
}

func TestTieredStorage_WriteBatch_MultipleGroups(t *testing.T) {
	ts, _ := newTestTieredStorage(t)

	now := time.Now()

	// Write 10 entries across 5 groups
	var entries []*DataPoint
	for i := 0; i < 10; i++ {
		entries = append(entries, &DataPoint{
			Database:   "db1",
			Collection: "sensors",
			ID:         fmt.Sprintf("device_%d", i),
			GroupID:    i % 5,
			Time:       now,
			Fields:     map[string]interface{}{"value": float64(i)},
		})
	}

	err := ts.WriteBatch(entries)
	if err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	// Each group should have 2 entries
	for g := 0; g < 5; g++ {
		devIDs := []string{
			fmt.Sprintf("device_%d", g),
			fmt.Sprintf("device_%d", g+5),
		}
		points, err := ts.QueryGroup(g, "db1", "sensors", devIDs, now.Add(-time.Minute), now.Add(time.Minute), nil)
		if err != nil {
			t.Fatalf("QueryGroup(%d) failed: %v", g, err)
		}
		if len(points) != 2 {
			t.Errorf("Group %d should have 2 points, got %d", g, len(points))
		}
	}
}

// ============================================================================
// Query Tests
// ============================================================================

func TestTieredStorage_Query_AcrossGroups(t *testing.T) {
	ts, _ := newTestTieredStorage(t)

	now := time.Now()
	entries := []*DataPoint{
		{Database: "db1", Collection: "col1", ID: "dev1", GroupID: 10, Time: now, Fields: map[string]interface{}{"temp": 1.0}},
		{Database: "db1", Collection: "col1", ID: "dev2", GroupID: 20, Time: now, Fields: map[string]interface{}{"temp": 2.0}},
		{Database: "db1", Collection: "col1", ID: "dev3", GroupID: 30, Time: now, Fields: map[string]interface{}{"temp": 3.0}},
	}

	if err := ts.WriteBatch(entries); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	// Query all devices across all groups
	points, err := ts.Query("db1", "col1", []string{"dev1", "dev2", "dev3"}, now.Add(-time.Minute), now.Add(time.Minute), nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(points) != 3 {
		t.Errorf("Expected 3 points across groups, got %d", len(points))
	}
}

func TestTieredStorage_Query_EmptyDeviceIDs(t *testing.T) {
	ts, _ := newTestTieredStorage(t)

	now := time.Now()
	points, err := ts.Query("db1", "col1", nil, now.Add(-time.Minute), now.Add(time.Minute), nil)
	if err != nil {
		t.Fatalf("Unexpected error when querying without device_ids: %v", err)
	}
	if len(points) != 0 {
		t.Errorf("Expected 0 points for empty database, got %d", len(points))
	}
}

// ============================================================================
// ListGroupIDs Tests
// ============================================================================

func TestTieredStorage_ListGroupIDs_Empty(t *testing.T) {
	ts, _ := newTestTieredStorage(t)

	ids, err := ts.ListGroupIDs()
	if err != nil {
		t.Fatalf("ListGroupIDs failed: %v", err)
	}
	if len(ids) != 0 {
		t.Errorf("Expected 0 group IDs, got %d", len(ids))
	}
}

func TestTieredStorage_ListGroupIDs_WithData(t *testing.T) {
	ts, tmpDir := newTestTieredStorage(t)

	// Create some group directories
	for _, gid := range []int{5, 42, 100, 200} {
		dir := filepath.Join(tmpDir, fmt.Sprintf("group_%04d", gid))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("Failed to create dir: %v", err)
		}
	}

	// Also create a non-group dir (should be ignored)
	if err := os.MkdirAll(filepath.Join(tmpDir, "other_dir"), 0o755); err != nil {
		t.Fatalf("Failed to create dir: %v", err)
	}

	ids, err := ts.ListGroupIDs()
	if err != nil {
		t.Fatalf("ListGroupIDs failed: %v", err)
	}

	expected := []int{5, 42, 100, 200}
	if len(ids) != len(expected) {
		t.Fatalf("Expected %d group IDs, got %d: %v", len(expected), len(ids), ids)
	}
	for i, id := range ids {
		if id != expected[i] {
			t.Errorf("GroupIDs[%d] = %d, want %d", i, id, expected[i])
		}
	}
}

// ============================================================================
// GroupDataSize Tests
// ============================================================================

func TestTieredStorage_GroupDataSize_Empty(t *testing.T) {
	ts, _ := newTestTieredStorage(t)

	size, err := ts.GroupDataSize(42)
	if err != nil {
		t.Fatalf("GroupDataSize failed: %v", err)
	}
	if size != 0 {
		t.Errorf("Expected 0 bytes for non-existent group, got %d", size)
	}
}

func TestTieredStorage_GroupDataSize_WithData(t *testing.T) {
	ts, _ := newTestTieredStorage(t)

	now := time.Now()
	entries := make([]*DataPoint, 50)
	for i := 0; i < 50; i++ {
		entries[i] = &DataPoint{
			Database:   "db1",
			Collection: "sensors",
			ID:         fmt.Sprintf("dev_%d", i%5),
			GroupID:    42,
			Time:       now.Add(time.Duration(i) * time.Second),
			Fields:     map[string]interface{}{"temp": float64(i), "humidity": float64(i) * 0.5},
		}
	}

	if err := ts.WriteBatch(entries); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	size, err := ts.GroupDataSize(42)
	if err != nil {
		t.Fatalf("GroupDataSize failed: %v", err)
	}
	if size <= 0 {
		t.Errorf("Expected positive data size, got %d", size)
	}
}

// ============================================================================
// TieredStorageAdapter Tests
// ============================================================================

func TestTieredStorageAdapter_ImplementsMainStorage(t *testing.T) {
	ts, _ := newTestTieredStorage(t)
	adapter := NewTieredStorageAdapter(ts)

	// Verify interface compliance at compile time
	var _ MainStorage = adapter

	// Test WriteBatch through adapter
	now := time.Now()
	entries := []*DataPoint{
		{Database: "db1", Collection: "col1", ID: "dev1", GroupID: 5, Time: now, Fields: map[string]interface{}{"val": 1.0}},
	}
	if err := adapter.WriteBatch(entries); err != nil {
		t.Fatalf("Adapter WriteBatch failed: %v", err)
	}

	// Test Query through adapter
	points, err := adapter.Query("db1", "col1", []string{"dev1"}, now.Add(-time.Minute), now.Add(time.Minute), nil)
	if err != nil {
		t.Fatalf("Adapter Query failed: %v", err)
	}
	if len(points) != 1 {
		t.Errorf("Expected 1 point, got %d", len(points))
	}
}

func TestTieredStorageAdapter_SetTimezone(t *testing.T) {
	ts, _ := newTestTieredStorage(t)
	adapter := NewTieredStorageAdapter(ts)

	tokyo, _ := time.LoadLocation("Asia/Tokyo")
	adapter.SetTimezone(tokyo)

	if ts.config.Timezone != tokyo {
		t.Error("SetTimezone did not propagate to TieredStorage config")
	}
}

// ============================================================================
// Concurrent Access Tests
// ============================================================================

func TestTieredStorage_ConcurrentWrites(t *testing.T) {
	ts, _ := newTestTieredStorage(t)

	now := time.Now()
	const numGoroutines = 10
	const entriesPerGoroutine = 5

	errCh := make(chan error, numGoroutines)

	// Each goroutine writes to its own group to avoid file-level contention
	// (In production, WriteWorkerPool serializes writes per partition)
	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			entries := make([]*DataPoint, entriesPerGoroutine)
			for i := 0; i < entriesPerGoroutine; i++ {
				entries[i] = &DataPoint{
					Database:   "db1",
					Collection: "sensors",
					ID:         fmt.Sprintf("dev_%d_%d", goroutineID, i),
					GroupID:    goroutineID, // each goroutine gets its own group
					Time:       now.Add(time.Duration(i) * time.Second),
					Fields:     map[string]interface{}{"val": float64(goroutineID*100 + i)},
				}
			}
			errCh <- ts.WriteBatch(entries)
		}(g)
	}

	for i := 0; i < numGoroutines; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("Concurrent write failed: %v", err)
		}
	}

	// Verify all groups have data
	for g := 0; g < numGoroutines; g++ {
		engine := ts.GetEngine(g)
		if engine == nil {
			t.Errorf("Engine for group %d should exist", g)
		}
	}
}

// ============================================================================
// Field Selection Tests
// ============================================================================

func TestTieredStorage_QueryGroup_WithFieldSelection(t *testing.T) {
	ts, _ := newTestTieredStorage(t)

	now := time.Now()
	entries := []*DataPoint{
		{
			Database:   "db1",
			Collection: "sensors",
			ID:         "dev1",
			GroupID:    7,
			Time:       now,
			Fields: map[string]interface{}{
				"temperature": 25.5,
				"humidity":    60.0,
				"pressure":    1013.25,
			},
		},
	}

	if err := ts.WriteBatch(entries); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	// Query with field selection
	points, err := ts.QueryGroup(7, "db1", "sensors", []string{"dev1"}, now.Add(-time.Minute), now.Add(time.Minute), []string{"temperature"})
	if err != nil {
		t.Fatalf("QueryGroup failed: %v", err)
	}
	if len(points) != 1 {
		t.Fatalf("Expected 1 point, got %d", len(points))
	}

	// Should only have temperature field
	if _, ok := points[0].Fields["temperature"]; !ok {
		t.Error("Expected temperature field to be present")
	}
}

// ============================================================================
// groupByGroupID Tests
// ============================================================================

func TestTieredStorage_GroupByGroupID(t *testing.T) {
	ts, _ := newTestTieredStorage(t)

	entries := []*DataPoint{
		{ID: "a", GroupID: 1},
		{ID: "b", GroupID: 2},
		{ID: "c", GroupID: 1},
		{ID: "d", GroupID: 3},
		{ID: "e", GroupID: 2},
		{ID: "f", GroupID: 1},
	}

	grouped := ts.groupByGroupID(entries)

	if len(grouped) != 3 {
		t.Fatalf("Expected 3 groups, got %d", len(grouped))
	}
	if len(grouped[1]) != 3 {
		t.Errorf("Group 1 should have 3 entries, got %d", len(grouped[1]))
	}
	if len(grouped[2]) != 2 {
		t.Errorf("Group 2 should have 2 entries, got %d", len(grouped[2]))
	}
	if len(grouped[3]) != 1 {
		t.Errorf("Group 3 should have 1 entry, got %d", len(grouped[3]))
	}
}
