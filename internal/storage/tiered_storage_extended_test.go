package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/compression"
	"github.com/soltixdb/soltix/internal/logging"
)

// =============================================================================
// DefaultGroupStorageConfig
// =============================================================================

func TestDefaultGroupStorageConfig(t *testing.T) {
	config := DefaultGroupStorageConfig("/data/test")

	if config.DataDir != "/data/test" {
		t.Errorf("DataDir = %s, want /data/test", config.DataDir)
	}
	if config.Compression != compression.Snappy {
		t.Errorf("Compression = %v, want Snappy", config.Compression)
	}
	if config.MaxRowsPerPart != DefaultMaxRowsPerPart {
		t.Errorf("MaxRowsPerPart = %d, want %d", config.MaxRowsPerPart, DefaultMaxRowsPerPart)
	}
	if config.MaxPartSize != DefaultMaxPartSize {
		t.Errorf("MaxPartSize = %d, want %d", config.MaxPartSize, DefaultMaxPartSize)
	}
	if config.MinRowsPerPart != DefaultMinRowsPerPart {
		t.Errorf("MinRowsPerPart = %d, want %d", config.MinRowsPerPart, DefaultMinRowsPerPart)
	}
	if config.MaxDevicesPerGroup != DefaultMaxDevicesPerGroup {
		t.Errorf("MaxDevicesPerGroup = %d, want %d", config.MaxDevicesPerGroup, DefaultMaxDevicesPerGroup)
	}
	if config.Timezone != nil {
		t.Errorf("Timezone should be nil by default")
	}
}

// =============================================================================
// GetAllEngines
// =============================================================================

func TestTieredStorage_GetAllEngines_Empty(t *testing.T) {
	ts, _ := newTestTieredStorage(t)

	engines := ts.GetAllEngines()
	if len(engines) != 0 {
		t.Errorf("GetAllEngines on empty tiered storage = %d, want 0", len(engines))
	}
}

func TestTieredStorage_GetAllEngines_WithEngines(t *testing.T) {
	ts, _ := newTestTieredStorage(t)

	// Create some engines
	ts.getOrCreateEngine(0)
	ts.getOrCreateEngine(1)
	ts.getOrCreateEngine(2)

	engines := ts.GetAllEngines()
	if len(engines) != 3 {
		t.Errorf("GetAllEngines = %d, want 3", len(engines))
	}
}

// =============================================================================
// GroupDateDir edge cases
// =============================================================================

func TestTieredStorage_GroupDateDir_NilTimezone(t *testing.T) {
	logger := logging.NewDevelopment()
	config := GroupStorageConfig{
		DataDir:            t.TempDir(),
		Compression:        compression.Snappy,
		MaxRowsPerPart:     100,
		MaxPartSize:        1 << 20,
		MinRowsPerPart:     1,
		MaxDevicesPerGroup: 100,
		Timezone:           nil, // nil timezone should default to UTC
	}
	ts := NewTieredStorage(config, logger)

	testTime := time.Date(2025, 3, 15, 10, 0, 0, 0, time.UTC)
	result := ts.GroupDateDir(0, "db", "col", testTime)

	if filepath.Base(result) != "20250315" {
		t.Errorf("Date dir = %s, want 20250315", filepath.Base(result))
	}
}

// =============================================================================
// GroupDataSize edge cases
// =============================================================================

func TestTieredStorage_GroupDataSize_NonExistentDir(t *testing.T) {
	ts, _ := newTestTieredStorage(t)

	size, err := ts.GroupDataSize(999)
	if err != nil {
		t.Fatalf("GroupDataSize for non-existent group should not error: %v", err)
	}
	if size != 0 {
		t.Errorf("GroupDataSize for non-existent = %d, want 0", size)
	}
}

func TestTieredStorage_GroupDataSize_WithFiles(t *testing.T) {
	ts, tmpDir := newTestTieredStorage(t)

	// Create group dir with some files
	groupDir := filepath.Join(tmpDir, "group_0005")
	if err := os.MkdirAll(filepath.Join(groupDir, "subdir"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(groupDir, "file1.bin"), make([]byte, 100), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(groupDir, "subdir", "file2.bin"), make([]byte, 200), 0o644); err != nil {
		t.Fatal(err)
	}

	size, err := ts.GroupDataSize(5)
	if err != nil {
		t.Fatalf("GroupDataSize failed: %v", err)
	}
	if size != 300 {
		t.Errorf("GroupDataSize = %d, want 300", size)
	}
}

// =============================================================================
// ListGroupIDs edge cases
// =============================================================================

func TestTieredStorage_ListGroupIDs_NonExistentDataDir(t *testing.T) {
	logger := logging.NewDevelopment()
	config := GroupStorageConfig{
		DataDir:            "/nonexistent/path/to/data",
		Compression:        compression.Snappy,
		MaxRowsPerPart:     100,
		MaxPartSize:        1 << 20,
		MinRowsPerPart:     1,
		MaxDevicesPerGroup: 100,
	}
	// Don't create data dir
	ts := &TieredStorage{
		config:       config,
		logger:       logger,
		groupEngines: make(map[int]*Storage),
	}

	ids, err := ts.ListGroupIDs()
	if err != nil {
		t.Fatalf("ListGroupIDs should not error for nonexistent dir: %v", err)
	}
	if len(ids) != 0 {
		t.Errorf("ListGroupIDs = %d, want 0", len(ids))
	}
}

func TestTieredStorage_ListGroupIDs_SkipsNonGroupDirs(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	config := GroupStorageConfig{
		DataDir:            tmpDir,
		Compression:        compression.Snappy,
		MaxRowsPerPart:     100,
		MaxPartSize:        1 << 20,
		MinRowsPerPart:     1,
		MaxDevicesPerGroup: 100,
	}
	ts := NewTieredStorage(config, logger)

	// Create mix of valid and invalid dirs
	if err := os.MkdirAll(filepath.Join(tmpDir, "group_0001"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(tmpDir, "group_0003"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(tmpDir, "not_a_group"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(tmpDir, "group_invalid"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "group_0099_file"), nil, 0o644); err != nil { // file, not dir
		t.Fatal(err)
	}

	ids, err := ts.ListGroupIDs()
	if err != nil {
		t.Fatalf("ListGroupIDs failed: %v", err)
	}
	if len(ids) != 2 {
		t.Errorf("ListGroupIDs = %v, want [1, 3]", ids)
	}
	if ids[0] != 1 || ids[1] != 3 {
		t.Errorf("ListGroupIDs = %v, want [1, 3]", ids)
	}
}

// =============================================================================
// SetTimezone propagation
// =============================================================================

func TestTieredStorage_SetTimezone_PropagatesToEngines(t *testing.T) {
	ts, _ := newTestTieredStorage(t)

	// Create engines
	ts.getOrCreateEngine(0)
	ts.getOrCreateEngine(1)

	// Set timezone
	tokyo, _ := time.LoadLocation("Asia/Tokyo")
	ts.SetTimezone(tokyo)

	// Verify config is updated
	if ts.config.Timezone != tokyo {
		t.Error("config.Timezone not updated")
	}

	// Verify a new engine created after SetTimezone gets the timezone
	engine := ts.getOrCreateEngine(2)
	if engine.timezone != tokyo {
		t.Error("New engine should inherit timezone")
	}
}

// =============================================================================
// Query with write/read round-trip
// =============================================================================

func TestTieredStorage_Query_MultipleGroups_WithDeviceFilter(t *testing.T) {
	ts, _ := newTestTieredStorage(t)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	// Write to group 0
	entries1 := []*DataPoint{
		{ID: "dev1", Database: "db", Collection: "c", GroupID: 0, Time: now, Fields: map[string]interface{}{"v": 1.0}},
		{ID: "dev2", Database: "db", Collection: "c", GroupID: 0, Time: now.Add(time.Minute), Fields: map[string]interface{}{"v": 2.0}},
	}

	// Write to group 1
	entries2 := []*DataPoint{
		{ID: "dev3", Database: "db", Collection: "c", GroupID: 1, Time: now, Fields: map[string]interface{}{"v": 3.0}},
	}

	if err := ts.WriteBatch(entries1); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}
	if err := ts.WriteBatch(entries2); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	// Query all
	result, err := ts.Query("db", "c", nil, now.Add(-time.Hour), now.Add(time.Hour), nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(result) != 3 {
		t.Errorf("Query all = %d, want 3", len(result))
	}

	// Query specific device
	result, err = ts.Query("db", "c", []string{"dev1"}, now.Add(-time.Hour), now.Add(time.Hour), nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(result) != 1 {
		t.Errorf("Query dev1 = %d, want 1", len(result))
	}
}

// =============================================================================
// QueryGroup
// =============================================================================

func TestTieredStorage_QueryGroup_EmptyGroup(t *testing.T) {
	ts, _ := newTestTieredStorage(t)

	// Query non-existent group
	result, err := ts.QueryGroup(99, "db", "col", nil, time.Now().Add(-time.Hour), time.Now(), nil)
	if err != nil {
		t.Fatalf("QueryGroup should not error for empty group: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("QueryGroup empty = %d, want 0", len(result))
	}
}

// =============================================================================
// TieredStorageAdapter — WriteBatch
// =============================================================================

func TestTieredStorageAdapter_WriteBatch(t *testing.T) {
	ts, _ := newTestTieredStorage(t)
	adapter := NewTieredStorageAdapter(ts)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)
	entries := []*DataPoint{
		{ID: "d1", Database: "db", Collection: "c", GroupID: 0, Time: now, Fields: map[string]interface{}{"v": 1.0}},
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

// =============================================================================
// ParseGroupID edge cases
// =============================================================================

func TestParseGroupID_InvalidFormat(t *testing.T) {
	tests := []string{
		"",
		"not_a_group",
		"group_",
		"GROUP_0001",
		"grp_0001",
	}

	for _, input := range tests {
		_, err := ParseGroupID(input)
		if err == nil {
			t.Errorf("ParseGroupID(%q) should error", input)
		}
	}
}
