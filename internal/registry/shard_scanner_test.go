package registry

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/soltixdb/soltix/internal/logging"
)

// setupTestScanner creates a test shard scanner
func setupTestScanner(t *testing.T) (*ShardScanner, string) {
	dir := t.TempDir()
	logger := logging.NewDevelopment()
	scanner := NewShardScanner(dir, logger)
	return scanner, dir
}

// TestNewShardScanner tests creating a new shard scanner
func TestNewShardScanner(t *testing.T) {
	logger := logging.NewDevelopment()
	scanner := NewShardScanner("/tmp/test", logger)

	if scanner == nil {
		t.Fatal("Expected scanner to be created")
		return
	}
	if scanner.dataDir != "/tmp/test" {
		t.Errorf("Expected dataDir '/tmp/test', got '%s'", scanner.dataDir)
	}
}

// TestScanShards_EmptyDirectory tests scanning empty directory
func TestScanShards_EmptyDirectory(t *testing.T) {
	scanner, _ := setupTestScanner(t)

	shards, err := scanner.ScanShards()
	if err != nil {
		t.Fatalf("ScanShards failed: %v", err)
	}

	if len(shards) != 0 {
		t.Errorf("Expected 0 shards, got %d", len(shards))
	}
}

// TestScanShards_NonExistentDirectory tests scanning non-existent directory
func TestScanShards_NonExistentDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nonexistent")
	logger := logging.NewDevelopment()
	scanner := NewShardScanner(dir, logger)

	shards, err := scanner.ScanShards()
	if err != nil {
		t.Fatalf("ScanShards failed: %v", err)
	}

	// Should create directory and return empty shards
	if len(shards) != 0 {
		t.Errorf("Expected 0 shards, got %d", len(shards))
	}

	// Verify directory was created
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		t.Error("Expected directory to be created")
	}
}

// TestScanShards_SingleShard tests scanning single shard
func TestScanShards_SingleShard(t *testing.T) {
	scanner, dir := setupTestScanner(t)

	// Create shard structure: {dataDir}/testdb/metrics/shard-1/
	shardPath := filepath.Join(dir, "testdb", "metrics", "shard-1")
	if err := os.MkdirAll(shardPath, 0o755); err != nil {
		t.Fatalf("Failed to create shard directory: %v", err)
	}

	// Create some test files
	testFile := filepath.Join(shardPath, "data.bin")
	if err := os.WriteFile(testFile, []byte("test data"), 0o644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	shards, err := scanner.ScanShards()
	if err != nil {
		t.Fatalf("ScanShards failed: %v", err)
	}

	if len(shards) != 1 {
		t.Fatalf("Expected 1 shard, got %d", len(shards))
	}

	shard := shards[0]
	if shard.Database != "testdb" {
		t.Errorf("Expected database 'testdb', got '%s'", shard.Database)
	}
	if shard.Collection != "metrics" {
		t.Errorf("Expected collection 'metrics', got '%s'", shard.Collection)
	}
	if shard.ShardID != 1 {
		t.Errorf("Expected shard ID 1, got %d", shard.ShardID)
	}
	if shard.Role != "primary" {
		t.Errorf("Expected role 'primary', got '%s'", shard.Role)
	}
	if shard.DataSize <= 0 {
		t.Error("Expected data size > 0")
	}
}

// TestScanShards_MultipleShards tests scanning multiple shards
func TestScanShards_MultipleShards(t *testing.T) {
	scanner, dir := setupTestScanner(t)

	// Create multiple shards
	shards := []struct {
		database   string
		collection string
		shardID    int
	}{
		{"db1", "col1", 1},
		{"db1", "col1", 2},
		{"db1", "col2", 3},
		{"db2", "col1", 4},
	}

	for _, s := range shards {
		shardPath := filepath.Join(dir, s.database, s.collection, "shard-"+string(rune(s.shardID+'0')))
		if err := os.MkdirAll(shardPath, 0o755); err != nil {
			t.Fatalf("Failed to create shard directory: %v", err)
		}
		// Create test file
		testFile := filepath.Join(shardPath, "data.bin")
		if err := os.WriteFile(testFile, []byte("test"), 0o644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
	}

	scannedShards, err := scanner.ScanShards()
	if err != nil {
		t.Fatalf("ScanShards failed: %v", err)
	}

	if len(scannedShards) != 4 {
		t.Fatalf("Expected 4 shards, got %d", len(scannedShards))
	}

	// Verify each shard
	databaseCount := make(map[string]int)
	for _, shard := range scannedShards {
		databaseCount[shard.Database]++
		if shard.Role != "primary" {
			t.Errorf("Expected role 'primary', got '%s'", shard.Role)
		}
		if shard.DataSize <= 0 {
			t.Errorf("Expected data size > 0 for shard %d", shard.ShardID)
		}
	}

	if databaseCount["db1"] != 3 {
		t.Errorf("Expected 3 shards in db1, got %d", databaseCount["db1"])
	}
	if databaseCount["db2"] != 1 {
		t.Errorf("Expected 1 shard in db2, got %d", databaseCount["db2"])
	}
}

// TestScanGroups_EmptyDirectory tests scanning when no groups exist
func TestScanGroups_EmptyDirectory(t *testing.T) {
	scanner, _ := setupTestScanner(t)

	results, err := scanner.ScanGroups()
	if err != nil {
		t.Fatalf("ScanGroups failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("Expected 0 groups, got %d", len(results))
	}
}

// TestScanGroups_WithGroups tests scanning with group directories on disk
func TestScanGroups_WithGroups(t *testing.T) {
	scanner, dir := setupTestScanner(t)

	// Create data/data/ subdirectory with group dirs
	dataDir := filepath.Join(dir, "data")
	for _, gid := range []int{0, 42, 100, 255} {
		groupDir := filepath.Join(dataDir, fmt.Sprintf("group_%04d", gid))
		if err := os.MkdirAll(groupDir, 0o755); err != nil {
			t.Fatalf("Failed to create group dir: %v", err)
		}

		// Add a dummy file to simulate data
		dummyFile := filepath.Join(groupDir, "data.bin")
		if err := os.WriteFile(dummyFile, []byte("test data"), 0o644); err != nil {
			t.Fatalf("Failed to create dummy file: %v", err)
		}
	}

	// Also create a non-group dir (should be ignored)
	if err := os.MkdirAll(filepath.Join(dataDir, "other_dir"), 0o755); err != nil {
		t.Fatalf("Failed to create other dir: %v", err)
	}

	results, err := scanner.ScanGroups()
	if err != nil {
		t.Fatalf("ScanGroups failed: %v", err)
	}

	if len(results) != 4 {
		t.Fatalf("Expected 4 groups, got %d", len(results))
	}

	expectedIDs := map[int]bool{0: false, 42: false, 100: false, 255: false}
	for _, r := range results {
		if _, ok := expectedIDs[r.GroupID]; !ok {
			t.Errorf("Unexpected group ID: %d", r.GroupID)
		}
		expectedIDs[r.GroupID] = true

		if r.DataSize <= 0 {
			t.Errorf("Group %d should have positive data size, got %d", r.GroupID, r.DataSize)
		}
	}

	for id, found := range expectedIDs {
		if !found {
			t.Errorf("Group %d was not found", id)
		}
	}
}

// TestParseGroupDirName tests group directory name parsing
func TestParseGroupDirName(t *testing.T) {
	tests := []struct {
		name     string
		expected int
		hasError bool
	}{
		{"group_0000", 0, false},
		{"group_0042", 42, false},
		{"group_0255", 255, false},
		{"group_1000", 1000, false},
		{"invalid", 0, true},
		{"", 0, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseGroupDirName(tc.name)
			if tc.hasError {
				if err == nil {
					t.Errorf("Expected error for %q", tc.name)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error for %q: %v", tc.name, err)
			}
			if got != tc.expected {
				t.Errorf("parseGroupDirName(%q) = %d, want %d", tc.name, got, tc.expected)
			}
		})
	}
}
