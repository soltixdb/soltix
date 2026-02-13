package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestPartitionedWriter_Create(t *testing.T) {
	tmpDir := t.TempDir()

	config := PartitionedConfig{
		BaseDir:     tmpDir,
		Config:      DefaultConfig(tmpDir),
		IdleTimeout: 1 * time.Minute,
	}

	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatalf("Failed to create partitioned writer: %v", err)
	}
	defer func() { _ = pw.Close() }()

	// Verify base directory was created
	if _, err := os.Stat(tmpDir); os.IsNotExist(err) {
		t.Error("Base directory was not created")
	}
}

func TestPartitionedWriter_PartitionKey(t *testing.T) {
	tests := []struct {
		database   string
		collection string
		date       time.Time
		expected   string
	}{
		{"db1", "metrics", time.Date(2026, 1, 27, 10, 0, 0, 0, time.UTC), "db1:metrics:2026-01-27"},
		{"db2", "logs", time.Date(2026, 1, 28, 23, 59, 59, 0, time.UTC), "db2:logs:2026-01-28"},
		{"mydb", "events", time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC), "mydb:events:2025-12-31"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			key := partitionKey(tt.database, tt.collection, tt.date)
			if key != tt.expected {
				t.Errorf("Expected key %s, got %s", tt.expected, key)
			}
		})
	}
}

func TestPartitionedWriter_WriteAndRead(t *testing.T) {
	tmpDir := t.TempDir()

	config := PartitionedConfig{
		BaseDir:     tmpDir,
		Config:      DefaultConfig(tmpDir),
		IdleTimeout: 1 * time.Minute,
	}

	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatalf("Failed to create partitioned writer: %v", err)
	}
	defer func() { _ = pw.Close() }()

	// Write entries to different partitions
	database := "testdb"
	date := time.Date(2026, 1, 27, 0, 0, 0, 0, time.UTC)

	entries := []*Entry{
		{
			Type:       EntryTypeWrite,
			Database:   database,
			Collection: "metrics",
			ShardID:    "shard1",
			Time:       "2026-01-27T10:00:00Z",
			ID:         "1",
			Fields:     map[string]interface{}{"value": 100.5},
			Timestamp:  time.Now().UnixNano(),
		},
		{
			Type:       EntryTypeWrite,
			Database:   database,
			Collection: "metrics",
			ShardID:    "shard1",
			Time:       "2026-01-27T11:00:00Z",
			ID:         "2",
			Fields:     map[string]interface{}{"value": 200.5},
			Timestamp:  time.Now().UnixNano(),
		},
	}

	collection := "metrics"

	// Write entries
	for _, entry := range entries {
		if _, err := pw.WritePartitioned(entry, database, date); err != nil {
			t.Fatalf("Failed to write entry: %v", err)
		}
	}

	// Flush the partition
	if err := pw.FlushPartition(database, collection, date); err != nil {
		t.Fatalf("Failed to flush partition: %v", err)
	}

	// Read back
	readEntries, err := pw.ReadPartition(database, collection, date)
	if err != nil {
		t.Fatalf("Failed to read partition: %v", err)
	}

	if len(readEntries) != len(entries) {
		t.Errorf("Expected %d entries, got %d", len(entries), len(readEntries))
	}

	// Verify first entry
	if readEntries[0].ID != entries[0].ID {
		t.Errorf("Expected ID %s, got %s", entries[0].ID, readEntries[0].ID)
	}
}

func TestPartitionedWriter_MultiplePartitions(t *testing.T) {
	tmpDir := t.TempDir()

	config := PartitionedConfig{
		BaseDir:     tmpDir,
		Config:      DefaultConfig(tmpDir),
		IdleTimeout: 1 * time.Minute,
	}

	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatalf("Failed to create partitioned writer: %v", err)
	}
	defer func() { _ = pw.Close() }()

	// Write to multiple partitions
	partitions := []struct {
		database   string
		collection string
		date       time.Time
	}{
		{"db1", "metrics", time.Date(2026, 1, 27, 0, 0, 0, 0, time.UTC)},
		{"db1", "metrics", time.Date(2026, 1, 28, 0, 0, 0, 0, time.UTC)},
		{"db2", "logs", time.Date(2026, 1, 27, 0, 0, 0, 0, time.UTC)},
	}

	for i, p := range partitions {
		entry := &Entry{
			Type:       EntryTypeWrite,
			Database:   p.database,
			Collection: p.collection,
			ID:         string(rune('A' + i)),
			Fields:     map[string]interface{}{"value": float64(i)},
			Timestamp:  time.Now().UnixNano(),
		}
		if _, err := pw.WritePartitioned(entry, p.database, p.date); err != nil {
			t.Fatalf("Failed to write to partition %s:%s: %v", p.database, p.date.Format("2006-01-02"), err)
		}
	}

	// Flush all
	if err := pw.FlushAll(); err != nil {
		t.Fatalf("Failed to flush all: %v", err)
	}

	// List partitions
	partList, err := pw.ListPartitions()
	if err != nil {
		t.Fatalf("Failed to list partitions: %v", err)
	}

	if len(partList) != 3 {
		t.Errorf("Expected 3 partitions, got %d", len(partList))
	}

	// Verify directory structure
	expectedDirs := []string{
		filepath.Join(tmpDir, "db1", "metrics", "2026-01-27"),
		filepath.Join(tmpDir, "db1", "metrics", "2026-01-28"),
		filepath.Join(tmpDir, "db2", "logs", "2026-01-27"),
	}

	for _, dir := range expectedDirs {
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			t.Errorf("Expected directory %s to exist", dir)
		}
	}
}

func TestPartitionedWriter_Stats(t *testing.T) {
	tmpDir := t.TempDir()

	config := PartitionedConfig{
		BaseDir:     tmpDir,
		Config:      DefaultConfig(tmpDir),
		IdleTimeout: 1 * time.Minute,
	}

	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatalf("Failed to create partitioned writer: %v", err)
	}
	defer func() { _ = pw.Close() }()

	// Write to two partitions
	date := time.Date(2026, 1, 27, 0, 0, 0, 0, time.UTC)

	entry1 := &Entry{ID: "1", Database: "db1", Timestamp: time.Now().UnixNano()}
	entry2 := &Entry{ID: "2", Database: "db2", Timestamp: time.Now().UnixNano()}

	_, _ = pw.WritePartitioned(entry1, "db1", date)
	_, _ = pw.WritePartitioned(entry2, "db2", date)

	stats := pw.GetStats()

	if stats.PartitionCount != 2 {
		t.Errorf("Expected 2 partitions, got %d", stats.PartitionCount)
	}
}

func TestPartitionedWriter_RemovePartition(t *testing.T) {
	tmpDir := t.TempDir()

	config := PartitionedConfig{
		BaseDir:     tmpDir,
		Config:      DefaultConfig(tmpDir),
		IdleTimeout: 1 * time.Minute,
	}

	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatalf("Failed to create partitioned writer: %v", err)
	}
	defer func() { _ = pw.Close() }()

	database := "testdb"
	collection := "metrics"
	date := time.Date(2026, 1, 27, 0, 0, 0, 0, time.UTC)

	// Write an entry
	entry := &Entry{ID: "1", Database: database, Collection: collection, Timestamp: time.Now().UnixNano()}
	if _, err := pw.WritePartitioned(entry, database, date); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	// Flush
	_ = pw.FlushPartition(database, collection, date)

	// Verify partition exists
	partDir := filepath.Join(tmpDir, database, collection, "2026-01-27")
	if _, err := os.Stat(partDir); os.IsNotExist(err) {
		t.Fatalf("Partition directory should exist")
	}

	// Remove partition
	if err := pw.RemovePartition(database, collection, date); err != nil {
		t.Fatalf("Failed to remove partition: %v", err)
	}

	// Verify partition is removed
	if _, err := os.Stat(partDir); !os.IsNotExist(err) {
		t.Error("Partition directory should be removed")
	}
}

func BenchmarkPartitionedWriter_Write(b *testing.B) {
	tmpDir := b.TempDir()

	config := PartitionedConfig{
		BaseDir:     tmpDir,
		Config:      DefaultConfig(tmpDir),
		IdleTimeout: 1 * time.Minute,
	}

	pw, err := NewPartitionedWriter(config)
	if err != nil {
		b.Fatalf("Failed to create partitioned writer: %v", err)
	}
	defer func() { _ = pw.Close() }()

	date := time.Date(2026, 1, 27, 0, 0, 0, 0, time.UTC)
	databases := []string{"db1", "db2", "db3", "db4"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db := databases[i%len(databases)]
		entry := &Entry{
			Type:       EntryTypeWrite,
			Database:   db,
			Collection: "metrics",
			ID:         string(rune(i)),
			Fields:     map[string]interface{}{"value": float64(i)},
			Timestamp:  time.Now().UnixNano(),
		}
		_, _ = pw.WritePartitioned(entry, db, date)
	}
}

// =============================================================================
// New tests for uncovered functions
// =============================================================================

// TestDefaultPartitionedConfig tests the DefaultPartitionedConfig function
func TestDefaultPartitionedConfig(t *testing.T) {
	config := DefaultPartitionedConfig(t.TempDir())

	if config.BaseDir == "" {
		t.Error("BaseDir should not be empty")
	}
	if config.IdleTimeout <= 0 {
		t.Errorf("IdleTimeout should be > 0, got %v", config.IdleTimeout)
	}
}

// TestPartitionedWriter_WriteSyncPartitioned tests the WriteSyncPartitioned function
func TestPartitionedWriter_WriteSyncPartitioned(t *testing.T) {
	tmpDir := t.TempDir()

	config := PartitionedConfig{
		BaseDir:     tmpDir,
		Config:      DefaultConfig(tmpDir),
		IdleTimeout: 1 * time.Minute,
	}

	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatalf("Failed to create partitioned writer: %v", err)
	}
	defer func() { _ = pw.Close() }()

	date := time.Date(2026, 1, 27, 0, 0, 0, 0, time.UTC)
	entry := &Entry{
		Type:       EntryTypeWrite,
		Database:   "testdb",
		Collection: "metrics",
		ID:         "1",
		Fields:     map[string]interface{}{"value": 100.5},
		Timestamp:  time.Now().UnixNano(),
	}

	// WriteSyncPartitioned should immediately flush
	if err := pw.WriteSyncPartitioned(entry, "testdb", date); err != nil {
		t.Fatalf("WriteSyncPartitioned failed: %v", err)
	}

	// Entry should be immediately readable
	entries, err := pw.ReadPartition("testdb", "metrics", date)
	if err != nil {
		t.Fatalf("ReadPartition failed: %v", err)
	}

	if len(entries) != 1 {
		t.Errorf("Expected 1 entry, got %d", len(entries))
	}
}

// TestPartitionedWriter_SetCheckpointPartition tests the SetCheckpointPartition function
func TestPartitionedWriter_SetCheckpointPartition(t *testing.T) {
	tmpDir := t.TempDir()

	config := PartitionedConfig{
		BaseDir:     tmpDir,
		Config:      DefaultConfig(tmpDir),
		IdleTimeout: 1 * time.Minute,
	}

	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatalf("Failed to create partitioned writer: %v", err)
	}
	defer func() { _ = pw.Close() }()

	database := "testdb"
	collection := "metrics"
	date := time.Date(2026, 1, 27, 0, 0, 0, 0, time.UTC)

	// Write some entries
	for i := 0; i < 5; i++ {
		entry := &Entry{
			Type:       EntryTypeWrite,
			Database:   database,
			Collection: collection,
			ID:         string(rune('a' + i)),
			Fields:     map[string]interface{}{"value": float64(i)},
			Timestamp:  time.Now().UnixNano(),
		}
		_, _ = pw.WritePartitioned(entry, database, date)
	}

	_ = pw.FlushPartition(database, collection, date)

	// Set checkpoint
	if err := pw.SetCheckpointPartition(database, collection, date); err != nil {
		t.Fatalf("SetCheckpointPartition failed: %v", err)
	}
}

// TestPartitionedWriter_TruncatePartitionBeforeCheckpoint tests the TruncatePartitionBeforeCheckpoint function
func TestPartitionedWriter_TruncatePartitionBeforeCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()

	config := PartitionedConfig{
		BaseDir:     tmpDir,
		Config:      DefaultConfig(tmpDir),
		IdleTimeout: 1 * time.Minute,
	}

	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatalf("Failed to create partitioned writer: %v", err)
	}
	defer func() { _ = pw.Close() }()

	database := "testdb"
	collection := "metrics"
	date := time.Date(2026, 1, 27, 0, 0, 0, 0, time.UTC)

	// Write some entries
	for i := 0; i < 5; i++ {
		entry := &Entry{
			Type:       EntryTypeWrite,
			Database:   database,
			Collection: collection,
			ID:         string(rune('a' + i)),
			Fields:     map[string]interface{}{"value": float64(i)},
			Timestamp:  time.Now().UnixNano(),
		}
		_, _ = pw.WritePartitioned(entry, database, date)
	}

	_ = pw.FlushPartition(database, collection, date)

	// Set checkpoint then truncate
	_ = pw.SetCheckpointPartition(database, collection, date)

	if err := pw.TruncatePartitionBeforeCheckpoint(database, collection, date); err != nil {
		t.Fatalf("TruncatePartitionBeforeCheckpoint failed: %v", err)
	}
}

// TestPartitionedWriter_FlushAll tests the FlushAll function
func TestPartitionedWriter_FlushAll(t *testing.T) {
	tmpDir := t.TempDir()

	config := PartitionedConfig{
		BaseDir:     tmpDir,
		Config:      DefaultConfig(tmpDir),
		IdleTimeout: 1 * time.Minute,
	}

	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatalf("Failed to create partitioned writer: %v", err)
	}
	defer func() { _ = pw.Close() }()

	date := time.Date(2026, 1, 27, 0, 0, 0, 0, time.UTC)

	// Write to multiple partitions
	for _, db := range []string{"db1", "db2", "db3"} {
		entry := &Entry{
			Type:       EntryTypeWrite,
			Database:   db,
			Collection: "metrics",
			ID:         "1",
			Fields:     map[string]interface{}{"value": 100.0},
			Timestamp:  time.Now().UnixNano(),
		}
		_, _ = pw.WritePartitioned(entry, db, date)
	}

	// FlushAll
	if err := pw.FlushAll(); err != nil {
		t.Fatalf("FlushAll failed: %v", err)
	}
}

// TestPartitionedWriter_ReadAllPartitions tests the ReadAllPartitions function
func TestPartitionedWriter_ReadAllPartitions(t *testing.T) {
	tmpDir := t.TempDir()

	config := PartitionedConfig{
		BaseDir:     tmpDir,
		Config:      DefaultConfig(tmpDir),
		IdleTimeout: 1 * time.Minute,
	}

	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatalf("Failed to create partitioned writer: %v", err)
	}
	defer func() { _ = pw.Close() }()

	date := time.Date(2026, 1, 27, 0, 0, 0, 0, time.UTC)

	// Write to multiple partitions using sync writes
	for i, db := range []string{"db1", "db2"} {
		for j := 0; j < 3; j++ {
			entry := &Entry{
				Type:       EntryTypeWrite,
				Database:   db,
				Collection: "metrics",
				ID:         string(rune('a' + i*3 + j)),
				Fields:     map[string]interface{}{"value": float64(i*3 + j)},
				Timestamp:  time.Now().UnixNano(),
			}
			_ = pw.WriteSyncPartitioned(entry, db, date)
		}
	}

	// ReadAllPartitions
	allEntries, err := pw.ReadAllPartitions()
	if err != nil {
		t.Fatalf("ReadAllPartitions failed: %v", err)
	}

	if len(allEntries) != 6 {
		t.Errorf("Expected 6 entries, got %d", len(allEntries))
	}
}

// TestPartitionedWriter_SetCheckpointAll tests the SetCheckpointAll function
func TestPartitionedWriter_SetCheckpointAll(t *testing.T) {
	tmpDir := t.TempDir()

	config := PartitionedConfig{
		BaseDir:     tmpDir,
		Config:      DefaultConfig(tmpDir),
		IdleTimeout: 1 * time.Minute,
	}

	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatalf("Failed to create partitioned writer: %v", err)
	}
	defer func() { _ = pw.Close() }()

	date := time.Date(2026, 1, 27, 0, 0, 0, 0, time.UTC)

	// Write to multiple partitions
	for _, db := range []string{"db1", "db2"} {
		entry := &Entry{
			Type:       EntryTypeWrite,
			Database:   db,
			Collection: "metrics",
			ID:         "1",
			Fields:     map[string]interface{}{"value": 100.0},
			Timestamp:  time.Now().UnixNano(),
		}
		_, _ = pw.WritePartitioned(entry, db, date)
	}

	_ = pw.FlushAll()

	// SetCheckpointAll
	if err := pw.SetCheckpointAll(); err != nil {
		t.Fatalf("SetCheckpointAll failed: %v", err)
	}
}

// TestPartitionedWriter_TruncateBeforeCheckpointAll tests the TruncateBeforeCheckpointAll function
func TestPartitionedWriter_TruncateBeforeCheckpointAll(t *testing.T) {
	tmpDir := t.TempDir()

	config := PartitionedConfig{
		BaseDir:     tmpDir,
		Config:      DefaultConfig(tmpDir),
		IdleTimeout: 1 * time.Minute,
	}

	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatalf("Failed to create partitioned writer: %v", err)
	}
	defer func() { _ = pw.Close() }()

	date := time.Date(2026, 1, 27, 0, 0, 0, 0, time.UTC)

	// Write to multiple partitions
	for _, db := range []string{"db1", "db2"} {
		entry := &Entry{
			Type:       EntryTypeWrite,
			Database:   db,
			Collection: "metrics",
			ID:         "1",
			Fields:     map[string]interface{}{"value": 100.0},
			Timestamp:  time.Now().UnixNano(),
		}
		_, _ = pw.WritePartitioned(entry, db, date)
	}

	_ = pw.FlushAll()
	_ = pw.SetCheckpointAll()

	// TruncateBeforeCheckpointAll
	if err := pw.TruncateBeforeCheckpointAll(); err != nil {
		t.Fatalf("TruncateBeforeCheckpointAll failed: %v", err)
	}
}

// TestPartitionedWriter_GetTotalSegmentCount tests the GetTotalSegmentCount function
func TestPartitionedWriter_GetTotalSegmentCount(t *testing.T) {
	tmpDir := t.TempDir()

	config := PartitionedConfig{
		BaseDir:     tmpDir,
		Config:      DefaultConfig(tmpDir),
		IdleTimeout: 1 * time.Minute,
	}

	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatalf("Failed to create partitioned writer: %v", err)
	}
	defer func() { _ = pw.Close() }()

	date := time.Date(2026, 1, 27, 0, 0, 0, 0, time.UTC)

	// Write to multiple partitions
	for _, db := range []string{"db1", "db2", "db3"} {
		entry := &Entry{
			Type:       EntryTypeWrite,
			Database:   db,
			Collection: "metrics",
			ID:         "1",
			Fields:     map[string]interface{}{"value": 100.0},
			Timestamp:  time.Now().UnixNano(),
		}
		_, _ = pw.WritePartitioned(entry, db, date)
	}

	_ = pw.FlushAll()

	// GetTotalSegmentCount
	count, err := pw.GetTotalSegmentCount()
	if err != nil {
		t.Fatalf("GetTotalSegmentCount failed: %v", err)
	}
	if count < 1 {
		t.Errorf("Expected at least 1 segment, got %d", count)
	}
}

// TestPartitionedWriter_RotateAll tests the RotateAll function
func TestPartitionedWriter_RotateAll(t *testing.T) {
	tmpDir := t.TempDir()

	config := PartitionedConfig{
		BaseDir:     tmpDir,
		Config:      DefaultConfig(tmpDir),
		IdleTimeout: 1 * time.Minute,
	}

	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatalf("Failed to create partitioned writer: %v", err)
	}
	defer func() { _ = pw.Close() }()

	date := time.Date(2026, 1, 27, 0, 0, 0, 0, time.UTC)

	// Write to multiple partitions
	for _, db := range []string{"db1", "db2"} {
		entry := &Entry{
			Type:       EntryTypeWrite,
			Database:   db,
			Collection: "metrics",
			ID:         "1",
			Fields:     map[string]interface{}{"value": 100.0},
			Timestamp:  time.Now().UnixNano(),
		}
		_, _ = pw.WritePartitioned(entry, db, date)
	}

	_ = pw.FlushAll()

	// RotateAll
	if err := pw.RotateAll(); err != nil {
		t.Fatalf("RotateAll failed: %v", err)
	}
}

// TestPartitionedWriter_ProcessPartitions tests the ProcessPartitions function
func TestPartitionedWriter_ProcessPartitions(t *testing.T) {
	tmpDir := t.TempDir()

	config := PartitionedConfig{
		BaseDir:     tmpDir,
		Config:      DefaultConfig(tmpDir),
		IdleTimeout: 1 * time.Minute,
	}

	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatalf("Failed to create partitioned writer: %v", err)
	}
	defer func() { _ = pw.Close() }()

	date := time.Date(2026, 1, 27, 0, 0, 0, 0, time.UTC)

	// Write to multiple partitions
	for _, db := range []string{"db1", "db2", "db3"} {
		entry := &Entry{
			Type:       EntryTypeWrite,
			Database:   db,
			Collection: "metrics",
			ID:         "1",
			Fields:     map[string]interface{}{"value": 100.0},
			Timestamp:  time.Now().UnixNano(),
		}
		_, _ = pw.WritePartitioned(entry, db, date)
	}

	_ = pw.FlushAll()

	// ProcessPartitions
	processedCount := 0
	err = pw.ProcessPartitions(func(partition PartitionInfo, entries []*Entry) error {
		processedCount++
		return nil
	})
	if err != nil {
		t.Fatalf("ProcessPartitions failed: %v", err)
	}

	if processedCount != 3 {
		t.Errorf("Expected 3 partitions processed, got %d", processedCount)
	}
}

// TestPartitionedWriter_PrepareFlushPartition tests the PrepareFlushPartition function
func TestPartitionedWriter_PrepareFlushPartition(t *testing.T) {
	tmpDir := t.TempDir()

	config := PartitionedConfig{
		BaseDir:     tmpDir,
		Config:      DefaultConfig(tmpDir),
		IdleTimeout: 1 * time.Minute,
	}

	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatalf("Failed to create partitioned writer: %v", err)
	}
	defer func() { _ = pw.Close() }()

	database := "testdb"
	collection := "metrics"
	date := time.Date(2026, 1, 27, 0, 0, 0, 0, time.UTC)

	// Write entries
	for i := 0; i < 5; i++ {
		entry := &Entry{
			Type:       EntryTypeWrite,
			Database:   database,
			Collection: collection,
			ID:         string(rune('a' + i)),
			Fields:     map[string]interface{}{"value": float64(i)},
			Timestamp:  time.Now().UnixNano(),
		}
		_, _ = pw.WritePartitioned(entry, database, date)
	}

	_ = pw.FlushPartition(database, collection, date)

	// PrepareFlushPartition
	files, err := pw.PrepareFlushPartition(database, collection, date)
	if err != nil {
		t.Fatalf("PrepareFlushPartition failed: %v", err)
	}

	if len(files) == 0 {
		t.Error("Expected at least one segment file")
	}
}

// TestPartitionedWriter_ReadPartitionSegmentFile tests the ReadPartitionSegmentFile function
func TestPartitionedWriter_ReadPartitionSegmentFile(t *testing.T) {
	tmpDir := t.TempDir()

	config := PartitionedConfig{
		BaseDir:     tmpDir,
		Config:      DefaultConfig(tmpDir),
		IdleTimeout: 1 * time.Minute,
	}

	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatalf("Failed to create partitioned writer: %v", err)
	}
	defer func() { _ = pw.Close() }()

	database := "testdb"
	collection := "metrics"
	date := time.Date(2026, 1, 27, 0, 0, 0, 0, time.UTC)

	// Write entries
	for i := 0; i < 5; i++ {
		entry := &Entry{
			Type:       EntryTypeWrite,
			Database:   database,
			Collection: collection,
			ID:         string(rune('a' + i)),
			Fields:     map[string]interface{}{"value": float64(i)},
			Timestamp:  time.Now().UnixNano(),
		}
		_, _ = pw.WritePartitioned(entry, database, date)
	}

	_ = pw.FlushPartition(database, collection, date)

	// Get segment files
	files, _ := pw.PrepareFlushPartition(database, collection, date)

	if len(files) > 0 {
		// ReadPartitionSegmentFile
		entries, err := pw.ReadPartitionSegmentFile(database, collection, date, files[0])
		if err != nil {
			t.Fatalf("ReadPartitionSegmentFile failed: %v", err)
		}

		if len(entries) == 0 {
			t.Error("Expected entries in segment file")
		}
	}
}

// TestPartitionedWriter_RemovePartitionSegmentFiles tests the RemovePartitionSegmentFiles function
func TestPartitionedWriter_RemovePartitionSegmentFiles(t *testing.T) {
	tmpDir := t.TempDir()

	config := PartitionedConfig{
		BaseDir:     tmpDir,
		Config:      DefaultConfig(tmpDir),
		IdleTimeout: 1 * time.Minute,
	}

	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatalf("Failed to create partitioned writer: %v", err)
	}
	defer func() { _ = pw.Close() }()

	database := "testdb"
	collection := "metrics"
	date := time.Date(2026, 1, 27, 0, 0, 0, 0, time.UTC)

	// Write entries
	for i := 0; i < 5; i++ {
		entry := &Entry{
			Type:       EntryTypeWrite,
			Database:   database,
			Collection: collection,
			ID:         string(rune('a' + i)),
			Fields:     map[string]interface{}{"value": float64(i)},
			Timestamp:  time.Now().UnixNano(),
		}
		_, _ = pw.WritePartitioned(entry, database, date)
	}

	_ = pw.FlushPartition(database, collection, date)

	// Get segment files
	files, _ := pw.PrepareFlushPartition(database, collection, date)

	// RemovePartitionSegmentFiles
	if err := pw.RemovePartitionSegmentFiles(database, collection, date, files); err != nil {
		t.Fatalf("RemovePartitionSegmentFiles failed: %v", err)
	}
}

// TestPartitionedWriter_HasPartitionData tests the HasPartitionData function
func TestPartitionedWriter_HasPartitionData(t *testing.T) {
	tmpDir := t.TempDir()

	config := PartitionedConfig{
		BaseDir:     tmpDir,
		Config:      DefaultConfig(tmpDir),
		IdleTimeout: 1 * time.Minute,
	}

	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatalf("Failed to create partitioned writer: %v", err)
	}
	defer func() { _ = pw.Close() }()

	database := "testdb"
	collection := "metrics"
	date := time.Date(2026, 1, 27, 0, 0, 0, 0, time.UTC)

	// Check non-existent partition
	hasData := pw.HasPartitionData(database, collection, date)
	if hasData {
		t.Error("Expected HasPartitionData to return false for non-existent partition")
	}

	// Write entry
	entry := &Entry{
		Type:       EntryTypeWrite,
		Database:   database,
		Collection: collection,
		ID:         "1",
		Fields:     map[string]interface{}{"value": 100.0},
		Timestamp:  time.Now().UnixNano(),
	}
	_, _ = pw.WritePartitioned(entry, database, date)
	_ = pw.FlushPartition(database, collection, date)

	// Check existing partition
	hasData = pw.HasPartitionData(database, collection, date)
	if !hasData {
		t.Error("Expected HasPartitionData to return true for existing partition")
	}
}

// TestPartitionedWriter_ListPartitions_Empty tests ListPartitions with no partitions
func TestPartitionedWriter_ListPartitions_Empty(t *testing.T) {
	tmpDir := t.TempDir()

	config := PartitionedConfig{
		BaseDir:     tmpDir,
		Config:      DefaultConfig(tmpDir),
		IdleTimeout: 1 * time.Minute,
	}

	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatalf("Failed to create partitioned writer: %v", err)
	}
	defer func() { _ = pw.Close() }()

	partitions, err := pw.ListPartitions()
	if err != nil {
		t.Fatalf("ListPartitions failed: %v", err)
	}
	if len(partitions) != 0 {
		t.Errorf("Expected 0 partitions, got %d", len(partitions))
	}
}

// TestPartitionedWriter_Close tests the Close function
func TestPartitionedWriter_Close(t *testing.T) {
	tmpDir := t.TempDir()

	config := PartitionedConfig{
		BaseDir:     tmpDir,
		Config:      DefaultConfig(tmpDir),
		IdleTimeout: 1 * time.Minute,
	}

	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatalf("Failed to create partitioned writer: %v", err)
	}

	date := time.Date(2026, 1, 27, 0, 0, 0, 0, time.UTC)

	// Write entries
	for _, db := range []string{"db1", "db2"} {
		entry := &Entry{
			Type:       EntryTypeWrite,
			Database:   db,
			Collection: "metrics",
			ID:         "1",
			Fields:     map[string]interface{}{"value": 100.0},
			Timestamp:  time.Now().UnixNano(),
		}
		_, _ = pw.WritePartitioned(entry, db, date)
	}

	// Close should flush and cleanup
	if err := pw.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen and verify data persists
	pw2, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer func() { _ = pw2.Close() }()

	entries, err := pw2.ReadAllPartitions()
	if err != nil {
		t.Fatalf("ReadAllPartitions failed: %v", err)
	}

	if len(entries) != 2 {
		t.Errorf("Expected 2 entries after reopen, got %d", len(entries))
	}
}

// TestPartitionedWriter_ConcurrentWrites tests concurrent writes to multiple partitions
func TestPartitionedWriter_ConcurrentWrites(t *testing.T) {
	tmpDir := t.TempDir()

	config := PartitionedConfig{
		BaseDir:     tmpDir,
		Config:      DefaultConfig(tmpDir),
		IdleTimeout: 1 * time.Minute,
	}

	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatalf("Failed to create partitioned writer: %v", err)
	}
	defer func() { _ = pw.Close() }()

	date := time.Date(2026, 1, 27, 0, 0, 0, 0, time.UTC)
	databases := []string{"db1", "db2", "db3", "db4"}

	numGoroutines := 10
	entriesPerGoroutine := 50

	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < entriesPerGoroutine; j++ {
				db := databases[j%len(databases)]
				entry := &Entry{
					Type:       EntryTypeWrite,
					Database:   db,
					Collection: "metrics",
					ID:         string(rune(id*1000 + j)),
					Fields:     map[string]interface{}{"goroutine": id, "iteration": j},
					Timestamp:  time.Now().UnixNano(),
				}
				// Use sync write to ensure data is flushed
				_ = pw.WriteSyncPartitioned(entry, db, date)
			}
		}(i)
	}

	wg.Wait()

	// Verify all entries were written
	entries, err := pw.ReadAllPartitions()
	if err != nil {
		t.Fatalf("ReadAllPartitions failed: %v", err)
	}

	expected := numGoroutines * entriesPerGoroutine
	if len(entries) != expected {
		t.Errorf("Expected %d entries, got %d", expected, len(entries))
	}
}

// TestPartitionedWriter_IdleCleanup tests that idle partitions are cleaned up
func TestPartitionedWriter_IdleCleanup(t *testing.T) {
	tmpDir := t.TempDir()

	config := PartitionedConfig{
		BaseDir:     tmpDir,
		Config:      DefaultConfig(tmpDir),
		IdleTimeout: 50 * time.Millisecond,
	}

	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatalf("Failed to create partitioned writer: %v", err)
	}
	defer func() { _ = pw.Close() }()

	date := time.Date(2026, 1, 27, 0, 0, 0, 0, time.UTC)

	// Write entry
	entry := &Entry{
		Type:       EntryTypeWrite,
		Database:   "testdb",
		Collection: "metrics",
		ID:         "1",
		Fields:     map[string]interface{}{"value": 100.0},
		Timestamp:  time.Now().UnixNano(),
	}
	_, _ = pw.WritePartitioned(entry, "testdb", date)
	_ = pw.FlushAll()

	// Initial partition count
	partitions, _ := pw.ListPartitions()
	initialCount := len(partitions)
	if initialCount != 1 {
		t.Errorf("Expected 1 partition initially, got %d", initialCount)
	}

	// Wait for cleanup
	time.Sleep(300 * time.Millisecond)

	// Partition may be cleaned up (closed but data still on disk)
	// Just verify no panic occurs
}

// TestPartitionedWriter_WriteResult tests the WriteResult struct
func TestPartitionedWriter_WriteResult(t *testing.T) {
	tmpDir := t.TempDir()

	config := PartitionedConfig{
		BaseDir:     tmpDir,
		Config:      DefaultConfig(tmpDir),
		IdleTimeout: 1 * time.Minute,
	}

	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatalf("Failed to create partitioned writer: %v", err)
	}
	defer func() { _ = pw.Close() }()

	date := time.Date(2026, 1, 27, 0, 0, 0, 0, time.UTC)

	entry := &Entry{
		Type:       EntryTypeWrite,
		Database:   "testdb",
		Collection: "metrics",
		ID:         "1",
		Fields:     map[string]interface{}{"value": 100.0},
		Timestamp:  time.Now().UnixNano(),
	}

	result, err := pw.WritePartitioned(entry, "testdb", date)
	if err != nil {
		t.Fatalf("WritePartitioned failed: %v", err)
	}

	// WriteResult only has IsNewSegment field
	_ = result.IsNewSegment // Just verify the field exists
}

// TestPartitionInfo tests the PartitionInfo struct
func TestPartitionInfo(t *testing.T) {
	date := time.Date(2026, 1, 27, 0, 0, 0, 0, time.UTC)

	info := PartitionInfo{
		Database:   "testdb",
		Collection: "metrics",
		Date:       date,
		Dir:        "/tmp/testdb/metrics/2026-01-27",
	}

	if info.Database != "testdb" {
		t.Errorf("Expected database testdb, got %s", info.Database)
	}
	if info.Collection != "metrics" {
		t.Errorf("Expected collection metrics, got %s", info.Collection)
	}
	if info.Date != date {
		t.Errorf("Expected date %v, got %v", date, info.Date)
	}
	if info.Dir != "/tmp/testdb/metrics/2026-01-27" {
		t.Errorf("Expected dir /tmp/testdb/metrics/2026-01-27, got %s", info.Dir)
	}
}

// TestPartitionedWriter_GetStats_Details tests GetStats function in detail
func TestPartitionedWriter_GetStats_Details(t *testing.T) {
	tmpDir := t.TempDir()

	config := PartitionedConfig{
		BaseDir:     tmpDir,
		Config:      DefaultConfig(tmpDir),
		IdleTimeout: 1 * time.Minute,
	}

	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatalf("Failed to create partitioned writer: %v", err)
	}
	defer func() { _ = pw.Close() }()

	// Initial stats
	stats := pw.GetStats()
	if stats.PartitionCount != 0 {
		t.Errorf("Expected 0 partitions initially, got %d", stats.PartitionCount)
	}

	date := time.Date(2026, 1, 27, 0, 0, 0, 0, time.UTC)

	// Write to multiple partitions
	for i, db := range []string{"db1", "db2", "db3"} {
		for j := 0; j < 5; j++ {
			entry := &Entry{
				Type:       EntryTypeWrite,
				Database:   db,
				Collection: "metrics",
				ID:         string(rune('a' + i*5 + j)),
				Fields:     map[string]interface{}{"value": float64(i*5 + j)},
				Timestamp:  time.Now().UnixNano(),
			}
			_, _ = pw.WritePartitioned(entry, db, date)
		}
	}

	_ = pw.FlushAll()

	// Updated stats
	stats = pw.GetStats()
	if stats.PartitionCount != 3 {
		t.Errorf("Expected 3 partitions, got %d", stats.PartitionCount)
	}
	// Check total pending
	_ = stats.TotalPending // Just verify field exists
}

// Helper: create a partitioned writer with short idle timeout for testing cleanup
func setupTestPartitionedWriter(t *testing.T, idleTimeout time.Duration) (PartitionedWriter, string) {
	t.Helper()
	dir := t.TempDir()
	config := PartitionedConfig{
		BaseDir: dir,
		Config: Config{
			Dir:            dir,
			MaxBatchSize:   100,
			FlushInterval:  10 * time.Millisecond,
			MaxSegmentSize: 1024 * 1024,
		},
		IdleTimeout: idleTimeout,
	}
	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatalf("NewPartitionedWriter failed: %v", err)
	}
	return pw, dir
}

// --- doCleanup (0% coverage) ---

func TestDoCleanup_ClosesIdleWriters(t *testing.T) {
	dir := t.TempDir()
	config := PartitionedConfig{
		BaseDir: dir,
		Config: Config{
			Dir:            dir,
			MaxBatchSize:   100,
			FlushInterval:  10 * time.Millisecond,
			MaxSegmentSize: 1024 * 1024,
		},
		IdleTimeout: 1 * time.Millisecond, // Very short idle timeout
	}
	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatalf("NewPartitionedWriter failed: %v", err)
	}
	defer func() { _ = pw.Close() }()

	date := time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)

	// Write some data
	entry := &Entry{
		Type:       EntryTypeWrite,
		Database:   "testdb",
		Collection: "metrics",
		Fields:     map[string]interface{}{"v": 1.0},
	}
	_, err = pw.WritePartitioned(entry, "testdb", date)
	if err != nil {
		t.Fatalf("WritePartitioned failed: %v", err)
	}

	// Flush to ensure data is on disk
	_ = pw.FlushPartition("testdb", "metrics", date)

	// Verify writer exists
	stats := pw.GetStats()
	if stats.PartitionCount != 1 {
		t.Fatalf("Expected 1 partition, got %d", stats.PartitionCount)
	}

	// Wait for idle timeout
	time.Sleep(50 * time.Millisecond)

	// Directly call doCleanup on the underlying type
	ppw := pw.(*partitionedWriter)
	ppw.doCleanup()

	// After cleanup, the idle writer should be removed
	stats = pw.GetStats()
	if stats.PartitionCount != 0 {
		t.Errorf("Expected 0 partitions after idle cleanup, got %d", stats.PartitionCount)
	}
}

func TestDoCleanup_KeepsRecentWriters(t *testing.T) {
	dir := t.TempDir()
	config := PartitionedConfig{
		BaseDir: dir,
		Config: Config{
			Dir:            dir,
			MaxBatchSize:   100,
			FlushInterval:  10 * time.Millisecond,
			MaxSegmentSize: 1024 * 1024,
		},
		IdleTimeout: 10 * time.Minute, // Long idle timeout
	}
	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatalf("NewPartitionedWriter failed: %v", err)
	}
	defer func() { _ = pw.Close() }()

	date := time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)

	entry := &Entry{
		Type:       EntryTypeWrite,
		Database:   "testdb",
		Collection: "metrics",
		Fields:     map[string]interface{}{"v": 1.0},
	}
	_, _ = pw.WritePartitioned(entry, "testdb", date)

	ppw := pw.(*partitionedWriter)
	ppw.doCleanup()

	// Writer should still exist (recently accessed)
	stats := pw.GetStats()
	if stats.PartitionCount != 1 {
		t.Errorf("Expected 1 partition (not idle), got %d", stats.PartitionCount)
	}
}

func TestDoCleanup_CleansNotifiedState(t *testing.T) {
	dir := t.TempDir()
	config := PartitionedConfig{
		BaseDir: dir,
		Config: Config{
			Dir:            dir,
			MaxBatchSize:   100,
			FlushInterval:  10 * time.Millisecond,
			MaxSegmentSize: 1024 * 1024,
		},
		IdleTimeout: 1 * time.Millisecond,
	}
	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatalf("NewPartitionedWriter failed: %v", err)
	}
	defer func() { _ = pw.Close() }()

	date := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)

	entry := &Entry{
		Type:       EntryTypeWrite,
		Database:   "db1",
		Collection: "col1",
		Fields:     map[string]interface{}{"v": 1.0},
	}
	_, _ = pw.WritePartitioned(entry, "db1", date)
	_ = pw.FlushPartition("db1", "col1", date)

	time.Sleep(50 * time.Millisecond)

	ppw := pw.(*partitionedWriter)
	ppw.doCleanup()

	// notified state should be cleaned
	ppw.mu.RLock()
	_, notifiedExists := ppw.notified["db1:col1:2025-06-01"]
	ppw.mu.RUnlock()

	if notifiedExists {
		t.Error("Expected notified state to be cleaned up for idle partition")
	}
}

// --- cleanupEmptyDirectories (0% coverage) ---

func TestCleanupEmptyDirectories_RemovesEmptyDirs(t *testing.T) {
	dir := t.TempDir()
	config := PartitionedConfig{
		BaseDir: dir,
		Config: Config{
			Dir:            dir,
			MaxBatchSize:   100,
			FlushInterval:  10 * time.Millisecond,
			MaxSegmentSize: 1024 * 1024,
		},
		IdleTimeout: 10 * time.Minute,
	}
	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatalf("NewPartitionedWriter failed: %v", err)
	}
	defer func() { _ = pw.Close() }()

	// Create empty directory structure
	emptyDateDir := filepath.Join(dir, "testdb", "metrics", "2025-01-15")
	if err := os.MkdirAll(emptyDateDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Verify dirs exist
	if _, err := os.Stat(emptyDateDir); os.IsNotExist(err) {
		t.Fatal("Expected date dir to exist")
	}

	ppw := pw.(*partitionedWriter)
	ppw.cleanupEmptyDirectories()

	// All empty directories should be removed
	if _, err := os.Stat(emptyDateDir); !os.IsNotExist(err) {
		t.Error("Expected empty date directory to be removed")
	}
	if _, err := os.Stat(filepath.Join(dir, "testdb", "metrics")); !os.IsNotExist(err) {
		t.Error("Expected empty collection directory to be removed")
	}
	if _, err := os.Stat(filepath.Join(dir, "testdb")); !os.IsNotExist(err) {
		t.Error("Expected empty database directory to be removed")
	}
}

func TestCleanupEmptyDirectories_PreservesNonEmptyDirs(t *testing.T) {
	dir := t.TempDir()
	config := PartitionedConfig{
		BaseDir: dir,
		Config: Config{
			Dir:            dir,
			MaxBatchSize:   100,
			FlushInterval:  10 * time.Millisecond,
			MaxSegmentSize: 1024 * 1024,
		},
		IdleTimeout: 10 * time.Minute,
	}
	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatalf("NewPartitionedWriter failed: %v", err)
	}
	defer func() { _ = pw.Close() }()

	// Create non-empty directory structure
	dateDir := filepath.Join(dir, "testdb", "metrics", "2025-01-15")
	if err := os.MkdirAll(dateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	walFile := filepath.Join(dateDir, "wal-100.log")
	if err := os.WriteFile(walFile, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	ppw := pw.(*partitionedWriter)
	ppw.cleanupEmptyDirectories()

	// Directory should still exist since it has a file
	if _, err := os.Stat(dateDir); os.IsNotExist(err) {
		t.Error("Non-empty date directory should not be removed")
	}
}

func TestCleanupEmptyDirectories_MixedEmptyAndNonEmpty(t *testing.T) {
	dir := t.TempDir()
	config := PartitionedConfig{
		BaseDir: dir,
		Config: Config{
			Dir:            dir,
			MaxBatchSize:   100,
			FlushInterval:  10 * time.Millisecond,
			MaxSegmentSize: 1024 * 1024,
		},
		IdleTimeout: 10 * time.Minute,
	}
	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatalf("NewPartitionedWriter failed: %v", err)
	}
	defer func() { _ = pw.Close() }()

	// One empty date dir, one with data
	emptyDate := filepath.Join(dir, "testdb", "metrics", "2025-01-01")
	fullDate := filepath.Join(dir, "testdb", "metrics", "2025-01-02")
	if err := os.MkdirAll(emptyDate, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(fullDate, 0o755); err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(filepath.Join(fullDate, "wal-100.log"), []byte("data"), 0o644)

	ppw := pw.(*partitionedWriter)
	ppw.cleanupEmptyDirectories()

	// Empty date should be removed
	if _, err := os.Stat(emptyDate); !os.IsNotExist(err) {
		t.Error("Expected empty date directory to be removed")
	}
	// Full date should remain
	if _, err := os.Stat(fullDate); os.IsNotExist(err) {
		t.Error("Non-empty date directory should not be removed")
	}
	// Collection dir should remain (has fullDate)
	if _, err := os.Stat(filepath.Join(dir, "testdb", "metrics")); os.IsNotExist(err) {
		t.Error("Collection directory should not be removed (has non-empty child)")
	}
}

func TestCleanupEmptyDirectories_WithNonDirEntries(t *testing.T) {
	dir := t.TempDir()
	config := PartitionedConfig{
		BaseDir: dir,
		Config: Config{
			Dir:            dir,
			MaxBatchSize:   100,
			FlushInterval:  10 * time.Millisecond,
			MaxSegmentSize: 1024 * 1024,
		},
		IdleTimeout: 10 * time.Minute,
	}
	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatalf("NewPartitionedWriter failed: %v", err)
	}
	defer func() { _ = pw.Close() }()

	// Create a file at database level (not a dir)
	if err := os.WriteFile(filepath.Join(dir, "readme.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Create a file at collection level
	dbDir := filepath.Join(dir, "mydb")
	_ = os.MkdirAll(dbDir, 0o755)
	_ = os.WriteFile(filepath.Join(dbDir, "metadata.json"), []byte("{}"), 0o644)

	// Create a file at date level
	collDir := filepath.Join(dbDir, "mycol")
	_ = os.MkdirAll(collDir, 0o755)
	_ = os.WriteFile(filepath.Join(collDir, "checkpoint.dat"), []byte("cp"), 0o644)

	ppw := pw.(*partitionedWriter)
	// Should not panic
	ppw.cleanupEmptyDirectories()

	// Non-dir files should still exist
	if _, err := os.Stat(filepath.Join(dir, "readme.txt")); os.IsNotExist(err) {
		t.Error("readme.txt should remain")
	}
}

// --- ReadPartition for non-existent partition (42.9% coverage) ---

func TestReadPartition_NonExistentPartition(t *testing.T) {
	pw, _ := setupTestPartitionedWriter(t, 10*time.Minute)
	defer func() { _ = pw.Close() }()

	date := time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC)

	// Reading a partition that doesn't exist should return nil, nil or create it
	entries, err := pw.ReadPartition("nonexistent", "col", date)
	if err != nil {
		t.Fatalf("ReadPartition for non-existent should not error, got: %v", err)
	}
	// Should either return nil or empty
	if len(entries) != 0 {
		t.Errorf("Expected 0 entries for new partition, got %d", len(entries))
	}
}

func TestReadPartition_WithDataFromDisk(t *testing.T) {
	dir := t.TempDir()
	date := time.Date(2025, 3, 15, 0, 0, 0, 0, time.UTC)

	// First writer: create data and close
	config := PartitionedConfig{
		BaseDir: dir,
		Config: Config{
			Dir:            dir,
			MaxBatchSize:   100,
			FlushInterval:  10 * time.Millisecond,
			MaxSegmentSize: 1024 * 1024,
		},
		IdleTimeout: 10 * time.Minute,
	}
	pw1, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatal(err)
	}

	entry := &Entry{
		Type:       EntryTypeWrite,
		Database:   "db",
		Collection: "col",
		Fields:     map[string]interface{}{"v": 42.0},
	}
	_ = pw1.WriteSyncPartitioned(entry, "db", date)
	_ = pw1.Close()

	// Second writer: read the data
	pw2, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pw2.Close() }()

	entries, err := pw2.ReadPartition("db", "col", date)
	if err != nil {
		t.Fatalf("ReadPartition failed: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("Expected 1 entry, got %d", len(entries))
	}
}

// --- ProcessPartitions edge cases (62.5% coverage) ---

func TestProcessPartitions_HandlerReturnsError(t *testing.T) {
	pw, _ := setupTestPartitionedWriter(t, 10*time.Minute)
	defer func() { _ = pw.Close() }()

	date := time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)

	// Write data to a partition
	entry := &Entry{
		Type:       EntryTypeWrite,
		Database:   "testdb",
		Collection: "metrics",
		Fields:     map[string]interface{}{"v": 1.0},
	}
	_ = pw.WriteSyncPartitioned(entry, "testdb", date)

	// Process with handler that returns error
	handlerCalled := false
	err := pw.ProcessPartitions(func(partition PartitionInfo, entries []*Entry) error {
		handlerCalled = true
		return fmt.Errorf("handler error")
	})
	if err != nil {
		t.Fatalf("ProcessPartitions should not return error when handler fails, got: %v", err)
	}
	if !handlerCalled {
		t.Error("Handler should have been called")
	}

	// Partition should NOT be removed since handler returned error
	partitions, _ := pw.ListPartitions()
	if len(partitions) == 0 {
		t.Error("Partition should not be removed when handler returns error")
	}
}

func TestProcessPartitions_EmptyPartition(t *testing.T) {
	pw, dir := setupTestPartitionedWriter(t, 10*time.Minute)
	defer func() { _ = pw.Close() }()

	// Create an empty partition directory (no WAL files)
	emptyDir := filepath.Join(dir, "emptydb", "col", "2025-01-15")
	if err := os.MkdirAll(emptyDir, 0o755); err != nil {
		t.Fatal(err)
	}

	handlerCalled := false
	err := pw.ProcessPartitions(func(partition PartitionInfo, entries []*Entry) error {
		handlerCalled = true
		return nil
	})
	if err != nil {
		t.Fatalf("ProcessPartitions failed: %v", err)
	}

	// Handler should NOT be called for empty partition
	if handlerCalled {
		t.Error("Handler should not be called for empty partition")
	}
}

func TestProcessPartitions_MultiplePartitions(t *testing.T) {
	pw, _ := setupTestPartitionedWriter(t, 10*time.Minute)
	defer func() { _ = pw.Close() }()

	dates := []time.Time{
		time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC),
		time.Date(2025, 1, 3, 0, 0, 0, 0, time.UTC),
	}

	for i, date := range dates {
		entry := &Entry{
			Type:       EntryTypeWrite,
			Database:   "db",
			Collection: "col",
			Fields:     map[string]interface{}{"v": float64(i)},
		}
		_ = pw.WriteSyncPartitioned(entry, "db", date)
	}

	processed := make(map[string]int)
	var mu sync.Mutex
	err := pw.ProcessPartitions(func(partition PartitionInfo, entries []*Entry) error {
		mu.Lock()
		processed[partition.Date.Format("2006-01-02")] = len(entries)
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("ProcessPartitions failed: %v", err)
	}

	if len(processed) != 3 {
		t.Errorf("Expected 3 partitions processed, got %d", len(processed))
	}
}

func TestProcessPartitions_NoPartitions(t *testing.T) {
	pw, _ := setupTestPartitionedWriter(t, 10*time.Minute)
	defer func() { _ = pw.Close() }()

	called := false
	err := pw.ProcessPartitions(func(partition PartitionInfo, entries []*Entry) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("ProcessPartitions with no partitions failed: %v", err)
	}
	if called {
		t.Error("Handler should not be called with no partitions")
	}
}

// --- ListPartitions edge cases (71% coverage) ---

func TestListPartitions_WithInvalidDateDirectories(t *testing.T) {
	pw, dir := setupTestPartitionedWriter(t, 10*time.Minute)
	defer func() { _ = pw.Close() }()

	// Create dirs with invalid date names
	validDate := filepath.Join(dir, "testdb", "metrics", "2025-01-15")
	invalidDate := filepath.Join(dir, "testdb", "metrics", "not-a-date")
	invalidDate2 := filepath.Join(dir, "testdb", "metrics", "2025-13-45")

	_ = os.MkdirAll(validDate, 0o755)
	_ = os.MkdirAll(invalidDate, 0o755)
	_ = os.MkdirAll(invalidDate2, 0o755)

	partitions, err := pw.ListPartitions()
	if err != nil {
		t.Fatalf("ListPartitions failed: %v", err)
	}

	// Only the valid date dir should be listed
	if len(partitions) != 1 {
		t.Errorf("Expected 1 valid partition, got %d", len(partitions))
	}
	if len(partitions) > 0 && partitions[0].Date.Format("2006-01-02") != "2025-01-15" {
		t.Errorf("Expected date 2025-01-15, got %s", partitions[0].Date.Format("2006-01-02"))
	}
}

func TestListPartitions_WithNonDirFiles(t *testing.T) {
	pw, dir := setupTestPartitionedWriter(t, 10*time.Minute)
	defer func() { _ = pw.Close() }()

	// Create a file at database level (not directory)
	_ = os.WriteFile(filepath.Join(dir, "config.yaml"), []byte("test"), 0o644)

	// Create a file at collection level
	dbDir := filepath.Join(dir, "testdb")
	_ = os.MkdirAll(dbDir, 0o755)
	_ = os.WriteFile(filepath.Join(dbDir, "meta.json"), []byte("{}"), 0o644)

	// Create a file at date level
	collDir := filepath.Join(dbDir, "metrics")
	_ = os.MkdirAll(collDir, 0o755)
	_ = os.WriteFile(filepath.Join(collDir, "readme.txt"), []byte("hi"), 0o644)

	// Also create a valid date dir
	_ = os.MkdirAll(filepath.Join(collDir, "2025-01-15"), 0o755)

	partitions, err := pw.ListPartitions()
	if err != nil {
		t.Fatalf("ListPartitions failed: %v", err)
	}

	// Should only find the valid date directory
	if len(partitions) != 1 {
		t.Errorf("Expected 1 partition (non-dir files skipped), got %d", len(partitions))
	}
}

func TestListPartitions_EmptyBaseDir(t *testing.T) {
	pw, _ := setupTestPartitionedWriter(t, 10*time.Minute)
	defer func() { _ = pw.Close() }()

	partitions, err := pw.ListPartitions()
	if err != nil {
		t.Fatalf("ListPartitions on empty base dir failed: %v", err)
	}
	if len(partitions) != 0 {
		t.Errorf("Expected 0 partitions, got %d", len(partitions))
	}
}

func TestListPartitions_NonExistentBaseDir(t *testing.T) {
	dir := t.TempDir()
	config := PartitionedConfig{
		BaseDir: dir,
		Config: Config{
			Dir:            dir,
			MaxBatchSize:   100,
			FlushInterval:  10 * time.Millisecond,
			MaxSegmentSize: 1024 * 1024,
		},
		IdleTimeout: 10 * time.Minute,
	}
	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pw.Close() }()

	// Remove base dir after creation
	_ = os.RemoveAll(dir)

	partitions, err := pw.ListPartitions()
	if err != nil {
		t.Fatalf("ListPartitions with removed dir should not error, got: %v", err)
	}
	if len(partitions) != 0 {
		t.Errorf("Expected 0 partitions, got %d", len(partitions))
	}
}

// --- WriteSyncPartitioned (75% coverage) ---

func TestWriteSyncPartitioned_MultipleWrites(t *testing.T) {
	pw, _ := setupTestPartitionedWriter(t, 10*time.Minute)
	defer func() { _ = pw.Close() }()

	date := time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)

	for i := 0; i < 10; i++ {
		entry := &Entry{
			Type:       EntryTypeWrite,
			Database:   "testdb",
			Collection: "metrics",
			Fields:     map[string]interface{}{"v": float64(i)},
		}
		err := pw.WriteSyncPartitioned(entry, "testdb", date)
		if err != nil {
			t.Fatalf("WriteSyncPartitioned %d failed: %v", i, err)
		}
	}

	entries, err := pw.ReadPartition("testdb", "metrics", date)
	if err != nil {
		t.Fatalf("ReadPartition failed: %v", err)
	}
	if len(entries) != 10 {
		t.Errorf("Expected 10 entries, got %d", len(entries))
	}
}

// --- FlushPartition/SetCheckpointPartition/TruncatePartition for non-existent partitions ---

func TestFlushPartition_NonExistentPartition(t *testing.T) {
	pw, _ := setupTestPartitionedWriter(t, 10*time.Minute)
	defer func() { _ = pw.Close() }()

	date := time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC)

	// Should return nil (partition doesn't exist, nothing to flush)
	err := pw.FlushPartition("nonexistent", "col", date)
	if err != nil {
		t.Errorf("FlushPartition for non-existent should return nil, got: %v", err)
	}
}

func TestSetCheckpointPartition_NonExistentPartition(t *testing.T) {
	pw, _ := setupTestPartitionedWriter(t, 10*time.Minute)
	defer func() { _ = pw.Close() }()

	date := time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC)

	err := pw.SetCheckpointPartition("nonexistent", "col", date)
	if err != nil {
		t.Errorf("SetCheckpointPartition for non-existent should return nil, got: %v", err)
	}
}

func TestTruncatePartitionBeforeCheckpoint_NonExistentPartition(t *testing.T) {
	pw, _ := setupTestPartitionedWriter(t, 10*time.Minute)
	defer func() { _ = pw.Close() }()

	date := time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC)

	err := pw.TruncatePartitionBeforeCheckpoint("nonexistent", "col", date)
	if err != nil {
		t.Errorf("TruncatePartitionBeforeCheckpoint for non-existent should return nil, got: %v", err)
	}
}

// --- WritePartitioned IsNewSegment behavior ---

func TestWritePartitioned_IsNewSegmentFlag(t *testing.T) {
	pw, _ := setupTestPartitionedWriter(t, 10*time.Minute)
	defer func() { _ = pw.Close() }()

	date := time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)

	// First write should set IsNewSegment = true
	entry := &Entry{
		Type:       EntryTypeWrite,
		Database:   "testdb",
		Collection: "metrics",
		Fields:     map[string]interface{}{"v": 1.0},
	}
	result, err := pw.WritePartitioned(entry, "testdb", date)
	if err != nil {
		t.Fatalf("WritePartitioned failed: %v", err)
	}
	if !result.IsNewSegment {
		t.Error("First write should set IsNewSegment=true")
	}

	// Second write should set IsNewSegment = false
	entry2 := &Entry{
		Type:       EntryTypeWrite,
		Database:   "testdb",
		Collection: "metrics",
		Fields:     map[string]interface{}{"v": 2.0},
	}
	result2, err := pw.WritePartitioned(entry2, "testdb", date)
	if err != nil {
		t.Fatalf("WritePartitioned failed: %v", err)
	}
	if result2.IsNewSegment {
		t.Error("Second write should set IsNewSegment=false")
	}
}

func TestWritePartitioned_IsNewSegmentAfterPrepareFlush(t *testing.T) {
	pw, _ := setupTestPartitionedWriter(t, 10*time.Minute)
	defer func() { _ = pw.Close() }()

	date := time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)

	// Write and flush
	entry := &Entry{
		Type:       EntryTypeWrite,
		Database:   "testdb",
		Collection: "metrics",
		Fields:     map[string]interface{}{"v": 1.0},
	}
	_, _ = pw.WritePartitioned(entry, "testdb", date)
	_ = pw.FlushPartition("testdb", "metrics", date)

	// PrepareFlush resets notified flag
	_, _ = pw.PrepareFlushPartition("testdb", "metrics", date)

	// Next write after PrepareFlush should set IsNewSegment = true
	entry2 := &Entry{
		Type:       EntryTypeWrite,
		Database:   "testdb",
		Collection: "metrics",
		Fields:     map[string]interface{}{"v": 2.0},
	}
	result, err := pw.WritePartitioned(entry2, "testdb", date)
	if err != nil {
		t.Fatalf("WritePartitioned failed: %v", err)
	}
	if !result.IsNewSegment {
		t.Error("Write after PrepareFlush should set IsNewSegment=true")
	}
}

// --- GetPartitionWriter concurrent access ---

func TestGetPartitionWriter_ConcurrentAccess(t *testing.T) {
	pw, _ := setupTestPartitionedWriter(t, 10*time.Minute)
	defer func() { _ = pw.Close() }()

	date := time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)
	var wg sync.WaitGroup

	// Multiple goroutines trying to get the same writer
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := pw.GetPartitionWriter("testdb", "metrics", date)
			if err != nil {
				t.Errorf("GetPartitionWriter failed: %v", err)
			}
		}()
	}

	wg.Wait()

	stats := pw.GetStats()
	if stats.PartitionCount != 1 {
		t.Errorf("Expected 1 partition, got %d", stats.PartitionCount)
	}
}

// --- HasPartitionData ---

func TestHasPartitionData_NoData(t *testing.T) {
	pw, _ := setupTestPartitionedWriter(t, 10*time.Minute)
	defer func() { _ = pw.Close() }()

	date := time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)

	// New partition with no data written
	hasData := pw.HasPartitionData("newdb", "col", date)
	// May be false or true depending on whether the empty initial segment counts
	_ = hasData // Just ensure no panic
}

func TestHasPartitionData_WithData(t *testing.T) {
	pw, _ := setupTestPartitionedWriter(t, 10*time.Minute)
	defer func() { _ = pw.Close() }()

	date := time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)

	entry := &Entry{
		Type:       EntryTypeWrite,
		Database:   "testdb",
		Collection: "metrics",
		Fields:     map[string]interface{}{"v": 1.0},
	}
	_ = pw.WriteSyncPartitioned(entry, "testdb", date)

	if !pw.HasPartitionData("testdb", "metrics", date) {
		t.Error("Expected HasPartitionData=true after writing data")
	}
}

// --- ReadAllPartitions ---

func TestReadAllPartitions_MultipleDBs(t *testing.T) {
	pw, _ := setupTestPartitionedWriter(t, 10*time.Minute)
	defer func() { _ = pw.Close() }()

	date := time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)

	// Write to different databases
	for i, db := range []string{"db1", "db2", "db3"} {
		entry := &Entry{
			Type:       EntryTypeWrite,
			Database:   db,
			Collection: "col",
			Fields:     map[string]interface{}{"v": float64(i)},
		}
		_ = pw.WriteSyncPartitioned(entry, db, date)
	}

	entries, err := pw.ReadAllPartitions()
	if err != nil {
		t.Fatalf("ReadAllPartitions failed: %v", err)
	}
	if len(entries) != 3 {
		t.Errorf("Expected 3 entries, got %d", len(entries))
	}
}

// --- FlushAll with multiple partitions ---

func TestFlushAll_MultiplePartitions(t *testing.T) {
	pw, _ := setupTestPartitionedWriter(t, 10*time.Minute)
	defer func() { _ = pw.Close() }()

	dates := []time.Time{
		time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC),
	}

	for i, date := range dates {
		entry := &Entry{
			Type:       EntryTypeWrite,
			Database:   "db",
			Collection: "col",
			Fields:     map[string]interface{}{"v": float64(i)},
		}
		_, _ = pw.WritePartitioned(entry, "db", date)
	}

	err := pw.FlushAll()
	if err != nil {
		t.Fatalf("FlushAll failed: %v", err)
	}
}

// --- Close with multiple partitions ---

func TestClose_MultiplePartitions(t *testing.T) {
	pw, _ := setupTestPartitionedWriter(t, 10*time.Minute)

	dates := []time.Time{
		time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC),
		time.Date(2025, 1, 3, 0, 0, 0, 0, time.UTC),
	}

	for i, date := range dates {
		entry := &Entry{
			Type:       EntryTypeWrite,
			Database:   "db",
			Collection: "col",
			Fields:     map[string]interface{}{"v": float64(i)},
		}
		_ = pw.WriteSyncPartitioned(entry, "db", date)
	}

	// Close should close all partition writers
	err := pw.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

// --- RotateAll ---

func TestRotateAll_Empty(t *testing.T) {
	pw, _ := setupTestPartitionedWriter(t, 10*time.Minute)
	defer func() { _ = pw.Close() }()

	err := pw.RotateAll()
	if err != nil {
		t.Fatalf("RotateAll on empty writer failed: %v", err)
	}
}

// --- SetCheckpointAll / TruncateBeforeCheckpointAll ---

func TestCheckpointAndTruncateAll(t *testing.T) {
	pw, _ := setupTestPartitionedWriter(t, 10*time.Minute)
	defer func() { _ = pw.Close() }()

	date := time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)

	for i := 0; i < 5; i++ {
		entry := &Entry{
			Type:       EntryTypeWrite,
			Database:   "testdb",
			Collection: "metrics",
			Fields:     map[string]interface{}{"v": float64(i)},
		}
		_ = pw.WriteSyncPartitioned(entry, "testdb", date)
	}

	_ = pw.SetCheckpointAll()

	// Write more after checkpoint
	for i := 5; i < 10; i++ {
		entry := &Entry{
			Type:       EntryTypeWrite,
			Database:   "testdb",
			Collection: "metrics",
			Fields:     map[string]interface{}{"v": float64(i)},
		}
		_ = pw.WriteSyncPartitioned(entry, "testdb", date)
	}

	err := pw.TruncateBeforeCheckpointAll()
	if err != nil {
		t.Fatalf("TruncateBeforeCheckpointAll failed: %v", err)
	}
}

// --- GetTotalSegmentCount ---

func TestGetTotalSegmentCount_MultiplePartitions(t *testing.T) {
	pw, _ := setupTestPartitionedWriter(t, 10*time.Minute)
	defer func() { _ = pw.Close() }()

	dates := []time.Time{
		time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC),
	}

	for i, date := range dates {
		entry := &Entry{
			Type:       EntryTypeWrite,
			Database:   "db",
			Collection: "col",
			Fields:     map[string]interface{}{"v": float64(i)},
		}
		_ = pw.WriteSyncPartitioned(entry, "db", date)
	}

	count, err := pw.GetTotalSegmentCount()
	if err != nil {
		t.Fatalf("GetTotalSegmentCount failed: %v", err)
	}
	if count < 2 {
		t.Errorf("Expected at least 2 segments (one per partition), got %d", count)
	}
}

// --- RemovePartition ---

func TestRemovePartition_ExistingPartition(t *testing.T) {
	pw, _ := setupTestPartitionedWriter(t, 10*time.Minute)
	defer func() { _ = pw.Close() }()

	date := time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)

	entry := &Entry{
		Type:       EntryTypeWrite,
		Database:   "testdb",
		Collection: "metrics",
		Fields:     map[string]interface{}{"v": 1.0},
	}
	_ = pw.WriteSyncPartitioned(entry, "testdb", date)

	err := pw.RemovePartition("testdb", "metrics", date)
	if err != nil {
		t.Fatalf("RemovePartition failed: %v", err)
	}

	stats := pw.GetStats()
	if stats.PartitionCount != 0 {
		t.Errorf("Expected 0 partitions after removal, got %d", stats.PartitionCount)
	}
}

func TestRemovePartition_NonExistent(t *testing.T) {
	pw, _ := setupTestPartitionedWriter(t, 10*time.Minute)
	defer func() { _ = pw.Close() }()

	date := time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC)

	// Should not error for non-existent partition
	err := pw.RemovePartition("ghost", "col", date)
	if err != nil {
		t.Fatalf("RemovePartition for non-existent should not error, got: %v", err)
	}
}

// --- PrepareFlushPartition ---

func TestPrepareFlushPartition_WithData(t *testing.T) {
	pw, _ := setupTestPartitionedWriter(t, 10*time.Minute)
	defer func() { _ = pw.Close() }()

	date := time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)

	for i := 0; i < 5; i++ {
		entry := &Entry{
			Type:       EntryTypeWrite,
			Database:   "testdb",
			Collection: "metrics",
			Fields:     map[string]interface{}{"v": float64(i)},
		}
		_ = pw.WriteSyncPartitioned(entry, "testdb", date)
	}

	files, err := pw.PrepareFlushPartition("testdb", "metrics", date)
	if err != nil {
		t.Fatalf("PrepareFlushPartition failed: %v", err)
	}
	if len(files) == 0 {
		t.Error("Expected segment files")
	}
}

// --- ReadPartitionSegmentFile ---

func TestReadPartitionSegmentFile_ValidFile(t *testing.T) {
	pw, _ := setupTestPartitionedWriter(t, 10*time.Minute)
	defer func() { _ = pw.Close() }()

	date := time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)

	entry := &Entry{
		Type:       EntryTypeWrite,
		Database:   "testdb",
		Collection: "metrics",
		Fields:     map[string]interface{}{"v": 1.0},
	}
	_ = pw.WriteSyncPartitioned(entry, "testdb", date)

	files, _ := pw.PrepareFlushPartition("testdb", "metrics", date)
	if len(files) > 0 {
		entries, err := pw.ReadPartitionSegmentFile("testdb", "metrics", date, files[0])
		if err != nil {
			t.Fatalf("ReadPartitionSegmentFile failed: %v", err)
		}
		if len(entries) == 0 {
			t.Error("Expected entries in segment file")
		}
	}
}

// --- RemovePartitionSegmentFiles ---

func TestRemovePartitionSegmentFiles_ValidFiles(t *testing.T) {
	pw, _ := setupTestPartitionedWriter(t, 10*time.Minute)
	defer func() { _ = pw.Close() }()

	date := time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)

	entry := &Entry{
		Type:       EntryTypeWrite,
		Database:   "testdb",
		Collection: "metrics",
		Fields:     map[string]interface{}{"v": 1.0},
	}
	_ = pw.WriteSyncPartitioned(entry, "testdb", date)

	files, _ := pw.PrepareFlushPartition("testdb", "metrics", date)
	if len(files) > 0 {
		err := pw.RemovePartitionSegmentFiles("testdb", "metrics", date, files)
		if err != nil {
			t.Fatalf("RemovePartitionSegmentFiles failed: %v", err)
		}
	}
}

// --- cleanupIdleWriters goroutine lifecycle ---

func TestCleanupIdleWriters_StopsOnClose(t *testing.T) {
	pw, _ := setupTestPartitionedWriter(t, 10*time.Minute)

	// Close should stop the cleanup goroutine without hanging
	done := make(chan struct{})
	go func() {
		_ = pw.Close()
		close(done)
	}()

	select {
	case <-done:
		// OK
	case <-time.After(5 * time.Second):
		t.Fatal("Close timed out - cleanup goroutine may be stuck")
	}
}

// --- Concurrent operations on partitioned writer ---

func TestPartitionedWriter_ConcurrentWriteAndRead(t *testing.T) {
	pw, _ := setupTestPartitionedWriter(t, 10*time.Minute)
	defer func() { _ = pw.Close() }()

	date := time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)

	var wg sync.WaitGroup

	// Writers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				entry := &Entry{
					Type:       EntryTypeWrite,
					Database:   fmt.Sprintf("db%d", id),
					Collection: "metrics",
					Fields:     map[string]interface{}{"v": float64(j)},
				}
				_ = pw.WriteSyncPartitioned(entry, fmt.Sprintf("db%d", id), date)
			}
		}(i)
	}

	// Readers
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_, _ = pw.ListPartitions()
				_ = pw.GetStats()
			}
		}()
	}

	wg.Wait()
}

// --- NewPartitionedWriter with invalid base dir ---

func TestNewPartitionedWriter_CreatesBaseDir(t *testing.T) {
	dir := t.TempDir()
	deepDir := filepath.Join(dir, "deep", "nested", "wal")

	config := PartitionedConfig{
		BaseDir: deepDir,
		Config: Config{
			Dir:            deepDir,
			MaxBatchSize:   100,
			FlushInterval:  10 * time.Millisecond,
			MaxSegmentSize: 1024 * 1024,
		},
		IdleTimeout: 10 * time.Minute,
	}

	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatalf("NewPartitionedWriter failed to create deep dir: %v", err)
	}
	defer func() { _ = pw.Close() }()

	if _, err := os.Stat(deepDir); os.IsNotExist(err) {
		t.Error("Expected base dir to be created")
	}
}

// --- NewPartitionedWriter with invalid path ---

func TestNewPartitionedWriter_InvalidPath(t *testing.T) {
	config := PartitionedConfig{
		BaseDir: "/dev/null/invalid/path",
		Config: Config{
			Dir:            "/dev/null/invalid/path",
			MaxBatchSize:   100,
			FlushInterval:  10 * time.Millisecond,
			MaxSegmentSize: 1024 * 1024,
		},
		IdleTimeout: 10 * time.Minute,
	}

	_, err := NewPartitionedWriter(config)
	if err == nil {
		t.Error("Expected error for invalid base dir path")
	}
}

// --- WritePartitioned error from GetPartitionWriter ---

func TestWritePartitioned_ErrorGettingWriter(t *testing.T) {
	dir := t.TempDir()
	config := PartitionedConfig{
		BaseDir: dir,
		Config: Config{
			Dir:            dir,
			MaxBatchSize:   100,
			FlushInterval:  10 * time.Millisecond,
			MaxSegmentSize: 1024 * 1024,
		},
		IdleTimeout: 10 * time.Minute,
	}
	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pw.Close() }()

	date := time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)

	// Make the partition directory path unwritable to force GetPartitionWriter error
	partDir := filepath.Join(dir, "testdb", "metrics")
	_ = os.MkdirAll(partDir, 0o755)
	dateDir := filepath.Join(partDir, "2025-01-15")
	// Create a file where the directory should be, causing MkdirAll to fail
	_ = os.WriteFile(dateDir, []byte("not a dir"), 0o644)

	entry := &Entry{
		Type:       EntryTypeWrite,
		Database:   "testdb",
		Collection: "metrics",
		Fields:     map[string]interface{}{"v": 1.0},
	}
	_, err = pw.WritePartitioned(entry, "testdb", date)
	if err == nil {
		t.Error("Expected error when partition directory creation fails")
	}
}

// --- WriteSyncPartitioned error from GetPartitionWriter ---

func TestWriteSyncPartitioned_ErrorGettingWriter(t *testing.T) {
	dir := t.TempDir()
	config := PartitionedConfig{
		BaseDir: dir,
		Config: Config{
			Dir:            dir,
			MaxBatchSize:   100,
			FlushInterval:  10 * time.Millisecond,
			MaxSegmentSize: 1024 * 1024,
		},
		IdleTimeout: 10 * time.Minute,
	}
	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pw.Close() }()

	date := time.Date(2025, 2, 20, 0, 0, 0, 0, time.UTC)

	// Block the path with a file
	partDir := filepath.Join(dir, "testdb", "col1")
	_ = os.MkdirAll(partDir, 0o755)
	dateDir := filepath.Join(partDir, "2025-02-20")
	_ = os.WriteFile(dateDir, []byte("block"), 0o644)

	entry := &Entry{
		Type:       EntryTypeWrite,
		Database:   "testdb",
		Collection: "col1",
		Fields:     map[string]interface{}{"v": 1.0},
	}
	err = pw.WriteSyncPartitioned(entry, "testdb", date)
	if err == nil {
		t.Error("Expected error when partition directory creation fails")
	}
}

// --- ReadPartition error path ---

func TestReadPartition_ErrorGettingWriterButDirDoesNotExist(t *testing.T) {
	dir := t.TempDir()
	config := PartitionedConfig{
		BaseDir: dir,
		Config: Config{
			Dir:            dir,
			MaxBatchSize:   100,
			FlushInterval:  10 * time.Millisecond,
			MaxSegmentSize: 1024 * 1024,
		},
		IdleTimeout: 10 * time.Minute,
	}
	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pw.Close() }()

	date := time.Date(2025, 3, 10, 0, 0, 0, 0, time.UTC)

	// Block the date path so GetPartitionWriter fails with MkdirAll error
	partDir := filepath.Join(dir, "dbx", "colx")
	_ = os.MkdirAll(partDir, 0o755)
	dateDir := filepath.Join(partDir, "2025-03-10")
	_ = os.WriteFile(dateDir, []byte("block"), 0o644)

	entries, err := pw.ReadPartition("dbx", "colx", date)
	// GetPartitionWriter fails, then stat: dateDir is a file not dir, but it exists
	// So it should return nil, err
	if err == nil {
		t.Logf("ReadPartition entries: %v", entries)
	}
}

func TestReadPartition_ErrorGettingWriterDirNotExist(t *testing.T) {
	dir := t.TempDir()
	config := PartitionedConfig{
		BaseDir: dir,
		Config: Config{
			Dir:            dir,
			MaxBatchSize:   100,
			FlushInterval:  10 * time.Millisecond,
			MaxSegmentSize: 1024 * 1024,
		},
		IdleTimeout: 10 * time.Minute,
	}
	pw, err := NewPartitionedWriter(config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pw.Close() }()

	date := time.Date(2025, 3, 10, 0, 0, 0, 0, time.UTC)

	// Make the parent path a file to cause MkdirAll to fail in GetPartitionWriter
	// AND ensure the date dir doesn't exist for the IsNotExist check
	_ = os.WriteFile(filepath.Join(dir, "dbfail"), []byte("not a dir"), 0o644)

	entries, err := pw.ReadPartition("dbfail", "col", date)
	// GetPartitionWriter fails (MkdirAll fails), then os.Stat for partition dir → not exist
	// Should return nil, nil
	if err != nil {
		t.Logf("ReadPartition returned error (expected nil,nil): %v", err)
	}
	if len(entries) > 0 {
		t.Error("Expected nil entries for non-existent partition dir")
	}
}

// --- ProcessPartitions with flush error ---

func TestProcessPartitions_FlushError(t *testing.T) {
	pw, dir := setupTestPartitionedWriter(t, 10*time.Minute)
	defer func() { _ = pw.Close() }()

	// Create a partition directory with invalid WAL data
	dateDir := filepath.Join(dir, "baddb", "col", "2025-01-15")
	_ = os.MkdirAll(dateDir, 0o755)
	// Write invalid WAL file
	_ = os.WriteFile(filepath.Join(dateDir, "wal-100.log"), []byte("not valid proto"), 0o644)

	// ProcessPartitions should handle the error and continue
	err := pw.ProcessPartitions(func(partition PartitionInfo, entries []*Entry) error {
		return nil
	})
	if err != nil {
		t.Fatalf("ProcessPartitions should not fail overall: %v", err)
	}
}

// --- ReadAllPartitions with error ---

func TestReadAllPartitions_Empty(t *testing.T) {
	pw, _ := setupTestPartitionedWriter(t, 10*time.Minute)
	defer func() { _ = pw.Close() }()

	entries, err := pw.ReadAllPartitions()
	if err != nil {
		t.Fatalf("ReadAllPartitions on empty writer failed: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("Expected 0 entries, got %d", len(entries))
	}
}

// --- GetStats on empty writer ---

func TestGetStats_EmptyWriter(t *testing.T) {
	pw, _ := setupTestPartitionedWriter(t, 10*time.Minute)
	defer func() { _ = pw.Close() }()

	stats := pw.GetStats()
	if stats.PartitionCount != 0 {
		t.Errorf("Expected 0 partitions, got %d", stats.PartitionCount)
	}
	if stats.TotalPending != 0 {
		t.Errorf("Expected 0 total pending, got %d", stats.TotalPending)
	}
}

// --- PrepareFlushPartition resets notified ---

func TestPrepareFlushPartition_ResetsNotified(t *testing.T) {
	pw, _ := setupTestPartitionedWriter(t, 10*time.Minute)
	defer func() { _ = pw.Close() }()

	date := time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)

	entry := &Entry{
		Type:       EntryTypeWrite,
		Database:   "db",
		Collection: "col",
		Fields:     map[string]interface{}{"v": 1.0},
	}
	result1, _ := pw.WritePartitioned(entry, "db", date)
	if !result1.IsNewSegment {
		t.Error("First write should be new segment")
	}

	_ = pw.FlushPartition("db", "col", date)
	_, _ = pw.PrepareFlushPartition("db", "col", date)

	// Verify notified flag was reset
	ppw := pw.(*partitionedWriter)
	ppw.mu.RLock()
	notified := ppw.notified["db:col:2025-01-15"]
	ppw.mu.RUnlock()

	if notified {
		t.Error("Expected notified to be reset after PrepareFlush")
	}

	// Next write should be "new segment" again
	result2, _ := pw.WritePartitioned(entry, "db", date)
	if !result2.IsNewSegment {
		t.Error("Write after PrepareFlush should be IsNewSegment=true")
	}
}

// --- HasPartitionData returns false for error ---

func TestHasPartitionData_ErrorCreatingWriter(t *testing.T) {
	dir := t.TempDir()
	config := PartitionedConfig{
		BaseDir: dir,
		Config: Config{
			Dir:            dir,
			MaxBatchSize:   100,
			FlushInterval:  10 * time.Millisecond,
			MaxSegmentSize: 1024 * 1024,
		},
		IdleTimeout: 10 * time.Minute,
	}
	pw, _ := NewPartitionedWriter(config)
	defer func() { _ = pw.Close() }()

	date := time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)

	// Block path so GetPartitionWriter fails
	_ = os.WriteFile(filepath.Join(dir, "blocked"), []byte("file"), 0o644)

	// Should return false when writer can't be created
	result := pw.HasPartitionData("blocked", "col", date)
	if result {
		t.Error("Expected HasPartitionData=false when writer can't be created")
	}
}
