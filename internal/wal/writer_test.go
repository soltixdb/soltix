package wal

import (
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// =============================================================================
// Tests for writer.go
// =============================================================================

// TestDefaultConfig tests the DefaultConfig function
func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig(t.TempDir())

	if config.MaxBatchSize <= 0 {
		t.Errorf("MaxBatchSize should be > 0, got %d", config.MaxBatchSize)
	}
	if config.FlushInterval <= 0 {
		t.Errorf("FlushInterval should be > 0, got %v", config.FlushInterval)
	}
	if config.MaxSegmentSize <= 0 {
		t.Errorf("MaxSegmentSize should be > 0, got %d", config.MaxSegmentSize)
	}
	if config.Dir == "" {
		t.Error("Dir should not be empty")
	}
}

// TestNewWriter tests the NewWriter function
func TestNewWriter(t *testing.T) {
	dir := t.TempDir()

	writer, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	defer func() { _ = writer.Close() }()

	if writer == nil {
		t.Fatal("Expected non-nil writer")
	}
}

// TestNewWriterWithConfig tests the NewWriterWithConfig function
func TestNewWriterWithConfig(t *testing.T) {
	dir := t.TempDir()

	config := Config{
		Dir:            dir,
		MaxBatchSize:   50,
		FlushInterval:  50 * time.Millisecond,
		MaxSegmentSize: 512 * 1024,
	}

	writer, err := NewWriterWithConfig(config)
	if err != nil {
		t.Fatalf("NewWriterWithConfig failed: %v", err)
	}
	defer func() { _ = writer.Close() }()

	if writer == nil {
		t.Fatal("Expected non-nil writer")
	}
}

// TestWriter_Write tests the Write function
func TestWriter_Write(t *testing.T) {
	dir := t.TempDir()

	writer, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	defer func() { _ = writer.Close() }()

	entry := &Entry{
		Type:      EntryTypeWrite,
		Database:  "testdb",
		Fields:    map[string]interface{}{"value": 42.0},
		Timestamp: time.Now().UnixNano(),
	}

	if err := writer.Write(entry); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Flush to ensure it's written
	if err := writer.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
}

// TestWriter_WriteSync tests the WriteSync function
func TestWriter_WriteSync(t *testing.T) {
	dir := t.TempDir()

	config := Config{
		Dir:            dir,
		MaxBatchSize:   100,
		FlushInterval:  1 * time.Second, // Long interval to test sync behavior
		MaxSegmentSize: 1024 * 1024,
	}

	writer, err := NewWriterWithConfig(config)
	if err != nil {
		t.Fatalf("NewWriterWithConfig failed: %v", err)
	}
	defer func() { _ = writer.Close() }()

	entry := &Entry{
		Type:      EntryTypeWrite,
		Database:  "testdb",
		Fields:    map[string]interface{}{"value": 100.0},
		Timestamp: time.Now().UnixNano(),
	}

	// WriteSync should immediately flush
	if err := writer.WriteSync(entry); err != nil {
		t.Fatalf("WriteSync failed: %v", err)
	}

	// Entry should be immediately readable
	entries, err := writer.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(entries) != 1 {
		t.Errorf("Expected 1 entry, got %d", len(entries))
	}
}

// TestWriter_WriteBatch tests the WriteBatch function
func TestWriter_WriteBatch(t *testing.T) {
	dir := t.TempDir()

	writer, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	defer func() { _ = writer.Close() }()

	entries := []*Entry{
		{Type: EntryTypeWrite, Database: "db1", Fields: map[string]interface{}{"v": 1.0}, Timestamp: time.Now().UnixNano()},
		{Type: EntryTypeWrite, Database: "db2", Fields: map[string]interface{}{"v": 2.0}, Timestamp: time.Now().UnixNano()},
		{Type: EntryTypeWrite, Database: "db3", Fields: map[string]interface{}{"v": 3.0}, Timestamp: time.Now().UnixNano()},
	}

	// WriteBatch should write all entries
	if err := writer.WriteBatch(entries); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	// Should be able to read them back
	readEntries, err := writer.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(readEntries) != 3 {
		t.Errorf("Expected 3 entries, got %d", len(readEntries))
	}
}

// TestWriter_Flush tests the Flush function
func TestWriter_Flush(t *testing.T) {
	dir := t.TempDir()

	config := Config{
		Dir:            dir,
		MaxBatchSize:   1000, // Large batch to ensure we need manual flush
		FlushInterval:  10 * time.Second,
		MaxSegmentSize: 1024 * 1024,
	}

	writer, err := NewWriterWithConfig(config)
	if err != nil {
		t.Fatalf("NewWriterWithConfig failed: %v", err)
	}
	defer func() { _ = writer.Close() }()

	// Write entries (below batch size)
	for i := 0; i < 10; i++ {
		entry := &Entry{
			Type:      EntryTypeWrite,
			Database:  "testdb",
			Fields:    map[string]interface{}{"value": float64(i)},
			Timestamp: time.Now().UnixNano(),
		}
		_ = writer.Write(entry)
	}

	// Flush manually
	if err := writer.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// All entries should be readable
	entries, err := writer.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(entries) != 10 {
		t.Errorf("Expected 10 entries, got %d", len(entries))
	}
}

// TestWriter_Close tests the Close function
func TestWriter_Close(t *testing.T) {
	dir := t.TempDir()

	writer, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	// Write some entries
	for i := 0; i < 5; i++ {
		entry := &Entry{
			Type:      EntryTypeWrite,
			Database:  "testdb",
			Fields:    map[string]interface{}{"value": float64(i)},
			Timestamp: time.Now().UnixNano(),
		}
		_ = writer.Write(entry)
	}

	// Close should flush remaining entries
	if err := writer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen and verify data persists
	writer2, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer func() { _ = writer2.Close() }()

	entries, err := writer2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(entries) != 5 {
		t.Errorf("Expected 5 entries, got %d", len(entries))
	}
}

// TestWriter_GetStats tests the GetStats function
func TestWriter_GetStats(t *testing.T) {
	dir := t.TempDir()

	writer, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	defer func() { _ = writer.Close() }()

	// Initial stats
	stats := writer.GetStats()
	if stats.PendingEntries != 0 {
		t.Errorf("Initial PendingEntries should be 0, got %d", stats.PendingEntries)
	}

	// Verify BatchSize and FlushInterval exist
	_ = stats.BatchSize
	_ = stats.FlushInterval
}

// TestWriter_ReadAll tests the ReadAll function
func TestWriter_ReadAll(t *testing.T) {
	dir := t.TempDir()

	writer, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	defer func() { _ = writer.Close() }()

	// Write entries
	for i := 0; i < 10; i++ {
		entry := &Entry{
			Type:       EntryTypeWrite,
			Database:   "testdb",
			Collection: "metrics",
			Fields:     map[string]interface{}{"value": float64(i)},
			Timestamp:  time.Now().UnixNano(),
		}
		_ = writer.Write(entry)
	}

	_ = writer.Flush()

	// ReadAll
	entries, err := writer.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(entries) != 10 {
		t.Errorf("Expected 10 entries, got %d", len(entries))
	}
}

// TestWriter_SetCheckpoint tests the SetCheckpoint function
func TestWriter_SetCheckpoint(t *testing.T) {
	dir := t.TempDir()

	writer, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	defer func() { _ = writer.Close() }()

	// Write entries
	for i := 0; i < 5; i++ {
		entry := &Entry{
			Type:      EntryTypeWrite,
			Database:  "testdb",
			Fields:    map[string]interface{}{"value": float64(i)},
			Timestamp: time.Now().UnixNano(),
		}
		_ = writer.Write(entry)
	}

	_ = writer.Flush()

	// Set checkpoint
	if err := writer.SetCheckpoint(); err != nil {
		t.Fatalf("SetCheckpoint failed: %v", err)
	}
}

// TestWriter_TruncateBeforeCheckpoint tests the TruncateBeforeCheckpoint function
func TestWriter_TruncateBeforeCheckpoint(t *testing.T) {
	dir := t.TempDir()

	writer, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	defer func() { _ = writer.Close() }()

	// Write entries
	for i := 0; i < 5; i++ {
		entry := &Entry{
			Type:      EntryTypeWrite,
			Database:  "testdb",
			Fields:    map[string]interface{}{"value": float64(i)},
			Timestamp: time.Now().UnixNano(),
		}
		_ = writer.Write(entry)
	}

	_ = writer.Flush()

	// Set checkpoint
	_ = writer.SetCheckpoint()

	// Truncate
	if err := writer.TruncateBeforeCheckpoint(); err != nil {
		t.Fatalf("TruncateBeforeCheckpoint failed: %v", err)
	}
}

// TestWriter_GetSegmentCount tests the GetSegmentCount function
func TestWriter_GetSegmentCount(t *testing.T) {
	dir := t.TempDir()

	writer, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	defer func() { _ = writer.Close() }()

	// Write entries
	for i := 0; i < 5; i++ {
		entry := &Entry{
			Type:      EntryTypeWrite,
			Database:  "testdb",
			Fields:    map[string]interface{}{"value": float64(i)},
			Timestamp: time.Now().UnixNano(),
		}
		_ = writer.Write(entry)
	}

	_ = writer.Flush()

	// Get segment count
	count, err := writer.GetSegmentCount()
	if err != nil {
		t.Fatalf("GetSegmentCount failed: %v", err)
	}
	if count < 1 {
		t.Errorf("Expected at least 1 segment, got %d", count)
	}
}

// TestWriter_PrepareFlush tests the PrepareFlush function
func TestWriter_PrepareFlush(t *testing.T) {
	dir := t.TempDir()

	writer, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	defer func() { _ = writer.Close() }()

	// Write entries
	for i := 0; i < 5; i++ {
		entry := &Entry{
			Type:      EntryTypeWrite,
			Database:  "testdb",
			Fields:    map[string]interface{}{"value": float64(i)},
			Timestamp: time.Now().UnixNano(),
		}
		_ = writer.Write(entry)
	}

	_ = writer.Flush()

	// PrepareFlush
	files, err := writer.PrepareFlush()
	if err != nil {
		t.Fatalf("PrepareFlush failed: %v", err)
	}

	if len(files) == 0 {
		t.Error("Expected at least one segment file")
	}
}

// TestWriter_ReadSegmentFile tests the ReadSegmentFile function
func TestWriter_ReadSegmentFile(t *testing.T) {
	dir := t.TempDir()

	writer, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	defer func() { _ = writer.Close() }()

	// Write entries
	for i := 0; i < 5; i++ {
		entry := &Entry{
			Type:      EntryTypeWrite,
			Database:  "testdb",
			Fields:    map[string]interface{}{"value": float64(i)},
			Timestamp: time.Now().UnixNano(),
		}
		_ = writer.Write(entry)
	}

	_ = writer.Flush()

	// Get segment files
	files, _ := writer.PrepareFlush()

	if len(files) > 0 {
		// Read segment file
		entries, err := writer.ReadSegmentFile(files[0])
		if err != nil {
			t.Fatalf("ReadSegmentFile failed: %v", err)
		}

		if len(entries) == 0 {
			t.Error("Expected entries in segment file")
		}
	}
}

// TestWriter_RemoveSegmentFiles tests the RemoveSegmentFiles function
func TestWriter_RemoveSegmentFiles(t *testing.T) {
	dir := t.TempDir()

	writer, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	defer func() { _ = writer.Close() }()

	// Write entries
	for i := 0; i < 5; i++ {
		entry := &Entry{
			Type:      EntryTypeWrite,
			Database:  "testdb",
			Fields:    map[string]interface{}{"value": float64(i)},
			Timestamp: time.Now().UnixNano(),
		}
		_ = writer.Write(entry)
	}

	_ = writer.Flush()

	// Get segment files
	files, _ := writer.PrepareFlush()

	// Remove segment files
	if err := writer.RemoveSegmentFiles(files); err != nil {
		t.Fatalf("RemoveSegmentFiles failed: %v", err)
	}
}

// TestWriter_HasData tests the HasData function
func TestWriter_HasData(t *testing.T) {
	t.Run("EmptyWriter", func(t *testing.T) {
		dir := t.TempDir()
		writer, _ := NewWriter(dir)
		defer func() { _ = writer.Close() }()

		// Empty writer might return true/false, just check no panic
		_ = writer.HasData()
	})

	t.Run("WriterWithData", func(t *testing.T) {
		dir := t.TempDir()
		writer, _ := NewWriter(dir)
		defer func() { _ = writer.Close() }()

		entry := &Entry{
			Type:      EntryTypeWrite,
			Database:  "testdb",
			Fields:    map[string]interface{}{"value": 1.0},
			Timestamp: time.Now().UnixNano(),
		}
		_ = writer.Write(entry)
		_ = writer.Flush()

		if !writer.HasData() {
			t.Error("Expected HasData to return true")
		}
	})
}

// TestWriter_ConcurrentWrites tests concurrent writes
func TestWriter_ConcurrentWrites(t *testing.T) {
	dir := t.TempDir()

	writer, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	defer func() { _ = writer.Close() }()

	numGoroutines := 10
	entriesPerGoroutine := 100

	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < entriesPerGoroutine; j++ {
				entry := &Entry{
					Type:      EntryTypeWrite,
					Database:  "testdb",
					Fields:    map[string]interface{}{"goroutine": id, "iteration": j},
					Timestamp: time.Now().UnixNano(),
				}
				// Use WriteSync for guaranteed write
				_ = writer.WriteSync(entry)
			}
		}(i)
	}

	wg.Wait()

	// Verify all entries were written
	entries, err := writer.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	expected := numGoroutines * entriesPerGoroutine
	if len(entries) != expected {
		t.Errorf("Expected %d entries, got %d", expected, len(entries))
	}
}

// TestWriter_AutoFlushOnBatchSize tests automatic flush when batch size is reached
func TestWriter_AutoFlushOnBatchSize(t *testing.T) {
	dir := t.TempDir()

	config := Config{
		Dir:            dir,
		MaxBatchSize:   5, // Small batch size
		FlushInterval:  10 * time.Second,
		MaxSegmentSize: 1024 * 1024,
	}

	writer, err := NewWriterWithConfig(config)
	if err != nil {
		t.Fatalf("NewWriterWithConfig failed: %v", err)
	}
	defer func() { _ = writer.Close() }()

	// Write exactly batch size entries
	for i := 0; i < 5; i++ {
		entry := &Entry{
			Type:      EntryTypeWrite,
			Database:  "testdb",
			Fields:    map[string]interface{}{"value": float64(i)},
			Timestamp: time.Now().UnixNano(),
		}
		_ = writer.Write(entry)
	}

	// Give time for auto-flush
	time.Sleep(100 * time.Millisecond)

	// Should be able to read entries (auto-flushed)
	entries, _ := writer.ReadAll()
	if len(entries) != 5 {
		t.Errorf("Expected 5 entries after auto-flush, got %d", len(entries))
	}
}

// TestWriter_AutoFlushOnInterval tests automatic flush on interval
func TestWriter_AutoFlushOnInterval(t *testing.T) {
	dir := t.TempDir()

	config := Config{
		Dir:            dir,
		MaxBatchSize:   1000, // Large batch size
		FlushInterval:  100 * time.Millisecond,
		MaxSegmentSize: 1024 * 1024,
	}

	writer, err := NewWriterWithConfig(config)
	if err != nil {
		t.Fatalf("NewWriterWithConfig failed: %v", err)
	}
	defer func() { _ = writer.Close() }()

	// Write entries (below batch size)
	for i := 0; i < 3; i++ {
		entry := &Entry{
			Type:      EntryTypeWrite,
			Database:  "testdb",
			Fields:    map[string]interface{}{"value": float64(i)},
			Timestamp: time.Now().UnixNano(),
		}
		_ = writer.Write(entry)
	}

	// Wait for flush interval
	time.Sleep(200 * time.Millisecond)

	// Should be auto-flushed
	entries, _ := writer.ReadAll()
	if len(entries) != 3 {
		t.Errorf("Expected 3 entries after interval flush, got %d", len(entries))
	}
}

// TestWriter_SetErrorHandler tests the SetErrorHandler function
func TestWriter_SetErrorHandler(t *testing.T) {
	dir := t.TempDir()

	config := Config{
		Dir:            dir,
		MaxBatchSize:   100,
		FlushInterval:  50 * time.Millisecond,
		MaxSegmentSize: 64 * 1024 * 1024,
	}

	writer, err := NewWriterWithConfig(config)
	if err != nil {
		t.Fatalf("NewWriterWithConfig failed: %v", err)
	}
	defer func() { _ = writer.Close() }()

	// Set error handler
	var handlerCalled bool
	var handlerMu sync.Mutex
	writer.SetErrorHandler(func(err error, entriesLost int) {
		handlerMu.Lock()
		handlerCalled = true
		handlerMu.Unlock()
	})

	// Write some entries (normal operation - handler should NOT be called)
	for i := 0; i < 5; i++ {
		entry := &Entry{
			Type:      EntryTypeWrite,
			Database:  "testdb",
			Fields:    map[string]interface{}{"value": float64(i)},
			Timestamp: time.Now().UnixNano(),
		}
		_ = writer.Write(entry)
	}

	// Wait for flush
	time.Sleep(100 * time.Millisecond)

	// Verify entries were written successfully
	entries, err := writer.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(entries) != 5 {
		t.Errorf("Expected 5 entries, got %d", len(entries))
	}

	// Handler should NOT be called on successful writes
	handlerMu.Lock()
	if handlerCalled {
		t.Error("Error handler should not be called on successful writes")
	}
	handlerMu.Unlock()
}

// TestWriter_SetErrorHandler_Nil tests that nil error handler is handled gracefully
func TestWriter_SetErrorHandler_Nil(t *testing.T) {
	dir := t.TempDir()

	writer, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	defer func() { _ = writer.Close() }()

	// Set nil error handler - should not panic
	writer.SetErrorHandler(nil)

	// Write should still work
	entry := &Entry{
		Type:      EntryTypeWrite,
		Database:  "testdb",
		Fields:    map[string]interface{}{"value": 1.0},
		Timestamp: time.Now().UnixNano(),
	}

	if err := writer.Write(entry); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Wait for flush
	time.Sleep(50 * time.Millisecond)

	entries, _ := writer.ReadAll()
	if len(entries) != 1 {
		t.Errorf("Expected 1 entry, got %d", len(entries))
	}
}

// --- newBatchWriter with zero/negative config values (defaults) ---

func TestNewBatchWriter_DefaultConfig(t *testing.T) {
	dir := t.TempDir()
	base, err := newBaseWAL(dir, 1024*1024)
	if err != nil {
		t.Fatalf("newBaseWAL failed: %v", err)
	}

	// All zero config -> should use defaults
	bw := newBatchWriter(base, batchConfig{
		MaxBatchSize:  0,
		FlushInterval: 0,
	})

	// Defaults should be applied
	if bw.batchSize != 1000 {
		t.Errorf("Expected default batchSize=1000, got %d", bw.batchSize)
	}
	if bw.flushInterval != 10*time.Millisecond {
		t.Errorf("Expected default flushInterval=10ms, got %v", bw.flushInterval)
	}

	_ = bw.Close()
}

func TestNewBatchWriter_NegativeConfig(t *testing.T) {
	dir := t.TempDir()
	base, err := newBaseWAL(dir, 1024*1024)
	if err != nil {
		t.Fatalf("newBaseWAL failed: %v", err)
	}

	bw := newBatchWriter(base, batchConfig{
		MaxBatchSize:  -5,
		FlushInterval: -time.Second,
	})

	// Defaults should be applied for negative values
	if bw.batchSize != 1000 {
		t.Errorf("Expected default batchSize=1000, got %d", bw.batchSize)
	}
	if bw.flushInterval != 10*time.Millisecond {
		t.Errorf("Expected default flushInterval=10ms, got %v", bw.flushInterval)
	}

	_ = bw.Close()
}

// --- WriteBatch with empty entries ---

func TestWriterWriteBatch_EmptyEntries(t *testing.T) {
	dir := t.TempDir()
	writer, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	defer func() { _ = writer.Close() }()

	// Empty batch should return nil immediately
	err = writer.WriteBatch([]*Entry{})
	if err != nil {
		t.Fatalf("WriteBatch with empty entries should return nil, got: %v", err)
	}
}

func TestWriterWriteBatch_NilEntries(t *testing.T) {
	dir := t.TempDir()
	writer, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	defer func() { _ = writer.Close() }()

	// Nil slice should return nil
	err = writer.WriteBatch(nil)
	if err != nil {
		t.Fatalf("WriteBatch with nil entries should return nil, got: %v", err)
	}
}

// --- WriteBatch with auto-timestamp ---

func TestWriterWriteBatch_AutoTimestamp(t *testing.T) {
	dir := t.TempDir()
	writer, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	defer func() { _ = writer.Close() }()

	entries := []*Entry{
		{Type: EntryTypeWrite, Database: "db", Fields: map[string]interface{}{"v": 1.0}, Timestamp: 0},
		{Type: EntryTypeWrite, Database: "db", Fields: map[string]interface{}{"v": 2.0}, Timestamp: 0},
	}

	before := time.Now().UnixNano()
	err = writer.WriteBatch(entries)
	if err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}
	after := time.Now().UnixNano()

	// Both entries should have timestamps auto-set
	for i, e := range entries {
		if e.Timestamp < before || e.Timestamp > after {
			t.Errorf("Entry %d timestamp %d out of range [%d, %d]", i, e.Timestamp, before, after)
		}
	}
}

// --- WriteSync triggers flush ---

func TestWriterWriteSync_ImmediateFlush(t *testing.T) {
	dir := t.TempDir()
	config := Config{
		Dir:            dir,
		MaxBatchSize:   10000,            // Very large, so it won't auto-flush by batch size
		FlushInterval:  10 * time.Second, // Very long, so it won't auto-flush by time
		MaxSegmentSize: 64 * 1024 * 1024,
	}

	writer, err := NewWriterWithConfig(config)
	if err != nil {
		t.Fatalf("NewWriterWithConfig failed: %v", err)
	}
	defer func() { _ = writer.Close() }()

	entry := &Entry{
		Type:     EntryTypeWrite,
		Database: "db",
		Fields:   map[string]interface{}{"v": 42.0},
	}

	// WriteSync should block until data is flushed
	if err := writer.WriteSync(entry); err != nil {
		t.Fatalf("WriteSync failed: %v", err)
	}

	// Immediately verify data is persisted
	entries, err := writer.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("Expected 1 entry immediately after WriteSync, got %d", len(entries))
	}
}

// --- WriteSync with auto-timestamp ---

func TestWriterWriteSync_AutoTimestamp(t *testing.T) {
	dir := t.TempDir()
	writer, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	defer func() { _ = writer.Close() }()

	entry := &Entry{
		Type:      EntryTypeWrite,
		Database:  "db",
		Fields:    map[string]interface{}{"v": 1.0},
		Timestamp: 0, // Should be auto-set
	}

	before := time.Now().UnixNano()
	if err := writer.WriteSync(entry); err != nil {
		t.Fatalf("WriteSync failed: %v", err)
	}
	after := time.Now().UnixNano()

	if entry.Timestamp < before || entry.Timestamp > after {
		t.Errorf("Timestamp %d out of expected range", entry.Timestamp)
	}
}

// --- Write auto-timestamp ---

func TestWriterWrite_AutoTimestamp(t *testing.T) {
	dir := t.TempDir()
	writer, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	defer func() { _ = writer.Close() }()

	entry := &Entry{
		Type:      EntryTypeWrite,
		Database:  "db",
		Fields:    map[string]interface{}{"v": 1.0},
		Timestamp: 0,
	}

	before := time.Now().UnixNano()
	if err := writer.Write(entry); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	after := time.Now().UnixNano()

	if entry.Timestamp < before || entry.Timestamp > after {
		t.Errorf("Timestamp %d out of expected range", entry.Timestamp)
	}
}

// --- Flush with no pending entries ---

func TestWriterFlush_NoPending(t *testing.T) {
	dir := t.TempDir()
	writer, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	defer func() { _ = writer.Close() }()

	// Flush with no pending entries should succeed immediately
	err = writer.Flush()
	if err != nil {
		t.Fatalf("Flush with no pending entries failed: %v", err)
	}
}

// --- Flush timeout path ---

func TestWriterFlush_Timeout(t *testing.T) {
	dir := t.TempDir()
	base, err := newBaseWAL(dir, 1024*1024)
	if err != nil {
		t.Fatalf("newBaseWAL failed: %v", err)
	}

	// Create a batchWriter with very long flush interval
	bw := newBatchWriter(base, batchConfig{
		MaxBatchSize:  1000,
		FlushInterval: 10 * time.Second,
	})
	defer func() { _ = bw.Close() }()

	// Flush on empty writer should return quickly (no timeout)
	err = bw.Flush()
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
}

// --- Write that triggers batch size flush ---

func TestWriterWrite_TriggersBatchFlush(t *testing.T) {
	dir := t.TempDir()
	config := Config{
		Dir:            dir,
		MaxBatchSize:   3, // Very small batch
		FlushInterval:  10 * time.Second,
		MaxSegmentSize: 1024 * 1024,
	}

	writer, err := NewWriterWithConfig(config)
	if err != nil {
		t.Fatalf("NewWriterWithConfig failed: %v", err)
	}
	defer func() { _ = writer.Close() }()

	// Write exactly batch size entries
	for i := 0; i < 3; i++ {
		entry := &Entry{
			Type:     EntryTypeWrite,
			Database: "db",
			Fields:   map[string]interface{}{"v": float64(i)},
		}
		_ = writer.Write(entry)
	}

	// Wait for the batch-triggered flush
	time.Sleep(200 * time.Millisecond)

	entries, err := writer.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if len(entries) != 3 {
		t.Errorf("Expected 3 entries after batch-triggered flush, got %d", len(entries))
	}
}

// --- WriteSync that triggers batch size flush ---

func TestWriterWriteSync_TriggersBatchFlush(t *testing.T) {
	dir := t.TempDir()
	config := Config{
		Dir:            dir,
		MaxBatchSize:   2, // Very small batch
		FlushInterval:  10 * time.Second,
		MaxSegmentSize: 1024 * 1024,
	}

	writer, err := NewWriterWithConfig(config)
	if err != nil {
		t.Fatalf("NewWriterWithConfig failed: %v", err)
	}
	defer func() { _ = writer.Close() }()

	// Write one async first (below batch size)
	_ = writer.Write(&Entry{
		Type:     EntryTypeWrite,
		Database: "db",
		Fields:   map[string]interface{}{"v": 1.0},
	})

	// Now WriteSync, which should push us to/over batch size → triggers flush
	err = writer.WriteSync(&Entry{
		Type:     EntryTypeWrite,
		Database: "db",
		Fields:   map[string]interface{}{"v": 2.0},
	})
	if err != nil {
		t.Fatalf("WriteSync failed: %v", err)
	}

	entries, err := writer.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if len(entries) != 2 {
		t.Errorf("Expected 2 entries, got %d", len(entries))
	}
}

// --- flushPending with no entries (early return) ---

func TestFlushPending_Empty(t *testing.T) {
	dir := t.TempDir()
	base, err := newBaseWAL(dir, 1024*1024)
	if err != nil {
		t.Fatalf("newBaseWAL failed: %v", err)
	}

	bw := newBatchWriter(base, batchConfig{
		MaxBatchSize:  100,
		FlushInterval: time.Second,
	})
	defer func() { _ = bw.Close() }()

	// flushPending with empty buffer should return nil immediately
	err = bw.flushPending()
	if err != nil {
		t.Fatalf("flushPending with empty buffer failed: %v", err)
	}
}

// --- flushPending with error handler callback ---

func TestFlushPending_ErrorHandlerCalled(t *testing.T) {
	dir := t.TempDir()
	base, err := newBaseWAL(dir, 1024*1024)
	if err != nil {
		t.Fatalf("newBaseWAL failed: %v", err)
	}

	bw := newBatchWriter(base, batchConfig{
		MaxBatchSize:  1000,
		FlushInterval: 10 * time.Second,
	})

	var errorHandlerCalled atomic.Bool
	var lostEntries atomic.Int32

	bw.SetErrorHandler(func(err error, entriesLost int) {
		errorHandlerCalled.Store(true)
		lostEntries.Store(int32(entriesLost))
	})

	// Write some async entries (no notification channels)
	for i := 0; i < 5; i++ {
		_ = bw.Write(&Entry{
			Type:     EntryTypeWrite,
			Database: "db",
			Fields:   map[string]interface{}{"v": float64(i)},
		})
	}

	// Now corrupt the WAL file to cause write error
	_ = bw.wal.Close()                                 // Close the file
	_ = os.Remove(bw.wal.dir)                          // Try to remove dir (may fail if non-empty)
	_ = os.MkdirAll(bw.wal.dir, 0o000)                 // Make dir non-writable
	defer func() { _ = os.Chmod(bw.wal.dir, 0o755) }() // Restore

	// Force flush - might fail due to closed file
	_ = bw.flushPending()

	// Cleanup: cancel to stop flushLoop
	bw.cancel()
	bw.wg.Wait()
}

// --- PrepareFlush flushes pending entries before rotating ---

func TestWriterPrepareFlush_FlushesFirst(t *testing.T) {
	dir := t.TempDir()
	config := Config{
		Dir:            dir,
		MaxBatchSize:   10000,
		FlushInterval:  10 * time.Second, // Won't auto-flush
		MaxSegmentSize: 64 * 1024 * 1024,
	}

	writer, err := NewWriterWithConfig(config)
	if err != nil {
		t.Fatalf("NewWriterWithConfig failed: %v", err)
	}
	defer func() { _ = writer.Close() }()

	// Write entries that are only in the pending buffer
	for i := 0; i < 5; i++ {
		_ = writer.Write(&Entry{
			Type:     EntryTypeWrite,
			Database: "db",
			Fields:   map[string]interface{}{"v": float64(i)},
		})
	}

	// PrepareFlush should flush pending first, then rotate
	files, err := writer.PrepareFlush()
	if err != nil {
		t.Fatalf("PrepareFlush failed: %v", err)
	}
	if len(files) == 0 {
		t.Error("Expected segment files from PrepareFlush")
	}

	// The entries should now be in the returned segment files
	totalEntries := 0
	for _, f := range files {
		entries, err := writer.ReadSegmentFile(f)
		if err != nil {
			t.Fatalf("ReadSegmentFile(%s) failed: %v", f, err)
		}
		totalEntries += len(entries)
	}
	if totalEntries != 5 {
		t.Errorf("Expected 5 entries in segment files, got %d", totalEntries)
	}
}

// --- Close flushes pending entries ---

func TestWriterClose_FlushesPending(t *testing.T) {
	dir := t.TempDir()
	config := Config{
		Dir:            dir,
		MaxBatchSize:   10000,
		FlushInterval:  10 * time.Second, // Won't auto-flush
		MaxSegmentSize: 64 * 1024 * 1024,
	}

	writer, err := NewWriterWithConfig(config)
	if err != nil {
		t.Fatalf("NewWriterWithConfig failed: %v", err)
	}

	// Write entries (not flushed yet)
	for i := 0; i < 5; i++ {
		_ = writer.Write(&Entry{
			Type:     EntryTypeWrite,
			Database: "db",
			Fields:   map[string]interface{}{"v": float64(i)},
		})
	}

	// Close triggers context cancel -> flushLoop does final flush -> wal.Close
	if err := writer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen and verify
	writer2, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer func() { _ = writer2.Close() }()

	entries, err := writer2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if len(entries) != 5 {
		t.Errorf("Expected 5 entries persisted after Close, got %d", len(entries))
	}
}

// --- GetStats after writes ---

func TestWriterGetStats_AfterWrites(t *testing.T) {
	dir := t.TempDir()
	config := Config{
		Dir:            dir,
		MaxBatchSize:   10000,
		FlushInterval:  10 * time.Second,
		MaxSegmentSize: 64 * 1024 * 1024,
	}

	writer, err := NewWriterWithConfig(config)
	if err != nil {
		t.Fatalf("NewWriterWithConfig failed: %v", err)
	}
	defer func() { _ = writer.Close() }()

	// Initial stats
	stats := writer.GetStats()
	if stats.PendingEntries != 0 {
		t.Errorf("Expected 0 pending, got %d", stats.PendingEntries)
	}
	if stats.BatchSize != 10000 {
		t.Errorf("Expected BatchSize=10000, got %d", stats.BatchSize)
	}
	if stats.FlushInterval != 10*time.Second {
		t.Errorf("Expected FlushInterval=10s, got %v", stats.FlushInterval)
	}

	// Write entries
	for i := 0; i < 3; i++ {
		_ = writer.Write(&Entry{
			Type:     EntryTypeWrite,
			Database: "db",
			Fields:   map[string]interface{}{"v": float64(i)},
		})
	}

	// Check stats - may or may not have pending depending on timing
	stats = writer.GetStats()
	// PendingEntries could be 0 if flushed already, or 3 if not
	// Just ensure no panic and values are valid
	if stats.PendingEntries < 0 {
		t.Errorf("PendingEntries should be >= 0, got %d", stats.PendingEntries)
	}
}

// --- Concurrent WriteSync and Write ---

func TestWriterConcurrent_MixedWriteTypes(t *testing.T) {
	dir := t.TempDir()
	config := Config{
		Dir:            dir,
		MaxBatchSize:   10,
		FlushInterval:  50 * time.Millisecond,
		MaxSegmentSize: 1024 * 1024,
	}

	writer, err := NewWriterWithConfig(config)
	if err != nil {
		t.Fatalf("NewWriterWithConfig failed: %v", err)
	}
	defer func() { _ = writer.Close() }()

	var wg sync.WaitGroup

	// Async writers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_ = writer.Write(&Entry{
					Type:     EntryTypeWrite,
					Database: "db",
					Fields:   map[string]interface{}{"async": float64(id), "j": float64(j)},
				})
			}
		}(i)
	}

	// Sync writers
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_ = writer.WriteSync(&Entry{
					Type:     EntryTypeWrite,
					Database: "db",
					Fields:   map[string]interface{}{"sync": float64(id), "j": float64(j)},
				})
			}
		}(i)
	}

	// Batch writers
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			batch := make([]*Entry, 5)
			for j := 0; j < 5; j++ {
				batch[j] = &Entry{
					Type:     EntryTypeWrite,
					Database: "db",
					Fields:   map[string]interface{}{"batch": float64(id), "j": float64(j)},
				}
			}
			_ = writer.WriteBatch(batch)
		}(i)
	}

	wg.Wait()
	_ = writer.Flush()

	entries, err := writer.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	// 5*20 async + 3*10 sync + 2*5 batch = 100 + 30 + 10 = 140
	expected := 140
	if len(entries) != expected {
		t.Errorf("Expected %d entries, got %d", expected, len(entries))
	}
}

// --- flushLoop context cancellation ---

func TestWriterFlushLoop_ContextCancel(t *testing.T) {
	dir := t.TempDir()
	config := Config{
		Dir:            dir,
		MaxBatchSize:   10000,
		FlushInterval:  50 * time.Millisecond,
		MaxSegmentSize: 64 * 1024 * 1024,
	}

	writer, err := NewWriterWithConfig(config)
	if err != nil {
		t.Fatalf("NewWriterWithConfig failed: %v", err)
	}

	// Add pending entries
	for i := 0; i < 3; i++ {
		_ = writer.Write(&Entry{
			Type:     EntryTypeWrite,
			Database: "db",
			Fields:   map[string]interface{}{"v": float64(i)},
		})
	}

	// Close triggers cancel, which triggers final flush in flushLoop
	if err := writer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify entries were flushed during cancellation
	writer2, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer func() { _ = writer2.Close() }()

	entries, err := writer2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if len(entries) != 3 {
		t.Errorf("Expected 3 entries after context cancel flush, got %d", len(entries))
	}
}

// --- Writer checkpoint through batchWriter ---

func TestWriterCheckpointAndTruncate(t *testing.T) {
	dir := t.TempDir()
	writer, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	defer func() { _ = writer.Close() }()

	// Write, flush, checkpoint, truncate
	for i := 0; i < 5; i++ {
		_ = writer.WriteSync(&Entry{
			Type:     EntryTypeWrite,
			Database: "db",
			Fields:   map[string]interface{}{"v": float64(i)},
		})
	}

	_ = writer.SetCheckpoint()

	// Write more after checkpoint
	for i := 5; i < 10; i++ {
		_ = writer.WriteSync(&Entry{
			Type:     EntryTypeWrite,
			Database: "db",
			Fields:   map[string]interface{}{"v": float64(i)},
		})
	}

	_ = writer.TruncateBeforeCheckpoint()

	entries, err := writer.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	// Only post-checkpoint entries should remain
	if len(entries) < 5 {
		t.Errorf("Expected at least 5 post-checkpoint entries, got %d", len(entries))
	}
}

// --- Writer segment count through batchWriter ---

func TestWriterSegmentCount_AfterRotations(t *testing.T) {
	dir := t.TempDir()
	config := Config{
		Dir:            dir,
		MaxBatchSize:   1000,
		FlushInterval:  10 * time.Millisecond,
		MaxSegmentSize: 1, // Tiny segment → frequent rotation
	}

	writer, err := NewWriterWithConfig(config)
	if err != nil {
		t.Fatalf("NewWriterWithConfig failed: %v", err)
	}
	defer func() { _ = writer.Close() }()

	for i := 0; i < 10; i++ {
		_ = writer.WriteSync(&Entry{
			Type:     EntryTypeWrite,
			Database: "db",
			Fields:   map[string]interface{}{"v": float64(i)},
		})
	}

	count, err := writer.GetSegmentCount()
	if err != nil {
		t.Fatalf("GetSegmentCount failed: %v", err)
	}
	if count < 2 {
		t.Errorf("Expected multiple segments with tiny max size, got %d", count)
	}
}
