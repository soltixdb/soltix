package wal

import (
	"testing"
	"time"
)

// BenchmarkWriter_Write tests async write performance (batched)
func BenchmarkWriter_Write(b *testing.B) {
	tempDir := b.TempDir()

	writer, err := NewWriter(tempDir)
	if err != nil {
		b.Fatalf("Failed to create writer: %v", err)
	}
	defer func() { _ = writer.Close() }()

	entry := &Entry{
		Type:       EntryTypeWrite,
		Database:   "testdb",
		Collection: "testcoll",
		ShardID:    "shard1",
		Time:       time.Now().Format(time.RFC3339),
		ID:         "device1",
		Fields:     map[string]interface{}{"temp": 25.5, "humidity": 60.0},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := writer.Write(entry); err != nil {
			b.Fatalf("Write failed: %v", err)
		}
	}

	b.StopTimer()
	// Flush remaining entries before closing
	_ = writer.Flush()
}

// BenchmarkWriter_WriteSync tests synchronous write performance
func BenchmarkWriter_WriteSync(b *testing.B) {
	tempDir := b.TempDir()

	writer, err := NewWriter(tempDir)
	if err != nil {
		b.Fatalf("Failed to create writer: %v", err)
	}
	defer func() { _ = writer.Close() }()

	entry := &Entry{
		Type:       EntryTypeWrite,
		Database:   "testdb",
		Collection: "testcoll",
		ShardID:    "shard1",
		Time:       time.Now().Format(time.RFC3339),
		ID:         "device1",
		Fields:     map[string]interface{}{"temp": 25.5, "humidity": 60.0},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := writer.WriteSync(entry); err != nil {
			b.Fatalf("WriteSync failed: %v", err)
		}
	}
}

// BenchmarkWriter_WriteBatch tests batch write performance
func BenchmarkWriter_WriteBatch(b *testing.B) {
	tempDir := b.TempDir()

	writer, err := NewWriter(tempDir)
	if err != nil {
		b.Fatalf("Failed to create writer: %v", err)
	}
	defer func() { _ = writer.Close() }()

	// Prepare batch of 1000 entries
	batchSize := 1000
	entries := make([]*Entry, batchSize)
	for i := 0; i < batchSize; i++ {
		entries[i] = &Entry{
			Type:       EntryTypeWrite,
			Database:   "testdb",
			Collection: "testcoll",
			ShardID:    "shard1",
			Time:       time.Now().Format(time.RFC3339),
			ID:         "device1",
			Fields:     map[string]interface{}{"temp": 25.5, "humidity": 60.0},
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := writer.WriteBatch(entries); err != nil {
			b.Fatalf("WriteBatch failed: %v", err)
		}
	}
}

// BenchmarkWriter_Comparison compares different write modes
func BenchmarkWriter_Comparison(b *testing.B) {
	benchmarks := []struct {
		name      string
		writeFunc func(writer Writer, entry *Entry) error
	}{
		{"Async", func(w Writer, e *Entry) error { return w.Write(e) }},
		{"Sync", func(w Writer, e *Entry) error { return w.WriteSync(e) }},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			tempDir := b.TempDir()

			writer, err := NewWriter(tempDir)
			if err != nil {
				b.Fatalf("Failed to create writer: %v", err)
			}
			defer func() { _ = writer.Close() }()

			entry := &Entry{
				Type:       EntryTypeWrite,
				Database:   "testdb",
				Collection: "testcoll",
				ShardID:    "shard1",
				Time:       time.Now().Format(time.RFC3339),
				ID:         "device1",
				Fields:     map[string]interface{}{"temp": 25.5},
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				if err := bm.writeFunc(writer, entry); err != nil {
					b.Fatalf("Write failed: %v", err)
				}
			}

			b.StopTimer()
			if bm.name == "Async" {
				_ = writer.Flush() // Ensure all async writes are flushed
			}
		})
	}
}

// BenchmarkWriter_HighThroughput simulates high-throughput scenario
func BenchmarkWriter_HighThroughput(b *testing.B) {
	tempDir := b.TempDir()

	writer, err := NewWriter(tempDir)
	if err != nil {
		b.Fatalf("Failed to create writer: %v", err)
	}
	defer func() { _ = writer.Close() }()

	entry := &Entry{
		Type:       EntryTypeWrite,
		Database:   "testdb",
		Collection: "testcoll",
		ShardID:    "shard1",
		Time:       time.Now().Format(time.RFC3339),
		ID:         "device1",
		Fields:     map[string]interface{}{"temp": 25.5},
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.SetParallelism(10) // Simulate 10 goroutines

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := writer.Write(entry); err != nil {
				b.Fatalf("Write failed: %v", err)
			}
		}
	})

	b.StopTimer()
	_ = writer.Flush()
}

// BenchmarkWriter_VsBaseWAL compares new Writer vs old baseWAL
func BenchmarkWriter_VsBaseWAL(b *testing.B) {
	entry := &Entry{
		Type:       EntryTypeWrite,
		Database:   "testdb",
		Collection: "testcoll",
		ShardID:    "shard1",
		Time:       time.Now().Format(time.RFC3339),
		ID:         "device1",
		Fields:     map[string]interface{}{"temp": 25.5},
	}

	b.Run("BaseWAL_Sync", func(b *testing.B) {
		tempDir := b.TempDir()

		wal, err := newBaseWAL(tempDir, 64*1024*1024)
		if err != nil {
			b.Fatalf("Failed to create baseWAL: %v", err)
		}
		defer func() { _ = wal.Close() }()

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			if err := wal.Write(entry); err != nil {
				b.Fatalf("Write failed: %v", err)
			}
		}
	})

	b.Run("Writer_Async", func(b *testing.B) {
		tempDir := b.TempDir()

		writer, err := NewWriter(tempDir)
		if err != nil {
			b.Fatalf("Failed to create writer: %v", err)
		}
		defer func() { _ = writer.Close() }()

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			if err := writer.Write(entry); err != nil {
				b.Fatalf("Write failed: %v", err)
			}
		}

		b.StopTimer()
		_ = writer.Flush()
	})
}

// BenchmarkWriter_BatchSizes tests different batch sizes
func BenchmarkWriter_BatchSizes(b *testing.B) {
	sizes := []int{10, 100, 1000, 5000}

	for _, size := range sizes {
		b.Run(string(rune(size)), func(b *testing.B) {
			tempDir := b.TempDir()

			config := Config{
				Dir:            tempDir,
				MaxSegmentSize: 64 * 1024 * 1024,
				MaxBatchSize:   size,
				FlushInterval:  10 * time.Millisecond,
			}

			writer, err := NewWriterWithConfig(config)
			if err != nil {
				b.Fatalf("Failed to create writer: %v", err)
			}
			defer func() { _ = writer.Close() }()

			entry := &Entry{
				Type:       EntryTypeWrite,
				Database:   "testdb",
				Collection: "testcoll",
				ShardID:    "shard1",
				Time:       time.Now().Format(time.RFC3339),
				ID:         "device1",
				Fields:     map[string]interface{}{"temp": 25.5},
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				if err := writer.Write(entry); err != nil {
					b.Fatalf("Write failed: %v", err)
				}
			}

			b.StopTimer()
			_ = writer.Flush()
		})
	}
}

// BenchmarkWriter_FlushIntervals tests different flush intervals
func BenchmarkWriter_FlushIntervals(b *testing.B) {
	intervals := []time.Duration{
		1 * time.Millisecond,
		5 * time.Millisecond,
		10 * time.Millisecond,
		50 * time.Millisecond,
	}

	for _, interval := range intervals {
		b.Run(interval.String(), func(b *testing.B) {
			tempDir := b.TempDir()

			config := Config{
				Dir:            tempDir,
				MaxSegmentSize: 64 * 1024 * 1024,
				MaxBatchSize:   1000,
				FlushInterval:  interval,
			}

			writer, err := NewWriterWithConfig(config)
			if err != nil {
				b.Fatalf("Failed to create writer: %v", err)
			}
			defer func() { _ = writer.Close() }()

			entry := &Entry{
				Type:       EntryTypeWrite,
				Database:   "testdb",
				Collection: "testcoll",
				ShardID:    "shard1",
				Time:       time.Now().Format(time.RFC3339),
				ID:         "device1",
				Fields:     map[string]interface{}{"temp": 25.5},
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				if err := writer.Write(entry); err != nil {
					b.Fatalf("Write failed: %v", err)
				}
			}

			b.StopTimer()
			_ = writer.Flush()
		})
	}
}
