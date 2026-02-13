package wal

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// BenchmarkWALWrite benchmarks writing entries to WAL
func BenchmarkWALWrite(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "wal-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	maxSegmentSize := int64(64 * 1024 * 1024) // 64MB
	wal, err := newBaseWAL(tempDir, maxSegmentSize)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = wal.Close() }()

	entry := &Entry{
		Type:       EntryTypeWrite,
		Database:   "benchmark_db",
		Collection: "test_collection",
		ShardID:    "shard-001",
		Time:       "2024-01-01T00:00:00Z",
		ID:         "device-001",
		Fields: map[string]interface{}{
			"temperature": 25.5,
			"humidity":    60.0,
			"voltage":     220.5,
		},
		Timestamp: time.Now().UnixNano(),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		entry.Timestamp = time.Now().UnixNano()
		entry.ID = fmt.Sprintf("device-%d", i%1000)
		if err := wal.Write(entry); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWALSync benchmarks WAL sync operations
func BenchmarkWALSync(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "wal-bench-sync-*")
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	maxSegmentSize := int64(64 * 1024 * 1024)
	wal, err := newBaseWAL(tempDir, maxSegmentSize)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = wal.Close() }()

	// Create test entry
	entry := &Entry{
		Type:       EntryTypeWrite,
		Database:   "benchmark_db",
		Collection: "test_collection",
		ShardID:    "shard-001",
		Time:       "2024-01-01T00:00:00Z",
		ID:         "device-001",
		Fields: map[string]interface{}{
			"temperature": 25.5,
		},
		Timestamp: time.Now().UnixNano(),
	}

	b.ResetTimer()
	b.ReportAllocs()

	// Benchmark just writes (Sync is done internally)
	for i := 0; i < b.N; i++ {
		entry.Timestamp = time.Now().UnixNano()
		if err := wal.Write(entry); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWALWriteParallel benchmarks concurrent WAL writes
func BenchmarkWALWriteParallel(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "wal-bench-parallel-*")
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	maxSegmentSize := int64(256 * 1024 * 1024) // 256MB for parallel writes
	wal, err := newBaseWAL(tempDir, maxSegmentSize)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = wal.Close() }()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		entry := &Entry{
			Type:       EntryTypeWrite,
			Database:   "benchmark_db",
			Collection: "test_collection",
			ShardID:    "shard-001",
			Time:       "2024-01-01T00:00:00Z",
			ID:         "device-001",
			Fields: map[string]interface{}{
				"temperature": 25.5,
				"humidity":    60.0,
			},
			Timestamp: time.Now().UnixNano(),
		}

		i := 0
		for pb.Next() {
			entry.Timestamp = time.Now().UnixNano()
			entry.ID = fmt.Sprintf("device-%d", i%1000)
			_ = wal.Write(entry)
			i++
		}
	})
}

// BenchmarkWALWriteLarge benchmarks writing large entries
func BenchmarkWALWriteLarge(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "wal-bench-large-*")
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	maxSegmentSize := int64(256 * 1024 * 1024)
	wal, err := newBaseWAL(tempDir, maxSegmentSize)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = wal.Close() }()

	// Create entry with many fields
	fields := make(map[string]interface{})
	for i := 0; i < 50; i++ {
		fields[fmt.Sprintf("field_%d", i)] = float64(i) * 1.5
	}

	entry := &Entry{
		Type:       EntryTypeWrite,
		Database:   "benchmark_db",
		Collection: "test_collection",
		ShardID:    "shard-001",
		Time:       "2024-01-01T00:00:00Z",
		ID:         "device-001",
		Fields:     fields,
		Timestamp:  time.Now().UnixNano(),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		entry.Timestamp = time.Now().UnixNano()
		entry.ID = fmt.Sprintf("device-%d", i%1000)
		if err := wal.Write(entry); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWALWriteSmall benchmarks writing small entries
func BenchmarkWALWriteSmall(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "wal-bench-small-*")
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	maxSegmentSize := int64(64 * 1024 * 1024)
	wal, err := newBaseWAL(tempDir, maxSegmentSize)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = wal.Close() }()

	entry := &Entry{
		Type:       EntryTypeWrite,
		Database:   "db",
		Collection: "col",
		ShardID:    "shard-1",
		Time:       "2024-01-01T00:00:00Z",
		ID:         "dev-1",
		Fields: map[string]interface{}{
			"temp": 25.5,
		},
		Timestamp: time.Now().UnixNano(),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		entry.Timestamp = time.Now().UnixNano()
		if err := wal.Write(entry); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWALRead benchmarks reading entries from WAL
func BenchmarkWALRead(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "wal-bench-read-*")
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	maxSegmentSize := int64(64 * 1024 * 1024)
	wal, err := newBaseWAL(tempDir, maxSegmentSize)
	if err != nil {
		b.Fatal(err)
	}

	// Pre-populate WAL with entries
	for i := 0; i < 10000; i++ {
		entry := &Entry{
			Type:       EntryTypeWrite,
			Database:   "benchmark_db",
			Collection: "test_collection",
			ShardID:    "shard-001",
			Time:       "2024-01-01T00:00:00Z",
			ID:         fmt.Sprintf("device-%d", i%1000),
			Fields: map[string]interface{}{
				"temperature": 25.5 + float64(i)*0.01,
			},
			Timestamp: time.Now().UnixNano(),
		}
		if err := wal.Write(entry); err != nil {
			b.Fatal(err)
		}
	}

	if err := wal.Close(); err != nil {
		b.Fatal(err)
	}

	// Re-open WAL for reading
	wal, err = newBaseWAL(tempDir, maxSegmentSize)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = wal.Close() }()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		entries, err := wal.ReadAll()
		if err != nil {
			b.Fatal(err)
		}
		if len(entries) == 0 {
			b.Fatal("Expected entries")
		}
	}
}

// BenchmarkWALDelete benchmarks delete entry writing
func BenchmarkWALDelete(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "wal-bench-delete-*")
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	maxSegmentSize := int64(64 * 1024 * 1024)
	wal, err := newBaseWAL(tempDir, maxSegmentSize)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = wal.Close() }()

	entry := &Entry{
		Type:       EntryTypeDelete,
		Database:   "benchmark_db",
		Collection: "test_collection",
		ShardID:    "shard-001",
		ID:         "device-001",
		Timestamp:  time.Now().UnixNano(),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		entry.Timestamp = time.Now().UnixNano()
		entry.ID = fmt.Sprintf("device-%d", i%1000)
		if err := wal.Write(entry); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWALMixedWorkload benchmarks mixed read/write operations
func BenchmarkWALMixedWorkload(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "wal-bench-mixed-*")
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	maxSegmentSize := int64(256 * 1024 * 1024)
	wal, err := newBaseWAL(tempDir, maxSegmentSize)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = wal.Close() }()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		entry := &Entry{
			Type:       EntryTypeWrite,
			Database:   "benchmark_db",
			Collection: "test_collection",
			ShardID:    "shard-001",
			Time:       "2024-01-01T00:00:00Z",
			Fields: map[string]interface{}{
				"temperature": 25.5,
				"humidity":    60.0,
			},
			Timestamp: time.Now().UnixNano(),
		}

		i := 0
		for pb.Next() {
			entry.ID = fmt.Sprintf("device-%d", i%1000)
			entry.Timestamp = time.Now().UnixNano()

			if i%10 == 0 {
				entry.Type = EntryTypeDelete
			} else {
				entry.Type = EntryTypeWrite
			}

			_ = wal.Write(entry)
			i++
		}
	})
}

// BenchmarkWALRecovery benchmarks WAL recovery from disk
func BenchmarkWALRecovery(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tempDir, err := os.MkdirTemp("", "wal-bench-recovery-*")
		if err != nil {
			b.Fatal(err)
		}

		// Create and populate WAL
		maxSegmentSize := int64(64 * 1024 * 1024)
		wal, err := newBaseWAL(tempDir, maxSegmentSize)
		if err != nil {
			b.Fatal(err)
		}

		for j := 0; j < 5000; j++ {
			entry := &Entry{
				Type:       EntryTypeWrite,
				Database:   "benchmark_db",
				Collection: "test_collection",
				ShardID:    "shard-001",
				Time:       "2024-01-01T00:00:00Z",
				ID:         fmt.Sprintf("device-%d", j%1000),
				Fields: map[string]interface{}{
					"temperature": 25.5 + float64(j)*0.01,
				},
				Timestamp: time.Now().UnixNano(),
			}
			_ = wal.Write(entry)
		}
		_ = wal.Close()
		b.StartTimer()

		// Now measure recovery time
		recoveryWAL, err := newBaseWAL(tempDir, maxSegmentSize)
		if err != nil {
			b.Fatal(err)
		}
		_ = recoveryWAL.Close()

		b.StopTimer()
		_ = os.RemoveAll(tempDir)
	}
}
