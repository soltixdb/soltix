# Write-Ahead Log (WAL)

This document describes in detail how Write-Ahead Log works in Soltix.

## Overview

Write-Ahead Log (WAL) is a mechanism that ensures durability of data. Every change is written to WAL before being saved to the main storage, ensuring data is not lost in case of a crash.

### Key Features

- **Group Commit Pattern**: Batching multiple writes to optimize I/O
- **Partitioned Storage**: Partitioning by database/collection/date
- **High Throughput**: ~137ns for async write
- **Zero Data Loss Option**: Support for sync write (~18ms)

## Kiến trúc

```
┌─────────────────────────────────────────────────────────────────┐
│                        Application Layer                        │
└─────────────────────────────────────┬───────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                    PartitionedWriter                            │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  Partition: db1:collection1:2026-01-29                  │    │
│  │  ┌─────────────────────────────────────────────────┐    │    │
│  │  │            batchWriter (Writer)                 │    │    │
│  │  │  ┌───────────┐    ┌─────────────────────────┐   │    │    │
│  │  │  │  Buffer   │───▶│  Flush Loop (10ms)      │   │    │    │
│  │  │  │ (entries) │    │  - Time-based trigger   │   │    │    │
│  │  │  └───────────┘    │  - Size-based trigger   │   │    │    │
│  │  │                   └───────────┬─────────────┘   │    │    │
│  │  │                               │                 │    │    │
│  │  │                               ▼                 │    │    │
│  │  │  ┌─────────────────────────────────────────┐    │    │    │
│  │  │  │           baseWAL                       │    │    │    │
│  │  │  │  ┌────────────────────────────────────┐ │    │    │    │
│  │  │  │  │  Segment Files                     │ │    │    │    │
│  │  │  │  │  wal-1706486400000.log (64MB max)  │ │    │    │    │
│  │  │  │  │  wal-1706486500000.log             │ │    │    │    │
│  │  │  │  └────────────────────────────────────┘ │    │    │    │
│  │  │  └─────────────────────────────────────────┘    │    │    │
│  │  └─────────────────────────────────────────────────┘    │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  Partition: db1:collection2:2026-01-29                  │    │
│  │  ...                                                    │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

## Components

### 1. baseWAL (`wal.go`)

Basic component handling I/O with disk:

```go
type baseWAL struct {
    dir             string      // Directory containing WAL files
    currentFile     *os.File    // Current segment file
    currentSegment  int64       // Segment timestamp (UnixNano)
    currentSize     int64       // Current size
    maxSegmentSize  int64       // Maximum size (default: 64MB)
    checkpointIndex int64       // Checkpoint position
}
```

**Main Functions:**
- `writeWithoutSync()`: Write entry without syncing
- `sync()`: Fsync data to disk
- `rotate()`: Rotate to a new segment file when full
- `SetCheckpoint()`: Mark checkpoint position
- `TruncateBeforeCheckpoint()`: Delete old segments that have been flushed

### 2. batchWriter (`writer.go`)

Wrapper providing batching and group commit:

```go
type batchWriter struct {
    wal            *baseWAL
    batchSize      int           // Max entries before auto-flush (default: 1000)
    flushInterval  time.Duration // Max time before auto-flush (default: 10ms)
    pendingEntries []*Entry      // Buffer containing pending entries
}
```

**Group Commit Flow:**
```
Write() ──▶ Buffer ──▶ Return immediately (137ns)
                  │
                  ▼ (every 10ms or 1000 entries)
            Batch Flush ──▶ Single fsync
```

### 3. PartitionedWriter (`partitioned_wal.go`)

Manages multiple WAL writers by partition:

```go
type partitionedWriter struct {
    baseDir     string
    writers     map[string]Writer  // key: "db:collection:YYYY-MM-DD"
    idleTimeout time.Duration      // Timeout đóng writer idle
}
```

**Directory Structure:**
```
data/wal/
├── database1/
│   ├── collection1/
│   │   ├── 2026-01-28/
│   │   │   ├── wal-1706400000000.log
│   │   │   └── wal-1706486400000.log
│   │   └── 2026-01-29/
│   │       └── wal-1706572800000.log
│   └── collection2/
│       └── ...
└── database2/
    └── ...
```

## Entry Format

Each WAL entry is serialized using Protobuf:

```go
type Entry struct {
    Type       EntryType              // Write = 1, Delete = 2
    Database   string
    Collection string
    ShardID    string
    Time       string                 // Timestamp of data point
    ID         string                 // Device/Entity ID
    Fields     map[string]interface{} // Data fields
    Timestamp  int64                  // WAL entry timestamp (UnixNano)
}
```

**Binary Format on Disk:**
```
┌─────────────┬─────────────┬────────────────────┐
│ Length (4B) │ CRC32 (4B)  │ Protobuf Data      │
└─────────────┴─────────────┴────────────────────┘
```

- **Length**: 4 bytes (uint32, little-endian) - length of Protobuf data
- **CRC32**: 4 bytes (uint32, little-endian) - checksum for integrity verification
- **Data**: Protobuf-encoded Entry

## Write Modes

### 1. Async Write (High Throughput)

```go
writer.Write(entry)  // ~137ns
```

- **Advantages**: Highest throughput
- **Disadvantages**: May lose data within FlushInterval (10ms) if crash occurs
- **Use Case**: Metrics, logs, non-critical data

### 2. Sync Write (Zero Data Loss)

```go
writer.WriteSync(entry)  // ~18ms
```

- **Advantages**: Ensures 100% durability
- **Disadvantages**: Significantly slower
- **Use Case**: Critical data, financial transactions

### 3. Batch Write (Best Balance)

```go
writer.WriteBatch(entries)  // ~18.5ms for 1000 entries
```

- **Advantages**: Amortize fsync cost across multiple entries
- **Disadvantages**: Requires collecting entries first
- **Use Case**: Bulk imports, batch processing

## Flush Loop

Background goroutine that automatically flushes buffer:

```go
func (bw *batchWriter) flushLoop() {
    ticker := time.NewTicker(bw.flushInterval)  // 10ms
    
    for {
        select {
        case <-ticker.C:
            bw.flushPending()  // Time-based flush
            
        case <-bw.flushChan:
            bw.flushPending()  // Size-based flush (when MaxBatchSize reached)
        }
    }
}
```

**Flush Process:**
1. Lock and retrieve all pending entries
2. Write each entry to file (without sync)
3. Fsync once only
4. Notify WriteSync waiters

## Segment Rotation

When segment file reaches `MaxSegmentSize` (default: 64MB):

```go
func (w *baseWAL) rotate() error {
    // 1. Sync and close current file
    w.currentFile.Sync()
    w.currentFile.Close()
    
    // 2. Create new segment with timestamp
    w.currentSegment = time.Now().UnixNano()
    filename := fmt.Sprintf("wal-%d.log", w.currentSegment)
    
    // 3. Open new file
    w.currentFile = os.OpenFile(filename, O_CREATE|O_WRONLY|O_APPEND)
    w.currentSize = 0
}
```

## Checkpoint & Truncate

Mechanism to delete WAL entries that have been flushed to main storage:

### 1. SetCheckpoint

Mark current position and rotate to new segment:

```go
func (w *baseWAL) SetCheckpoint() error {
    // Remember current segment
    w.checkpointIndex = w.currentSegment
    
    // Rotate so new writes go to new segment
    return w.rotate()
}
```

### 2. TruncateBeforeCheckpoint

Delete old segments that have been flushed:

```go
func (w *baseWAL) TruncateBeforeCheckpoint() error {
    // Delete all segments with timestamp <= checkpoint
    for each segmentFile {
        if segNum <= w.checkpointIndex {
            os.Remove(segmentFile)
        }
    }
}
```

### Complete Flow:

```
1. Write entries ──▶ wal-100.log (active)

2. SetCheckpoint() 
   - Remember checkpoint = 100
   - Rotate ──▶ wal-200.log (new active)

3. Flush WAL entries from wal-100.log to storage

4. TruncateBeforeCheckpoint()
   - Delete wal-100.log (flushed)
   - Keep wal-200.log (new entries)
```

## PrepareFlush Pattern

Safer pattern for flush process:

```go
// 1. Prepare flush - rotate and get list of old files
oldFiles, _ := writer.PrepareFlush()
// Returns: ["wal-100.log", "wal-150.log"]

// 2. Read and process each file
for _, file := range oldFiles {
    entries, _ := writer.ReadSegmentFile(file)
    // Process entries...
}

// 3. Delete processed files
writer.RemoveSegmentFiles(oldFiles)
```

**Advantages:**
- New writes are not affected (already rotated)
- Can retry if flush fails
- No data loss

## Partitioned WAL Operations

### WritePartitioned

```go
result, err := pw.WritePartitioned(entry, database, date)
if result.IsNewSegment {
    // This is the first write after PrepareFlush
    // Need to notify flush system
}
```

### ProcessPartitions

Utility to read and cleanup partitions:

```go
pw.ProcessPartitions(func(partition PartitionInfo, entries []*Entry) error {
    // Process entries
    return storage.Write(entries)
})
```

## Configuration

### Default Config

```go
Config{
    Dir:            "/path/to/wal",
    MaxSegmentSize: 64 * 1024 * 1024,  // 64MB
    MaxBatchSize:   1000,               // entries
    FlushInterval:  10 * time.Millisecond,
}
```

### Partitioned Config

```go
PartitionedConfig{
    BaseDir:     "/path/to/wal",
    Config:      DefaultConfig,
    IdleTimeout: 10 * time.Minute,  // Close idle writers after 10 minutes
}
```

## Performance Characteristics

| Operation | Latency | Throughput |
|-----------|---------|------------|
| Async Write | ~137ns | >7M ops/s |
| Sync Write | ~18ms | ~55 ops/s |
| Batch Write (1000) | ~18.5ms | ~54K entries/s |

### Tuning Tips

1. **High Throughput**: Use `Write()` with `FlushInterval = 10ms`
2. **Low Latency**: Reduce `FlushInterval` (trade-off with throughput)
3. **Bulk Load**: Use `WriteBatch()` with large batches
4. **Memory**: Adjust `MaxBatchSize` based on available memory

## Recovery

On startup, read and replay all WAL entries:

```go
entries, err := writer.ReadAll()
for _, entry := range entries {
    switch entry.Type {
    case EntryTypeWrite:
        storage.Write(entry)
    case EntryTypeDelete:
        storage.Delete(entry)
    }
}
```

## Error Handling

- **CRC32 Mismatch**: Entry is corrupted, skip or abort
- **Partial Write**: Last entry may be incomplete, skip
- **Directory Errors**: Automatically create directory if missing

## Best Practices

1. **Always call `Close()`**: Ensure all pending entries are flushed
2. **Truncate periodically**: Avoid running out of disk space
3. **Monitor segment count**: Alert if too many segments are not flushed
4. **Backup before truncate**: If audit trail is needed

