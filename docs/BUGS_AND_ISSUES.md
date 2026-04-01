# Bugs & Issues Found — Code Audit (April 2026)

## 🔴 Critical Issues

### 1. NaN/Inf Propagation in Aggregation Pipeline

**File:** `internal/aggregation/aggregator.go`

`AddValue()` and `Merge()` do not check for NaN or Inf values. If a sensor sends a NaN value (common in IoT when sensor errors), it will poison the entire aggregation chain:

```go
// Current code - no NaN check
func (af *AggregatedField) AddValue(value float64) {
    af.Count++
    af.Sum += value      // NaN + anything = NaN
    af.SumSquares += value * value  // NaN
    // Min/Max comparisons with NaN always return false
    af.Avg = af.Sum / float64(af.Count)  // NaN
}
```

**Impact:** A single NaN from any sensor corrupts hourly → daily → monthly → yearly aggregations permanently.

**Fix:** Add `math.IsNaN(value) || math.IsInf(value, 0)` guard at entry point.

---

### 2. WAL Entry Timestamp Parsing Inconsistency

**File:** `internal/storage/flush_worker_pool.go` → `walEntryToDataPoint()`

```go
func walEntryToDataPoint(entry *wal.Entry) *DataPoint {
    ts := time.Unix(0, entry.Timestamp)  // Uses Timestamp (nanos)
    // ...
}
```

But in `service.go` → `writeToStorageDirect()`:
```go
timeParsed, err := time.Parse(time.RFC3339, msg.Time)  // Uses Time string (RFC3339)
entry.Timestamp = timeParsed.UnixNano()
```

And in `replayWAL()`:
```go
timestamp, parseErr := time.Parse(time.RFC3339Nano, entry.Time)  // Uses Time string (RFC3339Nano)
if parseErr != nil {
    if entry.Timestamp > 0 {
        timestamp = time.Unix(0, entry.Timestamp)  // Fallback to Timestamp
    }
}
```

**Issue:** `walEntryToDataPoint()` ONLY uses `entry.Timestamp` (int64 nanos) and completely ignores `entry.Time` (string). If `Timestamp` is 0 or not set (e.g., from older WAL entries or sync), the data point gets timestamp `1970-01-01`.

**Fix:** `walEntryToDataPoint()` should try `entry.Time` first, fallback to `entry.Timestamp`, matching `replayWAL()` logic.

---

### 3. Aggregation Cascade Inefficiency — Full Range Re-read

**File:** `internal/aggregation/worker_pool.go`

```go
func (p *AggregationWorkerPool) aggregateHourly(worker *PartitionWorker) error {
    // Reads ENTIRE day of raw data on every notification
    dayStart := time.Date(...)
    dayEnd := dayStart.Add(24 * time.Hour)
    dataPoints, err := p.rawReader.Query(..., dayStart, dayEnd, ...)
    // Re-aggregates ALL 24 hours even if only 1 hour changed
}
```

**Impact:** Every flush triggers hourly aggregation that re-reads the entire day. With 100K points/day and flushes every few seconds, this creates massive unnecessary I/O.

**Same pattern in `aggregateDaily()`:** reads entire month, `aggregateMonthly()`: entire year.

**Fix:** Track dirty time buckets and only re-aggregate affected buckets.

---

## 🟡 Data Integrity Issues

### 4. MemoryStore Global Counter Race Condition

**File:** `internal/storage/memory_store.go`

```go
func (ms *MemoryStore) Write(dp *DataPoint) error {
    idx := getShard(dp.Database, dp.Collection, dp.ID)
    s := &ms.shards[idx]
    s.mu.Lock()
    // ... add to shard ...
    s.mu.Unlock()

    // Gap between shard unlock and global lock:
    // Another goroutine could read totalCount/totalSize here
    // and see inconsistent state (data in shard but count not updated)

    dpSize := dp.estimateSize()
    ms.globalMu.Lock()
    ms.totalCount++
    ms.totalSize += dpSize
    ms.globalMu.Unlock()
}
```

**Impact:** Between shard unlock and global lock update, `Count()` and `Size()` report stale values. This can cause delayed eviction triggering. Low severity but worth noting.

---

### 5. FlushWorker pendingCount Reset

**File:** `internal/storage/flush_worker_pool.go`

```go
func (w *FlushWorker) flush() {
    // ... read and process WAL segments ...

    // Reset pending count to 0 UNCONDITIONALLY
    w.mu.Lock()
    w.pendingCount = 0  // BUG: notifications received DURING flush are lost
    w.totalFlushCount++
    w.mu.Unlock()
}
```

**Impact:** If new write notifications arrive while `flush()` is executing (between `PrepareFlushPartition` and the final `pendingCount = 0`), those notifications are silently discarded. The 5-second `checkTicker` in `run()` partially mitigates this but introduces up to 5s delay.

**Fix:** Track pre-flush count, subtract only what was processed: `w.pendingCount -= processedCount`

---

### 6. Anti-Entropy Checksum Too Simple

**File:** `internal/sync/` (documented in SYNC.md)

Checksum format: `"{count}:{minTimestamp}:{maxTimestamp}"`

This misses:
- Data with same count and time range but different values
- Partial overwrites
- Deleted + re-inserted data

**Fix:** Use incremental content hash (xxHash or CRC32 of actual data).

---

## 🟡 Logic Issues in Aggregation

### 7. Hourly Aggregation Reads from TieredStorage (Disk), Not MemoryStore

**File:** `internal/aggregation/worker_pool.go` → `aggregateHourly()`

The `rawReader` is backed by `RawDataReaderAdapter` which reads from `TieredStorageAdapter`. This means hourly aggregation reads from disk AFTER flush, not from the in-memory hot data.

**Problem:** If data was just flushed 1 second ago, the aggregation reads it back from disk. For frequently-updated devices, this is wasteful — the data was just in memory.

**Partial mitigation:** Acceptable because aggregation runs after flush completes, so data is guaranteed on disk. But the disk I/O is unnecessary for hot data.

---

### 8. Variance Calculation Numerical Instability

**File:** `internal/aggregation/aggregator.go`

```go
func (af *AggregatedField) Variance() float64 {
    if af.Count <= 1 {
        return 0
    }
    // Var = E[X²] - (E[X])²  ← numerically unstable!
    return (af.SumSquares / float64(af.Count)) - (af.Avg * af.Avg)
}
```

This is the naive "textbook" formula that suffers from **catastrophic cancellation** when values are large but close together. Example: values [1000000.1, 1000000.2] → E[X²] ≈ 1e12, (E[X])² ≈ 1e12, difference ≈ 0.005 but floating point error dominates.

**Fix:** Use Welford's online algorithm or the two-pass algorithm.

---

### 9. WriteWorker Date Partitioning Uses UTC

**File:** `internal/storage/write_worker.go`

```go
func (p *WriteWorkerPool) Submit(msg WriteMessage) error {
    timeParsed, err := time.Parse(time.RFC3339, msg.Time)
    dateStr := timeParsed.Format("2006-01-02")  // Uses whatever TZ is in RFC3339
    partitionKey := fmt.Sprintf("%s:%s:%s", msg.Database, msg.Collection, dateStr)
}
```

But TieredStorage and aggregation use the configured timezone (e.g., Asia/Tokyo). If a write at `2026-01-15T23:30:00Z` (UTC) arrives, the WriteWorker partitions it as `2026-01-15`, but in JST it's `2026-01-16T08:30:00+09:00` → should be `2026-01-16`.

**Impact:** WAL partition key and storage date directory may not match, causing:
- Flush reads from wrong WAL partition
- Aggregation misses data at day boundaries

**Fix:** Convert to configured timezone before formatting date key.

---

## 🟡 Concurrency Issues

### 10. AggregationWorkerPool.preemptIdleWorker — Lock Ordering Risk

**File:** `internal/aggregation/worker_pool.go`

```go
func (p *AggregationWorkerPool) preemptIdleWorker() {
    p.workersMu.RLock()
    for _, w := range p.workers {
        w.mu.Lock()          // Acquires worker lock while holding pool RLock
        // ...
        w.mu.Unlock()
    }
    p.workersMu.RUnlock()

    if oldestWorker != nil {
        oldestWorker.mu.Lock()  // Re-acquires lock outside pool lock
        // ...
    }
}
```

Meanwhile `dispatchToWorker()`:
```go
func (p *AggregationWorkerPool) dispatchToWorker(notif AggregationNotification) {
    p.workersMu.Lock()       // Acquires pool WLock
    worker.mu.Lock()          // Then worker lock
}
```

**Risk:** `preemptIdleWorker` holds pool RLock + worker Lock. `dispatchToWorker` holds pool WLock + worker Lock. If dispatch blocks waiting for RLock to upgrade to WLock while preempt holds RLock waiting for a worker lock that dispatch holds → potential deadlock.

Actual risk is mitigated because preempt uses RLock (compatible with other RLocks) and dispatch uses WLock. But the lock ordering `pool → worker` vs `pool → worker(different)` should be documented.

---

### 11. AggregationWorkerPool Stop — Close stopCh of Running Workers

**File:** `internal/aggregation/worker_pool.go`

```go
func (p *AggregationWorkerPool) Stop() {
    close(p.stopCh)
    p.workersMu.Lock()
    for _, worker := range p.workers {
        worker.mu.Lock()
        if worker.state == workerRunning {
            close(worker.stopCh)  // Only closes running workers
        }
        worker.mu.Unlock()
    }
    // Workers in workerPending or workerWaitingForJob are NOT stopped
}
```

Workers in `workerPending` state are waiting in `pendingQueueLoop()` which listens on `p.stopCh` — OK.
Workers in `workerWaitingForJob` are in `runWorker()` which listens on both `p.stopCh` AND `worker.stopCh` — OK via `p.stopCh`.

So this is actually fine, but the code comment/intent is misleading.

---

## 🟢 Minor Issues

### 12. Missing Compression Implementations

**File:** `internal/compression/compression.go`

LZ4 and Zstd are defined as `Algorithm` constants but `GetCompressor()` only supports `None` and `Snappy`:
```go
func GetCompressor(algo Algorithm) (Compressor, error) {
    switch algo {
    case None:
        return &NoneCompressor{}, nil
    case Snappy:
        return NewSnappyCompressor(), nil
    default:
        return nil, fmt.Errorf("unsupported compression algorithm: %d", algo)
    }
}
```

### 13. DeletePoints Handler Not Implemented

**File:** `internal/handlers/write.go`
```go
func (h *Handler) DeletePoints(c *fiber.Ctx) error {
    return c.Status(fiber.StatusNotImplemented).JSON(...)
}
```

### 14. No Request Size Limits on Batch Write

**File:** `internal/handlers/write.go` → `WriteBatch()`

No limit on `len(req.Points)`. A client could send millions of points in a single batch, causing OOM on the router.

### 15. trackMetadataAsync Goroutine Leak Potential

**File:** `internal/handlers/write.go`

```go
func (h *Handler) trackMetadataAsync(...) {
    go func() {
        ctx, cancel := context.WithTimeout(context.Background(), utils.MetadataTrackingTimeout)
        defer cancel()
        // etcd calls...
    }()
}
```

Under heavy write load, this spawns unbounded goroutines (one per write request). If etcd is slow, thousands of goroutines can accumulate.

**Fix:** Use a bounded worker pool or semaphore.

---

## Summary Table

| # | Severity | Category | Component | Issue |
|---|----------|----------|-----------|-------|
| 1 | 🔴 Critical | Data corruption | aggregation | NaN/Inf poisons aggregation chain |
| 2 | 🔴 Critical | Data integrity | storage/WAL | Timestamp parsing inconsistency |
| 3 | 🔴 Critical | Performance | aggregation | Full range re-read on every flush |
| 4 | 🟡 Medium | Data integrity | sync | Checksum too simple |
| 5 | 🟡 Medium | Data integrity | storage | FlushWorker pendingCount reset |
| 6 | 🟡 Medium | Data integrity | sync | Anti-entropy not group-aware |
| 7 | 🟡 Medium | Performance | aggregation | Reads disk instead of memory |
| 8 | 🟡 Medium | Correctness | aggregation | Variance numerical instability |
| 9 | 🟡 Medium | Correctness | storage | WriteWorker date partition TZ mismatch |
| 10 | 🟡 Medium | Concurrency | aggregation | Lock ordering documentation |
| 11 | 🟡 Low | Correctness | aggregation | Stop() comment misleading |
| 12 | 🟢 Minor | Feature gap | compression | LZ4/Zstd not implemented |
| 13 | 🟢 Minor | Feature gap | handlers | DeletePoints not implemented |
| 14 | 🟢 Minor | Security | handlers | No batch size limit |
| 15 | 🟢 Minor | Resource leak | handlers | Unbounded goroutines in metadata tracking |
