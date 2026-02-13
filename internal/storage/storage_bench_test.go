package storage

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/compression"
	"github.com/soltixdb/soltix/internal/logging"
)

// BenchmarkMemoryStoreWrite benchmarks writing to memory store
func BenchmarkMemoryStoreWrite(b *testing.B) {
	logger := logging.NewDevelopment()
	ms := NewMemoryStore(24*time.Hour, 10000000, logger)
	defer func() { _ = ms.Close() }()

	dp := &DataPoint{
		Database:   "benchmark_db",
		Collection: "test_collection",
		ShardID:    "shard-001",
		Time:       time.Now(),
		ID:         "device-001",
		Fields: map[string]interface{}{
			"temperature": 25.5,
			"humidity":    60.0,
			"voltage":     220.5,
		},
		InsertedAt:  time.Now(),
		FlushStatus: FlushStatusNew,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		dp.Time = time.Now().Add(time.Duration(i) * time.Second)
		dp.ID = fmt.Sprintf("device-%d", i%1000)
		if err := ms.Write(dp); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMemoryStoreWriteParallel benchmarks concurrent writes
func BenchmarkMemoryStoreWriteParallel(b *testing.B) {
	logger := logging.NewDevelopment()
	// Use very large memory limit to avoid eviction during benchmark
	ms := NewMemoryStore(24*time.Hour, 100000000, logger)
	defer func() { _ = ms.Close() }()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		dp := &DataPoint{
			Database:   "benchmark_db",
			Collection: "test_collection",
			ShardID:    "shard-001",
			Time:       time.Now(),
			ID:         "device-001",
			Fields: map[string]interface{}{
				"temperature": 25.5,
				"humidity":    60.0,
			},
			InsertedAt:  time.Now(),
			FlushStatus: FlushStatusNew,
		}

		i := 0
		for pb.Next() {
			dp.Time = time.Now().Add(time.Duration(i) * time.Second)
			dp.ID = fmt.Sprintf("device-%d", i%100)
			if err := ms.Write(dp); err != nil {
				b.Fatal(err)
			}
			i++

			// Periodically mark items as flushed to enable eviction if needed
			if i%10000 == 0 {
				_ = ms.MarkFlushed()
			}
		}
	})
}

// BenchmarkMemoryStoreQuery benchmarks querying data
func BenchmarkMemoryStoreQuery(b *testing.B) {
	logger := logging.NewDevelopment()
	ms := NewMemoryStore(24*time.Hour, 10000000, logger)
	defer func() { _ = ms.Close() }()

	// Pre-populate with data
	baseTime := time.Now()
	for i := 0; i < 10000; i++ {
		dp := &DataPoint{
			Database:   "benchmark_db",
			Collection: "test_collection",
			ShardID:    "shard-001",
			Time:       baseTime.Add(time.Duration(i) * time.Second),
			ID:         fmt.Sprintf("device-%d", i%100),
			Fields: map[string]interface{}{
				"temperature": float64(i) * 0.1,
				"humidity":    float64(i) * 0.2,
			},
			InsertedAt:  time.Now(),
			FlushStatus: FlushStatusNew,
		}
		if err := ms.Write(dp); err != nil {
			b.Fatal(err)
		}
	}

	startTime := baseTime
	endTime := baseTime.Add(5000 * time.Second)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		results, err := ms.Query("benchmark_db", "test_collection", "", startTime, endTime)
		if err != nil {
			b.Fatal(err)
		}
		if len(results) == 0 {
			b.Fatal("Expected results")
		}
	}
}

// BenchmarkMemoryStoreQueryIter benchmarks the streaming iterator pattern
func BenchmarkMemoryStoreQueryIter(b *testing.B) {
	logger := logging.NewDevelopment()
	ms := NewMemoryStore(24*time.Hour, 10000000, logger)
	defer func() { _ = ms.Close() }()

	baseTime := time.Now()
	for i := 0; i < 10000; i++ {
		dp := &DataPoint{
			Database:   "benchmark_db",
			Collection: "test_collection",
			ShardID:    "shard-001",
			Time:       baseTime.Add(time.Duration(i) * time.Second),
			ID:         fmt.Sprintf("device-%d", i%100),
			Fields: map[string]interface{}{
				"temperature": float64(i) * 0.1,
				"humidity":    float64(i) * 0.2,
			},
			InsertedAt:  time.Now(),
			FlushStatus: FlushStatusNew,
		}
		if err := ms.Write(dp); err != nil {
			b.Fatal(err)
		}
	}

	startTime := baseTime
	endTime := baseTime.Add(5000 * time.Second)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		count := 0
		next := ms.QueryIter("benchmark_db", "test_collection", "", startTime, endTime)
		for _, ok := next(); ok; _, ok = next() {
			count++
		}
		if count == 0 {
			b.Fatal("Expected results")
		}
	}
}

// BenchmarkMemoryStoreQueryWithFilters benchmarks filtered queries
func BenchmarkMemoryStoreQueryWithFilters(b *testing.B) {
	logger := logging.NewDevelopment()
	ms := NewMemoryStore(24*time.Hour, 10000000, logger)
	defer func() { _ = ms.Close() }()

	// Pre-populate with data
	baseTime := time.Now()
	for i := 0; i < 10000; i++ {
		dp := &DataPoint{
			Database:   "benchmark_db",
			Collection: "test_collection",
			ShardID:    "shard-001",
			Time:       baseTime.Add(time.Duration(i) * time.Second),
			ID:         fmt.Sprintf("device-%d", i%100),
			Fields: map[string]interface{}{
				"temperature": float64(i) * 0.1,
				"humidity":    float64(i) * 0.2,
				"status":      fmt.Sprintf("status-%d", i%5),
			},
			InsertedAt:  time.Now(),
			FlushStatus: FlushStatusNew,
		}
		if err := ms.Write(dp); err != nil {
			b.Fatal(err)
		}
	}

	startTime := baseTime
	endTime := baseTime.Add(5000 * time.Second)

	b.ResetTimer()
	b.ReportAllocs()

	// Query each device separately
	for i := 0; i < b.N; i++ {
		results := make([]*DataPoint, 0)
		for _, deviceID := range []string{"device-1", "device-5", "device-10"} {
			deviceResults, err := ms.Query("benchmark_db", "test_collection", deviceID, startTime, endTime)
			if err != nil {
				b.Fatal(err)
			}
			results = append(results, deviceResults...)
		}
		if len(results) == 0 {
			b.Fatal("Expected results")
		}
	}
}

// BenchmarkMemoryStoreGetStats benchmarks stats retrieval
func BenchmarkMemoryStoreGetStats(b *testing.B) {
	logger := logging.NewDevelopment()
	ms := NewMemoryStore(24*time.Hour, 10000000, logger)
	defer func() { _ = ms.Close() }()

	// Pre-populate with data
	baseTime := time.Now()
	for i := 0; i < 1000; i++ {
		dp := &DataPoint{
			Database:   "benchmark_db",
			Collection: "test_collection",
			ShardID:    "shard-001",
			Time:       baseTime.Add(time.Duration(i) * time.Second),
			ID:         fmt.Sprintf("device-%d", i%10),
			Fields: map[string]interface{}{
				"temperature": float64(i) * 0.1,
			},
			InsertedAt:  time.Now(),
			FlushStatus: FlushStatusNew,
		}
		if err := ms.Write(dp); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = ms.GetStats()
	}
}

// BenchmarkMemoryStoreGetFlushableBatch benchmarks getting flushable batches
func BenchmarkMemoryStoreGetUnflushed(b *testing.B) {
	logger := logging.NewDevelopment()
	ms := NewMemoryStore(24*time.Hour, 10000000, logger)
	defer func() { _ = ms.Close() }()

	// Pre-populate with data
	baseTime := time.Now().Add(-10 * time.Minute) // Old enough to be flushable
	for i := 0; i < 5000; i++ {
		dp := &DataPoint{
			Database:   "benchmark_db",
			Collection: "test_collection",
			ShardID:    "shard-001",
			Time:       baseTime.Add(time.Duration(i) * time.Second),
			ID:         fmt.Sprintf("device-%d", i%100),
			Fields: map[string]interface{}{
				"temperature": float64(i) * 0.1,
			},
			InsertedAt:  baseTime,
			FlushStatus: FlushStatusNew,
		}
		if err := ms.Write(dp); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		batch, err := ms.GetUnFlushed()
		if err != nil {
			b.Fatal(err)
		}
		if len(batch) == 0 {
			b.Fatal("Expected unflushed data")
		}

		// Reset status for next iteration
		b.StopTimer()
		_ = ms.MarkFlushed()
		// Reset back to New status and re-add to unflushed index
		for _, dp := range batch {
			dp.FlushStatus = FlushStatusNew
		}
		ms.resetUnflushed(batch)
		b.StartTimer()
	}
}

// BenchmarkMemoryStoreMarkFlushed benchmarks marking data as flushed
func BenchmarkMemoryStoreMarkFlushed(b *testing.B) {
	logger := logging.NewDevelopment()
	ms := NewMemoryStore(24*time.Hour, 10000000, logger)
	defer func() { _ = ms.Close() }()

	// Pre-populate with data
	baseTime := time.Now()
	for i := 0; i < 1000; i++ {
		dp := &DataPoint{
			Database:   "benchmark_db",
			Collection: "test_collection",
			ShardID:    "shard-001",
			Time:       baseTime.Add(time.Duration(i) * time.Second),
			ID:         fmt.Sprintf("device-%d", i%10),
			Fields: map[string]interface{}{
				"temperature": float64(i) * 0.1,
			},
			InsertedAt:  baseTime,
			FlushStatus: FlushStatusNew,
		}
		if err := ms.Write(dp); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Get unflushed to mark them as flushing
		b.StopTimer()
		batch, _ := ms.GetUnFlushed()
		b.StartTimer()

		if err := ms.MarkFlushed(); err != nil {
			b.Fatal(err)
		}

		// Reset for next iteration
		b.StopTimer()
		for _, dp := range batch {
			dp.FlushStatus = FlushStatusNew
		}
		ms.resetUnflushed(batch)
		b.StartTimer()
	}
}

// BenchmarkOrderedSliceInsert benchmarks OrderedSlice insert operation
func BenchmarkOrderedSliceInsert(b *testing.B) {
	os := NewOrderedSlice(1000)

	dp := &DataPoint{
		Database:   "benchmark_db",
		Collection: "test_collection",
		ShardID:    "shard-001",
		Time:       time.Now(),
		ID:         "device-001",
		Fields: map[string]interface{}{
			"temperature": 25.5,
		},
		InsertedAt:  time.Now(),
		FlushStatus: FlushStatusNew,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		dp.Time = time.Now().Add(time.Duration(i) * time.Second)
		os.Add(dp)
	}
}

// BenchmarkOrderedSliceQuery benchmarks OrderedSlice query operation
func BenchmarkOrderedSliceQuery(b *testing.B) {
	os := NewOrderedSlice(10000)

	// Pre-populate
	baseTime := time.Now()
	for i := 0; i < 10000; i++ {
		dp := &DataPoint{
			Database:   "benchmark_db",
			Collection: "test_collection",
			ShardID:    "shard-001",
			Time:       baseTime.Add(time.Duration(i) * time.Second),
			ID:         "device-001",
			Fields: map[string]interface{}{
				"temperature": float64(i) * 0.1,
			},
			InsertedAt:  time.Now(),
			FlushStatus: FlushStatusNew,
		}
		os.Add(dp)
	}

	startTime := baseTime.Add(1000 * time.Second)
	endTime := baseTime.Add(5000 * time.Second)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		results := os.Query(startTime, endTime)
		if len(results) == 0 {
			b.Fatal("Expected results")
		}
	}
}

// BenchmarkBloomFilterInsertAndQuery benchmarks bloom filter operations
func BenchmarkBloomFilterInsertAndQuery(b *testing.B) {
	bf := NewBloomFilter(10000, 0.01)

	b.Run("Insert", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			bf.Add(fmt.Sprintf("device-%d", i))
		}
	})

	// Pre-populate for query test
	bf = NewBloomFilter(10000, 0.01)
	for i := 0; i < 10000; i++ {
		bf.Add(fmt.Sprintf("device-%d", i))
	}

	b.Run("Query", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = bf.MightContain(fmt.Sprintf("device-%d", i%10000))
		}
	})
}

// BenchmarkMemoryStoreWriteLarge benchmarks writing large batches
func BenchmarkMemoryStoreWriteLarge(b *testing.B) {
	logger := logging.NewDevelopment()
	// Increase memory limit significantly for large batches
	ms := NewMemoryStore(24*time.Hour, 50000000, logger)
	defer func() { _ = ms.Close() }()

	baseTime := time.Now()
	dataPoints := make([]*DataPoint, 1000)
	for i := 0; i < 1000; i++ {
		dataPoints[i] = &DataPoint{
			Database:   "benchmark_db",
			Collection: "test_collection",
			ShardID:    "shard-001",
			Time:       baseTime.Add(time.Duration(i) * time.Second),
			ID:         fmt.Sprintf("device-%d", i%100),
			Fields: map[string]interface{}{
				"temperature": float64(i) * 0.1,
				"humidity":    float64(i) * 0.2,
				"voltage":     float64(i) * 0.05,
				"power":       float64(i) * 1.5,
			},
			InsertedAt:  baseTime,
			FlushStatus: FlushStatusNew,
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, dp := range dataPoints {
			dp.Time = baseTime.Add(time.Duration(i) * time.Second)
			if err := ms.Write(dp); err != nil {
				b.Fatal(err)
			}
		}

		// Mark as flushed every iteration to avoid accumulation
		if i%10 == 0 {
			_ = ms.MarkFlushed()
		}
	}
}

// BenchmarkMemoryStoreWriteMultipleCollections benchmarks writes to multiple collections
func BenchmarkMemoryStoreWriteMultipleCollections(b *testing.B) {
	logger := logging.NewDevelopment()
	// Increase memory limit for multiple collections
	ms := NewMemoryStore(24*time.Hour, 50000000, logger)
	defer func() { _ = ms.Close() }()

	collections := []string{"collection_1", "collection_2", "collection_3"}
	baseTime := time.Now()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, collection := range collections {
			dp := &DataPoint{
				Database:    "benchmark_db",
				Collection:  collection,
				ShardID:     "shard-001",
				Time:        baseTime.Add(time.Duration(i) * time.Second),
				ID:          fmt.Sprintf("device-%d", i%100),
				Fields:      map[string]interface{}{"value": float64(i)},
				InsertedAt:  baseTime,
				FlushStatus: FlushStatusNew,
			}
			if err := ms.Write(dp); err != nil {
				b.Fatal(err)
			}
		}

		// Mark as flushed periodically
		if i%100 == 0 {
			_ = ms.MarkFlushed()
		}
	}
}

// BenchmarkMemoryStoreRangeQuery benchmarks range queries of different sizes
func BenchmarkMemoryStoreRangeQuery(b *testing.B) {
	logger := logging.NewDevelopment()
	ms := NewMemoryStore(24*time.Hour, 10000000, logger)
	defer func() { _ = ms.Close() }()

	baseTime := time.Now().Add(-1 * time.Hour)
	// Pre-populate with 10K data points
	for i := 0; i < 10000; i++ {
		dp := &DataPoint{
			Database:    "benchmark_db",
			Collection:  "test_collection",
			ShardID:     "shard-001",
			Time:        baseTime.Add(time.Duration(i) * time.Second),
			ID:          fmt.Sprintf("device-%d", i%100),
			Fields:      map[string]interface{}{"temperature": float64(i) * 0.1},
			InsertedAt:  baseTime,
			FlushStatus: FlushStatusNew,
		}
		if err := ms.Write(dp); err != nil {
			b.Fatal(err)
		}

		// Mark some as flushed to enable eviction
		if i > 0 && i%1000 == 0 {
			_ = ms.MarkFlushed()
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		start := baseTime.Add(time.Duration(i%9000) * time.Second)
		end := start.Add(1000 * time.Second)
		results, err := ms.Query("benchmark_db", "test_collection", "", start, end)
		if err != nil {
			b.Fatal(err)
		}
		if len(results) == 0 {
			b.Fatal("Expected results")
		}
	}
}

// BenchmarkMemoryStoreSpecificDeviceQuery benchmarks querying by specific device
func BenchmarkMemoryStoreSpecificDeviceQuery(b *testing.B) {
	logger := logging.NewDevelopment()
	ms := NewMemoryStore(24*time.Hour, 10000000, logger)
	defer func() { _ = ms.Close() }()

	baseTime := time.Now().Add(-1 * time.Hour)
	// Pre-populate with 10K data points for multiple devices
	for i := 0; i < 10000; i++ {
		dp := &DataPoint{
			Database:    "benchmark_db",
			Collection:  "test_collection",
			ShardID:     "shard-001",
			Time:        baseTime.Add(time.Duration(i) * time.Second),
			ID:          fmt.Sprintf("device-%d", i%50),
			Fields:      map[string]interface{}{"temperature": float64(i) * 0.1},
			InsertedAt:  baseTime,
			FlushStatus: FlushStatusNew,
		}
		if err := ms.Write(dp); err != nil {
			b.Fatal(err)
		}

		// Mark some as flushed to enable eviction
		if i > 0 && i%1000 == 0 {
			_ = ms.MarkFlushed()
		}
	}

	startTime := baseTime
	endTime := baseTime.Add(10000 * time.Second)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		deviceID := fmt.Sprintf("device-%d", i%50)
		results, err := ms.Query("benchmark_db", "test_collection", deviceID, startTime, endTime)
		if err != nil {
			b.Fatal(err)
		}
		if len(results) == 0 {
			b.Fatal("Expected results")
		}
	}
}

// BenchmarkV6PartFileReadDecode benchmarks reading and decoding a V6 part file.
// This measures the impact of typed decode (DecodeFloat64/DecodeInt64/DecodeStrings)
// vs the old generic Decode ([]interface{}) path.
func BenchmarkV6PartFileReadDecode(b *testing.B) {
	logger := logging.NewDevelopment()
	ms := NewMemoryStore(24*time.Hour, 100000000, logger)
	defer func() { _ = ms.Close() }()

	comp, _ := compression.GetCompressor(compression.None)
	storage := &Storage{
		logger:     logger,
		compressor: comp,
	}

	// Create test data: 10 devices × 1000 points × 5 fields
	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	numDevices := 10
	numPoints := 1000
	deviceIDs := make([]string, numDevices)
	deviceGroups := make(map[string][]*DataPoint)

	for d := 0; d < numDevices; d++ {
		devID := fmt.Sprintf("device-%03d", d)
		deviceIDs[d] = devID
		points := make([]*DataPoint, numPoints)
		for i := 0; i < numPoints; i++ {
			points[i] = &DataPoint{
				Time: baseTime.Add(time.Duration(i) * time.Second),
				ID:   devID,
				Fields: map[string]interface{}{
					"temperature": float64(20.0 + float64(i)*0.01),
					"humidity":    float64(55.0 + float64(i)*0.02),
					"pressure":    float64(1013.0 + float64(i)*0.001),
					"voltage":     float64(220.0 + float64(i)*0.1),
					"status":      "running",
				},
			}
		}
		deviceGroups[devID] = points
	}

	fieldNames := []string{"temperature", "humidity", "pressure", "voltage", "status"}
	fieldTypes := []compression.ColumnType{
		compression.ColumnTypeFloat64,
		compression.ColumnTypeFloat64,
		compression.ColumnTypeFloat64,
		compression.ColumnTypeFloat64,
		compression.ColumnTypeString,
	}

	// Write V6 part file
	tmpDir := b.TempDir()
	partPath := tmpDir + "/test_part.v6"
	footer, tmpInfo, err := storage.writeV6PartFile(partPath, 0, deviceIDs, deviceGroups, fieldNames, fieldTypes)
	if err != nil {
		b.Fatalf("Failed to write V6 part file: %v", err)
	}
	// Rename tmp to final
	if err := os.Rename(tmpInfo.TmpPath, partPath); err != nil {
		b.Fatalf("Failed to rename: %v", err)
	}

	// Benchmark: read all data from the part file
	startNano := baseTime.Add(-time.Hour).UnixNano()
	endNano := baseTime.Add(24 * time.Hour).UnixNano()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		points, err := storage.readV6PartFileData(partPath, footer, nil, nil, startNano, endNano)
		if err != nil {
			b.Fatalf("Failed to read V6 part file: %v", err)
		}
		if len(points) != numDevices*numPoints {
			b.Fatalf("Expected %d points, got %d", numDevices*numPoints, len(points))
		}
	}
}

// BenchmarkV6PartFileReadDecodeFiltered benchmarks V6 read with time filter
// (only 10% of rows match) — shows the advantage of lazy boxing.
func BenchmarkV6PartFileReadDecodeFiltered(b *testing.B) {
	logger := logging.NewDevelopment()
	ms := NewMemoryStore(24*time.Hour, 100000000, logger)
	defer func() { _ = ms.Close() }()

	comp, _ := compression.GetCompressor(compression.None)
	storage := &Storage{
		logger:     logger,
		compressor: comp,
	}

	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	numDevices := 10
	numPoints := 1000
	deviceIDs := make([]string, numDevices)
	deviceGroups := make(map[string][]*DataPoint)

	for d := 0; d < numDevices; d++ {
		devID := fmt.Sprintf("device-%03d", d)
		deviceIDs[d] = devID
		points := make([]*DataPoint, numPoints)
		for i := 0; i < numPoints; i++ {
			points[i] = &DataPoint{
				Time: baseTime.Add(time.Duration(i) * time.Second),
				ID:   devID,
				Fields: map[string]interface{}{
					"temperature": float64(20.0 + float64(i)*0.01),
					"humidity":    float64(55.0 + float64(i)*0.02),
					"pressure":    float64(1013.0 + float64(i)*0.001),
					"voltage":     float64(220.0 + float64(i)*0.1),
					"status":      "running",
				},
			}
		}
		deviceGroups[devID] = points
	}

	fieldNames := []string{"temperature", "humidity", "pressure", "voltage", "status"}
	fieldTypes := []compression.ColumnType{
		compression.ColumnTypeFloat64,
		compression.ColumnTypeFloat64,
		compression.ColumnTypeFloat64,
		compression.ColumnTypeFloat64,
		compression.ColumnTypeString,
	}

	tmpDir := b.TempDir()
	partPath := tmpDir + "/test_part_filtered.v6"
	footer, tmpInfo, err := storage.writeV6PartFile(partPath, 0, deviceIDs, deviceGroups, fieldNames, fieldTypes)
	if err != nil {
		b.Fatalf("Failed to write V6 part file: %v", err)
	}
	if err := os.Rename(tmpInfo.TmpPath, partPath); err != nil {
		b.Fatalf("Failed to rename: %v", err)
	}

	// Only match 10% of time range (first 100 of 1000 seconds)
	startNano := baseTime.UnixNano()
	endNano := baseTime.Add(100 * time.Second).UnixNano()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		points, err := storage.readV6PartFileData(partPath, footer, nil, nil, startNano, endNano)
		if err != nil {
			b.Fatalf("Failed to read V6 part file: %v", err)
		}
		if len(points) == 0 {
			b.Fatal("Expected some points")
		}
	}
}
