package storage

import (
	"hash/fnv"
	"sort"
	"sync"
	"time"

	"github.com/soltixdb/soltix/internal/logging"
)

// FlushStatus represents the flush state of a data point
type FlushStatus int

const (
	FlushStatusNew      FlushStatus = 0 // New record, not yet flushed
	FlushStatusFlushing FlushStatus = 1 // Currently being flushed
	FlushStatusFlushed  FlushStatus = 2 // Successfully flushed to disk
)

// Size estimation constants for memory usage calculation
const (
	// EstimatedBytesPerField is the average size in bytes for a field value
	// Accounts for: key string (~20 bytes) + interface{} overhead (~16 bytes) + value (~64 bytes avg)
	EstimatedBytesPerField int64 = 100

	// DataPointStructOverhead is the fixed overhead for DataPoint struct fields
	// Includes: Time (24 bytes) + InsertedAt (24 bytes) + FlushStatus (8 bytes) + pointers (~44 bytes)
	DataPointStructOverhead int64 = 100
)

// DataPoint represents a single time-series data point
type DataPoint struct {
	Database    string
	Collection  string
	ShardID     string
	GroupID     int
	Time        time.Time
	ID          string
	Fields      map[string]interface{}
	InsertedAt  time.Time
	FlushStatus FlushStatus // Track flush state: 0=New, 1=Flushing, 2=Flushed
}

// Implement aggregation.DataPointInterface
func (dp *DataPoint) GetID() string                     { return dp.ID }
func (dp *DataPoint) GetDatabase() string               { return dp.Database }
func (dp *DataPoint) GetCollection() string             { return dp.Collection }
func (dp *DataPoint) GetTime() time.Time                { return dp.Time }
func (dp *DataPoint) GetFields() map[string]interface{} { return dp.Fields }

// numShards is the number of lock shards for concurrent write scalability.
// 64 shards provide a good balance between reduced contention and memory overhead.
// With 64 shards, up to 64 goroutines can write concurrently without blocking
// (assuming they hash to different shards).
const numShards = 64

// shard is one partition of the MemoryStore's data, with its own mutex.
// Each shard independently manages a subset of devices based on FNV hash of deviceID.
type shard struct {
	mu sync.RWMutex
	// Nested structure: database -> collection -> deviceID -> ordered slice
	data      map[string]map[string]map[string]*OrderedSlice
	unflushed []*DataPoint // per-shard unflushed index
}

// MemoryStore is an in-memory time-series data store with sharded locking.
// Data is partitioned across 64 shards by FNV hash of deviceID for concurrent writes.
type MemoryStore struct {
	shards [numShards]shard

	maxAge  time.Duration
	maxSize int
	logger  *logging.Logger

	// Global counters — accessed atomically via global lock
	globalMu     sync.Mutex
	totalCount   int64
	totalSize    int64
	evicting     bool
	evictionCh   chan struct{}
	stopCh       chan struct{}
	cleanupDone  chan struct{}
	evictionDone chan struct{}
}

// getShard returns the shard index for a composite key of database, collection,
// and deviceID using FNV-1a hash. Hashing all three fields ensures that the same
// deviceID in different databases or collections maps to different shards,
// providing better load distribution.
func getShard(database, collection, deviceID string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(database))
	_, _ = h.Write([]byte{'/'})
	_, _ = h.Write([]byte(collection))
	_, _ = h.Write([]byte{'/'})
	_, _ = h.Write([]byte(deviceID))
	return h.Sum32() % numShards
}

// NewMemoryStore creates a new in-memory store with sharded locking
func NewMemoryStore(maxAge time.Duration, maxSize int, logger *logging.Logger) *MemoryStore {
	ms := &MemoryStore{
		maxAge:       maxAge,
		maxSize:      maxSize,
		logger:       logger,
		evictionCh:   make(chan struct{}, 1),
		stopCh:       make(chan struct{}),
		cleanupDone:  make(chan struct{}),
		evictionDone: make(chan struct{}),
	}

	// Initialize all shards
	for i := range ms.shards {
		ms.shards[i].data = make(map[string]map[string]map[string]*OrderedSlice)
		ms.shards[i].unflushed = make([]*DataPoint, 0, 16)
	}

	// Start background cleanup goroutine
	go ms.cleanupLoop()

	// Start background eviction goroutine
	go ms.evictionLoop()

	logger.Info("Memory store initialized",
		"max_age", maxAge,
		"max_size", maxSize,
		"num_shards", numShards)

	return ms
}

// Write adds a data point to the store.
// Uses shard-level locking: only the shard for this deviceID is locked,
// allowing concurrent writes to different devices without contention.
func (ms *MemoryStore) Write(dp *DataPoint) error {
	idx := getShard(dp.Database, dp.Collection, dp.ID)
	s := &ms.shards[idx]

	s.mu.Lock()

	// Get or create database level
	dbData, exists := s.data[dp.Database]
	if !exists {
		dbData = make(map[string]map[string]*OrderedSlice)
		s.data[dp.Database] = dbData
	}

	// Get or create collection level
	collData, exists := dbData[dp.Collection]
	if !exists {
		collData = make(map[string]*OrderedSlice)
		dbData[dp.Collection] = collData
	}

	// Get or create device ordered slice
	deviceData, exists := collData[dp.ID]
	if !exists {
		deviceData = NewOrderedSlice(100)
		collData[dp.ID] = deviceData
	}

	// Set flush status for new record
	dp.FlushStatus = FlushStatusNew

	// Add data point (maintains sorted order)
	deviceData.Add(dp)

	// Track in per-shard unflushed index
	s.unflushed = append(s.unflushed, dp)

	s.mu.Unlock()

	// Update global counters (separate lock to minimize shard lock hold time)
	dpSize := dp.estimateSize()
	ms.globalMu.Lock()
	ms.totalCount++
	ms.totalSize += dpSize
	needEviction := ms.totalCount > int64(ms.maxSize)
	ms.globalMu.Unlock()

	// Check if we need to evict old data (non-blocking trigger)
	if needEviction {
		select {
		case ms.evictionCh <- struct{}{}:
		default:
		}
	}

	return nil
}

// GetUnFlushed returns data points with status=New and marks them as Flushing.
// Drains each shard's unflushed index for O(unflushed) total complexity.
func (ms *MemoryStore) GetUnFlushed() ([]*DataPoint, error) {
	// Single pass: drain all shards, collecting directly into result
	result := make([]*DataPoint, 0, 256)

	for i := range ms.shards {
		s := &ms.shards[i]
		s.mu.Lock()

		if len(s.unflushed) > 0 {
			for _, dp := range s.unflushed {
				dp.FlushStatus = FlushStatusFlushing
			}
			result = append(result, s.unflushed...)
			s.unflushed = make([]*DataPoint, 0, len(s.unflushed))
		}

		s.mu.Unlock()
	}

	if len(result) == 0 {
		return nil, nil
	}

	ms.logger.Debug("Retrieved unFlushed entries and marked as flushing",
		"count", len(result))
	return result, nil
}

// MarkFlushed marks all Flushing entries as Flushed in-place with zero allocations.
// Iterates all shards; each shard locked independently.
func (ms *MemoryStore) MarkFlushed() error {
	for i := range ms.shards {
		s := &ms.shards[i]
		s.mu.Lock()
		for _, dbData := range s.data {
			for _, collData := range dbData {
				for _, deviceSlice := range collData {
					deviceSlice.UpdateStatus(FlushStatusFlushing, FlushStatusFlushed)
				}
			}
		}
		s.mu.Unlock()
	}
	return nil
}

// Size returns approximate memory size in bytes
func (ms *MemoryStore) Size() int64 {
	ms.globalMu.Lock()
	v := ms.totalSize
	ms.globalMu.Unlock()
	return v
}

// Count returns total number of data points
func (ms *MemoryStore) Count() int64 {
	ms.globalMu.Lock()
	v := ms.totalCount
	ms.globalMu.Unlock()
	return v
}

// GetStats returns memory store statistics
func (ms *MemoryStore) GetStats() map[string]interface{} {
	type dbCollKey struct {
		db   string
		coll string
	}

	dbSet := make(map[string]struct{})
	collSet := make(map[dbCollKey]struct{})
	deviceCount := 0

	for i := range ms.shards {
		s := &ms.shards[i]
		s.mu.RLock()
		for dbName, dbData := range s.data {
			dbSet[dbName] = struct{}{}
			for collName, collData := range dbData {
				collSet[dbCollKey{dbName, collName}] = struct{}{}
				deviceCount += len(collData)
			}
		}
		s.mu.RUnlock()
	}

	ms.globalMu.Lock()
	totalCount := ms.totalCount
	totalSize := ms.totalSize
	ms.globalMu.Unlock()

	return map[string]interface{}{
		"total_count":      totalCount,
		"total_size_mb":    float64(totalSize) / (1024 * 1024),
		"database_count":   len(dbSet),
		"collection_count": len(collSet),
		"device_count":     deviceCount,
		"max_age":          ms.maxAge.String(),
		"max_size":         ms.maxSize,
	}
}

// cleanupLoop periodically removes expired data
func (ms *MemoryStore) cleanupLoop() {
	defer close(ms.cleanupDone)

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ms.stopCh:
			ms.logger.Debug("Cleanup goroutine stopping")
			return
		case <-ticker.C:
			ms.cleanup()
		}
	}
}

// evictionLoop handles background eviction triggered by Write()
func (ms *MemoryStore) evictionLoop() {
	defer close(ms.evictionDone)

	for {
		select {
		case <-ms.stopCh:
			ms.logger.Debug("Eviction goroutine stopping")
			return
		case <-ms.evictionCh:
			ms.evictOldest()
		}
	}
}

// cleanup removes data older than maxAge across all shards
func (ms *MemoryStore) cleanup() {
	if ms.maxAge == 0 {
		return
	}

	now := time.Now()
	cutoff := now.Add(-ms.maxAge)

	removedCount := int64(0)
	removedSize := int64(0)

	for i := range ms.shards {
		s := &ms.shards[i]
		s.mu.Lock()

		for dbName, dbData := range s.data {
			for collName, collData := range dbData {
				for deviceID, deviceSlice := range collData {
					count, size := deviceSlice.RemoveFlushedBefore(cutoff)
					removedCount += count
					removedSize += size

					if deviceSlice.Len() == 0 {
						delete(collData, deviceID)
					}
				}
				if len(collData) == 0 {
					delete(dbData, collName)
				}
			}
			if len(dbData) == 0 {
				delete(s.data, dbName)
			}
		}

		s.mu.Unlock()
	}

	if removedCount > 0 {
		ms.globalMu.Lock()
		ms.totalCount -= removedCount
		ms.totalSize -= removedSize
		ms.globalMu.Unlock()

		ms.logger.Debug("Cleaned up expired data",
			"removed_count", removedCount,
			"removed_size_mb", float64(removedSize)/(1024*1024))
	}
}

// evictOldest removes oldest FLUSHED data points when maxSize is reached.
// Locks shards sequentially to collect flushed items, then removes them.
func (ms *MemoryStore) evictOldest() {
	ms.globalMu.Lock()
	if ms.evicting {
		ms.globalMu.Unlock()
		ms.logger.Debug("Eviction already in progress, skipping")
		return
	}

	toRemove := ms.totalCount - int64(ms.maxSize)
	if toRemove <= 0 {
		ms.globalMu.Unlock()
		return
	}
	ms.evicting = true
	ms.globalMu.Unlock()

	defer func() {
		ms.globalMu.Lock()
		ms.evicting = false
		ms.globalMu.Unlock()
	}()

	ms.logger.Warn("Memory store size limit reached, evicting oldest flushed data",
		"to_remove", toRemove)

	// Step 1: Collect flushed items from all shards
	type flushedItem struct {
		point      *DataPoint
		database   string
		collection string
		deviceID   string
		shardIdx   int
	}

	flushedItems := make([]flushedItem, 0, toRemove*2)

	for i := range ms.shards {
		s := &ms.shards[i]
		s.mu.RLock()
		for dbName, dbData := range s.data {
			for collName, collData := range dbData {
				for deviceID, deviceSlice := range collData {
					points := deviceSlice.FilterByStatus(FlushStatusFlushed)
					for _, point := range points {
						flushedItems = append(flushedItems, flushedItem{
							point:      point,
							database:   dbName,
							collection: collName,
							deviceID:   deviceID,
							shardIdx:   i,
						})
					}
				}
			}
		}
		s.mu.RUnlock()
	}

	if len(flushedItems) == 0 {
		ms.logger.Warn("No flushed items available to evict")
		return
	}

	if int64(len(flushedItems)) < toRemove {
		ms.logger.Warn("Not enough flushed items to evict",
			"requested", toRemove,
			"available_flushed", len(flushedItems))
		toRemove = int64(len(flushedItems))
	}

	// Step 2: Sort by time (oldest first)
	sort.Slice(flushedItems, func(i, j int) bool {
		return flushedItems[i].point.Time.Before(flushedItems[j].point.Time)
	})

	// Step 3: Group items to remove by shard
	type removalKey struct {
		database   string
		collection string
		deviceID   string
	}

	// shardRemovals[shardIdx] -> map[key] -> timestamps
	shardRemovals := make([]map[removalKey]map[time.Time]bool, numShards)
	removedCount := int64(0)
	removedSize := int64(0)

	for idx := 0; idx < int(toRemove) && idx < len(flushedItems); idx++ {
		item := flushedItems[idx]
		if shardRemovals[item.shardIdx] == nil {
			shardRemovals[item.shardIdx] = make(map[removalKey]map[time.Time]bool)
		}
		key := removalKey{item.database, item.collection, item.deviceID}
		if shardRemovals[item.shardIdx][key] == nil {
			shardRemovals[item.shardIdx][key] = make(map[time.Time]bool)
		}
		shardRemovals[item.shardIdx][key][item.point.Time] = true
		removedCount++
		removedSize += item.point.estimateSize()
	}

	// Step 4: Apply removals per shard
	affectedDevices := 0
	for i := range ms.shards {
		if shardRemovals[i] == nil {
			continue
		}
		s := &ms.shards[i]
		s.mu.Lock()
		for key, timestamps := range shardRemovals[i] {
			dbData := s.data[key.database]
			if dbData == nil {
				continue
			}
			collData := dbData[key.collection]
			if collData == nil {
				continue
			}
			deviceSlice := collData[key.deviceID]
			if deviceSlice == nil {
				continue
			}

			deviceSlice.RemoveByTimestamps(timestamps)
			affectedDevices++

			if deviceSlice.Len() == 0 {
				delete(collData, key.deviceID)
			}
			if len(collData) == 0 {
				delete(dbData, key.collection)
			}
			if len(dbData) == 0 {
				delete(s.data, key.database)
			}
		}
		s.mu.Unlock()
	}

	ms.globalMu.Lock()
	ms.totalCount -= removedCount
	ms.totalSize -= removedSize
	ms.globalMu.Unlock()

	ms.logger.Info("Evicted oldest flushed data points",
		"removed_count", removedCount,
		"removed_size_mb", float64(removedSize)/(1024*1024),
		"affected_devices", affectedDevices)
}

// estimateSize returns approximate size of a data point in bytes
func (dp *DataPoint) estimateSize() int64 {
	// Calculate size of string fields (UTF-8 encoded)
	size := int64(len(dp.Database) + len(dp.Collection) + len(dp.ShardID) + len(dp.ID))
	// Add estimated size for each field in the map
	size += int64(len(dp.Fields)) * EstimatedBytesPerField
	// Add fixed struct overhead (Time, InsertedAt, FlushStatus, pointers)
	size += DataPointStructOverhead
	return size
}

// Query retrieves data points for specific database, collection, and optional device ID within a time range.
// When deviceID is empty, queries all devices with optimizations:
//   - Pre-allocated result capacity based on estimated device count
//   - Skips devices whose time bounds don't overlap the query range (via OrderedSlice.Overlaps)
//   - Merges results from all shards that contain matching devices
func (ms *MemoryStore) Query(database, collection, deviceID string, startTime, endTime time.Time) ([]*DataPoint, error) {
	// If deviceID specified, go directly to the correct shard
	if deviceID != "" {
		idx := getShard(database, collection, deviceID)
		s := &ms.shards[idx]
		s.mu.RLock()
		defer s.mu.RUnlock()

		dbData, exists := s.data[database]
		if !exists {
			return make([]*DataPoint, 0), nil
		}
		collData, exists := dbData[collection]
		if !exists {
			return make([]*DataPoint, 0), nil
		}
		deviceSlice, exists := collData[deviceID]
		if !exists {
			return make([]*DataPoint, 0), nil
		}
		return deviceSlice.Query(startTime, endTime), nil
	}

	// Query all devices across all shards with pre-allocation and time-bounds skip
	// First pass: estimate result capacity
	estimatedCap := 0
	for i := range ms.shards {
		s := &ms.shards[i]
		s.mu.RLock()
		dbData, exists := s.data[database]
		if exists {
			if collData, ok := dbData[collection]; ok {
				estimatedCap += len(collData) * 50 // avg ~50 points per device
			}
		}
		s.mu.RUnlock()
	}
	if estimatedCap == 0 {
		return make([]*DataPoint, 0), nil
	}

	result := make([]*DataPoint, 0, estimatedCap)

	for i := range ms.shards {
		s := &ms.shards[i]
		s.mu.RLock()

		dbData, exists := s.data[database]
		if !exists {
			s.mu.RUnlock()
			continue
		}
		collData, exists := dbData[collection]
		if !exists {
			s.mu.RUnlock()
			continue
		}

		for _, deviceSlice := range collData {
			// Skip devices whose time range doesn't overlap the query — O(1) check
			if !deviceSlice.Overlaps(startTime, endTime) {
				continue
			}
			points := deviceSlice.Query(startTime, endTime)
			result = append(result, points...)
		}

		s.mu.RUnlock()
	}

	return result, nil
}

// QueryIter returns an iterator function for streaming through query results
// without collecting all points into a single slice. This reduces peak memory
// usage for large result sets.
//
// Usage:
//
//	next := ms.QueryIter("db", "coll", "", start, end)
//	for dp, ok := next(); ok; dp, ok = next() {
//	    process(dp)
//	}
func (ms *MemoryStore) QueryIter(database, collection, deviceID string, startTime, endTime time.Time) func() (*DataPoint, bool) {
	// Collect matching slices first (under read lock), then iterate without lock
	type sliceRange struct {
		points []*DataPoint // reference into OrderedSlice (no copy)
	}

	var ranges []sliceRange

	if deviceID != "" {
		idx := getShard(database, collection, deviceID)
		s := &ms.shards[idx]
		s.mu.RLock()
		if dbData, ok := s.data[database]; ok {
			if collData, ok := dbData[collection]; ok {
				if deviceSlice, ok := collData[deviceID]; ok {
					pts := deviceSlice.Query(startTime, endTime)
					if len(pts) > 0 {
						ranges = append(ranges, sliceRange{pts})
					}
				}
			}
		}
		s.mu.RUnlock()
	} else {
		for i := range ms.shards {
			s := &ms.shards[i]
			s.mu.RLock()
			if dbData, ok := s.data[database]; ok {
				if collData, ok := dbData[collection]; ok {
					for _, deviceSlice := range collData {
						if !deviceSlice.Overlaps(startTime, endTime) {
							continue
						}
						pts := deviceSlice.Query(startTime, endTime)
						if len(pts) > 0 {
							ranges = append(ranges, sliceRange{pts})
						}
					}
				}
			}
			s.mu.RUnlock()
		}
	}

	rangeIdx := 0
	pointIdx := 0

	return func() (*DataPoint, bool) {
		for rangeIdx < len(ranges) {
			r := &ranges[rangeIdx]
			if pointIdx < len(r.points) {
				dp := r.points[pointIdx]
				pointIdx++
				return dp, true
			}
			rangeIdx++
			pointIdx = 0
		}
		return nil, false
	}
}

// QueryDevice is a convenience method to query a specific device
func (ms *MemoryStore) QueryDevice(database, collection, deviceID string, startTime, endTime time.Time) ([]*DataPoint, error) {
	return ms.Query(database, collection, deviceID, startTime, endTime)
}

// QueryCollection is a convenience method to query all devices in a collection
func (ms *MemoryStore) QueryCollection(database, collection string, startTime, endTime time.Time) ([]*DataPoint, error) {
	return ms.Query(database, collection, "", startTime, endTime)
}

// GetDeviceIDs returns all device IDs for a given database and collection across all shards
func (ms *MemoryStore) GetDeviceIDs(database, collection string) []string {
	var deviceIDs []string

	for i := range ms.shards {
		s := &ms.shards[i]
		s.mu.RLock()

		dbData, exists := s.data[database]
		if exists {
			if collData, ok := dbData[collection]; ok {
				for deviceID := range collData {
					deviceIDs = append(deviceIDs, deviceID)
				}
			}
		}

		s.mu.RUnlock()
	}

	if len(deviceIDs) == 0 {
		return nil
	}
	return deviceIDs
}

// SortDataPoints sorts data points by time
func SortDataPoints(points []*DataPoint) {
	sort.Slice(points, func(i, j int) bool {
		return points[i].Time.Before(points[j].Time)
	})
}

// Close stops the cleanup and eviction goroutines
func (ms *MemoryStore) Close() error {
	// Signal all goroutines to stop
	close(ms.stopCh)

	// Wait for both goroutines to finish
	<-ms.cleanupDone
	<-ms.evictionDone

	ms.globalMu.Lock()
	finalCount := ms.totalCount
	finalSize := ms.totalSize
	ms.globalMu.Unlock()

	ms.logger.Info("Memory store closed",
		"final_count", finalCount,
		"final_size_mb", float64(finalSize)/(1024*1024))
	return nil
}

// GetMaxAge returns the max age configuration for data in memory store
func (ms *MemoryStore) GetMaxAge() time.Duration {
	return ms.maxAge
}

// getDeviceSlice returns the OrderedSlice for a specific device (for testing).
// Returns nil if not found. Caller must not hold any shard lock.
func (ms *MemoryStore) getDeviceSlice(database, collection, deviceID string) *OrderedSlice {
	idx := getShard(database, collection, deviceID)
	s := &ms.shards[idx]
	s.mu.RLock()
	defer s.mu.RUnlock()

	if dbData, ok := s.data[database]; ok {
		if collData, ok := dbData[collection]; ok {
			return collData[deviceID]
		}
	}
	return nil
}

// resetUnflushed re-adds the given points to the per-shard unflushed indices.
// Used by benchmarks to reset state between iterations.
func (ms *MemoryStore) resetUnflushed(points []*DataPoint) {
	// Group by shard
	shardPoints := make([][]*DataPoint, numShards)
	for _, dp := range points {
		idx := getShard(dp.Database, dp.Collection, dp.ID)
		shardPoints[idx] = append(shardPoints[idx], dp)
	}
	for i := range ms.shards {
		if len(shardPoints[i]) == 0 {
			continue
		}
		s := &ms.shards[i]
		s.mu.Lock()
		s.unflushed = append(s.unflushed, shardPoints[i]...)
		s.mu.Unlock()
	}
}

// hasDevice checks if a device exists in the store (for testing).
func (ms *MemoryStore) hasDevice(database, collection, deviceID string) bool {
	return ms.getDeviceSlice(database, collection, deviceID) != nil
}
