package storage

import (
	"sort"
	"time"
)

// OrderedSlice maintains a sorted slice of DataPoints by Time
// Optimized for time-series append pattern and range queries
//
// NOT THREAD-SAFE: This is an internal data structure used only within MemoryStore.
// All synchronization is handled by MemoryStore's RWMutex at a higher level.
// Do not use this struct directly outside of MemoryStore without proper locking.
type OrderedSlice struct {
	points  []*DataPoint
	minTime time.Time // cached earliest time — updated on Add/Remove
	maxTime time.Time // cached latest time — updated on Add/Remove
}

// NewOrderedSlice creates a new ordered slice with initial capacity
func NewOrderedSlice(capacity int) *OrderedSlice {
	return &OrderedSlice{
		points: make([]*DataPoint, 0, capacity),
		// minTime/maxTime left zero — updated on first Add
	}
}

// Add inserts a DataPoint in sorted order by Time
// Uses binary search to find insertion point - O(log N) search + O(N) insert
// For append-heavy workloads, often inserts at end (O(1))
// Handles duplicates: if same Time exists, keeps the one with latest InsertedAt (last-write-wins)
// If both have equal InsertedAt, the new point wins (arrived later)
func (os *OrderedSlice) Add(dp *DataPoint) {
	// Empty slice - just append
	if len(os.points) == 0 {
		os.points = append(os.points, dp)
		os.minTime = dp.Time
		os.maxTime = dp.Time
		return
	}

	last := os.points[len(os.points)-1]

	// Fast path: append if time > last element (common case)
	if dp.Time.After(last.Time) {
		os.points = append(os.points, dp)
		os.maxTime = dp.Time
		return
	}

	// Duplicate at end - replace if newer or equal InsertedAt (new point always wins on tie)
	if dp.Time.Equal(last.Time) && dp.ID == last.ID {
		// New point wins if: newer InsertedAt, OR old is from disk (zero), OR both zero (new came later)
		if dp.InsertedAt.After(last.InsertedAt) || last.InsertedAt.IsZero() || dp.InsertedAt.Equal(last.InsertedAt) {
			os.points[len(os.points)-1] = dp
		}
		return
	}

	// Binary search for insertion point
	idx := sort.Search(len(os.points), func(i int) bool {
		return os.points[i].Time.After(dp.Time) || os.points[i].Time.Equal(dp.Time)
	})

	// Check for duplicate at insertion point
	if idx < len(os.points) && os.points[idx].Time.Equal(dp.Time) && os.points[idx].ID == dp.ID {
		// Duplicate found - replace if newer or equal InsertedAt
		existing := os.points[idx]
		if dp.InsertedAt.After(existing.InsertedAt) || existing.InsertedAt.IsZero() || dp.InsertedAt.Equal(existing.InsertedAt) {
			os.points[idx] = dp
		}
		return
	}

	// Insert at position (no duplicate)
	os.points = append(os.points, nil)
	copy(os.points[idx+1:], os.points[idx:])
	os.points[idx] = dp

	// Update min bound if inserted before all existing points
	if dp.Time.Before(os.minTime) {
		os.minTime = dp.Time
	}
}

// Query returns all DataPoints within [start, end] time range
// Uses binary search to find range boundaries - O(log N + K) where K is result size
func (os *OrderedSlice) Query(start, end time.Time) []*DataPoint {
	if len(os.points) == 0 {
		return nil
	}

	// Find start index using binary search
	startIdx := sort.Search(len(os.points), func(i int) bool {
		return os.points[i].Time.After(start) || os.points[i].Time.Equal(start)
	})

	// Find end index using binary search
	endIdx := sort.Search(len(os.points), func(i int) bool {
		return os.points[i].Time.After(end)
	})

	// Return slice (no copy, just reference)
	if startIdx >= len(os.points) || startIdx >= endIdx {
		return nil
	}

	return os.points[startIdx:endIdx]
}

// QueryAll returns all DataPoints
func (os *OrderedSlice) QueryAll() []*DataPoint {
	return os.points
}

// Len returns the number of DataPoints
func (os *OrderedSlice) Len() int {
	return len(os.points)
}

// RemoveBefore removes all DataPoints before the given time
// Returns (count, removedPoints) where count is number removed and removedPoints is the slice of removed items
// The removedPoints slice is a reference to internal data, do not modify it
func (os *OrderedSlice) RemoveBefore(cutoff time.Time) (int64, int64) {
	if len(os.points) == 0 {
		return 0, 0
	}

	// Find first point >= cutoff
	idx := sort.Search(len(os.points), func(i int) bool {
		return os.points[i].Time.After(cutoff) || os.points[i].Time.Equal(cutoff)
	})

	if idx == 0 {
		return 0, 0 // Nothing to remove
	}

	var removedSize int64 = 0
	for i := range idx {
		removedSize += os.points[i].estimateSize()
	}

	// Remove from slice
	os.points = os.points[idx:]
	os.rebuildTimeBounds()

	return int64(idx), removedSize
}

// RemoveUpToIndex removes DataPoints up to (but not including) the given index
// Returns (count, removedSize) where count is number removed and removedSize is total size in bytes of removed points
func (os *OrderedSlice) RemoveUpToIndex(index int) (int64, int64) {
	if index <= 0 || index > len(os.points) {
		return 0, 0
	}

	var removedSize int64 = 0
	for i := range index {
		removedSize += os.points[i].estimateSize()
	}

	os.points = os.points[index:]
	os.rebuildTimeBounds()
	return int64(index), removedSize
}

// RemoveByStatus removes DataPoints matching the given FlushStatus
// Returns count of removed points
// Note: This is O(N) - use sparingly
func (os *OrderedSlice) RemoveByStatus(status FlushStatus) int {
	if len(os.points) == 0 {
		return 0
	}

	kept := make([]*DataPoint, 0, len(os.points))
	removed := 0

	for _, point := range os.points {
		if point.FlushStatus == status {
			removed++
		} else {
			kept = append(kept, point)
		}
	}

	os.points = kept
	os.rebuildTimeBounds()
	return removed
}

// RemoveByTimestamps removes DataPoints matching the given timestamps
// Returns count of removed points
// Optimized with map lookup for O(N) complexity instead of O(N*M)
func (os *OrderedSlice) RemoveByTimestamps(timestamps map[time.Time]bool) int {
	if len(os.points) == 0 || len(timestamps) == 0 {
		return 0
	}

	writeIdx := 0 // In-place compaction
	removed := 0

	for readIdx := 0; readIdx < len(os.points); readIdx++ {
		point := os.points[readIdx]

		// Check if this timestamp should be removed
		if timestamps[point.Time] {
			removed++
			// Don't copy (effectively removing)
		} else {
			// Keep this point
			if writeIdx != readIdx {
				os.points[writeIdx] = os.points[readIdx]
			}
			writeIdx++
		}
	}

	// Truncate slice
	os.points = os.points[:writeIdx]
	os.rebuildTimeBounds()
	return removed
}

// FilterByStatus returns DataPoints matching the given FlushStatus
// Does not modify the slice
func (os *OrderedSlice) FilterByStatus(status FlushStatus) []*DataPoint {
	result := make([]*DataPoint, 0)

	for _, point := range os.points {
		if point.FlushStatus == status {
			result = append(result, point)
		}
	}

	return result
}

// FilterAndMarkStatus filters DataPoints by filterStatus and changes them to newStatus
// Returns the filtered points with their status already updated
// This is more efficient than FilterByStatus + separate marking loop
func (os *OrderedSlice) FilterAndMarkStatus(filterStatus, newStatus FlushStatus) []*DataPoint {
	result := make([]*DataPoint, 0)

	for _, point := range os.points {
		if point.FlushStatus == filterStatus {
			point.FlushStatus = newStatus
			result = append(result, point)
		}
	}

	return result
}

// UpdateStatus changes FlushStatus in-place from fromStatus to toStatus.
// Returns the number of updated points. Zero allocations — no result slice created.
// Use this instead of FilterAndMarkStatus when the result slice is not needed.
func (os *OrderedSlice) UpdateStatus(fromStatus, toStatus FlushStatus) int {
	count := 0
	for _, point := range os.points {
		if point.FlushStatus == fromStatus {
			point.FlushStatus = toStatus
			count++
		}
	}
	return count
}

// GetAt returns the DataPoint at the given index
// Returns nil if index out of bounds
func (os *OrderedSlice) GetAt(index int) *DataPoint {
	if index < 0 || index >= len(os.points) {
		return nil
	}
	return os.points[index]
}

// GetFirst returns the first (oldest) DataPoint
// Returns nil if empty
func (os *OrderedSlice) GetFirst() *DataPoint {
	if len(os.points) == 0 {
		return nil
	}
	return os.points[0]
}

// GetLast returns the last (newest) DataPoint
// Returns nil if empty
func (os *OrderedSlice) GetLast() *DataPoint {
	if len(os.points) == 0 {
		return nil
	}
	return os.points[len(os.points)-1]
}

// RemoveFlushedBefore removes all flushed DataPoints before the given time
// Only removes points with FlushStatus = FlushStatusFlushed to prevent data loss
// Returns (count, removedSize) where count is number removed and removedSize is total size in bytes
func (os *OrderedSlice) RemoveFlushedBefore(cutoff time.Time) (int64, int64) {
	if len(os.points) == 0 {
		return 0, 0
	}

	removedCount := int64(0)
	removedSize := int64(0)
	writeIdx := 0

	for readIdx := 0; readIdx < len(os.points); readIdx++ {
		point := os.points[readIdx]

		// Remove if before cutoff AND flushed
		if point.Time.Before(cutoff) && point.FlushStatus == FlushStatusFlushed {
			removedCount++
			removedSize += point.estimateSize()
			// Don't copy (effectively removing)
		} else {
			// Keep this point
			if writeIdx != readIdx {
				os.points[writeIdx] = os.points[readIdx]
			}
			writeIdx++
		}
	}

	// Truncate slice to new length
	os.points = os.points[:writeIdx]
	os.rebuildTimeBounds()

	return removedCount, removedSize
}

// RemoveFlushedOldest removes up to maxCount oldest entries that have FlushStatus = FlushStatusFlushed
// Returns (count removed, size removed)
// This ensures we only evict data that has been safely persisted to disk
func (os *OrderedSlice) RemoveFlushedOldest(maxCount int) (int64, int64) {
	if len(os.points) == 0 || maxCount <= 0 {
		return 0, 0
	}

	removedCount := int64(0)
	removedSize := int64(0)
	writeIdx := 0

	for readIdx := 0; readIdx < len(os.points); readIdx++ {
		point := os.points[readIdx]

		// If we can still remove more and point is flushed, skip it (remove)
		if removedCount < int64(maxCount) && point.FlushStatus == FlushStatusFlushed {
			removedCount++
			removedSize += point.estimateSize()
			// Don't copy this point (effectively removing it)
		} else {
			// Keep this point by copying to write position
			if writeIdx != readIdx {
				os.points[writeIdx] = os.points[readIdx]
			}
			writeIdx++
		}
	}

	// Truncate slice to new length
	os.points = os.points[:writeIdx]
	os.rebuildTimeBounds()

	return removedCount, removedSize
}

// Clear removes all DataPoints and returns total size in bytes of removed points
func (os *OrderedSlice) Clear() int64 {
	var removedSize int64 = 0
	for _, point := range os.points {
		removedSize += point.estimateSize()
	}
	os.points = os.points[:0]
	os.minTime = time.Time{}
	os.maxTime = time.Time{}
	return removedSize
}

// Overlaps returns true if this slice's time range intersects [start, end].
// Uses cached minTime/maxTime for O(1) check — no scanning needed.
func (os *OrderedSlice) Overlaps(start, end time.Time) bool {
	if len(os.points) == 0 {
		return false
	}
	// No overlap if maxTime < start or minTime > end
	return !os.maxTime.Before(start) && !os.minTime.After(end)
}

// MinTime returns the cached minimum time. Zero if empty.
func (os *OrderedSlice) MinTime() time.Time { return os.minTime }

// MaxTime returns the cached maximum time. Zero if empty.
func (os *OrderedSlice) MaxTime() time.Time { return os.maxTime }

// rebuildTimeBounds recalculates minTime/maxTime from the sorted points slice.
// Called after Remove operations that may have changed the bounds.
func (os *OrderedSlice) rebuildTimeBounds() {
	if len(os.points) == 0 {
		os.minTime = time.Time{}
		os.maxTime = time.Time{}
		return
	}
	os.minTime = os.points[0].Time
	os.maxTime = os.points[len(os.points)-1].Time
}

// EstimateMemory returns approximate memory usage in bytes
func (os *OrderedSlice) EstimateMemory() int64 {
	// Slice overhead + pointer size per element
	return int64(24 + len(os.points)*8 + cap(os.points)*8)
}
