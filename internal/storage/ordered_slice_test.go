package storage

import (
	"testing"
	"time"
)

// Helper function to create test data point
func createTestDataPoint(t time.Time, deviceID string, flushStatus FlushStatus) *DataPoint {
	return &DataPoint{
		Database:    "testdb",
		Collection:  "testcoll",
		ShardID:     "shard1",
		Time:        t,
		ID:          deviceID,
		Fields:      map[string]interface{}{"temp": 25.5},
		InsertedAt:  time.Now(),
		FlushStatus: flushStatus,
	}
}

func TestNewOrderedSlice(t *testing.T) {
	os := NewOrderedSlice(100)

	if os == nil {
		t.Fatal("Expected non-nil OrderedSlice")
	}

	if os.Len() != 0 {
		t.Errorf("Expected length 0, got %d", os.Len())
	}

	if cap(os.points) != 100 {
		t.Errorf("Expected capacity 100, got %d", cap(os.points))
	}
}

func TestOrderedSlice_Add(t *testing.T) {
	t.Run("EmptySlice", func(t *testing.T) {
		os := NewOrderedSlice(10)
		now := time.Now()

		dp := createTestDataPoint(now, "device1", FlushStatusNew)
		os.Add(dp)

		if os.Len() != 1 {
			t.Errorf("Expected length 1, got %d", os.Len())
		}

		if os.GetFirst() != dp {
			t.Error("Expected first point to be the added point")
		}
	})

	t.Run("AppendToEnd", func(t *testing.T) {
		os := NewOrderedSlice(10)
		now := time.Now()

		// Add points in ascending order (fast path)
		dp1 := createTestDataPoint(now, "device1", FlushStatusNew)
		dp2 := createTestDataPoint(now.Add(1*time.Second), "device1", FlushStatusNew)
		dp3 := createTestDataPoint(now.Add(2*time.Second), "device1", FlushStatusNew)

		os.Add(dp1)
		os.Add(dp2)
		os.Add(dp3)

		if os.Len() != 3 {
			t.Errorf("Expected length 3, got %d", os.Len())
		}

		if os.GetFirst() != dp1 || os.GetLast() != dp3 {
			t.Error("Points not in correct order")
		}
	})

	t.Run("InsertInMiddle", func(t *testing.T) {
		os := NewOrderedSlice(10)
		now := time.Now()

		dp1 := createTestDataPoint(now, "device1", FlushStatusNew)
		dp3 := createTestDataPoint(now.Add(2*time.Second), "device1", FlushStatusNew)
		dp2 := createTestDataPoint(now.Add(1*time.Second), "device1", FlushStatusNew)

		os.Add(dp1)
		os.Add(dp3)
		os.Add(dp2) // Should insert between dp1 and dp3

		if os.Len() != 3 {
			t.Errorf("Expected length 3, got %d", os.Len())
		}

		// Verify sorted order
		if !os.GetAt(0).Time.Equal(now) {
			t.Error("First point has wrong time")
		}
		if !os.GetAt(1).Time.Equal(now.Add(1 * time.Second)) {
			t.Error("Second point has wrong time")
		}
		if !os.GetAt(2).Time.Equal(now.Add(2 * time.Second)) {
			t.Error("Third point has wrong time")
		}
	})

	t.Run("Duplicate_NewerInsertedAt", func(t *testing.T) {
		os := NewOrderedSlice(10)
		now := time.Now()
		insertTime1 := time.Now()
		insertTime2 := insertTime1.Add(1 * time.Second)

		dp1 := &DataPoint{
			Database:    "testdb",
			Collection:  "testcoll",
			Time:        now,
			ID:          "device1",
			Fields:      map[string]interface{}{"temp": 20.0},
			InsertedAt:  insertTime1,
			FlushStatus: FlushStatusNew,
		}

		dp2 := &DataPoint{
			Database:    "testdb",
			Collection:  "testcoll",
			Time:        now,
			ID:          "device1",
			Fields:      map[string]interface{}{"temp": 25.0},
			InsertedAt:  insertTime2,
			FlushStatus: FlushStatusNew,
		}

		os.Add(dp1)
		os.Add(dp2)

		if os.Len() != 1 {
			t.Errorf("Expected length 1 after duplicate, got %d", os.Len())
		}

		stored := os.GetFirst()
		if temp, ok := stored.Fields["temp"].(float64); !ok || temp != 25.0 {
			t.Error("Expected newer value to replace older value")
		}
	})

	t.Run("Duplicate_OlderInsertedAt", func(t *testing.T) {
		os := NewOrderedSlice(10)
		now := time.Now()
		insertTime1 := time.Now()
		insertTime2 := insertTime1.Add(-1 * time.Second)

		dp1 := &DataPoint{
			Database:    "testdb",
			Collection:  "testcoll",
			Time:        now,
			ID:          "device1",
			Fields:      map[string]interface{}{"temp": 20.0},
			InsertedAt:  insertTime1,
			FlushStatus: FlushStatusNew,
		}

		dp2 := &DataPoint{
			Database:    "testdb",
			Collection:  "testcoll",
			Time:        now,
			ID:          "device1",
			Fields:      map[string]interface{}{"temp": 25.0},
			InsertedAt:  insertTime2,
			FlushStatus: FlushStatusNew,
		}

		os.Add(dp1)
		os.Add(dp2)

		if os.Len() != 1 {
			t.Errorf("Expected length 1 after duplicate, got %d", os.Len())
		}

		stored := os.GetFirst()
		if temp, ok := stored.Fields["temp"].(float64); !ok || temp != 20.0 {
			t.Error("Expected older value to be kept when InsertedAt is newer")
		}
	})
}

func TestOrderedSlice_Query(t *testing.T) {
	os := NewOrderedSlice(10)
	now := time.Now()

	for i := 0; i < 5; i++ {
		dp := createTestDataPoint(now.Add(time.Duration(i)*2*time.Second), "device1", FlushStatusNew)
		os.Add(dp)
	}

	tests := []struct {
		name      string
		start     time.Time
		end       time.Time
		wantCount int
	}{
		{
			name:      "Query all",
			start:     now,
			end:       now.Add(10 * time.Second),
			wantCount: 5,
		},
		{
			name:      "Query middle range",
			start:     now.Add(2 * time.Second),
			end:       now.Add(6 * time.Second),
			wantCount: 3,
		},
		{
			name:      "Query before data",
			start:     now.Add(-10 * time.Second),
			end:       now.Add(-1 * time.Second),
			wantCount: 0,
		},
		{
			name:      "Query after data",
			start:     now.Add(20 * time.Second),
			end:       now.Add(30 * time.Second),
			wantCount: 0,
		},
		{
			name:      "Query single point",
			start:     now.Add(4 * time.Second),
			end:       now.Add(4 * time.Second),
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := os.Query(tt.start, tt.end)
			if len(result) != tt.wantCount {
				t.Errorf("Expected %d results, got %d", tt.wantCount, len(result))
			}
		})
	}
}

func TestOrderedSlice_RemoveBefore(t *testing.T) {
	t.Run("RemoveSome", func(t *testing.T) {
		os := NewOrderedSlice(10)
		now := time.Now()

		for i := 0; i < 5; i++ {
			dp := createTestDataPoint(now.Add(time.Duration(i)*time.Second), "device1", FlushStatusNew)
			os.Add(dp)
		}

		cutoff := now.Add(3 * time.Second)
		count, size := os.RemoveBefore(cutoff)

		if count != 3 {
			t.Errorf("Expected to remove 3 points, removed %d", count)
		}

		if size == 0 {
			t.Error("Expected non-zero removed size")
		}

		if os.Len() != 2 {
			t.Errorf("Expected 2 points remaining, got %d", os.Len())
		}

		first := os.GetFirst()
		if first.Time.Before(cutoff) {
			t.Error("First remaining point is before cutoff")
		}
	})

	t.Run("EmptySlice", func(t *testing.T) {
		os := NewOrderedSlice(10)
		now := time.Now()

		count, size := os.RemoveBefore(now)

		if count != 0 || size != 0 {
			t.Errorf("Expected 0 removed from empty slice, got count=%d, size=%d", count, size)
		}
	})

	t.Run("NoMatch", func(t *testing.T) {
		os := NewOrderedSlice(10)
		now := time.Now()

		for i := 5; i < 8; i++ {
			dp := createTestDataPoint(now.Add(time.Duration(i)*time.Second), "device1", FlushStatusNew)
			os.Add(dp)
		}

		count, size := os.RemoveBefore(now)

		if count != 0 || size != 0 {
			t.Errorf("Expected 0 removed, got count=%d, size=%d", count, size)
		}

		if os.Len() != 3 {
			t.Errorf("Expected 3 points remaining, got %d", os.Len())
		}
	})
}

func TestOrderedSlice_FilterAndMarkStatus(t *testing.T) {
	os := NewOrderedSlice(10)
	now := time.Now()

	dp1 := createTestDataPoint(now, "device1", FlushStatusNew)
	dp2 := createTestDataPoint(now.Add(1*time.Second), "device1", FlushStatusNew)
	dp3 := createTestDataPoint(now.Add(2*time.Second), "device1", FlushStatusFlushing)

	os.Add(dp1)
	os.Add(dp2)
	os.Add(dp3)

	filtered := os.FilterAndMarkStatus(FlushStatusNew, FlushStatusFlushing)

	if len(filtered) != 2 {
		t.Errorf("Expected 2 filtered points, got %d", len(filtered))
	}

	if os.GetAt(0).FlushStatus != FlushStatusFlushing {
		t.Error("Expected first point status changed to Flushing")
	}
	if os.GetAt(1).FlushStatus != FlushStatusFlushing {
		t.Error("Expected second point status changed to Flushing")
	}
	if os.GetAt(2).FlushStatus != FlushStatusFlushing {
		t.Error("Expected third point to remain Flushing")
	}
}

func TestOrderedSlice_GetAt(t *testing.T) {
	os := NewOrderedSlice(10)
	now := time.Now()

	dp := createTestDataPoint(now, "device1", FlushStatusNew)
	os.Add(dp)

	if os.GetAt(0) != dp {
		t.Error("GetAt(0) should return the added point")
	}

	if os.GetAt(-1) != nil {
		t.Error("GetAt(-1) should return nil")
	}

	if os.GetAt(100) != nil {
		t.Error("GetAt(100) should return nil")
	}
}

func TestOrderedSlice_Clear(t *testing.T) {
	os := NewOrderedSlice(10)
	now := time.Now()

	for i := 0; i < 5; i++ {
		dp := createTestDataPoint(now.Add(time.Duration(i)*time.Second), "device1", FlushStatusNew)
		os.Add(dp)
	}

	size := os.Clear()

	if size == 0 {
		t.Error("Expected non-zero size cleared")
	}

	if os.Len() != 0 {
		t.Errorf("Expected length 0 after clear, got %d", os.Len())
	}
}

func TestOrderedSlice_RemoveFlushedBefore(t *testing.T) {
	t.Run("EmptySlice", func(t *testing.T) {
		os := NewOrderedSlice(10)
		cutoff := time.Now()

		count, size := os.RemoveFlushedBefore(cutoff)

		if count != 0 {
			t.Errorf("Expected count 0, got %d", count)
		}
		if size != 0 {
			t.Errorf("Expected size 0, got %d", size)
		}
	})

	t.Run("NoFlushedItems", func(t *testing.T) {
		os := NewOrderedSlice(10)
		now := time.Now()

		// Add 5 new items all before cutoff
		for i := 0; i < 5; i++ {
			dp := createTestDataPoint(now.Add(time.Duration(i-10)*time.Second), "device1", FlushStatusNew)
			os.Add(dp)
		}

		cutoff := now

		count, size := os.RemoveFlushedBefore(cutoff)

		if count != 0 {
			t.Errorf("Expected count 0 (no flushed items), got %d", count)
		}
		if size != 0 {
			t.Errorf("Expected size 0, got %d", size)
		}
		if os.Len() != 5 {
			t.Errorf("Expected all items preserved, got %d", os.Len())
		}
	})

	t.Run("AllFlushedBeforeCutoff", func(t *testing.T) {
		os := NewOrderedSlice(10)
		now := time.Now()

		// Add 5 flushed items all before cutoff
		for i := 0; i < 5; i++ {
			dp := createTestDataPoint(now.Add(time.Duration(i-10)*time.Second), "device1", FlushStatusFlushed)
			os.Add(dp)
		}

		cutoff := now

		count, size := os.RemoveFlushedBefore(cutoff)

		if count != 5 {
			t.Errorf("Expected count 5, got %d", count)
		}
		if size == 0 {
			t.Error("Expected non-zero size removed")
		}
		if os.Len() != 0 {
			t.Errorf("Expected 0 items remaining, got %d", os.Len())
		}
	})

	t.Run("MixedTimesAndStatus", func(t *testing.T) {
		os := NewOrderedSlice(10)
		now := time.Now()

		// Add items at different times with different statuses
		// t=-5: Flushed (should remove)
		// t=-3: New (should keep)
		// t=-1: Flushed (should remove)
		// t=+1: Flushed (should keep - after cutoff)
		// t=+3: New (should keep - after cutoff)
		items := []struct {
			offset time.Duration
			status FlushStatus
		}{
			{-5 * time.Second, FlushStatusFlushed}, // Remove
			{-3 * time.Second, FlushStatusNew},     // Keep (not flushed)
			{-1 * time.Second, FlushStatusFlushed}, // Remove
			{+1 * time.Second, FlushStatusFlushed}, // Keep (after cutoff)
			{+3 * time.Second, FlushStatusNew},     // Keep (after cutoff)
		}

		for _, item := range items {
			dp := createTestDataPoint(now.Add(item.offset), "device1", item.status)
			os.Add(dp)
		}

		cutoff := now

		count, size := os.RemoveFlushedBefore(cutoff)

		if count != 2 {
			t.Errorf("Expected count 2 (2 flushed before cutoff), got %d", count)
		}
		if size == 0 {
			t.Error("Expected non-zero size removed")
		}
		if os.Len() != 3 {
			t.Errorf("Expected 3 items remaining, got %d", os.Len())
		}

		// Verify remaining items
		remaining := os.QueryAll()
		if len(remaining) != 3 {
			t.Fatalf("Expected 3 remaining items, got %d", len(remaining))
		}

		// Should be: t=-3 (New), t=+1 (Flushed), t=+3 (New)
		if !remaining[0].Time.Equal(now.Add(-3 * time.Second)) {
			t.Error("Expected first remaining at t=-3s")
		}
		if remaining[0].FlushStatus != FlushStatusNew {
			t.Error("Expected first remaining to be New")
		}
		if !remaining[1].Time.Equal(now.Add(1 * time.Second)) {
			t.Error("Expected second remaining at t=+1s")
		}
		if !remaining[2].Time.Equal(now.Add(3 * time.Second)) {
			t.Error("Expected third remaining at t=+3s")
		}
	})

	t.Run("AllFlushingStatus", func(t *testing.T) {
		os := NewOrderedSlice(10)
		now := time.Now()

		// Add items with Flushing status (should not be removed)
		for i := 0; i < 5; i++ {
			dp := createTestDataPoint(now.Add(time.Duration(i-10)*time.Second), "device1", FlushStatusFlushing)
			os.Add(dp)
		}

		cutoff := now

		count, size := os.RemoveFlushedBefore(cutoff)

		if count != 0 {
			t.Errorf("Expected count 0 (Flushing items should not be removed), got %d", count)
		}
		if size != 0 {
			t.Errorf("Expected size 0, got %d", size)
		}
		if os.Len() != 5 {
			t.Errorf("Expected all items preserved, got %d", os.Len())
		}
	})

	t.Run("CutoffAtExactTime", func(t *testing.T) {
		os := NewOrderedSlice(10)
		now := time.Now()

		// Add flushed items at t=-1, t=0 (cutoff), t=+1
		for _, offset := range []time.Duration{-1 * time.Second, 0, 1 * time.Second} {
			dp := createTestDataPoint(now.Add(offset), "device1", FlushStatusFlushed)
			os.Add(dp)
		}

		cutoff := now // At t=0

		count, size := os.RemoveFlushedBefore(cutoff)

		// Should only remove t=-1 (before cutoff)
		// t=0 and t=+1 should be kept
		if count != 1 {
			t.Errorf("Expected count 1 (only t=-1), got %d", count)
		}
		if size == 0 {
			t.Error("Expected non-zero size removed")
		}
		if os.Len() != 2 {
			t.Errorf("Expected 2 items remaining, got %d", os.Len())
		}
	})
}

func TestOrderedSlice_RemoveFlushedOldest(t *testing.T) {
	t.Run("EmptySlice", func(t *testing.T) {
		os := NewOrderedSlice(10)

		count, size := os.RemoveFlushedOldest(5)

		if count != 0 {
			t.Errorf("Expected count 0, got %d", count)
		}
		if size != 0 {
			t.Errorf("Expected size 0, got %d", size)
		}
	})

	t.Run("NoFlushedItems", func(t *testing.T) {
		os := NewOrderedSlice(10)
		now := time.Now()

		// Add 5 new items
		for i := 0; i < 5; i++ {
			dp := createTestDataPoint(now.Add(time.Duration(i)*time.Second), "device1", FlushStatusNew)
			os.Add(dp)
		}

		count, size := os.RemoveFlushedOldest(3)

		if count != 0 {
			t.Errorf("Expected count 0 (no flushed items), got %d", count)
		}
		if size != 0 {
			t.Errorf("Expected size 0, got %d", size)
		}
		if os.Len() != 5 {
			t.Errorf("Expected all items preserved, got %d", os.Len())
		}
	})

	t.Run("AllFlushedItems", func(t *testing.T) {
		os := NewOrderedSlice(10)
		now := time.Now()

		// Add 5 flushed items
		for i := 0; i < 5; i++ {
			dp := createTestDataPoint(now.Add(time.Duration(i)*time.Second), "device1", FlushStatusFlushed)
			os.Add(dp)
		}

		count, size := os.RemoveFlushedOldest(3)

		if count != 3 {
			t.Errorf("Expected count 3, got %d", count)
		}
		if size == 0 {
			t.Error("Expected non-zero size removed")
		}
		if os.Len() != 2 {
			t.Errorf("Expected 2 items remaining, got %d", os.Len())
		}

		// Verify remaining items are the newest ones (indices 3 and 4)
		if !os.GetAt(0).Time.Equal(now.Add(3 * time.Second)) {
			t.Error("Expected first remaining item at t=3s")
		}
		if !os.GetAt(1).Time.Equal(now.Add(4 * time.Second)) {
			t.Error("Expected second remaining item at t=4s")
		}
	})

	t.Run("MixedFlushStatus", func(t *testing.T) {
		os := NewOrderedSlice(10)
		now := time.Now()

		// Add mixed status items: F, N, F, F, N (F=Flushed, N=New)
		statuses := []FlushStatus{
			FlushStatusFlushed,
			FlushStatusNew,
			FlushStatusFlushed,
			FlushStatusFlushed,
			FlushStatusNew,
		}

		for i, status := range statuses {
			dp := createTestDataPoint(now.Add(time.Duration(i)*time.Second), "device1", status)
			os.Add(dp)
		}

		// Try to remove 2 flushed items (should remove indices 0 and 2)
		count, size := os.RemoveFlushedOldest(2)

		if count != 2 {
			t.Errorf("Expected count 2, got %d", count)
		}
		if size == 0 {
			t.Error("Expected non-zero size removed")
		}
		if os.Len() != 3 {
			t.Errorf("Expected 3 items remaining, got %d", os.Len())
		}

		// Verify remaining items are correct
		// Should have: N(1s), F(3s), N(4s)
		if !os.GetAt(0).Time.Equal(now.Add(1 * time.Second)) {
			t.Error("Expected first remaining item at t=1s")
		}
		if os.GetAt(0).FlushStatus != FlushStatusNew {
			t.Error("Expected first item to be New status")
		}

		if !os.GetAt(1).Time.Equal(now.Add(3 * time.Second)) {
			t.Error("Expected second remaining item at t=3s")
		}
		if os.GetAt(1).FlushStatus != FlushStatusFlushed {
			t.Error("Expected second item to be Flushed status")
		}

		if !os.GetAt(2).Time.Equal(now.Add(4 * time.Second)) {
			t.Error("Expected third remaining item at t=4s")
		}
		if os.GetAt(2).FlushStatus != FlushStatusNew {
			t.Error("Expected third item to be New status")
		}
	})

	t.Run("RemoveMoreThanAvailable", func(t *testing.T) {
		os := NewOrderedSlice(10)
		now := time.Now()

		// Add 2 flushed and 3 new items
		for i := 0; i < 2; i++ {
			dp := createTestDataPoint(now.Add(time.Duration(i)*time.Second), "device1", FlushStatusFlushed)
			os.Add(dp)
		}
		for i := 2; i < 5; i++ {
			dp := createTestDataPoint(now.Add(time.Duration(i)*time.Second), "device1", FlushStatusNew)
			os.Add(dp)
		}

		// Try to remove 10 flushed items, but only 2 exist
		count, size := os.RemoveFlushedOldest(10)

		if count != 2 {
			t.Errorf("Expected count 2 (only 2 flushed), got %d", count)
		}
		if size == 0 {
			t.Error("Expected non-zero size removed")
		}
		if os.Len() != 3 {
			t.Errorf("Expected 3 new items remaining, got %d", os.Len())
		}

		// Verify all remaining items are New status
		for i := 0; i < os.Len(); i++ {
			if os.GetAt(i).FlushStatus != FlushStatusNew {
				t.Errorf("Expected item %d to be New status", i)
			}
		}
	})

	t.Run("RemoveZeroCount", func(t *testing.T) {
		os := NewOrderedSlice(10)
		now := time.Now()

		// Add some flushed items
		for i := 0; i < 3; i++ {
			dp := createTestDataPoint(now.Add(time.Duration(i)*time.Second), "device1", FlushStatusFlushed)
			os.Add(dp)
		}

		count, size := os.RemoveFlushedOldest(0)

		if count != 0 {
			t.Errorf("Expected count 0, got %d", count)
		}
		if size != 0 {
			t.Errorf("Expected size 0, got %d", size)
		}
		if os.Len() != 3 {
			t.Errorf("Expected all 3 items preserved, got %d", os.Len())
		}
	})

	t.Run("PreservesFlushingStatus", func(t *testing.T) {
		os := NewOrderedSlice(10)
		now := time.Now()

		// Add items with different statuses: F, Flushing, F, New
		statuses := []FlushStatus{
			FlushStatusFlushed,
			FlushStatusFlushing,
			FlushStatusFlushed,
			FlushStatusNew,
		}

		for i, status := range statuses {
			dp := createTestDataPoint(now.Add(time.Duration(i)*time.Second), "device1", status)
			os.Add(dp)
		}

		// Remove 2 flushed items (indices 0 and 2)
		count, size := os.RemoveFlushedOldest(2)

		if count != 2 {
			t.Errorf("Expected count 2, got %d", count)
		}
		if size == 0 {
			t.Error("Expected non-zero size removed")
		}
		if os.Len() != 2 {
			t.Errorf("Expected 2 items remaining, got %d", os.Len())
		}

		// Verify Flushing and New items are preserved
		if os.GetAt(0).FlushStatus != FlushStatusFlushing {
			t.Error("Expected first remaining item to be Flushing status")
		}
		if os.GetAt(1).FlushStatus != FlushStatusNew {
			t.Error("Expected second remaining item to be New status")
		}
	})
}

func TestOrderedSlice_RemoveByTimestamps(t *testing.T) {
	t.Run("EmptySlice", func(t *testing.T) {
		os := NewOrderedSlice(10)
		timestamps := map[time.Time]bool{
			time.Now(): true,
		}

		count := os.RemoveByTimestamps(timestamps)

		if count != 0 {
			t.Errorf("Expected count 0, got %d", count)
		}
	})

	t.Run("EmptyTimestamps", func(t *testing.T) {
		os := NewOrderedSlice(10)
		now := time.Now()

		// Add some items
		for i := 0; i < 3; i++ {
			dp := createTestDataPoint(now.Add(time.Duration(i)*time.Second), "device1", FlushStatusFlushed)
			os.Add(dp)
		}

		count := os.RemoveByTimestamps(map[time.Time]bool{})

		if count != 0 {
			t.Errorf("Expected count 0, got %d", count)
		}
		if os.Len() != 3 {
			t.Errorf("Expected all 3 items preserved, got %d", os.Len())
		}
	})

	t.Run("RemoveSingle", func(t *testing.T) {
		os := NewOrderedSlice(10)
		now := time.Now()

		// Add 5 items
		times := make([]time.Time, 5)
		for i := 0; i < 5; i++ {
			times[i] = now.Add(time.Duration(i) * time.Second)
			dp := createTestDataPoint(times[i], "device1", FlushStatusFlushed)
			os.Add(dp)
		}

		// Remove item at index 2 (t=2s)
		toRemove := map[time.Time]bool{
			times[2]: true,
		}

		count := os.RemoveByTimestamps(toRemove)

		if count != 1 {
			t.Errorf("Expected count 1, got %d", count)
		}
		if os.Len() != 4 {
			t.Errorf("Expected 4 items remaining, got %d", os.Len())
		}

		// Verify correct item removed
		if os.GetAt(2).Time.Equal(times[2]) {
			t.Error("Item at t=2s should have been removed")
		}
	})

	t.Run("RemoveMultiple", func(t *testing.T) {
		os := NewOrderedSlice(10)
		now := time.Now()

		// Add 7 items
		times := make([]time.Time, 7)
		for i := 0; i < 7; i++ {
			times[i] = now.Add(time.Duration(i) * time.Second)
			dp := createTestDataPoint(times[i], "device1", FlushStatusFlushed)
			os.Add(dp)
		}

		// Remove items at indices 1, 3, 5 (t=1s, 3s, 5s)
		toRemove := map[time.Time]bool{
			times[1]: true,
			times[3]: true,
			times[5]: true,
		}

		count := os.RemoveByTimestamps(toRemove)

		if count != 3 {
			t.Errorf("Expected count 3, got %d", count)
		}
		if os.Len() != 4 {
			t.Errorf("Expected 4 items remaining, got %d", os.Len())
		}

		// Verify remaining items are correct (0, 2, 4, 6)
		expectedIndices := []int{0, 2, 4, 6}
		for i, expectedIdx := range expectedIndices {
			if !os.GetAt(i).Time.Equal(times[expectedIdx]) {
				t.Errorf("Item %d should be at t=%ds, got %v", i, expectedIdx, os.GetAt(i).Time)
			}
		}
	})

	t.Run("RemoveAll", func(t *testing.T) {
		os := NewOrderedSlice(10)
		now := time.Now()

		// Add 3 items
		toRemove := make(map[time.Time]bool)
		for i := 0; i < 3; i++ {
			t := now.Add(time.Duration(i) * time.Second)
			dp := createTestDataPoint(t, "device1", FlushStatusFlushed)
			os.Add(dp)
			toRemove[t] = true
		}

		count := os.RemoveByTimestamps(toRemove)

		if count != 3 {
			t.Errorf("Expected count 3, got %d", count)
		}
		if os.Len() != 0 {
			t.Errorf("Expected empty slice, got %d items", os.Len())
		}
	})

	t.Run("RemoveNonExistent", func(t *testing.T) {
		os := NewOrderedSlice(10)
		now := time.Now()

		// Add 3 items at t=0s, 1s, 2s
		for i := 0; i < 3; i++ {
			dp := createTestDataPoint(now.Add(time.Duration(i)*time.Second), "device1", FlushStatusFlushed)
			os.Add(dp)
		}

		// Try to remove item at t=10s (doesn't exist)
		toRemove := map[time.Time]bool{
			now.Add(10 * time.Second): true,
		}

		count := os.RemoveByTimestamps(toRemove)

		if count != 0 {
			t.Errorf("Expected count 0 (no matching timestamp), got %d", count)
		}
		if os.Len() != 3 {
			t.Errorf("Expected all 3 items preserved, got %d", os.Len())
		}
	})

	t.Run("MixedExistAndNonExist", func(t *testing.T) {
		os := NewOrderedSlice(10)
		now := time.Now()

		// Add items at t=0s, 1s, 2s
		times := make([]time.Time, 3)
		for i := 0; i < 3; i++ {
			times[i] = now.Add(time.Duration(i) * time.Second)
			dp := createTestDataPoint(times[i], "device1", FlushStatusFlushed)
			os.Add(dp)
		}

		// Try to remove t=1s (exists) and t=10s (doesn't exist)
		toRemove := map[time.Time]bool{
			times[1]:                  true,
			now.Add(10 * time.Second): true,
		}

		count := os.RemoveByTimestamps(toRemove)

		if count != 1 {
			t.Errorf("Expected count 1 (only 1 matching), got %d", count)
		}
		if os.Len() != 2 {
			t.Errorf("Expected 2 items remaining, got %d", os.Len())
		}

		// Verify t=1s was removed, t=0s and t=2s remain
		if !os.GetAt(0).Time.Equal(times[0]) {
			t.Error("First item should be at t=0s")
		}
		if !os.GetAt(1).Time.Equal(times[2]) {
			t.Error("Second item should be at t=2s")
		}
	})
}

func TestOrderedSlice_RemoveUpToIndex(t *testing.T) {
	t.Run("ValidIndex", func(t *testing.T) {
		os := NewOrderedSlice(10)
		now := time.Now()

		// Add 5 items
		for i := 0; i < 5; i++ {
			dp := createTestDataPoint(now.Add(time.Duration(i)*time.Second), "device1", FlushStatusNew)
			os.Add(dp)
		}

		// Remove up to index 3
		count, removedSize := os.RemoveUpToIndex(3)

		if count != 3 {
			t.Errorf("Expected count 3, got %d", count)
		}
		if removedSize == 0 {
			t.Error("Expected non-zero removedSize")
		}
		if os.Len() != 2 {
			t.Errorf("Expected 2 items remaining, got %d", os.Len())
		}
	})

	t.Run("IndexZero", func(t *testing.T) {
		os := NewOrderedSlice(10)
		now := time.Now()

		for i := 0; i < 3; i++ {
			dp := createTestDataPoint(now.Add(time.Duration(i)*time.Second), "device1", FlushStatusNew)
			os.Add(dp)
		}

		count, removedSize := os.RemoveUpToIndex(0)

		if count != 0 {
			t.Errorf("Expected count 0, got %d", count)
		}
		if removedSize != 0 {
			t.Errorf("Expected removedSize 0, got %d", removedSize)
		}
		if os.Len() != 3 {
			t.Errorf("Expected 3 items remaining, got %d", os.Len())
		}
	})

	t.Run("IndexNegative", func(t *testing.T) {
		os := NewOrderedSlice(10)
		now := time.Now()

		for i := 0; i < 3; i++ {
			dp := createTestDataPoint(now.Add(time.Duration(i)*time.Second), "device1", FlushStatusNew)
			os.Add(dp)
		}

		count, removedSize := os.RemoveUpToIndex(-1)

		if count != 0 {
			t.Errorf("Expected count 0, got %d", count)
		}
		if removedSize != 0 {
			t.Errorf("Expected removedSize 0, got %d", removedSize)
		}
	})

	t.Run("IndexBeyondLength", func(t *testing.T) {
		os := NewOrderedSlice(10)
		now := time.Now()

		for i := 0; i < 3; i++ {
			dp := createTestDataPoint(now.Add(time.Duration(i)*time.Second), "device1", FlushStatusNew)
			os.Add(dp)
		}

		count, removedSize := os.RemoveUpToIndex(10)

		if count != 0 {
			t.Errorf("Expected count 0, got %d", count)
		}
		if removedSize != 0 {
			t.Errorf("Expected removedSize 0, got %d", removedSize)
		}
	})

	t.Run("RemoveAll", func(t *testing.T) {
		os := NewOrderedSlice(10)
		now := time.Now()

		for i := 0; i < 3; i++ {
			dp := createTestDataPoint(now.Add(time.Duration(i)*time.Second), "device1", FlushStatusNew)
			os.Add(dp)
		}

		count, removedSize := os.RemoveUpToIndex(3)

		if count != 3 {
			t.Errorf("Expected count 3, got %d", count)
		}
		if removedSize == 0 {
			t.Error("Expected non-zero removedSize")
		}
		if os.Len() != 0 {
			t.Errorf("Expected 0 items remaining, got %d", os.Len())
		}
	})
}

func TestOrderedSlice_RemoveByStatus(t *testing.T) {
	t.Run("EmptySlice", func(t *testing.T) {
		os := NewOrderedSlice(10)

		count := os.RemoveByStatus(FlushStatusFlushed)

		if count != 0 {
			t.Errorf("Expected count 0, got %d", count)
		}
	})

	t.Run("NoMatch", func(t *testing.T) {
		os := NewOrderedSlice(10)
		now := time.Now()

		// Add items with FlushStatusNew
		for i := 0; i < 3; i++ {
			dp := createTestDataPoint(now.Add(time.Duration(i)*time.Second), "device1", FlushStatusNew)
			os.Add(dp)
		}

		// Try to remove FlushStatusFlushed
		count := os.RemoveByStatus(FlushStatusFlushed)

		if count != 0 {
			t.Errorf("Expected count 0, got %d", count)
		}
		if os.Len() != 3 {
			t.Errorf("Expected 3 items, got %d", os.Len())
		}
	})

	t.Run("RemoveSome", func(t *testing.T) {
		os := NewOrderedSlice(10)
		now := time.Now()

		// Add items with mixed statuses
		os.Add(createTestDataPoint(now, "device1", FlushStatusNew))
		os.Add(createTestDataPoint(now.Add(time.Second), "device1", FlushStatusFlushed))
		os.Add(createTestDataPoint(now.Add(2*time.Second), "device1", FlushStatusNew))
		os.Add(createTestDataPoint(now.Add(3*time.Second), "device1", FlushStatusFlushed))

		count := os.RemoveByStatus(FlushStatusFlushed)

		if count != 2 {
			t.Errorf("Expected count 2, got %d", count)
		}
		if os.Len() != 2 {
			t.Errorf("Expected 2 items remaining, got %d", os.Len())
		}
	})

	t.Run("RemoveAll", func(t *testing.T) {
		os := NewOrderedSlice(10)
		now := time.Now()

		// Add all items with same status
		for i := 0; i < 3; i++ {
			dp := createTestDataPoint(now.Add(time.Duration(i)*time.Second), "device1", FlushStatusFlushed)
			os.Add(dp)
		}

		count := os.RemoveByStatus(FlushStatusFlushed)

		if count != 3 {
			t.Errorf("Expected count 3, got %d", count)
		}
		if os.Len() != 0 {
			t.Errorf("Expected 0 items remaining, got %d", os.Len())
		}
	})
}

func TestOrderedSlice_EstimateMemory(t *testing.T) {
	t.Run("EmptySlice", func(t *testing.T) {
		os := NewOrderedSlice(10)

		mem := os.EstimateMemory()

		// Should have base overhead
		if mem <= 0 {
			t.Errorf("Expected positive memory estimate, got %d", mem)
		}
	})

	t.Run("WithItems", func(t *testing.T) {
		os := NewOrderedSlice(100)
		now := time.Now()

		emptyMem := os.EstimateMemory()

		// Add some items
		for i := 0; i < 50; i++ {
			dp := createTestDataPoint(now.Add(time.Duration(i)*time.Second), "device1", FlushStatusNew)
			os.Add(dp)
		}

		withItemsMem := os.EstimateMemory()

		// Memory should increase with items
		if withItemsMem <= emptyMem {
			t.Errorf("Memory should increase with items: empty=%d, withItems=%d", emptyMem, withItemsMem)
		}
	})

	t.Run("MemoryGrowsWithCapacity", func(t *testing.T) {
		small := NewOrderedSlice(10)
		large := NewOrderedSlice(1000)

		smallMem := small.EstimateMemory()
		largeMem := large.EstimateMemory()

		// Larger capacity should use more memory
		if largeMem <= smallMem {
			t.Errorf("Larger capacity should use more memory: small=%d, large=%d", smallMem, largeMem)
		}
	})
}
