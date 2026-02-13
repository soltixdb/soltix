package sync

import (
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/storage"
)

func TestDeduplicatePoints_Empty(t *testing.T) {
	result := deduplicatePoints([]*storage.DataPoint{})
	if len(result) != 0 {
		t.Errorf("Expected 0 points, got %d", len(result))
	}
}

func TestDeduplicatePoints_Nil(t *testing.T) {
	result := deduplicatePoints(nil)
	if result != nil {
		t.Errorf("Expected nil, got %v", result)
	}
}

func TestDeduplicatePoints_NoDuplicates(t *testing.T) {
	now := time.Now()
	points := []*storage.DataPoint{
		{Database: "db1", Collection: "col1", ID: "dev-1", Time: now, InsertedAt: now},
		{Database: "db1", Collection: "col1", ID: "dev-2", Time: now, InsertedAt: now},
		{Database: "db1", Collection: "col1", ID: "dev-1", Time: now.Add(1 * time.Minute), InsertedAt: now},
	}

	result := deduplicatePoints(points)
	if len(result) != 3 {
		t.Errorf("Expected 3 points (no duplicates), got %d", len(result))
	}
}

func TestDeduplicatePoints_WithDuplicates(t *testing.T) {
	now := time.Now()
	points := []*storage.DataPoint{
		{Database: "db1", Collection: "col1", ID: "dev-1", Time: now, InsertedAt: now},
		{Database: "db1", Collection: "col1", ID: "dev-1", Time: now, InsertedAt: now.Add(1 * time.Second)},  // newer duplicate
		{Database: "db1", Collection: "col1", ID: "dev-1", Time: now, InsertedAt: now.Add(-1 * time.Second)}, // older duplicate
	}

	result := deduplicatePoints(points)
	if len(result) != 1 {
		t.Fatalf("Expected 1 point after dedup, got %d", len(result))
	}

	// Should keep the newest InsertedAt
	if !result[0].InsertedAt.Equal(now.Add(1 * time.Second)) {
		t.Errorf("Expected newest InsertedAt to be kept")
	}
}

func TestDeduplicatePoints_SortedByTime(t *testing.T) {
	now := time.Now()
	points := []*storage.DataPoint{
		{Database: "db1", Collection: "col1", ID: "dev-1", Time: now.Add(2 * time.Minute), InsertedAt: now},
		{Database: "db1", Collection: "col1", ID: "dev-2", Time: now, InsertedAt: now},
		{Database: "db1", Collection: "col1", ID: "dev-3", Time: now.Add(1 * time.Minute), InsertedAt: now},
	}

	result := deduplicatePoints(points)
	if len(result) != 3 {
		t.Fatalf("Expected 3 points, got %d", len(result))
	}

	// Should be sorted by time ascending
	for i := 1; i < len(result); i++ {
		if result[i].Time.Before(result[i-1].Time) {
			t.Errorf("Points not sorted by time: [%d]=%v > [%d]=%v",
				i-1, result[i-1].Time, i, result[i].Time)
		}
	}
}

func TestDeduplicatePoints_DifferentDatabases(t *testing.T) {
	now := time.Now()
	points := []*storage.DataPoint{
		{Database: "db1", Collection: "col1", ID: "dev-1", Time: now, InsertedAt: now},
		{Database: "db2", Collection: "col1", ID: "dev-1", Time: now, InsertedAt: now}, // different DB
	}

	result := deduplicatePoints(points)
	if len(result) != 2 {
		t.Errorf("Expected 2 points (different DBs), got %d", len(result))
	}
}

func TestDeduplicatePoints_DifferentCollections(t *testing.T) {
	now := time.Now()
	points := []*storage.DataPoint{
		{Database: "db1", Collection: "col1", ID: "dev-1", Time: now, InsertedAt: now},
		{Database: "db1", Collection: "col2", ID: "dev-1", Time: now, InsertedAt: now}, // different collection
	}

	result := deduplicatePoints(points)
	if len(result) != 2 {
		t.Errorf("Expected 2 points (different collections), got %d", len(result))
	}
}

func TestDeduplicatePoints_SameKeyNewerWins(t *testing.T) {
	now := time.Now()
	olderInsertedAt := now.Add(-10 * time.Second)
	newerInsertedAt := now.Add(10 * time.Second)

	points := []*storage.DataPoint{
		{
			Database: "db1", Collection: "col1", ID: "dev-1", Time: now,
			Fields: map[string]interface{}{"v": 1.0}, InsertedAt: olderInsertedAt,
		},
		{
			Database: "db1", Collection: "col1", ID: "dev-1", Time: now,
			Fields: map[string]interface{}{"v": 2.0}, InsertedAt: newerInsertedAt,
		},
	}

	result := deduplicatePoints(points)
	if len(result) != 1 {
		t.Fatalf("Expected 1 point, got %d", len(result))
	}

	// The newer point (v=2.0) should win
	if result[0].Fields["v"] != 2.0 {
		t.Errorf("Expected newer point (v=2.0) to win, got v=%v", result[0].Fields["v"])
	}
}

func TestDeduplicatePoints_LargeDataset(t *testing.T) {
	now := time.Now()
	points := make([]*storage.DataPoint, 0, 1000)

	// 500 unique + 500 duplicates
	for i := 0; i < 500; i++ {
		ts := now.Add(time.Duration(i) * time.Minute)
		points = append(points, &storage.DataPoint{
			Database:   "db1",
			Collection: "col1",
			ID:         "dev-1",
			Time:       ts,
			InsertedAt: now,
		})
		// Duplicate with older InsertedAt
		points = append(points, &storage.DataPoint{
			Database:   "db1",
			Collection: "col1",
			ID:         "dev-1",
			Time:       ts,
			InsertedAt: now.Add(-1 * time.Second),
		})
	}

	result := deduplicatePoints(points)
	if len(result) != 500 {
		t.Errorf("Expected 500 unique points, got %d", len(result))
	}
}

func TestDeduplicatePoints_SinglePoint(t *testing.T) {
	now := time.Now()
	points := []*storage.DataPoint{
		{Database: "db1", Collection: "col1", ID: "dev-1", Time: now, InsertedAt: now},
	}

	result := deduplicatePoints(points)
	if len(result) != 1 {
		t.Errorf("Expected 1 point, got %d", len(result))
	}
}

func TestNewLocalStorageAdapter(t *testing.T) {
	adapter := NewLocalStorageAdapter(nil, nil, nil, nil)
	if adapter == nil {
		t.Fatal("Expected non-nil adapter")
	}
	if adapter.columnarStorage != nil {
		t.Error("Expected nil columnarStorage")
	}
	if adapter.memoryStore != nil {
		t.Error("Expected nil memoryStore")
	}
	if adapter.writeWorkerPool != nil {
		t.Error("Expected nil writeWorkerPool")
	}
}

func TestNewLocalStorageAdapter_WriteToWAL_NilPool(t *testing.T) {
	adapter := NewLocalStorageAdapter(nil, nil, nil, nil)

	err := adapter.WriteToWAL(nil)
	if err == nil {
		t.Fatal("Expected error when writeWorkerPool is nil")
	}
}

func TestNewLocalStorageAdapter_WriteToWALSync_NilPool(t *testing.T) {
	adapter := NewLocalStorageAdapter(nil, nil, nil, nil)

	err := adapter.WriteToWALSync(nil)
	if err == nil {
		t.Fatal("Expected error when writeWorkerPool is nil")
	}
}
