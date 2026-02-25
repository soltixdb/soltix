package aggregation

import (
	"os"
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/logging"
)

// TestAddRowAllMetrics_InconsistentFields tests that AddRowAllMetrics correctly
// pads columns when rows have different field sets, preventing column misalignment.
//
// This is the root cause of the bug where hourly aggregated data shows values
// at wrong time positions when some hours have fields (e.g., soc/soh) and
// others don't (null/missing data).
func TestAddRowAllMetrics_InconsistentFields(t *testing.T) {
	cd := NewColumnarData(4)

	// Hour 0: has soc and soh
	cd.AddRowAllMetrics(1000, "dev1", map[string]*AggregatedField{
		"soc": {Sum: 100, Avg: 50, Min: 10, Max: 90, Count: 10},
		"soh": {Sum: 200, Avg: 100, Min: 20, Max: 180, Count: 10},
	})

	// Hour 1: NO soc or soh (null data)
	cd.AddRowAllMetrics(2000, "dev1", map[string]*AggregatedField{})

	// Hour 2: NO soc or soh (null data)
	cd.AddRowAllMetrics(3000, "dev1", map[string]*AggregatedField{})

	// Hour 3: has soc and soh again
	cd.AddRowAllMetrics(4000, "dev1", map[string]*AggregatedField{
		"soc": {Sum: 300, Avg: 150, Min: 30, Max: 270, Count: 10},
		"soh": {Sum: 400, Avg: 200, Min: 40, Max: 360, Count: 10},
	})

	// Verify all columns have the same length as timestamps
	expectedLen := len(cd.Timestamps)
	if expectedLen != 4 {
		t.Fatalf("Expected 4 timestamps, got %d", expectedLen)
	}

	// Check float columns alignment
	for key, col := range cd.FloatColumns {
		if len(col) != expectedLen {
			t.Errorf("FloatColumn %q has length %d, expected %d", key, len(col), expectedLen)
		}
	}

	// Check int columns alignment
	for key, col := range cd.IntColumns {
		if len(col) != expectedLen {
			t.Errorf("IntColumn %q has length %d, expected %d", key, len(col), expectedLen)
		}
	}

	// Verify soc sum values are at correct positions
	socSumKey := v6FloatKey(MetricSum, "soc")
	socCol := cd.FloatColumns[socSumKey]
	if socCol[0] != 100 {
		t.Errorf("soc_sum[0] = %f, want 100 (hour 0 with data)", socCol[0])
	}
	if socCol[1] != 0 {
		t.Errorf("soc_sum[1] = %f, want 0 (hour 1 without data)", socCol[1])
	}
	if socCol[2] != 0 {
		t.Errorf("soc_sum[2] = %f, want 0 (hour 2 without data)", socCol[2])
	}
	if socCol[3] != 300 {
		t.Errorf("soc_sum[3] = %f, want 300 (hour 3 with data)", socCol[3])
	}

	// Verify soh count values are at correct positions
	sohCountKey := v6IntKey("soh")
	sohCountCol := cd.IntColumns[sohCountKey]
	if sohCountCol[0] != 10 {
		t.Errorf("soh_count[0] = %d, want 10", sohCountCol[0])
	}
	if sohCountCol[1] != 0 {
		t.Errorf("soh_count[1] = %d, want 0", sohCountCol[1])
	}
	if sohCountCol[2] != 0 {
		t.Errorf("soh_count[2] = %d, want 0", sohCountCol[2])
	}
	if sohCountCol[3] != 10 {
		t.Errorf("soh_count[3] = %d, want 10", sohCountCol[3])
	}
}

// TestAddRowAllMetrics_FieldAppearsLater tests alignment when a new field
// first appears in a later row (not the first row).
func TestAddRowAllMetrics_FieldAppearsLater(t *testing.T) {
	cd := NewColumnarData(3)

	// Row 0: only has soc
	cd.AddRowAllMetrics(1000, "dev1", map[string]*AggregatedField{
		"soc": {Sum: 100, Avg: 50, Min: 10, Max: 90, Count: 10},
	})

	// Row 1: has soc AND soh (soh appears for the first time)
	cd.AddRowAllMetrics(2000, "dev1", map[string]*AggregatedField{
		"soc": {Sum: 200, Avg: 100, Min: 20, Max: 180, Count: 20},
		"soh": {Sum: 150, Avg: 75, Min: 15, Max: 135, Count: 15},
	})

	// Row 2: only soc again
	cd.AddRowAllMetrics(3000, "dev1", map[string]*AggregatedField{
		"soc": {Sum: 300, Avg: 150, Min: 30, Max: 270, Count: 30},
	})

	expectedLen := 3

	// All columns must be length 3
	for key, col := range cd.FloatColumns {
		if len(col) != expectedLen {
			t.Errorf("FloatColumn %q has length %d, expected %d", key, len(col), expectedLen)
		}
	}
	for key, col := range cd.IntColumns {
		if len(col) != expectedLen {
			t.Errorf("IntColumn %q has length %d, expected %d", key, len(col), expectedLen)
		}
	}

	// soh_sum should be [0, 150, 0]
	sohSumKey := v6FloatKey(MetricSum, "soh")
	sohCol := cd.FloatColumns[sohSumKey]
	if sohCol[0] != 0 {
		t.Errorf("soh_sum[0] = %f, want 0 (soh not present in row 0)", sohCol[0])
	}
	if sohCol[1] != 150 {
		t.Errorf("soh_sum[1] = %f, want 150", sohCol[1])
	}
	if sohCol[2] != 0 {
		t.Errorf("soh_sum[2] = %f, want 0 (soh not present in row 2)", sohCol[2])
	}

	// soc_sum should be [100, 200, 300]
	socSumKey := v6FloatKey(MetricSum, "soc")
	socCol := cd.FloatColumns[socSumKey]
	if socCol[0] != 100 {
		t.Errorf("soc_sum[0] = %f, want 100", socCol[0])
	}
	if socCol[1] != 200 {
		t.Errorf("soc_sum[1] = %f, want 200", socCol[1])
	}
	if socCol[2] != 300 {
		t.Errorf("soc_sum[2] = %f, want 300", socCol[2])
	}
}

// TestWriteReadHourly_InconsistentFields verifies the full write/read cycle
// with inconsistent fields across hours (simulating the real-world bug scenario).
func TestWriteReadHourly_InconsistentFields(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "hourly_align_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tmpDir, logger)

	jst := time.FixedZone("JST", 9*60*60)
	storage.SetTimezone(jst)

	// Simulate the user's scenario:
	// Hours 0-2 JST have soc/soh data, hours 3-5 have null/empty fields
	dayStart := time.Date(2026, 2, 24, 0, 0, 0, 0, jst)

	points := []*AggregatedPoint{
		// Hours with data
		{DeviceID: "dev1", Time: dayStart, Fields: map[string]*AggregatedField{
			"soc": {Sum: 360, Avg: 1, Min: 1, Max: 1, Count: 360},
			"soh": {Sum: 360, Avg: 1, Min: 1, Max: 1, Count: 360},
		}},
		{DeviceID: "dev1", Time: dayStart.Add(1 * time.Hour), Fields: map[string]*AggregatedField{
			"soc": {Sum: 360, Avg: 1, Min: 1, Max: 1, Count: 360},
			"soh": {Sum: 360, Avg: 1, Min: 1, Max: 1, Count: 360},
		}},
		{DeviceID: "dev1", Time: dayStart.Add(2 * time.Hour), Fields: map[string]*AggregatedField{
			"soc": {Sum: 201, Avg: 1, Min: 1, Max: 1, Count: 201},
			"soh": {Sum: 201, Avg: 1, Min: 1, Max: 1, Count: 201},
		}},
		// Hours without soc/soh (null data - only _inserted_at as example)
		{DeviceID: "dev1", Time: dayStart.Add(3 * time.Hour), Fields: map[string]*AggregatedField{
			"_inserted_at": {Sum: 100, Avg: 100, Min: 100, Max: 100, Count: 360},
		}},
		{DeviceID: "dev1", Time: dayStart.Add(4 * time.Hour), Fields: map[string]*AggregatedField{
			"_inserted_at": {Sum: 200, Avg: 200, Min: 200, Max: 200, Count: 360},
		}},
		{DeviceID: "dev1", Time: dayStart.Add(5 * time.Hour), Fields: map[string]*AggregatedField{
			"_inserted_at": {Sum: 300, Avg: 300, Min: 300, Max: 300, Count: 360},
		}},
	}

	// Write
	if err := storage.WriteHourly("testdb", "testcol", points); err != nil {
		t.Fatalf("WriteHourly failed: %v", err)
	}

	// Read back
	readPoints, err := storage.ReadHourly("testdb", "testcol", dayStart.Format("20060102"))
	if err != nil {
		t.Fatalf("ReadHourly failed: %v", err)
	}

	if len(readPoints) != 6 {
		t.Fatalf("Expected 6 points, got %d", len(readPoints))
	}

	// Verify each point has correct time-to-value mapping
	for _, p := range readPoints {
		hourInJST := p.Time.In(jst).Hour()

		socField := p.Fields["soc"]
		sohField := p.Fields["soh"]

		switch hourInJST {
		case 0, 1:
			if socField == nil || socField.Sum != 360 {
				t.Errorf("Hour %d: expected soc.Sum=360, got %v", hourInJST, socField)
			}
			if sohField == nil || sohField.Sum != 360 {
				t.Errorf("Hour %d: expected soh.Sum=360, got %v", hourInJST, sohField)
			}
		case 2:
			if socField == nil || socField.Sum != 201 {
				t.Errorf("Hour %d: expected soc.Sum=201, got %v", hourInJST, socField)
			}
			if sohField == nil || sohField.Sum != 201 {
				t.Errorf("Hour %d: expected soh.Sum=201, got %v", hourInJST, sohField)
			}
		case 3, 4, 5:
			// soc/soh should be zero or absent
			if socField != nil && socField.Sum != 0 {
				t.Errorf("Hour %d: expected soc.Sum=0 or nil, got Sum=%f", hourInJST, socField.Sum)
			}
			if sohField != nil && sohField.Sum != 0 {
				t.Errorf("Hour %d: expected soh.Sum=0 or nil, got Sum=%f", hourInJST, sohField.Sum)
			}
		}
	}
}
