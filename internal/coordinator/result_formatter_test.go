package coordinator

import (
	"testing"

	"github.com/soltixdb/soltix/internal/models"
)

func TestResultFormatter_MergeAndFormat_Empty(t *testing.T) {
	f := NewResultFormatter()
	result := f.MergeAndFormat(nil, nil)
	if len(result) != 0 {
		t.Errorf("Expected empty result, got %d", len(result))
	}
}

func TestResultFormatter_MergeAndFormat_MultipleDevices(t *testing.T) {
	f := NewResultFormatter()

	results := []QueryResult{
		{
			DeviceID: "dev1",
			DataPoints: []models.DataPointView{
				{Time: "2026-01-01T00:00:00Z", Fields: map[string]interface{}{"temp": 20.0}},
				{Time: "2026-01-01T01:00:00Z", Fields: map[string]interface{}{"temp": 21.0}},
			},
		},
		{
			DeviceID: "dev2",
			DataPoints: []models.DataPointView{
				{Time: "2026-01-01T00:00:00Z", Fields: map[string]interface{}{"temp": 30.0}},
			},
		},
	}

	formatted := f.MergeAndFormat(results, nil)
	if len(formatted) != 2 {
		t.Fatalf("Expected 2 devices, got %d", len(formatted))
	}
}

func TestResultFormatter_MergeAndFormat_WithRequestedFields(t *testing.T) {
	f := NewResultFormatter()

	results := []QueryResult{
		{
			DeviceID: "dev1",
			DataPoints: []models.DataPointView{
				{Time: "2026-01-01T00:00:00Z", Fields: map[string]interface{}{"temp": 20.0, "humidity": 50.0}},
			},
		},
	}

	formatted := f.MergeAndFormat(results, []string{"temp"})
	if len(formatted) != 1 {
		t.Fatalf("Expected 1 device, got %d", len(formatted))
	}

	// Should only have "temp" field, not "humidity"
	if _, ok := formatted[0].Fields["humidity"]; ok {
		t.Error("Should not include unrequested 'humidity' field")
	}
	if _, ok := formatted[0].Fields["temp"]; !ok {
		t.Error("Should include requested 'temp' field")
	}
}

func TestResultFormatter_MergeAndFormat_DuplicateTimes(t *testing.T) {
	f := NewResultFormatter()

	// Same device from two results with overlapping times
	results := []QueryResult{
		{
			DeviceID: "dev1",
			DataPoints: []models.DataPointView{
				{Time: "2026-01-01T00:00:00Z", Fields: map[string]interface{}{"temp": 20.0}},
			},
		},
		{
			DeviceID: "dev1",
			DataPoints: []models.DataPointView{
				{Time: "2026-01-01T00:00:00Z", Fields: map[string]interface{}{"temp": 25.0}},
			},
		},
	}

	formatted := f.MergeAndFormat(results, nil)
	if len(formatted) != 1 {
		t.Fatalf("Expected 1 device (merged), got %d", len(formatted))
	}
	if len(formatted[0].Times) != 1 {
		t.Errorf("Expected 1 unique time, got %d", len(formatted[0].Times))
	}
	// Later value should overwrite
	if formatted[0].Fields["temp"][0] != 25.0 {
		t.Errorf("Expected overwritten value 25.0, got %v", formatted[0].Fields["temp"][0])
	}
}

func TestResultFormatter_ApplyLimit_NoLimit(t *testing.T) {
	f := NewResultFormatter()
	results := []FormattedQueryResult{
		{DeviceID: "d1", Times: []string{"t1", "t2", "t3"}},
	}

	limited := f.ApplyLimit(results, 0)
	if len(limited) != 1 || len(limited[0].Times) != 3 {
		t.Error("ApplyLimit(0) should return all results")
	}

	limited2 := f.ApplyLimit(results, -1)
	if len(limited2) != 1 || len(limited2[0].Times) != 3 {
		t.Error("ApplyLimit(-1) should return all results")
	}
}

func TestResultFormatter_ApplyLimit_Truncate(t *testing.T) {
	f := NewResultFormatter()
	results := []FormattedQueryResult{
		{
			DeviceID: "d1",
			Times:    []string{"t1", "t2", "t3"},
			Fields:   map[string][]interface{}{"temp": {1.0, 2.0, 3.0}},
		},
	}

	limited := f.ApplyLimit(results, 2)
	if len(limited) != 1 {
		t.Fatalf("Expected 1 device, got %d", len(limited))
	}
	if len(limited[0].Times) != 2 {
		t.Errorf("Expected 2 times after limit, got %d", len(limited[0].Times))
	}
	if len(limited[0].Fields["temp"]) != 2 {
		t.Errorf("Expected 2 field values after limit, got %d", len(limited[0].Fields["temp"]))
	}
}

func TestResultFormatter_ApplyLimit_AcrossDevices(t *testing.T) {
	f := NewResultFormatter()
	results := []FormattedQueryResult{
		{DeviceID: "d1", Times: []string{"t1", "t2", "t3"}},
		{DeviceID: "d2", Times: []string{"t4", "t5"}},
		{DeviceID: "d3", Times: []string{"t6"}},
	}

	// Limit of 4: should take all 3 from d1 + 1 from d2
	limited := f.ApplyLimit(results, 4)
	total := f.CountPoints(limited)
	if total != 4 {
		t.Errorf("Expected 4 total points, got %d", total)
	}
}

func TestResultFormatter_CountPoints(t *testing.T) {
	f := NewResultFormatter()
	results := []FormattedQueryResult{
		{Times: []string{"a", "b"}},
		{Times: []string{"c"}},
		{Times: []string{}},
	}
	if f.CountPoints(results) != 3 {
		t.Errorf("Expected 3, got %d", f.CountPoints(results))
	}
	if f.CountPoints(nil) != 0 {
		t.Error("Expected 0 for nil")
	}
}

// ===========================================================================
// NodeHasher interface — verify all implementations satisfy it
// ===========================================================================

func TestNodeHasherInterface_ConsistentHash(t *testing.T) {
	var _ NodeHasher = &ConsistentHash{}
	// If this compiles, ConsistentHash satisfies NodeHasher
}

func TestNodeHasherInterface_RendezvousHash(t *testing.T) {
	var _ NodeHasher = &RendezvousHash{}
}

// ===========================================================================
// FormattedQueryResult — MarshalJSON edge cases
// ===========================================================================

func TestFormattedQueryResult_MarshalJSON_EmptyFields(t *testing.T) {
	fqr := FormattedQueryResult{
		DeviceID: "device-1",
		Times:    []string{},
		Fields:   map[string][]interface{}{},
	}

	data, err := fqr.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON failed: %v", err)
	}
	if len(data) == 0 {
		t.Error("Expected non-empty JSON")
	}
}

func TestFormattedQueryResult_MarshalJSON_WithFields(t *testing.T) {
	fqr := FormattedQueryResult{
		DeviceID: "sensor-1",
		Times:    []string{"2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z"},
		Fields: map[string][]interface{}{
			"temperature": {23.5, 24.1},
			"humidity":    {65.0, nil},
		},
	}

	data, err := fqr.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON failed: %v", err)
	}

	// Verify it contains expected keys
	jsonStr := string(data)
	for _, expected := range []string{"sensor-1", "times", "temperature", "humidity"} {
		if !containsSubstr(jsonStr, expected) {
			t.Errorf("JSON missing expected key %q: %s", expected, jsonStr)
		}
	}
}
