package downsampling

import (
	"math"
	"testing"
)

func TestIsValid(t *testing.T) {
	tests := []struct {
		mode  string
		valid bool
	}{
		{"none", true},
		{"auto", true},
		{"lttb", true},
		{"invalid", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.mode, func(t *testing.T) {
			if got := IsValid(tt.mode); got != tt.valid {
				t.Errorf("IsValid(%q) = %v, want %v", tt.mode, got, tt.valid)
			}
		})
	}
}

func TestApplyDownsampling_None(t *testing.T) {
	times := []string{"2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z", "2024-01-01T02:00:00Z"}
	values := []interface{}{1.0, 2.0, 3.0}

	resultTimes, resultValues, err := ApplyDownsampling(times, values, ModeNone, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resultTimes) != len(times) {
		t.Errorf("expected %d times, got %d", len(times), len(resultTimes))
	}

	if len(resultValues) != len(values) {
		t.Errorf("expected %d values, got %d", len(values), len(resultValues))
	}
}

func TestApplyDownsampling_Auto_BelowThreshold(t *testing.T) {
	times := []string{"2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z", "2024-01-01T02:00:00Z"}
	values := []interface{}{1.0, 2.0, 3.0}

	// Threshold of 1000, data has only 3 points - should not downsample
	resultTimes, resultValues, err := ApplyDownsampling(times, values, ModeAuto, DefaultAutoThreshold)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resultTimes) != len(times) {
		t.Errorf("expected %d times (no downsampling), got %d", len(times), len(resultTimes))
	}

	if len(resultValues) != len(values) {
		t.Errorf("expected %d values (no downsampling), got %d", len(values), len(resultValues))
	}
}

func TestApplyDownsampling_Auto_AboveThreshold(t *testing.T) {
	// Create 2000 points
	times := make([]string, 2000)
	values := make([]interface{}, 2000)
	for i := 0; i < 2000; i++ {
		times[i] = "2024-01-01T00:00:00Z" // Simplified for test
		values[i] = float64(i)
	}

	// Auto mode with default threshold of 1000 - should downsample
	resultTimes, resultValues, err := ApplyDownsampling(times, values, ModeAuto, DefaultAutoThreshold)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have approximately 1000 points (threshold)
	if len(resultTimes) >= 2000 {
		t.Errorf("expected downsampling to reduce points, got %d points", len(resultTimes))
	}

	if len(resultValues) >= 2000 {
		t.Errorf("expected downsampling to reduce points, got %d values", len(resultValues))
	}

	if len(resultTimes) != len(resultValues) {
		t.Errorf("times and values length mismatch: %d != %d", len(resultTimes), len(resultValues))
	}
}

func TestApplyDownsampling_LTTB(t *testing.T) {
	// Create test data with clear pattern
	times := make([]string, 500)
	values := make([]interface{}, 500)
	for i := 0; i < 500; i++ {
		times[i] = "2024-01-01T00:00:00Z" // Simplified for test
		values[i] = float64(i)
	}

	// Downsample to 100 points (minimum threshold)
	resultTimes, resultValues, err := ApplyDownsampling(times, values, ModeLTTB, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resultTimes) != 100 {
		t.Errorf("expected 100 times, got %d", len(resultTimes))
	}

	if len(resultValues) != 100 {
		t.Errorf("expected 100 values, got %d", len(resultValues))
	}

	// First and last points should always be included
	if resultValues[0] != values[0] {
		t.Errorf("expected first value to be %v, got %v", values[0], resultValues[0])
	}

	if resultValues[len(resultValues)-1] != values[len(values)-1] {
		t.Errorf("expected last value to be %v, got %v", values[len(values)-1], resultValues[len(resultValues)-1])
	}
}

func TestApplyDownsampling_LTTB_BelowThreshold(t *testing.T) {
	times := []string{"2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z", "2024-01-01T02:00:00Z"}
	values := []interface{}{1.0, 2.0, 3.0}

	// Threshold of 100, data has only 3 points - should not downsample
	resultTimes, resultValues, err := ApplyDownsampling(times, values, ModeLTTB, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resultTimes) != len(times) {
		t.Errorf("expected %d times (no downsampling), got %d", len(times), len(resultTimes))
	}

	if len(resultValues) != len(values) {
		t.Errorf("expected %d values (no downsampling), got %d", len(values), len(resultValues))
	}
}

func TestApplyDownsampling_WithNilValues(t *testing.T) {
	times := []string{"T1", "T2", "T3", "T4", "T5"}
	values := []interface{}{1.0, nil, 3.0, nil, 5.0}

	// Should skip nil values - we have 3 non-nil values, downsample to 3 (which means no downsampling)
	resultTimes, resultValues, err := ApplyDownsampling(times, values, ModeLTTB, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Original arrays returned unchanged when no downsampling occurs
	if len(resultTimes) != len(times) {
		t.Errorf("expected %d times, got %d", len(times), len(resultTimes))
	}

	if len(resultValues) != len(values) {
		t.Errorf("expected %d values, got %d", len(values), len(resultValues))
	}
}

func TestApplyDownsampling_EmptyData(t *testing.T) {
	times := []string{}
	values := []interface{}{}

	resultTimes, resultValues, err := ApplyDownsampling(times, values, ModeLTTB, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resultTimes) != 0 {
		t.Errorf("expected 0 times, got %d", len(resultTimes))
	}

	if len(resultValues) != 0 {
		t.Errorf("expected 0 values, got %d", len(resultValues))
	}
}

func TestApplyDownsampling_MismatchedArrays(t *testing.T) {
	times := []string{"T1", "T2"}
	values := []interface{}{1.0, 2.0, 3.0}

	_, _, err := ApplyDownsampling(times, values, ModeLTTB, 10)
	if err == nil {
		t.Error("expected error for mismatched arrays, got nil")
	}
}

func TestApplyDownsampling_MixedTypes(t *testing.T) {
	times := make([]string, 500)
	values := make([]interface{}, 500)

	for i := 0; i < 500; i++ {
		switch i % 5 {
		case 0:
			values[i] = float64(i)
		case 1:
			values[i] = float32(i)
		case 2:
			values[i] = int(i)
		case 3:
			values[i] = int64(i)
		case 4:
			values[i] = int32(i)
		}
		times[i] = "T" + string(rune(i))
	}

	resultTimes, resultValues, err := ApplyDownsampling(times, values, ModeLTTB, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resultTimes) != 100 {
		t.Errorf("expected 100 times, got %d", len(resultTimes))
	}

	if len(resultValues) != 100 {
		t.Errorf("expected 100 values, got %d", len(resultValues))
	}
}

func TestIsValid_NewModes(t *testing.T) {
	tests := []struct {
		mode  string
		valid bool
	}{
		{"minmax", true},
		{"avg", true},
		{"m4", true},
	}

	for _, tt := range tests {
		t.Run(tt.mode, func(t *testing.T) {
			if got := IsValid(tt.mode); got != tt.valid {
				t.Errorf("IsValid(%q) = %v, want %v", tt.mode, got, tt.valid)
			}
		})
	}
}

func TestApplyDownsampling_MinMax(t *testing.T) {
	// Create test data with clear peaks
	times := make([]string, 100)
	values := make([]interface{}, 100)
	for i := 0; i < 100; i++ {
		times[i] = "2024-01-01T00:00:00Z"
		// Create a pattern: low-high-low-high...
		switch i % 10 {
		case 5:
			values[i] = 100.0 // Peak
		case 0:
			values[i] = 0.0 // Valley
		default:
			values[i] = 50.0
		}
	}

	// Downsample to ~20 points (10 buckets * 2 points)
	resultTimes, resultValues, err := ApplyDownsampling(times, values, ModeMinMax, 20)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have approximately threshold points
	if len(resultTimes) > 25 || len(resultTimes) < 10 {
		t.Errorf("expected ~20 times for MinMax, got %d", len(resultTimes))
	}

	// Check that we preserved min (0) and max (100) values
	hasMin := false
	hasMax := false
	for _, v := range resultValues {
		if v.(float64) == 0.0 {
			hasMin = true
		}
		if v.(float64) == 100.0 {
			hasMax = true
		}
	}
	if !hasMin {
		t.Error("MinMax should preserve minimum value (0)")
	}
	if !hasMax {
		t.Error("MinMax should preserve maximum value (100)")
	}
}

func TestApplyDownsampling_Average(t *testing.T) {
	// Create test data
	times := make([]string, 100)
	values := make([]interface{}, 100)
	for i := 0; i < 100; i++ {
		times[i] = "2024-01-01T00:00:00Z"
		values[i] = float64(i) // 0, 1, 2, ..., 99
	}

	// Downsample to 10 buckets
	resultTimes, resultValues, err := ApplyDownsampling(times, values, ModeAverage, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resultTimes) != 10 {
		t.Errorf("expected 10 times for Average, got %d", len(resultTimes))
	}

	if len(resultValues) != 10 {
		t.Errorf("expected 10 values for Average, got %d", len(resultValues))
	}

	// First bucket (0-9) should average to 4.5
	// Second bucket (10-19) should average to 14.5, etc.
	firstAvg := resultValues[0].(float64)
	if firstAvg < 3.0 || firstAvg > 6.0 {
		t.Errorf("expected first bucket average ~4.5, got %v", firstAvg)
	}
}

func TestApplyDownsampling_M4(t *testing.T) {
	// Create test data with clear pattern
	times := make([]string, 100)
	values := make([]interface{}, 100)
	for i := 0; i < 100; i++ {
		times[i] = "2024-01-01T00:00:00Z"
		// Create varying pattern
		values[i] = float64(i % 20) // 0-19 repeating
	}

	// Downsample to ~20 points (5 buckets * 4 points max)
	resultTimes, resultValues, err := ApplyDownsampling(times, values, ModeM4, 20)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// M4 returns up to 4 points per bucket
	if len(resultTimes) > 25 || len(resultTimes) < 5 {
		t.Errorf("expected 5-25 times for M4, got %d", len(resultTimes))
	}

	if len(resultTimes) != len(resultValues) {
		t.Errorf("times and values length mismatch: %d != %d", len(resultTimes), len(resultValues))
	}
}

func TestApplyDownsampling_MinMax_BelowThreshold(t *testing.T) {
	times := []string{"T1", "T2", "T3"}
	values := []interface{}{1.0, 2.0, 3.0}

	resultTimes, resultValues, err := ApplyDownsampling(times, values, ModeMinMax, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// No downsampling needed
	if len(resultTimes) != len(times) {
		t.Errorf("expected no downsampling, got %d times", len(resultTimes))
	}

	if len(resultValues) != len(values) {
		t.Errorf("expected no downsampling, got %d values", len(resultValues))
	}
}

func TestApplyDownsampling_Average_BelowThreshold(t *testing.T) {
	times := []string{"T1", "T2", "T3"}
	values := []interface{}{1.0, 2.0, 3.0}

	resultTimes, resultValues, err := ApplyDownsampling(times, values, ModeAverage, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// No downsampling needed
	if len(resultTimes) != len(times) {
		t.Errorf("expected no downsampling, got %d times", len(resultTimes))
	}

	if len(resultValues) != len(values) {
		t.Errorf("expected no downsampling, got %d values", len(resultValues))
	}
}

func TestApplyDownsampling_M4_BelowThreshold(t *testing.T) {
	times := []string{"T1", "T2", "T3"}
	values := []interface{}{1.0, 2.0, 3.0}

	resultTimes, resultValues, err := ApplyDownsampling(times, values, ModeM4, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// No downsampling needed
	if len(resultTimes) != len(times) {
		t.Errorf("expected no downsampling, got %d times", len(resultTimes))
	}

	if len(resultValues) != len(values) {
		t.Errorf("expected no downsampling, got %d values", len(resultValues))
	}
}

func TestDetectBestAlgorithm_SmoothData(t *testing.T) {
	// Create smooth, linear data
	points := make([]Point, 2000)
	for i := 0; i < 2000; i++ {
		points[i] = Point{TimeIndex: i, Value: float64(i) * 0.1} // Linear growth
	}

	mode := detectBestAlgorithm(points)
	// Smooth data should use LTTB
	if mode != ModeLTTB {
		t.Errorf("expected ModeLTTB for smooth data, got %s", mode)
	}
}

func TestDetectBestAlgorithm_SpikyData(t *testing.T) {
	// Create very spiky data with frequent large outliers
	points := make([]Point, 2000)
	for i := 0; i < 2000; i++ {
		if i%5 == 0 {
			// Every 5th point is a spike (20% of data are outliers)
			points[i] = Point{TimeIndex: i, Value: 1000.0}
		} else {
			points[i] = Point{TimeIndex: i, Value: 10.0}
		}
	}

	mode := detectBestAlgorithm(points)
	// Spiky data should use MinMax or M4 to preserve peaks
	if mode != ModeMinMax && mode != ModeM4 {
		t.Errorf("expected ModeMinMax or ModeM4 for spiky data, got %s", mode)
	}
}

func TestDetectBestAlgorithm_MediumVariance(t *testing.T) {
	// Create data with medium variance (sine wave)
	points := make([]Point, 2000)
	for i := 0; i < 2000; i++ {
		points[i] = Point{TimeIndex: i, Value: 50.0 + 30.0*math.Sin(float64(i)*0.1)}
	}

	mode := detectBestAlgorithm(points)
	// Medium variance could be LTTB or M4 (both acceptable)
	if mode != ModeLTTB && mode != ModeM4 {
		t.Errorf("expected ModeLTTB or ModeM4 for medium variance data, got %s", mode)
	}
}

func TestDetectBestAlgorithm_LargeDataset(t *testing.T) {
	// Create large smooth dataset
	points := make([]Point, 150000)
	for i := 0; i < 150000; i++ {
		points[i] = Point{TimeIndex: i, Value: float64(i) * 0.01}
	}

	mode := detectBestAlgorithm(points)
	// Large smooth dataset should use Average for performance
	if mode != ModeAverage {
		t.Errorf("expected ModeAverage for large smooth dataset, got %s", mode)
	}
}

func TestDetectBestAlgorithm_LargeSpikyDataset(t *testing.T) {
	// Create large spiky dataset with frequent spikes
	points := make([]Point, 150000)
	for i := 0; i < 150000; i++ {
		if i%5 == 0 {
			// 20% are spikes - significant enough to trigger MinMax
			points[i] = Point{TimeIndex: i, Value: 1000.0}
		} else {
			points[i] = Point{TimeIndex: i, Value: 10.0}
		}
	}

	mode := detectBestAlgorithm(points)
	// Large spiky dataset should NOT use Average (preserve peaks)
	if mode == ModeAverage || mode == ModeLTTB {
		t.Errorf("expected ModeMinMax or ModeM4 for large spiky dataset, got %s", mode)
	}
}

func TestCalculateSpikiness_Smooth(t *testing.T) {
	// Constant values - should be 0
	points := make([]Point, 100)
	for i := 0; i < 100; i++ {
		points[i] = Point{TimeIndex: i, Value: 50.0}
	}

	spikiness := calculateSpikiness(points)
	if spikiness != 0 {
		t.Errorf("expected spikiness 0 for constant data, got %f", spikiness)
	}
}

func TestCalculateSpikiness_VerySpiky(t *testing.T) {
	// Very spiky data - alternating high/low
	points := make([]Point, 100)
	for i := 0; i < 100; i++ {
		if i%2 == 0 {
			points[i] = Point{TimeIndex: i, Value: 0.0}
		} else {
			points[i] = Point{TimeIndex: i, Value: 100.0}
		}
	}

	spikiness := calculateSpikiness(points)
	// Should be high spikiness (close to 1)
	if spikiness < 0.5 {
		t.Errorf("expected high spikiness for alternating data, got %f", spikiness)
	}
}

func TestApplyDownsampling_Auto_SelectsAppropriateAlgorithm(t *testing.T) {
	// Create spiky data
	times := make([]string, 2000)
	values := make([]interface{}, 2000)
	for i := 0; i < 2000; i++ {
		times[i] = "2024-01-01T00:00:00Z"
		if i%50 == 0 {
			values[i] = 1000.0 // Spike
		} else {
			values[i] = 10.0
		}
	}

	// Auto mode should detect spiky data and downsample
	resultTimes, resultValues, err := ApplyDownsampling(times, values, ModeAuto, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have downsampled
	if len(resultTimes) >= 2000 {
		t.Errorf("expected downsampling, got %d points", len(resultTimes))
	}

	// Should preserve at least some spikes
	hasSpike := false
	for _, v := range resultValues {
		if v.(float64) > 500.0 {
			hasSpike = true
			break
		}
	}
	if !hasSpike {
		t.Error("auto mode should preserve spikes in spiky data")
	}
}
