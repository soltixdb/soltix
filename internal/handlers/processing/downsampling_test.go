package processing

import (
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/logging"
)

func TestApplyDownsampling_MinMax(t *testing.T) {
	logger := logging.NewDevelopment()
	p := NewProcessor(logger)

	// Create test data with spiky data (good for minmax)
	times := make([]string, 100)
	tempValues := make([]interface{}, 100)
	for i := 0; i < 100; i++ {
		tm := time.Date(2024, 1, 1, 0, i, 0, 0, time.UTC)
		times[i] = tm.Format(time.RFC3339)
		// Create spiky pattern
		if i%2 == 0 {
			tempValues[i] = 100.0
		} else {
			tempValues[i] = 0.0
		}
	}

	results := []QueryResult{
		{
			DeviceID: "device1",
			Times:    times,
			Fields: map[string][]interface{}{
				"temperature": tempValues,
			},
		},
	}

	downsampled := p.ApplyDownsampling(results, "minmax", 20)

	if len(downsampled) != 1 {
		t.Errorf("Expected 1 device, got %d", len(downsampled))
	}

	// MinMax should reduce points
	if len(downsampled[0].Times) >= 100 {
		t.Errorf("Expected fewer than 100 points after minmax, got %d", len(downsampled[0].Times))
	}
}

func TestApplyDownsampling_Average(t *testing.T) {
	logger := logging.NewDevelopment()
	p := NewProcessor(logger)

	times := make([]string, 100)
	tempValues := make([]interface{}, 100)
	for i := 0; i < 100; i++ {
		tm := time.Date(2024, 1, 1, 0, i, 0, 0, time.UTC)
		times[i] = tm.Format(time.RFC3339)
		tempValues[i] = float64(i)
	}

	results := []QueryResult{
		{
			DeviceID: "device1",
			Times:    times,
			Fields: map[string][]interface{}{
				"temperature": tempValues,
			},
		},
	}

	downsampled := p.ApplyDownsampling(results, "avg", 10)

	if len(downsampled) != 1 {
		t.Errorf("Expected 1 device, got %d", len(downsampled))
	}

	// Average should reduce to ~10 points
	if len(downsampled[0].Times) > 15 {
		t.Errorf("Expected ~10 points after avg, got %d", len(downsampled[0].Times))
	}
}

func TestApplyDownsampling_LTTB(t *testing.T) {
	logger := logging.NewDevelopment()
	p := NewProcessor(logger)

	// Create more data points to ensure downsampling happens
	times := make([]string, 200)
	tempValues := make([]interface{}, 200)
	for i := 0; i < 200; i++ {
		tm := time.Date(2024, 1, 1, 0, i, 0, 0, time.UTC)
		times[i] = tm.Format(time.RFC3339)
		tempValues[i] = float64(i % 10)
	}

	results := []QueryResult{
		{
			DeviceID: "device1",
			Times:    times,
			Fields: map[string][]interface{}{
				"temperature": tempValues,
			},
		},
	}

	downsampled := p.ApplyDownsampling(results, "lttb", 50)

	if len(downsampled) != 1 {
		t.Errorf("Expected 1 device, got %d", len(downsampled))
	}

	// LTTB should reduce points when threshold < data size
	if len(downsampled[0].Times) > 100 {
		t.Errorf("Expected reduced points after lttb, got %d", len(downsampled[0].Times))
	}
}

func TestApplyDownsampling_None(t *testing.T) {
	logger := logging.NewDevelopment()
	p := NewProcessor(logger)

	times := make([]string, 100)
	tempValues := make([]interface{}, 100)
	for i := 0; i < 100; i++ {
		tm := time.Date(2024, 1, 1, 0, i, 0, 0, time.UTC)
		times[i] = tm.Format(time.RFC3339)
		tempValues[i] = float64(i)
	}

	results := []QueryResult{
		{
			DeviceID: "device1",
			Times:    times,
			Fields: map[string][]interface{}{
				"temperature": tempValues,
			},
		},
	}

	// None should return unchanged
	downsampled := p.ApplyDownsampling(results, "none", 20)
	if len(downsampled[0].Times) != 100 {
		t.Errorf("Expected 100 points with none mode, got %d", len(downsampled[0].Times))
	}

	// Empty mode should also return unchanged
	downsampled = p.ApplyDownsampling(results, "", 20)
	if len(downsampled[0].Times) != 100 {
		t.Errorf("Expected 100 points with empty mode, got %d", len(downsampled[0].Times))
	}
}

func TestCalculateAutoThreshold(t *testing.T) {
	logger := logging.NewDevelopment()
	p := NewProcessor(logger)

	tests := []struct {
		name        string
		totalPoints int
		expectedMin int
		expectedMax int
		description string
	}{
		{
			name:        "small_dataset",
			totalPoints: 500,
			expectedMin: 500,
			expectedMax: 500,
			description: "Small datasets should return total points",
		},
		{
			name:        "boundary_1000",
			totalPoints: 1000,
			expectedMin: 1000,
			expectedMax: 1000,
			description: "Exactly 1000 points should return 1000",
		},
		{
			name:        "medium_dataset",
			totalPoints: 5000,
			expectedMin: 1000,
			expectedMax: 1000,
			description: "Medium datasets should return 1000",
		},
		{
			name:        "large_dataset",
			totalPoints: 50000,
			expectedMin: 2000,
			expectedMax: 2000,
			description: "Large datasets should return 2000",
		},
		{
			name:        "very_large_dataset",
			totalPoints: 200000,
			expectedMin: 5000,
			expectedMax: 5000,
			description: "Very large datasets should return max 5000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results := createMockResults(tt.totalPoints)
			threshold := p.CalculateAutoThreshold(results)

			if threshold < tt.expectedMin || threshold > tt.expectedMax {
				t.Errorf("%s: expected threshold between %d and %d, got %d",
					tt.description, tt.expectedMin, tt.expectedMax, threshold)
			}
		})
	}
}

func createMockResults(totalPoints int) []QueryResult {
	if totalPoints <= 0 {
		return []QueryResult{}
	}

	times := make([]string, totalPoints)
	for i := 0; i < totalPoints; i++ {
		times[i] = time.Date(2024, 1, 1, 0, 0, i, 0, time.UTC).Format(time.RFC3339)
	}

	return []QueryResult{
		{
			DeviceID: "device1",
			Times:    times,
			Fields: map[string][]interface{}{
				"value": make([]interface{}, totalPoints),
			},
		},
	}
}
