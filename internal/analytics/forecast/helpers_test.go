package forecast

import (
	"math"
	"testing"
	"time"
)

// Common test data and helpers for all forecast tests

var (
	testBaseTime = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testInterval = time.Hour
)

// generateLinearData creates test data with linear pattern: y = slope * x + intercept
func generateLinearData(n int, slope, intercept float64) []DataPoint {
	data := make([]DataPoint, n)
	for i := 0; i < n; i++ {
		data[i] = DataPoint{
			Time:  testBaseTime.Add(testInterval * time.Duration(i)),
			Value: slope*float64(i) + intercept,
		}
	}
	return data
}

// generateSeasonalTestData creates test data with seasonal pattern
func generateSeasonalTestData(n int, period int) []DataPoint {
	data := make([]DataPoint, n)
	for i := 0; i < n; i++ {
		trend := float64(i) * 0.1
		seasonal := 10 * math.Sin(2*math.Pi*float64(i%period)/float64(period))
		data[i] = DataPoint{
			Time:  testBaseTime.Add(testInterval * time.Duration(i)),
			Value: 50 + trend + seasonal,
		}
	}
	return data
}

// Test basic Forecaster interface compliance
func TestForecasterRegistry(t *testing.T) {
	// These algorithms are registered via init() in the forecast package
	algorithms := []string{
		"sma",
		"exponential",
		"linear",
		"holt_winters",
		"arima",
		"prophet",
		"auto",
	}

	for _, algo := range algorithms {
		forecaster, err := GetForecaster(algo)
		if err != nil {
			t.Errorf("Forecaster '%s' not registered: %v", algo, err)
		} else if forecaster.Name() != algo {
			t.Errorf("Forecaster name mismatch: expected '%s', got '%s'", algo, forecaster.Name())
		}
	}
}

func TestGetForecaster_Unknown_Extended(t *testing.T) {
	_, err := GetForecaster("unknown_algorithm")
	if err == nil {
		t.Error("Should return error for unknown algorithm")
	}
}

func TestCalculateMAPE_Extended(t *testing.T) {
	actual := []float64{100, 200, 300}
	predicted := []float64{110, 190, 310}

	mape := CalculateMAPE(actual, predicted)
	// Expected: (|10/100| + |10/200| + |10/300|) / 3 * 100 = (0.1 + 0.05 + 0.033) / 3 * 100 ≈ 6.1%
	if mape < 5 || mape > 7 {
		t.Errorf("MAPE calculation incorrect: got %v", mape)
	}
}

func TestCalculateMAE_Extended(t *testing.T) {
	actual := []float64{100, 200, 300}
	predicted := []float64{110, 190, 310}

	mae := CalculateMAE(actual, predicted)
	// Expected: (10 + 10 + 10) / 3 = 10
	if mae != 10 {
		t.Errorf("MAE calculation incorrect: got %v, expected 10", mae)
	}
}

func TestCalculateRMSE_Extended(t *testing.T) {
	actual := []float64{100, 200, 300}
	predicted := []float64{110, 190, 310}

	rmse := CalculateRMSE(actual, predicted)
	// Expected: sqrt((100 + 100 + 100) / 3) = sqrt(100) = 10
	if rmse != 10 {
		t.Errorf("RMSE calculation incorrect: got %v, expected 10", rmse)
	}
}

func TestForecastConfig_Defaults(t *testing.T) {
	config := ForecastConfig{
		Horizon: 5,
	}

	// Test that algorithms handle zero/default config values
	data := generateLinearData(30, 1.0, 0.0)

	forecaster := NewSMAForecaster()
	result, err := forecaster.Forecast(data, config)
	if err != nil {
		t.Fatalf("Forecast with minimal config failed: %v", err)
	}

	if len(result.Predictions) != 5 {
		t.Errorf("Expected 5 predictions, got %d", len(result.Predictions))
	}
}
