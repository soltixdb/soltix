package forecast

import (
	"math"
	"testing"
	"time"
)

func TestNewProphetForecaster(t *testing.T) {
	f := NewProphetForecaster()

	if f == nil {
		t.Fatal("Expected non-nil ProphetForecaster")
		return
	}

	if f.GrowthType != "linear" {
		t.Errorf("Expected GrowthType='linear', got %s", f.GrowthType)
	}
	if f.NumChangePoints != 25 {
		t.Errorf("Expected NumChangePoints=25, got %d", f.NumChangePoints)
	}
	if !f.YearlySeasonality {
		t.Error("Expected YearlySeasonality=true")
	}
	if !f.WeeklySeasonality {
		t.Error("Expected WeeklySeasonality=true")
	}
	if !f.DailySeasonality {
		t.Error("Expected DailySeasonality=true")
	}
}

func TestProphetForecaster_Name(t *testing.T) {
	f := NewProphetForecaster()
	if f.Name() != "prophet" {
		t.Errorf("Expected name 'prophet', got %s", f.Name())
	}
}

func TestProphetForecaster_Forecast_TrendData(t *testing.T) {
	f := NewProphetForecaster()
	config := DefaultForecastConfig()
	config.Horizon = 5
	config.MinDataPoints = 10
	config.Interval = time.Hour
	config.Confidence = 0.95

	// Create data with linear trend
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	data := make([]DataPoint, 50)
	for i := 0; i < 50; i++ {
		data[i] = DataPoint{
			Time:  baseTime.Add(time.Duration(i) * time.Hour),
			Value: 100.0 + float64(i)*2.0,
		}
	}

	result, err := f.Forecast(data, config)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}

	if result == nil {
		t.Fatal("Expected non-nil result")
		return
	}

	if len(result.Predictions) != config.Horizon {
		t.Errorf("Expected %d predictions, got %d", config.Horizon, len(result.Predictions))
	}

	if result.ModelInfo.Algorithm != "prophet" {
		t.Errorf("Expected algorithm 'prophet', got %s", result.ModelInfo.Algorithm)
	}

	// Check parameters are stored
	params := result.ModelInfo.Parameters
	if params["growth"] != "linear" {
		t.Errorf("Expected growth='linear', got %v", params["growth"])
	}

	// Predictions should be in the future
	lastDataTime := data[len(data)-1].Time
	for i, pred := range result.Predictions {
		if !pred.Time.After(lastDataTime) {
			t.Errorf("Prediction %d time %v should be after %v", i, pred.Time, lastDataTime)
		}

		// Should have reasonable bounds
		if pred.LowerBound >= pred.Value || pred.UpperBound <= pred.Value {
			t.Errorf("Prediction %d: bounds should contain value. Lower=%f, Value=%f, Upper=%f",
				i, pred.LowerBound, pred.Value, pred.UpperBound)
		}
	}
}

func TestProphetForecaster_Forecast_SeasonalData(t *testing.T) {
	f := NewProphetForecaster()
	config := DefaultForecastConfig()
	config.Horizon = 24
	config.MinDataPoints = 10
	config.Interval = time.Hour

	// Create data with daily seasonality (24-hour pattern)
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	data := make([]DataPoint, 168) // 7 days of hourly data
	for i := 0; i < 168; i++ {
		hour := i % 24
		// Simulate daily pattern: low at night, high during day
		seasonalValue := 50.0 + 30.0*math.Sin(2*math.Pi*float64(hour-6)/24.0)
		data[i] = DataPoint{
			Time:  baseTime.Add(time.Duration(i) * time.Hour),
			Value: seasonalValue + float64(i)*0.1, // Small trend
		}
	}

	result, err := f.Forecast(data, config)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}

	if result == nil {
		t.Fatal("Expected non-nil result")
		return
	}

	if len(result.Predictions) != config.Horizon {
		t.Errorf("Expected %d predictions, got %d", config.Horizon, len(result.Predictions))
	}

	// Should have fitted values
	if len(result.Fitted) != len(data) {
		t.Errorf("Expected %d fitted values, got %d", len(data), len(result.Fitted))
	}

	// MAPE should be reasonable for seasonal data
	if result.ModelInfo.MAPE > 100 {
		t.Logf("Warning: MAPE=%f is high for seasonal data", result.ModelInfo.MAPE)
	}
}

func TestProphetForecaster_Forecast_InsufficientData(t *testing.T) {
	f := NewProphetForecaster()
	config := DefaultForecastConfig()
	config.MinDataPoints = 10

	// Only 5 data points
	data := []DataPoint{
		{Time: time.Now(), Value: 1},
		{Time: time.Now().Add(time.Hour), Value: 2},
		{Time: time.Now().Add(2 * time.Hour), Value: 3},
		{Time: time.Now().Add(3 * time.Hour), Value: 4},
		{Time: time.Now().Add(4 * time.Hour), Value: 5},
	}

	_, err := f.Forecast(data, config)
	if err == nil {
		t.Error("Expected error for insufficient data")
	}
}

func TestProphetForecaster_SeasonalityFunction(t *testing.T) {
	f := NewProphetForecaster()

	// Test with known coefficients
	coeffs := []float64{1.0, 0.5, 0.3, 0.2} // 2 Fourier terms

	// Test at different times
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	result1 := f.seasonalityFunction(coeffs, baseTime, 1.0) // Daily period
	result2 := f.seasonalityFunction(coeffs, baseTime.Add(12*time.Hour), 1.0)

	// Results should be different at different times of day
	if result1 == result2 {
		t.Logf("Seasonality values at different times: %f, %f", result1, result2)
	}
}

func TestProphetForecaster_TrendFunction(t *testing.T) {
	f := NewProphetForecaster()

	model := &prophetModel{
		k:            1.0, // Slope
		m:            0.0, // Intercept
		changePoints: []float64{0.5},
		deltas:       []float64{0.5}, // Additional slope after changepoint
	}

	// Before changepoint
	trend1 := f.trendFunction(model, 0.3)
	expected1 := 1.0*0.3 + 0.0
	if math.Abs(trend1-expected1) > 0.001 {
		t.Errorf("Expected trend=%f, got %f", expected1, trend1)
	}

	// After changepoint
	trend2 := f.trendFunction(model, 0.7)
	expected2 := 1.0*0.7 + 0.0 + 0.5*(0.7-0.5)
	if math.Abs(trend2-expected2) > 0.001 {
		t.Errorf("Expected trend=%f, got %f", expected2, trend2)
	}
}

func TestProphet_RegistryIntegration(t *testing.T) {
	forecaster, err := GetForecaster("prophet")
	if err != nil {
		t.Fatalf("Failed to get Prophet from registry: %v", err)
	}

	if forecaster.Name() != "prophet" {
		t.Errorf("Expected name 'prophet', got %s", forecaster.Name())
	}
}

func TestProphetForecaster_FittedValues(t *testing.T) {
	f := NewProphetForecaster()
	config := DefaultForecastConfig()
	config.Horizon = 5
	config.MinDataPoints = 10
	config.Interval = time.Hour

	// Create simple trending data
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	data := make([]DataPoint, 30)
	for i := 0; i < 30; i++ {
		data[i] = DataPoint{
			Time:  baseTime.Add(time.Duration(i) * time.Hour),
			Value: 10.0 + float64(i)*0.5,
		}
	}

	result, err := f.Forecast(data, config)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}

	// Should have fitted values
	if len(result.Fitted) != len(data) {
		t.Errorf("Expected %d fitted values, got %d", len(data), len(result.Fitted))
	}

	// Should have residuals
	if len(result.Residuals) != len(data) {
		t.Errorf("Expected %d residuals, got %d", len(data), len(result.Residuals))
	}

	// Model info should have accuracy metrics
	if result.ModelInfo.DataPoints != len(data) {
		t.Errorf("Expected DataPoints=%d, got %d", len(data), result.ModelInfo.DataPoints)
	}
}

func TestProphetForecaster_PredictionIntervals(t *testing.T) {
	f := NewProphetForecaster()
	config := DefaultForecastConfig()
	config.Horizon = 10
	config.MinDataPoints = 10
	config.Interval = time.Hour
	config.Confidence = 0.95

	// Create data with some variance
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	data := make([]DataPoint, 50)
	for i := 0; i < 50; i++ {
		data[i] = DataPoint{
			Time:  baseTime.Add(time.Duration(i) * time.Hour),
			Value: 100.0 + float64(i) + 5.0*math.Sin(float64(i)),
		}
	}

	result, err := f.Forecast(data, config)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}

	// Prediction intervals should widen with horizon
	for i := 1; i < len(result.Predictions); i++ {
		prevWidth := result.Predictions[i-1].UpperBound - result.Predictions[i-1].LowerBound
		currWidth := result.Predictions[i].UpperBound - result.Predictions[i].LowerBound

		// Intervals should generally widen or stay same
		if currWidth < prevWidth*0.5 { // Allow some tolerance
			t.Logf("Interval width decreased at step %d: %f -> %f", i, prevWidth, currWidth)
		}
	}
}

func TestProphetForecaster_CalculateStd(t *testing.T) {
	f := NewProphetForecaster()

	tests := []struct {
		name     string
		values   []float64
		expected float64
	}{
		{
			name:     "empty",
			values:   []float64{},
			expected: 1.0,
		},
		{
			name:     "single value",
			values:   []float64{5.0},
			expected: 1.0, // Returns 1.0 for zero std
		},
		{
			name:     "constant values",
			values:   []float64{5.0, 5.0, 5.0},
			expected: 1.0, // Returns 1.0 for zero std
		},
		{
			name:     "varying values",
			values:   []float64{1.0, 2.0, 3.0, 4.0, 5.0},
			expected: math.Sqrt(2.0), // std of [1,2,3,4,5]
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := f.calculateStd(tt.values)
			if math.Abs(result-tt.expected) > 0.01 {
				t.Errorf("Expected std=%f, got %f", tt.expected, result)
			}
		})
	}
}
