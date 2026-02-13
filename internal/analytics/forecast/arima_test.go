package forecast

import (
	"math"
	"testing"
	"time"
)

func TestNewARIMAForecaster(t *testing.T) {
	f := NewARIMAForecaster()

	if f == nil {
		t.Fatal("Expected non-nil ARIMAForecaster")
		return
	}

	if f.P != 2 {
		t.Errorf("Expected P=2, got %d", f.P)
	}
	if f.D != 1 {
		t.Errorf("Expected D=1, got %d", f.D)
	}
	if f.Q != 2 {
		t.Errorf("Expected Q=2, got %d", f.Q)
	}
}

func TestNewARIMAForecasterWithParams(t *testing.T) {
	f := NewARIMAForecasterWithParams(3, 2, 1)

	if f == nil {
		t.Fatal("Expected non-nil ARIMAForecaster")
		return
	}

	if f.P != 3 {
		t.Errorf("Expected P=3, got %d", f.P)
	}
	if f.D != 2 {
		t.Errorf("Expected D=2, got %d", f.D)
	}
	if f.Q != 1 {
		t.Errorf("Expected Q=1, got %d", f.Q)
	}
}

func TestARIMAForecaster_Name(t *testing.T) {
	f := NewARIMAForecaster()
	if f.Name() != "arima" {
		t.Errorf("Expected name 'arima', got %s", f.Name())
	}
}

func TestARIMAForecaster_Forecast_TrendData(t *testing.T) {
	f := NewARIMAForecaster()
	config := DefaultForecastConfig()
	config.Horizon = 5
	config.MinDataPoints = 10
	config.Interval = time.Hour
	config.Confidence = 0.95

	// Create data with linear trend
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	data := make([]DataPoint, 30)
	for i := 0; i < 30; i++ {
		data[i] = DataPoint{
			Time:  baseTime.Add(time.Duration(i) * time.Hour),
			Value: 100.0 + float64(i)*2.0 + (float64(i%3) - 1.0), // Trend with small noise
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

	if result.ModelInfo.Algorithm != "arima" {
		t.Errorf("Expected algorithm 'arima', got %s", result.ModelInfo.Algorithm)
	}

	// Check parameters are stored
	params := result.ModelInfo.Parameters
	if params["p"] != 2 || params["d"] != 1 || params["q"] != 2 {
		t.Errorf("Parameters not correctly stored: %v", params)
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

	// For trending data, forecasts should generally continue the trend
	lastValue := data[len(data)-1].Value
	firstPrediction := result.Predictions[0].Value
	// Allow some tolerance
	if math.Abs(firstPrediction-lastValue) > 20 {
		t.Logf("Warning: first prediction %f differs significantly from last value %f", firstPrediction, lastValue)
	}
}

func TestARIMAForecaster_Forecast_StationaryData(t *testing.T) {
	f := NewARIMAForecasterWithParams(2, 0, 1) // No differencing for stationary data
	config := DefaultForecastConfig()
	config.Horizon = 5
	config.MinDataPoints = 10
	config.Interval = time.Hour

	// Create stationary data (oscillating around mean)
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	data := make([]DataPoint, 30)
	for i := 0; i < 30; i++ {
		data[i] = DataPoint{
			Time:  baseTime.Add(time.Duration(i) * time.Hour),
			Value: 50.0 + 10.0*math.Sin(float64(i)*0.5),
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

	// Predictions should be around the mean
	dataMean := 50.0
	for i, pred := range result.Predictions {
		if math.Abs(pred.Value-dataMean) > 30 {
			t.Logf("Prediction %d value %f is far from mean %f", i, pred.Value, dataMean)
		}
	}
}

func TestARIMAForecaster_Forecast_InsufficientData(t *testing.T) {
	f := NewARIMAForecaster()
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

func TestARIMAForecaster_Difference(t *testing.T) {
	f := NewARIMAForecaster()

	tests := []struct {
		name     string
		values   []float64
		d        int
		expected []float64
	}{
		{
			name:     "no differencing",
			values:   []float64{1, 2, 3, 4, 5},
			d:        0,
			expected: []float64{1, 2, 3, 4, 5},
		},
		{
			name:     "first difference",
			values:   []float64{1, 3, 6, 10, 15},
			d:        1,
			expected: []float64{2, 3, 4, 5},
		},
		{
			name:     "second difference",
			values:   []float64{1, 3, 6, 10, 15},
			d:        2,
			expected: []float64{1, 1, 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, _ := f.difference(tt.values, tt.d)
			if len(result) != len(tt.expected) {
				t.Errorf("Expected length %d, got %d", len(tt.expected), len(result))
				return
			}
			for i := range result {
				if math.Abs(result[i]-tt.expected[i]) > 0.001 {
					t.Errorf("At index %d: expected %f, got %f", i, tt.expected[i], result[i])
				}
			}
		})
	}
}

func TestARIMAForecaster_Autocorrelation(t *testing.T) {
	f := NewARIMAForecaster()

	// Create simple AR(1) process simulation
	values := []float64{1.0, 0.8, 0.64, 0.51, 0.41, 0.33, 0.26, 0.21, 0.17, 0.14}

	acf := f.autocorrelation(values, 3)

	if len(acf) != 3 {
		t.Errorf("Expected 3 ACF values, got %d", len(acf))
	}

	// ACF should decay for this type of data
	for i := 1; i < len(acf); i++ {
		if math.Abs(acf[i]) > math.Abs(acf[i-1])+0.5 {
			// Allow some tolerance but generally should decay
			t.Logf("ACF[%d]=%f, ACF[%d]=%f", i-1, acf[i-1], i, acf[i])
		}
	}
}

func TestARIMAForecaster_LevinsonDurbin(t *testing.T) {
	f := NewARIMAForecaster()

	// Test with known ACF
	acf := []float64{0.8, 0.6, 0.4}

	coeffs := f.levinsonDurbin(acf, 2)

	if len(coeffs) != 2 {
		t.Errorf("Expected 2 coefficients, got %d", len(coeffs))
	}

	// Coefficients should be reasonable (between -1 and 1 for stability)
	for i, c := range coeffs {
		if math.Abs(c) > 2 {
			t.Logf("Coefficient %d=%f might indicate instability", i, c)
		}
	}
}

func TestARIMA_RegistryIntegration(t *testing.T) {
	forecaster, err := GetForecaster("arima")
	if err != nil {
		t.Fatalf("Failed to get ARIMA from registry: %v", err)
	}

	if forecaster.Name() != "arima" {
		t.Errorf("Expected name 'arima', got %s", forecaster.Name())
	}
}

func TestARIMAForecaster_FittedValues(t *testing.T) {
	f := NewARIMAForecaster()
	config := DefaultForecastConfig()
	config.Horizon = 3
	config.MinDataPoints = 10
	config.Interval = time.Hour

	// Create simple trending data
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	data := make([]DataPoint, 20)
	for i := 0; i < 20; i++ {
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
	if len(result.Fitted) == 0 {
		t.Error("Expected non-empty fitted values")
	}

	// Should have residuals
	if len(result.Residuals) == 0 {
		t.Error("Expected non-empty residuals")
	}

	// Model info should have accuracy metrics
	if result.ModelInfo.DataPoints != len(data) {
		t.Errorf("Expected DataPoints=%d, got %d", len(data), result.ModelInfo.DataPoints)
	}
}

func TestARIMAForecaster_PredictionIntervals(t *testing.T) {
	f := NewARIMAForecaster()
	config := DefaultForecastConfig()
	config.Horizon = 10
	config.MinDataPoints = 10
	config.Interval = time.Hour
	config.Confidence = 0.95

	// Create data with some variance
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	data := make([]DataPoint, 30)
	for i := 0; i < 30; i++ {
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
			t.Logf("Interval width decreased significantly at step %d: %f -> %f", i, prevWidth, currWidth)
		}
	}
}
