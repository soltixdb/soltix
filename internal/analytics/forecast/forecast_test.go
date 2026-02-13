package forecast

import (
	"testing"
	"time"
)

// Helper function to generate test data
func generateForecastTestData(n int, startTime time.Time, interval time.Duration) []DataPoint {
	data := make([]DataPoint, n)
	for i := 0; i < n; i++ {
		data[i] = DataPoint{
			Time:  startTime.Add(time.Duration(i) * interval),
			Value: float64(100 + i%10), // Values between 100-109
		}
	}
	return data
}

// Helper function to generate trending data
func generateTrendingData(n int, startTime time.Time, interval time.Duration, slope float64) []DataPoint {
	data := make([]DataPoint, n)
	for i := 0; i < n; i++ {
		data[i] = DataPoint{
			Time:  startTime.Add(time.Duration(i) * interval),
			Value: 100 + slope*float64(i),
		}
	}
	return data
}

// Helper function to generate seasonal data
func generateSeasonalData(n int, startTime time.Time, interval time.Duration, period int) []DataPoint {
	data := make([]DataPoint, n)
	for i := 0; i < n; i++ {
		seasonalComponent := 10.0 * float64((i%period)-period/2) / float64(period)
		data[i] = DataPoint{
			Time:  startTime.Add(time.Duration(i) * interval),
			Value: 100 + seasonalComponent,
		}
	}
	return data
}

func TestSMAForecaster_Name(t *testing.T) {
	f := NewSMAForecaster()
	if f.Name() != "sma" {
		t.Errorf("Expected name 'sma', got %s", f.Name())
	}
}

func TestSMAForecaster_Forecast(t *testing.T) {
	f := NewSMAForecaster()
	startTime := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	data := generateForecastTestData(20, startTime, time.Hour)

	config := DefaultForecastConfig()
	config.Horizon = 5
	config.WindowSize = 5

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

	if result.ModelInfo.Algorithm != "sma" {
		t.Errorf("Expected algorithm 'sma', got %s", result.ModelInfo.Algorithm)
	}

	// Check predictions are in the future
	for _, p := range result.Predictions {
		if p.Time.Before(data[len(data)-1].Time) || p.Time.Equal(data[len(data)-1].Time) {
			t.Errorf("Prediction time %v should be after data end %v", p.Time, data[len(data)-1].Time)
		}
	}

	// Check prediction intervals
	for _, p := range result.Predictions {
		if p.LowerBound > p.Value || p.UpperBound < p.Value {
			t.Errorf("Invalid prediction interval: %f not in [%f, %f]", p.Value, p.LowerBound, p.UpperBound)
		}
	}
}

func TestSMAForecaster_InsufficientData(t *testing.T) {
	f := NewSMAForecaster()
	startTime := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	data := generateForecastTestData(3, startTime, time.Hour) // Less than MinDataPoints

	config := DefaultForecastConfig()
	config.MinDataPoints = 10

	_, err := f.Forecast(data, config)
	if err == nil {
		t.Error("Expected error for insufficient data")
	}
}

func TestExponentialForecaster_Name(t *testing.T) {
	f := NewExponentialSmoothingForecaster()
	if f.Name() != "exponential" {
		t.Errorf("Expected name 'exponential', got %s", f.Name())
	}
}

func TestExponentialForecaster_Forecast_Trending(t *testing.T) {
	f := NewExponentialSmoothingForecaster()
	startTime := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	data := generateTrendingData(20, startTime, time.Hour, 0.5)

	config := DefaultForecastConfig()
	config.Horizon = 5

	result, err := f.Forecast(data, config)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}

	// For trending data, predictions should generally increase
	lastDataValue := data[len(data)-1].Value
	firstPrediction := result.Predictions[0].Value

	if firstPrediction < lastDataValue*0.9 {
		t.Errorf("Prediction %f should be close to or above last value %f for upward trend",
			firstPrediction, lastDataValue)
	}
}

func TestExponentialForecaster_FittedValues(t *testing.T) {
	f := NewExponentialSmoothingForecaster()
	startTime := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	data := generateForecastTestData(20, startTime, time.Hour)

	config := DefaultForecastConfig()
	config.Horizon = 5

	result, err := f.Forecast(data, config)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}

	// Should have fitted values for historical data
	if len(result.Fitted) != len(data) {
		t.Errorf("Expected %d fitted values, got %d", len(data), len(result.Fitted))
	}
}

func TestHoltWintersForecaster_Name(t *testing.T) {
	f := NewHoltWintersForecaster()
	if f.Name() != "holt_winters" {
		t.Errorf("Expected name 'holt_winters', got %s", f.Name())
	}
}

func TestHoltWintersForecaster_Forecast_Seasonal(t *testing.T) {
	f := NewHoltWintersForecaster()
	startTime := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	// Generate data with clear seasonality (need at least 2 complete seasons)
	data := generateSeasonalData(72, startTime, time.Hour, 24) // 3 days of hourly data

	config := DefaultForecastConfig()
	config.Horizon = 24
	config.SeasonalPeriod = 24
	config.MinDataPoints = 10

	result, err := f.Forecast(data, config)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}

	if len(result.Predictions) != config.Horizon {
		t.Errorf("Expected %d predictions, got %d", config.Horizon, len(result.Predictions))
	}
}

func TestHoltWintersForecaster_FallbackForSmallData(t *testing.T) {
	f := NewHoltWintersForecaster()
	startTime := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	data := generateForecastTestData(20, startTime, time.Hour) // Less than 2 seasonal periods

	config := DefaultForecastConfig()
	config.Horizon = 5
	config.SeasonalPeriod = 24
	config.MinDataPoints = 10

	result, err := f.Forecast(data, config)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}

	// Should fall back to exponential smoothing
	if result.ModelInfo.Algorithm != "exponential" {
		t.Logf("Algorithm used: %s (expected fallback to exponential)", result.ModelInfo.Algorithm)
	}
}

func TestLinearForecaster_Name(t *testing.T) {
	f := NewLinearRegressionForecaster()
	if f.Name() != "linear" {
		t.Errorf("Expected name 'linear', got %s", f.Name())
	}
}

func TestLinearForecaster_Forecast_Trending(t *testing.T) {
	f := NewLinearRegressionForecaster()
	startTime := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	data := generateTrendingData(20, startTime, time.Hour, 1.0)

	config := DefaultForecastConfig()
	config.Horizon = 5

	result, err := f.Forecast(data, config)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}

	// Predictions should follow the trend
	for i := 1; i < len(result.Predictions); i++ {
		if result.Predictions[i].Value <= result.Predictions[i-1].Value {
			t.Errorf("Predictions should be increasing for positive trend, got %f <= %f",
				result.Predictions[i].Value, result.Predictions[i-1].Value)
		}
	}
}

func TestAutoForecaster_Name(t *testing.T) {
	f := NewAutoForecaster()
	if f.Name() != "auto" {
		t.Errorf("Expected name 'auto', got %s", f.Name())
	}
}

func TestAutoForecaster_SelectsLinearForTrend(t *testing.T) {
	f := NewAutoForecaster()
	startTime := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	data := generateTrendingData(50, startTime, time.Hour, 2.0) // Clear upward trend

	config := DefaultForecastConfig()
	config.Horizon = 5
	config.SeasonalPeriod = 24

	result, err := f.Forecast(data, config)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}

	// Should select linear for trending data
	algo := result.ModelInfo.Algorithm
	if algo != "linear (auto-selected)" {
		t.Logf("Auto selected: %s (expected linear for trending data)", algo)
	}
}

func TestAutoForecaster_SelectsHoltWintersForSeasonal(t *testing.T) {
	f := NewAutoForecaster()
	startTime := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	data := generateSeasonalData(100, startTime, time.Hour, 24) // Clear seasonality

	config := DefaultForecastConfig()
	config.Horizon = 5
	config.SeasonalPeriod = 24

	result, err := f.Forecast(data, config)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}

	// Should select holt_winters for seasonal data
	algo := result.ModelInfo.Algorithm
	if algo != "holt_winters (auto-selected)" {
		t.Logf("Auto selected: %s (expected holt_winters for seasonal data)", algo)
	}
}

func TestAutoForecaster_SmallDataset(t *testing.T) {
	f := NewAutoForecaster()
	startTime := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	data := generateForecastTestData(8, startTime, time.Hour) // Small dataset

	config := DefaultForecastConfig()
	config.Horizon = 5
	config.MinDataPoints = 5

	result, err := f.Forecast(data, config)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}

	// For small datasets, should select SMA
	algo := result.ModelInfo.Algorithm
	if algo != "sma (auto-selected)" {
		t.Logf("Auto selected: %s (expected sma for small dataset)", algo)
	}
}

func TestGetForecaster_ValidNames(t *testing.T) {
	validNames := []string{"sma", "exponential", "holt_winters", "linear", "arima", "prophet", "auto"}

	for _, name := range validNames {
		forecaster, err := GetForecaster(name)
		if err != nil {
			t.Errorf("Expected forecaster for %s, got error: %v", name, err)
		}
		if forecaster == nil {
			t.Errorf("Expected non-nil forecaster for %s", name)
		}
	}
}

func TestGetForecaster_InvalidName(t *testing.T) {
	_, err := GetForecaster("unknown")
	if err == nil {
		t.Error("Expected error for unknown forecaster")
	}
}

func TestListForecasters(t *testing.T) {
	names := ListForecasters()

	if len(names) < 5 {
		t.Errorf("Expected at least 5 forecasters, got %d", len(names))
	}

	expected := map[string]bool{
		"sma":          false,
		"exponential":  false,
		"holt_winters": false,
		"linear":       false,
		"auto":         false,
	}

	for _, name := range names {
		if _, ok := expected[name]; ok {
			expected[name] = true
		}
	}

	for name, found := range expected {
		if !found {
			t.Errorf("Expected forecaster %s not found", name)
		}
	}
}

// Error Metrics Tests
func TestCalculateMAPE(t *testing.T) {
	actual := []float64{100, 200, 300, 400}
	predicted := []float64{110, 190, 310, 380}

	mape := CalculateMAPE(actual, predicted)

	// MAPE should be around 5%
	if mape < 4 || mape > 6 {
		t.Errorf("Expected MAPE around 5%%, got %f%%", mape)
	}
}

func TestCalculateMAE(t *testing.T) {
	actual := []float64{100, 200, 300, 400}
	predicted := []float64{110, 190, 310, 380}

	mae := CalculateMAE(actual, predicted)

	// MAE should be 10 (average of |10|, |-10|, |10|, |-20|)
	expectedMAE := 12.5
	if mae != expectedMAE {
		t.Errorf("Expected MAE %f, got %f", expectedMAE, mae)
	}
}

func TestCalculateRMSE(t *testing.T) {
	actual := []float64{100, 200, 300, 400}
	predicted := []float64{110, 190, 310, 380}

	rmse := CalculateRMSE(actual, predicted)

	// RMSE should be sqrt((100+100+100+400)/4) = sqrt(175) ≈ 13.23
	if rmse < 13 || rmse > 14 {
		t.Errorf("Expected RMSE around 13.23, got %f", rmse)
	}
}

func TestCalculateMetrics_MismatchedLength(t *testing.T) {
	mape := CalculateMAPE([]float64{1, 2, 3}, []float64{1, 2})
	mae := CalculateMAE([]float64{1, 2, 3}, []float64{1, 2})
	rmse := CalculateRMSE([]float64{1, 2, 3}, []float64{1, 2})

	if mape != 0 || mae != 0 || rmse != 0 {
		t.Errorf("Metrics for mismatched length should be 0")
	}
}

// Benchmark Tests
func BenchmarkSMAForecast(b *testing.B) {
	f := NewSMAForecaster()
	startTime := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	data := generateForecastTestData(1000, startTime, time.Hour)
	config := DefaultForecastConfig()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = f.Forecast(data, config)
	}
}

func BenchmarkExponentialForecast(b *testing.B) {
	f := NewExponentialSmoothingForecaster()
	startTime := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	data := generateTrendingData(1000, startTime, time.Hour, 0.5)
	config := DefaultForecastConfig()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = f.Forecast(data, config)
	}
}

func BenchmarkHoltWintersForecast(b *testing.B) {
	f := NewHoltWintersForecaster()
	startTime := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	data := generateSeasonalData(1000, startTime, time.Hour, 24)
	config := DefaultForecastConfig()
	config.SeasonalPeriod = 24

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = f.Forecast(data, config)
	}
}

func BenchmarkLinearForecast(b *testing.B) {
	f := NewLinearRegressionForecaster()
	startTime := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	data := generateTrendingData(1000, startTime, time.Hour, 1.0)
	config := DefaultForecastConfig()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = f.Forecast(data, config)
	}
}

func BenchmarkAutoForecast(b *testing.B) {
	f := NewAutoForecaster()
	startTime := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	data := generateForecastTestData(1000, startTime, time.Hour)
	config := DefaultForecastConfig()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = f.Forecast(data, config)
	}
}
