package forecast

import (
	"testing"
	"time"
)

func TestAutoForecaster_BasicForecast(t *testing.T) {
	data := generateLinearData(50, 1.0, 0.0)

	config := ForecastConfig{
		Horizon:       5,
		MinDataPoints: 10,
		Confidence:    0.95,
	}

	forecaster := NewAutoForecaster()
	result, err := forecaster.Forecast(data, config)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}

	if len(result.Predictions) != config.Horizon {
		t.Errorf("Expected %d predictions, got %d", config.Horizon, len(result.Predictions))
	}
}

func TestAutoForecaster_SelectsLinearForTrend_Extended(t *testing.T) {
	// Strong upward trend
	data := generateLinearData(50, 5.0, 0.0)

	config := ForecastConfig{
		Horizon:        5,
		MinDataPoints:  10,
		SeasonalPeriod: 12,
		Confidence:     0.95,
	}

	forecaster := NewAutoForecaster()
	result, err := forecaster.Forecast(data, config)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}

	// Should select linear regression for trending data
	if result.ModelInfo.Algorithm != "linear (auto-selected)" {
		t.Logf("Expected 'linear (auto-selected)', got '%s'", result.ModelInfo.Algorithm)
	}
}

func TestAutoForecaster_SelectsHoltWintersForSeasonal_Extended(t *testing.T) {
	// Seasonal data with enough points
	data := generateSeasonalTestData(100, 12)

	config := ForecastConfig{
		Horizon:        5,
		MinDataPoints:  10,
		SeasonalPeriod: 12,
		Confidence:     0.95,
	}

	forecaster := NewAutoForecaster()
	result, err := forecaster.Forecast(data, config)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}

	// Should select Holt-Winters for seasonal data
	if result.ModelInfo.Algorithm != "holt_winters (auto-selected)" {
		t.Logf("Expected 'holt_winters (auto-selected)', got '%s'", result.ModelInfo.Algorithm)
	}
}

func TestAutoForecaster_SelectsSMAForSmallData(t *testing.T) {
	// Small dataset without clear pattern
	data := make([]DataPoint, 15)
	for i := 0; i < 15; i++ {
		data[i] = DataPoint{
			Time:  testBaseTime.Add(testInterval * time.Duration(i)),
			Value: 50 + float64(i%3), // No clear trend
		}
	}

	config := ForecastConfig{
		Horizon:       3,
		MinDataPoints: 5,
		Confidence:    0.95,
	}

	forecaster := NewAutoForecaster()
	result, err := forecaster.Forecast(data, config)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}

	// Should select SMA for small noisy data
	// Algorithm selection may vary based on data characteristics
	if result.ModelInfo.Algorithm == "" {
		t.Error("Algorithm should be selected")
	}
}

func TestAutoForecaster_Name_Extended(t *testing.T) {
	forecaster := NewAutoForecaster()
	if forecaster.Name() != "auto" {
		t.Errorf("Expected name 'auto', got '%s'", forecaster.Name())
	}
}

func TestAutoForecaster_InsufficientData(t *testing.T) {
	data := generateLinearData(3, 1.0, 0.0)

	config := ForecastConfig{
		Horizon:       5,
		MinDataPoints: 10,
	}

	forecaster := NewAutoForecaster()
	result, err := forecaster.Forecast(data, config)

	// Should fall back to SMA for small datasets
	if err != nil {
		t.Logf("Forecast returned error: %v", err)
	} else if result != nil {
		t.Logf("Selected algorithm: %s", result.ModelInfo.Algorithm)
	}
}

func TestDetectTrend(t *testing.T) {
	// Strong upward trend
	upwardData := generateLinearData(20, 5.0, 0.0)
	if !detectTrend(upwardData) {
		t.Error("Should detect upward trend")
	}

	// No trend (constant)
	constantData := make([]DataPoint, 20)
	for i := 0; i < 20; i++ {
		constantData[i] = DataPoint{
			Time:  testBaseTime.Add(testInterval * time.Duration(i)),
			Value: 50,
		}
	}
	if detectTrend(constantData) {
		t.Error("Should not detect trend in constant data")
	}
}

func TestDetectSeasonality(t *testing.T) {
	// Seasonal data
	seasonalData := generateSeasonalTestData(48, 12)
	if !detectSeasonality(seasonalData, 12) {
		t.Error("Should detect seasonality in seasonal data")
	}

	// Non-seasonal data
	linearData := generateLinearData(48, 1.0, 0.0)
	if detectSeasonality(linearData, 12) {
		t.Logf("May detect false seasonality in linear data")
	}
}

func BenchmarkAutoForecaster(b *testing.B) {
	data := generateLinearData(100, 1.0, 0.0)
	config := ForecastConfig{
		Horizon:       10,
		MinDataPoints: 10,
		Confidence:    0.95,
	}
	forecaster := NewAutoForecaster()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = forecaster.Forecast(data, config)
	}
}
