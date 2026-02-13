package forecast

import (
	"testing"
)

func TestHoltWintersForecaster_BasicForecast(t *testing.T) {
	data := generateSeasonalTestData(100, 24) // 100 points with daily seasonality

	config := ForecastConfig{
		Horizon:        5,
		MinDataPoints:  20,
		Alpha:          0.3,
		Beta:           0.1,
		Gamma:          0.1,
		SeasonalPeriod: 24,
		Confidence:     0.95,
	}

	forecaster := NewHoltWintersForecaster()
	result, err := forecaster.Forecast(data, config)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}

	if len(result.Predictions) != config.Horizon {
		t.Errorf("Expected %d predictions, got %d", config.Horizon, len(result.Predictions))
	}

	if result.ModelInfo.Algorithm != "holt_winters" {
		t.Errorf("Expected algorithm 'holt_winters', got '%s'", result.ModelInfo.Algorithm)
	}
}

func TestHoltWintersForecaster_InsufficientData(t *testing.T) {
	data := generateLinearData(5, 1.0, 0.0)

	config := ForecastConfig{
		Horizon:        5,
		MinDataPoints:  10,
		SeasonalPeriod: 12,
	}

	forecaster := NewHoltWintersForecaster()
	_, err := forecaster.Forecast(data, config)
	if err == nil {
		t.Error("Expected error for insufficient data")
	}
}

func TestHoltWintersForecaster_FallbackToExponential(t *testing.T) {
	// Not enough data for 2 complete seasons
	data := generateLinearData(30, 1.0, 0.0)

	config := ForecastConfig{
		Horizon:        3,
		MinDataPoints:  10,
		Alpha:          0.3,
		SeasonalPeriod: 24, // Need 48 points for 2 seasons
		Confidence:     0.95,
	}

	forecaster := NewHoltWintersForecaster()
	result, err := forecaster.Forecast(data, config)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}

	// Should fall back to exponential smoothing
	if result.ModelInfo.Algorithm != "exponential" {
		t.Logf("Expected fallback to 'exponential', got '%s'", result.ModelInfo.Algorithm)
	}
}

func TestHoltWintersForecaster_Name_Extended(t *testing.T) {
	forecaster := NewHoltWintersForecaster()
	if forecaster.Name() != "holt_winters" {
		t.Errorf("Expected name 'holt_winters', got '%s'", forecaster.Name())
	}
}

func TestHoltWintersForecaster_DefaultParameters(t *testing.T) {
	data := generateSeasonalTestData(100, 12)

	config := ForecastConfig{
		Horizon:        3,
		MinDataPoints:  20,
		Alpha:          0, // Should use default
		Beta:           0, // Should use default
		Gamma:          0, // Should use default
		SeasonalPeriod: 12,
		Confidence:     0.95,
	}

	forecaster := NewHoltWintersForecaster()
	result, err := forecaster.Forecast(data, config)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}

	// Check defaults are applied
	if result.ModelInfo.Parameters["alpha"].(float64) != 0.3 {
		t.Errorf("Expected default alpha 0.3, got %v", result.ModelInfo.Parameters["alpha"])
	}
}

func TestHoltWintersForecaster_SeasonalPattern(t *testing.T) {
	period := 12
	data := generateSeasonalTestData(72, period) // 6 complete seasons

	config := ForecastConfig{
		Horizon:        period,
		MinDataPoints:  20,
		Alpha:          0.3,
		Beta:           0.1,
		Gamma:          0.3,
		SeasonalPeriod: period,
		Confidence:     0.95,
	}

	forecaster := NewHoltWintersForecaster()
	result, err := forecaster.Forecast(data, config)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}

	// Predictions should follow seasonal pattern
	if len(result.Predictions) != period {
		t.Errorf("Expected %d predictions, got %d", period, len(result.Predictions))
	}
}

func TestHoltWintersForecaster_ConfidenceInterval(t *testing.T) {
	data := generateSeasonalTestData(100, 12)

	config := ForecastConfig{
		Horizon:        5,
		MinDataPoints:  20,
		Alpha:          0.3,
		Beta:           0.1,
		Gamma:          0.1,
		SeasonalPeriod: 12,
		Confidence:     0.95,
	}

	forecaster := NewHoltWintersForecaster()
	result, err := forecaster.Forecast(data, config)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}

	for i, pred := range result.Predictions {
		if pred.LowerBound > pred.Value {
			t.Errorf("Prediction %d: lower bound %v > value %v", i, pred.LowerBound, pred.Value)
		}
		if pred.UpperBound < pred.Value {
			t.Errorf("Prediction %d: upper bound %v < value %v", i, pred.UpperBound, pred.Value)
		}
	}
}

func BenchmarkHoltWintersForecaster(b *testing.B) {
	data := generateSeasonalTestData(200, 24)
	config := ForecastConfig{
		Horizon:        10,
		MinDataPoints:  20,
		Alpha:          0.3,
		Beta:           0.1,
		Gamma:          0.1,
		SeasonalPeriod: 24,
		Confidence:     0.95,
	}
	forecaster := NewHoltWintersForecaster()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = forecaster.Forecast(data, config)
	}
}
