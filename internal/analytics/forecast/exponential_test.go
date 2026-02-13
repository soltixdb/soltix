package forecast

import (
	"testing"
)

func TestExponentialSmoothingForecaster_BasicForecast(t *testing.T) {
	data := generateLinearData(50, 1.0, 0.0)

	config := ForecastConfig{
		Horizon:       5,
		MinDataPoints: 10,
		Alpha:         0.3,
		Confidence:    0.95,
	}

	forecaster := NewExponentialSmoothingForecaster()
	result, err := forecaster.Forecast(data, config)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}

	if len(result.Predictions) != config.Horizon {
		t.Errorf("Expected %d predictions, got %d", config.Horizon, len(result.Predictions))
	}

	if result.ModelInfo.Algorithm != "exponential" {
		t.Errorf("Expected algorithm 'exponential', got '%s'", result.ModelInfo.Algorithm)
	}
}

func TestExponentialSmoothingForecaster_InsufficientData(t *testing.T) {
	data := generateLinearData(5, 1.0, 0.0)

	config := ForecastConfig{
		Horizon:       5,
		MinDataPoints: 10,
		Alpha:         0.3,
	}

	forecaster := NewExponentialSmoothingForecaster()
	_, err := forecaster.Forecast(data, config)
	if err == nil {
		t.Error("Expected error for insufficient data")
	}
}

func TestExponentialSmoothingForecaster_DefaultAlpha(t *testing.T) {
	data := generateLinearData(30, 1.0, 0.0)

	config := ForecastConfig{
		Horizon:       3,
		MinDataPoints: 10,
		Alpha:         0, // Should use default 0.3
		Confidence:    0.95,
	}

	forecaster := NewExponentialSmoothingForecaster()
	result, err := forecaster.Forecast(data, config)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}

	if result.ModelInfo.Parameters["alpha"] != 0.3 {
		t.Errorf("Expected default alpha 0.3, got %v", result.ModelInfo.Parameters["alpha"])
	}
}

func TestExponentialSmoothingForecaster_Name(t *testing.T) {
	forecaster := NewExponentialSmoothingForecaster()
	if forecaster.Name() != "exponential" {
		t.Errorf("Expected name 'exponential', got '%s'", forecaster.Name())
	}
}

func TestExponentialSmoothingForecaster_FittedValues(t *testing.T) {
	data := generateLinearData(20, 1.0, 0.0)

	config := ForecastConfig{
		Horizon:       3,
		MinDataPoints: 10,
		Alpha:         0.5,
		Confidence:    0.95,
	}

	forecaster := NewExponentialSmoothingForecaster()
	result, err := forecaster.Forecast(data, config)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}

	if len(result.Fitted) != len(data) {
		t.Errorf("Expected %d fitted values, got %d", len(data), len(result.Fitted))
	}

	if len(result.Residuals) != len(data) {
		t.Errorf("Expected %d residuals, got %d", len(data), len(result.Residuals))
	}
}

func TestExponentialSmoothingForecaster_ConfidenceInterval(t *testing.T) {
	data := generateLinearData(50, 1.0, 0.0)

	config := ForecastConfig{
		Horizon:       5,
		MinDataPoints: 10,
		Alpha:         0.3,
		Confidence:    0.95,
	}

	forecaster := NewExponentialSmoothingForecaster()
	result, err := forecaster.Forecast(data, config)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}

	// Confidence interval should widen over time
	for i := 1; i < len(result.Predictions); i++ {
		prevWidth := result.Predictions[i-1].UpperBound - result.Predictions[i-1].LowerBound
		currWidth := result.Predictions[i].UpperBound - result.Predictions[i].LowerBound
		if currWidth < prevWidth {
			t.Errorf("Prediction interval should widen: index %d has width %v < previous %v",
				i, currWidth, prevWidth)
		}
	}
}

func BenchmarkExponentialSmoothingForecaster(b *testing.B) {
	data := generateLinearData(100, 1.0, 0.0)
	config := ForecastConfig{
		Horizon:       10,
		MinDataPoints: 10,
		Alpha:         0.3,
		Confidence:    0.95,
	}
	forecaster := NewExponentialSmoothingForecaster()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = forecaster.Forecast(data, config)
	}
}
