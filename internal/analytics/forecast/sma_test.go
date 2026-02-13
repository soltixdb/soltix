package forecast

import (
	"testing"
)

func TestSMAForecaster_BasicForecast_Extended(t *testing.T) {
	data := generateLinearData(50, 1.0, 0.0)

	config := ForecastConfig{
		Horizon:       5,
		MinDataPoints: 10,
		WindowSize:    7,
		Confidence:    0.95,
	}

	forecaster := NewSMAForecaster()
	result, err := forecaster.Forecast(data, config)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}

	if len(result.Predictions) != config.Horizon {
		t.Errorf("Expected %d predictions, got %d", config.Horizon, len(result.Predictions))
	}

	if result.ModelInfo.Algorithm != "sma" {
		t.Errorf("Expected algorithm 'sma', got '%s'", result.ModelInfo.Algorithm)
	}
}

func TestSMAForecaster_InsufficientData_Extended(t *testing.T) {
	data := generateLinearData(5, 1.0, 0.0)

	config := ForecastConfig{
		Horizon:       5,
		MinDataPoints: 10,
		WindowSize:    7,
	}

	forecaster := NewSMAForecaster()
	_, err := forecaster.Forecast(data, config)
	if err == nil {
		t.Error("Expected error for insufficient data")
	}
}

func TestSMAForecaster_WindowSize(t *testing.T) {
	data := generateLinearData(30, 1.0, 0.0)

	config := ForecastConfig{
		Horizon:       3,
		MinDataPoints: 10,
		WindowSize:    5,
		Confidence:    0.95,
	}

	forecaster := NewSMAForecaster()
	result, err := forecaster.Forecast(data, config)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}

	// Check that window size is correctly used
	if result.ModelInfo.Parameters["window_size"] != 5 {
		t.Errorf("Expected window_size 5, got %v", result.ModelInfo.Parameters["window_size"])
	}
}

func TestSMAForecaster_Name_Extended(t *testing.T) {
	forecaster := NewSMAForecaster()
	if forecaster.Name() != "sma" {
		t.Errorf("Expected name 'sma', got '%s'", forecaster.Name())
	}
}

func TestSMAForecaster_ConfidenceInterval(t *testing.T) {
	data := generateLinearData(50, 1.0, 0.0)

	config := ForecastConfig{
		Horizon:       3,
		MinDataPoints: 10,
		WindowSize:    7,
		Confidence:    0.95,
	}

	forecaster := NewSMAForecaster()
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

func BenchmarkSMAForecaster(b *testing.B) {
	data := generateLinearData(100, 1.0, 0.0)
	config := ForecastConfig{
		Horizon:       10,
		MinDataPoints: 10,
		WindowSize:    7,
		Confidence:    0.95,
	}
	forecaster := NewSMAForecaster()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = forecaster.Forecast(data, config)
	}
}
