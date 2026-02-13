package forecast

import (
	"testing"
)

func TestLinearRegressionForecaster_BasicForecast(t *testing.T) {
	data := generateLinearData(50, 2.0, 5.0) // y = 2x + 5

	config := ForecastConfig{
		Horizon:       5,
		MinDataPoints: 10,
		Confidence:    0.95,
	}

	forecaster := NewLinearRegressionForecaster()
	result, err := forecaster.Forecast(data, config)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}

	if len(result.Predictions) != config.Horizon {
		t.Errorf("Expected %d predictions, got %d", config.Horizon, len(result.Predictions))
	}

	if result.ModelInfo.Algorithm != "linear" {
		t.Errorf("Expected algorithm 'linear', got '%s'", result.ModelInfo.Algorithm)
	}
}

func TestLinearRegressionForecaster_InsufficientData(t *testing.T) {
	data := generateLinearData(5, 1.0, 0.0)

	config := ForecastConfig{
		Horizon:       5,
		MinDataPoints: 10,
	}

	forecaster := NewLinearRegressionForecaster()
	_, err := forecaster.Forecast(data, config)
	if err == nil {
		t.Error("Expected error for insufficient data")
	}
}

func TestLinearRegressionForecaster_Name(t *testing.T) {
	forecaster := NewLinearRegressionForecaster()
	if forecaster.Name() != "linear" {
		t.Errorf("Expected name 'linear', got '%s'", forecaster.Name())
	}
}

func TestLinearRegressionForecaster_TrendDetection(t *testing.T) {
	// Strong upward trend
	data := generateLinearData(30, 5.0, 0.0)

	config := ForecastConfig{
		Horizon:       3,
		MinDataPoints: 10,
		Confidence:    0.95,
	}

	forecaster := NewLinearRegressionForecaster()
	result, err := forecaster.Forecast(data, config)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}

	// Predictions should continue upward trend
	lastActual := data[len(data)-1].Value
	for i, pred := range result.Predictions {
		if pred.Value <= lastActual {
			t.Errorf("Prediction %d: value %v should be > last actual %v for upward trend",
				i, pred.Value, lastActual)
		}
	}
}

func TestLinearRegressionForecaster_ConfidenceInterval(t *testing.T) {
	data := generateLinearData(50, 1.0, 0.0)

	config := ForecastConfig{
		Horizon:       5,
		MinDataPoints: 10,
		Confidence:    0.95,
	}

	forecaster := NewLinearRegressionForecaster()
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

func TestLinearRegressionForecaster_Metrics(t *testing.T) {
	data := generateLinearData(30, 1.0, 0.0)

	config := ForecastConfig{
		Horizon:       3,
		MinDataPoints: 10,
		Confidence:    0.95,
	}

	forecaster := NewLinearRegressionForecaster()
	result, err := forecaster.Forecast(data, config)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}

	// For perfect linear data, RMSE should be very low
	if result.ModelInfo.RMSE > 1 {
		t.Errorf("RMSE too high for linear data: %v", result.ModelInfo.RMSE)
	}
}

func BenchmarkLinearRegressionForecaster(b *testing.B) {
	data := generateLinearData(100, 1.0, 0.0)
	config := ForecastConfig{
		Horizon:       10,
		MinDataPoints: 10,
		Confidence:    0.95,
	}
	forecaster := NewLinearRegressionForecaster()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = forecaster.Forecast(data, config)
	}
}
