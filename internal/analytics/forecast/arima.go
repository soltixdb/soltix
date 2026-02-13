package forecast

import (
	"fmt"
	"math"
	"time"
)

// ARIMAForecaster implements ARIMA (AutoRegressive Integrated Moving Average) forecasting
// ARIMA(p, d, q) where:
// - p: order of autoregressive (AR) part
// - d: degree of differencing (I) to make series stationary
// - q: order of moving average (MA) part
type ARIMAForecaster struct {
	P int // AR order
	D int // Differencing order
	Q int // MA order
}

// NewARIMAForecaster creates a new ARIMA forecaster with default parameters
func NewARIMAForecaster() *ARIMAForecaster {
	return &ARIMAForecaster{
		P: 2, // Default AR(2)
		D: 1, // Default first-order differencing
		Q: 2, // Default MA(2)
	}
}

// NewARIMAForecasterWithParams creates ARIMA forecaster with custom parameters
func NewARIMAForecasterWithParams(p, d, q int) *ARIMAForecaster {
	return &ARIMAForecaster{
		P: p,
		D: d,
		Q: q,
	}
}

func init() {
	RegisterForecaster("arima", NewARIMAForecaster())
}

// Name returns the algorithm name
func (f *ARIMAForecaster) Name() string {
	return "arima"
}

// Forecast generates predictions using ARIMA model
func (f *ARIMAForecaster) Forecast(data []DataPoint, config ForecastConfig) (*ForecastResult, error) {
	if len(data) < config.MinDataPoints {
		return nil, fmt.Errorf("insufficient data points: need at least %d, got %d", config.MinDataPoints, len(data))
	}

	// Extract values
	values := make([]float64, len(data))
	for i, dp := range data {
		values[i] = dp.Value
	}

	// Apply differencing
	diffValues, originalMean := f.difference(values, f.D)
	if len(diffValues) < f.P+f.Q+1 {
		return nil, fmt.Errorf("insufficient data after differencing: need at least %d, got %d", f.P+f.Q+1, len(diffValues))
	}

	// Estimate AR coefficients using Yule-Walker equations
	arCoeffs := f.estimateARCoefficients(diffValues, f.P)

	// Estimate MA coefficients using residuals
	maCoeffs := f.estimateMACoefficients(diffValues, arCoeffs, f.Q)

	// Calculate fitted values and residuals
	fitted := f.calculateFitted(diffValues, arCoeffs, maCoeffs)
	residuals := make([]float64, len(fitted))
	for i := range fitted {
		if i < len(diffValues) {
			residuals[i] = diffValues[i] - fitted[i]
		}
	}

	// Calculate standard error for prediction intervals
	stdError := f.calculateStdError(residuals)

	// Generate forecasts
	predictions := make([]ForecastPoint, config.Horizon)
	lastTime := data[len(data)-1].Time

	// Prepare recent values and residuals for forecasting
	recentDiff := make([]float64, len(diffValues))
	copy(recentDiff, diffValues)
	recentResiduals := make([]float64, len(residuals))
	copy(recentResiduals, residuals)

	for h := 0; h < config.Horizon; h++ {
		// Calculate AR component
		arComponent := 0.0
		for i := 0; i < f.P && i < len(recentDiff); i++ {
			arComponent += arCoeffs[i] * recentDiff[len(recentDiff)-1-i]
		}

		// Calculate MA component (use 0 for future errors)
		maComponent := 0.0
		for i := 0; i < f.Q && i < len(recentResiduals); i++ {
			maComponent += maCoeffs[i] * recentResiduals[len(recentResiduals)-1-i]
		}

		// Forecast in differenced space
		forecastDiff := arComponent + maComponent

		// Append to recent values for next iteration
		recentDiff = append(recentDiff, forecastDiff)
		recentResiduals = append(recentResiduals, 0) // Future residuals are 0

		// Inverse differencing to get actual forecast
		forecastValue := f.inverseDifference(values, recentDiff[len(diffValues):h+1+len(diffValues)], f.D, originalMean)

		// Calculate prediction interval (wider for further horizons)
		horizonFactor := math.Sqrt(float64(h + 1))
		lower, upper := calculatePredictionInterval(forecastValue, stdError*horizonFactor, config.Confidence)

		predictions[h] = ForecastPoint{
			Time:       lastTime.Add(time.Duration(h+1) * config.Interval),
			Value:      forecastValue,
			LowerBound: lower,
			UpperBound: upper,
		}
	}

	// Prepare fitted values in original scale
	fittedOriginal := f.inverseDifferenceAll(values, fitted, f.D)

	// Calculate accuracy metrics on original scale
	actualForMetrics := values[f.D:]
	if len(fittedOriginal) > len(actualForMetrics) {
		fittedOriginal = fittedOriginal[:len(actualForMetrics)]
	}

	return &ForecastResult{
		Predictions: predictions,
		Fitted:      fittedOriginal,
		Residuals:   residuals,
		ModelInfo: ModelInfo{
			Algorithm: "arima",
			Parameters: map[string]interface{}{
				"p": f.P,
				"d": f.D,
				"q": f.Q,
			},
			MAPE:       CalculateMAPE(actualForMetrics, fittedOriginal),
			MAE:        CalculateMAE(actualForMetrics, fittedOriginal),
			RMSE:       CalculateRMSE(actualForMetrics, fittedOriginal),
			DataPoints: len(data),
		},
	}, nil
}

// difference applies differencing d times to make series stationary
func (f *ARIMAForecaster) difference(values []float64, d int) ([]float64, float64) {
	if d == 0 || len(values) == 0 {
		return values, 0
	}

	result := values
	originalMean := mean(values)

	for i := 0; i < d; i++ {
		diffed := make([]float64, len(result)-1)
		for j := 1; j < len(result); j++ {
			diffed[j-1] = result[j] - result[j-1]
		}
		result = diffed
	}

	return result, originalMean
}

// inverseDifference reverses differencing to get original scale
func (f *ARIMAForecaster) inverseDifference(original, forecasts []float64, d int, _ float64) float64 {
	if d == 0 || len(forecasts) == 0 {
		return forecasts[len(forecasts)-1]
	}

	// Start from the last original value
	lastValue := original[len(original)-1]

	// Cumulatively add differenced forecasts
	for i := 0; i < len(forecasts); i++ {
		lastValue += forecasts[i]
	}

	return lastValue
}

// inverseDifferenceAll converts all fitted values back to original scale
func (f *ARIMAForecaster) inverseDifferenceAll(original, fitted []float64, d int) []float64 {
	if d == 0 {
		return fitted
	}

	result := make([]float64, len(fitted))
	for i := range fitted {
		// Reconstruct from original values plus cumulative differences
		if i+d < len(original) {
			baseValue := original[i]
			for j := 0; j < d && i+j < len(original)-1; j++ {
				baseValue = original[i+j+1]
			}
			result[i] = baseValue + fitted[i] - (original[i+d] - original[i+d-1])
		} else {
			result[i] = fitted[i]
		}
	}

	// Simpler approach: use differenced fitted values
	result = make([]float64, len(fitted))
	for i := range fitted {
		idx := i + d
		if idx < len(original) {
			result[i] = original[idx-1] + fitted[i]
		}
	}

	return result
}

// estimateARCoefficients estimates AR coefficients using Yule-Walker equations
func (f *ARIMAForecaster) estimateARCoefficients(values []float64, p int) []float64 {
	if p == 0 || len(values) < p+1 {
		return []float64{}
	}

	// Calculate autocorrelations
	acf := f.autocorrelation(values, p)

	// Solve Yule-Walker equations using Levinson-Durbin recursion
	return f.levinsonDurbin(acf, p)
}

// estimateMACoefficients estimates MA coefficients from residuals
func (f *ARIMAForecaster) estimateMACoefficients(values []float64, arCoeffs []float64, q int) []float64 {
	if q == 0 {
		return []float64{}
	}

	// Calculate residuals from AR model
	residuals := make([]float64, len(values))
	p := len(arCoeffs)

	for t := p; t < len(values); t++ {
		predicted := 0.0
		for i := 0; i < p; i++ {
			predicted += arCoeffs[i] * values[t-1-i]
		}
		residuals[t] = values[t] - predicted
	}

	// Estimate MA coefficients from autocorrelation of residuals
	// Using simplified approach: MA coefficients from residual autocorrelation
	maCoeffs := make([]float64, q)
	acf := f.autocorrelation(residuals[p:], q)

	for i := 0; i < q && i < len(acf); i++ {
		// Approximate MA coefficient from ACF
		// For MA(q), the ACF cuts off after lag q
		maCoeffs[i] = acf[i] * 0.5 // Damping factor for stability
	}

	return maCoeffs
}

// autocorrelation calculates autocorrelation function up to lag k
func (f *ARIMAForecaster) autocorrelation(values []float64, k int) []float64 {
	n := len(values)
	if n == 0 || k <= 0 {
		return []float64{}
	}

	mu := mean(values)
	variance := 0.0
	for _, v := range values {
		diff := v - mu
		variance += diff * diff
	}

	if variance == 0 {
		return make([]float64, k)
	}

	acf := make([]float64, k)
	for lag := 1; lag <= k; lag++ {
		cov := 0.0
		for t := lag; t < n; t++ {
			cov += (values[t] - mu) * (values[t-lag] - mu)
		}
		acf[lag-1] = cov / variance
	}

	return acf
}

// levinsonDurbin solves Yule-Walker equations using Levinson-Durbin algorithm
func (f *ARIMAForecaster) levinsonDurbin(acf []float64, p int) []float64 {
	if len(acf) == 0 || p == 0 {
		return []float64{}
	}

	// Ensure we have enough ACF values
	if len(acf) < p {
		p = len(acf)
	}

	phi := make([][]float64, p+1)
	for i := range phi {
		phi[i] = make([]float64, p+1)
	}

	// Initialize
	phi[1][1] = acf[0]
	v := 1 - acf[0]*acf[0]

	for k := 2; k <= p; k++ {
		// Calculate phi[k][k]
		num := acf[k-1]
		for j := 1; j < k; j++ {
			num -= phi[k-1][j] * acf[k-1-j]
		}

		if v == 0 {
			break
		}

		phi[k][k] = num / v

		// Update other coefficients
		for j := 1; j < k; j++ {
			phi[k][j] = phi[k-1][j] - phi[k][k]*phi[k-1][k-j]
		}

		// Update variance
		v = v * (1 - phi[k][k]*phi[k][k])
	}

	// Extract final coefficients
	result := make([]float64, p)
	for i := 1; i <= p; i++ {
		result[i-1] = phi[p][i]
	}

	return result
}

// calculateFitted calculates fitted values using AR and MA components
func (f *ARIMAForecaster) calculateFitted(values []float64, arCoeffs, maCoeffs []float64) []float64 {
	n := len(values)
	p := len(arCoeffs)
	q := len(maCoeffs)
	start := max(p, q)

	if n <= start {
		return make([]float64, n)
	}

	fitted := make([]float64, n)
	residuals := make([]float64, n)

	for t := start; t < n; t++ {
		// AR component
		arComponent := 0.0
		for i := 0; i < p; i++ {
			arComponent += arCoeffs[i] * values[t-1-i]
		}

		// MA component
		maComponent := 0.0
		for i := 0; i < q && t-1-i >= 0; i++ {
			maComponent += maCoeffs[i] * residuals[t-1-i]
		}

		fitted[t] = arComponent + maComponent
		residuals[t] = values[t] - fitted[t]
	}

	return fitted
}

// calculateStdError calculates standard error from residuals
func (f *ARIMAForecaster) calculateStdError(residuals []float64) float64 {
	if len(residuals) == 0 {
		return 1.0
	}

	// Filter out zero residuals from the beginning
	nonZero := make([]float64, 0, len(residuals))
	for _, r := range residuals {
		if r != 0 {
			nonZero = append(nonZero, r)
		}
	}

	if len(nonZero) == 0 {
		return 1.0
	}

	// Calculate standard deviation
	mu := mean(nonZero)
	sumSq := 0.0
	for _, r := range nonZero {
		diff := r - mu
		sumSq += diff * diff
	}

	return math.Sqrt(sumSq / float64(len(nonZero)))
}

// mean calculates the mean of a slice
func mean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

// max returns the maximum of two integers
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
