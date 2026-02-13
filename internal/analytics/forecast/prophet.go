package forecast

import (
	"fmt"
	"math"
	"sort"
	"time"
)

// ProphetForecaster implements a Prophet-style forecasting algorithm
// Inspired by Facebook Prophet, this implementation handles:
// - Piecewise linear trend with automatic changepoint detection
// - Multiple seasonality (daily, weekly, yearly) using Fourier series
// - Robust to missing data and outliers
type ProphetForecaster struct {
	// Trend parameters
	GrowthType       string  // "linear" or "logistic"
	ChangePointRange float64 // Proportion of history for potential changepoints (0-1)
	NumChangePoints  int     // Number of potential changepoints
	ChangePointScale float64 // Flexibility of trend changes

	// Seasonality parameters
	YearlySeasonality  bool
	WeeklySeasonality  bool
	DailySeasonality   bool
	SeasonalityMode    string // "additive" or "multiplicative"
	FourierOrderYearly int
	FourierOrderWeekly int
	FourierOrderDaily  int
}

// prophetModel holds the fitted model parameters
type prophetModel struct {
	// Trend
	k            float64   // Base growth rate
	m            float64   // Offset
	changePoints []float64 // Changepoint times (normalized)
	deltas       []float64 // Rate adjustments at changepoints

	// Seasonality coefficients
	yearlyCoeffs []float64
	weeklyCoeffs []float64
	dailyCoeffs  []float64

	// Scale factors
	yScale float64
	yMin   float64
	tScale float64
	tMin   float64

	// Residual std for prediction intervals
	sigma float64
}

// NewProphetForecaster creates a new Prophet-style forecaster with default parameters
func NewProphetForecaster() *ProphetForecaster {
	return &ProphetForecaster{
		GrowthType:         "linear",
		ChangePointRange:   0.8,
		NumChangePoints:    25,
		ChangePointScale:   0.05,
		YearlySeasonality:  true,
		WeeklySeasonality:  true,
		DailySeasonality:   true,
		SeasonalityMode:    "additive",
		FourierOrderYearly: 10,
		FourierOrderWeekly: 3,
		FourierOrderDaily:  4,
	}
}

func init() {
	RegisterForecaster("prophet", NewProphetForecaster())
}

// Name returns the algorithm name
func (f *ProphetForecaster) Name() string {
	return "prophet"
}

// Forecast generates predictions using Prophet-style decomposition
func (f *ProphetForecaster) Forecast(data []DataPoint, config ForecastConfig) (*ForecastResult, error) {
	if len(data) < config.MinDataPoints {
		return nil, fmt.Errorf("insufficient data points: need at least %d, got %d", config.MinDataPoints, len(data))
	}

	// Sort data by time
	sortedData := make([]DataPoint, len(data))
	copy(sortedData, data)
	sort.Slice(sortedData, func(i, j int) bool {
		return sortedData[i].Time.Before(sortedData[j].Time)
	})

	// Fit the model
	model, err := f.fit(sortedData)
	if err != nil {
		return nil, fmt.Errorf("failed to fit prophet model: %w", err)
	}

	// Calculate fitted values
	fitted := make([]float64, len(sortedData))
	for i, dp := range sortedData {
		fitted[i] = f.predict(model, dp.Time)
	}

	// Calculate residuals
	residuals := make([]float64, len(sortedData))
	for i := range sortedData {
		residuals[i] = sortedData[i].Value - fitted[i]
	}

	// Generate future predictions
	predictions := make([]ForecastPoint, config.Horizon)
	lastTime := sortedData[len(sortedData)-1].Time

	for h := 0; h < config.Horizon; h++ {
		futureTime := lastTime.Add(time.Duration(h+1) * config.Interval)
		value := f.predict(model, futureTime)

		// Prediction interval widens with horizon
		horizonFactor := math.Sqrt(float64(h + 1))
		lower, upper := calculatePredictionInterval(value, model.sigma*horizonFactor, config.Confidence)

		predictions[h] = ForecastPoint{
			Time:       futureTime,
			Value:      value,
			LowerBound: lower,
			UpperBound: upper,
		}
	}

	// Calculate accuracy metrics
	actual := make([]float64, len(sortedData))
	for i, dp := range sortedData {
		actual[i] = dp.Value
	}

	return &ForecastResult{
		Predictions: predictions,
		Fitted:      fitted,
		Residuals:   residuals,
		ModelInfo: ModelInfo{
			Algorithm: "prophet",
			Parameters: map[string]interface{}{
				"growth":             f.GrowthType,
				"changepoint_range":  f.ChangePointRange,
				"num_changepoints":   f.NumChangePoints,
				"yearly_seasonality": f.YearlySeasonality,
				"weekly_seasonality": f.WeeklySeasonality,
				"daily_seasonality":  f.DailySeasonality,
				"seasonality_mode":   f.SeasonalityMode,
			},
			MAPE:       CalculateMAPE(actual, fitted),
			MAE:        CalculateMAE(actual, fitted),
			RMSE:       CalculateRMSE(actual, fitted),
			DataPoints: len(data),
		},
	}, nil
}

// fit estimates model parameters from historical data
func (f *ProphetForecaster) fit(data []DataPoint) (*prophetModel, error) {
	n := len(data)
	if n < 2 {
		return nil, fmt.Errorf("need at least 2 data points")
	}

	model := &prophetModel{}

	// Extract time and value arrays
	t := make([]float64, n)
	y := make([]float64, n)

	tMin := float64(data[0].Time.Unix())
	tMax := float64(data[n-1].Time.Unix())
	model.tMin = tMin
	model.tScale = tMax - tMin
	if model.tScale == 0 {
		model.tScale = 1
	}

	// Normalize time to [0, 1]
	for i, dp := range data {
		t[i] = (float64(dp.Time.Unix()) - tMin) / model.tScale
		y[i] = dp.Value
	}

	// Normalize y values
	model.yMin = y[0]
	model.yScale = 1.0
	yMax := y[0]
	for _, v := range y {
		if v < model.yMin {
			model.yMin = v
		}
		if v > yMax {
			yMax = v
		}
	}
	model.yScale = yMax - model.yMin
	if model.yScale == 0 {
		model.yScale = 1
	}

	yNorm := make([]float64, n)
	for i := range y {
		yNorm[i] = (y[i] - model.yMin) / model.yScale
	}

	// Fit trend component
	f.fitTrend(model, t, yNorm)

	// Remove trend to get detrended series
	detrended := make([]float64, n)
	for i := range t {
		trendValue := f.trendFunction(model, t[i])
		detrended[i] = yNorm[i] - trendValue
	}

	// Fit seasonality components
	if f.YearlySeasonality {
		model.yearlyCoeffs = f.fitFourierSeasonality(data, detrended, 365.25, f.FourierOrderYearly)
	}
	if f.WeeklySeasonality {
		model.weeklyCoeffs = f.fitFourierSeasonality(data, detrended, 7.0, f.FourierOrderWeekly)
	}
	if f.DailySeasonality {
		model.dailyCoeffs = f.fitFourierSeasonality(data, detrended, 1.0, f.FourierOrderDaily)
	}

	// Calculate residual standard deviation for prediction intervals
	residuals := make([]float64, n)
	for i, dp := range data {
		predicted := f.predict(model, dp.Time)
		residuals[i] = dp.Value - predicted
	}
	model.sigma = f.calculateStd(residuals)

	return model, nil
}

// fitTrend fits the piecewise linear trend
func (f *ProphetForecaster) fitTrend(model *prophetModel, t, y []float64) {
	n := len(t)

	// Simple linear regression for base trend
	sumT, sumY, sumTY, sumT2 := 0.0, 0.0, 0.0, 0.0
	for i := range t {
		sumT += t[i]
		sumY += y[i]
		sumTY += t[i] * y[i]
		sumT2 += t[i] * t[i]
	}

	nf := float64(n)
	denom := nf*sumT2 - sumT*sumT
	if denom == 0 {
		model.k = 0
		model.m = sumY / nf
	} else {
		model.k = (nf*sumTY - sumT*sumY) / denom
		model.m = (sumY - model.k*sumT) / nf
	}

	// Detect changepoints
	if f.NumChangePoints > 0 && n > f.NumChangePoints {
		changePointIdx := f.detectChangePoints(t, y, model.k, model.m)
		model.changePoints = make([]float64, len(changePointIdx))
		model.deltas = make([]float64, len(changePointIdx))

		for i, idx := range changePointIdx {
			model.changePoints[i] = t[idx]
			// Estimate local slope change
			if idx > 0 && idx < n-1 {
				localSlope := (y[idx+1] - y[idx-1]) / (t[idx+1] - t[idx-1])
				model.deltas[i] = (localSlope - model.k) * f.ChangePointScale
			}
		}
	}
}

// detectChangePoints finds potential trend changepoints
func (f *ProphetForecaster) detectChangePoints(t, y []float64, k, m float64) []int {
	n := len(t)
	rangeEnd := int(float64(n) * f.ChangePointRange)
	if rangeEnd < 2 {
		return nil
	}

	// Calculate residuals from linear trend
	residuals := make([]float64, n)
	for i := range t {
		predicted := k*t[i] + m
		residuals[i] = y[i] - predicted
	}

	// Find points with largest absolute residual changes
	type changePoint struct {
		idx   int
		score float64
	}
	candidates := make([]changePoint, 0, rangeEnd)

	windowSize := max(3, n/20)
	for i := windowSize; i < rangeEnd-windowSize; i++ {
		// Calculate mean change before and after
		beforeMean := 0.0
		afterMean := 0.0
		for j := i - windowSize; j < i; j++ {
			beforeMean += residuals[j]
		}
		for j := i; j < i+windowSize; j++ {
			afterMean += residuals[j]
		}
		beforeMean /= float64(windowSize)
		afterMean /= float64(windowSize)

		score := math.Abs(afterMean - beforeMean)
		candidates = append(candidates, changePoint{idx: i, score: score})
	}

	// Sort by score and take top N
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].score > candidates[j].score
	})

	numCP := min(f.NumChangePoints, len(candidates))
	result := make([]int, numCP)
	for i := 0; i < numCP; i++ {
		result[i] = candidates[i].idx
	}

	// Sort by index
	sort.Ints(result)
	return result
}

// trendFunction calculates trend at normalized time t
func (f *ProphetForecaster) trendFunction(model *prophetModel, t float64) float64 {
	trend := model.k*t + model.m

	// Add changepoint adjustments
	for i, cp := range model.changePoints {
		if t > cp {
			trend += model.deltas[i] * (t - cp)
		}
	}

	return trend
}

// fitFourierSeasonality fits Fourier series for seasonality
func (f *ProphetForecaster) fitFourierSeasonality(data []DataPoint, detrended []float64, period float64, order int) []float64 {
	numCoeffs := 2 * order
	coeffs := make([]float64, numCoeffs)

	// Convert period to seconds
	periodSec := period * 24 * 3600

	// Calculate Fourier coefficients using least squares approximation
	for k := 1; k <= order; k++ {
		sinSum, cosSum := 0.0, 0.0
		sinSqSum, cosSqSum := 0.0, 0.0

		for i, dp := range data {
			tSec := float64(dp.Time.Unix())
			phase := 2.0 * math.Pi * float64(k) * tSec / periodSec

			sinVal := math.Sin(phase)
			cosVal := math.Cos(phase)

			sinSum += detrended[i] * sinVal
			cosSum += detrended[i] * cosVal
			sinSqSum += sinVal * sinVal
			cosSqSum += cosVal * cosVal
		}

		// Coefficients
		if sinSqSum > 0 {
			coeffs[2*(k-1)] = sinSum / sinSqSum
		}
		if cosSqSum > 0 {
			coeffs[2*(k-1)+1] = cosSum / cosSqSum
		}
	}

	return coeffs
}

// seasonalityFunction calculates seasonality at a given time
func (f *ProphetForecaster) seasonalityFunction(coeffs []float64, t time.Time, period float64) float64 {
	if len(coeffs) == 0 {
		return 0
	}

	periodSec := period * 24 * 3600
	tSec := float64(t.Unix())

	result := 0.0
	order := len(coeffs) / 2

	for k := 1; k <= order; k++ {
		phase := 2.0 * math.Pi * float64(k) * tSec / periodSec
		result += coeffs[2*(k-1)] * math.Sin(phase)
		result += coeffs[2*(k-1)+1] * math.Cos(phase)
	}

	return result
}

// predict generates prediction for a given time
func (f *ProphetForecaster) predict(model *prophetModel, t time.Time) float64 {
	// Normalize time
	tNorm := (float64(t.Unix()) - model.tMin) / model.tScale

	// Trend component
	trend := f.trendFunction(model, tNorm)

	// Seasonality components
	seasonality := 0.0
	if f.YearlySeasonality && len(model.yearlyCoeffs) > 0 {
		seasonality += f.seasonalityFunction(model.yearlyCoeffs, t, 365.25)
	}
	if f.WeeklySeasonality && len(model.weeklyCoeffs) > 0 {
		seasonality += f.seasonalityFunction(model.weeklyCoeffs, t, 7.0)
	}
	if f.DailySeasonality && len(model.dailyCoeffs) > 0 {
		seasonality += f.seasonalityFunction(model.dailyCoeffs, t, 1.0)
	}

	// Combine components
	var prediction float64
	if f.SeasonalityMode == "multiplicative" {
		prediction = trend * (1 + seasonality)
	} else {
		prediction = trend + seasonality
	}

	// Denormalize
	return prediction*model.yScale + model.yMin
}

// calculateStd calculates standard deviation
func (f *ProphetForecaster) calculateStd(values []float64) float64 {
	if len(values) == 0 {
		return 1.0
	}

	mean := 0.0
	for _, v := range values {
		mean += v
	}
	mean /= float64(len(values))

	variance := 0.0
	for _, v := range values {
		diff := v - mean
		variance += diff * diff
	}
	variance /= float64(len(values))

	std := math.Sqrt(variance)
	if std == 0 {
		return 1.0
	}
	return std
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
