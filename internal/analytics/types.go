// Package analytics provides common types and utilities for time-series analytics
// including forecasting and anomaly detection.
package analytics

import (
	"math"
	"time"
)

// TimeSeriesPoint represents a single time-series data point with time and value.
// This is the common type used across all analytics packages (forecast, anomaly, etc.)
type TimeSeriesPoint struct {
	Time  time.Time
	Value float64
}

// TimeSeriesData represents a collection of time-series data points
type TimeSeriesData []TimeSeriesPoint

// Values extracts just the values from the time series
func (ts TimeSeriesData) Values() []float64 {
	values := make([]float64, len(ts))
	for i, p := range ts {
		values[i] = p.Value
	}
	return values
}

// Times extracts just the times from the time series
func (ts TimeSeriesData) Times() []time.Time {
	times := make([]time.Time, len(ts))
	for i, p := range ts {
		times[i] = p.Time
	}
	return times
}

// Len returns the number of data points
func (ts TimeSeriesData) Len() int {
	return len(ts)
}

// Mean calculates the mean of all values
func (ts TimeSeriesData) Mean() float64 {
	if len(ts) == 0 {
		return 0
	}
	sum := 0.0
	for _, p := range ts {
		sum += p.Value
	}
	return sum / float64(len(ts))
}

// StdDev calculates the standard deviation of all values
func (ts TimeSeriesData) StdDev() float64 {
	if len(ts) < 2 {
		return 0
	}
	mean := ts.Mean()
	sumSq := 0.0
	for _, p := range ts {
		diff := p.Value - mean
		sumSq += diff * diff
	}
	return math.Sqrt(sumSq / float64(len(ts)-1))
}
