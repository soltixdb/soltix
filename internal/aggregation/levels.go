package aggregation

import "time"

// AggregationLevel represents the time bucket size
type AggregationLevel string

const (
	AggregationHourly  AggregationLevel = "1h"
	AggregationDaily   AggregationLevel = "1d"
	AggregationMonthly AggregationLevel = "1M"
	AggregationYearly  AggregationLevel = "1y"
)

// GetLevelDuration returns the duration for truncating timestamps
func GetLevelDuration(level AggregationLevel) time.Duration {
	switch level {
	case AggregationHourly:
		return time.Hour
	case AggregationDaily:
		return 24 * time.Hour
	case AggregationMonthly:
		return 24 * time.Hour // Will be handled specially
	case AggregationYearly:
		return 24 * time.Hour // Will be handled specially
	default:
		return time.Hour
	}
}

// TruncateToMonth truncates time to the start of the month
func TruncateToMonth(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location())
}

// TruncateToYear truncates time to the start of the year
func TruncateToYear(t time.Time) time.Time {
	return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location())
}
