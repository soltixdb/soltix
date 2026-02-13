package aggregation

import (
	"fmt"
	"time"
)

// RawDataPoint represents a minimal interface for raw data points
// This avoids circular dependency with storage package
type RawDataPoint struct {
	Time   time.Time
	Fields map[string]interface{}
}

// ConvertToRawDataPoint creates RawDataPoint from any point with Time and Fields
func ConvertToRawDataPoint(time time.Time, fields map[string]interface{}) *RawDataPoint {
	return &RawDataPoint{
		Time:   time,
		Fields: fields,
	}
}

// AggregateRawDataPoints aggregates raw data points into a single aggregated point
func AggregateRawDataPoints(deviceID string, points []*RawDataPoint, level AggregationLevel) (*AggregatedPoint, error) {
	if len(points) == 0 {
		return nil, fmt.Errorf("no data points to aggregate")
	}

	// Truncate time based on level
	var bucketTime time.Time
	switch level {
	case AggregationDaily:
		t := points[0].Time
		bucketTime = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	case AggregationMonthly:
		bucketTime = TruncateToMonth(points[0].Time)
	case AggregationYearly:
		bucketTime = TruncateToYear(points[0].Time)
	default:
		bucketTime = points[0].Time.Truncate(GetLevelDuration(level))
	}

	agg := &AggregatedPoint{
		Time:     bucketTime,
		DeviceID: deviceID,
		Level:    level,
		Complete: false,
		Fields:   make(map[string]*AggregatedField),
	}

	// Aggregate each field
	for _, point := range points {
		for fieldName, fieldValue := range point.Fields {
			// Convert to float64
			var value float64
			switch v := fieldValue.(type) {
			case float64:
				value = v
			case int:
				value = float64(v)
			case int64:
				value = float64(v)
			default:
				// Skip non-numeric fields
				continue
			}

			if agg.Fields[fieldName] == nil {
				agg.Fields[fieldName] = NewAggregatedField(value)
			} else {
				agg.Fields[fieldName].AddValue(value)
			}
		}
	}

	return agg, nil
}

// AggregateAggregatedPoints aggregates multiple aggregated points (for cascading)
func AggregateAggregatedPoints(deviceID string, points []*AggregatedPoint, level AggregationLevel) (*AggregatedPoint, error) {
	if len(points) == 0 {
		return nil, fmt.Errorf("no aggregated points to aggregate")
	}

	// Truncate time based on level
	var bucketTime time.Time
	switch level {
	case AggregationDaily:
		t := points[0].Time
		bucketTime = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	case AggregationMonthly:
		bucketTime = TruncateToMonth(points[0].Time)
	case AggregationYearly:
		bucketTime = TruncateToYear(points[0].Time)
	default:
		bucketTime = points[0].Time.Truncate(GetLevelDuration(level))
	}

	agg := &AggregatedPoint{
		Time:     bucketTime,
		DeviceID: deviceID,
		Level:    level,
		Complete: false,
		Fields:   make(map[string]*AggregatedField),
	}

	// Merge all fields
	for _, point := range points {
		for fieldName, field := range point.Fields {
			if agg.Fields[fieldName] == nil {
				// Deep copy
				agg.Fields[fieldName] = &AggregatedField{
					Count:      field.Count,
					Sum:        field.Sum,
					Avg:        field.Avg,
					Min:        field.Min,
					Max:        field.Max,
					SumSquares: field.SumSquares,
				}
			} else {
				agg.Fields[fieldName].Merge(field)
			}
		}
	}

	return agg, nil
}
