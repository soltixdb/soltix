package aggregation

import (
	"math"
	"time"

	pb "github.com/soltixdb/soltix/proto/storage/v1"
)

// AggregatedPoint represents a single aggregated data point
type AggregatedPoint struct {
	Time     time.Time
	DeviceID string
	Level    AggregationLevel
	Complete bool // Is this bucket complete (no more updates expected)?

	Fields map[string]*AggregatedField
}

// AggregatedField represents statistics for a single field
type AggregatedField struct {
	Count      int64   // Number of raw points
	Sum        float64 // Sum of values
	Avg        float64 // Average
	Min        float64 // Minimum
	Max        float64 // Maximum
	MinTime    int64   // UnixNano timestamp of minimum value (0 = unknown)
	MaxTime    int64   // UnixNano timestamp of maximum value (0 = unknown)
	SumSquares float64 // For variance calculation
}

// NewAggregatedField creates a new aggregated field from a single value
func NewAggregatedField(value float64) *AggregatedField {
	return NewAggregatedFieldWithTime(value, time.Time{})
}

// NewAggregatedFieldWithTime creates a new aggregated field from a single value and its timestamp
func NewAggregatedFieldWithTime(value float64, observedAt time.Time) *AggregatedField {
	ts := observedAt.UnixNano()
	if observedAt.IsZero() {
		ts = 0
	}

	return &AggregatedField{
		Count:      1,
		Sum:        value,
		Avg:        value,
		Min:        value,
		Max:        value,
		MinTime:    ts,
		MaxTime:    ts,
		SumSquares: value * value,
	}
}

func mergeExtremaTime(current, candidate int64) int64 {
	if candidate == 0 {
		return current
	}
	if current == 0 || candidate < current {
		return candidate
	}
	return current
}

// Merge combines another aggregated field into this one
func (af *AggregatedField) Merge(other *AggregatedField) {
	af.Count += other.Count
	af.Sum += other.Sum
	af.SumSquares += other.SumSquares

	if other.Min < af.Min {
		af.Min = other.Min
		af.MinTime = other.MinTime
	} else if other.Min == af.Min {
		af.MinTime = mergeExtremaTime(af.MinTime, other.MinTime)
	}
	if other.Max > af.Max {
		af.Max = other.Max
		af.MaxTime = other.MaxTime
	} else if other.Max == af.Max {
		af.MaxTime = mergeExtremaTime(af.MaxTime, other.MaxTime)
	}

	// Recalculate average
	if af.Count > 0 {
		af.Avg = af.Sum / float64(af.Count)
	}
}

// AddValue adds a single value to the aggregation
func (af *AggregatedField) AddValue(value float64) {
	af.AddValueWithTime(value, time.Time{})
}

// AddValueWithTime adds a single value and timestamp to the aggregation
func (af *AggregatedField) AddValueWithTime(value float64, observedAt time.Time) {
	ts := observedAt.UnixNano()
	if observedAt.IsZero() {
		ts = 0
	}

	af.Count++
	af.Sum += value
	af.SumSquares += value * value

	if value < af.Min {
		af.Min = value
		af.MinTime = ts
	} else if value == af.Min {
		af.MinTime = mergeExtremaTime(af.MinTime, ts)
	}
	if value > af.Max {
		af.Max = value
		af.MaxTime = ts
	} else if value == af.Max {
		af.MaxTime = mergeExtremaTime(af.MaxTime, ts)
	}

	af.Avg = af.Sum / float64(af.Count)
}

// Variance calculates the variance of the aggregated values
func (af *AggregatedField) Variance() float64 {
	if af.Count <= 1 {
		return 0
	}
	// Var = E[X²] - (E[X])²
	return (af.SumSquares / float64(af.Count)) - (af.Avg * af.Avg)
}

// StdDev calculates the standard deviation
func (af *AggregatedField) StdDev() float64 {
	return math.Sqrt(af.Variance())
}

// ToProto converts AggregatedField to protobuf
func (af *AggregatedField) ToProto() *pb.AggregatedFieldValue {
	return &pb.AggregatedFieldValue{
		Count:      af.Count,
		Sum:        af.Sum,
		Avg:        af.Avg,
		Min:        af.Min,
		Max:        af.Max,
		SumSquares: af.SumSquares,
	}
}

// AggregatedFieldFromProto creates AggregatedField from protobuf
func AggregatedFieldFromProto(pbField *pb.AggregatedFieldValue) *AggregatedField {
	return &AggregatedField{
		Count:      pbField.Count,
		Sum:        pbField.Sum,
		Avg:        pbField.Avg,
		Min:        pbField.Min,
		Max:        pbField.Max,
		SumSquares: pbField.SumSquares,
	}
}
