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
	SumSquares float64 // For variance calculation
}

// NewAggregatedField creates a new aggregated field from a single value
func NewAggregatedField(value float64) *AggregatedField {
	return &AggregatedField{
		Count:      1,
		Sum:        value,
		Avg:        value,
		Min:        value,
		Max:        value,
		SumSquares: value * value,
	}
}

// Merge combines another aggregated field into this one
func (af *AggregatedField) Merge(other *AggregatedField) {
	af.Count += other.Count
	af.Sum += other.Sum
	af.SumSquares += other.SumSquares

	if other.Min < af.Min {
		af.Min = other.Min
	}
	if other.Max > af.Max {
		af.Max = other.Max
	}

	// Recalculate average
	if af.Count > 0 {
		af.Avg = af.Sum / float64(af.Count)
	}
}

// AddValue adds a single value to the aggregation
func (af *AggregatedField) AddValue(value float64) {
	af.Count++
	af.Sum += value
	af.SumSquares += value * value

	if value < af.Min {
		af.Min = value
	}
	if value > af.Max {
		af.Max = value
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
