package aggregation

import (
	"time"
)

// =============================================================================
// Event types for aggregation pipeline
// =============================================================================

// FlushCompleteEvent is sent from storage FlushWorker to AggregationPipeline
// when a partition flush completes
type FlushCompleteEvent struct {
	Database    string
	Date        time.Time
	Collections []string // affected collections
	PointCount  int
	FlushTime   time.Time
}

// AggregateCompleteEvent is sent between aggregation stages
// when aggregation at one level completes
type AggregateCompleteEvent struct {
	Database   string
	Collection string
	Level      PipelineLevel
	TimeKey    time.Time // hour/day/month/year start time
	DeviceIDs  []string  // affected devices (optional)
}

// PipelineLevel represents the aggregation granularity in pipeline
// This is separate from AggregationLevel (which is the storage format "1h", "1d", etc.)
type PipelineLevel int

const (
	PipelineLevelHourly PipelineLevel = iota
	PipelineLevelDaily
	PipelineLevelMonthly
	PipelineLevelYearly
)

func (l PipelineLevel) String() string {
	switch l {
	case PipelineLevelHourly:
		return "hourly"
	case PipelineLevelDaily:
		return "daily"
	case PipelineLevelMonthly:
		return "monthly"
	case PipelineLevelYearly:
		return "yearly"
	default:
		return "unknown"
	}
}

// ToStorageLevel converts PipelineLevel to storage AggregationLevel
func (l PipelineLevel) ToStorageLevel() AggregationLevel {
	switch l {
	case PipelineLevelHourly:
		return AggregationHourly
	case PipelineLevelDaily:
		return AggregationDaily
	case PipelineLevelMonthly:
		return AggregationMonthly
	case PipelineLevelYearly:
		return AggregationYearly
	default:
		return AggregationHourly
	}
}

// =============================================================================
// Partition key helpers
// =============================================================================

// PartitionKey generates a partition key from database and date
func PartitionKey(database string, date time.Time) string {
	return database + ":" + date.Format("2006-01-02")
}

// CollectionPartitionKey generates a key for collection-level partitioning
func CollectionPartitionKey(database, collection string, date time.Time) string {
	return database + ":" + collection + ":" + date.Format("2006-01-02")
}

// HourPartitionKey generates a key for hour-level partitioning
func HourPartitionKey(database, collection string, hour time.Time) string {
	return database + ":" + collection + ":" + hour.Format("2006-01-02T15")
}
