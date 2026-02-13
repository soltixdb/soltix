package utils

import "time"

// =============================================================================
// Timeout Constants
// =============================================================================

// HTTP Handler Timeouts
const (
	// DefaultRequestTimeout is the default timeout for HTTP requests
	DefaultRequestTimeout = 30 * time.Second

	// ValidationTimeout is the timeout for input validation operations
	ValidationTimeout = 5 * time.Second

	// RouteTimeout is the timeout for routing operations
	RouteTimeout = 5 * time.Second

	// BatchWriteTimeout is the timeout for batch write operations
	BatchWriteTimeout = 10 * time.Second

	// MetadataTrackingTimeout is the timeout for async metadata tracking
	MetadataTrackingTimeout = 5 * time.Second
)

// gRPC Timeouts
const (
	// GRPCDialTimeout is the timeout for establishing gRPC connections
	GRPCDialTimeout = 10 * time.Second

	// GRPCRequestTimeout is the default timeout for gRPC requests
	GRPCRequestTimeout = 5 * time.Second

	// GRPCHealthCheckInterval is the interval between health checks for gRPC connections
	GRPCHealthCheckInterval = 30 * time.Second
)

// =============================================================================
// Storage Constants
// =============================================================================

const (
	// DefaultMemoryStoreRetention is the default retention period for in-memory data
	DefaultMemoryStoreRetention = 2 * time.Hour

	// DefaultMemoryStoreMaxSize is the default maximum size in MB for memory store
	DefaultMemoryStoreMaxSize = 1024

	// HoursPerDay is the number of hours in a day
	HoursPerDay = 24 * time.Hour
)

// =============================================================================
// Aggregation Constants
// =============================================================================

// Aggregation Worker Pool Configuration
const (
	// HourlyWorkerIdleTimeout is the idle timeout for hourly aggregation workers
	HourlyWorkerIdleTimeout = 5 * time.Minute
	// HourlyBatchDelay is the batch delay for hourly aggregation
	HourlyBatchDelay = 2 * time.Second

	// DailyWorkerIdleTimeout is the idle timeout for daily aggregation workers
	DailyWorkerIdleTimeout = 10 * time.Minute
	// DailyBatchDelay is the batch delay for daily aggregation
	DailyBatchDelay = 5 * time.Second

	// MonthlyWorkerIdleTimeout is the idle timeout for monthly aggregation workers
	MonthlyWorkerIdleTimeout = 15 * time.Minute
	// MonthlyBatchDelay is the batch delay for monthly aggregation
	MonthlyBatchDelay = 10 * time.Second

	// YearlyWorkerIdleTimeout is the idle timeout for yearly aggregation workers
	YearlyWorkerIdleTimeout = 30 * time.Minute
	// YearlyBatchDelay is the batch delay for yearly aggregation
	YearlyBatchDelay = 30 * time.Second
)

// =============================================================================
// Forecast Interval Constants
// =============================================================================

const (
	// ForecastInterval1m is the 1-minute forecast interval
	ForecastInterval1m = 5 * time.Minute
	// ForecastInterval5m is the 5-minute forecast interval
	ForecastInterval5m = 15 * time.Minute
	// ForecastInterval15m is the 15-minute forecast interval
	ForecastInterval15m = 30 * time.Minute
	// ForecastInterval1h is the 1-hour forecast interval
	ForecastInterval1h = 4 * time.Hour
	// ForecastInterval1d is the 1-day forecast interval
	ForecastInterval1d = 24 * time.Hour
	// ForecastInterval1w is the 1-week forecast interval
	ForecastInterval1w = 7 * 24 * time.Hour
)

// =============================================================================
// Retry and Backoff Constants
// =============================================================================

const (
	// DefaultMaxRetries is the default number of retry attempts
	DefaultMaxRetries = 3

	// DefaultRetryBackoff is the default backoff duration between retries
	DefaultRetryBackoff = 100 * time.Millisecond

	// MaxRetryBackoff is the maximum backoff duration
	MaxRetryBackoff = 5 * time.Second
)

// =============================================================================
// Buffer and Batch Size Constants
// =============================================================================

const (
	// DefaultBatchSize is the default batch size for bulk operations
	DefaultBatchSize = 1000

	// DefaultBufferSize is the default buffer size for channels
	DefaultBufferSize = 100

	// MaxBatchSize is the maximum allowed batch size
	MaxBatchSize = 10000
)

// =============================================================================
// Queue Type Constants
// =============================================================================
// QueueType represents the type of message queue
type QueueType string

const (
	// QueueTypeNATS represents NATS JetStream queue (default)
	QueueTypeNATS QueueType = "nats"

	// QueueTypeRedis represents Redis Streams queue
	QueueTypeRedis QueueType = "redis"

	// QueueTypeKafka represents Apache Kafka queue
	QueueTypeKafka QueueType = "kafka"

	// QueueTypeMemory represents in-memory queue (for testing)
	QueueTypeMemory QueueType = "memory"
)
