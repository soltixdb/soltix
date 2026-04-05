// Package metrics provides Prometheus metrics for SoltixDB.
//
// Metrics are organized by subsystem:
//   - router: HTTP API request metrics
//   - storage: WAL, memory store, flush metrics
//   - aggregation: aggregation pipeline metrics
//   - sync: replication sync metrics
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// =============================================================================
// Router metrics
// =============================================================================

var (
	// RouterWriteRequests counts write requests by status.
	RouterWriteRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "soltix",
		Subsystem: "router",
		Name:      "write_requests_total",
		Help:      "Total number of write requests by status.",
	}, []string{"database", "collection", "status"})

	// RouterBatchSize tracks batch write sizes.
	RouterBatchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "soltix",
		Subsystem: "router",
		Name:      "batch_size",
		Help:      "Number of points per batch write request.",
		Buckets:   []float64{1, 10, 50, 100, 500, 1000, 5000, 10000},
	})

	// RouterQueryDuration tracks query response time.
	RouterQueryDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "soltix",
		Subsystem: "router",
		Name:      "query_duration_seconds",
		Help:      "Query response time in seconds.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"database", "collection"})

	// RouterQueuePublishErrors counts queue publish failures.
	RouterQueuePublishErrors = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "soltix",
		Subsystem: "router",
		Name:      "queue_publish_errors_total",
		Help:      "Total number of queue publish errors.",
	})
)

// =============================================================================
// Storage metrics
// =============================================================================

var (
	// StorageWALWrites counts WAL write operations.
	StorageWALWrites = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "soltix",
		Subsystem: "storage",
		Name:      "wal_writes_total",
		Help:      "Total number of WAL write operations.",
	})

	// StorageWALSegments tracks current WAL segment count.
	StorageWALSegments = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "soltix",
		Subsystem: "storage",
		Name:      "wal_segment_count",
		Help:      "Current number of WAL segment files.",
	})

	// StorageMemoryStoreSize tracks memory store size in bytes.
	StorageMemoryStoreSize = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "soltix",
		Subsystem: "storage",
		Name:      "memory_store_size_bytes",
		Help:      "Current memory store size in bytes.",
	})

	// StorageMemoryStoreCount tracks memory store data point count.
	StorageMemoryStoreCount = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "soltix",
		Subsystem: "storage",
		Name:      "memory_store_count",
		Help:      "Current number of data points in memory store.",
	})

	// StorageFlushDuration tracks flush operation duration.
	StorageFlushDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "soltix",
		Subsystem: "storage",
		Name:      "flush_duration_seconds",
		Help:      "Flush operation duration in seconds.",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10},
	})

	// StorageFlushPoints counts flushed data points.
	StorageFlushPoints = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "soltix",
		Subsystem: "storage",
		Name:      "flush_points_total",
		Help:      "Total number of data points flushed to storage.",
	})

	// StorageFlushErrors counts flush failures.
	StorageFlushErrors = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "soltix",
		Subsystem: "storage",
		Name:      "flush_errors_total",
		Help:      "Total number of flush operation failures.",
	})

	// StorageWriteWorkerQueueSize tracks total pending writes across all workers.
	StorageWriteWorkerQueueSize = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "soltix",
		Subsystem: "storage",
		Name:      "write_worker_queue_size",
		Help:      "Total pending writes across all write workers.",
	})
)

// =============================================================================
// Aggregation metrics
// =============================================================================

var (
	// AggregationDuration tracks aggregation operation duration by level.
	AggregationDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "soltix",
		Subsystem: "aggregation",
		Name:      "duration_seconds",
		Help:      "Aggregation operation duration in seconds.",
		Buckets:   []float64{0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30},
	}, []string{"level"})

	// AggregationProcessed counts aggregation operations by level and status.
	AggregationProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "soltix",
		Subsystem: "aggregation",
		Name:      "operations_total",
		Help:      "Total aggregation operations by level and status.",
	}, []string{"level", "status"})
)

// =============================================================================
// Sync metrics
// =============================================================================

var (
	// SyncOperations counts sync operations by type and status.
	SyncOperations = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "soltix",
		Subsystem: "sync",
		Name:      "operations_total",
		Help:      "Total sync operations by type and status.",
	}, []string{"type", "status"})

	// SyncPointsSynced counts total points synced from replicas.
	SyncPointsSynced = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "soltix",
		Subsystem: "sync",
		Name:      "points_synced_total",
		Help:      "Total data points synced from replicas.",
	})
)
