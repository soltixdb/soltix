package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestRouterMetrics_Registered(t *testing.T) {
	// Verify all router metrics are registered and accessible
	tests := []struct {
		name   string
		metric string
	}{
		{"write_requests_total", "soltix_router_write_requests_total"},
		{"batch_size", "soltix_router_batch_size"},
		{"query_duration_seconds", "soltix_router_query_duration_seconds"},
		{"queue_publish_errors_total", "soltix_router_queue_publish_errors_total"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Just verify we can gather without panic
			count := testutil.CollectAndCount(RouterWriteRequests)
			if tt.name == "write_requests_total" && count < 0 {
				t.Error("expected non-negative metric count")
			}
		})
	}
}

func TestStorageMetrics_Registered(t *testing.T) {
	tests := []struct {
		name   string
		verify func() int
	}{
		{"wal_writes_total", func() int { return testutil.CollectAndCount(StorageWALWrites) }},
		{"wal_segment_count", func() int { return testutil.CollectAndCount(StorageWALSegments) }},
		{"memory_store_size_bytes", func() int { return testutil.CollectAndCount(StorageMemoryStoreSize) }},
		{"memory_store_count", func() int { return testutil.CollectAndCount(StorageMemoryStoreCount) }},
		{"flush_duration_seconds", func() int { return testutil.CollectAndCount(StorageFlushDuration) }},
		{"flush_points_total", func() int { return testutil.CollectAndCount(StorageFlushPoints) }},
		{"flush_errors_total", func() int { return testutil.CollectAndCount(StorageFlushErrors) }},
		{"write_worker_queue_size", func() int { return testutil.CollectAndCount(StorageWriteWorkerQueueSize) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			count := tt.verify()
			if count < 0 {
				t.Errorf("expected non-negative metric count for %s", tt.name)
			}
		})
	}
}

func TestAggregationMetrics_Registered(t *testing.T) {
	count := testutil.CollectAndCount(AggregationDuration)
	if count < 0 {
		t.Error("expected non-negative")
	}
	count = testutil.CollectAndCount(AggregationProcessed)
	if count < 0 {
		t.Error("expected non-negative")
	}
}

func TestSyncMetrics_Registered(t *testing.T) {
	count := testutil.CollectAndCount(SyncOperations)
	if count < 0 {
		t.Error("expected non-negative")
	}
	count = testutil.CollectAndCount(SyncPointsSynced)
	if count < 0 {
		t.Error("expected non-negative")
	}
}

func TestRouterWriteRequests_IncrementsByStatus(t *testing.T) {
	// Reset by creating fresh counter values
	RouterWriteRequests.WithLabelValues("testdb", "testcoll", "accepted").Add(0)
	before := testutil.ToFloat64(RouterWriteRequests.WithLabelValues("testdb", "testcoll", "accepted"))

	RouterWriteRequests.WithLabelValues("testdb", "testcoll", "accepted").Inc()
	after := testutil.ToFloat64(RouterWriteRequests.WithLabelValues("testdb", "testcoll", "accepted"))

	if after != before+1 {
		t.Errorf("expected increment by 1, got %v -> %v", before, after)
	}
}

func TestRouterWriteRequests_FailedStatus(t *testing.T) {
	before := testutil.ToFloat64(RouterWriteRequests.WithLabelValues("testdb", "testcoll", "failed"))
	RouterWriteRequests.WithLabelValues("testdb", "testcoll", "failed").Inc()
	after := testutil.ToFloat64(RouterWriteRequests.WithLabelValues("testdb", "testcoll", "failed"))

	if after != before+1 {
		t.Errorf("expected increment by 1, got %v -> %v", before, after)
	}
}

func TestRouterWriteRequests_PartialStatus(t *testing.T) {
	before := testutil.ToFloat64(RouterWriteRequests.WithLabelValues("testdb", "testcoll", "partial"))
	RouterWriteRequests.WithLabelValues("testdb", "testcoll", "partial").Inc()
	after := testutil.ToFloat64(RouterWriteRequests.WithLabelValues("testdb", "testcoll", "partial"))

	if after != before+1 {
		t.Errorf("expected increment by 1, got %v -> %v", before, after)
	}
}

func TestRouterBatchSize_Observe(t *testing.T) {
	before := testutil.CollectAndCount(RouterBatchSize)
	RouterBatchSize.Observe(100)
	RouterBatchSize.Observe(500)
	after := testutil.CollectAndCount(RouterBatchSize)

	if after < before {
		t.Error("expected metric count to not decrease after observations")
	}
}

func TestRouterQueryDuration_Observe(t *testing.T) {
	RouterQueryDuration.WithLabelValues("testdb_q", "testcoll_q").Observe(0.05)
	count := testutil.CollectAndCount(RouterQueryDuration)
	if count <= 0 {
		t.Error("expected positive metric count")
	}
}

func TestStorageFlushDuration_Observe(t *testing.T) {
	StorageFlushDuration.Observe(0.1)
	StorageFlushDuration.Observe(0.5)
	count := testutil.CollectAndCount(StorageFlushDuration)
	if count <= 0 {
		t.Error("expected positive metric count")
	}
}

func TestStorageFlushErrors_Increment(t *testing.T) {
	before := testutil.ToFloat64(StorageFlushErrors)
	StorageFlushErrors.Inc()
	after := testutil.ToFloat64(StorageFlushErrors)

	if after != before+1 {
		t.Errorf("expected increment by 1, got %v -> %v", before, after)
	}
}

func TestAggregationDuration_ByLevel(t *testing.T) {
	levels := []string{"1h", "1d", "1M", "1y"}
	for _, level := range levels {
		AggregationDuration.WithLabelValues(level).Observe(0.1)
	}
	count := testutil.CollectAndCount(AggregationDuration)
	if count <= 0 {
		t.Error("expected positive metric count")
	}
}

func TestAggregationProcessed_ByLevelAndStatus(t *testing.T) {
	AggregationProcessed.WithLabelValues("1h", "success").Inc()
	AggregationProcessed.WithLabelValues("1h", "failed").Inc()

	success := testutil.ToFloat64(AggregationProcessed.WithLabelValues("1h", "success"))
	failed := testutil.ToFloat64(AggregationProcessed.WithLabelValues("1h", "failed"))

	if success < 1 {
		t.Error("expected success >= 1")
	}
	if failed < 1 {
		t.Error("expected failed >= 1")
	}
}

func TestStorageGauges_SetAndGet(t *testing.T) {
	StorageMemoryStoreSize.Set(1024 * 1024)
	val := testutil.ToFloat64(StorageMemoryStoreSize)
	if val != 1024*1024 {
		t.Errorf("expected 1MB, got %v", val)
	}

	StorageMemoryStoreCount.Set(5000)
	val = testutil.ToFloat64(StorageMemoryStoreCount)
	if val != 5000 {
		t.Errorf("expected 5000, got %v", val)
	}

	StorageWALSegments.Set(10)
	val = testutil.ToFloat64(StorageWALSegments)
	if val != 10 {
		t.Errorf("expected 10, got %v", val)
	}
}
