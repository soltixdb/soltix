package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestAllMetrics_RegisteredInDefaultRegistry(t *testing.T) {
	// Gather all metric families from the default registry
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	// Build a set of registered metric names
	registered := make(map[string]bool)
	for _, f := range families {
		registered[f.GetName()] = true
	}

	// Verify all soltix metrics are registered
	// Non-Vec metrics should appear immediately in the registry.
	// Vec metrics (CounterVec, HistogramVec) only appear after first use with labels.
	expectedMetrics := []string{
		"soltix_router_batch_size",
		"soltix_router_queue_publish_errors_total",
		"soltix_storage_wal_writes_total",
		"soltix_storage_wal_segment_count",
		"soltix_storage_memory_store_size_bytes",
		"soltix_storage_memory_store_count",
		"soltix_storage_flush_duration_seconds",
		"soltix_storage_flush_points_total",
		"soltix_storage_flush_errors_total",
		"soltix_storage_write_worker_queue_size",
		"soltix_sync_points_synced_total",
	}

	for _, name := range expectedMetrics {
		if !registered[name] {
			t.Errorf("Expected metric %q to be registered in default registry", name)
		}
	}
}

func TestRouterWriteRequests_IncrementsByStatus(t *testing.T) {
	before := testutil.ToFloat64(RouterWriteRequests.WithLabelValues("testdb", "testcoll", "accepted"))
	RouterWriteRequests.WithLabelValues("testdb", "testcoll", "accepted").Inc()
	after := testutil.ToFloat64(RouterWriteRequests.WithLabelValues("testdb", "testcoll", "accepted"))

	if after != before+1 {
		t.Errorf("expected increment by 1, got %v -> %v", before, after)
	}
}

func TestRouterWriteRequests_FailedStatus(t *testing.T) {
	before := testutil.ToFloat64(RouterWriteRequests.WithLabelValues("db_f", "col_f", "failed"))
	RouterWriteRequests.WithLabelValues("db_f", "col_f", "failed").Inc()
	after := testutil.ToFloat64(RouterWriteRequests.WithLabelValues("db_f", "col_f", "failed"))

	if after != before+1 {
		t.Errorf("expected increment by 1, got %v -> %v", before, after)
	}
}

func TestRouterWriteRequests_PartialStatus(t *testing.T) {
	before := testutil.ToFloat64(RouterWriteRequests.WithLabelValues("db_p", "col_p", "partial"))
	RouterWriteRequests.WithLabelValues("db_p", "col_p", "partial").Inc()
	after := testutil.ToFloat64(RouterWriteRequests.WithLabelValues("db_p", "col_p", "partial"))

	if after != before+1 {
		t.Errorf("expected increment by 1, got %v -> %v", before, after)
	}
}

func TestRouterBatchSize_Observe(t *testing.T) {
	RouterBatchSize.Observe(100)
	RouterBatchSize.Observe(500)
	count := testutil.CollectAndCount(RouterBatchSize)
	if count == 0 {
		t.Error("expected positive metric count after observations")
	}
}

func TestRouterQueryDuration_Observe(t *testing.T) {
	RouterQueryDuration.WithLabelValues("db_q", "col_q").Observe(0.05)
	count := testutil.CollectAndCount(RouterQueryDuration)
	if count == 0 {
		t.Error("expected positive metric count after observation")
	}
}

func TestRouterQueuePublishErrors_Increment(t *testing.T) {
	before := testutil.ToFloat64(RouterQueuePublishErrors)
	RouterQueuePublishErrors.Inc()
	after := testutil.ToFloat64(RouterQueuePublishErrors)

	if after != before+1 {
		t.Errorf("expected increment by 1, got %v -> %v", before, after)
	}
}

func TestStorageFlushDuration_Observe(t *testing.T) {
	StorageFlushDuration.Observe(0.1)
	count := testutil.CollectAndCount(StorageFlushDuration)
	if count == 0 {
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

func TestStorageWALWrites_Increment(t *testing.T) {
	before := testutil.ToFloat64(StorageWALWrites)
	StorageWALWrites.Inc()
	after := testutil.ToFloat64(StorageWALWrites)

	if after != before+1 {
		t.Errorf("expected increment by 1, got %v -> %v", before, after)
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

func TestAggregationDuration_ByLevel(t *testing.T) {
	levels := []string{"1h", "1d", "1M", "1y"}
	for _, level := range levels {
		AggregationDuration.WithLabelValues(level).Observe(0.1)
	}
	count := testutil.CollectAndCount(AggregationDuration)
	if count == 0 {
		t.Error("expected positive metric count")
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

	StorageWriteWorkerQueueSize.Set(42)
	val = testutil.ToFloat64(StorageWriteWorkerQueueSize)
	if val != 42 {
		t.Errorf("expected 42, got %v", val)
	}
}

func TestSyncMetrics_Increment(t *testing.T) {
	before := testutil.ToFloat64(SyncPointsSynced)
	SyncPointsSynced.Add(100)
	after := testutil.ToFloat64(SyncPointsSynced)

	if after != before+100 {
		t.Errorf("expected +100, got %v -> %v", before, after)
	}

	SyncOperations.WithLabelValues("group", "success").Inc()
	val := testutil.ToFloat64(SyncOperations.WithLabelValues("group", "success"))
	if val < 1 {
		t.Error("expected >= 1")
	}
}
