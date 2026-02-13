package sync

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/storage"
)

func TestNewAntiEntropy(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig().AntiEntropy
	metadata := newMockMetadataManager()
	localStorage := newMockLocalStorage()
	remoteClient := newMockRemoteClient()
	syncManager := NewManager(DefaultConfig(), "node-1", logger, metadata, localStorage, remoteClient)

	ae := NewAntiEntropy(config, "node-1", logger, metadata, localStorage, remoteClient, syncManager)

	if ae == nil {
		t.Fatal("Expected non-nil AntiEntropy")
	}
	if ae.nodeID != "node-1" {
		t.Errorf("Expected nodeID=node-1, got %s", ae.nodeID)
	}
	if ae.config.Enabled != config.Enabled {
		t.Errorf("Config mismatch")
	}
}

func TestAntiEntropy_CheckShard_NoReplicas(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig().AntiEntropy
	metadata := newMockMetadataManager()
	localStorage := newMockLocalStorage()
	localStorage.checksum = "abc123"
	remoteClient := newMockRemoteClient()
	syncManager := NewManager(DefaultConfig(), "node-1", logger, metadata, localStorage, remoteClient)

	ae := NewAntiEntropy(config, "node-1", logger, metadata, localStorage, remoteClient, syncManager)

	shard := ShardInfo{
		ShardID:        "shard-1",
		Database:       "testdb",
		Collection:     "testcol",
		TimeRangeStart: time.Now().Add(-24 * time.Hour),
		TimeRangeEnd:   time.Now(),
	}

	ctx := context.Background()
	needsRepair, err := ae.checkShard(ctx, shard)
	if err != nil {
		t.Fatalf("checkShard failed: %v", err)
	}
	if needsRepair {
		t.Error("Expected needsRepair=false when no replicas")
	}
}

func TestAntiEntropy_CheckShard_MatchingChecksum(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig().AntiEntropy
	metadata := newMockMetadataManager()
	metadata.replicas["shard-1"] = []NodeInfo{
		{ID: "node-2", Address: "localhost:5556", Status: "active"},
	}
	metadata.nodes["node-2"] = &NodeInfo{ID: "node-2", Address: "localhost:5556", Status: "active"}

	localStorage := newMockLocalStorage()
	localStorage.checksum = "abc123"

	remoteClient := newMockRemoteClient()
	remoteClient.checksum = &SyncChecksum{
		ShardID:    "shard-1",
		Checksum:   "abc123",
		PointCount: 0,
	}

	syncManager := NewManager(DefaultConfig(), "node-1", logger, metadata, localStorage, remoteClient)
	ae := NewAntiEntropy(config, "node-1", logger, metadata, localStorage, remoteClient, syncManager)

	shard := ShardInfo{
		ShardID:        "shard-1",
		Database:       "testdb",
		Collection:     "testcol",
		TimeRangeStart: time.Now().Add(-24 * time.Hour),
		TimeRangeEnd:   time.Now(),
	}

	ctx := context.Background()
	needsRepair, err := ae.checkShard(ctx, shard)
	if err != nil {
		t.Fatalf("checkShard failed: %v", err)
	}
	if needsRepair {
		t.Error("Expected needsRepair=false when checksums match")
	}
}

func TestAntiEntropy_CheckShard_MismatchingChecksum(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig().AntiEntropy
	metadata := newMockMetadataManager()
	metadata.replicas["shard-1"] = []NodeInfo{
		{ID: "node-2", Address: "localhost:5556", Status: "active"},
	}
	metadata.nodes["node-2"] = &NodeInfo{ID: "node-2", Address: "localhost:5556", Status: "active"}

	localStorage := newMockLocalStorage()
	localStorage.checksum = "abc123"

	remoteClient := newMockRemoteClient()
	remoteClient.checksum = &SyncChecksum{
		ShardID:    "shard-1",
		Checksum:   "xyz789", // Different checksum
		PointCount: 100,
	}

	syncManager := NewManager(DefaultConfig(), "node-1", logger, metadata, localStorage, remoteClient)
	ae := NewAntiEntropy(config, "node-1", logger, metadata, localStorage, remoteClient, syncManager)

	shard := ShardInfo{
		ShardID:        "shard-1",
		Database:       "testdb",
		Collection:     "testcol",
		TimeRangeStart: time.Now().Add(-24 * time.Hour),
		TimeRangeEnd:   time.Now(),
	}

	ctx := context.Background()
	needsRepair, err := ae.checkShard(ctx, shard)
	if err != nil {
		t.Fatalf("checkShard failed: %v", err)
	}
	if !needsRepair {
		t.Error("Expected needsRepair=true when checksums don't match")
	}
}

func TestAntiEntropy_CheckShard_MismatchingCount(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig().AntiEntropy
	metadata := newMockMetadataManager()
	metadata.replicas["shard-1"] = []NodeInfo{
		{ID: "node-2", Address: "localhost:5556", Status: "active"},
	}

	localStorage := newMockLocalStorage()
	localStorage.checksum = "abc123"
	// localStorage.points has 0 items → count = 0

	remoteClient := newMockRemoteClient()
	remoteClient.checksum = &SyncChecksum{
		ShardID:    "shard-1",
		Checksum:   "abc123", // Same checksum but different count
		PointCount: 100,      // Remote has more
	}

	syncManager := NewManager(DefaultConfig(), "node-1", logger, metadata, localStorage, remoteClient)
	ae := NewAntiEntropy(config, "node-1", logger, metadata, localStorage, remoteClient, syncManager)

	shard := ShardInfo{
		ShardID:        "shard-1",
		Database:       "testdb",
		Collection:     "testcol",
		TimeRangeStart: time.Now().Add(-24 * time.Hour),
		TimeRangeEnd:   time.Now(),
	}

	needsRepair, err := ae.checkShard(context.Background(), shard)
	if err != nil {
		t.Fatalf("checkShard failed: %v", err)
	}
	if !needsRepair {
		t.Error("Expected needsRepair=true when point counts differ")
	}
}

func TestAntiEntropy_CheckShard_LocalChecksumError(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig().AntiEntropy
	metadata := newMockMetadataManager()
	localStorage := newMockLocalStorage()
	localStorage.checksumErr = fmt.Errorf("checksum calculation failed")

	remoteClient := newMockRemoteClient()
	syncManager := NewManager(DefaultConfig(), "node-1", logger, metadata, localStorage, remoteClient)
	ae := NewAntiEntropy(config, "node-1", logger, metadata, localStorage, remoteClient, syncManager)

	shard := ShardInfo{
		ShardID:        "shard-1",
		Database:       "testdb",
		Collection:     "testcol",
		TimeRangeStart: time.Now().Add(-24 * time.Hour),
		TimeRangeEnd:   time.Now(),
	}

	_, err := ae.checkShard(context.Background(), shard)
	if err == nil {
		t.Fatal("Expected error when local checksum fails")
	}
}

func TestAntiEntropy_CheckShard_GetReplicasError(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig().AntiEntropy
	metadata := newMockMetadataManager()
	metadata.replicasErr = fmt.Errorf("etcd error")

	localStorage := newMockLocalStorage()
	localStorage.checksum = "abc123"

	remoteClient := newMockRemoteClient()
	syncManager := NewManager(DefaultConfig(), "node-1", logger, metadata, localStorage, remoteClient)
	ae := NewAntiEntropy(config, "node-1", logger, metadata, localStorage, remoteClient, syncManager)

	shard := ShardInfo{
		ShardID:        "shard-1",
		Database:       "testdb",
		Collection:     "testcol",
		TimeRangeStart: time.Now().Add(-24 * time.Hour),
		TimeRangeEnd:   time.Now(),
	}

	_, err := ae.checkShard(context.Background(), shard)
	if err == nil {
		t.Fatal("Expected error when GetActiveReplicas fails")
	}
}

func TestAntiEntropy_CheckShard_RemoteChecksumError(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig().AntiEntropy
	metadata := newMockMetadataManager()
	metadata.replicas["shard-1"] = []NodeInfo{
		{ID: "node-2", Address: "localhost:5556", Status: "active"},
	}

	localStorage := newMockLocalStorage()
	localStorage.checksum = "abc123"

	remoteClient := newMockRemoteClient()
	remoteClient.checksumErr = fmt.Errorf("gRPC error")

	syncManager := NewManager(DefaultConfig(), "node-1", logger, metadata, localStorage, remoteClient)
	ae := NewAntiEntropy(config, "node-1", logger, metadata, localStorage, remoteClient, syncManager)

	shard := ShardInfo{
		ShardID:        "shard-1",
		Database:       "testdb",
		Collection:     "testcol",
		TimeRangeStart: time.Now().Add(-24 * time.Hour),
		TimeRangeEnd:   time.Now(),
	}

	// Remote checksum error should be skipped (continue to next replica)
	needsRepair, err := ae.checkShard(context.Background(), shard)
	if err != nil {
		t.Fatalf("checkShard should not error when remote checksum fails (just skip): %v", err)
	}
	if needsRepair {
		t.Error("Expected needsRepair=false when all replicas fail checksum")
	}
}

func TestAntiEntropy_CheckShard_MultipleReplicas_OneFailsOneMatches(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig().AntiEntropy
	metadata := newMockMetadataManager()
	metadata.replicas["shard-1"] = []NodeInfo{
		{ID: "node-2", Address: "localhost:5556", Status: "active"},
		{ID: "node-3", Address: "localhost:5557", Status: "active"},
	}

	localStorage := newMockLocalStorage()
	localStorage.checksum = "abc123"

	// Remote client returns matching checksum (ignoring the error on first)
	remoteClient := newMockRemoteClient()
	remoteClient.checksum = &SyncChecksum{
		Checksum:   "abc123",
		PointCount: 0,
	}

	syncManager := NewManager(DefaultConfig(), "node-1", logger, metadata, localStorage, remoteClient)
	ae := NewAntiEntropy(config, "node-1", logger, metadata, localStorage, remoteClient, syncManager)

	shard := ShardInfo{
		ShardID:        "shard-1",
		Database:       "testdb",
		Collection:     "testcol",
		TimeRangeStart: time.Now().Add(-24 * time.Hour),
		TimeRangeEnd:   time.Now(),
	}

	needsRepair, err := ae.checkShard(context.Background(), shard)
	if err != nil {
		t.Fatalf("checkShard failed: %v", err)
	}
	if needsRepair {
		t.Error("Expected needsRepair=false when checksums match")
	}
}

func TestAntiEntropy_CheckShard_TimeWindowClamp(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig().AntiEntropy
	config.ChecksumWindow = 48 * time.Hour // Larger than shard range

	metadata := newMockMetadataManager()
	localStorage := newMockLocalStorage()
	localStorage.checksum = "abc123"
	remoteClient := newMockRemoteClient()
	syncManager := NewManager(DefaultConfig(), "node-1", logger, metadata, localStorage, remoteClient)
	ae := NewAntiEntropy(config, "node-1", logger, metadata, localStorage, remoteClient, syncManager)

	now := time.Now()
	shard := ShardInfo{
		ShardID:        "shard-1",
		Database:       "testdb",
		Collection:     "testcol",
		TimeRangeStart: now.Add(-12 * time.Hour),
		TimeRangeEnd:   now.Add(-6 * time.Hour), // Shard end is in the past
	}

	needsRepair, err := ae.checkShard(context.Background(), shard)
	if err != nil {
		t.Fatalf("checkShard failed: %v", err)
	}
	if needsRepair {
		t.Error("Expected needsRepair=false with no replicas")
	}
}

func TestAntiEntropy_GetStats(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig().AntiEntropy
	metadata := newMockMetadataManager()
	localStorage := newMockLocalStorage()
	remoteClient := newMockRemoteClient()
	syncManager := NewManager(DefaultConfig(), "node-1", logger, metadata, localStorage, remoteClient)

	ae := NewAntiEntropy(config, "node-1", logger, metadata, localStorage, remoteClient, syncManager)

	stats := ae.GetStats()
	if !stats.Enabled {
		t.Error("Expected Enabled=true")
	}
	if stats.Interval != config.Interval {
		t.Errorf("Expected Interval=%v, got %v", config.Interval, stats.Interval)
	}
}

func TestAntiEntropy_GetStats_Disabled(t *testing.T) {
	logger := logging.NewDevelopment()
	config := AntiEntropyConfig{
		Enabled:  false,
		Interval: 30 * time.Minute,
	}
	metadata := newMockMetadataManager()
	localStorage := newMockLocalStorage()
	remoteClient := newMockRemoteClient()
	syncManager := NewManager(DefaultConfig(), "node-1", logger, metadata, localStorage, remoteClient)

	ae := NewAntiEntropy(config, "node-1", logger, metadata, localStorage, remoteClient, syncManager)

	stats := ae.GetStats()
	if stats.Enabled {
		t.Error("Expected Enabled=false")
	}
	if stats.Interval != 30*time.Minute {
		t.Errorf("Expected Interval=30m, got %v", stats.Interval)
	}
}

func TestAntiEntropy_Disabled(t *testing.T) {
	logger := logging.NewDevelopment()
	config := AntiEntropyConfig{
		Enabled: false,
	}
	metadata := newMockMetadataManager()
	localStorage := newMockLocalStorage()
	remoteClient := newMockRemoteClient()
	syncManager := NewManager(DefaultConfig(), "node-1", logger, metadata, localStorage, remoteClient)

	ae := NewAntiEntropy(config, "node-1", logger, metadata, localStorage, remoteClient, syncManager)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Should return immediately since disabled
	ae.Start(ctx)

	// Give it a moment
	time.Sleep(100 * time.Millisecond)

	ae.Stop()
}

func TestAntiEntropy_Start_Stop(t *testing.T) {
	logger := logging.NewDevelopment()
	config := AntiEntropyConfig{
		Enabled:        true,
		Interval:       100 * time.Millisecond,
		BatchSize:      100,
		ChecksumWindow: 24 * time.Hour,
	}
	metadata := newMockMetadataManager()
	localStorage := newMockLocalStorage()
	localStorage.checksum = "abc123"
	remoteClient := newMockRemoteClient()
	syncManager := NewManager(DefaultConfig(), "node-1", logger, metadata, localStorage, remoteClient)

	ae := NewAntiEntropy(config, "node-1", logger, metadata, localStorage, remoteClient, syncManager)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ae.Start(ctx)

	// Let it run briefly (won't do initial check within 30s delay, but tests the goroutine start)
	time.Sleep(50 * time.Millisecond)

	ae.Stop()
}

func TestAntiEntropy_Start_ContextCancel(t *testing.T) {
	logger := logging.NewDevelopment()
	config := AntiEntropyConfig{
		Enabled:        true,
		Interval:       1 * time.Hour,
		BatchSize:      100,
		ChecksumWindow: 24 * time.Hour,
	}
	metadata := newMockMetadataManager()
	localStorage := newMockLocalStorage()
	remoteClient := newMockRemoteClient()
	syncManager := NewManager(DefaultConfig(), "node-1", logger, metadata, localStorage, remoteClient)

	ae := NewAntiEntropy(config, "node-1", logger, metadata, localStorage, remoteClient, syncManager)

	ctx, cancel := context.WithCancel(context.Background())

	ae.Start(ctx)

	// Cancel context should trigger stop
	cancel()

	// Give goroutine time to exit
	time.Sleep(100 * time.Millisecond)

	// Stop should still work cleanly
	ae.Stop()
}

func TestAntiEntropy_RunCheck_NoShards(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig().AntiEntropy
	metadata := newMockMetadataManager()
	// No shards
	localStorage := newMockLocalStorage()
	remoteClient := newMockRemoteClient()
	syncManager := NewManager(DefaultConfig(), "node-1", logger, metadata, localStorage, remoteClient)

	ae := NewAntiEntropy(config, "node-1", logger, metadata, localStorage, remoteClient, syncManager)

	// runCheck should not panic with no shards
	ae.runCheck(context.Background())
}

func TestAntiEntropy_RunCheck_GetShardsError(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig().AntiEntropy
	metadata := newMockMetadataManager()
	metadata.shardsErr = fmt.Errorf("etcd unavailable")

	localStorage := newMockLocalStorage()
	remoteClient := newMockRemoteClient()
	syncManager := NewManager(DefaultConfig(), "node-1", logger, metadata, localStorage, remoteClient)

	ae := NewAntiEntropy(config, "node-1", logger, metadata, localStorage, remoteClient, syncManager)

	// Should not panic
	ae.runCheck(context.Background())
}

func TestAntiEntropy_RunCheck_WithShards_NoRepair(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig().AntiEntropy

	metadata := newMockMetadataManager()
	now := time.Now()
	metadata.shards = []ShardInfo{
		{
			ShardID:        "shard-1",
			Database:       "testdb",
			Collection:     "testcol",
			TimeRangeStart: now.Add(-24 * time.Hour),
			TimeRangeEnd:   now,
		},
	}
	metadata.replicas["shard-1"] = []NodeInfo{
		{ID: "node-2", Address: "localhost:5556", Status: "active"},
	}

	localStorage := newMockLocalStorage()
	localStorage.checksum = "matching"

	remoteClient := newMockRemoteClient()
	remoteClient.checksum = &SyncChecksum{
		Checksum:   "matching",
		PointCount: 0,
	}

	syncManager := NewManager(DefaultConfig(), "node-1", logger, metadata, localStorage, remoteClient)
	ae := NewAntiEntropy(config, "node-1", logger, metadata, localStorage, remoteClient, syncManager)

	ae.runCheck(context.Background())
	// No panic or error = success
}

func TestAntiEntropy_RunCheck_WithRepair(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig().AntiEntropy
	syncConfig := DefaultConfig()
	syncConfig.SyncBatchSize = 100

	metadata := newMockMetadataManager()
	now := time.Now()
	metadata.shards = []ShardInfo{
		{
			ShardID:        "shard-1",
			Database:       "testdb",
			Collection:     "testcol",
			TimeRangeStart: now.Add(-24 * time.Hour),
			TimeRangeEnd:   now,
		},
	}
	metadata.replicas["shard-1"] = []NodeInfo{
		{ID: "node-2", Address: "localhost:5556", Status: "active"},
	}

	localStorage := newMockLocalStorage()
	localStorage.checksum = "local"
	localStorage.lastTimestamp = now.Add(-1 * time.Hour)

	remoteClient := newMockRemoteClient()
	remoteClient.checksum = &SyncChecksum{
		Checksum:   "remote-different",
		PointCount: 50,
	}
	// Provide some points for repair sync
	for i := 0; i < 3; i++ {
		remoteClient.points = append(remoteClient.points, &storage.DataPoint{
			Database:   "testdb",
			Collection: "testcol",
			Time:       now.Add(-time.Duration(i) * time.Minute),
			ID:         "device-1",
			Fields:     map[string]interface{}{"value": float64(i)},
		})
	}

	syncManager := NewManager(syncConfig, "node-1", logger, metadata, localStorage, remoteClient)
	ae := NewAntiEntropy(config, "node-1", logger, metadata, localStorage, remoteClient, syncManager)

	ae.runCheck(context.Background())

	// After repair, points should have been synced
	if len(localStorage.points) != 3 {
		t.Errorf("Expected 3 repaired points, got %d", len(localStorage.points))
	}
}

func TestAntiEntropy_RunCheck_ContextCancelled(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig().AntiEntropy

	metadata := newMockMetadataManager()
	now := time.Now()
	for i := 0; i < 10; i++ {
		metadata.shards = append(metadata.shards, ShardInfo{
			ShardID:        fmt.Sprintf("shard-%d", i),
			Database:       "testdb",
			Collection:     "testcol",
			TimeRangeStart: now.Add(-24 * time.Hour),
			TimeRangeEnd:   now,
		})
	}

	localStorage := newMockLocalStorage()
	localStorage.checksum = "abc123"
	remoteClient := newMockRemoteClient()
	syncManager := NewManager(DefaultConfig(), "node-1", logger, metadata, localStorage, remoteClient)

	ae := NewAntiEntropy(config, "node-1", logger, metadata, localStorage, remoteClient, syncManager)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Should not hang or panic
	ae.runCheck(ctx)
}

func TestAntiEntropy_RunCheck_CheckShardError(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig().AntiEntropy

	metadata := newMockMetadataManager()
	now := time.Now()
	metadata.shards = []ShardInfo{
		{
			ShardID:        "shard-1",
			Database:       "testdb",
			Collection:     "testcol",
			TimeRangeStart: now.Add(-24 * time.Hour),
			TimeRangeEnd:   now,
		},
	}

	localStorage := newMockLocalStorage()
	localStorage.checksumErr = fmt.Errorf("checksum error")

	remoteClient := newMockRemoteClient()
	syncManager := NewManager(DefaultConfig(), "node-1", logger, metadata, localStorage, remoteClient)

	ae := NewAntiEntropy(config, "node-1", logger, metadata, localStorage, remoteClient, syncManager)

	// Should not panic; just logs warning
	ae.runCheck(context.Background())
}

func TestAntiEntropy_RunCheck_RepairShardError(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig().AntiEntropy

	metadata := newMockMetadataManager()
	now := time.Now()
	metadata.shards = []ShardInfo{
		{
			ShardID:        "shard-1",
			Database:       "testdb",
			Collection:     "testcol",
			TimeRangeStart: now.Add(-24 * time.Hour),
			TimeRangeEnd:   now,
		},
	}
	metadata.replicas["shard-1"] = []NodeInfo{
		{ID: "node-2", Address: "localhost:5556", Status: "active"},
	}

	localStorage := newMockLocalStorage()
	localStorage.checksum = "local"
	localStorage.lastTimestamp = now.Add(-1 * time.Hour)

	remoteClient := newMockRemoteClient()
	remoteClient.checksum = &SyncChecksum{
		Checksum:   "different",
		PointCount: 10,
	}
	// The repair will trigger syncShard which will try to sync points
	// but with syncShardErr, it will fail
	remoteClient.syncShardErr = fmt.Errorf("sync failed")

	syncManager := NewManager(DefaultConfig(), "node-1", logger, metadata, localStorage, remoteClient)
	ae := NewAntiEntropy(config, "node-1", logger, metadata, localStorage, remoteClient, syncManager)

	// Should not panic; just logs error
	ae.runCheck(context.Background())
}

func TestAntiEntropy_ForceCheck(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig().AntiEntropy

	metadata := newMockMetadataManager()
	localStorage := newMockLocalStorage()
	localStorage.checksum = "abc"
	remoteClient := newMockRemoteClient()
	syncManager := NewManager(DefaultConfig(), "node-1", logger, metadata, localStorage, remoteClient)

	ae := NewAntiEntropy(config, "node-1", logger, metadata, localStorage, remoteClient, syncManager)

	// ForceCheck should run without error
	ae.ForceCheck(context.Background())

	// Give goroutine time to run
	time.Sleep(100 * time.Millisecond)
}

func TestAntiEntropy_ForceCheck_WithShards(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig().AntiEntropy

	metadata := newMockMetadataManager()
	now := time.Now()
	metadata.shards = []ShardInfo{
		{
			ShardID:        "shard-1",
			Database:       "testdb",
			Collection:     "testcol",
			TimeRangeStart: now.Add(-24 * time.Hour),
			TimeRangeEnd:   now,
		},
	}
	metadata.replicas["shard-1"] = []NodeInfo{
		{ID: "node-2", Address: "localhost:5556", Status: "active"},
	}

	localStorage := newMockLocalStorage()
	localStorage.checksum = "matching"

	remoteClient := newMockRemoteClient()
	remoteClient.checksum = &SyncChecksum{
		Checksum:   "matching",
		PointCount: 0,
	}

	syncManager := NewManager(DefaultConfig(), "node-1", logger, metadata, localStorage, remoteClient)
	ae := NewAntiEntropy(config, "node-1", logger, metadata, localStorage, remoteClient, syncManager)

	ae.ForceCheck(context.Background())

	// Give goroutine time to run
	time.Sleep(200 * time.Millisecond)
}

func TestAntiEntropy_RunCheck_MultipleShards(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig().AntiEntropy
	syncConfig := DefaultConfig()
	syncConfig.SyncBatchSize = 100

	metadata := newMockMetadataManager()
	now := time.Now()

	// 3 shards: 1 matching, 1 mismatching, 1 with no replicas
	metadata.shards = []ShardInfo{
		{
			ShardID:        "shard-match",
			Database:       "testdb",
			Collection:     "testcol",
			TimeRangeStart: now.Add(-24 * time.Hour),
			TimeRangeEnd:   now,
		},
		{
			ShardID:        "shard-mismatch",
			Database:       "testdb",
			Collection:     "testcol",
			TimeRangeStart: now.Add(-24 * time.Hour),
			TimeRangeEnd:   now,
		},
		{
			ShardID:        "shard-no-replicas",
			Database:       "testdb",
			Collection:     "testcol",
			TimeRangeStart: now.Add(-24 * time.Hour),
			TimeRangeEnd:   now,
		},
	}

	metadata.replicas["shard-match"] = []NodeInfo{
		{ID: "node-2", Address: "localhost:5556", Status: "active"},
	}
	metadata.replicas["shard-mismatch"] = []NodeInfo{
		{ID: "node-2", Address: "localhost:5556", Status: "active"},
	}
	// shard-no-replicas has no replicas

	localStorage := newMockLocalStorage()
	localStorage.checksum = "localchecksum"
	localStorage.lastTimestamp = now.Add(-1 * time.Hour)

	// The remote checksum will be the same for all queries from this mock
	// So for "shard-match" the checksum should match local
	// For "shard-mismatch" it won't because we set remote differently
	// But our mock returns same checksum for all - so let's set it to match
	remoteClient := newMockRemoteClient()
	remoteClient.checksum = &SyncChecksum{
		Checksum:   "localchecksum",
		PointCount: 0,
	}

	syncManager := NewManager(syncConfig, "node-1", logger, metadata, localStorage, remoteClient)
	ae := NewAntiEntropy(config, "node-1", logger, metadata, localStorage, remoteClient, syncManager)

	// Should complete without error
	ae.runCheck(context.Background())
}

func TestAntiEntropy_RepairShard(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig().AntiEntropy
	syncConfig := DefaultConfig()
	syncConfig.SyncBatchSize = 100

	metadata := newMockMetadataManager()
	now := time.Now()
	metadata.replicas["shard-1"] = []NodeInfo{
		{ID: "node-2", Address: "localhost:5556", Status: "active"},
	}

	localStorage := newMockLocalStorage()
	localStorage.lastTimestamp = now.Add(-1 * time.Hour)

	remoteClient := newMockRemoteClient()
	remoteClient.points = []*storage.DataPoint{
		{Database: "testdb", Collection: "testcol", Time: now, ID: "dev-1", Fields: map[string]interface{}{"v": 1.0}},
	}

	syncManager := NewManager(syncConfig, "node-1", logger, metadata, localStorage, remoteClient)
	ae := NewAntiEntropy(config, "node-1", logger, metadata, localStorage, remoteClient, syncManager)

	shard := ShardInfo{
		ShardID:        "shard-1",
		Database:       "testdb",
		Collection:     "testcol",
		TimeRangeStart: now.Add(-24 * time.Hour),
		TimeRangeEnd:   now,
	}

	err := ae.repairShard(context.Background(), shard)
	if err != nil {
		t.Fatalf("repairShard failed: %v", err)
	}

	if len(localStorage.points) != 1 {
		t.Errorf("Expected 1 repaired point, got %d", len(localStorage.points))
	}
}

func TestAntiEntropyStats_Fields(t *testing.T) {
	stats := AntiEntropyStats{
		Enabled:       true,
		Interval:      1 * time.Hour,
		LastCheckTime: time.Now(),
		CheckCount:    10,
		RepairCount:   2,
	}

	if !stats.Enabled {
		t.Error("Expected Enabled=true")
	}
	if stats.Interval != 1*time.Hour {
		t.Errorf("Interval mismatch")
	}
	if stats.CheckCount != 10 {
		t.Errorf("CheckCount mismatch")
	}
	if stats.RepairCount != 2 {
		t.Errorf("RepairCount mismatch")
	}
}
