package sync

import (
	"context"
	"sync"
	"time"

	"github.com/soltixdb/soltix/internal/logging"
)

// AntiEntropy handles background consistency checking and repair
type AntiEntropy struct {
	config          AntiEntropyConfig
	nodeID          string
	logger          *logging.Logger
	metadataManager MetadataManager
	localStorage    LocalStorage
	remoteClient    RemoteSyncClient
	syncManager     *Manager

	mu     sync.Mutex
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewAntiEntropy creates a new anti-entropy service
func NewAntiEntropy(
	config AntiEntropyConfig,
	nodeID string,
	logger *logging.Logger,
	metadataManager MetadataManager,
	localStorage LocalStorage,
	remoteClient RemoteSyncClient,
	syncManager *Manager,
) *AntiEntropy {
	return &AntiEntropy{
		config:          config,
		nodeID:          nodeID,
		logger:          logger,
		metadataManager: metadataManager,
		localStorage:    localStorage,
		remoteClient:    remoteClient,
		syncManager:     syncManager,
		stopCh:          make(chan struct{}),
	}
}

// Start starts the anti-entropy background process
func (ae *AntiEntropy) Start(ctx context.Context) {
	if !ae.config.Enabled {
		ae.logger.Info("Anti-entropy is disabled")
		return
	}

	ae.logger.Info("Starting anti-entropy service",
		"interval", ae.config.Interval,
		"batch_size", ae.config.BatchSize)

	ae.wg.Add(1)
	go ae.run(ctx)
}

// Stop stops the anti-entropy service
func (ae *AntiEntropy) Stop() {
	close(ae.stopCh)
	ae.wg.Wait()
	ae.logger.Info("Anti-entropy service stopped")
}

// run is the main anti-entropy loop
func (ae *AntiEntropy) run(ctx context.Context) {
	defer ae.wg.Done()

	ticker := time.NewTicker(ae.config.Interval)
	defer ticker.Stop()

	// Run initial check after a short delay
	initialDelay := time.NewTimer(30 * time.Second)
	defer initialDelay.Stop()

	select {
	case <-initialDelay.C:
		ae.runCheck(ctx)
	case <-ae.stopCh:
		return
	case <-ctx.Done():
		return
	}

	for {
		select {
		case <-ticker.C:
			ae.runCheck(ctx)
		case <-ae.stopCh:
			return
		case <-ctx.Done():
			return
		}
	}
}

// runCheck performs a single anti-entropy check
func (ae *AntiEntropy) runCheck(ctx context.Context) {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	ae.logger.Debug("Starting anti-entropy check")

	// Get all shards this node is responsible for
	shards, err := ae.metadataManager.GetMyShards(ctx, ae.nodeID)
	if err != nil {
		ae.logger.Error("Failed to get shards for anti-entropy", "error", err)
		return
	}

	if len(shards) == 0 {
		ae.logger.Debug("No shards to check for anti-entropy")
		return
	}

	repairCount := 0
	checkCount := 0

	for _, shard := range shards {
		select {
		case <-ctx.Done():
			return
		case <-ae.stopCh:
			return
		default:
		}

		needsRepair, err := ae.checkShard(ctx, shard)
		if err != nil {
			ae.logger.Warn("Failed to check shard",
				"shard_id", shard.ShardID,
				"error", err)
			continue
		}

		checkCount++

		if needsRepair {
			ae.logger.Info("Shard needs repair, triggering sync",
				"shard_id", shard.ShardID,
				"database", shard.Database,
				"collection", shard.Collection)

			if err := ae.repairShard(ctx, shard); err != nil {
				ae.logger.Error("Failed to repair shard",
					"shard_id", shard.ShardID,
					"error", err)
				continue
			}
			repairCount++
		}
	}

	ae.logger.Info("Anti-entropy check completed",
		"checked_shards", checkCount,
		"repaired_shards", repairCount)
}

// checkShard checks if a shard needs repair by comparing checksums with replicas
func (ae *AntiEntropy) checkShard(ctx context.Context, shard ShardInfo) (bool, error) {
	// Calculate time window for checksum
	now := time.Now()
	toTime := now
	fromTime := now.Add(-ae.config.ChecksumWindow)

	// Adjust to shard boundaries
	if fromTime.Before(shard.TimeRangeStart) {
		fromTime = shard.TimeRangeStart
	}
	if toTime.After(shard.TimeRangeEnd) {
		toTime = shard.TimeRangeEnd
	}

	// Get local checksum
	localChecksum, localCount, err := ae.localStorage.GetChecksum(
		shard.Database,
		shard.Collection,
		fromTime,
		toTime,
	)
	if err != nil {
		return false, err
	}

	// Get replicas to compare with
	replicas, err := ae.metadataManager.GetActiveReplicas(ctx, shard.ShardID, ae.nodeID)
	if err != nil {
		return false, err
	}

	if len(replicas) == 0 {
		// No replicas to compare with
		return false, nil
	}

	// Compare with each replica
	for _, replica := range replicas {
		remoteChecksum, err := ae.remoteClient.GetChecksum(ctx, replica.Address, &ChecksumRequest{
			Database:      shard.Database,
			Collection:    shard.Collection,
			ShardID:       shard.ShardID,
			FromTimestamp: fromTime,
			ToTimestamp:   toTime,
		})
		if err != nil {
			ae.logger.Warn("Failed to get checksum from replica",
				"replica", replica.ID,
				"error", err)
			continue
		}

		// Compare checksums
		if localChecksum != remoteChecksum.Checksum || localCount != remoteChecksum.PointCount {
			ae.logger.Warn("Checksum mismatch detected",
				"shard_id", shard.ShardID,
				"local_checksum", localChecksum,
				"local_count", localCount,
				"remote_checksum", remoteChecksum.Checksum,
				"remote_count", remoteChecksum.PointCount,
				"replica", replica.ID)
			return true, nil
		}
	}

	return false, nil
}

// repairShard repairs a shard by syncing from a replica
func (ae *AntiEntropy) repairShard(ctx context.Context, shard ShardInfo) error {
	return ae.syncManager.SyncShard(ctx, shard)
}

// ForceCheck triggers an immediate anti-entropy check
func (ae *AntiEntropy) ForceCheck(ctx context.Context) {
	go ae.runCheck(ctx)
}

// Stats returns anti-entropy statistics
type AntiEntropyStats struct {
	Enabled       bool
	Interval      time.Duration
	LastCheckTime time.Time
	CheckCount    int64
	RepairCount   int64
}

// GetStats returns current anti-entropy statistics
func (ae *AntiEntropy) GetStats() AntiEntropyStats {
	return AntiEntropyStats{
		Enabled:  ae.config.Enabled,
		Interval: ae.config.Interval,
	}
}
