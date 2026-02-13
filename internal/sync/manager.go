package sync

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/storage"
	"github.com/soltixdb/soltix/internal/wal"
)

// Manager handles data synchronization between storage nodes
type Manager struct {
	config          Config
	nodeID          string
	logger          *logging.Logger
	metadataManager MetadataManager
	localStorage    LocalStorage
	remoteClient    RemoteSyncClient
	eventHandlers   []SyncEventHandler

	// State
	mu             sync.RWMutex
	syncInProgress map[string]*SyncProgress // shardID -> progress
	stopCh         chan struct{}
	wg             sync.WaitGroup
}

// NewManager creates a new sync manager
func NewManager(
	config Config,
	nodeID string,
	logger *logging.Logger,
	metadataManager MetadataManager,
	localStorage LocalStorage,
	remoteClient RemoteSyncClient,
) *Manager {
	if err := config.Validate(); err != nil {
		logger.Warn("Invalid sync config, using defaults", "error", err)
		config = DefaultConfig()
	}

	return &Manager{
		config:          config,
		nodeID:          nodeID,
		logger:          logger,
		metadataManager: metadataManager,
		localStorage:    localStorage,
		remoteClient:    remoteClient,
		eventHandlers:   make([]SyncEventHandler, 0),
		syncInProgress:  make(map[string]*SyncProgress),
		stopCh:          make(chan struct{}),
	}
}

// OnSyncEvent registers an event handler for sync events
func (m *Manager) OnSyncEvent(handler SyncEventHandler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.eventHandlers = append(m.eventHandlers, handler)
}

// emitEvent emits a sync event to all registered handlers
func (m *Manager) emitEvent(event SyncEvent) {
	m.mu.RLock()
	handlers := make([]SyncEventHandler, len(m.eventHandlers))
	copy(handlers, m.eventHandlers)
	m.mu.RUnlock()

	for _, handler := range handlers {
		handler(event)
	}
}

// SyncOnStartup performs sync from replicas when the node starts up
// This should be called before the node starts accepting new writes
func (m *Manager) SyncOnStartup(ctx context.Context) error {
	if !m.config.Enabled || !m.config.StartupSync {
		m.logger.Info("Startup sync is disabled")
		return nil
	}

	m.logger.Info("Starting startup sync from replicas", "node_id", m.nodeID)

	// Create context with timeout
	syncCtx, cancel := context.WithTimeout(ctx, m.config.StartupTimeout)
	defer cancel()

	// Get all shards this node is responsible for
	shards, err := m.metadataManager.GetMyShards(syncCtx, m.nodeID)
	if err != nil {
		return fmt.Errorf("failed to get my shards: %w", err)
	}

	if len(shards) == 0 {
		m.logger.Info("No shards to sync")
		return nil
	}

	m.logger.Info("Found shards to sync", "count", len(shards))

	// Sync shards concurrently with limit
	semaphore := make(chan struct{}, m.config.MaxConcurrentSyncs)
	var wg sync.WaitGroup
	errors := make(chan error, len(shards))

	for _, shard := range shards {
		wg.Add(1)
		go func(s ShardInfo) {
			defer wg.Done()

			// Acquire semaphore
			select {
			case semaphore <- struct{}{}:
				defer func() { <-semaphore }()
			case <-syncCtx.Done():
				errors <- syncCtx.Err()
				return
			}

			// Sync this shard
			if err := m.syncShard(syncCtx, s); err != nil {
				m.logger.Error("Failed to sync shard",
					"shard_id", s.ShardID,
					"database", s.Database,
					"collection", s.Collection,
					"error", err)
				errors <- fmt.Errorf("shard %s: %w", s.ShardID, err)
			}
		}(shard)
	}

	// Wait for all syncs to complete
	wg.Wait()
	close(errors)

	// Collect errors
	var syncErrors []error
	for err := range errors {
		if err != nil {
			syncErrors = append(syncErrors, err)
		}
	}

	if len(syncErrors) > 0 {
		m.logger.Warn("Startup sync completed with errors",
			"total_shards", len(shards),
			"failed_shards", len(syncErrors))
		// Don't return error - allow node to start even with partial sync
		return nil
	}

	m.logger.Info("Startup sync completed successfully",
		"synced_shards", len(shards))
	return nil
}

// SyncGroupOnStartup performs group-scoped sync when the node starts up.
// In the 4-tier architecture, sync only happens between replica nodes within the same group.
func (m *Manager) SyncGroupOnStartup(ctx context.Context) error {
	if !m.config.Enabled || !m.config.StartupSync {
		m.logger.Info("Group startup sync is disabled")
		return nil
	}

	m.logger.Info("Starting group-scoped sync from replicas", "node_id", m.nodeID)

	syncCtx, cancel := context.WithTimeout(ctx, m.config.StartupTimeout)
	defer cancel()

	// Get all groups this node belongs to
	groups, err := m.metadataManager.GetMyGroups(syncCtx, m.nodeID)
	if err != nil {
		return fmt.Errorf("failed to get my groups: %w", err)
	}

	if len(groups) == 0 {
		m.logger.Info("No groups to sync")
		return nil
	}

	m.logger.Info("Found groups to sync", "count", len(groups))

	// Sync groups concurrently with limit
	semaphore := make(chan struct{}, m.config.MaxConcurrentSyncs)
	var wg sync.WaitGroup
	errors := make(chan error, len(groups))

	for _, group := range groups {
		wg.Add(1)
		go func(g GroupInfo) {
			defer wg.Done()

			select {
			case semaphore <- struct{}{}:
				defer func() { <-semaphore }()
			case <-syncCtx.Done():
				errors <- syncCtx.Err()
				return
			}

			if err := m.syncGroup(syncCtx, g); err != nil {
				m.logger.Error("Failed to sync group",
					"group_id", g.GroupID,
					"error", err)
				errors <- fmt.Errorf("group %d: %w", g.GroupID, err)
			}
		}(group)
	}

	wg.Wait()
	close(errors)

	var syncErrors []error
	for err := range errors {
		if err != nil {
			syncErrors = append(syncErrors, err)
		}
	}

	if len(syncErrors) > 0 {
		m.logger.Warn("Group startup sync completed with errors",
			"total_groups", len(groups),
			"failed_groups", len(syncErrors))
		return nil
	}

	m.logger.Info("Group startup sync completed successfully",
		"synced_groups", len(groups))
	return nil
}

// syncGroup syncs a single group from its replicas
func (m *Manager) syncGroup(ctx context.Context, group GroupInfo) error {
	groupKey := fmt.Sprintf("group_%d", group.GroupID)

	m.logger.Debug("Starting group sync",
		"group_id", group.GroupID)

	progress := &SyncProgress{
		GroupID:   group.GroupID,
		StartedAt: time.Now(),
	}
	m.setProgress(groupKey, progress)
	defer m.clearProgress(groupKey)

	m.emitEvent(SyncEvent{
		Type:      SyncEventStarted,
		GroupID:   group.GroupID,
		NodeID:    m.nodeID,
		Progress:  progress,
		Timestamp: time.Now(),
	})

	// Get replica nodes for this group (excluding ourselves)
	replicas, err := m.metadataManager.GetGroupReplicas(ctx, group.GroupID, m.nodeID)
	if err != nil {
		return fmt.Errorf("failed to get group replicas: %w", err)
	}

	if len(replicas) == 0 {
		m.logger.Warn("No active replicas found for group",
			"group_id", group.GroupID)
		return nil
	}

	// Try each replica until one succeeds
	var lastErr error
	for _, replica := range replicas {
		m.logger.Debug("Attempting group sync from replica",
			"group_id", group.GroupID,
			"replica", replica.ID,
			"address", replica.Address)

		err := m.syncGroupFromReplica(ctx, group, replica, progress)
		if err == nil {
			m.logger.Info("Group sync completed",
				"group_id", group.GroupID,
				"from_replica", replica.ID,
				"synced_points", progress.SyncedPoints)

			m.emitEvent(SyncEvent{
				Type:      SyncEventCompleted,
				GroupID:   group.GroupID,
				NodeID:    replica.ID,
				Progress:  progress,
				Timestamp: time.Now(),
			})
			return nil
		}

		lastErr = err
		m.logger.Warn("Failed to sync group from replica, trying next",
			"group_id", group.GroupID,
			"replica", replica.ID,
			"error", err)
	}

	m.emitEvent(SyncEvent{
		Type:      SyncEventFailed,
		GroupID:   group.GroupID,
		Error:     lastErr,
		Timestamp: time.Now(),
	})

	return fmt.Errorf("all replicas failed for group %d, last error: %w", group.GroupID, lastErr)
}

// syncGroupFromReplica syncs group data from a specific replica node
func (m *Manager) syncGroupFromReplica(
	ctx context.Context,
	group GroupInfo,
	replica NodeInfo,
	progress *SyncProgress,
) error {
	req := &GroupSyncRequest{
		GroupID:       group.GroupID,
		FromTimestamp: time.Time{}, // sync all (could optimize with local last timestamp)
		ToTimestamp:   time.Now(),
	}

	// Stream data from replica
	pointsCh, errCh := m.remoteClient.SyncGroup(ctx, replica.Address, req)

	batch := make([]*storage.DataPoint, 0, m.config.SyncBatchSize)
	var totalSynced int64

	for point := range pointsCh {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		batch = append(batch, point)

		if len(batch) >= m.config.SyncBatchSize {
			entries := dataPointsToWALEntries(batch)
			if err := m.localStorage.WriteToWAL(entries); err != nil {
				return fmt.Errorf("failed to write batch to WAL: %w", err)
			}
			totalSynced += int64(len(batch))
			batch = batch[:0]

			progress.SyncedPoints = totalSynced
			m.emitEvent(SyncEvent{
				Type:      SyncEventProgress,
				GroupID:   group.GroupID,
				NodeID:    replica.ID,
				Progress:  progress,
				Timestamp: time.Now(),
			})
		}
	}

	// Write remaining batch
	if len(batch) > 0 {
		entries := dataPointsToWALEntries(batch)
		if err := m.localStorage.WriteToWAL(entries); err != nil {
			return fmt.Errorf("failed to write final batch to WAL: %w", err)
		}
		totalSynced += int64(len(batch))
	}

	// Check for stream errors
	select {
	case syncErr := <-errCh:
		if syncErr != nil {
			return syncErr
		}
	default:
	}

	progress.SyncedPoints = totalSynced
	progress.PercentDone = 100.0
	return nil
}

// syncShard syncs a single shard from replicas
func (m *Manager) syncShard(ctx context.Context, shard ShardInfo) error {
	m.logger.Debug("Starting shard sync",
		"shard_id", shard.ShardID,
		"database", shard.Database,
		"collection", shard.Collection)

	// Track progress
	progress := &SyncProgress{
		ShardID:   shard.ShardID,
		StartedAt: time.Now(),
	}
	m.setProgress(shard.ShardID, progress)
	defer m.clearProgress(shard.ShardID)

	// Emit started event
	m.emitEvent(SyncEvent{
		Type:      SyncEventStarted,
		ShardID:   shard.ShardID,
		NodeID:    m.nodeID,
		Progress:  progress,
		Timestamp: time.Now(),
	})

	// Get last timestamp we have locally
	lastLocalTS, err := m.localStorage.GetLastTimestamp(
		shard.Database,
		shard.Collection,
		shard.TimeRangeStart,
		shard.TimeRangeEnd,
	)
	if err != nil {
		m.logger.Debug("No local data for shard, will sync all",
			"shard_id", shard.ShardID,
			"error", err)
		lastLocalTS = shard.TimeRangeStart
	}

	m.logger.Debug("Local timestamp for shard",
		"shard_id", shard.ShardID,
		"last_local_ts", lastLocalTS,
		"shard_end", shard.TimeRangeEnd)

	// If we're already up to date, skip
	if !lastLocalTS.Before(shard.TimeRangeEnd) {
		m.logger.Debug("Shard is already up to date",
			"shard_id", shard.ShardID)
		return nil
	}

	// Find an active replica to sync from
	replicas, err := m.metadataManager.GetActiveReplicas(ctx, shard.ShardID, m.nodeID)
	if err != nil {
		return fmt.Errorf("failed to get active replicas: %w", err)
	}

	if len(replicas) == 0 {
		m.logger.Warn("No active replicas found for shard",
			"shard_id", shard.ShardID)
		return nil // Not an error - might be the only replica
	}

	// Try each replica until one succeeds
	var lastErr error
	for _, replica := range replicas {
		m.logger.Debug("Attempting sync from replica",
			"shard_id", shard.ShardID,
			"replica", replica.ID,
			"address", replica.Address)

		err := m.syncFromReplica(ctx, shard, replica, lastLocalTS, progress)
		if err == nil {
			m.logger.Info("Shard sync completed",
				"shard_id", shard.ShardID,
				"from_replica", replica.ID,
				"synced_points", progress.SyncedPoints)

			// Emit completed event
			m.emitEvent(SyncEvent{
				Type:      SyncEventCompleted,
				ShardID:   shard.ShardID,
				NodeID:    replica.ID,
				Progress:  progress,
				Timestamp: time.Now(),
			})
			return nil
		}

		lastErr = err
		m.logger.Warn("Failed to sync from replica, trying next",
			"shard_id", shard.ShardID,
			"replica", replica.ID,
			"error", err)
	}

	// Emit failed event
	m.emitEvent(SyncEvent{
		Type:      SyncEventFailed,
		ShardID:   shard.ShardID,
		Error:     lastErr,
		Timestamp: time.Now(),
	})

	return fmt.Errorf("all replicas failed, last error: %w", lastErr)
}

// syncFromReplica syncs data from a specific replica
func (m *Manager) syncFromReplica(
	ctx context.Context,
	shard ShardInfo,
	replica NodeInfo,
	since time.Time,
	progress *SyncProgress,
) error {
	// Create sync request
	req := &SyncRequest{
		Database:      shard.Database,
		Collection:    shard.Collection,
		ShardID:       shard.ShardID,
		FromTimestamp: since,
		ToTimestamp:   shard.TimeRangeEnd,
	}

	// Start streaming data from replica
	pointsCh, errCh := m.remoteClient.SyncShard(ctx, replica.Address, req)

	// Collect points in batches
	batch := make([]*storage.DataPoint, 0, m.config.SyncBatchSize)
	var totalSynced int64
	var syncErr error

	// Read all points first
	for point := range pointsCh {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		batch = append(batch, point)

		// Write batch when full
		if len(batch) >= m.config.SyncBatchSize {
			// Convert DataPoints to WAL entries
			entries := dataPointsToWALEntries(batch)
			if err := m.localStorage.WriteToWAL(entries); err != nil {
				return fmt.Errorf("failed to write batch to WAL: %w", err)
			}
			totalSynced += int64(len(batch))
			batch = batch[:0]

			// Update progress
			progress.SyncedPoints = totalSynced
			m.emitEvent(SyncEvent{
				Type:      SyncEventProgress,
				ShardID:   shard.ShardID,
				NodeID:    replica.ID,
				Progress:  progress,
				Timestamp: time.Now(),
			})
		}
	}

	// Write remaining batch
	if len(batch) > 0 {
		// Convert DataPoints to WAL entries
		entries := dataPointsToWALEntries(batch)
		if err := m.localStorage.WriteToWAL(entries); err != nil {
			return fmt.Errorf("failed to write final batch to WAL: %w", err)
		}
		totalSynced += int64(len(batch))
	}

	// Check for errors from the stream
	select {
	case syncErr = <-errCh:
	default:
	}

	if syncErr != nil {
		return syncErr
	}

	progress.SyncedPoints = totalSynced
	progress.PercentDone = 100.0
	return nil
}

// setProgress sets the sync progress for a shard
func (m *Manager) setProgress(shardID string, progress *SyncProgress) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.syncInProgress[shardID] = progress
}

// clearProgress clears the sync progress for a shard
func (m *Manager) clearProgress(shardID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.syncInProgress, shardID)
}

// GetProgress returns the sync progress for a shard
func (m *Manager) GetProgress(shardID string) *SyncProgress {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if p, ok := m.syncInProgress[shardID]; ok {
		return p
	}
	return nil
}

// GetAllProgress returns sync progress for all shards
func (m *Manager) GetAllProgress() map[string]*SyncProgress {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make(map[string]*SyncProgress, len(m.syncInProgress))
	for k, v := range m.syncInProgress {
		result[k] = v
	}
	return result
}

// SyncShard manually triggers a sync for a specific shard
func (m *Manager) SyncShard(ctx context.Context, shard ShardInfo) error {
	return m.syncShard(ctx, shard)
}

// Stop stops the sync manager
func (m *Manager) Stop() {
	close(m.stopCh)
	m.wg.Wait()
	if m.remoteClient != nil {
		if err := m.remoteClient.Close(); err != nil {
			m.logger.Warn("Failed to close remote client", "error", err)
		}
	}
	m.logger.Info("Sync manager stopped")
}

// dataPointsToWALEntries converts DataPoints to WAL entries for writing through WAL
func dataPointsToWALEntries(points []*storage.DataPoint) []*wal.Entry {
	entries := make([]*wal.Entry, 0, len(points))
	for _, p := range points {
		entry := &wal.Entry{
			Type:       wal.EntryTypeWrite,
			Database:   p.Database,
			Collection: p.Collection,
			ShardID:    p.ShardID,
			Time:       p.Time.Format(time.RFC3339Nano),
			ID:         p.ID,
			Fields:     p.Fields,
			Timestamp:  p.Time.UnixNano(),
		}
		entries = append(entries, entry)
	}
	return entries
}
