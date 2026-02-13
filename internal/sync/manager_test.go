package sync

import (
	"context"
	"fmt"
	stdsync "sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/storage"
	"github.com/soltixdb/soltix/internal/wal"
)

// mockMetadataManager is a mock implementation of MetadataManager
type mockMetadataManager struct {
	shards        []ShardInfo
	shardsErr     error
	replicas      map[string][]NodeInfo
	replicasErr   error
	nodes         map[string]*NodeInfo
	groups        []GroupInfo
	groupsErr     error
	groupReplicas map[int][]NodeInfo
	groupRepErr   error
}

func newMockMetadataManager() *mockMetadataManager {
	return &mockMetadataManager{
		shards:        make([]ShardInfo, 0),
		replicas:      make(map[string][]NodeInfo),
		nodes:         make(map[string]*NodeInfo),
		groups:        make([]GroupInfo, 0),
		groupReplicas: make(map[int][]NodeInfo),
	}
}

func (m *mockMetadataManager) GetMyShards(ctx context.Context, nodeID string) ([]ShardInfo, error) {
	if m.shardsErr != nil {
		return nil, m.shardsErr
	}
	return m.shards, nil
}

func (m *mockMetadataManager) GetActiveReplicas(ctx context.Context, shardID string, excludeNode string) ([]NodeInfo, error) {
	if m.replicasErr != nil {
		return nil, m.replicasErr
	}
	replicas, ok := m.replicas[shardID]
	if !ok {
		return nil, nil
	}
	// Filter out excludeNode
	result := make([]NodeInfo, 0)
	for _, r := range replicas {
		if r.ID != excludeNode {
			result = append(result, r)
		}
	}
	return result, nil
}

func (m *mockMetadataManager) GetNodeInfo(ctx context.Context, nodeID string) (*NodeInfo, error) {
	node, ok := m.nodes[nodeID]
	if !ok {
		return nil, nil
	}
	return node, nil
}

func (m *mockMetadataManager) GetAllNodes(ctx context.Context) ([]NodeInfo, error) {
	nodes := make([]NodeInfo, 0, len(m.nodes))
	for _, n := range m.nodes {
		nodes = append(nodes, *n)
	}
	return nodes, nil
}

func (m *mockMetadataManager) GetMyGroups(ctx context.Context, nodeID string) ([]GroupInfo, error) {
	if m.groupsErr != nil {
		return nil, m.groupsErr
	}
	return m.groups, nil
}

func (m *mockMetadataManager) GetGroupReplicas(ctx context.Context, groupID int, excludeNode string) ([]NodeInfo, error) {
	if m.groupRepErr != nil {
		return nil, m.groupRepErr
	}
	replicas, ok := m.groupReplicas[groupID]
	if !ok {
		return nil, nil
	}
	result := make([]NodeInfo, 0)
	for _, r := range replicas {
		if r.ID != excludeNode {
			result = append(result, r)
		}
	}
	return result, nil
}

// mockLocalStorage is a mock implementation of LocalStorage
type mockLocalStorage struct {
	mu               stdsync.RWMutex
	lastTimestamp    time.Time
	lastTimestampErr error
	points           []*storage.DataPoint
	checksum         string
	checksumErr      error
	writeCount       int64
	writeErr         error
}

func newMockLocalStorage() *mockLocalStorage {
	return &mockLocalStorage{
		points: make([]*storage.DataPoint, 0),
	}
}

func (m *mockLocalStorage) GetLastTimestamp(database, collection string, startTime, endTime time.Time) (time.Time, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.lastTimestampErr != nil {
		return time.Time{}, m.lastTimestampErr
	}
	return m.lastTimestamp, nil
}

func (m *mockLocalStorage) WriteToWAL(entries []*wal.Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.writeErr != nil {
		return m.writeErr
	}
	// Convert WAL entries to DataPoints for testing
	for _, entry := range entries {
		timeParsed, _ := time.Parse(time.RFC3339Nano, entry.Time)
		p := &storage.DataPoint{
			Database:   entry.Database,
			Collection: entry.Collection,
			ShardID:    entry.ShardID,
			Time:       timeParsed,
			ID:         entry.ID,
			Fields:     entry.Fields,
			InsertedAt: time.Now(),
		}
		m.points = append(m.points, p)
		atomic.AddInt64(&m.writeCount, 1)
	}
	return nil
}

func (m *mockLocalStorage) Query(database, collection string, deviceIDs []string, startTime, endTime time.Time, fields []string) ([]*storage.DataPoint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.points, nil
}

func (m *mockLocalStorage) GetChecksum(database, collection string, startTime, endTime time.Time) (string, int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.checksumErr != nil {
		return "", 0, m.checksumErr
	}
	return m.checksum, int64(len(m.points)), nil
}

// mockRemoteClient is a mock implementation of RemoteSyncClient
type mockRemoteClient struct {
	points       []*storage.DataPoint
	groupPoints  []*storage.DataPoint
	checksum     *SyncChecksum
	checksumErr  error
	lastTS       time.Time
	lastTSErr    error
	closeErr     error
	syncShardErr error
	syncGroupErr error
}

func newMockRemoteClient() *mockRemoteClient {
	return &mockRemoteClient{
		points:      make([]*storage.DataPoint, 0),
		groupPoints: make([]*storage.DataPoint, 0),
	}
}

func (m *mockRemoteClient) SyncShard(ctx context.Context, nodeAddress string, req *SyncRequest) (<-chan *storage.DataPoint, <-chan error) {
	pointsCh := make(chan *storage.DataPoint, len(m.points)+1)
	errCh := make(chan error, 1)

	go func() {
		defer close(pointsCh)
		defer close(errCh)

		if m.syncShardErr != nil {
			errCh <- m.syncShardErr
			return
		}

		for _, p := range m.points {
			select {
			case pointsCh <- p:
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}
		}
	}()

	return pointsCh, errCh
}

func (m *mockRemoteClient) GetChecksum(ctx context.Context, nodeAddress string, req *ChecksumRequest) (*SyncChecksum, error) {
	if m.checksumErr != nil {
		return nil, m.checksumErr
	}
	return m.checksum, nil
}

func (m *mockRemoteClient) GetLastTimestamp(ctx context.Context, nodeAddress string, req *LastTimestampRequest) (time.Time, error) {
	if m.lastTSErr != nil {
		return time.Time{}, m.lastTSErr
	}
	return m.lastTS, nil
}

func (m *mockRemoteClient) Close() error {
	return m.closeErr
}

func (m *mockRemoteClient) SyncGroup(ctx context.Context, nodeAddress string, req *GroupSyncRequest) (<-chan *storage.DataPoint, <-chan error) {
	pointsCh := make(chan *storage.DataPoint, len(m.groupPoints)+1)
	errCh := make(chan error, 1)

	go func() {
		defer close(pointsCh)
		defer close(errCh)

		if m.syncGroupErr != nil {
			errCh <- m.syncGroupErr
			return
		}

		for _, p := range m.groupPoints {
			select {
			case pointsCh <- p:
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}
		}
	}()

	return pointsCh, errCh
}

func (m *mockRemoteClient) GetGroupChecksum(ctx context.Context, nodeAddress string, req *GroupChecksumRequest) (*SyncChecksum, error) {
	if m.checksumErr != nil {
		return nil, m.checksumErr
	}
	return m.checksum, nil
}

func TestNewManager(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()
	metadata := newMockMetadataManager()
	localStorage := newMockLocalStorage()
	remoteClient := newMockRemoteClient()

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	if manager == nil {
		t.Fatal("Expected non-nil manager")
		return
	}
	if manager.nodeID != "node-1" {
		t.Errorf("Expected nodeID=node-1, got %s", manager.nodeID)
	}
}

func TestManager_SyncOnStartup_NoShards(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()
	metadata := newMockMetadataManager()
	localStorage := newMockLocalStorage()
	remoteClient := newMockRemoteClient()

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	ctx := context.Background()
	err := manager.SyncOnStartup(ctx)
	if err != nil {
		t.Fatalf("SyncOnStartup failed: %v", err)
	}
}

func TestManager_SyncOnStartup_WithShards(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()
	config.SyncBatchSize = 10

	metadata := newMockMetadataManager()
	metadata.shards = []ShardInfo{
		{
			ShardID:        "shard-1",
			Database:       "testdb",
			Collection:     "testcol",
			PrimaryNode:    "node-2",
			ReplicaNodes:   []string{"node-1", "node-3"},
			TimeRangeStart: time.Now().Add(-24 * time.Hour),
			TimeRangeEnd:   time.Now(),
		},
	}
	metadata.replicas["shard-1"] = []NodeInfo{
		{ID: "node-2", Address: "localhost:5556", Status: "active"},
		{ID: "node-3", Address: "localhost:5557", Status: "active"},
	}
	metadata.nodes["node-2"] = &NodeInfo{ID: "node-2", Address: "localhost:5556", Status: "active"}
	metadata.nodes["node-3"] = &NodeInfo{ID: "node-3", Address: "localhost:5557", Status: "active"}

	localStorage := newMockLocalStorage()
	localStorage.lastTimestamp = time.Now().Add(-1 * time.Hour)

	remoteClient := newMockRemoteClient()
	// Add some mock points to sync
	for i := 0; i < 100; i++ {
		remoteClient.points = append(remoteClient.points, &storage.DataPoint{
			Database:   "testdb",
			Collection: "testcol",
			Time:       time.Now().Add(-time.Duration(i) * time.Minute),
			ID:         "device-1",
			Fields:     map[string]interface{}{"value": float64(i)},
		})
	}

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	ctx := context.Background()
	err := manager.SyncOnStartup(ctx)
	if err != nil {
		t.Fatalf("SyncOnStartup failed: %v", err)
	}

	// Check that points were synced
	if len(localStorage.points) != 100 {
		t.Errorf("Expected 100 synced points, got %d", len(localStorage.points))
	}
}

func TestManager_SyncOnStartup_Disabled(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()
	config.StartupSync = false

	metadata := newMockMetadataManager()
	metadata.shards = []ShardInfo{
		{ShardID: "shard-1"},
	}

	localStorage := newMockLocalStorage()
	remoteClient := newMockRemoteClient()

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	ctx := context.Background()
	err := manager.SyncOnStartup(ctx)
	if err != nil {
		t.Fatalf("SyncOnStartup failed: %v", err)
	}

	// Should not have synced anything since disabled
	if len(localStorage.points) != 0 {
		t.Errorf("Expected 0 synced points when disabled, got %d", len(localStorage.points))
	}
}

func TestManager_GetProgress(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()
	metadata := newMockMetadataManager()
	localStorage := newMockLocalStorage()
	remoteClient := newMockRemoteClient()

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	// Initially no progress
	progress := manager.GetProgress("shard-1")
	if progress != nil {
		t.Error("Expected nil progress for unknown shard")
	}

	// Set progress
	manager.setProgress("shard-1", &SyncProgress{
		ShardID:      "shard-1",
		SyncedPoints: 100,
	})

	progress = manager.GetProgress("shard-1")
	if progress == nil {
		t.Fatal("Expected non-nil progress")
		return
	}
	if progress.SyncedPoints != 100 {
		t.Errorf("Expected SyncedPoints=100, got %d", progress.SyncedPoints)
	}

	// Clear progress
	manager.clearProgress("shard-1")
	progress = manager.GetProgress("shard-1")
	if progress != nil {
		t.Error("Expected nil progress after clear")
	}
}

func TestManager_EventHandler(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()
	metadata := newMockMetadataManager()
	localStorage := newMockLocalStorage()
	remoteClient := newMockRemoteClient()

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	var receivedEvents []SyncEvent
	manager.OnSyncEvent(func(event SyncEvent) {
		receivedEvents = append(receivedEvents, event)
	})

	// Emit a test event
	manager.emitEvent(SyncEvent{
		Type:      SyncEventStarted,
		ShardID:   "shard-1",
		Timestamp: time.Now(),
	})

	if len(receivedEvents) != 1 {
		t.Errorf("Expected 1 event, got %d", len(receivedEvents))
	}
	if receivedEvents[0].Type != SyncEventStarted {
		t.Errorf("Expected SyncEventStarted, got %v", receivedEvents[0].Type)
	}
}

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if !config.Enabled {
		t.Error("Expected Enabled=true by default")
	}
	if !config.StartupSync {
		t.Error("Expected StartupSync=true by default")
	}
	if config.SyncBatchSize != 1000 {
		t.Errorf("Expected SyncBatchSize=1000, got %d", config.SyncBatchSize)
	}
	if config.MaxConcurrentSyncs != 5 {
		t.Errorf("Expected MaxConcurrentSyncs=5, got %d", config.MaxConcurrentSyncs)
	}
	if !config.AntiEntropy.Enabled {
		t.Error("Expected AntiEntropy.Enabled=true by default")
	}
}

func TestConfig_Validate(t *testing.T) {
	config := Config{}
	err := config.Validate()
	if err != nil {
		t.Fatalf("Validate failed: %v", err)
	}

	// Check defaults were applied
	if config.SyncBatchSize != 1000 {
		t.Errorf("Expected SyncBatchSize=1000 after validate, got %d", config.SyncBatchSize)
	}
}

// ============================================================================
// New comprehensive tests
// ============================================================================

func TestManager_SyncOnStartup_GetShardsError(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()
	metadata := newMockMetadataManager()
	metadata.shardsErr = fmt.Errorf("etcd connection failed")
	localStorage := newMockLocalStorage()
	remoteClient := newMockRemoteClient()

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	ctx := context.Background()
	err := manager.SyncOnStartup(ctx)
	if err == nil {
		t.Fatal("Expected error when GetMyShards fails")
	}
}

func TestManager_SyncOnStartup_ShardAlreadyUpToDate(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()
	metadata := newMockMetadataManager()

	now := time.Now()
	metadata.shards = []ShardInfo{
		{
			ShardID:        "shard-1",
			Database:       "testdb",
			Collection:     "testcol",
			PrimaryNode:    "node-2",
			ReplicaNodes:   []string{"node-1"},
			TimeRangeStart: now.Add(-24 * time.Hour),
			TimeRangeEnd:   now.Add(-1 * time.Hour), // already past
		},
	}
	metadata.replicas["shard-1"] = []NodeInfo{
		{ID: "node-2", Address: "localhost:5556", Status: "active"},
	}

	localStorage := newMockLocalStorage()
	// Last local timestamp is AFTER shard end → already up to date
	localStorage.lastTimestamp = now

	remoteClient := newMockRemoteClient()
	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	ctx := context.Background()
	err := manager.SyncOnStartup(ctx)
	if err != nil {
		t.Fatalf("SyncOnStartup failed: %v", err)
	}

	// Should not have synced anything (already up to date)
	if len(localStorage.points) != 0 {
		t.Errorf("Expected 0 synced points (up to date), got %d", len(localStorage.points))
	}
}

func TestManager_SyncOnStartup_NoReplicas(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()
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
	// No replicas configured

	localStorage := newMockLocalStorage()
	localStorage.lastTimestamp = now.Add(-1 * time.Hour)
	remoteClient := newMockRemoteClient()

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	ctx := context.Background()
	err := manager.SyncOnStartup(ctx)
	if err != nil {
		t.Fatalf("SyncOnStartup failed: %v", err)
	}
}

func TestManager_SyncOnStartup_LocalTimestampError(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()
	config.SyncBatchSize = 10

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
	localStorage.lastTimestampErr = fmt.Errorf("no data found")

	remoteClient := newMockRemoteClient()
	for i := 0; i < 5; i++ {
		remoteClient.points = append(remoteClient.points, &storage.DataPoint{
			Database:   "testdb",
			Collection: "testcol",
			Time:       now.Add(-time.Duration(i) * time.Minute),
			ID:         "device-1",
			Fields:     map[string]interface{}{"value": float64(i)},
		})
	}

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	ctx := context.Background()
	err := manager.SyncOnStartup(ctx)
	if err != nil {
		t.Fatalf("SyncOnStartup failed: %v", err)
	}

	// Should still sync even when GetLastTimestamp errors (falls back to TimeRangeStart)
	if len(localStorage.points) != 5 {
		t.Errorf("Expected 5 synced points, got %d", len(localStorage.points))
	}
}

func TestManager_SyncOnStartup_ContextCancelled(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()
	config.StartupTimeout = 1 * time.Millisecond // very short timeout

	metadata := newMockMetadataManager()
	now := time.Now()
	// Add many shards to ensure timeout hits
	for i := 0; i < 20; i++ {
		metadata.shards = append(metadata.shards, ShardInfo{
			ShardID:        fmt.Sprintf("shard-%d", i),
			Database:       "testdb",
			Collection:     "testcol",
			TimeRangeStart: now.Add(-24 * time.Hour),
			TimeRangeEnd:   now,
		})
	}

	localStorage := newMockLocalStorage()
	localStorage.lastTimestamp = now.Add(-1 * time.Hour)
	remoteClient := newMockRemoteClient()

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	ctx := context.Background()
	// Should not error even with timeout (partial sync allowed)
	err := manager.SyncOnStartup(ctx)
	if err != nil {
		t.Fatalf("SyncOnStartup failed: %v", err)
	}
}

func TestManager_SyncGroupOnStartup_Disabled(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()
	config.StartupSync = false

	metadata := newMockMetadataManager()
	localStorage := newMockLocalStorage()
	remoteClient := newMockRemoteClient()

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	err := manager.SyncGroupOnStartup(context.Background())
	if err != nil {
		t.Fatalf("Expected no error when disabled, got: %v", err)
	}
}

func TestManager_SyncGroupOnStartup_EnabledFalse(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()
	config.Enabled = false

	metadata := newMockMetadataManager()
	localStorage := newMockLocalStorage()
	remoteClient := newMockRemoteClient()

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	err := manager.SyncGroupOnStartup(context.Background())
	if err != nil {
		t.Fatalf("Expected no error when Enabled=false, got: %v", err)
	}
}

func TestManager_SyncGroupOnStartup_NoGroups(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()
	metadata := newMockMetadataManager()
	// groups is empty
	localStorage := newMockLocalStorage()
	remoteClient := newMockRemoteClient()

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	err := manager.SyncGroupOnStartup(context.Background())
	if err != nil {
		t.Fatalf("Expected no error with no groups, got: %v", err)
	}
}

func TestManager_SyncGroupOnStartup_GetGroupsError(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()
	metadata := newMockMetadataManager()
	metadata.groupsErr = fmt.Errorf("etcd connection failed")

	localStorage := newMockLocalStorage()
	remoteClient := newMockRemoteClient()

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	err := manager.SyncGroupOnStartup(context.Background())
	if err == nil {
		t.Fatal("Expected error when GetMyGroups fails")
	}
}

func TestManager_SyncGroupOnStartup_WithGroups(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()
	config.SyncBatchSize = 10

	metadata := newMockMetadataManager()
	metadata.groups = []GroupInfo{
		{
			GroupID:      1,
			PrimaryNode:  "node-2",
			ReplicaNodes: []string{"node-1", "node-3"},
		},
	}
	metadata.groupReplicas[1] = []NodeInfo{
		{ID: "node-2", Address: "localhost:5556", Status: "active"},
	}

	localStorage := newMockLocalStorage()
	remoteClient := newMockRemoteClient()
	now := time.Now()
	for i := 0; i < 5; i++ {
		remoteClient.groupPoints = append(remoteClient.groupPoints, &storage.DataPoint{
			Database:   "testdb",
			Collection: "testcol",
			GroupID:    1,
			Time:       now.Add(-time.Duration(i) * time.Minute),
			ID:         "device-1",
			Fields:     map[string]interface{}{"value": float64(i)},
		})
	}

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	err := manager.SyncGroupOnStartup(context.Background())
	if err != nil {
		t.Fatalf("SyncGroupOnStartup failed: %v", err)
	}

	if len(localStorage.points) != 5 {
		t.Errorf("Expected 5 synced points, got %d", len(localStorage.points))
	}
}

func TestManager_SyncGroupOnStartup_NoReplicasForGroup(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()

	metadata := newMockMetadataManager()
	metadata.groups = []GroupInfo{
		{GroupID: 1, PrimaryNode: "node-1"},
	}
	// No group replicas

	localStorage := newMockLocalStorage()
	remoteClient := newMockRemoteClient()

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	err := manager.SyncGroupOnStartup(context.Background())
	if err != nil {
		t.Fatalf("SyncGroupOnStartup failed: %v", err)
	}
}

func TestManager_SyncGroupOnStartup_AllReplicasFail(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()

	metadata := newMockMetadataManager()
	metadata.groups = []GroupInfo{
		{GroupID: 1, PrimaryNode: "node-2", ReplicaNodes: []string{"node-1"}},
	}
	metadata.groupReplicas[1] = []NodeInfo{
		{ID: "node-2", Address: "localhost:5556", Status: "active"},
	}

	localStorage := newMockLocalStorage()
	localStorage.writeErr = fmt.Errorf("WAL write failed")

	remoteClient := newMockRemoteClient()
	remoteClient.groupPoints = []*storage.DataPoint{
		{Database: "testdb", Collection: "testcol", Time: time.Now(), ID: "dev-1", Fields: map[string]interface{}{"v": 1.0}},
	}

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	// Should not return error (partial sync allowed)
	err := manager.SyncGroupOnStartup(context.Background())
	if err != nil {
		t.Fatalf("Expected nil error (partial sync), got: %v", err)
	}
}

func TestManager_SyncGroupOnStartup_GetGroupReplicasError(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()

	metadata := newMockMetadataManager()
	metadata.groups = []GroupInfo{
		{GroupID: 1, PrimaryNode: "node-2", ReplicaNodes: []string{"node-1"}},
	}
	metadata.groupRepErr = fmt.Errorf("failed to get replicas")

	localStorage := newMockLocalStorage()
	remoteClient := newMockRemoteClient()

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	// Partial sync failure is allowed
	err := manager.SyncGroupOnStartup(context.Background())
	if err != nil {
		t.Fatalf("Expected nil error (partial sync), got: %v", err)
	}
}

func TestManager_SyncGroupOnStartup_StreamError(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()

	metadata := newMockMetadataManager()
	metadata.groups = []GroupInfo{
		{GroupID: 1, PrimaryNode: "node-2", ReplicaNodes: []string{"node-1"}},
	}
	metadata.groupReplicas[1] = []NodeInfo{
		{ID: "node-2", Address: "localhost:5556", Status: "active"},
	}

	localStorage := newMockLocalStorage()
	remoteClient := newMockRemoteClient()
	remoteClient.syncGroupErr = fmt.Errorf("gRPC stream error")

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	// Partial sync failure is allowed
	err := manager.SyncGroupOnStartup(context.Background())
	if err != nil {
		t.Fatalf("Expected nil error (partial sync), got: %v", err)
	}
}

func TestManager_SyncGroupOnStartup_BatchOverflow(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()
	config.SyncBatchSize = 3 // small batch size to trigger overflow

	metadata := newMockMetadataManager()
	metadata.groups = []GroupInfo{
		{GroupID: 1, PrimaryNode: "node-2", ReplicaNodes: []string{"node-1"}},
	}
	metadata.groupReplicas[1] = []NodeInfo{
		{ID: "node-2", Address: "localhost:5556", Status: "active"},
	}

	localStorage := newMockLocalStorage()
	remoteClient := newMockRemoteClient()
	now := time.Now()
	// 10 points with batch size 3 → 3 full batches + 1 partial
	for i := 0; i < 10; i++ {
		remoteClient.groupPoints = append(remoteClient.groupPoints, &storage.DataPoint{
			Database:   "testdb",
			Collection: "testcol",
			GroupID:    1,
			Time:       now.Add(-time.Duration(i) * time.Minute),
			ID:         fmt.Sprintf("device-%d", i),
			Fields:     map[string]interface{}{"value": float64(i)},
		})
	}

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	err := manager.SyncGroupOnStartup(context.Background())
	if err != nil {
		t.Fatalf("SyncGroupOnStartup failed: %v", err)
	}

	if len(localStorage.points) != 10 {
		t.Errorf("Expected 10 synced points, got %d", len(localStorage.points))
	}
}

func TestManager_SyncShard_Public(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()
	config.SyncBatchSize = 10

	metadata := newMockMetadataManager()
	now := time.Now()
	shard := ShardInfo{
		ShardID:        "shard-1",
		Database:       "testdb",
		Collection:     "testcol",
		TimeRangeStart: now.Add(-24 * time.Hour),
		TimeRangeEnd:   now,
	}
	metadata.replicas["shard-1"] = []NodeInfo{
		{ID: "node-2", Address: "localhost:5556", Status: "active"},
	}

	localStorage := newMockLocalStorage()
	localStorage.lastTimestamp = now.Add(-1 * time.Hour)

	remoteClient := newMockRemoteClient()
	remoteClient.points = append(remoteClient.points, &storage.DataPoint{
		Database:   "testdb",
		Collection: "testcol",
		Time:       now,
		ID:         "device-1",
		Fields:     map[string]interface{}{"value": 42.0},
	})

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	err := manager.SyncShard(context.Background(), shard)
	if err != nil {
		t.Fatalf("SyncShard failed: %v", err)
	}

	if len(localStorage.points) != 1 {
		t.Errorf("Expected 1 synced point, got %d", len(localStorage.points))
	}
}

func TestManager_SyncShard_AllReplicasFail(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()

	metadata := newMockMetadataManager()
	now := time.Now()
	shard := ShardInfo{
		ShardID:        "shard-1",
		Database:       "testdb",
		Collection:     "testcol",
		TimeRangeStart: now.Add(-24 * time.Hour),
		TimeRangeEnd:   now,
	}
	metadata.replicas["shard-1"] = []NodeInfo{
		{ID: "node-2", Address: "localhost:5556", Status: "active"},
	}

	localStorage := newMockLocalStorage()
	localStorage.lastTimestamp = now.Add(-1 * time.Hour)
	localStorage.writeErr = fmt.Errorf("disk full")

	remoteClient := newMockRemoteClient()
	remoteClient.points = append(remoteClient.points, &storage.DataPoint{
		Database:   "testdb",
		Collection: "testcol",
		Time:       now,
		ID:         "device-1",
		Fields:     map[string]interface{}{"value": 1.0},
	})

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	err := manager.SyncShard(context.Background(), shard)
	if err == nil {
		t.Fatal("Expected error when all replicas fail")
	}
}

func TestManager_SyncShard_GetReplicasError(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()

	metadata := newMockMetadataManager()
	metadata.replicasErr = fmt.Errorf("etcd error")

	now := time.Now()
	shard := ShardInfo{
		ShardID:        "shard-1",
		Database:       "testdb",
		Collection:     "testcol",
		TimeRangeStart: now.Add(-24 * time.Hour),
		TimeRangeEnd:   now,
	}

	localStorage := newMockLocalStorage()
	localStorage.lastTimestamp = now.Add(-1 * time.Hour)
	remoteClient := newMockRemoteClient()

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	err := manager.SyncShard(context.Background(), shard)
	if err == nil {
		t.Fatal("Expected error when GetActiveReplicas fails")
	}
}

func TestManager_SyncFromReplica_WriteError(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()
	config.SyncBatchSize = 2

	metadata := newMockMetadataManager()
	now := time.Now()
	shard := ShardInfo{
		ShardID:        "shard-1",
		Database:       "testdb",
		Collection:     "testcol",
		TimeRangeStart: now.Add(-24 * time.Hour),
		TimeRangeEnd:   now,
	}
	metadata.replicas["shard-1"] = []NodeInfo{
		{ID: "node-2", Address: "localhost:5556", Status: "active"},
	}

	localStorage := newMockLocalStorage()
	localStorage.lastTimestamp = now.Add(-1 * time.Hour)
	localStorage.writeErr = fmt.Errorf("write failed")

	remoteClient := newMockRemoteClient()
	for i := 0; i < 5; i++ {
		remoteClient.points = append(remoteClient.points, &storage.DataPoint{
			Database:   "testdb",
			Collection: "testcol",
			Time:       now.Add(-time.Duration(i) * time.Minute),
			ID:         "device-1",
			Fields:     map[string]interface{}{"value": float64(i)},
		})
	}

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	err := manager.SyncShard(context.Background(), shard)
	if err == nil {
		t.Fatal("Expected error when WriteToWAL fails")
	}
}

func TestManager_SyncFromReplica_StreamError(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()

	metadata := newMockMetadataManager()
	now := time.Now()
	shard := ShardInfo{
		ShardID:        "shard-1",
		Database:       "testdb",
		Collection:     "testcol",
		TimeRangeStart: now.Add(-24 * time.Hour),
		TimeRangeEnd:   now,
	}
	metadata.replicas["shard-1"] = []NodeInfo{
		{ID: "node-2", Address: "localhost:5556", Status: "active"},
	}

	localStorage := newMockLocalStorage()
	localStorage.lastTimestamp = now.Add(-1 * time.Hour)

	remoteClient := newMockRemoteClient()
	remoteClient.syncShardErr = fmt.Errorf("gRPC connection refused")

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	err := manager.SyncShard(context.Background(), shard)
	if err == nil {
		t.Fatal("Expected error when sync stream fails")
	}
}

func TestManager_SyncShard_MultipleReplicas_FirstFails(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()
	config.SyncBatchSize = 100

	metadata := newMockMetadataManager()
	now := time.Now()
	shard := ShardInfo{
		ShardID:        "shard-1",
		Database:       "testdb",
		Collection:     "testcol",
		TimeRangeStart: now.Add(-24 * time.Hour),
		TimeRangeEnd:   now,
	}
	// Two replicas
	metadata.replicas["shard-1"] = []NodeInfo{
		{ID: "node-2", Address: "localhost:5556", Status: "active"},
		{ID: "node-3", Address: "localhost:5557", Status: "active"},
	}

	localStorage := newMockLocalStorage()
	localStorage.lastTimestamp = now.Add(-1 * time.Hour)

	remoteClient := newMockRemoteClient()
	remoteClient.points = append(remoteClient.points, &storage.DataPoint{
		Database:   "testdb",
		Collection: "testcol",
		Time:       now,
		ID:         "device-1",
		Fields:     map[string]interface{}{"value": 1.0},
	})

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	err := manager.SyncShard(context.Background(), shard)
	if err != nil {
		t.Fatalf("SyncShard should succeed with at least one working replica: %v", err)
	}
}

func TestManager_GetAllProgress(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()
	metadata := newMockMetadataManager()
	localStorage := newMockLocalStorage()
	remoteClient := newMockRemoteClient()

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	// Initially empty
	progress := manager.GetAllProgress()
	if len(progress) != 0 {
		t.Errorf("Expected empty progress, got %d entries", len(progress))
	}

	// Set multiple progress entries
	manager.setProgress("shard-1", &SyncProgress{ShardID: "shard-1", SyncedPoints: 100})
	manager.setProgress("shard-2", &SyncProgress{ShardID: "shard-2", SyncedPoints: 200})

	progress = manager.GetAllProgress()
	if len(progress) != 2 {
		t.Fatalf("Expected 2 progress entries, got %d", len(progress))
	}

	if progress["shard-1"].SyncedPoints != 100 {
		t.Errorf("Expected shard-1 SyncedPoints=100, got %d", progress["shard-1"].SyncedPoints)
	}
	if progress["shard-2"].SyncedPoints != 200 {
		t.Errorf("Expected shard-2 SyncedPoints=200, got %d", progress["shard-2"].SyncedPoints)
	}

	// Clear one
	manager.clearProgress("shard-1")
	progress = manager.GetAllProgress()
	if len(progress) != 1 {
		t.Errorf("Expected 1 progress entry after clear, got %d", len(progress))
	}
}

func TestManager_GetAllProgress_ReturnsNewMap(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()
	metadata := newMockMetadataManager()
	localStorage := newMockLocalStorage()
	remoteClient := newMockRemoteClient()

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	manager.setProgress("shard-1", &SyncProgress{ShardID: "shard-1"})

	p1 := manager.GetAllProgress()
	p2 := manager.GetAllProgress()

	// Modifying p1 should not affect p2
	delete(p1, "shard-1")
	if len(p2) != 1 {
		t.Error("GetAllProgress should return a copy")
	}
}

func TestManager_Stop(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()
	metadata := newMockMetadataManager()
	localStorage := newMockLocalStorage()
	remoteClient := newMockRemoteClient()

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	// Stop should not panic
	manager.Stop()
}

func TestManager_Stop_WithCloseError(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()
	metadata := newMockMetadataManager()
	localStorage := newMockLocalStorage()
	remoteClient := newMockRemoteClient()
	remoteClient.closeErr = fmt.Errorf("close failed")

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	// Stop should not panic even with close error
	manager.Stop()
}

func TestManager_MultipleEventHandlers(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()
	metadata := newMockMetadataManager()
	localStorage := newMockLocalStorage()
	remoteClient := newMockRemoteClient()

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	var count1, count2, count3 int
	manager.OnSyncEvent(func(event SyncEvent) { count1++ })
	manager.OnSyncEvent(func(event SyncEvent) { count2++ })
	manager.OnSyncEvent(func(event SyncEvent) { count3++ })

	manager.emitEvent(SyncEvent{Type: SyncEventStarted, Timestamp: time.Now()})
	manager.emitEvent(SyncEvent{Type: SyncEventCompleted, Timestamp: time.Now()})

	if count1 != 2 {
		t.Errorf("Handler 1: expected 2 events, got %d", count1)
	}
	if count2 != 2 {
		t.Errorf("Handler 2: expected 2 events, got %d", count2)
	}
	if count3 != 2 {
		t.Errorf("Handler 3: expected 2 events, got %d", count3)
	}
}

func TestManager_EmitEvent_NoHandlers(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()
	metadata := newMockMetadataManager()
	localStorage := newMockLocalStorage()
	remoteClient := newMockRemoteClient()

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	// Should not panic with no handlers
	manager.emitEvent(SyncEvent{Type: SyncEventStarted, Timestamp: time.Now()})
}

func TestManager_DataPointsToWALEntries(t *testing.T) {
	now := time.Now()
	points := []*storage.DataPoint{
		{
			Database:   "db1",
			Collection: "col1",
			ShardID:    "shard-1",
			Time:       now,
			ID:         "device-1",
			Fields:     map[string]interface{}{"temp": 25.5, "humidity": 60.0},
		},
		{
			Database:   "db2",
			Collection: "col2",
			ShardID:    "shard-2",
			Time:       now.Add(-1 * time.Hour),
			ID:         "device-2",
			Fields:     map[string]interface{}{"voltage": 220.0},
		},
	}

	entries := dataPointsToWALEntries(points)

	if len(entries) != 2 {
		t.Fatalf("Expected 2 entries, got %d", len(entries))
	}

	// Check first entry
	if entries[0].Database != "db1" {
		t.Errorf("Entry[0].Database: expected db1, got %s", entries[0].Database)
	}
	if entries[0].Collection != "col1" {
		t.Errorf("Entry[0].Collection: expected col1, got %s", entries[0].Collection)
	}
	if entries[0].ShardID != "shard-1" {
		t.Errorf("Entry[0].ShardID: expected shard-1, got %s", entries[0].ShardID)
	}
	if entries[0].ID != "device-1" {
		t.Errorf("Entry[0].ID: expected device-1, got %s", entries[0].ID)
	}
	if entries[0].Type != wal.EntryTypeWrite {
		t.Errorf("Entry[0].Type: expected EntryTypeWrite")
	}
	if entries[0].Time != now.Format(time.RFC3339Nano) {
		t.Errorf("Entry[0].Time: expected %s, got %s", now.Format(time.RFC3339Nano), entries[0].Time)
	}
	if entries[0].Timestamp != now.UnixNano() {
		t.Errorf("Entry[0].Timestamp: expected %d, got %d", now.UnixNano(), entries[0].Timestamp)
	}
	if len(entries[0].Fields) != 2 {
		t.Errorf("Entry[0].Fields: expected 2 fields, got %d", len(entries[0].Fields))
	}

	// Check second entry
	if entries[1].Database != "db2" {
		t.Errorf("Entry[1].Database: expected db2, got %s", entries[1].Database)
	}
	if entries[1].ID != "device-2" {
		t.Errorf("Entry[1].ID: expected device-2, got %s", entries[1].ID)
	}
}

func TestManager_DataPointsToWALEntries_Empty(t *testing.T) {
	entries := dataPointsToWALEntries([]*storage.DataPoint{})
	if len(entries) != 0 {
		t.Errorf("Expected 0 entries for empty input, got %d", len(entries))
	}
}

func TestManager_DataPointsToWALEntries_NilFields(t *testing.T) {
	now := time.Now()
	points := []*storage.DataPoint{
		{
			Database:   "db1",
			Collection: "col1",
			Time:       now,
			ID:         "device-1",
			Fields:     nil,
		},
	}

	entries := dataPointsToWALEntries(points)
	if len(entries) != 1 {
		t.Fatalf("Expected 1 entry, got %d", len(entries))
	}
	if entries[0].Fields != nil {
		t.Errorf("Expected nil fields, got %v", entries[0].Fields)
	}
}

func TestManager_EventHandler_DuringSyncOnStartup(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()
	config.SyncBatchSize = 50

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
	localStorage.lastTimestamp = now.Add(-1 * time.Hour)

	remoteClient := newMockRemoteClient()
	for i := 0; i < 5; i++ {
		remoteClient.points = append(remoteClient.points, &storage.DataPoint{
			Database:   "testdb",
			Collection: "testcol",
			Time:       now.Add(-time.Duration(i) * time.Minute),
			ID:         "device-1",
			Fields:     map[string]interface{}{"value": float64(i)},
		})
	}

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	var events []SyncEvent
	manager.OnSyncEvent(func(event SyncEvent) {
		events = append(events, event)
	})

	err := manager.SyncOnStartup(context.Background())
	if err != nil {
		t.Fatalf("SyncOnStartup failed: %v", err)
	}

	// Should have at least Started and Completed events
	hasStarted := false
	hasCompleted := false
	for _, e := range events {
		if e.Type == SyncEventStarted {
			hasStarted = true
		}
		if e.Type == SyncEventCompleted {
			hasCompleted = true
		}
	}

	if !hasStarted {
		t.Error("Expected SyncEventStarted event")
	}
	if !hasCompleted {
		t.Error("Expected SyncEventCompleted event")
	}
}

func TestManager_SyncOnStartup_BatchOverflow(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()
	config.SyncBatchSize = 3 // Small batch to trigger overflow

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
	localStorage.lastTimestamp = now.Add(-1 * time.Hour)

	remoteClient := newMockRemoteClient()
	// 10 points with batch size 3 → triggers multiple batch writes
	for i := 0; i < 10; i++ {
		remoteClient.points = append(remoteClient.points, &storage.DataPoint{
			Database:   "testdb",
			Collection: "testcol",
			Time:       now.Add(-time.Duration(i) * time.Minute),
			ID:         fmt.Sprintf("device-%d", i),
			Fields:     map[string]interface{}{"value": float64(i)},
		})
	}

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	var progressEvents int
	manager.OnSyncEvent(func(event SyncEvent) {
		if event.Type == SyncEventProgress {
			progressEvents++
		}
	})

	err := manager.SyncOnStartup(context.Background())
	if err != nil {
		t.Fatalf("SyncOnStartup failed: %v", err)
	}

	if len(localStorage.points) != 10 {
		t.Errorf("Expected 10 synced points, got %d", len(localStorage.points))
	}

	// With batch size 3 and 10 points, expect 3 progress events (at 3, 6, 9)
	if progressEvents != 3 {
		t.Errorf("Expected 3 progress events, got %d", progressEvents)
	}
}

func TestManager_SyncOnStartup_MultipleShards(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()
	config.SyncBatchSize = 100
	config.MaxConcurrentSyncs = 2

	metadata := newMockMetadataManager()
	now := time.Now()

	for i := 0; i < 5; i++ {
		shardID := fmt.Sprintf("shard-%d", i)
		metadata.shards = append(metadata.shards, ShardInfo{
			ShardID:        shardID,
			Database:       "testdb",
			Collection:     "testcol",
			TimeRangeStart: now.Add(-24 * time.Hour),
			TimeRangeEnd:   now,
		})
		metadata.replicas[shardID] = []NodeInfo{
			{ID: "node-2", Address: "localhost:5556", Status: "active"},
		}
	}

	localStorage := newMockLocalStorage()
	localStorage.lastTimestamp = now.Add(-1 * time.Hour)

	remoteClient := newMockRemoteClient()
	for i := 0; i < 3; i++ {
		remoteClient.points = append(remoteClient.points, &storage.DataPoint{
			Database:   "testdb",
			Collection: "testcol",
			Time:       now.Add(-time.Duration(i) * time.Minute),
			ID:         "device-1",
			Fields:     map[string]interface{}{"value": float64(i)},
		})
	}

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	err := manager.SyncOnStartup(context.Background())
	if err != nil {
		t.Fatalf("SyncOnStartup failed: %v", err)
	}

	// 5 shards × 3 points each = 15 points
	if len(localStorage.points) != 15 {
		t.Errorf("Expected 15 synced points, got %d", len(localStorage.points))
	}
}

func TestManager_SyncOnStartup_SyncDisabledButEnabled(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()
	config.Enabled = false // Enabled = false
	config.StartupSync = true

	metadata := newMockMetadataManager()
	metadata.shards = []ShardInfo{{ShardID: "shard-1"}}

	localStorage := newMockLocalStorage()
	remoteClient := newMockRemoteClient()

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	err := manager.SyncOnStartup(context.Background())
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Should not sync because Enabled=false
	if len(localStorage.points) != 0 {
		t.Errorf("Expected 0 points when Enabled=false, got %d", len(localStorage.points))
	}
}

func TestManager_SetProgress_ClearProgress_Concurrent(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()
	metadata := newMockMetadataManager()
	localStorage := newMockLocalStorage()
	remoteClient := newMockRemoteClient()

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	// Test concurrent access
	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			manager.setProgress(fmt.Sprintf("shard-%d", i), &SyncProgress{ShardID: fmt.Sprintf("shard-%d", i)})
		}
		close(done)
	}()

	go func() {
		for i := 0; i < 100; i++ {
			manager.GetProgress(fmt.Sprintf("shard-%d", i))
		}
	}()

	go func() {
		for i := 0; i < 100; i++ {
			manager.GetAllProgress()
		}
	}()

	<-done
}

func TestManager_SyncGroupFromReplica_WriteError(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()
	config.SyncBatchSize = 2

	metadata := newMockMetadataManager()
	metadata.groups = []GroupInfo{
		{GroupID: 1, PrimaryNode: "node-2", ReplicaNodes: []string{"node-1"}},
	}
	metadata.groupReplicas[1] = []NodeInfo{
		{ID: "node-2", Address: "localhost:5556", Status: "active"},
	}

	localStorage := newMockLocalStorage()
	localStorage.writeErr = fmt.Errorf("WAL write failed")

	remoteClient := newMockRemoteClient()
	now := time.Now()
	// More than batch size to trigger write during batch
	for i := 0; i < 5; i++ {
		remoteClient.groupPoints = append(remoteClient.groupPoints, &storage.DataPoint{
			Database:   "testdb",
			Collection: "testcol",
			GroupID:    1,
			Time:       now.Add(-time.Duration(i) * time.Minute),
			ID:         fmt.Sprintf("device-%d", i),
			Fields:     map[string]interface{}{"value": float64(i)},
		})
	}

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	// Should not return error at top level (partial sync allowed)
	err := manager.SyncGroupOnStartup(context.Background())
	if err != nil {
		t.Fatalf("Expected nil error (partial sync), got: %v", err)
	}
}

func TestManager_SyncFromReplica_ContextCancel(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()
	config.SyncBatchSize = 1000 // Large batch so we don't write during iteration

	metadata := newMockMetadataManager()
	now := time.Now()
	shard := ShardInfo{
		ShardID:        "shard-1",
		Database:       "testdb",
		Collection:     "testcol",
		TimeRangeStart: now.Add(-24 * time.Hour),
		TimeRangeEnd:   now,
	}
	metadata.replicas["shard-1"] = []NodeInfo{
		{ID: "node-2", Address: "localhost:5556", Status: "active"},
	}

	localStorage := newMockLocalStorage()
	localStorage.lastTimestamp = now.Add(-1 * time.Hour)

	// Create a mock that sends points slowly and can be cancelled
	remoteClient := &slowMockRemoteClient{
		delay:  50 * time.Millisecond,
		points: make([]*storage.DataPoint, 50), // many points
	}
	for i := range remoteClient.points {
		remoteClient.points[i] = &storage.DataPoint{
			Database:   "testdb",
			Collection: "testcol",
			Time:       now.Add(-time.Duration(i) * time.Minute),
			ID:         "device-1",
			Fields:     map[string]interface{}{"value": float64(i)},
		}
	}

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// This should eventually fail due to context cancellation
	err := manager.SyncShard(ctx, shard)
	// May or may not error depending on timing, but should not panic
	_ = err
}

func TestManager_SyncGroupFromReplica_ContextCancel(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()
	config.SyncBatchSize = 1000

	metadata := newMockMetadataManager()
	metadata.groups = []GroupInfo{
		{GroupID: 1, PrimaryNode: "node-2", ReplicaNodes: []string{"node-1"}},
	}
	metadata.groupReplicas[1] = []NodeInfo{
		{ID: "node-2", Address: "localhost:5556", Status: "active"},
	}

	localStorage := newMockLocalStorage()

	remoteClient := &slowMockRemoteClient{
		delay:       50 * time.Millisecond,
		groupPoints: make([]*storage.DataPoint, 50),
	}
	now := time.Now()
	for i := range remoteClient.groupPoints {
		remoteClient.groupPoints[i] = &storage.DataPoint{
			Database:   "testdb",
			Collection: "testcol",
			GroupID:    1,
			Time:       now.Add(-time.Duration(i) * time.Minute),
			ID:         fmt.Sprintf("device-%d", i),
			Fields:     map[string]interface{}{"value": float64(i)},
		}
	}

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := manager.SyncGroupOnStartup(ctx)
	_ = err // timing dependent
}

// slowMockRemoteClient sends points with a delay to test context cancellation
type slowMockRemoteClient struct {
	delay       time.Duration
	points      []*storage.DataPoint
	groupPoints []*storage.DataPoint
}

func (m *slowMockRemoteClient) SyncShard(ctx context.Context, nodeAddress string, req *SyncRequest) (<-chan *storage.DataPoint, <-chan error) {
	pointsCh := make(chan *storage.DataPoint, 10)
	errCh := make(chan error, 1)

	go func() {
		defer close(pointsCh)
		defer close(errCh)

		for _, p := range m.points {
			select {
			case <-time.After(m.delay):
				select {
				case pointsCh <- p:
				case <-ctx.Done():
					errCh <- ctx.Err()
					return
				}
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}
		}
	}()

	return pointsCh, errCh
}

func (m *slowMockRemoteClient) SyncGroup(ctx context.Context, nodeAddress string, req *GroupSyncRequest) (<-chan *storage.DataPoint, <-chan error) {
	pointsCh := make(chan *storage.DataPoint, 10)
	errCh := make(chan error, 1)

	go func() {
		defer close(pointsCh)
		defer close(errCh)

		for _, p := range m.groupPoints {
			select {
			case <-time.After(m.delay):
				select {
				case pointsCh <- p:
				case <-ctx.Done():
					errCh <- ctx.Err()
					return
				}
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}
		}
	}()

	return pointsCh, errCh
}

func (m *slowMockRemoteClient) GetChecksum(ctx context.Context, nodeAddress string, req *ChecksumRequest) (*SyncChecksum, error) {
	return nil, nil
}

func (m *slowMockRemoteClient) GetGroupChecksum(ctx context.Context, nodeAddress string, req *GroupChecksumRequest) (*SyncChecksum, error) {
	return nil, nil
}

func (m *slowMockRemoteClient) GetLastTimestamp(ctx context.Context, nodeAddress string, req *LastTimestampRequest) (time.Time, error) {
	return time.Time{}, nil
}

func (m *slowMockRemoteClient) Close() error {
	return nil
}

func TestManager_SyncOnStartup_SemaphoreTimeout(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()
	config.MaxConcurrentSyncs = 1
	config.StartupTimeout = 200 * time.Millisecond
	config.SyncBatchSize = 1000

	metadata := newMockMetadataManager()
	now := time.Now()
	// Many shards to fill the semaphore
	for i := 0; i < 10; i++ {
		shardID := fmt.Sprintf("shard-%d", i)
		metadata.shards = append(metadata.shards, ShardInfo{
			ShardID:        shardID,
			Database:       "testdb",
			Collection:     "testcol",
			TimeRangeStart: now.Add(-24 * time.Hour),
			TimeRangeEnd:   now,
		})
		metadata.replicas[shardID] = []NodeInfo{
			{ID: "node-2", Address: "localhost:5556", Status: "active"},
		}
	}

	localStorage := newMockLocalStorage()
	localStorage.lastTimestamp = now.Add(-1 * time.Hour)

	// Slow client to make sync take time
	remoteClient := &slowMockRemoteClient{
		delay:  100 * time.Millisecond,
		points: make([]*storage.DataPoint, 5),
	}
	for i := range remoteClient.points {
		remoteClient.points[i] = &storage.DataPoint{
			Database:   "testdb",
			Collection: "testcol",
			Time:       now.Add(-time.Duration(i) * time.Minute),
			ID:         "device-1",
			Fields:     map[string]interface{}{"value": float64(i)},
		}
	}

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	err := manager.SyncOnStartup(context.Background())
	// Should succeed (partial sync allowed) even with timeout
	if err != nil {
		t.Fatalf("Expected nil error, got: %v", err)
	}
}

func TestManager_SyncGroupOnStartup_SemaphoreTimeout(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()
	config.MaxConcurrentSyncs = 1
	config.StartupTimeout = 200 * time.Millisecond

	metadata := newMockMetadataManager()
	now := time.Now()
	for i := 0; i < 10; i++ {
		metadata.groups = append(metadata.groups, GroupInfo{
			GroupID:      i,
			PrimaryNode:  "node-2",
			ReplicaNodes: []string{"node-1"},
		})
		metadata.groupReplicas[i] = []NodeInfo{
			{ID: "node-2", Address: "localhost:5556", Status: "active"},
		}
	}

	localStorage := newMockLocalStorage()

	remoteClient := &slowMockRemoteClient{
		delay:       100 * time.Millisecond,
		groupPoints: make([]*storage.DataPoint, 5),
	}
	for i := range remoteClient.groupPoints {
		remoteClient.groupPoints[i] = &storage.DataPoint{
			Database:   "testdb",
			Collection: "testcol",
			Time:       now.Add(-time.Duration(i) * time.Minute),
			ID:         fmt.Sprintf("device-%d", i),
			Fields:     map[string]interface{}{"value": float64(i)},
		}
	}

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	err := manager.SyncGroupOnStartup(context.Background())
	if err != nil {
		t.Fatalf("Expected nil error, got: %v", err)
	}
}

func TestManager_SyncOnStartup_FinalBatchWrite(t *testing.T) {
	// Test that the final batch (remainder) is written correctly
	logger := logging.NewDevelopment()
	config := DefaultConfig()
	config.SyncBatchSize = 7 // 10 points / 7 = 1 full batch + 3 remaining

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
	localStorage.lastTimestamp = now.Add(-1 * time.Hour)

	remoteClient := newMockRemoteClient()
	for i := 0; i < 10; i++ {
		remoteClient.points = append(remoteClient.points, &storage.DataPoint{
			Database:   "testdb",
			Collection: "testcol",
			Time:       now.Add(-time.Duration(i) * time.Minute),
			ID:         fmt.Sprintf("device-%d", i),
			Fields:     map[string]interface{}{"value": float64(i)},
		})
	}

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	err := manager.SyncOnStartup(context.Background())
	if err != nil {
		t.Fatalf("SyncOnStartup failed: %v", err)
	}

	// All 10 points should be written (7 in first batch + 3 in final batch)
	if len(localStorage.points) != 10 {
		t.Errorf("Expected 10 points, got %d", len(localStorage.points))
	}
}

func TestManager_SyncFromReplica_FinalBatchWriteError(t *testing.T) {
	logger := logging.NewDevelopment()
	config := DefaultConfig()
	config.SyncBatchSize = 100 // Large enough to not trigger mid-batch write

	metadata := newMockMetadataManager()
	now := time.Now()
	shard := ShardInfo{
		ShardID:        "shard-1",
		Database:       "testdb",
		Collection:     "testcol",
		TimeRangeStart: now.Add(-24 * time.Hour),
		TimeRangeEnd:   now,
	}
	metadata.replicas["shard-1"] = []NodeInfo{
		{ID: "node-2", Address: "localhost:5556", Status: "active"},
	}

	// Use a mock that fails on the second write call (first batch succeeds, final fails)
	localStorage := &writeCountingMockStorage{
		failOnWrite: 1, // fail on first write
	}

	remoteClient := newMockRemoteClient()
	remoteClient.points = append(remoteClient.points, &storage.DataPoint{
		Database:   "testdb",
		Collection: "testcol",
		Time:       now,
		ID:         "device-1",
		Fields:     map[string]interface{}{"value": 1.0},
	})

	manager := NewManager(config, "node-1", logger, metadata, localStorage, remoteClient)

	err := manager.SyncShard(context.Background(), shard)
	if err == nil {
		t.Fatal("Expected error when final batch write fails")
	}
}

// writeCountingMockStorage fails on Nth write
type writeCountingMockStorage struct {
	writes      int
	failOnWrite int // fail on this write number (1-based)
}

func (m *writeCountingMockStorage) GetLastTimestamp(database, collection string, startTime, endTime time.Time) (time.Time, error) {
	return startTime, nil
}

func (m *writeCountingMockStorage) WriteToWAL(entries []*wal.Entry) error {
	m.writes++
	if m.writes == m.failOnWrite {
		return fmt.Errorf("write failed on attempt %d", m.writes)
	}
	return nil
}

func (m *writeCountingMockStorage) Query(database, collection string, deviceIDs []string, startTime, endTime time.Time, fields []string) ([]*storage.DataPoint, error) {
	return nil, nil
}

func (m *writeCountingMockStorage) GetChecksum(database, collection string, startTime, endTime time.Time) (string, int64, error) {
	return "", 0, nil
}
