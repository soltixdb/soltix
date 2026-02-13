package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/soltixdb/soltix/internal/config"
	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/metadata"
	"github.com/soltixdb/soltix/internal/models"
)

// MockMetadataManager implements metadata.Manager interface for testing
type MockMetadataManager struct {
	data map[string]string
	err  error
}

func NewMockMetadataManager() *MockMetadataManager {
	return &MockMetadataManager{
		data: make(map[string]string),
	}
}

func (m *MockMetadataManager) Get(ctx context.Context, key string) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	val, ok := m.data[key]
	if !ok {
		return "", nil
	}
	return val, nil
}

func (m *MockMetadataManager) Put(ctx context.Context, key, value string) error {
	if m.err != nil {
		return m.err
	}
	m.data[key] = value
	return nil
}

func (m *MockMetadataManager) GetPrefix(ctx context.Context, prefix string) (map[string]string, error) {
	if m.err != nil {
		return nil, m.err
	}
	result := make(map[string]string)
	for k, v := range m.data {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			result[k] = v
		}
	}
	return result, nil
}

func (m *MockMetadataManager) Close() error {
	return nil
}

func (m *MockMetadataManager) CreateDatabase(ctx context.Context, db *metadata.Database) error {
	return nil
}

func (m *MockMetadataManager) GetDatabase(ctx context.Context, name string) (*metadata.Database, error) {
	return nil, nil
}

func (m *MockMetadataManager) ListDatabases(ctx context.Context) ([]*metadata.Database, error) {
	return nil, nil
}

func (m *MockMetadataManager) DeleteDatabase(ctx context.Context, name string) error {
	return nil
}

func (m *MockMetadataManager) DatabaseExists(ctx context.Context, name string) (bool, error) {
	return false, nil
}

func (m *MockMetadataManager) CreateCollection(ctx context.Context, dbName string, coll *metadata.Collection) error {
	return nil
}

func (m *MockMetadataManager) GetCollection(ctx context.Context, dbName, collName string) (*metadata.Collection, error) {
	return nil, nil
}

func (m *MockMetadataManager) ListCollections(ctx context.Context, dbName string) ([]*metadata.Collection, error) {
	return nil, nil
}

func (m *MockMetadataManager) UpdateCollection(ctx context.Context, dbName string, coll *metadata.Collection) error {
	return nil
}

func (m *MockMetadataManager) DeleteCollection(ctx context.Context, dbName, collName string) error {
	return nil
}

func (m *MockMetadataManager) CollectionExists(ctx context.Context, dbName, collName string) (bool, error) {
	return false, nil
}

func (m *MockMetadataManager) ValidateCollection(ctx context.Context, dbName, collName string) error {
	return nil
}

func (m *MockMetadataManager) TrackDeviceID(ctx context.Context, dbName, collName, deviceID string) error {
	return nil
}

func (m *MockMetadataManager) TrackField(ctx context.Context, dbName, collName, fieldName, fieldType string) error {
	return nil
}

func (m *MockMetadataManager) TrackFields(ctx context.Context, dbName, collName string, fieldSchemas map[string]string) error {
	return nil
}

func (m *MockMetadataManager) GetDeviceIDs(ctx context.Context, dbName, collName string) ([]string, error) {
	return []string{}, nil
}

func (m *MockMetadataManager) GetFieldSchemas(ctx context.Context, dbName, collName string) (map[string]string, error) {
	return map[string]string{}, nil
}

func (m *MockMetadataManager) Delete(ctx context.Context, key string) error {
	if m.err != nil {
		return m.err
	}
	delete(m.data, key)
	return nil
}

func TestNewShardRouter(t *testing.T) {
	logger := logging.NewDevelopment()
	mockMeta := NewMockMetadataManager()
	cfg := config.CoordinatorConfig{
		HashThreshold:  20,
		VNodeCount:     200,
		ShardKeyFormat: "daily",
	}

	sr := NewShardRouter(logger, mockMeta, cfg)

	if sr == nil {
		t.Fatal("Expected non-nil ShardRouter")
		return
	}

	if sr.logger == nil {
		t.Error("Expected non-nil logger")
	}

	if sr.metadataManager == nil {
		t.Error("Expected non-nil metadataManager")
	}

	if sr.nodeHasher == nil {
		t.Error("Expected non-nil nodeHasher")
	}

	if sr.groupManager == nil {
		t.Error("Expected non-nil groupManager")
	}
}

func TestRouteWriteByDevice(t *testing.T) {
	logger := logging.NewDevelopment()
	mockMeta := NewMockMetadataManager()
	cfg := config.CoordinatorConfig{
		HashThreshold: 20,
		VNodeCount:    200,
		ReplicaFactor: 3,
		TotalGroups:   256,
	}

	// Add nodes to metadata before creating router
	for i := 1; i <= 3; i++ {
		nodeID := fmt.Sprintf("node%d", i)
		nodeInfo := models.NodeInfo{
			ID:     nodeID,
			Status: "active",
		}
		data, _ := json.Marshal(nodeInfo)
		mockMeta.data[fmt.Sprintf("/soltix/nodes/%s", nodeID)] = string(data)
	}

	sr := NewShardRouter(logger, mockMeta, cfg)

	ctx := context.Background()

	group, err := sr.RouteWriteByDevice(ctx, "testdb", "testcol", "device-001")
	if err != nil {
		t.Fatalf("RouteWriteByDevice failed: %v", err)
	}

	if group == nil {
		t.Fatal("Expected non-nil group assignment")
	}

	if group.PrimaryNode == "" {
		t.Error("Expected non-empty primary node")
	}

	if group.GroupID < 0 || group.GroupID >= 256 {
		t.Errorf("GroupID %d out of bounds [0, 256)", group.GroupID)
	}

	// Same device should return same group
	group2, err := sr.RouteWriteByDevice(ctx, "testdb", "testcol", "device-001")
	if err != nil {
		t.Fatalf("Second RouteWriteByDevice failed: %v", err)
	}

	if group.GroupID != group2.GroupID {
		t.Errorf("Same device should route to same group: %d vs %d", group.GroupID, group2.GroupID)
	}
}

func TestSyncNodes(t *testing.T) {
	logger := logging.NewDevelopment()
	mockMeta := NewMockMetadataManager()
	cfg := config.CoordinatorConfig{
		HashThreshold:  20,
		VNodeCount:     200,
		ShardKeyFormat: "daily",
	}

	sr := NewShardRouter(logger, mockMeta, cfg)

	// Add node info to mock metadata
	node1 := models.NodeInfo{
		ID:     "node1",
		Status: "active",
	}
	node2 := models.NodeInfo{
		ID:     "node2",
		Status: "active",
	}

	data1, _ := json.Marshal(node1)
	data2, _ := json.Marshal(node2)

	mockMeta.data["/soltix/nodes/node1"] = string(data1)
	mockMeta.data["/soltix/nodes/node2"] = string(data2)

	err := sr.syncNodes()
	if err != nil {
		t.Fatalf("syncNodes failed: %v", err)
	}

	if sr.nodeHasher.GetNodeCount() != 2 {
		t.Errorf("Expected 2 nodes in hasher, got %d", sr.nodeHasher.GetNodeCount())
	}
}

func TestSyncNodes_InactiveNodes(t *testing.T) {
	logger := logging.NewDevelopment()
	mockMeta := NewMockMetadataManager()
	cfg := config.CoordinatorConfig{
		HashThreshold:  20,
		VNodeCount:     200,
		ShardKeyFormat: "daily",
	}

	sr := NewShardRouter(logger, mockMeta, cfg)

	// Add active and inactive nodes
	activeNode := models.NodeInfo{
		ID:     "node1",
		Status: "active",
	}
	inactiveNode := models.NodeInfo{
		ID:     "node2",
		Status: "inactive",
	}

	data1, _ := json.Marshal(activeNode)
	data2, _ := json.Marshal(inactiveNode)

	mockMeta.data["/soltix/nodes/node1"] = string(data1)
	mockMeta.data["/soltix/nodes/node2"] = string(data2)

	err := sr.syncNodes()
	if err != nil {
		t.Fatalf("syncNodes failed: %v", err)
	}

	// Only active nodes should be added
	if sr.nodeHasher.GetNodeCount() != 1 {
		t.Errorf("Expected 1 active node in hasher, got %d", sr.nodeHasher.GetNodeCount())
	}
}
