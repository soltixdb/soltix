package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/config"
	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/models"
)

func setupAutoAssigner(t *testing.T, nodeCount int, cfg AutoAssignerConfig) (*GroupAutoAssigner, *GroupManager, *MockMetadataManager) {
	t.Helper()
	logger := logging.NewDevelopment()
	mockMeta := NewMockMetadataManager()
	coordCfg := config.CoordinatorConfig{
		HashThreshold: 20,
		VNodeCount:    200,
		ReplicaFactor: 3,
		TotalGroups:   256,
	}

	// Register nodes
	for i := 0; i < nodeCount; i++ {
		nodeID := fmt.Sprintf("storage-%d", i+1)
		nodeInfo := models.NodeInfo{
			ID:      nodeID,
			Address: fmt.Sprintf("localhost:%d", 5556+i),
			Status:  "active",
		}
		data, _ := json.Marshal(nodeInfo)
		mockMeta.data[fmt.Sprintf("/soltix/nodes/%s", nodeID)] = string(data)
	}

	gm := NewGroupManager(logger, mockMeta, coordCfg)
	assigner := NewGroupAutoAssigner(logger, mockMeta, gm, cfg)

	return assigner, gm, mockMeta
}

func TestAutoAssigner_DetectNewNodes(t *testing.T) {
	cfg := DefaultAutoAssignerConfig()
	cfg.Enabled = true
	assigner, _, _ := setupAutoAssigner(t, 3, cfg)

	// Set initial known nodes
	assigner.knownNodes = map[string]bool{
		"storage-1": true,
		"storage-2": true,
	}

	activeNodes := []string{"storage-1", "storage-2", "storage-3"}
	newNodes, removedNodes := assigner.detectChanges(activeNodes)

	if len(newNodes) != 1 || newNodes[0] != "storage-3" {
		t.Errorf("Expected [storage-3] as new nodes, got %v", newNodes)
	}
	if len(removedNodes) != 0 {
		t.Errorf("Expected no removed nodes, got %v", removedNodes)
	}
}

func TestAutoAssigner_DetectRemovedNodes(t *testing.T) {
	cfg := DefaultAutoAssignerConfig()
	assigner, _, _ := setupAutoAssigner(t, 2, cfg)

	assigner.knownNodes = map[string]bool{
		"storage-1": true,
		"storage-2": true,
		"storage-3": true,
	}

	activeNodes := []string{"storage-1", "storage-2"}
	newNodes, removedNodes := assigner.detectChanges(activeNodes)

	if len(newNodes) != 0 {
		t.Errorf("Expected no new nodes, got %v", newNodes)
	}
	if len(removedNodes) != 1 || removedNodes[0] != "storage-3" {
		t.Errorf("Expected [storage-3] as removed, got %v", removedNodes)
	}
}

func TestAutoAssigner_NodeSetKeyDeterministic(t *testing.T) {
	cfg := DefaultAutoAssignerConfig()
	assigner, _, _ := setupAutoAssigner(t, 3, cfg)

	key1 := assigner.buildNodeSetKey([]string{"storage-3", "storage-1", "storage-2"})
	key2 := assigner.buildNodeSetKey([]string{"storage-1", "storage-2", "storage-3"})

	if key1 != key2 {
		t.Errorf("Expected same key regardless of order: %s vs %s", key1, key2)
	}
}

func TestAutoAssigner_GetActiveNodes(t *testing.T) {
	cfg := DefaultAutoAssignerConfig()
	assigner, _, mockMeta := setupAutoAssigner(t, 3, cfg)

	// Add an inactive node
	inactiveNode := models.NodeInfo{
		ID:      "storage-inactive",
		Address: "localhost:9999",
		Status:  "inactive",
	}
	data, _ := json.Marshal(inactiveNode)
	mockMeta.data["/soltix/nodes/storage-inactive"] = string(data)

	ctx := context.Background()
	nodes, err := assigner.getActiveNodes(ctx)
	if err != nil {
		t.Fatalf("getActiveNodes failed: %v", err)
	}

	// Should only have 3 active nodes, not the inactive one
	if len(nodes) != 3 {
		t.Errorf("Expected 3 active nodes, got %d: %v", len(nodes), nodes)
	}

	for _, n := range nodes {
		if n == "storage-inactive" {
			t.Error("Inactive node should not be in active list")
		}
	}
}

func TestAutoAssigner_FullAssignment_NoGroups(t *testing.T) {
	cfg := DefaultAutoAssignerConfig()
	assigner, _, _ := setupAutoAssigner(t, 3, cfg)

	ctx := context.Background()
	activeNodes := []string{"storage-1", "storage-2", "storage-3"}

	err := assigner.fullAssignment(ctx, activeNodes)
	if err != nil {
		t.Fatalf("fullAssignment failed: %v", err)
	}
	// No groups exist yet, should not error
}

func TestAutoAssigner_FullAssignment_WithOrphans(t *testing.T) {
	cfg := DefaultAutoAssignerConfig()
	assigner, gm, _ := setupAutoAssigner(t, 3, cfg)

	ctx := context.Background()

	// Create a group with a dead primary node
	group := &GroupAssignment{
		GroupID:      42,
		PrimaryNode:  "storage-dead",
		ReplicaNodes: []string{"storage-1"},
		State:        GroupStateActive,
		Epoch:        1,
		UpdatedAt:    time.Now(),
	}
	if err := gm.saveGroup(ctx, group); err != nil {
		t.Fatalf("Failed to save group: %v", err)
	}

	activeNodes := []string{"storage-1", "storage-2", "storage-3"}
	err := assigner.fullAssignment(ctx, activeNodes)
	if err != nil {
		t.Fatalf("fullAssignment failed: %v", err)
	}

	// Check that the orphaned group got a new primary
	updatedGroup, err := gm.GetGroup(ctx, 42)
	if err != nil {
		t.Fatalf("Failed to get group: %v", err)
	}

	// Primary should be promoted from replica (storage-1) since it was the active replica
	if updatedGroup.PrimaryNode == "storage-dead" {
		t.Error("Orphaned group should have a new primary, still has dead node")
	}

	if !assigner.isNodeActive(updatedGroup.PrimaryNode, activeNodes) {
		t.Errorf("New primary %s should be an active node", updatedGroup.PrimaryNode)
	}
}

func TestAutoAssigner_RebalanceForNewNodes(t *testing.T) {
	cfg := DefaultAutoAssignerConfig()
	cfg.RebalanceThreshold = 5
	assigner, gm, _ := setupAutoAssigner(t, 3, cfg)

	ctx := context.Background()

	// Create some groups assigned only to storage-1 and storage-2 (replica factor=2)
	for i := 0; i < 5; i++ {
		group := &GroupAssignment{
			GroupID:      i,
			PrimaryNode:  "storage-1",
			ReplicaNodes: []string{"storage-2"},
			State:        GroupStateActive,
			Epoch:        1,
			UpdatedAt:    time.Now(),
		}
		if err := gm.saveGroup(ctx, group); err != nil {
			t.Fatalf("Failed to save group: %v", err)
		}
	}

	// storage-3 is a new node
	activeNodes := []string{"storage-1", "storage-2", "storage-3"}
	newNodes := []string{"storage-3"}

	err := assigner.rebalanceForNewNodes(ctx, activeNodes, newNodes)
	if err != nil {
		t.Fatalf("rebalanceForNewNodes failed: %v", err)
	}

	// storage-3 should now be in some groups (groups need 3 replicas, currently only have 2)
	groupsForNewNode := 0
	groups, _ := gm.ListGroups(ctx)
	for _, g := range groups {
		for _, n := range g.GetAllNodes() {
			if n == "storage-3" {
				groupsForNewNode++
				break
			}
		}
	}

	if groupsForNewNode == 0 {
		t.Error("New node storage-3 should have been assigned to at least one group")
	}
}

func TestAutoAssigner_ReassignFromRemovedNodes(t *testing.T) {
	cfg := DefaultAutoAssignerConfig()
	assigner, gm, _ := setupAutoAssigner(t, 3, cfg)

	ctx := context.Background()

	// Create a group with storage-3 as primary
	group := &GroupAssignment{
		GroupID:      10,
		PrimaryNode:  "storage-3",
		ReplicaNodes: []string{"storage-1", "storage-2"},
		State:        GroupStateActive,
		Epoch:        1,
		UpdatedAt:    time.Now(),
	}
	if err := gm.saveGroup(ctx, group); err != nil {
		t.Fatalf("Failed to save group: %v", err)
	}

	// storage-3 goes down
	activeNodes := []string{"storage-1", "storage-2"}
	removedNodes := []string{"storage-3"}

	err := assigner.reassignFromRemovedNodes(ctx, activeNodes, removedNodes)
	if err != nil {
		t.Fatalf("reassignFromRemovedNodes failed: %v", err)
	}

	// Group should no longer have storage-3
	updatedGroup, err := gm.GetGroup(ctx, 10)
	if err != nil {
		t.Fatalf("Failed to get group: %v", err)
	}

	for _, n := range updatedGroup.GetAllNodes() {
		if n == "storage-3" {
			t.Error("Removed node storage-3 should not be in the group anymore")
		}
	}

	// Group should still have a primary
	if updatedGroup.PrimaryNode == "" {
		t.Error("Group should still have a primary node")
	}
	if updatedGroup.PrimaryNode != "storage-1" && updatedGroup.PrimaryNode != "storage-2" {
		t.Errorf("Primary should be an active node, got %s", updatedGroup.PrimaryNode)
	}
}

func TestAutoAssigner_EnsureReplicaCoverage(t *testing.T) {
	cfg := DefaultAutoAssignerConfig()
	assigner, gm, _ := setupAutoAssigner(t, 3, cfg)

	ctx := context.Background()

	// Create a group with only 1 node (under replica factor of 3)
	group := &GroupAssignment{
		GroupID:      20,
		PrimaryNode:  "storage-1",
		ReplicaNodes: nil,
		State:        GroupStateActive,
		Epoch:        1,
		UpdatedAt:    time.Now(),
	}
	if err := gm.saveGroup(ctx, group); err != nil {
		t.Fatalf("Failed to save group: %v", err)
	}

	groups, _ := gm.ListGroups(ctx)
	activeNodes := []string{"storage-1", "storage-2", "storage-3"}

	err := assigner.ensureReplicaCoverage(ctx, groups, activeNodes)
	if err != nil {
		t.Fatalf("ensureReplicaCoverage failed: %v", err)
	}

	updatedGroup, err := gm.GetGroup(ctx, 20)
	if err != nil {
		t.Fatalf("Failed to get group: %v", err)
	}

	nodeCount := len(updatedGroup.GetAllNodes())
	if nodeCount != 3 {
		t.Errorf("Expected 3 nodes in group (replica factor), got %d: %v",
			nodeCount, updatedGroup.GetAllNodes())
	}
}

func TestAutoAssigner_CountGroupsPerNode(t *testing.T) {
	cfg := DefaultAutoAssignerConfig()
	assigner, _, _ := setupAutoAssigner(t, 3, cfg)

	groups := []*GroupAssignment{
		{GroupID: 1, PrimaryNode: "storage-1", ReplicaNodes: []string{"storage-2", "storage-3"}},
		{GroupID: 2, PrimaryNode: "storage-2", ReplicaNodes: []string{"storage-1"}},
		{GroupID: 3, PrimaryNode: "storage-3", ReplicaNodes: []string{"storage-1", "storage-2"}},
	}

	counts := assigner.countGroupsPerNode(groups)

	if counts["storage-1"] != 3 {
		t.Errorf("storage-1 expected 3 groups, got %d", counts["storage-1"])
	}
	if counts["storage-2"] != 3 {
		t.Errorf("storage-2 expected 3 groups, got %d", counts["storage-2"])
	}
	if counts["storage-3"] != 2 {
		t.Errorf("storage-3 expected 2 groups, got %d", counts["storage-3"])
	}
}

func TestAutoAssigner_FindLeastLoaded(t *testing.T) {
	cfg := DefaultAutoAssignerConfig()
	assigner, _, _ := setupAutoAssigner(t, 3, cfg)

	group := &GroupAssignment{
		GroupID:      1,
		PrimaryNode:  "storage-1",
		ReplicaNodes: []string{"storage-2"},
	}

	nodeGroupCount := map[string]int{
		"storage-1": 10,
		"storage-2": 5,
		"storage-3": 2,
	}

	least := assigner.findLeastLoaded(
		[]string{"storage-1", "storage-2", "storage-3"},
		group,
		nodeGroupCount,
	)

	// storage-3 is not in the group and has least load
	if least != "storage-3" {
		t.Errorf("Expected storage-3 as least loaded (not in group), got %s", least)
	}
}

func TestAutoAssigner_CheckAndAssign_NoChange(t *testing.T) {
	cfg := DefaultAutoAssignerConfig()
	assigner, _, _ := setupAutoAssigner(t, 3, cfg)

	ctx := context.Background()

	// First call should detect changes (initial)
	err := assigner.checkAndAssign(ctx)
	if err != nil {
		t.Fatalf("First checkAndAssign failed: %v", err)
	}

	// Second call with no changes should be a no-op
	err = assigner.checkAndAssign(ctx)
	if err != nil {
		t.Fatalf("Second checkAndAssign failed: %v", err)
	}
}

func TestAutoAssigner_CheckAndAssign_NodeAdded(t *testing.T) {
	cfg := DefaultAutoAssignerConfig()
	cfg.RebalanceOnJoin = true
	assigner, gm, mockMeta := setupAutoAssigner(t, 2, cfg)

	ctx := context.Background()

	// Create some groups
	for i := 0; i < 3; i++ {
		group := &GroupAssignment{
			GroupID:      i,
			PrimaryNode:  "storage-1",
			ReplicaNodes: []string{"storage-2"},
			State:        GroupStateActive,
			Epoch:        1,
			UpdatedAt:    time.Now(),
		}
		_ = gm.saveGroup(ctx, group)
	}

	// First check
	err := assigner.checkAndAssign(ctx)
	if err != nil {
		t.Fatalf("First checkAndAssign failed: %v", err)
	}

	// Add a new node
	nodeInfo := models.NodeInfo{
		ID:      "storage-3",
		Address: "localhost:5558",
		Status:  "active",
	}
	data, _ := json.Marshal(nodeInfo)
	mockMeta.data["/soltix/nodes/storage-3"] = string(data)

	// Sync new node into hasher
	gm.nodeHasher.AddNode("storage-3")

	// Second check - should detect new node and rebalance
	err = assigner.checkAndAssign(ctx)
	if err != nil {
		t.Fatalf("Second checkAndAssign (after add) failed: %v", err)
	}

	// Verify storage-3 got some groups
	groups, _ := gm.ListGroups(ctx)
	found := false
	for _, g := range groups {
		for _, n := range g.GetAllNodes() {
			if n == "storage-3" {
				found = true
				break
			}
		}
		if found {
			break
		}
	}

	if !found {
		t.Error("New node storage-3 should have been assigned to at least one group after rebalance")
	}
}

func TestAutoAssigner_Disabled(t *testing.T) {
	cfg := DefaultAutoAssignerConfig()
	cfg.Enabled = false
	assigner, _, _ := setupAutoAssigner(t, 3, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start should be a no-op when disabled
	assigner.Start(ctx)

	// Stop should not panic
	assigner.Stop()
}

func TestGroupAssignment_GetAllNodes(t *testing.T) {
	g := &GroupAssignment{
		PrimaryNode:  "primary",
		ReplicaNodes: []string{"replica1", "replica2"},
	}

	nodes := g.GetAllNodes()
	if len(nodes) != 3 {
		t.Fatalf("Expected 3 nodes, got %d", len(nodes))
	}
	if nodes[0] != "primary" {
		t.Errorf("First node should be primary, got %q", nodes[0])
	}
}

func TestGroupAssignment_GetAllNodes_NoReplicas(t *testing.T) {
	g := &GroupAssignment{
		PrimaryNode:  "only",
		ReplicaNodes: nil,
	}

	nodes := g.GetAllNodes()
	if len(nodes) != 1 {
		t.Fatalf("Expected 1 node, got %d", len(nodes))
	}
	if nodes[0] != "only" {
		t.Errorf("Expected 'only', got %q", nodes[0])
	}
}

// helper
func containsSubstr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
