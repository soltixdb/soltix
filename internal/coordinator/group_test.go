package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/soltixdb/soltix/internal/config"
	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/models"
)

func setupGroupManager(t *testing.T, nodeCount int) (*GroupManager, *MockMetadataManager) {
	t.Helper()
	logger := logging.NewDevelopment()
	mockMeta := NewMockMetadataManager()
	cfg := config.CoordinatorConfig{
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

	gm := NewGroupManager(logger, mockMeta, cfg)
	return gm, mockMeta
}

func TestCalculateGroupID_Deterministic(t *testing.T) {
	gm, _ := setupGroupManager(t, 3)

	// Same input should always produce the same group ID
	id1 := gm.CalculateGroupID("mydb", "panels", "device-001")
	id2 := gm.CalculateGroupID("mydb", "panels", "device-001")

	if id1 != id2 {
		t.Errorf("Expected same group ID for same input, got %d and %d", id1, id2)
	}
}

func TestCalculateGroupID_DifferentDevices(t *testing.T) {
	gm, _ := setupGroupManager(t, 3)

	groups := make(map[int]int)
	deviceCount := 1000

	for i := 0; i < deviceCount; i++ {
		deviceID := fmt.Sprintf("device-%04d", i)
		groupID := gm.CalculateGroupID("mydb", "panels", deviceID)
		groups[groupID]++
	}

	// Should distribute across multiple groups
	if len(groups) < 10 {
		t.Errorf("Expected devices to spread across many groups, only got %d groups", len(groups))
	}

	t.Logf("Distributed %d devices across %d groups", deviceCount, len(groups))
}

func TestCalculateGroupID_BoundedByTotalGroups(t *testing.T) {
	gm, _ := setupGroupManager(t, 3)

	totalGroups := gm.cfg.TotalGroups
	for i := 0; i < 1000; i++ {
		groupID := gm.CalculateGroupID("db", "col", fmt.Sprintf("dev-%d", i))
		if groupID < 0 || groupID >= totalGroups {
			t.Errorf("Group ID %d out of bounds [0, %d)", groupID, totalGroups)
		}
	}
}

func TestRouteWrite_CreatesGroup(t *testing.T) {
	gm, _ := setupGroupManager(t, 3)
	ctx := context.Background()

	group, err := gm.RouteWrite(ctx, "mydb", "panels", "device-001")
	if err != nil {
		t.Fatalf("RouteWrite failed: %v", err)
	}

	if group == nil {
		t.Fatal("Expected non-nil group")
	}

	if group.PrimaryNode == "" {
		t.Error("Expected non-empty primary node")
	}

	if group.State != GroupStateActive {
		t.Errorf("Expected state=active, got %s", group.State)
	}

	t.Logf("Group %d: primary=%s, replicas=%v", group.GroupID, group.PrimaryNode, group.ReplicaNodes)
}

func TestRouteWrite_SameDeviceReturnsSameGroup(t *testing.T) {
	gm, _ := setupGroupManager(t, 3)
	ctx := context.Background()

	group1, err := gm.RouteWrite(ctx, "mydb", "panels", "device-001")
	if err != nil {
		t.Fatalf("First RouteWrite failed: %v", err)
	}

	group2, err := gm.RouteWrite(ctx, "mydb", "panels", "device-001")
	if err != nil {
		t.Fatalf("Second RouteWrite failed: %v", err)
	}

	if group1.GroupID != group2.GroupID {
		t.Errorf("Same device should route to same group: %d vs %d", group1.GroupID, group2.GroupID)
	}

	if group1.PrimaryNode != group2.PrimaryNode {
		t.Errorf("Same group should have same primary: %s vs %s", group1.PrimaryNode, group2.PrimaryNode)
	}
}

func TestRouteWrite_DifferentDevicesMayDifferentGroups(t *testing.T) {
	gm, _ := setupGroupManager(t, 5)
	ctx := context.Background()

	groups := make(map[int]bool)
	for i := 0; i < 100; i++ {
		group, err := gm.RouteWrite(ctx, "mydb", "panels", fmt.Sprintf("device-%03d", i))
		if err != nil {
			t.Fatalf("RouteWrite failed for device-%03d: %v", i, err)
		}
		groups[group.GroupID] = true
	}

	if len(groups) < 2 {
		t.Errorf("Expected multiple groups for 100 devices, got %d", len(groups))
	}
	t.Logf("100 devices distributed to %d groups", len(groups))
}

func TestAddNodeToGroup(t *testing.T) {
	gm, mockMeta := setupGroupManager(t, 5)
	ctx := context.Background()

	// Create a group first
	group, err := gm.RouteWrite(ctx, "mydb", "panels", "device-001")
	if err != nil {
		t.Fatalf("RouteWrite failed: %v", err)
	}

	initialNodeCount := len(group.GetAllNodes())

	// Add a new node not already in the group
	existingNodes := make(map[string]bool)
	for _, n := range group.GetAllNodes() {
		existingNodes[n] = true
	}

	var newNodeID string
	for i := 1; i <= 5; i++ {
		nid := fmt.Sprintf("storage-%d", i)
		if !existingNodes[nid] {
			newNodeID = nid
			break
		}
	}

	if newNodeID == "" {
		t.Skip("All nodes already in group")
	}

	// Register the new node in metadata
	nodeInfo := models.NodeInfo{ID: newNodeID, Status: "active"}
	data, _ := json.Marshal(nodeInfo)
	mockMeta.data[fmt.Sprintf("/soltix/nodes/%s", newNodeID)] = string(data)

	updated, err := gm.AddNodeToGroup(ctx, group.GroupID, newNodeID)
	if err != nil {
		t.Fatalf("AddNodeToGroup failed: %v", err)
	}

	if len(updated.GetAllNodes()) != initialNodeCount+1 {
		t.Errorf("Expected %d nodes after add, got %d", initialNodeCount+1, len(updated.GetAllNodes()))
	}

	if updated.State != GroupStateSyncing {
		t.Errorf("Expected state=syncing after add, got %s", updated.State)
	}
}

func TestRemoveNodeFromGroup(t *testing.T) {
	gm, _ := setupGroupManager(t, 5)
	ctx := context.Background()

	group, err := gm.RouteWrite(ctx, "mydb", "panels", "device-001")
	if err != nil {
		t.Fatalf("RouteWrite failed: %v", err)
	}

	if len(group.ReplicaNodes) == 0 {
		t.Skip("No replica nodes to remove")
	}

	nodeToRemove := group.ReplicaNodes[0]
	initialCount := len(group.GetAllNodes())

	updated, err := gm.RemoveNodeFromGroup(ctx, group.GroupID, nodeToRemove)
	if err != nil {
		t.Fatalf("RemoveNodeFromGroup failed: %v", err)
	}

	if len(updated.GetAllNodes()) != initialCount-1 {
		t.Errorf("Expected %d nodes after remove, got %d", initialCount-1, len(updated.GetAllNodes()))
	}

	// Verify node is not in the group anymore
	for _, n := range updated.GetAllNodes() {
		if n == nodeToRemove {
			t.Errorf("Node %s should have been removed", nodeToRemove)
		}
	}
}

func TestRemoveNodeFromGroup_PromotesPrimary(t *testing.T) {
	gm, _ := setupGroupManager(t, 5)
	ctx := context.Background()

	group, err := gm.RouteWrite(ctx, "mydb", "panels", "device-001")
	if err != nil {
		t.Fatalf("RouteWrite failed: %v", err)
	}

	if len(group.ReplicaNodes) == 0 {
		t.Skip("No replica nodes, can't test primary promotion")
	}

	oldPrimary := group.PrimaryNode
	expectedNewPrimary := group.ReplicaNodes[0]

	updated, err := gm.RemoveNodeFromGroup(ctx, group.GroupID, oldPrimary)
	if err != nil {
		t.Fatalf("RemoveNodeFromGroup failed: %v", err)
	}

	if updated.PrimaryNode != expectedNewPrimary {
		t.Errorf("Expected new primary=%s, got %s", expectedNewPrimary, updated.PrimaryNode)
	}

	// Old primary should not be in the group
	for _, n := range updated.GetAllNodes() {
		if n == oldPrimary {
			t.Errorf("Old primary %s should have been removed", oldPrimary)
		}
	}
}

func TestAddNodeToGroup_DuplicateError(t *testing.T) {
	gm, _ := setupGroupManager(t, 3)
	ctx := context.Background()

	group, err := gm.RouteWrite(ctx, "mydb", "panels", "device-001")
	if err != nil {
		t.Fatalf("RouteWrite failed: %v", err)
	}

	// Try to add the primary node again
	_, err = gm.AddNodeToGroup(ctx, group.GroupID, group.PrimaryNode)
	if err == nil {
		t.Error("Expected error when adding duplicate node")
	}
}

func TestSetGroupActive(t *testing.T) {
	gm, _ := setupGroupManager(t, 5)
	ctx := context.Background()

	group, err := gm.RouteWrite(ctx, "mydb", "panels", "device-001")
	if err != nil {
		t.Fatalf("RouteWrite failed: %v", err)
	}

	// Find a new node to add
	existingNodes := make(map[string]bool)
	for _, n := range group.GetAllNodes() {
		existingNodes[n] = true
	}
	var newNodeID string
	for i := 1; i <= 5; i++ {
		nid := fmt.Sprintf("storage-%d", i)
		if !existingNodes[nid] {
			newNodeID = nid
			break
		}
	}
	if newNodeID == "" {
		t.Skip("No available nodes")
	}

	// Add node (sets state to syncing)
	_, err = gm.AddNodeToGroup(ctx, group.GroupID, newNodeID)
	if err != nil {
		t.Fatalf("AddNodeToGroup failed: %v", err)
	}

	// Activate
	err = gm.SetGroupActive(ctx, group.GroupID)
	if err != nil {
		t.Fatalf("SetGroupActive failed: %v", err)
	}

	// Verify
	updated, err := gm.GetGroup(ctx, group.GroupID)
	if err != nil {
		t.Fatalf("GetGroup failed: %v", err)
	}
	if updated.State != GroupStateActive {
		t.Errorf("Expected state=active, got %s", updated.State)
	}
}

func TestUpdateReplicaFactor_Increase(t *testing.T) {
	gm, _ := setupGroupManager(t, 5)
	ctx := context.Background()

	group, err := gm.RouteWrite(ctx, "mydb", "panels", "device-001")
	if err != nil {
		t.Fatalf("RouteWrite failed: %v", err)
	}

	currentCount := len(group.GetAllNodes())
	newFactor := currentCount + 1

	if newFactor > 5 {
		t.Skip("Not enough nodes to increase replica factor")
	}

	updated, err := gm.UpdateReplicaFactor(ctx, group.GroupID, newFactor)
	if err != nil {
		t.Fatalf("UpdateReplicaFactor failed: %v", err)
	}

	if len(updated.GetAllNodes()) != newFactor {
		t.Errorf("Expected %d nodes, got %d", newFactor, len(updated.GetAllNodes()))
	}
}

func TestUpdateReplicaFactor_Decrease(t *testing.T) {
	gm, _ := setupGroupManager(t, 5)
	ctx := context.Background()

	group, err := gm.RouteWrite(ctx, "mydb", "panels", "device-001")
	if err != nil {
		t.Fatalf("RouteWrite failed: %v", err)
	}

	if len(group.GetAllNodes()) < 2 {
		t.Skip("Need at least 2 nodes to test decrease")
	}

	updated, err := gm.UpdateReplicaFactor(ctx, group.GroupID, 1)
	if err != nil {
		t.Fatalf("UpdateReplicaFactor failed: %v", err)
	}

	if len(updated.GetAllNodes()) != 1 {
		t.Errorf("Expected 1 node, got %d", len(updated.GetAllNodes()))
	}

	// Primary should be preserved
	if updated.PrimaryNode == "" {
		t.Error("Expected primary node to be preserved")
	}
}

func TestListGroups(t *testing.T) {
	gm, _ := setupGroupManager(t, 3)
	ctx := context.Background()

	// Create a few groups
	for i := 0; i < 10; i++ {
		_, err := gm.RouteWrite(ctx, "mydb", "panels", fmt.Sprintf("device-%03d", i))
		if err != nil {
			t.Fatalf("RouteWrite failed: %v", err)
		}
	}

	groups, err := gm.ListGroups(ctx)
	if err != nil {
		t.Fatalf("ListGroups failed: %v", err)
	}

	if len(groups) == 0 {
		t.Error("Expected at least one group")
	}

	// Groups should be sorted by ID
	for i := 1; i < len(groups); i++ {
		if groups[i].GroupID < groups[i-1].GroupID {
			t.Error("Groups should be sorted by GroupID")
		}
	}

	t.Logf("Created %d groups for 10 devices", len(groups))
}

func TestGetGroupsForNode(t *testing.T) {
	gm, _ := setupGroupManager(t, 3)
	ctx := context.Background()

	// Create groups
	for i := 0; i < 20; i++ {
		_, err := gm.RouteWrite(ctx, "mydb", "panels", fmt.Sprintf("device-%03d", i))
		if err != nil {
			t.Fatalf("RouteWrite failed: %v", err)
		}
	}

	// Check which groups node "storage-1" belongs to
	groups, err := gm.GetGroupsForNode(ctx, "storage-1")
	if err != nil {
		t.Fatalf("GetGroupsForNode failed: %v", err)
	}

	if len(groups) == 0 {
		t.Error("Expected storage-1 to be part of at least one group")
	}

	t.Logf("storage-1 is part of %d groups", len(groups))
}

func TestGroupRouteWrite_NoNodes(t *testing.T) {
	logger := logging.NewDevelopment()
	mockMeta := NewMockMetadataManager()
	cfg := config.CoordinatorConfig{
		HashThreshold: 20,
		VNodeCount:    200,
		ReplicaFactor: 3,
		TotalGroups:   256,
	}

	gm := NewGroupManager(logger, mockMeta, cfg)
	ctx := context.Background()

	_, err := gm.RouteWrite(ctx, "mydb", "panels", "device-001")
	if err == nil {
		t.Error("Expected error when no nodes available")
	}
}

func TestGroupEpochIncrementsOnChange(t *testing.T) {
	gm, _ := setupGroupManager(t, 5)
	ctx := context.Background()

	group, err := gm.RouteWrite(ctx, "mydb", "panels", "device-001")
	if err != nil {
		t.Fatalf("RouteWrite failed: %v", err)
	}

	initialEpoch := group.Epoch

	// Find a new node
	existingNodes := make(map[string]bool)
	for _, n := range group.GetAllNodes() {
		existingNodes[n] = true
	}
	var newNodeID string
	for i := 1; i <= 5; i++ {
		nid := fmt.Sprintf("storage-%d", i)
		if !existingNodes[nid] {
			newNodeID = nid
			break
		}
	}
	if newNodeID == "" {
		t.Skip("No available nodes")
	}

	updated, err := gm.AddNodeToGroup(ctx, group.GroupID, newNodeID)
	if err != nil {
		t.Fatalf("AddNodeToGroup failed: %v", err)
	}

	if updated.Epoch <= initialEpoch {
		t.Errorf("Expected epoch to increase: %d -> %d", initialEpoch, updated.Epoch)
	}
}
