package coordinator

import (
	"fmt"
	"sync"
	"testing"
)

func TestNewAdaptiveHasher(t *testing.T) {
	ah := NewAdaptiveHasher(15, 150)
	if ah == nil {
		t.Fatal("Expected non-nil AdaptiveHasher")
		return
	}
	if ah.threshold != 15 {
		t.Errorf("Expected threshold=15, got %d", ah.threshold)
	}
}

func TestAdaptiveHasher_AddNode(t *testing.T) {
	ah := NewAdaptiveHasher(20, 200)
	ah.AddNode("node1")

	if ah.GetNodeCount() != 1 {
		t.Errorf("Expected 1 node, got %d", ah.GetNodeCount())
	}
}

func TestAdaptiveHasher_GetCurrentStrategy(t *testing.T) {
	ah := NewAdaptiveHasher(20, 200)

	for i := 0; i < 15; i++ {
		ah.AddNode(string(rune('a' + i)))
	}

	strategy := ah.GetCurrentStrategy()
	if strategy != "rendezvous" {
		t.Errorf("Expected rendezvous strategy for 15 nodes, got %s", strategy)
	}

	for i := 15; i < 25; i++ {
		ah.AddNode(string(rune('a' + i)))
	}

	strategy = ah.GetCurrentStrategy()
	if strategy != "consistent" {
		t.Errorf("Expected consistent strategy for 25 nodes, got %s", strategy)
	}
}

func TestAdaptiveHasher_GetNode(t *testing.T) {
	ah := NewAdaptiveHasher(20, 200)
	ah.AddNode("node1")
	ah.AddNode("node2")
	ah.AddNode("node3")

	node := ah.GetNode("test-key")
	if node == "" {
		t.Error("Expected non-empty node")
	}

	node2 := ah.GetNode("test-key")
	if node != node2 {
		t.Error("Expected consistent node for same key")
	}
}

func TestAdaptiveHasher_Clear(t *testing.T) {
	ah := NewAdaptiveHasher(20, 200)
	ah.AddNode("node1")
	ah.Clear()

	if ah.GetNodeCount() != 0 {
		t.Errorf("Expected 0 nodes after clear, got %d", ah.GetNodeCount())
	}

	if ah.currentStrategy != "rendezvous" {
		t.Errorf("Expected strategy reset to rendezvous, got %s", ah.currentStrategy)
	}
}

func TestAdaptiveHasher_Defaults(t *testing.T) {
	ah := NewAdaptiveHasher(0, 0)
	if ah.threshold != 20 {
		t.Errorf("Expected default threshold=20, got %d", ah.threshold)
	}
	if ah.vnodeCount != 200 {
		t.Errorf("Expected default vnodeCount=200, got %d", ah.vnodeCount)
	}
}

func TestAdaptiveHasher_RemoveNode_RendezvousMode(t *testing.T) {
	ah := NewAdaptiveHasher(20, 100)
	ah.AddNode("node1")
	ah.AddNode("node2")
	ah.AddNode("node3")

	// Should be in rendezvous mode (3 < 20)
	if ah.GetCurrentStrategy() != "rendezvous" {
		t.Fatalf("Expected rendezvous, got %s", ah.GetCurrentStrategy())
	}

	ah.RemoveNode("node2")
	if ah.GetNodeCount() != 2 {
		t.Errorf("Expected 2 nodes, got %d", ah.GetNodeCount())
	}

	// Should still work
	node := ah.GetNode("key")
	if node == "" {
		t.Error("Expected non-empty node")
	}
	if node == "node2" {
		t.Error("Should not return removed node")
	}
}

func TestAdaptiveHasher_RemoveNode_ConsistentMode(t *testing.T) {
	ah := NewAdaptiveHasher(5, 50)

	// Add 10 nodes → triggers consistent mode
	for i := 0; i < 10; i++ {
		ah.AddNode(fmt.Sprintf("node-%d", i))
	}

	// Force strategy selection to switch to consistent
	if ah.GetCurrentStrategy() != "consistent" {
		t.Fatalf("Expected consistent, got %s", ah.GetCurrentStrategy())
	}

	// Now remove in consistent mode — should update both
	ah.RemoveNode("node-5")
	if ah.GetNodeCount() != 9 {
		t.Errorf("Expected 9 nodes, got %d", ah.GetNodeCount())
	}

	// Key should still resolve
	node := ah.GetNode("key")
	if node == "" {
		t.Error("Expected non-empty node")
	}
	if node == "node-5" {
		t.Error("Should not return removed node")
	}
}

func TestAdaptiveHasher_LazySync(t *testing.T) {
	ah := NewAdaptiveHasher(5, 10)

	// Add 3 nodes in rendezvous mode — consistent hash should NOT be synced
	ah.AddNode("a")
	ah.AddNode("b")
	ah.AddNode("c")

	if ah.consistentHash.GetNodeCount() != 0 {
		t.Errorf("Consistent hash should be empty in rendezvous mode, got %d",
			ah.consistentHash.GetNodeCount())
	}
	if !ah.needsSync {
		t.Error("needsSync should be true after adding in rendezvous mode")
	}

	// Now cross threshold → should trigger sync
	ah.AddNode("d")
	ah.AddNode("e")

	strategy := ah.GetCurrentStrategy()
	if strategy != "consistent" {
		t.Fatalf("Expected consistent with 5 nodes, got %s", strategy)
	}

	// After GetCurrentStrategy triggers selectStrategy, consistent hash should be synced
	if ah.consistentHash.GetNodeCount() != 5 {
		t.Errorf("Consistent hash should have 5 nodes after sync, got %d",
			ah.consistentHash.GetNodeCount())
	}
	if ah.needsSync {
		t.Error("needsSync should be false after sync")
	}
}

func TestAdaptiveHasher_LazySync_RemoveDropsBelowThreshold(t *testing.T) {
	ah := NewAdaptiveHasher(5, 10)

	// Build up to consistent mode
	for i := 0; i < 7; i++ {
		ah.AddNode(fmt.Sprintf("n%d", i))
	}
	ah.GetCurrentStrategy() // trigger sync

	if ah.GetCurrentStrategy() != "consistent" {
		t.Fatal("Should be in consistent mode")
	}

	// Remove nodes to drop below threshold
	ah.RemoveNode("n5")
	ah.RemoveNode("n6")
	ah.RemoveNode("n4")

	// Should switch back to rendezvous
	if ah.GetCurrentStrategy() != "rendezvous" {
		t.Errorf("Expected rendezvous after dropping below threshold, got %s",
			ah.GetCurrentStrategy())
	}
}

func TestAdaptiveHasher_GetNodes(t *testing.T) {
	ah := NewAdaptiveHasher(20, 100)
	ah.AddNode("node1")
	ah.AddNode("node2")
	ah.AddNode("node3")

	nodes := ah.GetNodes("key", 2)
	if len(nodes) != 2 {
		t.Errorf("Expected 2 nodes, got %d", len(nodes))
	}
	if nodes[0] == nodes[1] {
		t.Error("Expected distinct nodes")
	}
}

func TestAdaptiveHasher_GetNodes_MoreThanAvailable(t *testing.T) {
	ah := NewAdaptiveHasher(20, 100)
	ah.AddNode("node1")

	nodes := ah.GetNodes("key", 5)
	if len(nodes) != 1 {
		t.Errorf("Expected 1 node (only one available), got %d", len(nodes))
	}
}

func TestAdaptiveHasher_GetAllNodes(t *testing.T) {
	ah := NewAdaptiveHasher(20, 100)
	ah.AddNode("z-node")
	ah.AddNode("a-node")
	ah.AddNode("m-node")

	nodes := ah.GetAllNodes()
	if len(nodes) != 3 {
		t.Fatalf("Expected 3 nodes, got %d", len(nodes))
	}
}

func TestAdaptiveHasher_StrategySwitchConsistency(t *testing.T) {
	ah := NewAdaptiveHasher(5, 100)

	// Start below threshold (rendezvous)
	for i := 0; i < 4; i++ {
		ah.AddNode(fmt.Sprintf("node-%d", i))
	}

	key := "test-device"
	nodeInRendezvous := ah.GetNode(key)
	if nodeInRendezvous == "" {
		t.Fatal("Expected non-empty node")
	}

	// Cross threshold → switch to consistent
	ah.AddNode("node-4")
	ah.AddNode("node-5")

	if ah.GetCurrentStrategy() != "consistent" {
		t.Fatal("Should have switched to consistent")
	}

	nodeInConsistent := ah.GetNode(key)
	if nodeInConsistent == "" {
		t.Fatal("Expected non-empty node in consistent mode")
	}

	// Note: the node may differ between strategies — that's OK.
	// What matters is each is deterministic within its strategy.
	for i := 0; i < 10; i++ {
		if ah.GetNode(key) != nodeInConsistent {
			t.Fatal("Non-deterministic result in consistent mode")
		}
	}
}

func TestAdaptiveHasher_NeedsSyncReset(t *testing.T) {
	ah := NewAdaptiveHasher(3, 10)

	ah.AddNode("a")
	ah.AddNode("b")
	if !ah.needsSync {
		t.Error("needsSync should be true")
	}

	// Cross threshold
	ah.AddNode("c")
	_ = ah.GetCurrentStrategy() // triggers sync

	if ah.needsSync {
		t.Error("needsSync should be false after sync")
	}

	// Remove back to rendezvous
	ah.RemoveNode("c")
	if ah.GetCurrentStrategy() != "rendezvous" {
		t.Fatal("Should be rendezvous")
	}

	// Add again while in rendezvous mode — should set needsSync
	ah.AddNode("d")
	if !ah.needsSync {
		t.Error("needsSync should be true after adding in rendezvous mode")
	}
}

func TestAdaptiveHasher_Concurrent(t *testing.T) {
	ah := NewAdaptiveHasher(5, 50)
	for i := 0; i < 3; i++ {
		ah.AddNode(fmt.Sprintf("init-%d", i))
	}

	var wg sync.WaitGroup

	// Writers that push across threshold and back
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			nodeID := fmt.Sprintf("dynamic-%d", id)
			ah.AddNode(nodeID)
			_ = ah.GetCurrentStrategy()
			_ = ah.GetNode(fmt.Sprintf("key-%d", id))
			ah.RemoveNode(nodeID)
		}(i)
	}

	// Readers
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_ = ah.GetNode(fmt.Sprintf("key-%d", id))
			_ = ah.GetNodes(fmt.Sprintf("key-%d", id), 2)
			_ = ah.GetNodeCount()
			_ = ah.GetAllNodes()
		}(i)
	}

	wg.Wait()
}

func TestAdaptiveHasher_ClearResetsNeedsSync(t *testing.T) {
	ah := NewAdaptiveHasher(5, 10)
	ah.AddNode("a")
	ah.AddNode("b")
	// needsSync = true now

	ah.Clear()

	if ah.needsSync {
		t.Error("needsSync should be false after Clear")
	}
	if ah.currentStrategy != "rendezvous" {
		t.Errorf("Strategy should be rendezvous after clear, got %s", ah.currentStrategy)
	}
}
