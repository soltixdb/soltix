package coordinator

import (
	"fmt"
	"sort"
	"sync"
	"testing"
)

func TestNewConsistentHash(t *testing.T) {
	ch := NewConsistentHash(100)
	if ch == nil {
		t.Fatal("Expected non-nil ConsistentHash")
		return
	}
	if ch.vnodeCount != 100 {
		t.Errorf("Expected vnodeCount=100, got %d", ch.vnodeCount)
	}

	chDefault := NewConsistentHash(0)
	if chDefault.vnodeCount != 200 {
		t.Errorf("Expected default vnodeCount=200, got %d", chDefault.vnodeCount)
	}
}

func TestConsistentHash_AddNode(t *testing.T) {
	ch := NewConsistentHash(10)
	ch.AddNode("node1")

	if ch.GetNodeCount() != 1 {
		t.Errorf("Expected 1 node, got %d", ch.GetNodeCount())
	}

	if len(ch.ring) != 10 {
		t.Errorf("Expected 10 vnodes, got %d", len(ch.ring))
	}

	// Add duplicate
	ch.AddNode("node1")
	if ch.GetNodeCount() != 1 {
		t.Errorf("Expected 1 node after duplicate, got %d", ch.GetNodeCount())
	}
}

func TestConsistentHash_RemoveNode(t *testing.T) {
	ch := NewConsistentHash(10)
	ch.AddNode("node1")
	ch.AddNode("node2")
	ch.RemoveNode("node1")

	if ch.GetNodeCount() != 1 {
		t.Errorf("Expected 1 node, got %d", ch.GetNodeCount())
	}
}

func TestConsistentHash_GetNode(t *testing.T) {
	ch := NewConsistentHash(100)
	ch.AddNode("node1")
	ch.AddNode("node2")
	ch.AddNode("node3")

	node := ch.GetNode("test-key")
	if node == "" {
		t.Error("Expected non-empty node")
	}

	node2 := ch.GetNode("test-key")
	if node != node2 {
		t.Errorf("Expected consistent node for same key")
	}
}

func TestConsistentHash_GetNodes(t *testing.T) {
	ch := NewConsistentHash(100)
	ch.AddNode("node1")
	ch.AddNode("node2")
	ch.AddNode("node3")

	nodes := ch.GetNodes("test-key", 2)
	if len(nodes) != 2 {
		t.Errorf("Expected 2 nodes, got %d", len(nodes))
	}

	if nodes[0] == nodes[1] {
		t.Error("Expected distinct nodes")
	}
}

func TestConsistentHash_Clear(t *testing.T) {
	ch := NewConsistentHash(10)
	ch.AddNode("node1")
	ch.AddNode("node2")
	ch.Clear()

	if ch.GetNodeCount() != 0 {
		t.Errorf("Expected 0 nodes after clear, got %d", ch.GetNodeCount())
	}
}

func TestConsistentHash_GetNode_EmptyRing(t *testing.T) {
	ch := NewConsistentHash(10)
	node := ch.GetNode("any-key")
	if node != "" {
		t.Errorf("Expected empty string from empty ring, got %q", node)
	}
}

func TestConsistentHash_GetNodes_EmptyRing(t *testing.T) {
	ch := NewConsistentHash(10)
	nodes := ch.GetNodes("any-key", 3)
	if len(nodes) != 0 {
		t.Errorf("Expected 0 nodes from empty ring, got %d", len(nodes))
	}
}

func TestConsistentHash_RemoveNode_NonExistent(t *testing.T) {
	ch := NewConsistentHash(10)
	ch.AddNode("node1")
	// Should not panic or change state
	ch.RemoveNode("node-does-not-exist")
	if ch.GetNodeCount() != 1 {
		t.Errorf("Expected 1 node after removing non-existent, got %d", ch.GetNodeCount())
	}
}

func TestConsistentHash_RemoveNode_AllNodes(t *testing.T) {
	ch := NewConsistentHash(10)
	ch.AddNode("node1")
	ch.AddNode("node2")
	ch.AddNode("node3")
	ch.RemoveNode("node1")
	ch.RemoveNode("node2")
	ch.RemoveNode("node3")

	if ch.GetNodeCount() != 0 {
		t.Errorf("Expected 0 nodes, got %d", ch.GetNodeCount())
	}
	if len(ch.ring) != 0 {
		t.Errorf("Expected empty ring, got %d entries", len(ch.ring))
	}
	if len(ch.vnodeMap) != 0 {
		t.Errorf("Expected empty vnodeMap, got %d entries", len(ch.vnodeMap))
	}
	if len(ch.nodeVnodes) != 0 {
		t.Errorf("Expected empty nodeVnodes, got %d entries", len(ch.nodeVnodes))
	}
	// GetNode on empty ring after removal
	if node := ch.GetNode("key"); node != "" {
		t.Errorf("Expected empty from emptied ring, got %q", node)
	}
}

func TestConsistentHash_GetAllNodes(t *testing.T) {
	ch := NewConsistentHash(10)
	ch.AddNode("node-a")
	ch.AddNode("node-b")
	ch.AddNode("node-c")

	nodes := ch.GetAllNodes()
	if len(nodes) != 3 {
		t.Fatalf("Expected 3 nodes, got %d", len(nodes))
	}

	// Should contain all three
	nodeSet := make(map[string]bool)
	for _, n := range nodes {
		nodeSet[n] = true
	}
	for _, expected := range []string{"node-a", "node-b", "node-c"} {
		if !nodeSet[expected] {
			t.Errorf("GetAllNodes missing %q", expected)
		}
	}
}

func TestConsistentHash_RingSortedInvariant(t *testing.T) {
	ch := NewConsistentHash(50)

	// Add multiple nodes
	for i := 0; i < 10; i++ {
		ch.AddNode(fmt.Sprintf("node-%d", i))
	}

	// Ring must be sorted after adds
	if !sort.SliceIsSorted(ch.ring, func(i, j int) bool {
		return ch.ring[i] < ch.ring[j]
	}) {
		t.Error("Ring is not sorted after AddNode")
	}

	// Remove some nodes
	ch.RemoveNode("node-3")
	ch.RemoveNode("node-7")

	// Ring must still be sorted after removes
	if !sort.SliceIsSorted(ch.ring, func(i, j int) bool {
		return ch.ring[i] < ch.ring[j]
	}) {
		t.Error("Ring is not sorted after RemoveNode")
	}

	// Verify ring size: (10 - 2) * 50 = 400
	if len(ch.ring) != 8*50 {
		t.Errorf("Expected ring size %d, got %d", 8*50, len(ch.ring))
	}
}

func TestConsistentHash_NodeVnodesConsistency(t *testing.T) {
	ch := NewConsistentHash(20)
	ch.AddNode("alpha")
	ch.AddNode("beta")

	// Each node should have exactly vnodeCount vnodes tracked
	for _, nodeID := range []string{"alpha", "beta"} {
		vnodes := ch.nodeVnodes[nodeID]
		if len(vnodes) != 20 {
			t.Errorf("Node %q has %d vnodes tracked, expected 20", nodeID, len(vnodes))
		}
		// Each vnode should map back to this node
		for _, vh := range vnodes {
			if ch.vnodeMap[vh] != nodeID {
				t.Errorf("Vnode %d maps to %q, expected %q", vh, ch.vnodeMap[vh], nodeID)
			}
		}
	}

	ch.RemoveNode("alpha")
	if _, exists := ch.nodeVnodes["alpha"]; exists {
		t.Error("nodeVnodes should not contain removed node")
	}
	// beta should still be intact
	if len(ch.nodeVnodes["beta"]) != 20 {
		t.Errorf("beta vnodes corrupted after removing alpha")
	}
}

func TestConsistentHash_GetNodes_MoreThanAvailable(t *testing.T) {
	ch := NewConsistentHash(10)
	ch.AddNode("node1")
	ch.AddNode("node2")

	// Request 5 but only 2 physical nodes exist
	nodes := ch.GetNodes("key", 5)
	if len(nodes) != 2 {
		t.Errorf("Expected 2 nodes (all available), got %d", len(nodes))
	}
}

func TestConsistentHash_GetNodes_SingleNode(t *testing.T) {
	ch := NewConsistentHash(10)
	ch.AddNode("solo")

	nodes := ch.GetNodes("key", 3)
	if len(nodes) != 1 {
		t.Errorf("Expected 1 node (only one exists), got %d", len(nodes))
	}
	if nodes[0] != "solo" {
		t.Errorf("Expected 'solo', got %q", nodes[0])
	}
}

func TestConsistentHash_Consistency_AfterRemoveAdd(t *testing.T) {
	ch := NewConsistentHash(100)
	ch.AddNode("node1")
	ch.AddNode("node2")
	ch.AddNode("node3")

	// Record assignments for keys
	keys := make([]string, 100)
	for i := range keys {
		keys[i] = fmt.Sprintf("device-%d", i)
	}

	before := make(map[string]string)
	for _, k := range keys {
		before[k] = ch.GetNode(k)
	}

	// Remove node2 and add it back
	ch.RemoveNode("node2")
	ch.AddNode("node2")

	// Most keys should map to the same node (consistent hashing property)
	sameCount := 0
	for _, k := range keys {
		if ch.GetNode(k) == before[k] {
			sameCount++
		}
	}

	// At least 60% should stay the same (conservative threshold for vnodes)
	if sameCount < 60 {
		t.Errorf("Only %d/100 keys remained consistent after remove+add, expected ≥60", sameCount)
	}
}

func TestConsistentHash_ClearThenReuse(t *testing.T) {
	ch := NewConsistentHash(10)
	ch.AddNode("node1")
	ch.AddNode("node2")

	ch.Clear()

	if ch.GetNodeCount() != 0 {
		t.Errorf("Expected 0 after clear, got %d", ch.GetNodeCount())
	}
	if len(ch.ring) != 0 {
		t.Errorf("Expected empty ring after clear, got %d", len(ch.ring))
	}
	if len(ch.nodeVnodes) != 0 {
		t.Errorf("Expected empty nodeVnodes after clear, got %d", len(ch.nodeVnodes))
	}

	// Re-add should work
	ch.AddNode("node3")
	if ch.GetNodeCount() != 1 {
		t.Errorf("Expected 1 after re-add, got %d", ch.GetNodeCount())
	}
	if ch.GetNode("key") != "node3" {
		t.Errorf("Expected 'node3', got %q", ch.GetNode("key"))
	}
}

func TestConsistentHash_GetNodes_Deterministic(t *testing.T) {
	ch := NewConsistentHash(100)
	ch.AddNode("node1")
	ch.AddNode("node2")
	ch.AddNode("node3")

	// Same key should always return same nodes in same order
	for i := 0; i < 50; i++ {
		nodes := ch.GetNodes("stable-key", 2)
		if len(nodes) != 2 {
			t.Fatalf("Iteration %d: expected 2 nodes", i)
		}
		expected := ch.GetNodes("stable-key", 2)
		if nodes[0] != expected[0] || nodes[1] != expected[1] {
			t.Fatalf("Iteration %d: non-deterministic result", i)
		}
	}
}

func TestConsistentHash_Concurrent_ReadWrite(t *testing.T) {
	ch := NewConsistentHash(50)
	for i := 0; i < 5; i++ {
		ch.AddNode(fmt.Sprintf("initial-%d", i))
	}

	var wg sync.WaitGroup
	errs := make(chan error, 100)

	// Concurrent writers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			nodeID := fmt.Sprintf("dynamic-%d", id)
			ch.AddNode(nodeID)
			ch.RemoveNode(nodeID)
		}(i)
	}

	// Concurrent readers
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", id)
			node := ch.GetNode(key)
			if node == "" {
				errs <- fmt.Errorf("got empty node for key %q", key)
			}
			_ = ch.GetNodes(key, 3)
			_ = ch.GetNodeCount()
			_ = ch.GetAllNodes()
		}(i)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestConsistentHash_Distribution(t *testing.T) {
	ch := NewConsistentHash(150)
	nodeCount := 5
	for i := 0; i < nodeCount; i++ {
		ch.AddNode(fmt.Sprintf("node-%d", i))
	}

	keyCount := 10000
	dist := make(map[string]int)
	for i := 0; i < keyCount; i++ {
		node := ch.GetNode(fmt.Sprintf("device-%d", i))
		dist[node]++
	}

	// Each node should get roughly keyCount/nodeCount = 2000 keys
	// With 150 vnodes, distribution should be fairly even
	expected := keyCount / nodeCount
	for node, count := range dist {
		ratio := float64(count) / float64(expected)
		if ratio < 0.5 || ratio > 1.5 {
			t.Errorf("Node %s got %d keys (expected ~%d, ratio %.2f) — poor distribution",
				node, count, expected, ratio)
		}
	}
}

func TestConsistentHash_WrapAround(t *testing.T) {
	// Test that keys with hash > max vnode hash wrap to first vnode
	ch := NewConsistentHash(5)
	ch.AddNode("only-node")

	// Multiple different keys should all resolve to the only node
	for i := 0; i < 20; i++ {
		node := ch.GetNode(fmt.Sprintf("key-%d", i))
		if node != "only-node" {
			t.Errorf("Key %d: expected 'only-node', got %q", i, node)
		}
	}
}

func TestConsistentHash_AddNode_Duplicate(t *testing.T) {
	ch := NewConsistentHash(10)
	ch.AddNode("node1")
	ringSize := len(ch.ring)
	vnodesBefore := len(ch.nodeVnodes["node1"])

	// Adding same node again should be no-op
	ch.AddNode("node1")
	if len(ch.ring) != ringSize {
		t.Errorf("Ring grew from %d to %d on duplicate add", ringSize, len(ch.ring))
	}
	if len(ch.nodeVnodes["node1"]) != vnodesBefore {
		t.Error("nodeVnodes changed on duplicate add")
	}
}
