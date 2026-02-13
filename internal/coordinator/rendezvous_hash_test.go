package coordinator

import (
	"fmt"
	"sort"
	"sync"
	"testing"
)

func TestNewRendezvousHash(t *testing.T) {
	rh := NewRendezvousHash()
	if rh == nil {
		t.Fatal("Expected non-nil RendezvousHash")
	}
	if rh.GetNodeCount() != 0 {
		t.Errorf("Expected 0 initial nodes, got %d", rh.GetNodeCount())
	}
}

func TestRendezvousHash_AddNode(t *testing.T) {
	rh := NewRendezvousHash()
	rh.AddNode("node1")

	if rh.GetNodeCount() != 1 {
		t.Errorf("Expected 1 node, got %d", rh.GetNodeCount())
	}
}

func TestRendezvousHash_GetNode(t *testing.T) {
	rh := NewRendezvousHash()
	rh.AddNode("node1")
	rh.AddNode("node2")

	node := rh.GetNode("test-key")
	if node == "" {
		t.Error("Expected non-empty node")
	}

	node2 := rh.GetNode("test-key")
	if node != node2 {
		t.Error("Expected consistent node for same key")
	}
}

func TestRendezvousHash_GetNodes(t *testing.T) {
	rh := NewRendezvousHash()
	rh.AddNode("node1")
	rh.AddNode("node2")
	rh.AddNode("node3")

	nodes := rh.GetNodes("test-key", 2)
	if len(nodes) != 2 {
		t.Errorf("Expected 2 nodes, got %d", len(nodes))
	}

	if nodes[0] == nodes[1] {
		t.Error("Expected distinct nodes")
	}
}

func TestRendezvousHash_Clear(t *testing.T) {
	rh := NewRendezvousHash()
	rh.AddNode("node1")
	rh.Clear()

	if rh.GetNodeCount() != 0 {
		t.Errorf("Expected 0 nodes after clear, got %d", rh.GetNodeCount())
	}
}

func TestRendezvousHash_RemoveNode(t *testing.T) {
	rh := NewRendezvousHash()
	rh.AddNode("node1")
	rh.AddNode("node2")
	rh.AddNode("node3")

	rh.RemoveNode("node2")
	if rh.GetNodeCount() != 2 {
		t.Errorf("Expected 2 nodes after remove, got %d", rh.GetNodeCount())
	}

	// Verify node2 is gone
	nodes := rh.GetAllNodes()
	for _, n := range nodes {
		if n == "node2" {
			t.Error("node2 should have been removed")
		}
	}
}

func TestRendezvousHash_RemoveNode_NonExistent(t *testing.T) {
	rh := NewRendezvousHash()
	rh.AddNode("node1")

	// Should not panic
	rh.RemoveNode("node-nope")
	if rh.GetNodeCount() != 1 {
		t.Errorf("Expected 1 node, got %d", rh.GetNodeCount())
	}
}

func TestRendezvousHash_RemoveNode_All(t *testing.T) {
	rh := NewRendezvousHash()
	rh.AddNode("a")
	rh.AddNode("b")
	rh.RemoveNode("a")
	rh.RemoveNode("b")

	if rh.GetNodeCount() != 0 {
		t.Errorf("Expected 0 nodes, got %d", rh.GetNodeCount())
	}
	if rh.GetNode("key") != "" {
		t.Error("Expected empty from empty hash")
	}
}

func TestRendezvousHash_AddNode_Duplicate(t *testing.T) {
	rh := NewRendezvousHash()
	rh.AddNode("node1")
	rh.AddNode("node1")

	if rh.GetNodeCount() != 1 {
		t.Errorf("Expected 1 node after duplicate add, got %d", rh.GetNodeCount())
	}
}

func TestRendezvousHash_GetNode_Empty(t *testing.T) {
	rh := NewRendezvousHash()
	if rh.GetNode("key") != "" {
		t.Error("Expected empty from empty hash")
	}
}

func TestRendezvousHash_GetNodes_Empty(t *testing.T) {
	rh := NewRendezvousHash()
	nodes := rh.GetNodes("key", 3)
	if len(nodes) != 0 {
		t.Errorf("Expected 0 nodes from empty hash, got %d", len(nodes))
	}
}

func TestRendezvousHash_GetNodes_MoreThanAvailable(t *testing.T) {
	rh := NewRendezvousHash()
	rh.AddNode("node1")
	rh.AddNode("node2")

	nodes := rh.GetNodes("key", 5)
	if len(nodes) != 2 {
		t.Errorf("Expected 2 nodes (all available), got %d", len(nodes))
	}
}

func TestRendezvousHash_GetNodes_SingleNode(t *testing.T) {
	rh := NewRendezvousHash()
	rh.AddNode("solo")

	nodes := rh.GetNodes("key", 3)
	if len(nodes) != 1 {
		t.Errorf("Expected 1 node, got %d", len(nodes))
	}
	if nodes[0] != "solo" {
		t.Errorf("Expected 'solo', got %q", nodes[0])
	}
}

func TestRendezvousHash_GetAllNodes_Sorted(t *testing.T) {
	rh := NewRendezvousHash()
	rh.AddNode("charlie")
	rh.AddNode("alpha")
	rh.AddNode("bravo")

	nodes := rh.GetAllNodes()
	if len(nodes) != 3 {
		t.Fatalf("Expected 3 nodes, got %d", len(nodes))
	}
	if !sort.StringsAreSorted(nodes) {
		t.Errorf("GetAllNodes should return sorted list, got %v", nodes)
	}
}

func TestRendezvousHash_Consistency(t *testing.T) {
	rh := NewRendezvousHash()
	rh.AddNode("node1")
	rh.AddNode("node2")
	rh.AddNode("node3")

	// Same key → same node
	key := "test-device"
	first := rh.GetNode(key)
	for i := 0; i < 50; i++ {
		if got := rh.GetNode(key); got != first {
			t.Fatalf("Inconsistent: iteration %d returned %q, expected %q", i, got, first)
		}
	}
}

func TestRendezvousHash_GetNode_ConsistentWithGetNodes(t *testing.T) {
	rh := NewRendezvousHash()
	rh.AddNode("node1")
	rh.AddNode("node2")
	rh.AddNode("node3")

	// GetNode should return the same as GetNodes(..., 1)[0]
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key-%d", i)
		single := rh.GetNode(key)
		multi := rh.GetNodes(key, 1)
		if len(multi) != 1 || multi[0] != single {
			t.Errorf("Key %q: GetNode=%q but GetNodes=%v", key, single, multi)
		}
	}
}

func TestRendezvousHash_MinimalDisruption(t *testing.T) {
	rh := NewRendezvousHash()
	rh.AddNode("node1")
	rh.AddNode("node2")
	rh.AddNode("node3")

	keys := make([]string, 100)
	before := make(map[string]string)
	for i := range keys {
		keys[i] = fmt.Sprintf("device-%d", i)
		before[keys[i]] = rh.GetNode(keys[i])
	}

	// Remove one node — only keys previously assigned to it should move
	rh.RemoveNode("node2")

	movedCount := 0
	for _, k := range keys {
		current := rh.GetNode(k)
		if current != before[k] {
			// Key moved — it should have been on node2
			if before[k] != "node2" {
				t.Errorf("Key %q moved from %q to %q but was not on removed node",
					k, before[k], current)
			}
			movedCount++
		}
	}

	// Should have some keys that moved (those on node2)
	if movedCount == 0 {
		t.Error("Expected some keys to move after removing a node")
	}
}

func TestRendezvousHash_Concurrent(t *testing.T) {
	rh := NewRendezvousHash()
	for i := 0; i < 5; i++ {
		rh.AddNode(fmt.Sprintf("node-%d", i))
	}

	var wg sync.WaitGroup

	// Concurrent readers
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_ = rh.GetNode(fmt.Sprintf("key-%d", id))
			_ = rh.GetNodes(fmt.Sprintf("key-%d", id), 3)
			_ = rh.GetNodeCount()
			_ = rh.GetAllNodes()
		}(i)
	}

	// Concurrent writers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			nodeID := fmt.Sprintf("dynamic-%d", id)
			rh.AddNode(nodeID)
			rh.RemoveNode(nodeID)
		}(i)
	}

	wg.Wait()
	// If we get here without panic, mutex is working
}

func TestRendezvousHash_ClearThenReuse(t *testing.T) {
	rh := NewRendezvousHash()
	rh.AddNode("a")
	rh.AddNode("b")
	rh.Clear()

	if rh.GetNodeCount() != 0 {
		t.Errorf("Expected 0 after clear, got %d", rh.GetNodeCount())
	}
	if len(rh.GetAllNodes()) != 0 {
		t.Error("GetAllNodes should be empty after clear")
	}

	// Re-add should work
	rh.AddNode("c")
	if rh.GetNodeCount() != 1 {
		t.Errorf("Expected 1 after re-add, got %d", rh.GetNodeCount())
	}
	if rh.GetNode("key") != "c" {
		t.Errorf("Expected 'c', got %q", rh.GetNode("key"))
	}
}
