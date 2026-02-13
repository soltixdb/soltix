package coordinator

import (
	"fmt"
	"testing"
)

// BenchmarkConsistentHashAdd benchmarks adding nodes to consistent hash
func BenchmarkConsistentHashAdd(b *testing.B) {
	ch := NewConsistentHash(150)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		ch.AddNode(fmt.Sprintf("node-%d", i))
	}
}

// BenchmarkConsistentHashGet benchmarks getting nodes from consistent hash
func BenchmarkConsistentHashGet(b *testing.B) {
	ch := NewConsistentHash(150)

	// Pre-populate with nodes
	for i := 0; i < 100; i++ {
		ch.AddNode(fmt.Sprintf("node-%d", i))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("device-%d", i%1000)
		_ = ch.GetNode(key)
	}
}

// BenchmarkConsistentHashGetN benchmarks getting multiple nodes
func BenchmarkConsistentHashGetN(b *testing.B) {
	ch := NewConsistentHash(150)

	// Pre-populate with nodes
	for i := 0; i < 100; i++ {
		ch.AddNode(fmt.Sprintf("node-%d", i))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("device-%d", i%1000)
		_ = ch.GetNodes(key, 3)
	}
}

// BenchmarkRendezvousHashAdd benchmarks adding nodes to rendezvous hash
func BenchmarkRendezvousHashAdd(b *testing.B) {
	rh := NewRendezvousHash()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		rh.AddNode(fmt.Sprintf("node-%d", i))
	}
}

// BenchmarkRendezvousHashGet benchmarks getting nodes from rendezvous hash
func BenchmarkRendezvousHashGet(b *testing.B) {
	rh := NewRendezvousHash()

	// Pre-populate with nodes
	for i := 0; i < 100; i++ {
		rh.AddNode(fmt.Sprintf("node-%d", i))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("device-%d", i%1000)
		_ = rh.GetNode(key)
	}
}

// BenchmarkRendezvousHashGetN benchmarks getting multiple nodes
func BenchmarkRendezvousHashGetN(b *testing.B) {
	rh := NewRendezvousHash()

	// Pre-populate with nodes
	for i := 0; i < 100; i++ {
		rh.AddNode(fmt.Sprintf("node-%d", i))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("device-%d", i%1000)
		_ = rh.GetNodes(key, 3)
	}
}

// BenchmarkAdaptiveHasher benchmarks adaptive hasher (switches between algorithms)
func BenchmarkAdaptiveHasher(b *testing.B) {
	ah := NewAdaptiveHasher(10, 150)

	// Test with different node counts
	b.Run("SmallCluster", func(b *testing.B) {
		// Add 5 nodes (below threshold)
		for i := 0; i < 5; i++ {
			ah.AddNode(fmt.Sprintf("node-%d", i))
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("device-%d", i%1000)
			_ = ah.GetNode(key)
		}
	})

	b.Run("LargeCluster", func(b *testing.B) {
		ah := NewAdaptiveHasher(10, 150)
		// Add 50 nodes (above threshold)
		for i := 0; i < 50; i++ {
			ah.AddNode(fmt.Sprintf("node-%d", i))
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("device-%d", i%1000)
			_ = ah.GetNode(key)
		}
	})
}

// BenchmarkAdaptiveHasherGetN benchmarks getting multiple nodes with adaptive hasher
func BenchmarkAdaptiveHasherGetN(b *testing.B) {
	ah := NewAdaptiveHasher(10, 150)

	// Add nodes
	for i := 0; i < 20; i++ {
		ah.AddNode(fmt.Sprintf("node-%d", i))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("device-%d", i%1000)
		_ = ah.GetNodes(key, 3)
	}
}

// BenchmarkHashComparison compares different hash algorithms
func BenchmarkHashComparison(b *testing.B) {
	nodeCount := 50
	replicaCount := 3

	// Setup
	ch := NewConsistentHash(150)
	rh := NewRendezvousHash()
	ah := NewAdaptiveHasher(10, 150)

	for i := 0; i < nodeCount; i++ {
		nodeName := fmt.Sprintf("node-%d", i)
		ch.AddNode(nodeName)
		rh.AddNode(nodeName)
		ah.AddNode(nodeName)
	}

	b.Run("ConsistentHash", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("device-%d", i%1000)
			_ = ch.GetNodes(key, replicaCount)
		}
	})

	b.Run("RendezvousHash", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("device-%d", i%1000)
			_ = rh.GetNodes(key, replicaCount)
		}
	})

	b.Run("AdaptiveHasher", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("device-%d", i%1000)
			_ = ah.GetNodes(key, replicaCount)
		}
	})
}

// BenchmarkConsistentHashRemove benchmarks removing nodes from consistent hash
func BenchmarkConsistentHashRemove(b *testing.B) {
	nodeCount := 100

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		ch := NewConsistentHash(150)
		for n := 0; n < nodeCount; n++ {
			ch.AddNode(fmt.Sprintf("node-%d", n))
		}
		for n := 0; n < nodeCount; n++ {
			ch.RemoveNode(fmt.Sprintf("node-%d", n))
		}
	}
}

// BenchmarkRendezvousHashRemove benchmarks removing nodes from rendezvous hash
func BenchmarkRendezvousHashRemove(b *testing.B) {
	nodeCount := 100

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		rh := NewRendezvousHash()
		for n := 0; n < nodeCount; n++ {
			rh.AddNode(fmt.Sprintf("node-%d", n))
		}
		for n := 0; n < nodeCount; n++ {
			rh.RemoveNode(fmt.Sprintf("node-%d", n))
		}
	}
}

// BenchmarkAdaptiveHasherStrategySwitch benchmarks switching strategies around threshold
func BenchmarkAdaptiveHasherStrategySwitch(b *testing.B) {
	threshold := 20
	ah := NewAdaptiveHasher(threshold, 150)

	// Pre-add nodes below threshold
	for i := 0; i < threshold-1; i++ {
		ah.AddNode(fmt.Sprintf("node-%d", i))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Trigger switch to consistent
		nodeID := fmt.Sprintf("node-%d", threshold+i)
		ah.AddNode(nodeID)
		_ = ah.GetCurrentStrategy()
		ah.RemoveNode(nodeID)
		_ = ah.GetCurrentStrategy()
	}
}

// BenchmarkGetAllNodes benchmarks retrieving all nodes
func BenchmarkGetAllNodes(b *testing.B) {
	ah := NewAdaptiveHasher(10, 150)
	for i := 0; i < 200; i++ {
		ah.AddNode(fmt.Sprintf("node-%d", i))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = ah.GetAllNodes()
	}
}

// BenchmarkShardRouterRouteWrite benchmarks shard routing for writes
func BenchmarkShardRouterRouteWrite(b *testing.B) {
	b.Skip("RouteWrite requires metadata manager - skipping for simple benchmark")
}

// BenchmarkShardRouterCalculateShardID benchmarks shard ID calculation
func BenchmarkShardRouterCalculateShardID(b *testing.B) {
	b.Skip("calculateShardID not exported - skipping")
}

// BenchmarkQueryCoordinatorDistributeQuery benchmarks query distribution
func BenchmarkQueryCoordinatorDistributeQuery(b *testing.B) {
	b.Skip("DistributeQuery method needs verification - skipping for now")
}

// BenchmarkNodeFailover benchmarks node failover scenarios
func BenchmarkNodeFailover(b *testing.B) {
	ch := NewConsistentHash(150)

	// Add initial nodes
	for i := 0; i < 20; i++ {
		ch.AddNode(fmt.Sprintf("node-%d", i))
	}

	keys := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		keys[i] = fmt.Sprintf("device-%d", i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Simulate node failure
		b.StopTimer()
		failedNode := fmt.Sprintf("node-%d", i%20)
		ch.RemoveNode(failedNode)
		b.StartTimer()

		// Get new assignments for all keys
		for _, key := range keys {
			_ = ch.GetNode(key)
		}

		// Restore node for next iteration
		b.StopTimer()
		ch.AddNode(failedNode)
		b.StartTimer()
	}
}

// BenchmarkDistributionUniformity benchmarks hash distribution quality
func BenchmarkDistributionUniformity(b *testing.B) {
	nodeCount := 10
	keyCount := 10000

	b.Run("ConsistentHash", func(b *testing.B) {
		ch := NewConsistentHash(150)
		for i := 0; i < nodeCount; i++ {
			ch.AddNode(fmt.Sprintf("node-%d", i))
		}

		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			distribution := make(map[string]int)
			for j := 0; j < keyCount; j++ {
				key := fmt.Sprintf("device-%d", j)
				node := ch.GetNode(key)
				distribution[node]++
			}
		}
	})

	b.Run("RendezvousHash", func(b *testing.B) {
		rh := NewRendezvousHash()
		for i := 0; i < nodeCount; i++ {
			rh.AddNode(fmt.Sprintf("node-%d", i))
		}

		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			distribution := make(map[string]int)
			for j := 0; j < keyCount; j++ {
				key := fmt.Sprintf("device-%d", j)
				node := rh.GetNode(key)
				distribution[node]++
			}
		}
	})
}
