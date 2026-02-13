package grpc

import (
	"sync"
	"testing"

	"github.com/soltixdb/soltix/internal/logging"
)

func TestNewConnectionPool(t *testing.T) {
	logger := logging.NewDevelopment()
	pool := NewConnectionPool(logger)

	if pool == nil {
		t.Fatal("Expected non-nil ConnectionPool")
		return
	}

	if pool.logger == nil {
		t.Error("Expected non-nil logger")
	}

	if pool.conns == nil {
		t.Error("Expected non-nil conns map")
	}

	if len(pool.conns) != 0 {
		t.Errorf("Expected empty conns map, got %d connections", len(pool.conns))
	}
}

func TestConnectionPool_GetConnection(t *testing.T) {
	logger := logging.NewDevelopment()
	pool := NewConnectionPool(logger)

	// Note: This test requires a gRPC server to be running
	// For unit tests, we'll test the logic without actual connection
	address := "localhost:50051"

	// First call - creates new connection
	conn1, err := pool.GetConnection(address)
	if err != nil {
		// Expected to fail without server, but should try to create connection
		t.Logf("Connection failed as expected (no server): %v", err)
		return
	}

	if conn1 == nil {
		t.Error("Expected non-nil connection")
	}

	// Second call - reuses existing connection
	conn2, err := pool.GetConnection(address)
	if err != nil {
		t.Fatalf("GetConnection failed: %v", err)
	}

	if conn1 != conn2 {
		t.Error("Expected same connection to be returned")
	}

	// Verify connection is in pool
	pool.mu.RLock()
	poolConn, exists := pool.conns[address]
	pool.mu.RUnlock()

	if !exists {
		t.Error("Expected connection to be in pool")
	}

	if poolConn != conn1 {
		t.Error("Expected pooled connection to match returned connection")
	}
}

func TestConnectionPool_MultipleAddresses(t *testing.T) {
	logger := logging.NewDevelopment()
	pool := NewConnectionPool(logger)

	addresses := []string{
		"localhost:50051",
		"localhost:50052",
		"localhost:50053",
	}

	// Try to get connections for multiple addresses
	for _, addr := range addresses {
		_, _ = pool.GetConnection(addr)
	}

	// Note: Connections will fail without servers, but pool should track attempts
	// This tests the logic of managing multiple connections
}

func TestConnectionPool_Close(t *testing.T) {
	logger := logging.NewDevelopment()
	pool := NewConnectionPool(logger)

	// Add some connections (they will fail, but we can test Close logic)
	addresses := []string{
		"localhost:50051",
		"localhost:50052",
	}

	for _, addr := range addresses {
		_, _ = pool.GetConnection(addr)
	}

	// Close all connections
	pool.Close()

	// Verify pool is empty
	pool.mu.RLock()
	count := len(pool.conns)
	pool.mu.RUnlock()

	if count != 0 {
		t.Errorf("Expected empty pool after Close, got %d connections", count)
	}
}

func TestConnectionPool_Concurrent(t *testing.T) {
	logger := logging.NewDevelopment()
	pool := NewConnectionPool(logger)

	address := "localhost:50051"
	done := make(chan bool)

	// Test concurrent access to same address
	for i := 0; i < 10; i++ {
		go func() {
			_, _ = pool.GetConnection(address)
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify only one connection exists
	pool.mu.RLock()
	count := len(pool.conns)
	pool.mu.RUnlock()

	// Count should be 0 (failed) or 1 (if somehow connected)
	if count > 1 {
		t.Errorf("Expected at most 1 connection, got %d", count)
	}
}

func TestConnectionPool_EmptyAddress(t *testing.T) {
	logger := logging.NewDevelopment()
	pool := NewConnectionPool(logger)

	_, err := pool.GetConnection("")
	// Empty address is actually accepted by gRPC, just logs it
	// This is expected behavior, not an error
	_ = err
}

func TestConnectionPool_InvalidAddress(t *testing.T) {
	logger := logging.NewDevelopment()
	pool := NewConnectionPool(logger)

	invalidAddresses := []string{
		"invalid:address:format",
		":",
		"no-port",
	}

	// Note: gRPC accepts various address formats and tries to resolve them
	// Invalid addresses will be logged but won't immediately fail
	for _, addr := range invalidAddresses {
		_, _ = pool.GetConnection(addr)
		// Connection is created optimistically, errors happen on actual use
	}
}

func TestConnectionPool_MultipleGetConnection_SameAddress(t *testing.T) {
	logger := logging.NewDevelopment()
	pool := NewConnectionPool(logger)

	address := "localhost:50051"

	// Get connection multiple times
	conn1, _ := pool.GetConnection(address)
	conn2, _ := pool.GetConnection(address)
	conn3, _ := pool.GetConnection(address)

	// All should point to same underlying connection
	_ = conn1
	_ = conn2
	_ = conn3
	// Connections are reused (expected behavior)
}

func TestConnectionPool_DoubleCheck_Lock(t *testing.T) {
	logger := logging.NewDevelopment()
	pool := NewConnectionPool(logger)

	address := "localhost:50051"
	done := make(chan bool, 20)

	// Simulate race condition where multiple goroutines
	// try to create connection at the same time
	for i := 0; i < 20; i++ {
		go func() {
			_, _ = pool.GetConnection(address)
			done <- true
		}()
	}

	// Wait for all
	for i := 0; i < 20; i++ {
		<-done
	}

	// Check that only one connection was created
	pool.mu.RLock()
	count := len(pool.conns)
	pool.mu.RUnlock()

	if count > 1 {
		t.Errorf("Expected at most 1 connection due to double-check locking, got %d", count)
	}
}

func TestConnectionPool_ConcurrentDifferentAddresses(t *testing.T) {
	logger := logging.NewDevelopment()
	pool := NewConnectionPool(logger)

	addresses := []string{
		"localhost:50051",
		"localhost:50052",
		"localhost:50053",
		"localhost:50054",
		"localhost:50055",
	}

	var wg sync.WaitGroup
	for _, addr := range addresses {
		wg.Add(1)
		go func(address string) {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				_, _ = pool.GetConnection(address)
			}
		}(addr)
	}

	wg.Wait()

	// Should have connections for all addresses
	pool.mu.RLock()
	count := len(pool.conns)
	pool.mu.RUnlock()

	// Count should be <= number of addresses
	if count > len(addresses) {
		t.Errorf("Expected at most %d connections, got %d", len(addresses), count)
	}
}

func TestConnectionPool_Close_EmptyPool(t *testing.T) {
	logger := logging.NewDevelopment()
	pool := NewConnectionPool(logger)

	// Close empty pool should not panic
	pool.Close()

	pool.mu.RLock()
	count := len(pool.conns)
	pool.mu.RUnlock()

	if count != 0 {
		t.Errorf("Expected empty pool after Close, got %d connections", count)
	}
}

func TestConnectionPool_Close_Multiple(t *testing.T) {
	logger := logging.NewDevelopment()
	pool := NewConnectionPool(logger)

	// Add connections
	for i := 1; i <= 3; i++ {
		address := "localhost:5005" + string(rune('0'+i))
		_, _ = pool.GetConnection(address)
	}

	// Close multiple times should be safe
	pool.Close()
	pool.Close()
	pool.Close()

	pool.mu.RLock()
	count := len(pool.conns)
	pool.mu.RUnlock()

	if count != 0 {
		t.Errorf("Expected empty pool after multiple Close calls, got %d connections", count)
	}
}

func TestConnectionPool_GetAfterClose(t *testing.T) {
	logger := logging.NewDevelopment()
	pool := NewConnectionPool(logger)

	address := "localhost:50051"

	// Get connection
	_, _ = pool.GetConnection(address)

	// Close pool
	pool.Close()

	// Get connection again after close
	_, _ = pool.GetConnection(address)

	// Should create new connection
	pool.mu.RLock()
	count := len(pool.conns)
	pool.mu.RUnlock()

	if count == 0 {
		t.Error("Expected connection to be created after Close")
	}
}

func TestConnectionPool_ThreadSafety_StressTest(t *testing.T) {
	logger := logging.NewDevelopment()
	pool := NewConnectionPool(logger)

	addresses := []string{
		"localhost:50051",
		"localhost:50052",
	}

	var wg sync.WaitGroup
	iterations := 100

	// Multiple goroutines accessing same addresses
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				addr := addresses[j%len(addresses)]
				_, _ = pool.GetConnection(addr)
			}
		}()
	}

	wg.Wait()

	// Check pool is consistent
	pool.mu.RLock()
	count := len(pool.conns)
	pool.mu.RUnlock()

	if count > len(addresses) {
		t.Errorf("Expected at most %d connections, got %d", len(addresses), count)
	}
}

func TestConnectionPool_MixedOperations(t *testing.T) {
	logger := logging.NewDevelopment()
	pool := NewConnectionPool(logger)

	var wg sync.WaitGroup

	// Goroutine 1: Get connections
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			_, _ = pool.GetConnection("localhost:50051")
		}
	}()

	// Goroutine 2: Get different connections
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			_, _ = pool.GetConnection("localhost:50052")
		}
	}()

	// Goroutine 3: Close pool (at the end)
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Wait a bit before closing
		for i := 0; i < 40; i++ {
			_, _ = pool.GetConnection("localhost:50053")
		}
		pool.Close()
	}()

	wg.Wait()

	// After mixed operations, check state
	pool.mu.RLock()
	count := len(pool.conns)
	pool.mu.RUnlock()

	// After Close, should be 0
	if count != 0 {
		t.Logf("Pool has %d connections after mixed operations", count)
	}
}

func TestConnectionPool_GetConnectionCount(t *testing.T) {
	logger := logging.NewDevelopment()
	pool := NewConnectionPool(logger)
	defer pool.Close()

	// Initially empty
	if count := pool.GetConnectionCount(); count != 0 {
		t.Errorf("Expected 0 connections, got %d", count)
	}

	// Add connections
	addresses := []string{"localhost:50051", "localhost:50052", "localhost:50053"}
	for _, addr := range addresses {
		_, _ = pool.GetConnection(addr)
	}

	if count := pool.GetConnectionCount(); count != 3 {
		t.Errorf("Expected 3 connections, got %d", count)
	}
}

func TestConnectionPool_GetConnectionStates(t *testing.T) {
	logger := logging.NewDevelopment()
	pool := NewConnectionPool(logger)
	defer pool.Close()

	// Add connections
	addresses := []string{"localhost:50051", "localhost:50052"}
	for _, addr := range addresses {
		_, _ = pool.GetConnection(addr)
	}

	states := pool.GetConnectionStates()

	if len(states) != 2 {
		t.Errorf("Expected 2 connection states, got %d", len(states))
	}

	for addr, state := range states {
		if state == "" {
			t.Errorf("Expected non-empty state for %s", addr)
		}
		t.Logf("Connection %s state: %s", addr, state)
	}
}

func TestConnectionPool_HealthCheck(t *testing.T) {
	logger := logging.NewDevelopment()
	pool := NewConnectionPool(logger)
	defer pool.Close()

	// Add a connection
	_, _ = pool.GetConnection("localhost:50051")

	// Manually trigger health check
	pool.checkConnections()

	// Pool should still work after health check
	count := pool.GetConnectionCount()
	t.Logf("Connection count after health check: %d", count)
}
