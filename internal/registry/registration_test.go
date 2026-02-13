package registry

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/models"
	"go.etcd.io/etcd/client/pkg/v3/types"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

// setupEmbeddedEtcd starts an embedded etcd server for testing
func setupEmbeddedEtcd(t *testing.T) (*clientv3.Client, func()) {
	t.Helper()

	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.LogLevel = "error"

	// Use random local ports for all URLs
	cfg.ListenClientUrls, _ = types.NewURLs([]string{"http://127.0.0.1:0"})
	cfg.ListenPeerUrls, _ = types.NewURLs([]string{"http://127.0.0.1:0"})

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatalf("Failed to start embedded etcd: %v", err)
	}

	select {
	case <-e.Server.ReadyNotify():
		// Server is ready
	case <-time.After(10 * time.Second):
		e.Close()
		t.Fatal("Etcd server took too long to start")
	}

	// Get actual listening address from server
	endpoints := []string{}
	for _, listener := range e.Clients {
		endpoints = append(endpoints, "http://"+listener.Addr().String())
	}

	// Create client with actual endpoints
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		e.Close()
		t.Fatalf("Failed to create etcd client: %v", err)
	}

	cleanup := func() {
		_ = client.Close()
		e.Close()
	}

	return client, cleanup
}

// TestNewNodeRegistration tests creating node registration
func TestNewNodeRegistration(t *testing.T) {
	client, cleanup := setupEmbeddedEtcd(t)
	defer cleanup()

	logger := logging.NewDevelopment()
	scanner, _ := setupTestScanner(t)

	nodeInfo := models.NodeInfo{
		ID:      "test-node-1",
		Address: "localhost:5555",
		Status:  "active",
	}

	reg := NewNodeRegistration(client, nodeInfo, scanner, logger)

	if reg == nil {
		t.Fatal("Expected registration to be created")
		return
	}
	if reg.nodeInfo.ID != "test-node-1" {
		t.Errorf("Expected node ID 'test-node-1', got '%s'", reg.nodeInfo.ID)
	}
}

// TestRegister tests node registration
func TestRegister(t *testing.T) {
	client, cleanup := setupEmbeddedEtcd(t)
	defer cleanup()

	logger := logging.NewDevelopment()
	scanner, dir := setupTestScanner(t)

	// Create a test shard
	shardPath := filepath.Join(dir, "testdb", "metrics", "shard-1")
	if err := os.MkdirAll(shardPath, 0o755); err != nil {
		t.Fatalf("Failed to create shard: %v", err)
	}

	nodeInfo := models.NodeInfo{
		ID:      "test-node-1",
		Address: "localhost:5555",
		Status:  "active",
		Capacity: models.Capacity{
			TotalShards: 100,
		},
	}

	reg := NewNodeRegistration(client, nodeInfo, scanner, logger)

	ctx := context.Background()
	err := reg.Register(ctx)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// Verify node was registered in etcd
	key := "/soltix/nodes/test-node-1"
	resp, err := client.Get(ctx, key)
	if err != nil {
		t.Fatalf("Failed to get node from etcd: %v", err)
	}

	if len(resp.Kvs) != 1 {
		t.Fatalf("Expected 1 key-value pair, got %d", len(resp.Kvs))
	}

	// Parse node info
	var registeredNode models.NodeInfo
	if err := json.Unmarshal(resp.Kvs[0].Value, &registeredNode); err != nil {
		t.Fatalf("Failed to unmarshal node info: %v", err)
	}

	if registeredNode.ID != "test-node-1" {
		t.Errorf("Expected ID 'test-node-1', got '%s'", registeredNode.ID)
	}
	if registeredNode.Address != "localhost:5555" {
		t.Errorf("Expected address 'localhost:5555', got '%s'", registeredNode.Address)
	}
	if len(registeredNode.Shards) != 1 {
		t.Errorf("Expected 1 shard, got %d", len(registeredNode.Shards))
	}
	if registeredNode.Capacity.DiskTotal <= 0 {
		t.Error("Expected DiskTotal > 0")
	}
}

// TestDeregister tests node deregistration
func TestDeregister(t *testing.T) {
	client, cleanup := setupEmbeddedEtcd(t)
	defer cleanup()

	logger := logging.NewDevelopment()
	scanner, _ := setupTestScanner(t)

	nodeInfo := models.NodeInfo{
		ID:      "test-node-1",
		Address: "localhost:5555",
		Status:  "active",
	}

	reg := NewNodeRegistration(client, nodeInfo, scanner, logger)

	ctx := context.Background()

	// First register
	if err := reg.Register(ctx); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// Verify registered
	key := "/soltix/nodes/test-node-1"
	resp, err := client.Get(ctx, key)
	if err != nil || len(resp.Kvs) != 1 {
		t.Fatal("Node was not registered properly")
	}

	// Deregister
	if err := reg.Deregister(ctx); err != nil {
		t.Fatalf("Deregister failed: %v", err)
	}

	// Verify deregistered
	resp, err = client.Get(ctx, key)
	if err != nil {
		t.Fatalf("Failed to get node from etcd: %v", err)
	}

	if len(resp.Kvs) != 0 {
		t.Errorf("Expected node to be removed, but found %d keys", len(resp.Kvs))
	}
}

// TestUpdateShards tests updating shard information
func TestUpdateShards(t *testing.T) {
	client, cleanup := setupEmbeddedEtcd(t)
	defer cleanup()

	logger := logging.NewDevelopment()
	scanner, dir := setupTestScanner(t)

	nodeInfo := models.NodeInfo{
		ID:      "test-node-1",
		Address: "localhost:5555",
		Status:  "active",
	}

	reg := NewNodeRegistration(client, nodeInfo, scanner, logger)

	ctx := context.Background()

	// Register with no shards
	if err := reg.Register(ctx); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// Add shards
	shard1 := filepath.Join(dir, "db1", "col1", "shard-1")
	shard2 := filepath.Join(dir, "db1", "col1", "shard-2")
	if err := os.MkdirAll(shard1, 0o755); err != nil {
		t.Fatalf("Failed to create shard1: %v", err)
	}
	if err := os.MkdirAll(shard2, 0o755); err != nil {
		t.Fatalf("Failed to create shard2: %v", err)
	}

	// Update shards
	if err := reg.UpdateShards(ctx); err != nil {
		t.Fatalf("UpdateShards failed: %v", err)
	}

	// Verify updated in etcd
	key := "/soltix/nodes/test-node-1"
	resp, err := client.Get(ctx, key)
	if err != nil {
		t.Fatalf("Failed to get node from etcd: %v", err)
	}

	var updatedNode models.NodeInfo
	if err := json.Unmarshal(resp.Kvs[0].Value, &updatedNode); err != nil {
		t.Fatalf("Failed to unmarshal node info: %v", err)
	}

	if len(updatedNode.Shards) != 2 {
		t.Errorf("Expected 2 shards, got %d", len(updatedNode.Shards))
	}
	if updatedNode.Capacity.CurrentShards != 2 {
		t.Errorf("Expected CurrentShards 2, got %d", updatedNode.Capacity.CurrentShards)
	}
}

// TestUpdateCapacity tests updating capacity information
func TestUpdateCapacity(t *testing.T) {
	client, cleanup := setupEmbeddedEtcd(t)
	defer cleanup()

	logger := logging.NewDevelopment()
	scanner, _ := setupTestScanner(t)

	nodeInfo := models.NodeInfo{
		ID:      "test-node-1",
		Address: "localhost:5555",
		Status:  "active",
	}

	reg := NewNodeRegistration(client, nodeInfo, scanner, logger)

	ctx := context.Background()

	// Register
	if err := reg.Register(ctx); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// Update capacity
	if err := reg.updateCapacity(ctx); err != nil {
		t.Fatalf("updateCapacity failed: %v", err)
	}

	// Verify capacity in etcd
	key := "/soltix/nodes/test-node-1"
	resp, err := client.Get(ctx, key)
	if err != nil {
		t.Fatalf("Failed to get node from etcd: %v", err)
	}

	var updatedNode models.NodeInfo
	if err := json.Unmarshal(resp.Kvs[0].Value, &updatedNode); err != nil {
		t.Fatalf("Failed to unmarshal node info: %v", err)
	}

	if updatedNode.Capacity.DiskTotal <= 0 {
		t.Error("Expected DiskTotal > 0")
	}
	if updatedNode.Capacity.DiskAvailable < 0 {
		t.Error("Expected DiskAvailable >= 0")
	}
}

// TestRegisterWithMultipleShards tests registration with multiple shards
func TestRegisterWithMultipleShards(t *testing.T) {
	client, cleanup := setupEmbeddedEtcd(t)
	defer cleanup()

	logger := logging.NewDevelopment()
	scanner, dir := setupTestScanner(t)

	// Create multiple shards
	for i := 1; i <= 5; i++ {
		shardPath := filepath.Join(dir, "testdb", "metrics", "shard-"+string(rune(i+'0')))
		if err := os.MkdirAll(shardPath, 0o755); err != nil {
			t.Fatalf("Failed to create shard %d: %v", i, err)
		}
	}

	nodeInfo := models.NodeInfo{
		ID:      "test-node-1",
		Address: "localhost:5555",
		Status:  "active",
	}

	reg := NewNodeRegistration(client, nodeInfo, scanner, logger)

	ctx := context.Background()
	if err := reg.Register(ctx); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// Verify all shards registered
	key := "/soltix/nodes/test-node-1"
	resp, err := client.Get(ctx, key)
	if err != nil {
		t.Fatalf("Failed to get node from etcd: %v", err)
	}

	var registeredNode models.NodeInfo
	if err := json.Unmarshal(resp.Kvs[0].Value, &registeredNode); err != nil {
		t.Fatalf("Failed to unmarshal node info: %v", err)
	}

	if len(registeredNode.Shards) != 5 {
		t.Errorf("Expected 5 shards, got %d", len(registeredNode.Shards))
	}
}

// TestKeepAliveContext tests keep-alive respects context cancellation
func TestKeepAliveContext(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping keep-alive test in short mode")
	}

	client, cleanup := setupEmbeddedEtcd(t)
	defer cleanup()

	logger := logging.NewDevelopment()
	scanner, _ := setupTestScanner(t)

	nodeInfo := models.NodeInfo{
		ID:      "test-node-1",
		Address: "localhost:5555",
		Status:  "active",
	}

	reg := NewNodeRegistration(client, nodeInfo, scanner, logger)

	ctx, cancel := context.WithCancel(context.Background())

	// Register (starts keep-alive)
	if err := reg.Register(ctx); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// Wait a bit for keep-alive to start
	time.Sleep(100 * time.Millisecond)

	// Cancel context
	cancel()

	// Wait for keep-alive to stop
	time.Sleep(200 * time.Millisecond)

	// Test passes if no panic/deadlock
}

// TestLeaseExpiration tests that lease expires without keep-alive
func TestLeaseExpiration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping lease expiration test in short mode")
	}

	// Skip in CI/race mode - etcd lease expiration timing is unreliable
	// This test verifies etcd behavior rather than our code logic
	if os.Getenv("CI") != "" {
		t.Skip("Skipping lease expiration test in CI - timing unreliable")
	}

	client, cleanup := setupEmbeddedEtcd(t)
	defer cleanup()

	logger := logging.NewDevelopment()
	scanner, _ := setupTestScanner(t)

	nodeInfo := models.NodeInfo{
		ID:      "test-node-1",
		Address: "localhost:5555",
		Status:  "active",
	}

	reg := NewNodeRegistration(client, nodeInfo, scanner, logger)

	ctx, cancel := context.WithCancel(context.Background())

	// Register
	if err := reg.Register(ctx); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// Immediately cancel to stop keep-alive
	cancel()

	// Wait longer than lease TTL (10s)
	time.Sleep(12 * time.Second)

	// Check if key still exists (should be gone due to lease expiration)
	key := "/soltix/nodes/test-node-1"
	resp, err := client.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Failed to get key: %v", err)
	}

	if len(resp.Kvs) != 0 {
		t.Error("Expected key to be expired, but it still exists")
	}
}
