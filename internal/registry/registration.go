package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/models"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// NodeRegistration handles node registration with etcd
type NodeRegistration struct {
	etcdClient   *clientv3.Client
	leaseID      clientv3.LeaseID
	nodeInfo     models.NodeInfo
	logger       *logging.Logger
	shardScanner *ShardScanner
}

// NewNodeRegistration creates a new node registration instance
func NewNodeRegistration(
	etcdClient *clientv3.Client,
	nodeInfo models.NodeInfo,
	shardScanner *ShardScanner,
	logger *logging.Logger,
) *NodeRegistration {
	return &NodeRegistration{
		etcdClient:   etcdClient,
		nodeInfo:     nodeInfo,
		shardScanner: shardScanner,
		logger:       logger,
	}
}

// Register registers the node with etcd
func (r *NodeRegistration) Register(ctx context.Context) error {
	r.logger.Info("Starting node registration")

	// 1. Scan local data directory to discover shards
	shards, err := r.shardScanner.ScanShards()
	if err != nil {
		return fmt.Errorf("failed to scan local shards: %w", err)
	}
	r.nodeInfo.Shards = shards
	r.nodeInfo.Capacity.CurrentShards = len(shards)

	// 2. Get disk capacity
	capacity, err := r.shardScanner.GetDiskCapacity()
	if err != nil {
		return fmt.Errorf("failed to get disk capacity: %w", err)
	}
	r.nodeInfo.Capacity.DiskTotal = capacity.DiskTotal
	r.nodeInfo.Capacity.DiskUsed = capacity.DiskUsed
	r.nodeInfo.Capacity.DiskAvailable = capacity.DiskAvailable

	r.nodeInfo.UpdatedAt = time.Now()

	r.logger.Info(
		"Local shard scan completed",
		"shards", len(shards),
		"disk_total_gb", capacity.DiskTotal/(1024*1024*1024),
		"disk_available_gb", capacity.DiskAvailable/(1024*1024*1024),
	)
	// 3. Create lease with 10s TTL
	lease, err := r.etcdClient.Grant(ctx, 10)
	if err != nil {
		return fmt.Errorf("failed to create lease: %w", err)
	}
	r.leaseID = lease.ID

	r.logger.Info(
		"Lease created",
		"lease_id", int64(r.leaseID),
		"ttl", 10,
	)

	// 4. Register node info to etcd
	key := fmt.Sprintf("/soltix/nodes/%s", r.nodeInfo.ID)
	data, err := json.Marshal(r.nodeInfo)
	if err != nil {
		return fmt.Errorf("failed to marshal node info: %w", err)
	}

	_, err = r.etcdClient.Put(ctx, key, string(data), clientv3.WithLease(r.leaseID))
	if err != nil {
		return fmt.Errorf("failed to register node: %w", err)
	}

	r.logger.Info(
		"Node registered successfully",
		"node_id", r.nodeInfo.ID,
		"address", r.nodeInfo.Address,
		"status", r.nodeInfo.Status,
	)
	// 5. Start keep-alive goroutine
	go r.keepAlive(ctx)

	return nil
}

// keepAlive maintains the lease by sending heartbeats
func (r *NodeRegistration) keepAlive(ctx context.Context) {
	r.logger.Info("Starting keep-alive loop", "lease_id", int64(r.leaseID))
	ch, err := r.etcdClient.KeepAlive(ctx, r.leaseID)
	if err != nil {
		r.logger.Error("Failed to start keep-alive", "error", err)
		return
	}

	ticker := time.NewTicker(30 * time.Second) // Update capacity every 30s
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			r.logger.Info("Keep-alive stopped (context done)")
			return

		case ka, ok := <-ch:
			if !ok {
				r.logger.Warn("Keep-alive channel closed, attempting re-registration")
				// Re-register after a delay
				time.Sleep(2 * time.Second)
				if err := r.Register(context.Background()); err != nil {
					r.logger.Error("Failed to re-register", "error", err)
				}
				return
			}

			if ka == nil {
				r.logger.Warn("Received nil keep-alive response")
				continue
			}

			// Heartbeat successful
			r.logger.Debug("Heartbeat sent",
				"lease_id", int64(r.leaseID), "ttl", ka.TTL)

		case <-ticker.C:
			// Periodically update capacity information
			if err := r.updateCapacity(ctx); err != nil {
				r.logger.Error("Failed to update capacity", "error", err)
			}
		}
	}
}

// updateCapacity updates node capacity info in etcd
func (r *NodeRegistration) updateCapacity(ctx context.Context) error {
	// Get current disk capacity
	capacity, err := r.shardScanner.GetDiskCapacity()
	if err != nil {
		r.logger.Error("Failed to get disk capacity", "error", err)
		return fmt.Errorf("failed to get disk capacity: %w", err)
	}

	// Update node info
	r.nodeInfo.Capacity.DiskUsed = capacity.DiskUsed
	r.nodeInfo.Capacity.DiskAvailable = capacity.DiskAvailable
	r.nodeInfo.UpdatedAt = time.Now()

	key := fmt.Sprintf("/soltix/nodes/%s", r.nodeInfo.ID)
	data, err := json.Marshal(r.nodeInfo)
	if err != nil {
		return err
	}

	_, err = r.etcdClient.Put(ctx, key, string(data), clientv3.WithLease(r.leaseID))
	if err != nil {
		return err
	}

	r.logger.Debug("Capacity updated",
		"disk_used_gb", capacity.DiskUsed/(1024*1024*1024),
		"disk_available_gb", capacity.DiskAvailable/(1024*1024*1024))
	return nil
}

// UpdateShards re-scans shards and updates etcd
func (r *NodeRegistration) UpdateShards(ctx context.Context) error {
	// Scan local shards
	shards, err := r.shardScanner.ScanShards()
	if err != nil {
		r.logger.Error("Failed to scan shards", "error", err)
		return fmt.Errorf("failed to scan shards: %w", err)
	}

	// Update node info
	r.nodeInfo.Shards = shards
	r.nodeInfo.Capacity.CurrentShards = len(shards)
	r.nodeInfo.UpdatedAt = time.Now()

	key := fmt.Sprintf("/soltix/nodes/%s", r.nodeInfo.ID)
	data, err := json.Marshal(r.nodeInfo)
	if err != nil {
		return err
	}

	_, err = r.etcdClient.Put(ctx, key, string(data), clientv3.WithLease(r.leaseID))
	if err != nil {
		return err
	}

	r.logger.Info("Shards updated in etcd", "shards", len(shards))

	return nil
}

// Deregister removes node from etcd
func (r *NodeRegistration) Deregister(ctx context.Context) error {
	r.logger.Info("Deregistering node", "node_id", r.nodeInfo.ID)

	key := fmt.Sprintf("/soltix/nodes/%s", r.nodeInfo.ID)

	// Delete node key
	_, err := r.etcdClient.Delete(ctx, key)
	if err != nil {
		r.logger.Error("Failed to delete node key", "error", err)
	}

	// Revoke lease
	if r.leaseID != 0 {
		_, err := r.etcdClient.Revoke(ctx, r.leaseID)
		if err != nil {
			r.logger.Error("Failed to revoke lease", "error", err)
		}
	}

	r.logger.Info("Node deregistered successfully", "node_id", r.nodeInfo.ID)

	return err
}
