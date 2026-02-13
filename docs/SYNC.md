# Sync — Data Synchronization

This document describes the data synchronization mechanism between storage nodes in Soltix.

## Overview

The `sync` package ensures data consistency between replica nodes. There are **2 main triggers**:

1. **Startup Sync**: When a node starts, sync missing data from replicas
2. **Anti-Entropy**: Background process that periodically detects and repairs data inconsistencies

Supports 2 modes:
- **Group-based sync** (4-tier architecture): `SyncGroupOnStartup()` — sync by group boundary
- **Shard-based sync** (legacy): `SyncOnStartup()` — sync by shard/time range

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              SYNC PACKAGE                                   │
│                                                                             │
│  ┌──────────────┐    ┌──────────────────┐    ┌────────────────────────┐     │
│  │   Manager    │───▶│  RemoteSyncClient │───▶│  Other Storage Nodes   │    │
│  │              │    │   (GRPCClient)    │    │  (gRPC StorageService) │    │
│  └──────┬───────┘    └──────────────────┘    └────────────────────────┘     │
│         │                                                                   │
│         │ uses                                                              │
│         ▼                                                                   │
│  ┌──────────────┐    ┌──────────────────┐                                  │
│  │ LocalStorage │    │ MetadataManager  │                                  │
│  │  (Adapter)   │    │  (EtcdAdapter)   │                                  │
│  └──────┬───────┘    └────────┬─────────┘                                  │
│         │                     │                                             │
│         ▼                     ▼                                             │
│  ┌──────────────┐    ┌──────────────────┐                                  │
│  │ TieredStorage│    │      etcd        │                                  │
│  │ + MemoryStore│    │ (group/shard     │                                  │
│  │ + WAL        │    │  metadata)       │                                  │
│  └──────────────┘    └──────────────────┘                                  │
│                                                                             │
│  ┌──────────────┐                                                          │
│  │ AntiEntropy  │  Background goroutine                                    │
│  │   Service    │  - Periodic checksum comparison                          │
│  └──────────────┘  - Auto-repair on mismatch                               │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Components

### 1. Manager

Core component coordinating sync:

| Method | Description |
|--------|-------------|
| `SyncGroupOnStartup(ctx)` | Sync all groups when node starts (4-tier) |
| `SyncOnStartup(ctx)` | Sync all shards when node starts (legacy) |
| `SyncShard(ctx, shard)` | Sync a specific shard (on-demand / repair) |
| `syncGroupFromReplica()` | Stream data from replica → batch → WriteToWAL |
| `OnSyncEvent(handler)` | Subscribe to sync events |

### 2. AntiEntropy

Background service that detects and repairs data inconsistencies:

| Behavior | Detail |
|----------|--------|
| **Initial delay** | 30 seconds after start (avoid conflict with startup sync) |
| **Interval** | Default 1 hour |
| **Check method** | Checksum comparison (local vs remote) |
| **Repair method** | Trigger `syncManager.SyncShard()` |

### 3. GRPCClient (RemoteSyncClient)

Communication with remote storage nodes:

| Method | Description |
|--------|-------------|
| `SyncShard()` | Stream data points for a shard (uses `QueryShard` RPC fallback) |
| `SyncGroup()` | Stream data points for a group |
| `GetChecksum()` | Get checksum from remote node |
| `GetGroupChecksum()` | Get group checksum from remote node |

> **Note**: Currently uses `QueryShard` RPC as fallback — no dedicated sync RPC yet.

### 4. Interfaces

```go
// MetadataManager — etcd cluster metadata
type MetadataManager interface {
    GetMyShards(ctx, nodeID) ([]ShardInfo, error)      // Legacy
    GetMyGroups(ctx, nodeID) ([]GroupInfo, error)       // 4-tier
    GetActiveReplicas(ctx, shardID, excludeNode) ([]NodeInfo, error)
    GetGroupReplicas(ctx, groupID, excludeNode) ([]NodeInfo, error)
    GetNodeInfo(ctx, nodeID) (*NodeInfo, error)
    GetAllNodes(ctx) ([]NodeInfo, error)
}

// LocalStorage — local data access
type LocalStorage interface {
    GetLastTimestamp(database, collection, startTime, endTime) (time.Time, error)
    WriteToWAL(entries []*wal.Entry) error
    Query(database, collection, deviceIDs, startTime, endTime, fields) ([]*DataPoint, error)
    GetChecksum(database, collection, startTime, endTime) (string, int64, error)
}

// GroupInfo — group metadata for 4-tier sync
type GroupInfo struct {
    GroupID      int
    PrimaryNode  string
    ReplicaNodes []string
    Databases    []string
    Collections  []string
}
```

## Processing Flows

### 1. Startup Sync Flow (Group-based)

When storage node starts:

```
Storage Node                      etcd                     Replica Node
     │                              │                            │
     │ 1. WAL Replay (local)        │                            │
     │ ◄──────────────              │                            │
     │                              │                            │
     │ 2. SyncGroupOnStartup()      │                            │
     │ ─────────────────────────────►                            │
     │   GetMyGroups(nodeID)        │                            │
     │ ◄─────────────────────────────                            │
     │   [group_0, group_42, ...]   │                            │
     │                              │                            │
     │ 3. For each group:           │                            │
     │    GetGroupReplicas(groupID) │                            │
     │ ─────────────────────────────►                            │
     │ ◄─────────────────────────────                            │
     │    [node-2, node-3]          │                            │
     │                              │                            │
     │ 4. SyncGroup(addr, req)      │                            │
     │ ──────────────────────────────────────────────────────────►│
     │                              │         gRPC QueryShard    │
     │ ◄──────────────────────────────────────────────────────────│
     │        Stream DataPoints     │                            │
     │                              │                            │
     │ 5. Batch 1000 points         │                            │
     │    → dataPointsToWALEntries()│                            │
     │    → WriteToWAL(entries)     │                            │
     │                              │                            │
     │ 6. WAL → Flush → TieredStorage                           │
     │    → OnFlushComplete → Aggregation Pipeline               │
     │                              │                            │
     │ 7. Register with etcd        │                            │
     │ ─────────────────────────────►                            │
     │                              │                            │
     │ 8. Start NATS subscriber     │                            │
     │    (accept new writes)       │                            │
```

**Key details:**

| Step | Component | Action |
|------|-----------|--------|
| 1 | Storage | WAL Replay — restore committed data |
| 2 | Manager | GetMyGroups — retrieve groups this node is responsible for |
| 3 | MetadataManager | GetGroupReplicas — find replicas for each group |
| 4 | GRPCClient | SyncGroup — stream data from replica |
| 5 | Manager | Batch → WriteToWAL — write via WAL (durability) |
| 6 | Storage | WAL → Flush → Storage → Aggregation (same pipeline as normal writes) |

**Concurrency:** Up to `MaxConcurrentSyncs` (default: 5) groups can sync concurrently.

**Failure handling:** Try each replica until success. If all fail, node still starts with partial data — Anti-Entropy will repair later.

### 2. Anti-Entropy Flow

Background process runs periodically:

```
AntiEntropy                       Local                      Replica
Service                          Storage                      Node
     │                              │                            │
     │ 1. Wait 30s (initial delay)  │                            │
     │                              │                            │
     │ 2. Get my shards (from etcd) │                            │
     │                              │                            │
     │ 3. For each shard:           │                            │
     │    GetChecksum(shard, window) │                            │
     │ ─────────────────────────────►                            │
     │ ◄─────────────────────────────                            │
     │    local = "1000:min:max"    │                            │
     │                              │                            │
     │ 4. GetChecksum from replica  │                            │
     │ ──────────────────────────────────────────────────────────►│
     │ ◄──────────────────────────────────────────────────────────│
     │    remote = "1050:min:max"   │                            │
     │                              │                            │
     │ 5. Compare: local ≠ remote   │                            │
     │    → repairShard()           │                            │
     │    → syncManager.SyncShard() │                            │
     │ ──────────────────────────────────────────────────────────►│
     │         Stream missing data  │                            │
     │ ◄──────────────────────────────────────────────────────────│
     │                              │                            │
     │ 6. WriteToWAL → Flush        │                            │
     │ ─────────────────────────────►                            │
     │                              │                            │
     │ 7. Sleep(1h)                 │                            │
     │    → goto step 2             │                            │
```

**Checksum format:** `"{count}:{minTimestamp}:{maxTimestamp}"`

### 3. Data Flow: Sync → Aggregation

```
Remote Node
    │
    │  gRPC QueryShard (stream DataPoints)
    │
    ▼
Sync Manager
    │
    │  Batch 1000 points → dataPointsToWALEntries()
    │
    ▼
WriteToWAL(entries)
    │
    │  Same pipeline as normal writes
    │
    ▼
WAL → Flush → TieredStorage.WriteBatch()
    │                      │
    │                      ▼
    │             group_XXXX/V5 files
    │
    ▼
OnFlushComplete
    │
    ▼
Aggregation Pipeline
    │
    ├── Hourly → Daily → Monthly → Yearly
    │
    ▼
agg/agg_1h/, agg_1d/, agg_1M/, agg_1y/
```

> **Important**: Synced data goes through **WriteToWAL** — same pipeline as normal writes. Aggregation is automatically computed after flush.

## Integration with Storage Node

```
main()
  │
  ├── 1. Load config
  ├── 2. Connect to etcd
  ├── 3. Initialize storage (TieredStorage)
  │
  ├── 4. WAL Replay (existing data)
  │
  ├── 5. initializeSync()                    ◄── sync package
  │      ├── Create SyncManager
  │      ├── Create adapters (etcd, local storage, gRPC client)
  │      ├── syncManager.SyncOnStartup()     ◄── startup sync
  │      └── antiEntropy.Start(ctx)          ◄── background
  │
  ├── 6. Register with etcd
  ├── 7. Start NATS subscriber
  ├── 8. Start gRPC server
  └── 9. Serve requests
```

## Configuration

```yaml
sync:
  enabled: true                # Enable sync
```

**Default values:**

| Setting | Default | Description |
|---------|---------|-------------|
| `startup_sync` | `true` | Sync on node startup |
| `startup_timeout` | `5 min` | Max time for startup sync |
| `sync_batch_size` | `1000` | Points per WAL write batch |
| `max_concurrent_syncs` | `5` | Parallel group syncs |
| `anti_entropy.interval` | `1 hour` | Consistency check interval |
| `anti_entropy.checksum_window` | `24 hours` | Window for checksum comparison |

## Event System

```go
const (
    SyncEventStarted   SyncEventType = iota  // Sync started for a group/shard
    SyncEventProgress                         // Progress update (points synced)
    SyncEventCompleted                        // Sync completed successfully
    SyncEventFailed                           // Sync failed
)

manager.OnSyncEvent(func(event SyncEvent) {
    // event.Type, event.GroupID, event.ShardID, event.Progress, event.Error
})
```

## Known Limitations

1. **Simple checksum** — only compares `count:minTime:maxTime`, no content hash → may miss cases where data differs but count/time are the same
2. **Uses QueryShard RPC as fallback** — no dedicated optimized sync RPC for large-scale streaming yet
3. **Anti-Entropy only checks shard-level** — no `checkGroup()` for 4-tier architecture yet
4. **No incremental checksum** — each check must query the entire 24h window
5. **Group sync not yet optimized** — `syncGroupFromReplica()` syncs everything (doesn't use lastTimestamp)
