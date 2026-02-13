package sync

// Package sync provides data synchronization capabilities between storage nodes.
//
// The sync package handles:
// - Startup sync: When a node restarts, it syncs missing data from replicas
// - Anti-entropy: Background process that detects and repairs data inconsistencies
// - On-demand sync: Manual sync triggers for specific shards
//
// ============================================================================
// ARCHITECTURE OVERVIEW
// ============================================================================
//
//	┌─────────────────────────────────────────────────────────────────────────┐
//	│                         SYNC PACKAGE                                    │
//	│                                                                         │
//	│  ┌─────────────┐    ┌─────────────────┐    ┌──────────────────────┐    │
//	│  │   Manager   │───▶│  RemoteSyncClient│───▶│   Other Storage Nodes │   │
//	│  │             │    │   (GRPCClient)   │    │   (gRPC StorageService) │  │
//	│  └──────┬──────┘    └─────────────────┘    └──────────────────────┘    │
//	│         │                                                               │
//	│         │ uses                                                          │
//	│         ▼                                                               │
//	│  ┌─────────────┐    ┌─────────────────┐                                │
//	│  │LocalStorage │    │ MetadataManager │                                │
//	│  │  (Adapter)  │    │  (EtcdAdapter)  │                                │
//	│  └──────┬──────┘    └────────┬────────┘                                │
//	│         │                    │                                          │
//	│         ▼                    ▼                                          │
//	│  ┌─────────────┐    ┌─────────────────┐                                │
//	│  │Columnar    │    │     etcd        │                                │
//	│  │Storage +    │    │ (shard metadata)│                                │
//	│  │MemoryStore  │    └─────────────────┘                                │
//	│  └─────────────┘                                                        │
//	│                                                                         │
//	│  ┌─────────────┐                                                        │
//	│  │AntiEntropy  │  (Background goroutine)                               │
//	│  │  Service    │  - Periodic checksum comparison                       │
//	│  └─────────────┘  - Auto-repair on mismatch                            │
//	│                                                                         │
//	└─────────────────────────────────────────────────────────────────────────┘
//
// ============================================================================
// STARTUP SYNC FLOW
// ============================================================================
//
// When a storage node restarts, it needs to sync data it missed while down.
//
//	┌────────────────────────────────────────────────────────────────────────┐
//	│                    NODE STARTUP SEQUENCE                                │
//	└────────────────────────────────────────────────────────────────────────┘
//
//	  Storage Node                     etcd                    Replica Node
//	       │                             │                           │
//	       │ 1. WAL Replay (local)       │                           │
//	       │ ◄────────────────           │                           │
//	       │                             │                           │
//	       │ 2. SyncOnStartup()          │                           │
//	       │ ────────────────────────────►                           │
//	       │ GetMyShards(nodeID)         │                           │
//	       │ ◄────────────────────────────                           │
//	       │ [shard-1, shard-2, ...]     │                           │
//	       │                             │                           │
//	       │ 3. For each shard:          │                           │
//	       │    GetLastTimestamp()       │                           │
//	       │    (local storage)          │                           │
//	       │                             │                           │
//	       │ 4. GetActiveReplicas()      │                           │
//	       │ ────────────────────────────►                           │
//	       │ ◄────────────────────────────                           │
//	       │ [node-2, node-3]            │                           │
//	       │                             │                           │
//	       │ 5. SyncShard(shard, since)  │                           │
//	       │ ──────────────────────────────────────────────────────►│
//	       │                             │          gRPC QueryShard  │
//	       │ ◄──────────────────────────────────────────────────────│
//	       │         Stream DataPoints   │                           │
//	       │                             │                           │
//	       │ 6. WriteToWAL(entries)      │                           │
//	       │    (through WAL for durability)                         │
//	       │    WAL → MemoryStore → Flush to Storage                 │
//	       │                             │                           │
//	       │ 7. Register with etcd       │                           │
//	       │ ────────────────────────────►                           │
//	       │                             │                           │
//	       │ 8. Subscribe NATS           │                           │
//	       │    (start accepting writes) │                           │
//	       │                             │                           │
//
// ============================================================================
// ANTI-ENTROPY FLOW
// ============================================================================
//
// Background process that detects and repairs data inconsistencies.
// Runs periodically (default: every 1 hour).
//
//	┌────────────────────────────────────────────────────────────────────────┐
//	│                    ANTI-ENTROPY PROCESS                                 │
//	└────────────────────────────────────────────────────────────────────────┘
//
//	  AntiEntropy                      Local                     Replica
//	  Service                         Storage                     Node
//	       │                             │                           │
//	       │ 1. Get my shards            │                           │
//	       │ (from etcd)                 │                           │
//	       │                             │                           │
//	       │ 2. For each shard:          │                           │
//	       │    GetChecksum(shard, timeWindow)                       │
//	       │ ────────────────────────────►                           │
//	       │ ◄────────────────────────────                           │
//	       │    localChecksum = "abc123" │                           │
//	       │    localCount = 1000        │                           │
//	       │                             │                           │
//	       │ 3. GetChecksum from replica │                           │
//	       │ ──────────────────────────────────────────────────────►│
//	       │ ◄──────────────────────────────────────────────────────│
//	       │    remoteChecksum = "xyz789"│                           │
//	       │    remoteCount = 1050       │                           │
//	       │                             │                           │
//	       │ 4. Compare checksums        │                           │
//	       │    if localChecksum != remoteChecksum:                  │
//	       │                             │                           │
//	       │ 5. Trigger repair           │                           │
//	       │    syncManager.SyncShard()  │                           │
//	       │ ──────────────────────────────────────────────────────►│
//	       │         Stream missing data │                           │
//	       │ ◄──────────────────────────────────────────────────────│
//	       │                             │                           │
//	       │ 6. WriteBatch               │                           │
//	       │ ────────────────────────────►                           │
//	       │                             │                           │
//	       │ 7. Sleep(interval)          │                           │
//	       │    goto step 1              │                           │
//	       │                             │                           │
//
// ============================================================================
// SYNC SHARD FLOW (DETAILED)
// ============================================================================
//
//	                    syncShard(ctx, shard)
//	                           │
//	                           ▼
//	           ┌───────────────────────────────┐
//	           │ 1. Get last local timestamp   │
//	           │    localStorage.GetLastTS()   │
//	           └───────────────┬───────────────┘
//	                           │
//	                           ▼
//	           ┌───────────────────────────────┐
//	           │ 2. Already up to date?        │
//	           │    lastLocalTS >= shardEndTS  │
//	           └───────────────┬───────────────┘
//	                           │
//	              ┌────────────┴────────────┐
//	              │ Yes                     │ No
//	              ▼                         ▼
//	        ┌───────────┐     ┌───────────────────────────────┐
//	        │  Return   │     │ 3. Find active replicas       │
//	        │  (skip)   │     │    metadataManager.           │
//	        └───────────┘     │    GetActiveReplicas()        │
//	                          └───────────────┬───────────────┘
//	                                          │
//	                                          ▼
//	                          ┌───────────────────────────────┐
//	                          │ 4. For each replica, try sync │
//	                          └───────────────┬───────────────┘
//	                                          │
//	                                          ▼
//	                          ┌───────────────────────────────┐
//	                          │ 5. syncFromReplica()          │
//	                          │    - Create SyncRequest       │
//	                          │    - Stream data via gRPC     │
//	                          │    - Batch write to local     │
//	                          └───────────────┬───────────────┘
//	                                          │
//	                              ┌───────────┴───────────┐
//	                              │ Success               │ Failure
//	                              ▼                       ▼
//	                        ┌───────────┐     ┌───────────────────┐
//	                        │  Return   │     │ Try next replica  │
//	                        │  success  │     │ or return error   │
//	                        └───────────┘     └───────────────────┘
//
// ============================================================================
// DATA POINT STREAMING FLOW
// ============================================================================
//
//	┌─────────────────────────────────────────────────────────────────────────┐
//	│                    syncFromReplica()                                    │
//	└─────────────────────────────────────────────────────────────────────────┘
//
//	  Manager              GRPCClient              Replica Node
//	     │                      │                       │
//	     │ SyncShard(addr, req) │                       │
//	     │ ─────────────────────►                       │
//	     │                      │ gRPC QueryShard()     │
//	     │                      │ ──────────────────────►
//	     │                      │ ◄──────────────────────
//	     │                      │   QueryShardResponse   │
//	     │                      │                       │
//	     │   ◄──── pointsCh ─── │ (buffered channel)    │
//	     │   ◄──── errCh ────── │                       │
//	     │                      │                       │
//	     │ for point := range pointsCh:                 │
//	     │   batch = append(batch, point)               │
//	     │   if len(batch) >= batchSize:                │
//	     │     entries = dataPointsToWALEntries(batch)  │
//	     │     localStorage.WriteToWAL(entries)         │
//	     │       └─► WriteWorkerPool.Submit()           │
//	     │           └─► WAL.WritePartitioned()         │
//	     │               └─► MemoryStore.Write()        │
//	     │                   └─► (async) Flush to disk  │
//	     │     batch = batch[:0]                        │
//	     │     emit ProgressEvent                       │
//	     │                                              │
//	     │ // Write remaining                           │
//	     │ localStorage.WriteToWAL(remaining entries)   │
//	     │                                              │
//	     │ check errCh for errors                       │
//	     │                                              │
//
// ============================================================================
// EVENT SYSTEM
// ============================================================================
//
// The sync package emits events for monitoring and debugging:
//
//	SyncEventType:
//	  - SyncEventStarted   : Sync operation started for a shard
//	  - SyncEventProgress  : Progress update (points synced)
//	  - SyncEventCompleted : Sync completed successfully
//	  - SyncEventFailed    : Sync failed with error
//
//	Usage:
//	  manager.OnSyncEvent(func(event SyncEvent) {
//	      switch event.Type {
//	      case SyncEventStarted:
//	          log.Printf("Sync started for shard %s", event.ShardID)
//	      case SyncEventProgress:
//	          log.Printf("Progress: %d points synced", event.Progress.SyncedPoints)
//	      case SyncEventCompleted:
//	          log.Printf("Sync completed for shard %s", event.ShardID)
//	      case SyncEventFailed:
//	          log.Printf("Sync failed: %v", event.Error)
//	      }
//	  })
//
// ============================================================================
// INTEGRATION WITH STORAGE NODE
// ============================================================================
//
//	┌─────────────────────────────────────────────────────────────────────────┐
//	│                    STORAGE NODE STARTUP                                 │
//	└─────────────────────────────────────────────────────────────────────────┘
//
//	  main()
//	    │
//	    ▼
//	  ┌───────────────────────────────┐
//	  │ 1. Load config               │
//	  │ 2. Connect to etcd           │
//	  │ 3. Initialize storage        │
//	  └───────────────┬───────────────┘
//	                  │
//	                  ▼
//	  ┌───────────────────────────────┐
//	  │ 4. WAL Replay (existing)      │
//	  │    replayWAL()                │
//	  └───────────────┬───────────────┘
//	                  │
//	                  ▼
//	  ┌───────────────────────────────┐
//	  │ 5. Sync from Replicas (NEW)   │◄─────── sync package
//	  │    syncManager.SyncOnStartup()│
//	  └───────────────┬───────────────┘
//	                  │
//	                  ▼
//	  ┌───────────────────────────────┐
//	  │ 6. Register with etcd         │
//	  │    nodeRegistration.Register()│
//	  └───────────────┬───────────────┘
//	                  │
//	                  ▼
//	  ┌───────────────────────────────┐
//	  │ 7. Start NATS subscriber      │
//	  │    subscriber.Start()         │
//	  └───────────────┬───────────────┘
//	                  │
//	                  ▼
//	  ┌───────────────────────────────┐
//	  │ 8. Start Anti-Entropy (NEW)   │◄─────── sync package
//	  │    antiEntropy.Start()        │
//	  └───────────────┬───────────────┘
//	                  │
//	                  ▼
//	  ┌───────────────────────────────┐
//	  │ 9. Start gRPC server          │
//	  │    grpcServer.Start()         │
//	  └───────────────────────────────┘
//

// Version is the sync package version
const Version = "1.0.0"

// BuildInfo returns build information
func BuildInfo() map[string]string {
	return map[string]string{
		"version": Version,
		"package": "github.com/soltixdb/soltix/internal/sync",
	}
}
