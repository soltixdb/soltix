# Architecture — Soltix Time Series Database

This document describes the overall architecture of Soltix, a distributed time series database.

## Overview

Soltix is a **distributed time series database** designed for:
- **High throughput**: millions of data points per second
- **Low latency**: millisecond-level query response
- **Horizontal scalability**: add nodes without downtime
- **High availability**: automatic replica failover
- **Cost-effective storage**: data compression (up to 90%), tiered storage (hot/warm/cold)

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           SOLTIX CLUSTER                                    │
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────┐      │
│  │                     ROUTING LAYER (Router)                       │      │
│  │  - Client requests → shard routing → storage node               │      │
│  │  - Hash-based sharding: hash(device_id) % num_shards            │      │
│  │  - Load balancing, failover                                     │      │
│  └──────────────────────────────────────────────────────────────────┘      │
│                    │                                  │                     │
│                    ▼                                  ▼                     │
│  ┌──────────────────────────┐  ┌──────────────────────────┐                │
│  │    Storage Node (0)      │  │   Storage Node (1)       │   ...          │
│  │                          │  │                          │                │
│  │  ┌────────────────────┐  │  │  ┌────────────────────┐  │                │
│  │  │  Write-Ahead Log   │  │  │  │  Write-Ahead Log   │  │                │
│  │  │  (WAL)             │  │  │  │  (WAL)             │  │                │
│  │  └────┬───────────────┘  │  │  └────┬───────────────┘  │                │
│  │       │                  │  │       │                  │                │
│  │       ▼                  │  │       ▼                  │                │
│  │  ┌────────────────────┐  │  │  ┌────────────────────┐  │                │
│  │  │  Aggregation       │  │  │  │  Aggregation       │  │                │
│  │  │  Pipeline          │  │  │  │  Pipeline          │  │                │
│  │  │  (1h→1d→1M→1y)     │  │  │  │  (1h→1d→1M→1y)     │  │                │
│  │  └────┬───────────────┘  │  │  └────┬───────────────┘  │                │
│  │       │                  │  │       │                  │                │
│  │       ▼                  │  │       ▼                  │                │
│  │  ┌────────────────────┐  │  │  ┌────────────────────┐  │                │
│  │  │  Tiered Storage    │  │  │  │  Tiered Storage    │  │                │
│  │  │  Hot (Memory)      │  │  │  │  Hot (Memory)      │  │                │
│  │  │  Warm (Local SSD)  │  │  │  │  Warm (Local SSD)  │  │                │
│  │  │  Cold (S3/Remote)  │  │  │  │  Cold (S3/Remote)  │  │                │
│  │  └────────────────────┘  │  │  └────────────────────┘  │                │
│  │                          │  │                          │                │
│  └──────────────────────────┘  └──────────────────────────┘                │
│          │              ◄──────────────────────────────────────┐           │
│          │                                                     │           │
│          └────────────────────────────┬─────────────────────────┘           │
│                                       │                                     │
│                                       ▼                                     │
│                    ┌──────────────────────────────┐                         │
│                    │   Metadata Store (etcd)      │                         │
│                    │                              │                         │
│                    │ - Shard/group mappings       │                         │
│                    │ - Node registration          │                         │
│                    │ - Leadership                 │                         │
│                    │ - Aggregation config         │                         │
│                    │ - Retention policies         │                         │
│                    └──────────────────────────────┘                         │
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────┐      │
│  │            Message Bus (NATS)                                   │      │
│  │  - Asynchronous writes from clients                            │      │
│  │  - Sync events between nodes                                   │      │
│  │  - System notifications                                        │      │
│  └──────────────────────────────────────────────────────────────────┘      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Router (Entry Point)

Handles all client requests:

| Function | Description |
|----------|-------------|
| **Client Request Handling** | Receive write/query from client (gRPC) |
| **Shard Routing** | Determine target storage node via hash(device_id) |
| **Load Balancing** | Route to least-loaded shard/replica |
| **Failover** | If primary down, route to replica |
| **Streaming** | Stream results back to client (gRPC streaming) |

### 2. Storage Node

Each storage node is a complete processing unit:

#### 2.1 Write Path

```
Client Request
  │
  ├─ Parse data (device_id, timestamp, value, fields)
  │
  ├─ Validate (schema, timestamp range)
  │
  ├─ WAL (Write-Ahead Log)
  │  └─ Durable write to disk before accepting
  │
  ├─ Memory (In-Memory Buffer)
  │  └─ Temporary storage for quick queries
  │
  ├─ Flush Trigger (when buffer full or time-based)
  │  └─ TieredStorage.WriteBatch()
  │
  ├─ Flush Handler
  │  ├─ Compress data (bitstream)
  │  ├─ Write to warm storage (local SSD)
  │  └─ OnFlushComplete event
  │
  ├─ Aggregation Pipeline
  │  ├─ 1h aggregate (sum, avg, min, max)
  │  ├─ 1d aggregate (reuse 1h data)
  │  ├─ 1M aggregate (reuse 1d data)
  │  └─ 1y aggregate (reuse 1M data)
  │
  └─ Done
```

#### 2.2 Read Path

```
Client Query (device_id, timerange, fields)
  │
  ├─ Router.Query()
  │  ├─ Lookup metadata (device_id → shard)
  │  └─ Route to Storage
  │
  ├─ Storage.ExecuteQuery()
  │  ├─ Memory (latest data)
  │  ├─ Warm storage (SSD, compressed)
  │  ├─ Cold storage (S3, compressed)
  │  └─ Merge + Decompress
  │
  ├─ Process Pipeline
  │  ├─ Apply aggregation (1h, 1d, etc.)
  │  ├─ Apply downsampling
  │  ├─ Field selection/filtering
  │  └─ Sorting
  │
  ├─ Format Response
  │  └─ DataPoints array (timestamp, value, fields)
  │
  └─ Stream back to client
```

### 3. Write-Ahead Log (WAL)

Ensures durability and crash recovery:

- **File format**: Sequential append-only log of entries
- **Entry types**: Write, Flush, Delete, etc.
- **Replay on startup**: Read WAL → reconstruct in-memory state
- **Rotation**: New WAL file periodically to prevent unbounded growth
- **Compression**: WAL can be compressed after rotation

### 4. Tiered Storage

3-tier storage for cost optimization:

| Tier | Storage | Access Speed | Retention | Usage |
|------|---------|--------------|-----------|-------|
| **Hot** | Memory (DRAM) | ns | 1-5 min | Latest uncompressed data |
| **Warm** | Local SSD | µs-ms | 7-30 days | Compressed, frequently accessed |
| **Cold** | S3/Remote | seconds-min | 1+ year | Compressed, rarely accessed |

Data automatically migrates from hot → warm → cold based on age.

### 5. Aggregation Pipeline

Computes multiple aggregation levels:

```
Raw 1-second data
  │
  ├─ Hourly aggregation (avg, sum, min, max, count)
  │  └─ 1 data point/hour/device
  │
  ├─ Daily aggregation (from hourly data)
  │  └─ 1 data point/day/device
  │
  ├─ Monthly aggregation (from daily data)
  │  └─ 1 data point/month/device
  │
  └─ Yearly aggregation (from monthly data)
     └─ 1 data point/year/device
```

**Benefits:**
- Reduces storage (e.g., 1 year = 365 points instead of 31M)
- Enables fast long-term queries
- Automatic background process

### 6. Metadata Store (etcd)

Distributed coordination:

| Data | Purpose |
|------|---------|
| Shard assignments | Which storage node owns shard X |
| Group mappings | Devices → groups (for 4-tier sync) |
| Node registry | Active storage nodes |
| Leader election | Coordinator leadership |
| Policies | Retention, aggregation config |

## 4-Tier Architecture (Shard Groups)

Data is organized into hierarchical groups for efficient sync and management:

```
Level 0: Shard 0000-0999
  │
  ├─ Group 0 (devices 0-99)
  │  ├─ Collection: raw
  │  ├─ Replicas: [node-1, node-2, node-3]
  │  └─ Data files: agg/group_0000/v5_*.colstore
  │
  ├─ Group 1 (devices 100-199)
  │  └─ ...
  │
  └─ Group N (devices N*100-(N+1)*100)
     └─ ...

Level 1: Shard 1000-1999
  └─ Similar structure

Level 2: ... (more shards)

Level 3: ... (organization by time range)
```

**Advantages:**
- Sync between replicas by group (more granular than shard)
- Easier failure recovery (only missing group, not entire shard)
- Better load balancing (can migrate individual groups)

## Data Flow

### 1. Write Flow

```
Client (IoT Device / Sensor)
  │
  ├─ MQTT / gRPC Write Request
  │  └─ device_id, timestamp, value, fields
  │
  ▼
Router (Load Balanced)
  │
  ├─ Parse request
  ├─ Compute shard: hash(device_id) % num_shards
  ├─ Lookup shard leader (from etcd)
  ├─ Route to Storage Node (primary)
  │
  ├─ Retry on failure (fallback to replicas)
  │
  └─ Return ACK
  │
  ▼
Storage Node (Primary)
  │
  ├─ Validate data (schema, timestamp, value range)
  │
  ├─ WAL Write
  │  └─ Append to write-ahead log (atomic)
  │
  ├─ Memory Buffer
  │  └─ Add to in-memory store
  │
  ├─ Replication (async)
  │  └─ Publish to NATS
  │  └─ Replicas consume and apply
  │
  ├─ Buffer Full or Timeout?
  │  ├─ YES: Trigger Flush
  │  └─ NO: Wait for next event
  │
  └─ Continue...

Flush Handler (Async)
  │
  ├─ Get buffered data (sorted by time)
  │
  ├─ Compress
  │  └─ BitStream encoding (90% compression)
  │
  ├─ Write to Warm Storage
  │  └─ /data/group_XXXX/V5_YYYY.colstore (columnar format)
  │
  ├─ Trigger Aggregation
  │  ├─ Compute 1h aggregates
  │  ├─ Compute 1d aggregates (from 1h)
  │  ├─ Compute 1M aggregates (from 1d)
  │  └─ Compute 1y aggregates (from 1M)
  │
  └─ Archive to Cold Storage (if retention policy)
     └─ Upload to S3/external storage
```

### 2. Query Flow

```
Client Query
  │
  ├─ Query params
  │  └─ device_id, start_time, end_time, aggregation (1h/1d/1M/1y), fields
  │
  ▼
Router
  │
  ├─ Lookup shard (hash(device_id) % num_shards)
  ├─ Get replicas from etcd
  ├─ Route to available replica
  │
  └─ → Storage Node

Storage Node Query Handler
  │
  ├─ Determine aggregation level
  │  ├─ If query range < 24h: use raw/1h data
  │  ├─ If query range < 1 month: use 1d data
  │  ├─ If query range < 1 year: use 1M data
  │  └─ If query range > 1 year: use 1y data
  │
  ├─ Fetch from Storage Tiers
  │  ├─ Memory (hit rate ~90% for recent queries)
  │  ├─ Warm SSD (decompression + filtering)
  │  └─ Cold S3 (parallel fetch, decompression)
  │
  ├─ Merge Results
  │  └─ Combine data from multiple tiers
  │
  ├─ Apply Filters
  │  ├─ Device ID filtering
  │  ├─ Field selection (only requested fields)
  │  ├─ Value range filtering (min/max/avg/sum)
  │  └─ Sorting (by timestamp)
  │
  ├─ Stream Response
  │  ├─ Chunk size: 1000 points / message
  │  ├─ Use gRPC streaming
  │  └─ Send back to client
  │
  └─ Client Receives
     └─ DataPoints stream (async, non-blocking)
```

## Replication & High Availability

### Replica Strategy

```
Primary Node
  │
  ├─ Write data to WAL (durable)
  │
  ├─ Publish write event to NATS
  │
  └─ ACK to client (before replicas confirm!)
     │
     ▼
  Replica 1, Replica 2, Replica 3
     │
     ├─ Consume event from NATS
     │
     ├─ Apply to own WAL
     │
     ├─ Apply to memory buffer
     │
     └─ Done (async, no ACK back to client)
```

**Model:** Write acknowledgment is sent to client **before** replicas confirm (eventually consistent).

**Failover:**
- Router detects primary down (timeout / gRPC error)
- Automatically routes to replica
- Replica promoted to primary (leader election via etcd)
- Lost writes (if < replica replication latency) are ignored

### Sync on Startup

When a replica starts:
1. Connects to etcd, gets group assignments
2. Contacts primary (or first available replica)
3. Streams missing data from primary → writes to own WAL
4. Replays WAL to restore in-memory state
5. Registers as ready with etcd

## Configuration & Deployment

### Configuration File (config.yaml)

```yaml
server:
  port: 50051              # gRPC port
  max_connections: 10000

storage:
  data_dir: /data          # Local data directory
  retention_days: 365      # 1-year retention

wal:
  enabled: true
  rotation_size: 1GB       # New WAL file every 1GB
  rotation_time: 24h

aggregation:
  enabled: true
  levels: [1h, 1d, 1M, 1y]

metadata:
  etcd_urls:
    - http://etcd-0:2379
    - http://etcd-1:2379
    - http://etcd-2:2379

message_bus:
  nats_url: nats://nats:4222

compression:
  enabled: true
  method: bitstream        # Or: snappy, zstd
  ratio: 0.10              # Target 90% compression (10% size)
```

### Deployment

```
Kubernetes Pod (Storage Node)
  │
  ├─ Init Container
  │  ├─ Format disk
  │  └─ Create directories
  │
  ├─ Main Container
  │  ├─ Storage Service (binary)
  │  ├─ Mount /data (PersistentVolume)
  │  ├─ Mount /config (ConfigMap)
  │  └─ Port 50051 (gRPC)
  │
  ├─ Liveness Probe
  │  └─ gRPC health check every 10s
  │
  ├─ Readiness Probe
  │  └─ Check etcd registration
  │
  └─ Graceful Shutdown
     └─ Stop writes, flush buffer, exit
```

## Performance Characteristics

### Write Performance

| Metric | Value |
|--------|-------|
| **Throughput** | 100k+ writes/sec (per node) |
| **Latency (p99)** | < 10ms (WAL write) |
| **Replication latency** | < 100ms (async to replicas) |

### Query Performance

| Scenario | Latency (p99) |
|----------|--------------|
| Recent data (memory hit) | < 5ms |
| Historical data (SSD) | 10-50ms |
| Cold data (S3) | 100ms - 1s |
| Large result set (streaming) | Depends on network |

### Storage

| Compression | Size | Retention |
|-------------|------|-----------|
| Raw 1-second data | 100% | 7 days |
| 1h aggregates | ~1% | 365 days |
| 1d aggregates | ~0.1% | 5 years |
| Total | ~10% | Mixed |

## Extension Points

1. **Custom Aggregation Functions**: Implement `Aggregator` interface
2. **Storage Backends**: Pluggable storage tier (S3, GCS, local)
3. **Compression Algorithms**: Add new encoder (BitStream, Snappy, etc.)
4. **Message Bus**: Switch NATS to Kafka, RabbitMQ
5. **Metadata Store**: Alternative to etcd (Consul, ZooKeeper)

## Known Limitations

1. **Write consistency**: Eventually consistent (not strict consistency)
2. **Multi-shard transactions**: Not supported (single-shard only)
3. **Full-text search**: Not supported (only numeric queries)
4. **Real-time aggregation**: Fixed levels (1h, 1d, etc.), not custom intervals
5. **Geographic distribution**: No built-in geo-replication (can be added via custom shard placement)
