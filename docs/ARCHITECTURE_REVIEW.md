# Architecture Review — SoltixDB (April 2026)

## Executive Summary

SoltixDB is a distributed time-series database (~34.8K LoC Go) with a well-structured codebase organized into 21 internal packages. The system uses a **2-service architecture** (Router + Storage) with etcd for metadata/coordination and NATS/Redis/Kafka for async message passing.

This review documents the current architecture, identifies bugs and potential issues, and proposes improvements.

## Current Architecture

### Service Architecture

```
┌─────────────────────────────────────────────────┐
│                  Clients                         │
│         (REST API / SSE Streaming)               │
└──────────────────┬──────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────┐
│            Router Service (Fiber HTTP)            │
│  Port 5555 │ API Gateway + Query Coordinator     │
│  ┌─────────────────────────────────────────────┐ │
│  │ handlers/  → write, query, stream, forecast │ │
│  │ coordinator/ → shard routing, group assign  │ │
│  │ middleware/ → auth (API key), logging       │ │
│  │ services/ → download, stream                │ │
│  │ analytics/ → anomaly, forecasting           │ │
│  │ downsampling/ → LTTB, MinMax, M4            │ │
│  └─────────────────────────────────────────────┘ │
└──────┬────────────────────┬──────────────────────┘
       │ Publish             │ gRPC Query
       ▼                     ▼
┌────────────┐   ┌──────────────────────────────────┐
│ Message Q  │   │     Storage Service (gRPC)        │
│ NATS/Redis │   │  Port 5556 │ Data Engine          │
│ /Kafka     │   │  ┌──────────────────────────────┐ │
└────────────┘   │  │ subscriber/ → consume writes │ │
       │         │  │ wal/ → Partitioned WAL       │ │
       └────────►│  │ storage/ → TieredStorage     │ │
                 │  │ aggregation/ → 1h/1d/1M/1y   │ │
                 │  │ compression/ → Snappy         │ │
                 │  │ sync/ → replica sync          │ │
                 │  │ registry/ → node registration │ │
                 │  └──────────────────────────────┘ │
                 └──────────────┬───────────────────┘
                                │
                    ┌───────────┴───────────┐
                    │     etcd (v3.5+)      │
                    │ Metadata / Discovery  │
                    └───────────────────────┘
```

### Key Design Decisions

| Decision | Rationale | Trade-off |
|----------|-----------|-----------|
| Timezone-native storage | Solar plants are location-fixed; pre-compute agg at local TZ boundaries | Loses cross-timezone flexibility |
| Queue-decoupled writes | Router publishes to MQ, Storage consumes → natural back-pressure | Higher latency vs direct write |
| V6 columnar format | Single-file parts with footer index; eliminates CG directories | Larger files if many fields |
| WAL group commit | ~137ns async write via 10ms batch flush | Up to 10ms data loss window |
| 3-tier storage | Group → Device Group → Partition (time-based) | Complex but efficient |

### Data Flow

#### Write Path
```
Client → Router.Write() → Validate → RouteByDevice (hash) →
  Publish to NATS (per-node subject) →
  Storage.Subscribe() → WriteWorkerPool → WAL.WritePartitioned() →
  MemoryStore (if recent) → FlushWorkerPool → TieredStorage.WriteBatch() →
  AggregationPipeline (1h → 1d → 1M → 1y)
```

#### Read Path
```
Client → Router.Query() → QueryCoordinator →
  gRPC QueryShard to Storage nodes →
  MemoryStore (hot) + TieredStorage (warm) + AggStorage (pre-computed) →
  Merge + Downsample + AnomalyDetect → Stream Response
```

## Identified Issues

See GitHub Issues for detailed tracking. Summary:

### Critical Bugs
1. **NaN/Inf propagation in aggregation** — No NaN/Inf checks on float values
2. **WAL timestamp parsing inconsistency** — service.go uses RFC3339, walEntryToDataPoint uses Timestamp (nanos)
3. **Aggregation cascade re-reads entire time range** — hourly reads full day on every flush

### Potential Data Integrity Issues
4. **Simple checksum in anti-entropy** — count:minTime:maxTime misses data corruption
5. **MemoryStore eviction can lose unflushed data** — evictOldest only targets FlushStatusFlushed but race exists
6. **FlushWorker pendingCount reset to 0 after flush** — ignores notifications received during flush

### Performance Concerns
7. **aggregateHourly re-reads entire day** — should read only changed hours
8. **preemptIdleWorker iterates all workers** — O(N) scan under RLock
9. **evictOldest collects all flushed items** — massive allocation on large stores

### Missing Features
10. **DeletePoints not implemented** — returns 501
11. **LZ4/Zstd compression defined but not implemented**
12. **No metrics/tracing** — No Prometheus or OpenTelemetry integration
