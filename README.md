# Soltix - Distributed Time-Series Database

[![Go Version](https://img.shields.io/badge/Go-1.25+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)]()

A high-performance distributed time-series database built with Go, designed for IoT and solar panel monitoring systems. Soltix uses a **MapReduce paradigm** with sharding, replication, and multi-level aggregation to handle millions of data points efficiently.

## Why Soltix?

Soltix is purpose-built for **solar plant monitoring** — a domain where every installation is physically fixed at a single geographic location with a permanent timezone.

### Design Philosophy: Timezone-Native Storage

Unlike general-purpose time-series databases that store everything in UTC and convert at query time, Soltix takes a **timezone-native** approach:

```
┌─────────────────────────────────────────────────────────────┐
│  General-purpose TSDB          │  Soltix                    │
│                                │                            │
│  Store in UTC                  │  Store in local timezone   │
│  Re-aggregate at query time    │  Pre-computed aggregates   │
│  Flexible but slower queries   │  O(1) seek, zero re-agg    │
│  Complex timezone edge cases   │  Simple, correct by design │
└─────────────────────────────────────────────────────────────┘
```

**Why this matters for solar plants:**

- **Data is location-bound** — A solar plant in Tokyo is always in JST. It will never move to another timezone. All reporting (daily yield, monthly production, yearly capacity factor) is inherently tied to local sunrise/sunset cycles.
- **"Today" means local today** — When an operator asks _"What was yesterday's total output?"_, the answer must be computed from `00:00` to `23:59` in the plant's local time, not UTC. Pre-computing aggregates in the correct timezone eliminates query-time re-aggregation entirely.
- **One cluster per site (or timezone)** — Each Soltix deployment serves a single site or a group of sites sharing the same timezone. This keeps the storage engine simple, fast, and free of multi-timezone complexity.

```yaml
# Each cluster is configured with a single timezone
storage:
  timezone: "Asia/Tokyo"   # All aggregation aligns to this timezone
```

This architectural decision yields:

| Benefit | Detail |
|---|---|
| **Zero re-aggregation** | Daily/monthly/yearly rollups are pre-computed at correct local boundaries — queries read directly |
| **Intuitive disk layout** | `agg_1d/{db}/{col}/2026/01/13/` = January 13th in local time, easy to inspect and debug |
| **Correct by construction** | No edge cases from UTC→local conversion at daily/monthly boundaries |
| **Maximum query speed** | Aggregated queries are a single file seek — no hourly-bucket merging needed |

> **Note**: For cross-site analytics (comparing plants across different timezones), this is handled at the application layer or by a dedicated analytics service that queries multiple Soltix clusters.

## Features

### Core Capabilities
- **High Performance** - Handles 10K+ writes/sec and sub-100ms query latency
- **Multi-Level Aggregation** - Pre-computed 1h/1d/1mo/1y aggregates for fast analytics
- **Horizontal Scalability** - Add storage nodes to scale linearly
- **Efficient Storage** - Snappy compression + multi-part columnar format
- **MapReduce Queries** - Parallel query execution across shards
- **Hot/Cold Data Separation** - Memory cache for hot data, disk for cold data
- **API Authentication** - Secure API key authentication
- **Timezone Support** - Store and query data in any timezone (JST, UTC, etc.)

### Advanced Analytics
- **Anomaly Detection** - Built-in Z-Score, IQR, Moving Average algorithms with auto-detection
- **Forecasting** - SMA, Exponential Smoothing, Holt-Winters, Linear Regression, ARIMA, Prophet-style, and ML-based models
- **Downsampling** - LTTB, MinMax, Average, M4 algorithms with auto-detection
- **Streaming Queries** - Server-Sent Events (SSE) for large dataset streaming

### Architecture Highlights
- **Router Service** - HTTP REST API + Query coordinator
- **Storage Service** - gRPC server + message queue consumer + multi-tier storage
- **Consistent Hashing** - Rendezvous hashing for even data distribution
- **Time-Based Sharding** - Daily shards for efficient time-range queries
- **Concurrent I/O** - Parallel memory + file storage queries
- **WAL + Batching** - Write-ahead log for durability, batch flush for performance
- **Multi-Queue Support** - NATS, Redis Streams, Kafka

## Architecture

```
┌──────────────────────────────────────────────┐
│              Client Applications              │
└───────────────────┬──────────────────────────┘
                    │
                    ↓
┌──────────────────────────────────────────────┐
│         Router Service (HTTP API)            │
│  ┌────────────────────────────────────────┐  │
│  │  ShardRouter + QueryCoordinator        │  │
│  │  - Hash-based sharding                 │  │
│  │  - Node selection (Consistent Hash)    │  │
│  │  - Query aggregation                   │  │
│  │  - Anomaly Detection + Forecasting     │  │
│  │  - Streaming (SSE) + Downloads         │  │
│  └────────────────────────────────────────┘  │
└───────────────────┬──────────────────────────┘
                    │
          ┌─────────┴─────────┐
          │   Message Queue   │
          │ (NATS/Redis/Kafka)│
          └─────────┬─────────┘
                    │
       ┌────────────┼────────────┐
       │            │            │
       ↓            ↓            ↓
  ┌─────────┐ ┌─────────┐ ┌─────────┐
  │Storage-1│ │Storage-2│ │Storage-3│
  │Group    │ │Group    │ │Group    │
  │0,3,6    │ │1,4,7    │ │2,5,8    │
  │Queue Sub│ │Queue Sub│ │Queue Sub│
  │gRPC Srv │ │gRPC Srv │ │gRPC Srv │
  └────┬────┘ └────┬────┘ └────┬────┘
       │           │           │
   ┌───┴───┐   ┌───┴───┐   ┌───┴───┐
   │Memory │   │Memory │   │Memory │
   │  +    │   │  +    │   │  +    │
   │ WAL   │   │ WAL   │   │ WAL   │
   │  +    │   │  +    │   │  +    │
   │ File  │   │ File  │   │ File  │
   └───────┘   └───────┘   └───────┘
```

## Quick Start

### Prerequisites

- Go 1.25 or higher
- etcd 3.5+ (for service discovery)
- NATS Server 2.10+ (for message queue, or Redis/Kafka)

### Installation

```bash
# Clone repository
git clone https://github.com/soltixdb/soltix.git
cd soltix

# Install dependencies
go mod download

# Build services
make build

# Binaries will be in bin/
# - bin/router  (HTTP API service)
# - bin/storage (Storage service)
```

### Running

```bash
# Start infrastructure (etcd + NATS)
docker-compose up -d etcd nats

# Start storage node
./bin/storage -config configs/config.yaml

# Start router
./bin/router -config configs/config.yaml
```

## API Authentication

Soltix supports API key authentication for production deployments.

### Enable Authentication

```yaml
# configs/config.yaml
auth:
  enabled: true
  api_keys:
    - "your-secret-api-key-1"
    - "your-secret-api-key-2"
```

### Generate API Key

```bash
openssl rand -hex 32
```

### Making Authenticated Requests

```bash
# Using X-API-Key header
curl -H "X-API-Key: your-secret-api-key-1" \
     http://localhost:5555/v1/databases

# Using Authorization Bearer
curl -H "Authorization: Bearer your-secret-api-key-1" \
     http://localhost:5555/v1/databases
```

## Configuration

See `configs/config.yaml.sample` for a complete reference with all options.

## Development

### Build

```bash
# Build all services (current platform)
make build

# Build router only
make build-router

# Build storage only
make build-storage

# Build for all platforms (Linux, macOS Intel, macOS ARM)
make build-all
```

### Test

```bash
# Run all tests with race detection
make test

# Run tests without race detection
make test-short

# Show test coverage in browser
make test-coverage

# Run specific package tests
go test -v ./internal/storage/...
```

### Code Quality

```bash
# Format code
make fmt

# Run linter (requires golangci-lint)
make lint

# Tidy dependencies
make tidy
```

### Generate Protobuf

```bash
make proto
```

## Performance Benchmarks

### Write Performance
- **Single Write**: ~1ms latency
- **Batch Write (1000 points)**: ~50ms latency (20K writes/sec)
- **Memory Usage**: ~200 bytes per data point

### Query Performance
- **Single Device**: 10-50ms (hot data), 50-200ms (cold data)
- **Multi-Device (100)**: 200-500ms
- **Aggregated Query (1 day hourly)**: 100-300ms

### Storage Efficiency
- **Compression Ratio**: 5:1 (Snappy compression)
- **Disk I/O**: Batch flush reduces disk ops by 90%
- **Memory Cache Hit Rate**: 95%+ for hot data

## Docker Deployment

```bash
# Build images
docker build -f Dockerfile.router -t soltix-router:latest .
docker build -f Dockerfile.storage -t soltix-storage:latest .

# Start full cluster (etcd + NATS + storage + router + ml-service)
docker-compose up -d

# Check logs
docker-compose logs -f router
docker-compose logs -f storage
```

The `docker-compose.yml` includes etcd, NATS, router, storage, and the ML service. For Kafka-based deployments, see `docker-compose.kafka.yml`.

## API Reference

### Health
| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/health` | No | Health check |

### Database Operations
| Method | Path | Description |
|--------|------|-------------|
| POST | `/v1/databases` | Create database |
| GET | `/v1/databases` | List databases |
| GET | `/v1/databases/:database` | Get database |
| DELETE | `/v1/databases/:database` | Delete database |

### Collection Operations
| Method | Path | Description |
|--------|------|-------------|
| POST | `/v1/databases/:database/collections` | Create collection |
| GET | `/v1/databases/:database/collections` | List collections |
| GET | `/v1/databases/:database/collections/:collection` | Get collection |
| DELETE | `/v1/databases/:database/collections/:collection` | Delete collection |

### Data Operations
| Method | Path | Description |
|--------|------|-------------|
| POST | `/v1/databases/:database/collections/:collection/write` | Write single point |
| POST | `/v1/databases/:database/collections/:collection/write/batch` | Batch write |
| DELETE | `/v1/databases/:database/collections/:collection/delete` | Delete data |

### Query
| Method | Path | Description |
|--------|------|-------------|
| GET | `/v1/databases/:database/collections/:collection/query` | Query (GET) |
| POST | `/v1/databases/:database/collections/:collection/query` | Query (POST) |

**Query Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `start_time` | Yes | Start time (RFC3339) |
| `end_time` | Yes | End time (RFC3339) |
| `ids` | No | Comma-separated device IDs |
| `fields` | No | Comma-separated field names |
| `limit` | No | Max data points (0 = no limit) |
| `interval` | No | Aggregation interval (1m, 5m, 1h, 1d, 1mo, 1y) |
| `aggregation` | No | Function (sum, avg, min, max, count) |
| `downsampling` | No | Algorithm (none, auto, lttb, minmax, avg, m4) |
| `downsampling_threshold` | No | Target number of points (0 = auto) |
| `anomaly_detection` | No | Enable anomaly detection (true/false) |
| `anomaly_threshold` | No | Anomaly sensitivity threshold |
| `anomaly_field` | No | Field to run anomaly detection on |

### Streaming Query (SSE)
| Method | Path | Description |
|--------|------|-------------|
| GET | `/v1/databases/:database/collections/:collection/query/stream` | Streaming query (GET) |
| POST | `/v1/databases/:database/collections/:collection/query/stream` | Streaming query (POST) |

Additional parameters: `chunk_size`, `chunk_interval`, `legacy`

### Forecast
| Method | Path | Description |
|--------|------|-------------|
| GET | `/v1/databases/:database/collections/:collection/forecast` | Forecast (GET) |
| POST | `/v1/databases/:database/collections/:collection/forecast` | Forecast (POST) |

**Forecast Parameters:**

| Parameter | Description |
|-----------|-------------|
| `method` | Algorithm: sma, exponential, holt_winters, linear, arima, prophet, auto, ml |
| `horizon` | Number of future points to predict |
| `field` | Field to forecast |
| `ml_model` | ML model type: random_forest, lstm |
| `seasonal_period` | Seasonal period for Holt-Winters/ARIMA |
| `confidence_level` | Confidence interval (0-1) |

### Download / Export
| Method | Path | Description |
|--------|------|-------------|
| POST | `/v1/databases/:database/collections/:collection/downloads` | Create async download task |
| GET | `/v1/databases/:database/collections/:collection/downloads/:id/status` | Check download status |
| GET | `/v1/databases/:database/collections/:collection/downloads/:id/file` | Download file |

### Device Group Lookup
| Method | Path | Description |
|--------|------|-------------|
| GET | `/v1/databases/:database/collections/:collection/device-group` | Lookup device group |

### Admin Operations
| Method | Path | Auth | Description |
|--------|------|------|-------------|
| POST | `/admin/flush` | Yes | Trigger manual flush |
| GET | `/admin/groups` | Yes | List all group assignments |
| GET | `/admin/groups/:group_id` | Yes | Get single group |
| GET | `/admin/nodes/:node_id/groups` | Yes | Get groups for a node |

### Downsampling Modes

| Mode | Description | Best For |
|------|-------------|----------|
| **none** | No downsampling | Small datasets |
| **auto** | Auto-detect best algorithm | General use (recommended) |
| **lttb** | Largest-Triangle-Three-Buckets | Smooth/trending data, visualization |
| **minmax** | Preserves min/max in each bucket | Spiky data with outliers, IoT sensors |
| **avg** | Simple averaging per bucket | Large datasets, performance priority |
| **m4** | Min-Max + First-Last per bucket | Balanced visualization |

### Anomaly Detection Algorithms

Built-in anomaly detection runs as a post-processing step on query results:

| Algorithm | Description |
|-----------|-------------|
| **zscore** | Statistical outlier detection using standard deviations |
| **iqr** | Interquartile Range based detection |
| **moving_average** | Deviation from moving average |
| **auto** | Auto-select best algorithm based on data characteristics |

Detected anomaly types: `spike`, `drop`, `outlier`, `flatline`

### Forecast Algorithms

| Algorithm | Description |
|-----------|-------------|
| **sma** | Simple Moving Average |
| **exponential** | Exponential Smoothing |
| **holt_winters** | Triple Exponential Smoothing (seasonal) |
| **linear** | Linear Regression |
| **arima** | AutoRegressive Integrated Moving Average |
| **prophet** | Prophet-style seasonal decomposition |
| **auto** | Auto-select best algorithm |

---

**Built with Go for high-performance time-series data storage**
