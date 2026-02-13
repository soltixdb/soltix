# Soltix API Benchmark Tool

Comprehensive benchmark tool to evaluate the performance of Soltix API.

## Features

### 1. **Write Performance**
- Throughput (records per second)
- Latency (latency): Min, Avg, P50, P95, P99, Max
- Error rate
- Concurrent writes
- Data distribution over time

### 2. **Query Performance**
- Query latency for different time ranges
- Query throughput (queries per second)
- Concurrent queries
- Error rate

### 3. **Mixed Workload**
- Run writes and queries simultaneously
- Simulate real-world workload
- Real-time progress tracking

### 4. **Metrics Collection**
- Request latency: min, max, avg, p50, p95, p99
- Throughput: operations/second
- Error rate: success/failure ratio
- Duration: test execution time
- Automatic result saving to file

## Usage

### Quick Start

```bash
# Basic benchmark (60 seconds, 10 write workers, 5 query workers)
go run cmd/tools/benchmark/main.go

# With custom configuration
go run cmd/tools/benchmark/main.go \
  -url http://localhost:8080 \
  -db benchmark_db \
  -coll benchmark_coll \
  -devices 100 \
  -fields 10 \
  -duration 5m \
  -write-workers 20 \
  -query-workers 10
```

### Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `-url` | `http://localhost:8080` | URL of API server |
| `-db` | `benchmark_db` | Test database name |
| `-coll` | `benchmark_coll` | Test collection name |
| `-devices` | `100` | Number of devices |
| `-fields` | `10` | Number of fields per record |
| `-duration` | `60s` | Test execution time |
| `-write-workers` | `10` | Number of concurrent write workers |
| `-query-workers` | `5` | Number of concurrent query workers |
| `-batch-size` | `1` | Batch size for writes |
| `-query-interval` | `1s` | Time interval between queries |
| `-time-range` | `24h` | Time range for data distribution |
| `-api-key` | `""` | API key if authentication is required |

## Use Cases

### 1. Test Write Performance
```bash
# High throughput write test
go run cmd/tools/benchmark/main.go \
  -duration 5m \
  -write-workers 50 \
  -query-workers 0 \
  -devices 1000 \
  -fields 20
```

### 2. Test Query Performance
```bash
# Focus on query performance
go run cmd/tools/benchmark/main.go \
  -duration 5m \
  -write-workers 5 \
  -query-workers 20 \
  -query-interval 100ms
```

### 3. Test Mixed Workload
```bash
# Realistic mixed workload
go run cmd/tools/benchmark/main.go \
  -duration 10m \
  -write-workers 20 \
  -query-workers 10 \
  -devices 500 \
  -fields 15
```

### 4. Stress Test
```bash
# High load stress test
go run cmd/tools/benchmark/main.go \
  -duration 30m \
  -write-workers 100 \
  -query-workers 50 \
  -devices 5000 \
  -fields 30
```

### 5. Latency Test
```bash
# Low concurrency for accurate latency measurement
go run cmd/tools/benchmark/main.go \
  -duration 5m \
  -write-workers 1 \
  -query-workers 1 \
  -devices 10
```

## Results

Results are automatically saved to the `benchmark_results/` directory with format:
```
api_benchmark_YYYYMMDD_HHMMSS.txt
```
