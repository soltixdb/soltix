# Bin to CSV Converter

Tool to convert Soltix binary storage files to CSV format for analysis and debugging.

## Features

- ✅ Read and decompress binary storage files (Snappy compression)
- ✅ Support both raw data and aggregated data
- ✅ Convert to CSV with human-readable timestamps
- ✅ Support timezone conversion
- ✅ Automatic data type detection (raw/aggregated)

## Installation

```bash
cd cmd/tools/bin_to_csv
go build -o bin_to_csv
```

Or run directly:
```bash
go run cmd/tools/bin_to_csv/main.go [flags]
```

## Usage

### Basic Syntax

```bash
./bin_to_csv -database <db> -collection <coll> -interval <interval> -date <yyyymmdd>
```

### Parameters

| Flag | Description | Default | Required |
|------|-------------|---------|----------|
| `-database` | Database name | `shirokuma` | No |
| `-collection` | Collection name | `plant_power` | No |
| `-interval` | Time interval: `raw`, `1h`, `1d`, `1mo` (or `1m`), `1y` | `raw` | No |
| `-date` | Date to export (format: yyyymmdd) | - | **Yes** |
| `-data-dir` | Base data directory | `./data/data` | No |
| `-output` | Output directory for CSV | `./data/csv` | No |
| `-timezone` | Timezone for conversion | `Asia/Tokyo` | No |

### File Path Structure

The tool automatically determines the file path based on interval:

**Raw data:**
```
{data-dir}/{database}/{collection}/{yyyy}/{mm}/{yyyymmdd}.bin
```

**Hourly aggregation:**
```
{data-dir}/agg_1h/{database}/{collection}/{yyyy}/{mm}/{yyyymmdd}.bin
```

**Daily aggregation:**
```
{data-dir}/agg_1d/{database}/{collection}/{yyyy}/{yyyymm}.bin
```

**Monthly aggregation:**
```
{data-dir}/agg_1M/{database}/{collection}/{yyyy}.bin
```

**Yearly aggregation:**
```
{data-dir}/agg_1y/{database}/{collection}/all.bin
```

## Examples

### 1. Export raw data for 2026-01-13

```bash
go run cmd/tools/bin_to_csv/main.go \
  -database shirokuma \
  -collection plant_power \
  -interval raw \
  -date 20260113
```

**Output:** `./data/csv/shirokuma_plant_power_raw_20260113.csv`

```csv
timestamp,id,voltage,current,power
2026-01-13T00:00:00+09:00,device-001,220.5,10.2,2249.1
2026-01-13T00:01:00+09:00,device-001,221.0,10.3,2276.3
```

### 2. Export hourly aggregation

```bash
go run cmd/tools/bin_to_csv/main.go \
  -database shirokuma \
  -collection plant_power \
  -interval 1h \
  -date 20260113
```

**Output:** `./data/csv/shirokuma_plant_power_1h_20260113.csv`

```csv
timestamp,id,power_count,power_sum,power_min,power_max,power_avg
2026-01-13T00:00:00+09:00,device-001,60,134946,2200,2300,2249.1
2026-01-13T01:00:00+09:00,device-001,60,135000,2210,2320,2250.0
```

### 3. Export daily aggregation (entire month)

```bash
go run cmd/tools/bin_to_csv/main.go \
  -database shirokuma \
  -collection plant_power \
  -interval 1d \
  -date 20260113
```

**Note:** Daily aggregation is stored by month, so the file will contain all days in January 2026.

### 4. Export with different timezone

```bash
go run cmd/tools/bin_to_csv/main.go \
  -database shirokuma \
  -collection plant_power \
  -interval raw \
  -date 20260113 \
  -timezone "America/New_York"
```

### 5. Custom data directory and output path

```bash
go run cmd/tools/bin_to_csv/main.go \
  -database bench_db \
  -collection bench_coll \
  -interval raw \
  -date 20260114 \
  -data-dir /path/to/soltix/data \
  -output /path/to/output/csv
```

## Output Format

### Raw Data CSV

Each line is a data point with:
- `timestamp`: Time in selected timezone (RFC3339)
- `id`: Device ID
- Remaining columns: Data point fields

### Aggregated Data CSV

Each field has 5 statistic columns:
- `{field}_count`: Number of points
- `{field}_sum`: Sum of values
- `{field}_min`: Minimum value
- `{field}_max`: Maximum value
- `{field}_avg`: Average value

## Error Handling

### File not found
```bash
Error: File not found: ./data/data/shirokuma/plant_power/2026/01/20260113.bin
```
→ Check the data-dir path and ensure the file exists

### Invalid date format
```bash
Error: Invalid date format '2026-01-13'. Expected yyyymmdd (e.g., 20260113)
```
→ Use YYYYMMDD format without hyphens

### Invalid timezone
```bash
Error: Invalid timezone 'Asia/Toky': unknown time zone Asia/Toky
```
→ Check timezone name (see: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones)

### No data points found
```bash
Warning: No data points found in file
```
→ File exists but contains no data, possibly corrupted or empty

## Use Cases

### 1. Debug aggregation issues
```bash
# Export raw data
go run cmd/tools/bin_to_csv/main.go -interval raw -date 20260113

# Export aggregated data for the same date
go run cmd/tools/bin_to_csv/main.go -interval 1h -date 20260113

# Compare to verify aggregation logic
```

### 2. Performance analysis
```bash
# Export multiple days to analyze trends
for date in 20260110 20260111 20260112 20260113; do
  go run cmd/tools/bin_to_csv/main.go -interval 1d -date $date
done
```

### 3. Data migration
```bash
# Export all raw data for import to another system
go run cmd/tools/bin_to_csv/main.go -interval raw -date 20260113 -output /export/path
```

### 4. Verify timezone handling
```bash
# Export same data with different timezones
go run cmd/tools/bin_to_csv/main.go -interval raw -date 20260113 -timezone "Asia/Tokyo"
go run cmd/tools/bin_to_csv/main.go -interval raw -date 20260113 -timezone "UTC"
```

## Technical Details

### Binary Format

The tool supports reading Soltix binary format:

1. **File Header** (256 bytes):
   - Magic number: `0x534F4C54` ("SOLT")
   - Index offset and size
   - Other metadata

2. **Device Blocks** (compressed):
   - Device ID
   - Timestamps (base + deltas)
   - Field values (protobuf)

3. **Index** (end of file):
   - Device ID → block offset/size mapping
   - Compressed with Snappy

### Protobuf Schemas

- Raw data: `DeviceBlock` (proto/storage/v1)
- Aggregated data: `AggregatedDeviceBlock` (proto/storage/v1)

## Troubleshooting

**Q: CSV file too large?**  
A: Use aggregated interval (`1h`, `1d`) instead of raw to reduce size

**Q: Timestamp in wrong timezone?**  
A: Check `-timezone` flag, default is `Asia/Tokyo`

**Q: Some fields missing in CSV?**  
A: Fields starting with `_` (system metadata) are automatically skipped

**Q: Performance slow with large files?**  
A: Tool loads entire file into memory, files >1GB may need optimization

## Related

- [Aggregation Documentation](../../../docs/AGGREGATION.md)
- [Storage Format](../../../internal/storage/README.md)
- [Benchmark Tool](../benchmark/README.md)
