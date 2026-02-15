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
| `-data-dir` | Base raw data directory | `./data/data` | No |
| `-agg-dir` | Base aggregated data directory | `./data/agg` | No |
| `-output` | Output directory for CSV | `./data/csv` | No |
| `-timezone` | Timezone for conversion | `Asia/Tokyo` | No |
| `-device` | Filter by device ID | - | No |

### File Path Structure

The tool automatically determines the file path based on interval:

**Raw data (TieredStorage, group-aware):**
```
{data-dir}/group_XXXX/{database}/{collection}/{yyyy}/{mm}/{yyyymmdd}/dg_XXXX/part_XXXX.bin
```

The tool auto-discovers all `group_XXXX` directories and queries across them.

**Aggregated data (V6 columnar):**
```
{agg-dir}/agg_1h/{db}/{collection}/{yyyy}/{mm}/{dd}/_metadata.idx
{agg-dir}/agg_1h/{db}/{collection}/{yyyy}/{mm}/{dd}/dg_0000/part_0000.bin

{agg-dir}/agg_1d/{db}/{collection}/{yyyy}/{mm}/_metadata.idx
{agg-dir}/agg_1M/{db}/{collection}/{yyyy}/_metadata.idx
{agg-dir}/agg_1y/{db}/{collection}/_metadata.idx
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

### 5. Filter by device ID

```bash
go run cmd/tools/bin_to_csv/main.go \
  -database shirokuma \
  -collection plant_power \
  -interval raw \
  -date 20260113 \
  -device "device-001"
```

### 6. Custom data directory and output path

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

### No groups found
```bash
Discovered 0 groups in ./data/data
Warning: No data points found
```
→ Check that `--data-dir` points to the directory containing `group_XXXX` folders

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

### Storage Architecture

The tool uses the V6 3-tier storage architecture:

1. **Tier 1 — Group** (`group_XXXX/`):
   - Devices are hash-assigned to groups: `hash(db, collection, device_id) % TotalGroups`
   - TieredStorage auto-discovers all groups on disk

2. **Tier 2 — Device Group** (`dg_XXXX/`):
   - Within each date directory, devices are batched (max 50 per DG)

3. **Tier 3 — Partition** (`part_XXXX.bin`):
   - V6 columnar format with Snappy compression
   - Magic number: `0x56364447` ("V6DG")
   - Footer-based column index for efficient reads

### Aggregation Format

- V6 columnar format with magic `0x41474750` ("AGGP")
- Metrics per column: sum, avg, min, max, count
- Organized by level: `agg_1h`, `agg_1d`, `agg_1M`, `agg_1y`

## Troubleshooting

**Q: "Discovered 0 groups" — no data found?**  
A: Make sure `--data-dir` points to the directory that contains `group_XXXX/` folders

**Q: CSV file too large?**  
A: Use aggregated interval (`1h`, `1d`) instead of raw to reduce size

**Q: Timestamp in wrong timezone?**  
A: Check `-timezone` flag, default is `Asia/Tokyo`. Use `-timezone UTC` if data was stored in UTC

**Q: Data stored in UTC but I want JST output?**  
A: Use `-timezone Asia/Tokyo` — timestamps will be converted automatically

**Q: How to export a specific device only?**  
A: Use `-device "device-001"` to filter by device ID

## Related

- [Aggregation Documentation](../../../docs/AGGREGATION.md)
- [Storage Format](../../../internal/storage/README.md)
- [Benchmark Tool](../benchmark/README.md)
