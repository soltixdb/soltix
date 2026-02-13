#!/usr/bin/env python3
"""
Verify aggregated data from Soltix against calculated values from raw data.
Supports multi-type data: only numeric fields (int, float) are aggregated.
Bool, string, and null fields are skipped for aggregation verification.
"""

import requests
import json
import sys
import os
from datetime import datetime, timezone, timedelta
from collections import defaultdict

# Color codes
RED = '\033[0;31m'
GREEN = '\033[0;32m'
YELLOW = '\033[1;33m'
BLUE = '\033[0;34m'
NC = '\033[0m'


def print_color(color, text, end='\n'):
    print(f"{color}{text}{NC}", end=end)


def normalize_time_to_utc(time_str):
    """Convert any time string to UTC datetime"""
    try:
        if '+' in time_str or time_str.endswith('Z'):
            if time_str.endswith('Z'):
                parsed = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
            else:
                parsed = datetime.fromisoformat(time_str)
            return parsed.astimezone(timezone.utc)
        else:
            return datetime.fromisoformat(time_str).replace(tzinfo=timezone.utc)
    except Exception:
        return None


def truncate_to_hour(dt):
    """Truncate datetime to hour"""
    return dt.replace(minute=0, second=0, microsecond=0)


def truncate_to_day(dt):
    """Truncate datetime to day in CONFIGURED timezone (default: Asia/Tokyo)"""
    jst = timezone(timedelta(hours=9))
    dt_jst = dt.astimezone(jst)
    day_start_jst = dt_jst.replace(hour=0, minute=0, second=0, microsecond=0)
    return day_start_jst.astimezone(timezone.utc)


def truncate_to_month(dt):
    """Truncate datetime to month in CONFIGURED timezone (default: Asia/Tokyo)"""
    jst = timezone(timedelta(hours=9))
    dt_jst = dt.astimezone(jst)
    month_start_jst = dt_jst.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return month_start_jst.astimezone(timezone.utc)


def truncate_to_year(dt):
    """Truncate datetime to year in CONFIGURED timezone (default: Asia/Tokyo)"""
    jst = timezone(timedelta(hours=9))
    dt_jst = dt.astimezone(jst)
    year_start_jst = dt_jst.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    return year_start_jst.astimezone(timezone.utc)


def is_numeric_field(field_name, field_schemas):
    """Check if a field is numeric (int or float) based on field_schemas"""
    if not field_schemas:
        # Legacy mode: assume all fields are numeric
        return True
    ftype = field_schemas.get(field_name, '')
    return ftype in ('int', 'float', 'number')


def calculate_aggregations_from_raw(raw_points, interval, fields, field_schemas=None):
    """
    Calculate expected aggregations from raw data points.
    Only numeric fields (int, float) are aggregated. Null values are skipped.
    Returns dict: {bucket_time: {device_id: {field: {sum, avg, min, max, count}}}}
    """
    buckets = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    for point in raw_points:
        time_str = point.get('time')
        device_id = point.get('id')

        dt = normalize_time_to_utc(time_str)
        if dt is None:
            continue

        if interval == '1h':
            bucket_time = truncate_to_hour(dt)
        elif interval == '1d':
            bucket_time = truncate_to_day(dt)
        elif interval == '1mo':
            bucket_time = truncate_to_month(dt)
        elif interval == '1y':
            bucket_time = truncate_to_year(dt)
        else:
            bucket_time = truncate_to_hour(dt)

        bucket_key = bucket_time.strftime('%Y-%m-%dT%H:%M:%SZ')

        for field in fields:
            if not is_numeric_field(field, field_schemas):
                continue  # Skip non-numeric fields for aggregation

            val = point.get(field)
            if val is None:
                continue  # Skip null values in aggregation

            # Convert to numeric
            try:
                numeric_val = float(val)
                buckets[bucket_key][device_id][field].append(numeric_val)
            except (TypeError, ValueError):
                continue  # Skip non-convertible values

    # Calculate aggregations
    result = {}
    for bucket_key, devices in buckets.items():
        result[bucket_key] = {}
        for device_id, device_fields in devices.items():
            result[bucket_key][device_id] = {}
            for field, values in device_fields.items():
                if values:
                    result[bucket_key][device_id][field] = {
                        'sum': sum(values),
                        'avg': sum(values) / len(values),
                        'min': min(values),
                        'max': max(values),
                        'count': len(values)
                    }

    return result


def query_aggregated_data(base_url, database, collection, start_time, end_time,
                          device_ids, fields, interval, aggregation, limit=10000):
    """Query aggregated data from Soltix API"""
    url = f"{base_url}/v1/databases/{database}/collections/{collection}/query"

    payload = {
        "start_time": start_time,
        "end_time": end_time,
        "limit": limit,
        "interval": interval,
        "aggregation": aggregation
    }

    if device_ids:
        payload["ids"] = device_ids

    if fields:
        payload["fields"] = fields

    response = requests.post(url, json=payload, timeout=60)
    return response.json()


def convert_columnar_to_points(results):
    """Convert columnar format to row format"""
    points = []

    if not results or not isinstance(results, list):
        return points

    for result in results:
        device_id = result.get('id')
        times = result.get('times', [])
        field_names = [k for k in result.keys() if k not in ['id', 'times']]

        for i, time_str in enumerate(times):
            dt = normalize_time_to_utc(time_str)
            if dt:
                normalized_time = dt.strftime('%Y-%m-%dT%H:%M:%SZ')
            else:
                normalized_time = time_str

            point = {
                'time': normalized_time,
                'id': device_id,
                '_original_time': time_str
            }
            for field_name in field_names:
                if i < len(result[field_name]):
                    point[field_name] = result[field_name][i]
            points.append(point)

    return points


def verify_aggregation(expected, actual_points, aggregation_type, fields, interval, tolerance=0.0001):
    """
    Verify aggregated results against expected values
    Returns (matched, mismatched, missing, details)
    """
    matched = 0
    mismatched = 0
    missing = 0
    details = []

    actual_lookup = {}
    for point in actual_points:
        if interval in ['1d', '1mo', '1y']:
            dt = normalize_time_to_utc(point['time'])
            if dt:
                if interval == '1d':
                    bucket_start = truncate_to_day(dt)
                elif interval == '1mo':
                    bucket_start = truncate_to_month(dt)
                elif interval == '1y':
                    bucket_start = truncate_to_year(dt)
                normalized_key = f"{bucket_start.strftime('%Y-%m-%dT%H:%M:%SZ')}_{point['id']}"
                actual_lookup[normalized_key] = point
        else:
            key = f"{point['time']}_{point['id']}"
            actual_lookup[key] = point

    for bucket_time, devices in expected.items():
        for device_id, device_fields in devices.items():
            key = f"{bucket_time}_{device_id}"

            if key not in actual_lookup:
                missing += 1
                if len(details) < 10:
                    details.append({
                        'type': 'MISSING',
                        'bucket': bucket_time,
                        'device': device_id
                    })
                continue

            actual_point = actual_lookup[key]
            all_fields_match = True
            field_mismatches = []

            for field in fields:
                if field not in device_fields:
                    continue

                expected_val = device_fields[field][aggregation_type]
                actual_val = actual_point.get(field)

                if actual_val is None:
                    all_fields_match = False
                    field_mismatches.append({
                        'field': field,
                        'expected': expected_val,
                        'actual': None
                    })
                elif abs(expected_val - actual_val) > tolerance:
                    all_fields_match = False
                    field_mismatches.append({
                        'field': field,
                        'expected': expected_val,
                        'actual': actual_val,
                        'diff': abs(expected_val - actual_val)
                    })

            if all_fields_match:
                matched += 1
            else:
                mismatched += 1
                if len(details) < 10:
                    details.append({
                        'type': 'MISMATCH',
                        'bucket': bucket_time,
                        'device': device_id,
                        'fields': field_mismatches
                    })

    return matched, mismatched, missing, details


def find_latest_generated_file(data_dir):
    """Find the most recent generated data file and extract metadata"""
    if not os.path.exists(data_dir):
        return None, None

    generated_files = []
    for filename in os.listdir(data_dir):
        if filename.startswith("generated_data_") and filename.endswith(".json"):
            filepath = os.path.join(data_dir, filename)
            generated_files.append((filename, filepath))

    if not generated_files:
        return None, None

    generated_files.sort(reverse=True)
    latest_file = generated_files[0][1]

    try:
        with open(latest_file, 'r') as f:
            data = json.load(f)
            metadata = data.get('metadata')
            if metadata:
                return latest_file, metadata

        # Fallback: infer from filename
        filename = os.path.basename(latest_file)
        parts = filename.replace("generated_data_", "").replace(".json", "").rsplit("_", 1)
        if len(parts) >= 2:
            db_coll = parts[0].rsplit("_", 1)
            if len(db_coll) == 2:
                return latest_file, {"database": db_coll[0], "collection": db_coll[1]}
    except Exception as e:
        print_color(RED, f"Error reading metadata: {e}")

    return latest_file, None


def parse_metadata_time(time_str):
    """Parse time string from metadata format"""
    try:
        if 'UTC' in time_str:
            time_str = time_str.split(' UTC')[0]
        dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
        return dt
    except Exception:
        return None


def main():
    base_url = os.environ.get('SOLTIX_URL', 'http://localhost:5555')

    print_color(BLUE, "=" * 70)
    print_color(BLUE, "Soltix Aggregation Verification Tool (Auto Mode - Multi-Type)")
    print_color(BLUE, "=" * 70)
    print()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, "test_data")

    print_color(YELLOW, "🔍 Searching for latest generated data file...")
    generated_file, metadata = find_latest_generated_file(data_dir)

    if not generated_file:
        print_color(RED, f"❌ No generated data files found in {data_dir}")
        print_color(YELLOW, "Please run generate_test_data.py first to create test data.")
        return

    print_color(GREEN, f"✅ Found: {os.path.basename(generated_file)}")

    if not metadata:
        print_color(RED, "❌ Could not read metadata from file")
        return

    # Extract parameters from metadata
    database = metadata.get('database', 'shirokuma')
    collection = metadata.get('collection', 'plant_power')
    field_names = metadata.get('field_names', [f'field_{i}' for i in range(10)])
    num_devices = metadata.get('num_devices', 10)
    field_schemas = metadata.get('field_schemas', {})

    # Parse time range from metadata
    start_time_str = metadata.get('start_time', '')
    end_time_str = metadata.get('end_time', '')

    start_dt = parse_metadata_time(start_time_str)
    end_dt = parse_metadata_time(end_time_str)

    if not start_dt or not end_dt:
        print_color(RED, "❌ Could not parse time range from metadata")
        return

    # Format times for API
    start_time = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_time = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    start_time_input = start_dt.strftime("%Y%m%d%H%M%S")
    end_time_input = end_dt.strftime("%Y%m%d%H%M%S")

    # Generate device IDs
    device_ids = [f"device-{i:03d}" for i in range(1, num_devices + 1)]

    # Filter to only numeric fields for aggregation verification
    numeric_fields = [f for f in field_names if is_numeric_field(f, field_schemas)]
    non_numeric_fields = [f for f in field_names if not is_numeric_field(f, field_schemas)]

    # Use first 5 numeric fields for verification (or all if fewer)
    test_fields = numeric_fields[:5]

    # Test configurations
    intervals = ['1h', '1d', '1mo', '1y']
    aggregations = ['sum', 'avg', 'min', 'max', 'count']

    test_configs = []
    for interval in intervals:
        for agg in aggregations:
            test_configs.append({'interval': interval, 'aggregation': agg})

    print()
    print_color(BLUE, "=" * 70)
    print_color(BLUE, "Metadata from Generated File")
    print_color(BLUE, "=" * 70)
    print(f"  Database: {database}")
    print(f"  Collection: {collection}")
    print(f"  Time Range: {start_time} → {end_time}")
    print(f"  Devices: {num_devices} devices")
    print(f"  Total Fields: {len(field_names)}")
    print(f"  Total Points: {metadata.get('total_points', 'N/A'):,}")
    print()

    if field_schemas:
        print_color(YELLOW, "  Field Type Breakdown:")
        type_groups = defaultdict(list)
        for fname, ftype in field_schemas.items():
            type_groups[ftype].append(fname)
        for ftype in sorted(type_groups.keys()):
            print(f"    {ftype}: {', '.join(type_groups[ftype])}")
        print()

    print_color(YELLOW, f"  Numeric fields (for aggregation): {len(numeric_fields)}")
    if numeric_fields:
        print(f"    {', '.join(numeric_fields)}")
    print_color(YELLOW, f"  Non-numeric fields (skipped): {len(non_numeric_fields)}")
    if non_numeric_fields:
        print(f"    {', '.join(non_numeric_fields)}")
    print_color(YELLOW, f"  Testing with fields: {', '.join(test_fields)}")

    if not test_fields:
        print_color(RED, "❌ No numeric fields available for aggregation testing!")
        return

    print()

    # Run all test configurations
    all_results = []

    for config in test_configs:
        interval = config['interval']
        aggregation = config['aggregation']

        print()
        print_color(BLUE, "=" * 70)
        print_color(BLUE, f"Testing: {interval} / {aggregation}")
        print_color(BLUE, "=" * 70)

        result = run_verification(
            base_url, database, collection,
            generated_file, start_time_input, end_time_input,
            start_time, end_time,
            device_ids[:3], test_fields[:3],
            interval, aggregation,
            field_schemas=field_schemas
        )

        all_results.append({
            'interval': interval,
            'aggregation': aggregation,
            'result': result
        })

    # Summary
    print()
    print_color(BLUE, "=" * 70)
    print_color(BLUE, "VERIFICATION SUMMARY")
    print_color(BLUE, "=" * 70)
    print()

    all_passed = True
    for r in all_results:
        status = "✅ PASS" if r['result'] else "❌ FAIL"
        color = GREEN if r['result'] else RED
        print_color(color, f"  {r['interval']:4} / {r['aggregation']:5}: {status}")
        if not r['result']:
            all_passed = False

    print()
    if all_passed:
        print_color(GREEN, "🎉 ALL TESTS PASSED!")
    else:
        print_color(RED, "⚠️  SOME TESTS FAILED")

    print()
    print_color(BLUE, "=" * 70)


def run_verification(base_url, database, collection, generated_file,
                     start_time_input, end_time_input, start_time, end_time,
                     device_ids, fields, interval, aggregation,
                     field_schemas=None):
    """Run a single verification test and return True/False"""

    print()
    print_color(YELLOW, "Loading raw data...")

    with open(generated_file, 'r') as f:
        generated = json.load(f)

    raw_points = generated.get('points', [])
    print_color(GREEN, f"✅ Loaded {len(raw_points):,} raw points")

    # Filter by time range and devices
    filtered_points = []
    start_dt_utc = datetime.strptime(start_time_input, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    end_dt_utc = datetime.strptime(end_time_input, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)

    query_start_time = start_time
    query_end_time = end_time
    if interval in ['1d', '1mo', '1y']:
        jst = timezone(timedelta(hours=9))
        start_jst = start_dt_utc.astimezone(jst)
        end_jst = end_dt_utc.astimezone(jst)

        if interval == '1d':
            start_jst = start_jst.replace(hour=0, minute=0, second=0, microsecond=0)
            end_jst = end_jst.replace(hour=23, minute=59, second=59, microsecond=0)
        elif interval == '1mo':
            start_jst = start_jst.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            if end_jst.month == 12:
                end_jst = end_jst.replace(year=end_jst.year + 1, month=1, day=1,
                                          hour=0, minute=0, second=0,
                                          microsecond=0) - timedelta(seconds=1)
            else:
                end_jst = end_jst.replace(month=end_jst.month + 1, day=1,
                                          hour=0, minute=0, second=0,
                                          microsecond=0) - timedelta(seconds=1)
        elif interval == '1y':
            start_jst = start_jst.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
            end_jst = end_jst.replace(year=end_jst.year + 1, month=1, day=1,
                                      hour=0, minute=0, second=0,
                                      microsecond=0) - timedelta(seconds=1)

        start_dt_utc = start_jst.astimezone(timezone.utc)
        end_dt_utc = end_jst.astimezone(timezone.utc)

        query_start_time = start_dt_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
        query_end_time = end_dt_utc.strftime('%Y-%m-%dT%H:%M:%SZ')

        print_color(YELLOW, f"📅 Expanded to Japan {interval}: {query_start_time} → {query_end_time}")

    for point in raw_points:
        dt = normalize_time_to_utc(point.get('time'))
        if dt is None:
            continue

        if dt < start_dt_utc or dt >= end_dt_utc:
            continue

        if device_ids and point.get('id') not in device_ids:
            continue

        filtered_points.append(point)

    print_color(GREEN, f"✅ Filtered to {len(filtered_points):,} points")

    # Calculate expected aggregations (only numeric fields, skip nulls)
    expected = calculate_aggregations_from_raw(filtered_points, interval, fields, field_schemas)
    total_buckets = sum(len(devices) for devices in expected.values())
    print_color(GREEN, f"✅ Calculated {len(expected)} buckets, {total_buckets} device-buckets")

    # Query from Soltix
    print_color(YELLOW, "Querying Soltix API...")

    try:
        result = query_aggregated_data(
            base_url, database, collection,
            query_start_time, query_end_time,
            device_ids, fields, interval, aggregation
        )
    except Exception as e:
        print_color(RED, f"❌ Query failed: {e}")
        return False

    results = result.get('results', result.get('data', []))
    actual_points = convert_columnar_to_points(results)

    print_color(GREEN, f"✅ Received {len(actual_points):,} points from API")

    # Verify
    matched, mismatched, missing, details = verify_aggregation(
        expected, actual_points, aggregation, fields, interval
    )

    total = matched + mismatched + missing
    match_rate = (matched / total * 100) if total > 0 else 0

    print_color(YELLOW, f"Result: {matched}/{total} matched ({match_rate:.1f}%)")

    if matched == total:
        print_color(GREEN, "✅ PASSED")
        return True
    else:
        print_color(RED, f"❌ FAILED: {mismatched} mismatched, {missing} missing")
        for detail in details[:3]:
            if detail['type'] == 'MISSING':
                print_color(RED, f"   MISSING: {detail['bucket']} / {detail['device']}")
            else:
                print_color(RED, f"   MISMATCH: {detail['bucket']} / {detail['device']}")
                for fm in detail.get('fields', []):
                    print_color(RED, f"     {fm['field']}: expected={fm['expected']}, "
                                    f"actual={fm['actual']}")
        return False


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print_color(YELLOW, "\n\n⚠️  Interrupted by user")
        sys.exit(130)
    except Exception as e:
        print_color(RED, f"\n\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
