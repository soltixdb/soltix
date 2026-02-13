#!/usr/bin/env python3
"""
Query data from Soltix and verify against generated data.
Supports multi-type verification: int, float, bool, string, and null.
"""

import requests
import json
import time
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


def format_size(size_bytes):
    """Format size in human readable format"""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.2f} KB"
    else:
        return f"{size_bytes / (1024 * 1024):.2f} MB"


def normalize_time_to_utc(time_str):
    """Convert any time string to UTC format YYYY-MM-DDTHH:MM:SSZ"""
    try:
        if '+' in time_str or time_str.endswith('Z'):
            if time_str.endswith('Z'):
                parsed = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
            else:
                parsed = datetime.fromisoformat(time_str)
            utc_time = parsed.astimezone(timezone.utc)
            return utc_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        else:
            # Assume UTC if no timezone
            return time_str
    except Exception:
        return time_str


def compare_values(query_val, generated_val, field_type=None, float_tolerance=0.0001):
    """
    Compare two values with type-aware logic.
    Returns (is_match: bool, detail: str or None)

    Handles: int, float, bool, string, null
    """
    # Both null
    if query_val is None and generated_val is None:
        return True, None

    # One is null, other is not
    if query_val is None or generated_val is None:
        return False, f"query={query_val!r}, gen={generated_val!r} (null mismatch)"

    # Float comparison with tolerance
    if field_type == 'float' or (isinstance(query_val, float) and isinstance(generated_val, float)):
        try:
            q = float(query_val)
            g = float(generated_val)
            if abs(q - g) <= float_tolerance:
                return True, None
            return False, f"query={q:.6f}, gen={g:.6f}, diff={abs(q - g):.6e}"
        except (TypeError, ValueError):
            return False, f"query={query_val!r}, gen={generated_val!r} (float conversion failed)"

    # Int comparison - server may return as float (JSON number)
    if field_type == 'int':
        try:
            q = query_val
            g = generated_val
            # JSON numbers may come back as float (e.g., 42.0 instead of 42)
            if isinstance(q, float) and q == int(q):
                q = int(q)
            if isinstance(g, float) and g == int(g):
                g = int(g)
            if q == g:
                return True, None
            return False, f"query={query_val!r}, gen={generated_val!r}"
        except (TypeError, ValueError):
            return False, f"query={query_val!r}, gen={generated_val!r} (int comparison failed)"

    # Bool comparison - server may return as different types
    if field_type == 'bool':
        try:
            # Normalize to bool
            q = query_val
            g = generated_val
            if isinstance(q, str):
                q = q.lower() in ('true', '1', 'yes')
            elif isinstance(q, (int, float)):
                q = bool(q)
            if isinstance(g, str):
                g = g.lower() in ('true', '1', 'yes')
            elif isinstance(g, (int, float)):
                g = bool(g)
            if q == g:
                return True, None
            return False, f"query={query_val!r}, gen={generated_val!r}"
        except (TypeError, ValueError):
            return False, f"query={query_val!r}, gen={generated_val!r} (bool comparison failed)"

    # String comparison
    if field_type == 'string':
        if str(query_val) == str(generated_val):
            return True, None
        return False, f"query={query_val!r}, gen={generated_val!r}"

    # Generic comparison (no type info)
    # Try numeric comparison first
    if isinstance(query_val, (int, float)) and isinstance(generated_val, (int, float)):
        if isinstance(query_val, float) or isinstance(generated_val, float):
            if abs(float(query_val) - float(generated_val)) <= float_tolerance:
                return True, None
        elif query_val == generated_val:
            return True, None
        return False, f"query={query_val!r}, gen={generated_val!r}"

    # Bool comparison
    if isinstance(query_val, bool) and isinstance(generated_val, bool):
        if query_val == generated_val:
            return True, None
        return False, f"query={query_val!r}, gen={generated_val!r}"

    # String/default comparison
    if query_val == generated_val:
        return True, None
    return False, f"query={query_val!r}, gen={generated_val!r}"


def verify_query_results(query_result_file, generated_data_file, device_ids=None,
                         fields=None, start_time=None, end_time=None):
    """
    Verify query results against generated data.
    Supports multi-type verification using field_schemas from metadata.
    Returns (is_valid, statistics_dict)
    """
    print()
    print_color(BLUE, "=" * 60)
    print_color(BLUE, "Data Verification (Multi-Type)")
    print_color(BLUE, "=" * 60)
    print()

    # Create log file
    script_dir = os.path.dirname(os.path.abspath(__file__))
    verify_dir = os.path.join(script_dir, "test_data", "verify")
    os.makedirs(verify_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(verify_dir, f"verification_{timestamp}.log")

    print_color(YELLOW, f"Creating log file: {log_file}")
    print()

    log = open(log_file, 'w', encoding='utf-8')

    def log_print(text):
        """Print to console and write to log file"""
        print(text)
        log.write(text + '\n')
        log.flush()

    try:
        # Load generated data
        print_color(YELLOW, f"Loading generated data: {generated_data_file}")
        log_print(f"Loading generated data: {generated_data_file}")
        with open(generated_data_file, 'r') as f:
            generated = json.load(f)

        generated_points = generated.get('points', [])
        metadata = generated.get('metadata', {})
        field_schemas = metadata.get('field_schemas', {})

        print_color(GREEN, f"✅ Loaded {len(generated_points):,} generated points")
        log_print(f"✅ Loaded {len(generated_points):,} generated points")

        if field_schemas:
            print_color(BLUE, f"Field schemas: {json.dumps(field_schemas)}")
            log_print(f"Field schemas: {json.dumps(field_schemas)}")

            # Show type breakdown
            type_breakdown = defaultdict(list)
            for fname, ftype in field_schemas.items():
                type_breakdown[ftype].append(fname)
            for ftype, fnames in sorted(type_breakdown.items()):
                print_color(BLUE, f"  {ftype}: {', '.join(fnames)}")
                log_print(f"  {ftype}: {', '.join(fnames)}")
        else:
            print_color(YELLOW, "⚠️  No field_schemas in metadata - using generic comparison")
            log_print("⚠️  No field_schemas in metadata - using generic comparison")

        # Load query results
        print_color(YELLOW, f"Loading query results: {query_result_file}")
        log_print(f"Loading query results: {query_result_file}")
        with open(query_result_file, 'r') as f:
            query_result = json.load(f)

        # Convert query results from columnar format to row format
        query_points = []
        results = query_result.get('results', query_result.get('data', []))

        if results and isinstance(results, list) and len(results) > 0:
            first_result = results[0]
            if isinstance(first_result, dict) and 'times' in first_result:
                # Columnar format: convert to row format
                print_color(BLUE, "Converting columnar format to row format...")
                for result in results:
                    device_id = result.get('id')
                    times = result.get('times', [])
                    field_names_in_result = [k for k in result.keys() if k not in ['id', 'times']]

                    for i, time_str in enumerate(times):
                        original_time_str = time_str
                        try:
                            if '+' in time_str or time_str.endswith('Z'):
                                if time_str.endswith('Z'):
                                    parsed = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
                                else:
                                    parsed = datetime.fromisoformat(time_str)
                                utc_time = parsed.astimezone(timezone.utc)
                                time_str = utc_time.strftime('%Y-%m-%dT%H:%M:%SZ')
                        except Exception:
                            pass

                        point = {'time': time_str, 'id': device_id, '_original_time': original_time_str}
                        for field_name in field_names_in_result:
                            if i < len(result[field_name]):
                                point[field_name] = result[field_name][i]
                        query_points.append(point)
            else:
                query_points = results

        print_color(GREEN, f"✅ Loaded {len(query_points):,} query result points")
        log_print(f"✅ Loaded {len(query_points):,} query result points")

        # Show device distribution in query results
        query_devices = set(p.get('id') for p in query_points)
        print_color(BLUE, f"Query returned {len(query_devices)} unique devices: {sorted(query_devices)}")
        log_print(f"Query returned {len(query_devices)} unique devices: {sorted(query_devices)}")

        # Log time range of query results
        if query_points:
            query_times = sorted([p['time'] for p in query_points])
            print_color(BLUE, f"Query time range: {query_times[0]} → {query_times[-1]}")
            log_print(f"Query time range: {query_times[0]} → {query_times[-1]}")

        print()
        log_print("")
        print_color(BLUE, "Creating lookup index for generated data...")
        log_print("Creating lookup index for generated data...")

        # Create lookup dictionary: key = "normalized_utc_time_deviceid", value = point
        generated_lookup = {}
        for point in generated_points:
            original_time = point.get('time')
            normalized_time = normalize_time_to_utc(original_time)
            key = f"{normalized_time}_{point.get('id')}"
            generated_lookup[key] = point

        print_color(GREEN, f"✅ Indexed {len(generated_lookup):,} generated points")
        log_print(f"✅ Indexed {len(generated_lookup):,} generated points")

        # Verify each query point against generated data
        print()
        log_print("")
        print_color(BLUE, "=" * 60)
        print_color(BLUE, "Verifying Points (Multi-Type) - only errors shown")
        print_color(BLUE, "=" * 60)
        print()
        log_print("=" * 60)
        log_print("Verification Log (only errors)")
        log_print("=" * 60)
        log_print("")

        matched_points = 0
        mismatched_points = 0
        missing_points = 0
        mismatch_details = []

        # Per-type statistics
        type_match_stats = defaultdict(lambda: {'matched': 0, 'mismatched': 0, 'null_match': 0, 'null_mismatch': 0})

        for i, query_point in enumerate(query_points):
            key = f"{query_point.get('time')}_{query_point.get('id')}"
            generated_point = generated_lookup.get(key)

            if not generated_point:
                missing_points += 1
                msg = f"❌ MISSING Point #{i + 1}: time={query_point.get('time')}, id={query_point.get('id')}"
                print_color(RED, msg)
                log_print(msg)

                if len(mismatch_details) < 100:
                    mismatch_details.append({
                        'type': 'MISSING',
                        'index': i + 1,
                        'time': query_point.get('time'),
                        'id': query_point.get('id'),
                        'query': {k: v for k, v in query_point.items() if k != '_original_time'}
                    })
                continue

            # Compare field values
            field_match = True
            mismatched_fields = []

            # Get all fields to compare (exclude time, id, and internal fields)
            query_fields = [k for k in query_point.keys() if k not in ['time', 'id', '_original_time']]

            for field in query_fields:
                q_val = query_point.get(field)
                g_val = generated_point.get(field)
                field_type = field_schemas.get(field)  # May be None for legacy data

                is_match, detail = compare_values(q_val, g_val, field_type)

                if is_match:
                    if q_val is None and g_val is None:
                        type_match_stats[field_type or 'unknown']['null_match'] += 1
                    else:
                        type_match_stats[field_type or 'unknown']['matched'] += 1
                else:
                    field_match = False
                    if q_val is None or g_val is None:
                        type_match_stats[field_type or 'unknown']['null_mismatch'] += 1
                    else:
                        type_match_stats[field_type or 'unknown']['mismatched'] += 1
                    mismatched_fields.append({
                        'field': field,
                        'field_type': field_type,
                        'detail': detail,
                        'query': q_val,
                        'generated': g_val
                    })

            if field_match:
                matched_points += 1
            else:
                mismatched_points += 1
                msg = f"❌ MISMATCH Point #{i + 1}: time={query_point.get('time')}, id={query_point.get('id')}"
                print_color(RED, msg)
                log_print(msg)
                for mf in mismatched_fields:
                    detail_msg = f"   - {mf['field']} [{mf['field_type'] or '?'}]: {mf['detail']}"
                    print(detail_msg)
                    log_print(detail_msg)

                if len(mismatch_details) < 100:
                    mismatch_details.append({
                        'type': 'MISMATCH',
                        'index': i + 1,
                        'time': query_point.get('time'),
                        'id': query_point.get('id'),
                        'mismatched_fields': mismatched_fields
                    })

            # Progress indicator (every 10000 points)
            if (i + 1) % 10000 == 0:
                progress = (i + 1) / len(query_points) * 100
                print(f"\r  Progress: {i + 1:,}/{len(query_points):,} ({progress:.1f}%) - "
                      f"✅ {matched_points:,} | ❌ {mismatched_points:,} | "
                      f"⚠️ {missing_points:,}", end='', flush=True)

        # Print final progress
        print(f"\r  Progress: {len(query_points):,}/{len(query_points):,} (100.0%) - "
              f"✅ {matched_points:,} | ❌ {mismatched_points:,} | ⚠️ {missing_points:,}")

        print()
        log_print("")

        # Initialize stats
        stats = {
            'total_query_points': len(query_points),
            'matched': matched_points,
            'mismatched': mismatched_points,
            'missing': missing_points,
            'match_rate': (matched_points / len(query_points) * 100) if query_points else 0
        }

        # Per-type verification stats
        print_color(BLUE, "=" * 60)
        print_color(BLUE, "Per-Type Verification Statistics")
        print_color(BLUE, "=" * 60)
        print()
        log_print("=" * 60)
        log_print("Per-Type Verification Statistics")
        log_print("=" * 60)
        log_print("")

        for ftype in sorted(type_match_stats.keys()):
            ts = type_match_stats[ftype]
            total_for_type = ts['matched'] + ts['mismatched'] + ts['null_match'] + ts['null_mismatch']
            matched_for_type = ts['matched'] + ts['null_match']
            rate = (matched_for_type / total_for_type * 100) if total_for_type > 0 else 0
            color = GREEN if rate == 100 else (YELLOW if rate > 90 else RED)

            msg = (f"  {ftype:8}: {matched_for_type}/{total_for_type} matched ({rate:.1f}%) "
                   f"[values: ✅{ts['matched']} ❌{ts['mismatched']} | "
                   f"nulls: ✅{ts['null_match']} ❌{ts['null_mismatch']}]")
            print_color(color, msg)
            log_print(msg)

        print()
        log_print("")

        # Check for generated points that were NOT found in query results
        print_color(BLUE, "=" * 60)
        print_color(BLUE, "Checking for Missing Generated Points")
        print_color(BLUE, "=" * 60)
        print()
        log_print("=" * 60)
        log_print("Missing Generated Points (in generated but not in query)")
        log_print("=" * 60)
        log_print("")

        query_keys = set()
        for point in query_points:
            query_keys.add((point.get('time'), point.get('id')))

        not_in_query = []
        for point in generated_points:
            normalized_time = normalize_time_to_utc(point.get('time'))
            key = (normalized_time, point.get('id'))
            if key not in query_keys:
                not_in_query.append(point)

        if not_in_query:
            print_color(RED, f"❌ Found {len(not_in_query)} generated points NOT in query results:")
            log_print(f"❌ Found {len(not_in_query)} generated points NOT in query results:")

            missing_by_time_device = defaultdict(list)
            for point in not_in_query:
                key = (point.get('time'), point.get('id'))
                flds = [k for k in point.keys() if k not in ['time', 'id']]
                missing_by_time_device[key] = flds

            for (time_str, device_id), flds in sorted(missing_by_time_device.items()):
                msg = f"  - Time: {time_str}, Device: {device_id}, Fields: {flds}"
                print_color(YELLOW, msg)
                log_print(msg)

            stats['generated_not_in_query'] = len(not_in_query)
        else:
            print_color(GREEN, "✅ All generated points were found in query results")
            log_print("✅ All generated points were found in query results")
            stats['generated_not_in_query'] = 0

        print()
        log_print("")

        # Overall Statistics
        print_color(BLUE, "=" * 60)
        print_color(BLUE, "Verification Results")
        print_color(BLUE, "=" * 60)
        print()
        log_print("=" * 60)
        log_print("Verification Results")
        log_print("=" * 60)
        log_print("")

        print_color(YELLOW, f"Total query points: {stats['total_query_points']:,}")
        log_print(f"Total query points: {stats['total_query_points']:,}")
        print_color(GREEN, f"✅ Matched points: {stats['matched']:,} ({stats['match_rate']:.2f}%)")
        log_print(f"✅ Matched points: {stats['matched']:,} ({stats['match_rate']:.2f}%)")
        if stats['mismatched'] > 0:
            print_color(RED, f"❌ Mismatched points: {stats['mismatched']:,}")
            log_print(f"❌ Mismatched points: {stats['mismatched']:,}")
        if stats['missing'] > 0:
            print_color(RED, f"❌ Missing points: {stats['missing']:,}")
            log_print(f"❌ Missing points: {stats['missing']:,}")
        print()
        log_print("")

        if stats['matched'] == stats['total_query_points']:
            print_color(GREEN, "🎉 PERFECT MATCH: All query points match generated data!")
            log_print("🎉 PERFECT MATCH: All query points match generated data!")
            log_print("")
            print_color(YELLOW, f"📝 Verification log saved to: {log_file}")
            return True, stats
        else:
            print_color(RED, f"⚠️  VERIFICATION FAILED: "
                            f"{stats['mismatched'] + stats['missing']} points do not match")
            log_print(f"⚠️  VERIFICATION FAILED: "
                      f"{stats['mismatched'] + stats['missing']} points do not match")

            # Save errors to JSON file
            error_file = os.path.join(verify_dir, f"errors_{timestamp}.json")
            with open(error_file, 'w') as ef:
                json.dump({
                    'summary': stats,
                    'type_stats': dict(type_match_stats),
                    'errors': mismatch_details
                }, ef, indent=2)
            print_color(YELLOW, f"📝 Error details saved to: {error_file}")
            log_print(f"Error details saved to: {error_file}")

            log_print("")
            print_color(YELLOW, f"📝 Verification log saved to: {log_file}")
            return False, stats

    except Exception as e:
        print_color(RED, f"❌ Verification failed: {e}")
        log_print(f"❌ Verification failed: {e}")
        import traceback
        traceback.print_exc()
        log_print(traceback.format_exc())
        return False, {}
    finally:
        log.close()


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
            return latest_file, metadata
    except Exception as e:
        print_color(RED, f"Error reading metadata: {e}")

    return latest_file, None


def parse_metadata_time(time_str):
    """Parse time string from metadata format like '2026-01-28 00:00:00 UTC+09:00'"""
    try:
        if 'UTC' in time_str:
            time_str = time_str.split(' UTC')[0]
        dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
        return dt
    except Exception:
        return None


def auto_verify_raw_data():
    """Auto-verify raw data using metadata from latest generated file"""
    base_url = os.environ.get('SOLTIX_URL', 'http://localhost:5555')

    print_color(BLUE, "=" * 70)
    print_color(BLUE, "Soltix Raw Data Verification Tool (Auto Mode - Multi-Type)")
    print_color(BLUE, "=" * 70)
    print()

    # Find latest generated file and extract metadata
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

    # Format times for API - use +09:00 timezone (Asia/Tokyo)
    start_time = start_dt.strftime("%Y-%m-%dT%H:%M:%S+09:00")
    end_time = end_dt.strftime("%Y-%m-%dT%H:%M:%S+09:00")

    # Generate device IDs and fields - use ALL devices and fields
    device_ids = [f"device-{i:03d}" for i in range(1, num_devices + 1)]
    fields = field_names  # All fields

    print()
    print_color(BLUE, "=" * 70)
    print_color(BLUE, "Metadata from Generated File")
    print_color(BLUE, "=" * 70)
    print(f"  Database: {database}")
    print(f"  Collection: {collection}")
    print(f"  Time Range: {start_time} → {end_time}")
    print(f"  Total Devices: {num_devices}")
    print(f"  Total Fields: {len(field_names)}")
    print(f"  Total Points: {metadata.get('total_points', 'N/A'):,}")

    if field_schemas:
        print()
        print_color(YELLOW, "  Field Types:")
        type_groups = defaultdict(list)
        for fname, ftype in field_schemas.items():
            type_groups[ftype].append(fname)
        for ftype in sorted(type_groups.keys()):
            print(f"    {ftype}: {', '.join(type_groups[ftype])}")

    null_pct = metadata.get('null_percentage', 0)
    if null_pct:
        print(f"  Null Percentage: {null_pct}%")

    print()

    # Use full time range
    test_end_time = end_time

    # Query raw data from Soltix - chunked by device, field, day
    print_color(BLUE, "=" * 70)
    print_color(BLUE, "Step 1: Query Raw Data from Soltix (Chunked)")
    print_color(BLUE, "=" * 70)
    print()

    url = f"{base_url}/v1/databases/{database}/collections/{collection}/query"

    # Calculate number of days
    num_days = (end_dt - start_dt).days + 1
    total_queries = len(device_ids) * len(fields) * num_days

    print_color(YELLOW, f"Query Strategy: 1 device × 1 field × 1 day per query")
    print_color(YELLOW, f"Total queries needed: {total_queries:,}")
    print_color(YELLOW, f"  - Devices: {len(device_ids)}")
    print_color(YELLOW, f"  - Fields: {len(fields)}")
    print_color(YELLOW, f"  - Days: {num_days}")
    print()

    # Collect all results
    all_results = []
    total_points_received = 0
    query_count = 0
    failed_queries = 0

    start_total = time.time()

    for device_id in device_ids:
        for field in fields:
            current_day = start_dt
            while current_day < end_dt:
                next_day = current_day + timedelta(days=1)
                if next_day > end_dt:
                    next_day = end_dt

                day_start = current_day.strftime("%Y-%m-%dT%H:%M:%S+09:00")
                day_end = next_day.strftime("%Y-%m-%dT%H:%M:%S+09:00")

                payload = {
                    "start_time": day_start,
                    "end_time": day_end,
                    "limit": 0,
                    "ids": [device_id],
                    "fields": [field]
                }

                query_count += 1

                try:
                    response = requests.post(url, json=payload, timeout=30)

                    if response.status_code == 200:
                        response_data = response.json()
                        results = response_data.get('results', response_data.get('data', []))

                        if results:
                            for result in results:
                                all_results.append(result)
                                if 'times' in result:
                                    total_points_received += len(result['times'])
                                else:
                                    total_points_received += 1
                    else:
                        failed_queries += 1
                except Exception:
                    failed_queries += 1

                progress = query_count / total_queries * 100
                print(f"\r  Progress: {query_count:,}/{total_queries:,} ({progress:.1f}%) "
                      f"- Points: {total_points_received:,} - Failed: {failed_queries}",
                      end='', flush=True)

                current_day = next_day

    total_time = time.time() - start_total
    print()
    print()

    if failed_queries > 0:
        print_color(RED, f"⚠️  {failed_queries} queries failed")

    print_color(GREEN, f"✅ Completed {query_count:,} queries in {total_time:.1f}s")
    print_color(GREEN, f"✅ Total points received: {total_points_received:,}")

    # Build response data structure
    response_data = {'results': all_results}

    # Save query results to temp file
    results_dir = os.path.join(script_dir, "test_data", "results")
    os.makedirs(results_dir, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    query_result_file = os.path.join(results_dir, f"auto_verify_{ts}.json")

    with open(query_result_file, 'w') as f:
        json.dump(response_data, f, indent=2)

    print()

    # Verify against generated data
    print_color(BLUE, "=" * 70)
    print_color(BLUE, "Step 2: Verify Against Generated Data (Multi-Type)")
    print_color(BLUE, "=" * 70)

    verify_query_results(
        query_result_file,
        generated_file,
        device_ids=device_ids,
        fields=fields,
        start_time=start_time,
        end_time=test_end_time
    )

    # Cleanup temp file
    os.remove(query_result_file)

    print()
    print_color(BLUE, "=" * 70)
    print_color(BLUE, "Verification Complete")
    print_color(BLUE, "=" * 70)


def main():
    auto_verify_raw_data()


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
