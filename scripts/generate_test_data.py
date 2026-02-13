#!/usr/bin/env python3
"""
Interactive script to generate and push random time-series data to Soltix
This script will:
1. Check and create database if not exists
2. Check and create collection if not exists
3. Generate and push random data from start to end time with specified interval

Supports multiple data types: int, float, bool, string, and null
"""

import requests
import random
import time
import string
from datetime import datetime, timedelta, timezone
import sys
import os
import json

# Timezone UTC+9 (Asia/Tokyo)
TZ_UTC9 = timezone(timedelta(hours=9))

# Color codes
RED = '\033[0;31m'
GREEN = '\033[0;32m'
YELLOW = '\033[1;33m'
BLUE = '\033[0;34m'
NC = '\033[0m'

# Data type categories
FIELD_TYPES = ['int', 'float', 'bool', 'string']


def print_color(color, text, end='\n'):
    print(f"{color}{text}{NC}", end=end)


def generate_int_value(counter):
    """Generate an integer value (sequential)"""
    return counter


def generate_float_value(counter):
    """Generate a float value (sequential with decimal)"""
    return round(counter * 1.1, 2)


def generate_bool_value(counter):
    """Generate a boolean value (alternating)"""
    return counter % 2 == 0


def generate_string_value(counter):
    """Generate a string value"""
    labels = ["alpha", "beta", "gamma", "delta", "epsilon",
              "zeta", "eta", "theta", "iota", "kappa"]
    return f"{labels[counter % len(labels)]}_{counter}"


def get_api_schema_type(field_type):
    """Map internal field type to API schema type"""
    mapping = {
        'int': 'number',
        'float': 'number',
        'bool': 'boolean',
        'string': 'string',
    }
    return mapping.get(field_type, 'string')


def main():
    # Get base URL from environment or use default
    base_url = os.environ.get('SOLTIX_URL', 'http://localhost:5555')

    print_color(BLUE, "=" * 60)
    print_color(BLUE, "Soltix Multi-Type Test Data Generator (Python)")
    print_color(BLUE, "=" * 60)
    print()
    print_color(YELLOW, "Supported field types: int, float, bool, string, null")
    print()

    # Prompt for inputs with defaults
    database = input("Enter database name [shirokuma]: ").strip() or "shirokuma"
    collection = input("Enter collection name [multi_type_test]: ").strip() or "multi_type_test"
    num_ids_input = input("Enter number of device IDs [3]: ").strip()
    num_ids = int(num_ids_input) if num_ids_input else 3

    # Fields per type
    num_int_fields_input = input("Enter number of int fields [2]: ").strip()
    num_int_fields = int(num_int_fields_input) if num_int_fields_input else 2

    num_float_fields_input = input("Enter number of float fields [2]: ").strip()
    num_float_fields = int(num_float_fields_input) if num_float_fields_input else 2

    num_bool_fields_input = input("Enter number of bool fields [2]: ").strip()
    num_bool_fields = int(num_bool_fields_input) if num_bool_fields_input else 2

    num_string_fields_input = input("Enter number of string fields [2]: ").strip()
    num_string_fields = int(num_string_fields_input) if num_string_fields_input else 2

    null_percentage_input = input("Enter null value percentage (0-100) [10]: ").strip()
    null_percentage = int(null_percentage_input) if null_percentage_input else 10

    start_date_input = input("Enter start date (YYYYMMDD) [20260201]: ").strip() or "20260201"
    time_range_input = input("Enter time range in days [1]: ").strip()
    time_range_days = int(time_range_input) if time_range_input else 1
    interval_input = input("Enter interval in seconds [60]: ").strip()
    interval = int(interval_input) if interval_input else 60

    # Parse start date and set time to 00:00:00 in UTC+9
    start_time = datetime.strptime(start_date_input, "%Y%m%d").replace(tzinfo=TZ_UTC9)
    start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S %Z")

    # Calculate end time
    end_time = start_time + timedelta(days=time_range_days) - timedelta(seconds=1)
    end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S %Z")

    # Build field definitions: {name, type, api_type}
    field_defs = []

    for i in range(num_int_fields):
        field_defs.append({'name': f'int_field_{i}', 'type': 'int', 'api_type': 'number'})

    for i in range(num_float_fields):
        field_defs.append({'name': f'float_field_{i}', 'type': 'float', 'api_type': 'number'})

    for i in range(num_bool_fields):
        field_defs.append({'name': f'bool_field_{i}', 'type': 'bool', 'api_type': 'boolean'})

    for i in range(num_string_fields):
        field_defs.append({'name': f'string_field_{i}', 'type': 'string', 'api_type': 'string'})

    field_names = [f['name'] for f in field_defs]
    num_fields = len(field_defs)

    # Debug: Print parsed times
    print()
    print_color(BLUE, "Parsed times:")
    print(f"  Start: {start_time} (timestamp: {start_time.timestamp()})")
    print(f"  End: {end_time} (timestamp: {end_time.timestamp()})")
    print(f"  Time range: {time_range_days} days")
    print(f"  Duration: {end_time - start_time}")

    # Calculate total points
    total_seconds = int((end_time - start_time).total_seconds())
    points_per_id = total_seconds // interval + 1
    total_points = points_per_id * num_ids

    print()
    # Prepare data file path
    timestamp_now = datetime.now().strftime("%Y%m%d_%H%M%S")
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_data")
    os.makedirs(data_dir, exist_ok=True)
    data_file = os.path.join(data_dir, f"generated_data_{database}_{collection}_{timestamp_now}.json")

    print_color(YELLOW, "Configuration:")
    print(f"  Base URL: {base_url}")
    print(f"  Database: {database}")
    print(f"  Collection: {collection}")
    print(f"  Device IDs: {num_ids}")
    print(f"  Start Time: {start_time_str}")
    print(f"  End Time: {end_time_str}")
    print(f"  Interval: {interval}s")
    print()
    print_color(YELLOW, "Field Configuration:")
    print(f"  Int fields:    {num_int_fields} ({', '.join(f['name'] for f in field_defs if f['type'] == 'int')})")
    print(f"  Float fields:  {num_float_fields} ({', '.join(f['name'] for f in field_defs if f['type'] == 'float')})")
    print(f"  Bool fields:   {num_bool_fields} ({', '.join(f['name'] for f in field_defs if f['type'] == 'bool')})")
    print(f"  String fields: {num_string_fields} ({', '.join(f['name'] for f in field_defs if f['type'] == 'string')})")
    print(f"  Null %:        {null_percentage}% (randomly inserted across all fields)")
    print(f"  Total fields:  {num_fields}")
    print()
    print_color(YELLOW, f"Estimated data points: {total_points} ({num_ids} devices × {points_per_id} points)")
    print_color(YELLOW, f"Data will be saved to: {data_file}")
    print()

    confirm = input("Continue? (y/n): ").strip().lower()
    if confirm not in ['y', 'yes']:
        print("Cancelled.")
        return

    print()
    print_color(BLUE, "=" * 60)
    print_color(BLUE, "Starting Data Generation")
    print_color(BLUE, "=" * 60)
    print()

    # Step 1: Check/Create Database
    print_color(YELLOW, "Step 1: Checking database...")
    try:
        resp = requests.get(f"{base_url}/v1/databases/{database}")
        if resp.status_code == 200:
            print_color(GREEN, f"✅ Database '{database}' already exists")
        elif resp.status_code == 404:
            print_color(YELLOW, f"Creating database '{database}'...")
            resp = requests.post(f"{base_url}/v1/databases",
                                json={"name": database, "description": "Auto-generated test database"})
            if resp.status_code == 201:
                print_color(GREEN, f"✅ Database '{database}' created successfully")
            else:
                print_color(RED, f"❌ Failed to create database (HTTP {resp.status_code})")
                print(resp.text)
                return
        else:
            print_color(RED, f"❌ Failed to check database (HTTP {resp.status_code})")
            return
    except Exception as e:
        print_color(RED, f"❌ Error: {e}")
        return

    print()

    # Step 2: Check/Create Collection
    print_color(YELLOW, "Step 2: Checking collection...")
    try:
        resp = requests.get(f"{base_url}/v1/databases/{database}/collections/{collection}")
        if resp.status_code == 200:
            print_color(GREEN, f"✅ Collection '{collection}' already exists")
        elif resp.status_code == 404:
            print_color(YELLOW, f"Creating collection '{collection}'...")

            # Build schema fields - time and id are required
            schema_fields = [
                {"name": "time", "type": "timestamp", "required": True},
                {"name": "id", "type": "string", "required": True}
            ]
            # Add data fields with proper API types
            for fd in field_defs:
                schema_fields.append({
                    "name": fd['name'],
                    "type": fd['api_type'],
                    "required": False
                })

            resp = requests.post(f"{base_url}/v1/databases/{database}/collections",
                                json={
                                    "name": collection,
                                    "description": "Auto-generated multi-type test collection",
                                    "fields": schema_fields
                                })
            if resp.status_code == 201:
                print_color(GREEN, f"✅ Collection '{collection}' created successfully")
            else:
                print_color(RED, f"❌ Failed to create collection (HTTP {resp.status_code})")
                print(resp.text)
                return
        else:
            print_color(RED, f"❌ Failed to check collection (HTTP {resp.status_code})")
            return
    except Exception as e:
        print_color(RED, f"❌ Error: {e}")
        return

    print()

    # Step 3: Push data
    print_color(YELLOW, "Step 3: Generating and pushing multi-type data...")
    print_color(BLUE, f"Starting at {start_time_str}")
    print_color(BLUE, f"Fields: {' '.join(field_names)}")
    print()

    # Statistics
    pushed_count = 0
    failed_count = 0
    batch_size = 100
    batch_num = 0
    null_count = 0
    type_counts = {t: 0 for t in FIELD_TYPES}

    # Generate and push data
    batch_points = []
    all_points = []
    current_time = start_time
    generated_count = 0
    iterations = 0
    batch_start_time = None
    batch_end_time = None

    # Counter for sequential values per field
    field_counters = {fd['name']: 1 for fd in field_defs}

    # Use a fixed seed for reproducibility
    random.seed(42)

    print_color(BLUE, "⏳ Generating and pushing multi-type data...")
    print_color(BLUE, f"DEBUG: Start={start_time}, End={end_time}, Interval={interval}s")
    print_color(BLUE, f"Field types: int={num_int_fields}, float={num_float_fields}, "
                      f"bool={num_bool_fields}, string={num_string_fields}")
    print_color(BLUE, f"Null percentage: {null_percentage}%")

    while current_time <= end_time:
        iterations += 1
        iso_time = current_time.strftime("%Y-%m-%dT%H:%M:%S") + "+09:00"

        # Debug first 3 iterations
        if iterations <= 3:
            print_color(YELLOW, f"  Iteration {iterations}: time={iso_time}")

        for device_id in range(1, num_ids + 1):
            device_name = f"device-{device_id:03d}"

            # Build point
            point = {
                "time": iso_time,
                "id": device_name
            }

            # Add typed field values
            for fd in field_defs:
                fname = fd['name']
                ftype = fd['type']
                counter = field_counters[fname]

                # Check if this value should be null
                if null_percentage > 0 and random.randint(1, 100) <= null_percentage:
                    point[fname] = None
                    null_count += 1
                else:
                    if ftype == 'int':
                        point[fname] = generate_int_value(counter)
                        type_counts['int'] += 1
                    elif ftype == 'float':
                        point[fname] = generate_float_value(counter)
                        type_counts['float'] += 1
                    elif ftype == 'bool':
                        point[fname] = generate_bool_value(counter)
                        type_counts['bool'] += 1
                    elif ftype == 'string':
                        point[fname] = generate_string_value(counter)
                        type_counts['string'] += 1

                field_counters[fname] = counter + 1

            batch_points.append(point)
            all_points.append(point)
            generated_count += 1

            # Track batch time range
            if batch_start_time is None:
                batch_start_time = iso_time
            batch_end_time = iso_time

            # Show progress every 100 points
            if generated_count % 100 == 0:
                print(f"\r{BLUE}Generated: {generated_count} / {total_points} points "
                      f"(batch #{batch_num + 1}...){NC}", end='', flush=True)

            # Push batch when full
            if len(batch_points) >= batch_size:
                batch_num += 1

                print()
                json_size = len(json.dumps(batch_points))
                print_color(BLUE, f"\n[Batch #{batch_num}] JSON created: "
                                  f"{len(batch_points)} points, {json_size:,} bytes")
                print_color(BLUE, f"[Batch #{batch_num}] Time range: "
                                  f"{batch_start_time} → {batch_end_time}")

                print_color(YELLOW, f"[Batch #{batch_num}] Calling API...")
                start_api = time.time()

                try:
                    resp = requests.post(
                        f"{base_url}/v1/databases/{database}/collections/{collection}/write/batch",
                        json={"points": batch_points}
                    )

                    api_duration = int((time.time() - start_api) * 1000)

                    if resp.status_code in [200, 202]:
                        pushed_count += len(batch_points)
                        print_color(GREEN, f"[Batch #{batch_num}] ✅ Success (HTTP {resp.status_code}) "
                                          f"- Duration: {api_duration}ms "
                                          f"- Total: {pushed_count} / {total_points}")
                    else:
                        failed_count += len(batch_points)
                        print_color(RED, f"[Batch #{batch_num}] ❌ Failed (HTTP {resp.status_code}) "
                                        f"- Duration: {api_duration}ms")
                        print(resp.text)
                except Exception as e:
                    failed_count += len(batch_points)
                    api_duration = int((time.time() - start_api) * 1000)
                    print_color(RED, f"[Batch #{batch_num}] ❌ Error: {e} - Duration: {api_duration}ms")

                batch_points = []
                batch_start_time = None
                batch_end_time = None

        current_time += timedelta(seconds=interval)

    print()
    print_color(BLUE, f"DEBUG: Total iterations = {iterations}, Generated points = {generated_count}")

    # Push remaining batch
    if batch_points:
        batch_num += 1

        print()
        json_size = len(json.dumps(batch_points))
        print_color(BLUE, f"\n[Batch #{batch_num} - Final] JSON created: "
                          f"{len(batch_points)} points, {json_size:,} bytes")
        print_color(BLUE, f"[Batch #{batch_num} - Final] Time range: "
                          f"{batch_start_time} → {batch_end_time}")

        print_color(YELLOW, f"[Batch #{batch_num} - Final] Calling API...")
        start_api = time.time()

        try:
            resp = requests.post(
                f"{base_url}/v1/databases/{database}/collections/{collection}/write/batch",
                json={"points": batch_points}
            )

            api_duration = int((time.time() - start_api) * 1000)

            if resp.status_code in [200, 202]:
                pushed_count += len(batch_points)
                print_color(GREEN, f"[Batch #{batch_num} - Final] ✅ Success "
                                  f"(HTTP {resp.status_code}) - Duration: {api_duration}ms")
            else:
                failed_count += len(batch_points)
                print_color(RED, f"[Batch #{batch_num} - Final] ❌ Failed "
                                f"(HTTP {resp.status_code}) - Duration: {api_duration}ms")
                print(resp.text)
        except Exception as e:
            failed_count += len(batch_points)
            api_duration = int((time.time() - start_api) * 1000)
            print_color(RED, f"[Batch #{batch_num} - Final] ❌ Error: {e} "
                            f"- Duration: {api_duration}ms")

    # Build field_schemas for metadata
    field_schemas = {}
    for fd in field_defs:
        field_schemas[fd['name']] = fd['type']

    # Save all points to file
    print()
    print_color(YELLOW, "Saving data to file...")
    try:
        with open(data_file, 'w') as f:
            json.dump({
                "metadata": {
                    "database": database,
                    "collection": collection,
                    "start_time": start_time_str,
                    "end_time": end_time_str,
                    "interval_seconds": interval,
                    "num_devices": num_ids,
                    "num_fields": num_fields,
                    "field_names": field_names,
                    "field_schemas": field_schemas,
                    "null_percentage": null_percentage,
                    "total_points": len(all_points),
                    "generated_at": timestamp_now,
                    "type_stats": {
                        "int_values": type_counts['int'],
                        "float_values": type_counts['float'],
                        "bool_values": type_counts['bool'],
                        "string_values": type_counts['string'],
                        "null_values": null_count,
                    }
                },
                "points": all_points
            }, f, indent=2)
        print_color(GREEN, f"✅ Data saved to: {data_file}")
    except Exception as e:
        print_color(RED, f"❌ Failed to save data: {e}")

    # Summary
    print()
    print()
    print_color(BLUE, "=" * 60)
    print_color(BLUE, "Summary")
    print_color(BLUE, "=" * 60)
    print_color(GREEN, f"✅ Successfully pushed: {pushed_count} points")
    if failed_count > 0:
        print_color(RED, f"❌ Failed: {failed_count} points")
    print_color(YELLOW, f"Database: {database}")
    print_color(YELLOW, f"Collection: {collection}")
    print_color(YELLOW, f"Devices: {num_ids}")
    print_color(YELLOW, f"Time range: {start_time_str} to {end_time_str}")
    print()
    print_color(YELLOW, "Type Statistics:")
    print(f"  Int values:    {type_counts['int']:,}")
    print(f"  Float values:  {type_counts['float']:,}")
    print(f"  Bool values:   {type_counts['bool']:,}")
    print(f"  String values: {type_counts['string']:,}")
    print(f"  Null values:   {null_count:,}")
    print_color(YELLOW, f"Data file: {data_file}")
    print()
    print_color(GREEN, "Done!")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print_color(YELLOW, "\n\n⚠️  Interrupted by user")
        sys.exit(130)
    except Exception as e:
        print_color(RED, f"\n\n❌ Error: {e}")
        sys.exit(1)
