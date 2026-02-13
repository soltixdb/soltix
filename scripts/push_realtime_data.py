#!/usr/bin/env python3
"""
Real-time data pusher for Soltix
This script continuously pushes time-series data every minute at second 00
"""

import requests
import random
import time
from datetime import datetime, timedelta, timezone
import sys
import os

# Timezone UTC+9 (Asia/Tokyo)
TZ_UTC9 = timezone(timedelta(hours=9))

# Color codes
RED = '\033[0;31m'
GREEN = '\033[0;32m'
YELLOW = '\033[1;33m'
BLUE = '\033[0;34m'
NC = '\033[0m'

def print_color(color, text, end='\n'):
    print(f"{color}{text}{NC}", end=end)

def wait_for_next_interval(interval_seconds):
    """Wait until the next interval boundary"""
    now = datetime.now(TZ_UTC9)
    current_second = now.second
    # Calculate next interval boundary
    seconds_to_wait = interval_seconds - (current_second % interval_seconds)
    if seconds_to_wait == interval_seconds:
        seconds_to_wait = 0
    if seconds_to_wait > 0:
        time.sleep(seconds_to_wait)

def main():
    # Get base URL from environment or use default
    base_url = os.environ.get('SOLTIX_URL', 'http://localhost:5555')
    
    print_color(BLUE, "=" * 50)
    print_color(BLUE, "Soltix Real-Time Data Pusher")
    print_color(BLUE, "=" * 50)
    print()
    
    # Prompt for inputs with defaults
    database = input("Enter database name [shirokuma]: ").strip() or "shirokuma"
    collection = input("Enter collection name [plant_power]: ").strip() or "plant_power"
    num_ids_input = input("Enter number of device IDs [10]: ").strip()
    num_ids = int(num_ids_input) if num_ids_input else 10
    num_fields_input = input("Enter number of data fields [10]: ").strip()
    num_fields = int(num_fields_input) if num_fields_input else 10
    interval_input = input("Enter data interval in seconds [10]: ").strip()
    interval = int(interval_input) if interval_input else 10
    
    print()
    print_color(YELLOW, "Configuration:")
    print(f"  Base URL: {base_url}")
    print(f"  Database: {database}")
    print(f"  Collection: {collection}")
    print(f"  Device IDs: {num_ids}")
    print(f"  Data Fields: {num_fields}")
    print(f"  Interval: {interval}s")
    print()
    
    # Step 1: Check/Create Database
    print_color(YELLOW, "Step 1: Checking database...")
    try:
        resp = requests.get(f"{base_url}/v1/databases/{database}")
        if resp.status_code == 200:
            print_color(GREEN, f"✅ Database '{database}' exists")
        elif resp.status_code == 404:
            print_color(YELLOW, f"Creating database '{database}'...")
            resp = requests.post(f"{base_url}/v1/databases", 
                               json={"name": database, "description": "Real-time test database"})
            if resp.status_code == 201:
                print_color(GREEN, f"✅ Database '{database}' created")
            else:
                print_color(RED, f"❌ Failed to create database (HTTP {resp.status_code})")
                return
        else:
            print_color(RED, f"❌ Failed to check database (HTTP {resp.status_code})")
            return
    except Exception as e:
        print_color(RED, f"❌ Error: {e}")
        return
    
    # Step 2: Check/Create Collection
    print_color(YELLOW, "Step 2: Checking collection...")
    try:
        resp = requests.get(f"{base_url}/v1/databases/{database}/collections/{collection}")
        if resp.status_code == 200:
            print_color(GREEN, f"✅ Collection '{collection}' exists")
        elif resp.status_code == 404:
            print_color(YELLOW, f"Creating collection '{collection}'...")
            resp = requests.post(f"{base_url}/v1/databases/{database}/collections",
                               json={"name": collection, "description": "Real-time test collection"})
            if resp.status_code == 201:
                print_color(GREEN, f"✅ Collection '{collection}' created")
            else:
                print_color(RED, f"❌ Failed to create collection (HTTP {resp.status_code})")
                return
        else:
            print_color(RED, f"❌ Failed to check collection (HTTP {resp.status_code})")
            return
    except Exception as e:
        print_color(RED, f"❌ Error: {e}")
        return
    
    print()
    
    # Step 3: Generate field names
    field_names = [f"field_{i}" for i in range(num_fields)]
    print_color(GREEN, f"✅ Fields: {' '.join(field_names)}")
    print()
    
    # Step 4: Start real-time pushing
    print_color(BLUE, "=" * 50)
    print_color(BLUE, "Starting Real-Time Data Push")
    print_color(BLUE, "=" * 50)
    print_color(YELLOW, "Press Ctrl+C to stop")
    print()
    
    # Statistics
    total_pushed = 0
    batch_count = 0
    
    # Wait for the next interval to start
    print_color(BLUE, f"⏳ Waiting for next {interval}s interval...")
    wait_for_next_interval(interval)
    
    # Main loop - push data every interval
    while True:
        try:
            batch_count += 1
            current_time = datetime.now(TZ_UTC9)
            # Format with timezone offset (e.g., 2026-01-26T10:00:00+09:00)
            iso_time = current_time.strftime("%Y-%m-%dT%H:%M:%S") + "+09:00"
            
            # Generate batch data for all devices
            batch_points = []
            for device_id in range(1, num_ids + 1):
                device_name = f"device-{device_id:03d}"
                
                # Build point
                point = {
                    "time": iso_time,
                    "id": device_name
                }
                
                # Add random field values
                for field_name in field_names:
                    point[field_name] = random.uniform(0.0, 10000000.0)
                
                batch_points.append(point)
            
            # Push batch
            print_color(BLUE, f"[{current_time.strftime('%Y-%m-%d %H:%M:%S')}] Pushing batch #{batch_count}...")
            print_color(BLUE, f"  Time: {iso_time}")
            print_color(BLUE, f"  Points: {len(batch_points)}")
            
            start_api = time.time()
            try:
                resp = requests.post(
                    f"{base_url}/v1/databases/{database}/collections/{collection}/write/batch",
                    json={"points": batch_points},
                    timeout=5
                )
                
                api_duration = int((time.time() - start_api) * 1000)
                
                if resp.status_code in [200, 202]:
                    total_pushed += len(batch_points)
                    print_color(GREEN, f"  ✅ Success (HTTP {resp.status_code}) - Duration: {api_duration}ms - Total: {total_pushed} points")
                else:
                    print_color(RED, f"  ❌ Failed (HTTP {resp.status_code}) - Duration: {api_duration}ms")
                    print(f"  {resp.text}")
            except requests.exceptions.Timeout:
                print_color(RED, f"  ❌ Request timeout after 5s")
            except Exception as e:
                print_color(RED, f"  ❌ Error: {e}")
            
            print()
            
            # Wait for next interval
            time.sleep(interval)
            
        except KeyboardInterrupt:
            print()
            print_color(YELLOW, "\n⚠️  Stopping...")
            break
    
    # Summary
    print()
    print_color(BLUE, "=" * 50)
    print_color(BLUE, "Summary")
    print_color(BLUE, "=" * 50)
    print_color(GREEN, f"✅ Total batches pushed: {batch_count}")
    print_color(GREEN, f"✅ Total data points: {total_pushed}")
    print_color(YELLOW, f"Database: {database}")
    print_color(YELLOW, f"Collection: {collection}")
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
        import traceback
        traceback.print_exc()
        sys.exit(1)
