#!/bin/bash
# Example: Testing Streaming Query API

# Set base URL and API key
BASE_URL="http://localhost:5555"
API_KEY="your-api-key-here"

echo "===================="
echo "Streaming Query API Demo"
echo "===================="
echo ""

# Example 3: POST request with JSON body
echo "3. Streaming with POST (JSON body)"
echo "   POST /v1/databases/shirokuma/collections/plant_power/query/stream"
echo ""
curl -N -X POST \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "start_time": "2026-02-01T00:00:00Z",
    "end_time": "2026-02-28T00:00:00Z",
    "ids": ["device-001", "device-002", "device-003", "device-004", "device-005", "device-006", "device-007", "device-008", "device-009", "device-010"],
    "fields": ["field_0", "field_1", "field_2", "field_3", "field_4", "field_5", "field_6", "field_7", "field_8", "field_9"],
    "interval": "1m",
    "aggregation": "avg",
    "chunk_size": 1000
  }' \
  "${BASE_URL}/v1/databases/shirokuma/collections/plant_power/query/stream" \
  2>/dev/null | head -20
echo ""
echo ""
