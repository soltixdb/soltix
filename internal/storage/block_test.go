package storage

import (
	"testing"
	"time"
)

func TestNewDeviceBlock(t *testing.T) {
	now := time.Now()
	points := []*DataPoint{
		{
			Time:       now,
			ID:         "device1",
			Fields:     map[string]interface{}{"temp": 20.0},
			InsertedAt: now,
		},
		{
			Time:       now.Add(1 * time.Second),
			ID:         "device1",
			Fields:     map[string]interface{}{"temp": 21.0},
			InsertedAt: now,
		},
		{
			Time:       now.Add(2 * time.Second),
			ID:         "device1",
			Fields:     map[string]interface{}{"temp": 22.0},
			InsertedAt: now,
		},
	}

	block, err := NewDeviceBlock("device1", points)
	if err != nil {
		t.Fatalf("NewDeviceBlock failed: %v", err)
	}

	if block == nil {
		t.Fatal("Expected non-nil DeviceBlock")
		return
	}

	if block.DeviceID != "device1" {
		t.Errorf("Expected DeviceID device1, got %s", block.DeviceID)
	}

	if block.EntryCount != 3 {
		t.Errorf("Expected EntryCount 3, got %d", block.EntryCount)
	}

	if block.BaseTime != now.UnixNano() {
		t.Error("BaseTime not set correctly")
	}

	if len(block.Deltas) != 2 {
		t.Errorf("Expected 2 deltas, got %d", len(block.Deltas))
	}

	if len(block.Fields) != 3 {
		t.Errorf("Expected 3 field entries, got %d", len(block.Fields))
	}
}

func TestNewDeviceBlock_EmptyPoints(t *testing.T) {
	points := []*DataPoint{}

	block, err := NewDeviceBlock("device1", points)
	if err == nil {
		t.Error("Expected error for empty points")
	}

	if block != nil {
		t.Error("Expected nil block for empty points")
	}
}

func TestNewDeviceBlock_DeltaEncoding(t *testing.T) {
	now := time.Now()

	// Create points with 1 second intervals
	points := make([]*DataPoint, 5)
	for i := 0; i < 5; i++ {
		points[i] = &DataPoint{
			Time:       now.Add(time.Duration(i) * time.Second),
			ID:         "device1",
			Fields:     map[string]interface{}{"temp": float64(20 + i)},
			InsertedAt: now,
		}
	}

	block, err := NewDeviceBlock("device1", points)
	if err != nil {
		t.Fatalf("NewDeviceBlock failed: %v", err)
	}

	// Verify deltas are all 1000ms (1 second)
	for i, delta := range block.Deltas {
		if delta != 1000 {
			t.Errorf("Delta %d: expected 1000ms, got %d", i, delta)
		}
	}
}

func TestNewDeviceBlock_FieldsWithMetadata(t *testing.T) {
	now := time.Now()
	insertTime := now.Add(-1 * time.Second)

	points := []*DataPoint{
		{
			Time:       now,
			ID:         "device1",
			Fields:     map[string]interface{}{"temp": 20.0, "humidity": 60.0},
			InsertedAt: insertTime,
		},
	}

	block, err := NewDeviceBlock("device1", points)
	if err != nil {
		t.Fatalf("NewDeviceBlock failed: %v", err)
	}

	fields := block.Fields[0]

	// Verify user fields
	if temp, ok := fields["temp"].(float64); !ok || temp != 20.0 {
		t.Error("User field 'temp' not preserved")
	}

	if humidity, ok := fields["humidity"].(float64); !ok || humidity != 60.0 {
		t.Error("User field 'humidity' not preserved")
	}

	// Verify system metadata added
	if _, ok := fields["_inserted_at"]; !ok {
		t.Error("System metadata '_inserted_at' not added")
	}

	if insertedAt, ok := fields["_inserted_at"].(int64); !ok || insertedAt != insertTime.UnixNano() {
		t.Error("System metadata '_inserted_at' has wrong value")
	}
}

func TestDeviceBlock_Encode(t *testing.T) {
	now := time.Now()
	points := []*DataPoint{
		{
			Time:       now,
			ID:         "device1",
			Fields:     map[string]interface{}{"temp": 20.0},
			InsertedAt: now,
		},
		{
			Time:       now.Add(1 * time.Second),
			ID:         "device1",
			Fields:     map[string]interface{}{"temp": 21.0},
			InsertedAt: now,
		},
	}

	block, _ := NewDeviceBlock("device1", points)

	data, err := block.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	if len(data) == 0 {
		t.Error("Expected non-empty encoded data")
	}

	// Verify data contains expected components
	// Device ID length (4 bytes) + Device ID + Entry count (4 bytes) + BaseTime (8 bytes) + Deltas
	minExpectedSize := 4 + len("device1") + 4 + 8 + 4 // 1 delta
	if len(data) < minExpectedSize {
		t.Errorf("Encoded data too small: %d bytes (expected at least %d)", len(data), minExpectedSize)
	}
}

func TestDeviceBlock_EncodeAndDecode(t *testing.T) {
	now := time.Now()
	deviceID := "test-device-123"

	points := []*DataPoint{
		{
			Time:       now,
			ID:         deviceID,
			Fields:     map[string]interface{}{"temp": 20.0},
			InsertedAt: now,
		},
		{
			Time:       now.Add(1 * time.Second),
			ID:         deviceID,
			Fields:     map[string]interface{}{"temp": 21.0},
			InsertedAt: now,
		},
		{
			Time:       now.Add(3 * time.Second),
			ID:         deviceID,
			Fields:     map[string]interface{}{"temp": 22.0},
			InsertedAt: now,
		},
	}

	// Encode
	block, _ := NewDeviceBlock(deviceID, points)
	data, err := block.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Decode
	decoded, err := DecodeDeviceBlock(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// Verify decoded data
	if decoded.DeviceID != deviceID {
		t.Errorf("DeviceID mismatch: expected %s, got %s", deviceID, decoded.DeviceID)
	}

	if decoded.EntryCount != 3 {
		t.Errorf("EntryCount mismatch: expected 3, got %d", decoded.EntryCount)
	}

	if decoded.BaseTime != now.UnixNano() {
		t.Error("BaseTime mismatch")
	}

	if len(decoded.Deltas) != 2 {
		t.Errorf("Deltas length mismatch: expected 2, got %d", len(decoded.Deltas))
	}

	// Verify deltas
	if decoded.Deltas[0] != 1000 {
		t.Errorf("Delta 0: expected 1000ms, got %d", decoded.Deltas[0])
	}
	if decoded.Deltas[1] != 2000 {
		t.Errorf("Delta 1: expected 2000ms, got %d", decoded.Deltas[1])
	}
}

func TestDecodeDeviceBlock_InvalidData(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "Empty data",
			data: []byte{},
		},
		{
			name: "Too short",
			data: []byte{1, 2, 3},
		},
		{
			name: "Invalid device ID length",
			data: []byte{255, 255, 255, 255}, // Huge device ID length
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeDeviceBlock(tt.data)
			if err == nil {
				t.Error("Expected error for invalid data")
			}
		})
	}
}

func TestDeviceBlock_GetTimestamps(t *testing.T) {
	now := time.Now()

	points := []*DataPoint{
		{
			Time:       now,
			ID:         "device1",
			Fields:     map[string]interface{}{"temp": 20.0},
			InsertedAt: now,
		},
		{
			Time:       now.Add(1 * time.Second),
			ID:         "device1",
			Fields:     map[string]interface{}{"temp": 21.0},
			InsertedAt: now,
		},
		{
			Time:       now.Add(3 * time.Second),
			ID:         "device1",
			Fields:     map[string]interface{}{"temp": 22.0},
			InsertedAt: now,
		},
	}

	block, _ := NewDeviceBlock("device1", points)
	timestamps := block.GetTimestamps()

	if len(timestamps) != 3 {
		t.Errorf("Expected 3 timestamps, got %d", len(timestamps))
	}

	// Verify timestamps match original
	for i, ts := range timestamps {
		expected := points[i].Time
		if !ts.Equal(expected) {
			t.Errorf("Timestamp %d: expected %v, got %v", i, expected, ts)
		}
	}
}

func TestDeviceBlock_CompressionStats(t *testing.T) {
	now := time.Now()

	// Create 10 points
	points := make([]*DataPoint, 10)
	for i := 0; i < 10; i++ {
		points[i] = &DataPoint{
			Time:       now.Add(time.Duration(i) * time.Second),
			ID:         "device1",
			Fields:     map[string]interface{}{"temp": float64(20 + i)},
			InsertedAt: now,
		}
	}

	block, _ := NewDeviceBlock("device1", points)
	stats := block.CompressionStats()

	if stats["original_bytes"] == nil {
		t.Error("Missing original_bytes in stats")
	}

	if stats["delta_bytes"] == nil {
		t.Error("Missing delta_bytes in stats")
	}

	if stats["compression"] == nil {
		t.Error("Missing compression in stats")
	}

	// Original should be 10 * 8 = 80 bytes (int64 timestamps)
	if originalBytes, ok := stats["original_bytes"].(int); !ok || originalBytes != 80 {
		t.Errorf("Expected original_bytes 80, got %v", stats["original_bytes"])
	}

	// Delta should be 8 (base) + 9 * 4 = 44 bytes
	if deltaBytes, ok := stats["delta_bytes"].(int); !ok || deltaBytes != 44 {
		t.Errorf("Expected delta_bytes 44, got %v", stats["delta_bytes"])
	}

	t.Logf("Compression stats: %+v", stats)
}

func TestEncodeFieldsProto(t *testing.T) {
	fields := []map[string]interface{}{
		{"temp": 20.0, "humidity": 60.0},
		{"temp": 21.0, "humidity": 61.0},
		{"temp": 22.0, "humidity": 62.0},
	}

	data, err := EncodeFieldsProto(fields)
	if err != nil {
		t.Fatalf("EncodeFieldsProto failed: %v", err)
	}

	if len(data) == 0 {
		t.Error("Expected non-empty encoded data")
	}
}

func TestEncodeAndDecodeFieldsProto(t *testing.T) {
	original := []map[string]interface{}{
		{"temp": 20.5, "humidity": 60.2, "pressure": 1013.25},
		{"temp": 21.5, "humidity": 61.2, "pressure": 1013.50},
		{"temp": 22.5, "humidity": 62.2, "pressure": 1013.75},
	}

	// Encode
	data, err := EncodeFieldsProto(original)
	if err != nil {
		t.Fatalf("EncodeFieldsProto failed: %v", err)
	}

	// Decode
	decoded, err := DecodeFieldsProto(data)
	if err != nil {
		t.Fatalf("DecodeFieldsProto failed: %v", err)
	}

	// Verify
	if len(decoded) != len(original) {
		t.Errorf("Length mismatch: expected %d, got %d", len(original), len(decoded))
	}

	for i, fields := range decoded {
		if len(fields) != len(original[i]) {
			t.Errorf("Entry %d: field count mismatch", i)
		}

		// Verify each field
		for key, val := range original[i] {
			decodedVal, ok := fields[key]
			if !ok {
				t.Errorf("Entry %d: missing field %s", i, key)
				continue
			}

			// Compare values (handle float64 conversion)
			if origFloat, ok := val.(float64); ok {
				if decodedFloat, ok := decodedVal.(float64); !ok || origFloat != decodedFloat {
					t.Errorf("Entry %d field %s: expected %v, got %v", i, key, val, decodedVal)
				}
			}
		}
	}
}

func TestEncodeFieldsProto_DifferentTypes(t *testing.T) {
	fields := []map[string]interface{}{
		{
			"float_val":  20.5,
			"string_val": "test",
			"bool_val":   true,
			"int_val":    int64(100),
		},
	}

	// Encode
	data, err := EncodeFieldsProto(fields)
	if err != nil {
		t.Fatalf("EncodeFieldsProto failed: %v", err)
	}

	// Decode
	decoded, err := DecodeFieldsProto(data)
	if err != nil {
		t.Fatalf("DecodeFieldsProto failed: %v", err)
	}

	// Verify all types preserved
	if decoded[0]["float_val"].(float64) != 20.5 {
		t.Error("Float value not preserved")
	}

	if decoded[0]["string_val"].(string) != "test" {
		t.Error("String value not preserved")
	}

	if decoded[0]["bool_val"].(bool) != true {
		t.Error("Bool value not preserved")
	}

	if decoded[0]["int_val"].(int64) != 100 {
		t.Error("Int value not preserved")
	}
}

func TestDecodeFieldsProto_InvalidData(t *testing.T) {
	invalidData := []byte{1, 2, 3, 4, 5}

	_, err := DecodeFieldsProto(invalidData)
	if err == nil {
		t.Error("Expected error for invalid protobuf data")
	}
}

func TestDeviceBlock_SinglePoint(t *testing.T) {
	now := time.Now()

	points := []*DataPoint{
		{
			Time:       now,
			ID:         "device1",
			Fields:     map[string]interface{}{"temp": 20.0},
			InsertedAt: now,
		},
	}

	block, err := NewDeviceBlock("device1", points)
	if err != nil {
		t.Fatalf("NewDeviceBlock failed: %v", err)
	}

	if block.EntryCount != 1 {
		t.Errorf("Expected EntryCount 1, got %d", block.EntryCount)
	}

	if len(block.Deltas) != 0 {
		t.Errorf("Expected 0 deltas for single point, got %d", len(block.Deltas))
	}

	timestamps := block.GetTimestamps()
	if len(timestamps) != 1 {
		t.Errorf("Expected 1 timestamp, got %d", len(timestamps))
	}

	if !timestamps[0].Equal(now) {
		t.Error("Timestamp mismatch for single point")
	}
}

func TestDeviceBlock_LargeDataset(t *testing.T) {
	now := time.Now()

	// Create 1000 points
	points := make([]*DataPoint, 1000)
	for i := 0; i < 1000; i++ {
		points[i] = &DataPoint{
			Time:       now.Add(time.Duration(i) * time.Second),
			ID:         "device1",
			Fields:     map[string]interface{}{"temp": float64(20 + i%10)},
			InsertedAt: now,
		}
	}

	block, err := NewDeviceBlock("device1", points)
	if err != nil {
		t.Fatalf("NewDeviceBlock failed: %v", err)
	}

	if block.EntryCount != 1000 {
		t.Errorf("Expected EntryCount 1000, got %d", block.EntryCount)
	}

	// Test encoding
	data, err := block.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Test decoding
	decoded, err := DecodeDeviceBlock(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.EntryCount != block.EntryCount {
		t.Error("EntryCount mismatch after encode/decode")
	}

	stats := block.CompressionStats()
	t.Logf("Large dataset compression: %+v", stats)
}
