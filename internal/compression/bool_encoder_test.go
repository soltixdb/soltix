package compression

import (
	"encoding/binary"
	"testing"
)

// =============================================================================
// Bool Encoder — Basic Tests
// =============================================================================

func TestBoolEncoder_Type(t *testing.T) {
	encoder := NewBoolEncoder()
	if encoder.Type() != ColumnTypeBool {
		t.Errorf("Expected ColumnTypeBool, got %d", encoder.Type())
	}
}

func TestBoolEncoder_EmptyValues(t *testing.T) {
	encoder := NewBoolEncoder()

	encoded, err := encoder.Encode([]interface{}{})
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	if encoded != nil {
		t.Errorf("Expected nil for empty values")
	}

	decoded, err := encoder.Decode(nil, 0)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	if decoded != nil {
		t.Errorf("Expected nil for empty decode")
	}
}

func TestBoolEncoder_BoolValues(t *testing.T) {
	encoder := NewBoolEncoder()

	values := []interface{}{true, false, true, false, true}

	encoded, err := encoder.Encode(values)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := encoder.Decode(encoded, len(values))
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	for i, v := range values {
		expected := v.(bool)
		actual := decoded[i].(bool)
		if expected != actual {
			t.Errorf("Value %d mismatch: expected %t, got %t", i, expected, actual)
		}
	}
}

func TestBoolEncoder_WithNulls(t *testing.T) {
	encoder := NewBoolEncoder()

	values := []interface{}{true, nil, false, nil, true}

	encoded, err := encoder.Encode(values)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := encoder.Decode(encoded, len(values))
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	for i, v := range values {
		if v == nil {
			if decoded[i] != nil {
				t.Errorf("Value %d: expected nil, got %v", i, decoded[i])
			}
		} else {
			expected := v.(bool)
			actual := decoded[i].(bool)
			if expected != actual {
				t.Errorf("Value %d mismatch: expected %t, got %t", i, expected, actual)
			}
		}
	}
}

func TestBoolEncoder_AllTrue(t *testing.T) {
	encoder := NewBoolEncoder()

	values := []interface{}{true, true, true, true, true}

	encoded, err := encoder.Encode(values)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := encoder.Decode(encoded, len(values))
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	for i, v := range values {
		expected := v.(bool)
		actual := decoded[i].(bool)
		if expected != actual {
			t.Errorf("Value %d mismatch: expected %t, got %t", i, expected, actual)
		}
	}
}

func TestBoolEncoder_AllFalse(t *testing.T) {
	encoder := NewBoolEncoder()

	values := []interface{}{false, false, false, false, false}

	encoded, err := encoder.Encode(values)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := encoder.Decode(encoded, len(values))
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	for i, v := range values {
		expected := v.(bool)
		actual := decoded[i].(bool)
		if expected != actual {
			t.Errorf("Value %d mismatch: expected %t, got %t", i, expected, actual)
		}
	}
}

func TestBoolEncoder_LargeDataset(t *testing.T) {
	encoder := NewBoolEncoder()

	values := make([]interface{}, 1000)
	for i := range values {
		values[i] = i%2 == 0
	}

	encoded, err := encoder.Encode(values)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := encoder.Decode(encoded, len(values))
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	for i, v := range values {
		expected := v.(bool)
		actual := decoded[i].(bool)
		if expected != actual {
			t.Errorf("Value %d mismatch: expected %t, got %t", i, expected, actual)
		}
	}
}

// =============================================================================
// Bool Encoder — Edge Cases
// =============================================================================

func TestBoolEncoder_SingleValue(t *testing.T) {
	encoder := NewBoolEncoder()

	for _, v := range []bool{true, false} {
		encoded, err := encoder.Encode([]interface{}{v})
		if err != nil {
			t.Fatalf("Encode(%v) failed: %v", v, err)
		}

		decoded, err := encoder.Decode(encoded, 1)
		if err != nil {
			t.Fatalf("Decode failed: %v", err)
		}

		if decoded[0].(bool) != v {
			t.Errorf("Expected %v, got %v", v, decoded[0])
		}
	}
}

func TestBoolEncoder_SingleNull(t *testing.T) {
	encoder := NewBoolEncoder()

	encoded, err := encoder.Encode([]interface{}{nil})
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := encoder.Decode(encoded, 1)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded[0] != nil {
		t.Errorf("Expected nil, got %v", decoded[0])
	}
}

func TestBoolEncoder_AllNulls(t *testing.T) {
	encoder := NewBoolEncoder()

	values := []interface{}{nil, nil, nil}
	encoded, err := encoder.Encode(values)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := encoder.Decode(encoded, 3)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	for i, d := range decoded {
		if d != nil {
			t.Errorf("Value %d: expected nil, got %v", i, d)
		}
	}
}

func TestBoolEncoder_NineValues(t *testing.T) {
	encoder := NewBoolEncoder()

	values := []interface{}{true, false, true, false, true, false, true, false, nil}
	encoded, err := encoder.Encode(values)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := encoder.Decode(encoded, 9)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	for i := 0; i < 8; i++ {
		expected := i%2 == 0
		if decoded[i].(bool) != expected {
			t.Errorf("Value %d: expected %v, got %v", i, expected, decoded[i])
		}
	}
	if decoded[8] != nil {
		t.Errorf("Value 8: expected nil, got %v", decoded[8])
	}
}

func TestBoolEncoder_NonBoolValues(t *testing.T) {
	encoder := NewBoolEncoder()

	values := []interface{}{true, "not_bool", 42, false}
	encoded, err := encoder.Encode(values)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := encoder.Decode(encoded, 4)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded[0].(bool) != true {
		t.Error("Index 0 should be true")
	}
	if decoded[1].(bool) != false {
		t.Error("Index 1 (string) should decode as false")
	}
	if decoded[2].(bool) != false {
		t.Error("Index 2 (int) should decode as false")
	}
	if decoded[3].(bool) != false {
		t.Error("Index 3 should be false")
	}
}

// =============================================================================
// Bool Encoder — Decode Error Paths
// =============================================================================

func TestBoolEncoder_Decode_TooShortForCount(t *testing.T) {
	encoder := NewBoolEncoder()
	_, err := encoder.Decode([]byte{0x01}, 5)
	if err == nil {
		t.Error("Expected error for data too short for count")
	}
}

func TestBoolEncoder_Decode_CountMismatch(t *testing.T) {
	encoder := NewBoolEncoder()
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, 99)
	_, err := encoder.Decode(data, 5)
	if err == nil {
		t.Error("Expected error for count mismatch")
	}
}

func TestBoolEncoder_Decode_TooShortForMasks(t *testing.T) {
	encoder := NewBoolEncoder()
	data := make([]byte, 5)
	binary.LittleEndian.PutUint32(data, 8)
	_, err := encoder.Decode(data, 8)
	if err == nil {
		t.Error("Expected error for data too short for masks")
	}
}

func TestBoolEncoder_Decode_EmptyAndZero(t *testing.T) {
	encoder := NewBoolEncoder()

	decoded, err := encoder.Decode(nil, 0)
	if err != nil || decoded != nil {
		t.Errorf("Expected (nil, nil) for nil data")
	}

	decoded, err = encoder.Decode([]byte{}, 0)
	if err != nil || decoded != nil {
		t.Errorf("Expected (nil, nil) for empty data")
	}
}

// =============================================================================
// Benchmarks
// =============================================================================

func BenchmarkBoolEncoder_Encode(b *testing.B) {
	encoder := NewBoolEncoder()
	values := make([]interface{}, 1000)
	for i := range values {
		values[i] = i%2 == 0
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = encoder.Encode(values)
	}
}

func BenchmarkBoolEncoder_Decode(b *testing.B) {
	encoder := NewBoolEncoder()
	values := make([]interface{}, 1000)
	for i := range values {
		values[i] = i%2 == 0
	}
	encoded, _ := encoder.Encode(values)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = encoder.Decode(encoded, len(values))
	}
}
