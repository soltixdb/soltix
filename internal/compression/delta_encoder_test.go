package compression

import (
	"encoding/binary"
	"math"
	"testing"
)

// =============================================================================
// Delta Encoder — Basic Tests
// =============================================================================

func TestDeltaEncoder_Type(t *testing.T) {
	encoder := NewDeltaEncoder()
	if encoder.Type() != ColumnTypeInt64 {
		t.Errorf("Expected ColumnTypeInt64, got %d", encoder.Type())
	}
}

func TestDeltaEncoder_EmptyValues(t *testing.T) {
	encoder := NewDeltaEncoder()

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

func TestDeltaEncoder_Int64Values(t *testing.T) {
	encoder := NewDeltaEncoder()

	values := []interface{}{int64(100), int64(105), int64(110), int64(115), int64(120)}

	encoded, err := encoder.Encode(values)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := encoder.Decode(encoded, len(values))
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	for i, v := range values {
		expected := v.(int64)
		actual := decoded[i].(int64)
		if expected != actual {
			t.Errorf("Value %d mismatch: expected %d, got %d", i, expected, actual)
		}
	}
}

func TestDeltaEncoder_WithNulls(t *testing.T) {
	encoder := NewDeltaEncoder()

	values := []interface{}{int64(100), nil, int64(200), nil, int64(300)}

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
			expected := v.(int64)
			actual := decoded[i].(int64)
			if expected != actual {
				t.Errorf("Value %d mismatch: expected %d, got %d", i, expected, actual)
			}
		}
	}
}

func TestDeltaEncoder_NegativeValues(t *testing.T) {
	encoder := NewDeltaEncoder()

	values := []interface{}{int64(-100), int64(-50), int64(0), int64(50), int64(100)}

	encoded, err := encoder.Encode(values)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := encoder.Decode(encoded, len(values))
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	for i, v := range values {
		expected := v.(int64)
		actual := decoded[i].(int64)
		if expected != actual {
			t.Errorf("Value %d mismatch: expected %d, got %d", i, expected, actual)
		}
	}
}

func TestDeltaEncoder_LargeDeltas(t *testing.T) {
	encoder := NewDeltaEncoder()

	values := []interface{}{
		int64(0),
		int64(1000000),
		int64(-1000000),
		int64(math.MaxInt64 / 2),
		int64(math.MinInt64 / 2),
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
		expected := v.(int64)
		actual := decoded[i].(int64)
		if expected != actual {
			t.Errorf("Value %d mismatch: expected %d, got %d", i, expected, actual)
		}
	}
}

func TestDeltaEncoder_MixedIntTypes(t *testing.T) {
	encoder := NewDeltaEncoder()

	values := []interface{}{
		int64(100),
		int(200),
		int32(300),
		float64(400),
	}

	encoded, err := encoder.Encode(values)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := encoder.Decode(encoded, len(values))
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	expected := []int64{100, 200, 300, 400}
	for i, exp := range expected {
		actual := decoded[i].(int64)
		if exp != actual {
			t.Errorf("Value %d mismatch: expected %d, got %d", i, exp, actual)
		}
	}
}

// =============================================================================
// Delta Encoder — Edge Cases
// =============================================================================

func TestDeltaEncoder_SingleValue(t *testing.T) {
	encoder := NewDeltaEncoder()

	values := []interface{}{int64(42)}
	encoded, err := encoder.Encode(values)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := encoder.Decode(encoded, 1)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded[0].(int64) != 42 {
		t.Errorf("Expected 42, got %v", decoded[0])
	}
}

func TestDeltaEncoder_SingleNullValue(t *testing.T) {
	encoder := NewDeltaEncoder()

	values := []interface{}{nil}
	encoded, err := encoder.Encode(values)
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

func TestDeltaEncoder_AllNulls(t *testing.T) {
	encoder := NewDeltaEncoder()

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

func TestDeltaEncoder_FirstValueNull(t *testing.T) {
	encoder := NewDeltaEncoder()

	values := []interface{}{nil, int64(100), int64(200)}
	encoded, err := encoder.Encode(values)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := encoder.Decode(encoded, 3)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded[0] != nil {
		t.Errorf("Value 0: expected nil, got %v", decoded[0])
	}
	if decoded[1].(int64) != 100 {
		t.Errorf("Value 1: expected 100, got %v", decoded[1])
	}
	if decoded[2].(int64) != 200 {
		t.Errorf("Value 2: expected 200, got %v", decoded[2])
	}
}

func TestDeltaEncoder_UnsupportedType(t *testing.T) {
	encoder := NewDeltaEncoder()

	// Truly unsupported types should still error
	values := []interface{}{complex(1, 2)}
	_, err := encoder.Encode(values)
	if err == nil {
		t.Error("Expected error for unsupported type complex128")
	}

	// String values should be treated as null (not error) for robustness
	strValues := []interface{}{"not_an_int"}
	data, err := encoder.Encode(strValues)
	if err != nil {
		t.Errorf("Expected string to be treated as null, got error: %v", err)
	}
	if data == nil {
		t.Error("Expected non-nil encoded data")
	}

	// Bool values should be coerced (true=1, false=0)
	boolValues := []interface{}{true, false}
	data, err = encoder.Encode(boolValues)
	if err != nil {
		t.Errorf("Expected bool to be coerced to int, got error: %v", err)
	}
	if data == nil {
		t.Error("Expected non-nil encoded data")
	}
}

func TestDeltaEncoder_BoundaryValues(t *testing.T) {
	encoder := NewDeltaEncoder()

	values := []interface{}{
		int64(math.MaxInt64),
		int64(math.MinInt64),
		int64(0),
		int64(1),
		int64(-1),
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
		expected := v.(int64)
		actual := decoded[i].(int64)
		if expected != actual {
			t.Errorf("Value %d: expected %d, got %d", i, expected, actual)
		}
	}
}

func TestDeltaEncoder_MonotonicallyDecreasing(t *testing.T) {
	encoder := NewDeltaEncoder()

	values := make([]interface{}, 100)
	for i := range values {
		values[i] = int64(1000 - i*10)
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
		if decoded[i].(int64) != v.(int64) {
			t.Errorf("Value %d: expected %d, got %v", i, v, decoded[i])
		}
	}
}

func TestDeltaEncoder_NineValues(t *testing.T) {
	encoder := NewDeltaEncoder()

	values := []interface{}{
		int64(1), int64(2), int64(3), int64(4), int64(5),
		int64(6), int64(7), int64(8), nil,
	}
	encoded, err := encoder.Encode(values)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := encoder.Decode(encoded, 9)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	for i := 0; i < 8; i++ {
		if decoded[i].(int64) != int64(i+1) {
			t.Errorf("Value %d mismatch", i)
		}
	}
	if decoded[8] != nil {
		t.Errorf("Value 8: expected nil, got %v", decoded[8])
	}
}

func TestDeltaEncoder_ZeroDelta(t *testing.T) {
	encoder := NewDeltaEncoder()

	values := make([]interface{}, 50)
	for i := range values {
		values[i] = int64(42)
	}

	encoded, err := encoder.Encode(values)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := encoder.Decode(encoded, len(values))
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	for i, d := range decoded {
		if d.(int64) != 42 {
			t.Errorf("Value %d: expected 42, got %v", i, d)
		}
	}
}

// =============================================================================
// Delta Encoder — DecodeInt64 Tests
// =============================================================================

func TestDeltaEncoder_DecodeInt64_Roundtrip(t *testing.T) {
	encoder := NewDeltaEncoder()

	values := []interface{}{int64(10), nil, int64(30), int64(40)}
	encoded, err := encoder.Encode(values)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	ints, nulls, err := encoder.DecodeInt64(encoded, len(values))
	if err != nil {
		t.Fatalf("DecodeInt64 failed: %v", err)
	}

	if ints[0] != 10 || nulls[0] {
		t.Errorf("Index 0: expected 10 non-null")
	}
	if !nulls[1] {
		t.Error("Index 1 should be null")
	}
	if ints[2] != 30 || nulls[2] {
		t.Errorf("Index 2: expected 30 non-null")
	}
	if ints[3] != 40 || nulls[3] {
		t.Errorf("Index 3: expected 40 non-null")
	}
}

func TestDeltaEncoder_DecodeInt64_EmptyAndZero(t *testing.T) {
	encoder := NewDeltaEncoder()

	f, n, err := encoder.DecodeInt64(nil, 0)
	if err != nil || f != nil || n != nil {
		t.Errorf("Expected (nil, nil, nil) for empty, got (%v, %v, %v)", f, n, err)
	}

	f, n, err = encoder.DecodeInt64([]byte{}, 0)
	if err != nil || f != nil || n != nil {
		t.Errorf("Expected (nil, nil, nil) for empty bytes, got (%v, %v, %v)", f, n, err)
	}
}

func TestDeltaEncoder_DecodeInt64_TooShortForCount(t *testing.T) {
	encoder := NewDeltaEncoder()
	_, _, err := encoder.DecodeInt64([]byte{0x01, 0x02}, 5)
	if err == nil {
		t.Error("Expected error for data too short for count")
	}
}

func TestDeltaEncoder_DecodeInt64_CountMismatch(t *testing.T) {
	encoder := NewDeltaEncoder()
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, 99)
	_, _, err := encoder.DecodeInt64(data, 5)
	if err == nil {
		t.Error("Expected error for count mismatch")
	}
}

func TestDeltaEncoder_DecodeInt64_TooShortForNullMask(t *testing.T) {
	encoder := NewDeltaEncoder()
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, 100)
	_, _, err := encoder.DecodeInt64(data, 100)
	if err == nil {
		t.Error("Expected error for too short for null mask")
	}
}

func TestDeltaEncoder_DecodeInt64_TooShortForFirstValue(t *testing.T) {
	encoder := NewDeltaEncoder()
	data := make([]byte, 5)
	binary.LittleEndian.PutUint32(data, 1)
	_, _, err := encoder.DecodeInt64(data, 1)
	if err == nil {
		t.Error("Expected error for too short for first value")
	}
}

func TestDeltaEncoder_DecodeInt64_TruncatedVarint(t *testing.T) {
	encoder := NewDeltaEncoder()

	// Build valid header: count(4) + nullMask(1) + firstValue(8) = 13 bytes
	data := make([]byte, 0, 20)
	countBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(countBuf, 3) // count=3, need 2 varints
	data = append(data, countBuf...)
	data = append(data, 0x00) // null mask (1 byte for 3 values)
	firstBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(firstBuf, 100)
	data = append(data, firstBuf...)

	// First varint (valid): delta=10 → zigzag=20 → single byte 0x14
	data = AppendVarint(data, 20)

	// Second varint (truncated): high bit set but no continuation
	data = append(data, 0x80) // more-bytes flag, but data ends here

	_, _, err := encoder.DecodeInt64(data, 3)
	if err == nil {
		t.Error("Expected error for truncated varint")
	}
}

// =============================================================================
// Delta Encoder — Decode Wrapper
// =============================================================================

func TestDeltaEncoder_Decode_ReturnsNilForNilData(t *testing.T) {
	encoder := NewDeltaEncoder()
	decoded, err := encoder.Decode(nil, 0)
	if err != nil || decoded != nil {
		t.Errorf("Expected (nil, nil), got (%v, %v)", decoded, err)
	}
}

func TestDeltaEncoder_Decode_WithValidData(t *testing.T) {
	encoder := NewDeltaEncoder()
	values := []interface{}{int64(10), int64(20), nil, int64(40)}
	encoded, err := encoder.Encode(values)
	if err != nil {
		t.Fatalf("Encode error: %v", err)
	}

	decoded, err := encoder.Decode(encoded, len(values))
	if err != nil {
		t.Fatalf("Decode error: %v", err)
	}

	if len(decoded) != 4 {
		t.Fatalf("Expected 4 values, got %d", len(decoded))
	}
	if decoded[0].(int64) != 10 || decoded[1].(int64) != 20 || decoded[3].(int64) != 40 {
		t.Error("Mismatch in non-null values")
	}
	if decoded[2] != nil {
		t.Error("Expected nil at index 2")
	}
}

func TestDeltaEncoder_Decode_PropagatesError(t *testing.T) {
	encoder := NewDeltaEncoder()
	_, err := encoder.Decode([]byte{0x01, 0x00, 0x00, 0x00}, 5)
	if err == nil {
		t.Error("Expected error from count mismatch")
	}
}

// =============================================================================
// Benchmarks
// =============================================================================

func BenchmarkDeltaEncoder_Encode(b *testing.B) {
	encoder := NewDeltaEncoder()
	values := make([]interface{}, 1000)
	for i := range values {
		values[i] = int64(i * 100)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = encoder.Encode(values)
	}
}

func BenchmarkDeltaEncoder_Decode(b *testing.B) {
	encoder := NewDeltaEncoder()
	values := make([]interface{}, 1000)
	for i := range values {
		values[i] = int64(i * 100)
	}
	encoded, _ := encoder.Encode(values)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = encoder.Decode(encoded, len(values))
	}
}

func BenchmarkDeltaEncoder_DecodeInt64(b *testing.B) {
	encoder := NewDeltaEncoder()
	values := make([]interface{}, 1000)
	for i := range values {
		values[i] = int64(i * 100)
	}
	encoded, _ := encoder.Encode(values)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = encoder.DecodeInt64(encoded, len(values))
	}
}
