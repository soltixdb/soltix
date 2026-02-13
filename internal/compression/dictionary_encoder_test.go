package compression

import (
	"encoding/binary"
	"testing"
)

// =============================================================================
// Dictionary Encoder — Basic Tests
// =============================================================================

func TestDictionaryEncoder_Type(t *testing.T) {
	encoder := NewDictionaryEncoder()
	if encoder.Type() != ColumnTypeString {
		t.Errorf("Expected ColumnTypeString, got %d", encoder.Type())
	}
}

func TestDictionaryEncoder_EmptyValues(t *testing.T) {
	encoder := NewDictionaryEncoder()

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

func TestDictionaryEncoder_StringValues(t *testing.T) {
	encoder := NewDictionaryEncoder()

	values := []interface{}{"apple", "banana", "cherry", "apple", "banana"}

	encoded, err := encoder.Encode(values)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := encoder.Decode(encoded, len(values))
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	for i, v := range values {
		expected := v.(string)
		actual := decoded[i].(string)
		if expected != actual {
			t.Errorf("Value %d mismatch: expected %s, got %s", i, expected, actual)
		}
	}
}

func TestDictionaryEncoder_WithNulls(t *testing.T) {
	encoder := NewDictionaryEncoder()

	values := []interface{}{"hello", nil, "world", nil, "hello"}

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
			expected := v.(string)
			actual := decoded[i].(string)
			if expected != actual {
				t.Errorf("Value %d mismatch: expected %s, got %s", i, expected, actual)
			}
		}
	}
}

func TestDictionaryEncoder_UniqueStrings(t *testing.T) {
	encoder := NewDictionaryEncoder()

	values := []interface{}{"a", "b", "c", "d", "e"}

	encoded, err := encoder.Encode(values)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := encoder.Decode(encoded, len(values))
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	for i, v := range values {
		expected := v.(string)
		actual := decoded[i].(string)
		if expected != actual {
			t.Errorf("Value %d mismatch: expected %s, got %s", i, expected, actual)
		}
	}
}

func TestDictionaryEncoder_RepeatedStrings(t *testing.T) {
	encoder := NewDictionaryEncoder()

	values := make([]interface{}, 100)
	for i := range values {
		values[i] = "repeated_value"
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
		expected := v.(string)
		actual := decoded[i].(string)
		if expected != actual {
			t.Errorf("Value %d mismatch: expected %s, got %s", i, expected, actual)
		}
	}
}

func TestDictionaryEncoder_EmptyStrings(t *testing.T) {
	encoder := NewDictionaryEncoder()

	values := []interface{}{"", "a", "", "b", ""}

	encoded, err := encoder.Encode(values)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := encoder.Decode(encoded, len(values))
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	for i, v := range values {
		expected := v.(string)
		actual := decoded[i].(string)
		if expected != actual {
			t.Errorf("Value %d mismatch: expected '%s', got '%s'", i, expected, actual)
		}
	}
}

func TestDictionaryEncoder_UnicodeStrings(t *testing.T) {
	encoder := NewDictionaryEncoder()

	values := []interface{}{"Hello", "世界", "مرحبا", "Привет", "🚀🎉"}

	encoded, err := encoder.Encode(values)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := encoder.Decode(encoded, len(values))
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	for i, v := range values {
		expected := v.(string)
		actual := decoded[i].(string)
		if expected != actual {
			t.Errorf("Value %d mismatch: expected %s, got %s", i, expected, actual)
		}
	}
}

// =============================================================================
// Dictionary Encoder — Edge Cases
// =============================================================================

func TestDictionaryEncoder_SingleValue(t *testing.T) {
	encoder := NewDictionaryEncoder()

	values := []interface{}{"hello"}
	encoded, err := encoder.Encode(values)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := encoder.Decode(encoded, 1)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded[0].(string) != "hello" {
		t.Errorf("Expected 'hello', got %v", decoded[0])
	}
}

func TestDictionaryEncoder_SingleNull(t *testing.T) {
	encoder := NewDictionaryEncoder()

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

func TestDictionaryEncoder_AllNulls(t *testing.T) {
	encoder := NewDictionaryEncoder()

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

func TestDictionaryEncoder_NonStringTypes(t *testing.T) {
	encoder := NewDictionaryEncoder()

	values := []interface{}{42, 3.14, true}
	encoded, err := encoder.Encode(values)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := encoder.Decode(encoded, 3)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded[0].(string) != "42" {
		t.Errorf("Expected '42', got %v", decoded[0])
	}
	if decoded[1].(string) != "3.14" {
		t.Errorf("Expected '3.14', got %v", decoded[1])
	}
	if decoded[2].(string) != "true" {
		t.Errorf("Expected 'true', got %v", decoded[2])
	}
}

func TestDictionaryEncoder_NonStringTypesExtended(t *testing.T) {
	encoder := NewDictionaryEncoder()

	// Test all toStringFast branches: int, int64, int32, float64, float32, bool, fallback
	values := []interface{}{
		int(42),
		int64(123456789),
		int32(-99),
		float64(3.14),
		float32(2.5),
		true,
		struct{ X int }{X: 1}, // fallback to fmt.Sprintf
	}
	encoded, err := encoder.Encode(values)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := encoder.Decode(encoded, len(values))
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	expected := []string{"42", "123456789", "-99", "3.14", "2.5", "true", "{1}"}
	for i, exp := range expected {
		if decoded[i].(string) != exp {
			t.Errorf("Value %d: expected %q, got %v", i, exp, decoded[i])
		}
	}
}

func TestDictionaryEncoder_NineValues(t *testing.T) {
	encoder := NewDictionaryEncoder()

	values := []interface{}{"a", "b", "c", "d", "e", "f", "g", "h", nil}
	encoded, err := encoder.Encode(values)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := encoder.Decode(encoded, 9)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	for i := 0; i < 8; i++ {
		expected := string(rune('a' + i))
		if decoded[i].(string) != expected {
			t.Errorf("Value %d: expected '%s', got %v", i, expected, decoded[i])
		}
	}
	if decoded[8] != nil {
		t.Errorf("Value 8: expected nil, got %v", decoded[8])
	}
}

func TestDictionaryEncoder_LargeDict(t *testing.T) {
	encoder := NewDictionaryEncoder()

	values := make([]interface{}, 1000)
	for i := range values {
		values[i] = string(rune('A'+i%26)) + string(rune('0'+i%10)) + string(rune('a'+i%26))
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
		if decoded[i].(string) != v.(string) {
			t.Errorf("Value %d: expected %s, got %v", i, v, decoded[i])
		}
	}
}

// =============================================================================
// Dictionary Encoder — DecodeStrings Tests
// =============================================================================

func TestDictionaryEncoder_DecodeStrings_Roundtrip(t *testing.T) {
	encoder := NewDictionaryEncoder()

	values := []interface{}{"a", nil, "b", "a", nil}
	encoded, err := encoder.Encode(values)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	strs, nulls, err := encoder.DecodeStrings(encoded, len(values))
	if err != nil {
		t.Fatalf("DecodeStrings failed: %v", err)
	}

	if strs[0] != "a" || nulls[0] {
		t.Errorf("Index 0 mismatch")
	}
	if !nulls[1] {
		t.Error("Index 1 should be null")
	}
	if strs[2] != "b" || nulls[2] {
		t.Errorf("Index 2 mismatch")
	}
	if strs[3] != "a" || nulls[3] {
		t.Errorf("Index 3 mismatch")
	}
	if !nulls[4] {
		t.Error("Index 4 should be null")
	}
}

func TestDictionaryEncoder_DecodeStrings_EmptyAndZero(t *testing.T) {
	encoder := NewDictionaryEncoder()

	f, n, err := encoder.DecodeStrings(nil, 0)
	if err != nil || f != nil || n != nil {
		t.Errorf("Expected (nil, nil, nil) for empty")
	}

	f, n, err = encoder.DecodeStrings([]byte{}, 0)
	if err != nil || f != nil || n != nil {
		t.Errorf("Expected (nil, nil, nil) for empty bytes")
	}
}

func TestDictionaryEncoder_DecodeStrings_TooShortForCount(t *testing.T) {
	encoder := NewDictionaryEncoder()
	_, _, err := encoder.DecodeStrings([]byte{0x01}, 5)
	if err == nil {
		t.Error("Expected error for data too short for count")
	}
}

func TestDictionaryEncoder_DecodeStrings_CountMismatch(t *testing.T) {
	encoder := NewDictionaryEncoder()
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, 99)
	_, _, err := encoder.DecodeStrings(data, 5)
	if err == nil {
		t.Error("Expected error for count mismatch")
	}
}

func TestDictionaryEncoder_DecodeStrings_TooShortForNullMask(t *testing.T) {
	encoder := NewDictionaryEncoder()
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, 100)
	_, _, err := encoder.DecodeStrings(data, 100)
	if err == nil {
		t.Error("Expected error for too short for null mask")
	}
}

func TestDictionaryEncoder_DecodeStrings_TooShortForDictSize(t *testing.T) {
	encoder := NewDictionaryEncoder()
	data := make([]byte, 5)
	binary.LittleEndian.PutUint32(data, 1)
	_, _, err := encoder.DecodeStrings(data, 1)
	if err == nil {
		t.Error("Expected error for too short for dict size")
	}
}

func TestDictionaryEncoder_DecodeStrings_TooShortForDictEntryLen(t *testing.T) {
	encoder := NewDictionaryEncoder()
	data := make([]byte, 9)
	binary.LittleEndian.PutUint32(data, 1)
	data[4] = 0
	binary.LittleEndian.PutUint32(data[5:], 1)

	_, _, err := encoder.DecodeStrings(data, 1)
	if err == nil {
		t.Error("Expected error for too short for dict entry length")
	}
}

func TestDictionaryEncoder_DecodeStrings_TooShortForDictEntry(t *testing.T) {
	encoder := NewDictionaryEncoder()
	data := make([]byte, 13)
	binary.LittleEndian.PutUint32(data, 1)
	data[4] = 0
	binary.LittleEndian.PutUint32(data[5:], 1)
	binary.LittleEndian.PutUint32(data[9:], 100)

	_, _, err := encoder.DecodeStrings(data, 1)
	if err == nil {
		t.Error("Expected error for too short for dict entry")
	}
}

func TestDictionaryEncoder_DecodeStrings_TruncatedIndex(t *testing.T) {
	encoder := NewDictionaryEncoder()

	values := []interface{}{"a", "b", "c", "d", "e"}
	encoded, _ := encoder.Encode(values)

	truncated := encoded[:len(encoded)-2]
	_, _, err := encoder.DecodeStrings(truncated, 5)
	if err == nil {
		t.Error("Expected error for truncated indices")
	}
}

func TestDictionaryEncoder_DecodeStrings_IndexOutOfRange(t *testing.T) {
	encoder := NewDictionaryEncoder()

	data := make([]byte, 0, 50)

	countBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(countBuf, 1)
	data = append(data, countBuf...)

	data = append(data, 0x00)

	binary.LittleEndian.PutUint32(countBuf, 1)
	data = append(data, countBuf...)

	binary.LittleEndian.PutUint32(countBuf, 2)
	data = append(data, countBuf...)
	data = append(data, 'h', 'i')

	data = AppendVarint(data, 5)

	_, _, err := encoder.DecodeStrings(data, 1)
	if err == nil {
		t.Error("Expected error for dictionary index out of range")
	}
}

// =============================================================================
// Dictionary Encoder — Decode Wrapper
// =============================================================================

func TestDictionaryEncoder_Decode_ReturnsNilForNilData(t *testing.T) {
	encoder := NewDictionaryEncoder()
	decoded, err := encoder.Decode(nil, 0)
	if err != nil || decoded != nil {
		t.Errorf("Expected (nil, nil), got (%v, %v)", decoded, err)
	}
}

func TestDictionaryEncoder_Decode_WithValidData(t *testing.T) {
	encoder := NewDictionaryEncoder()
	values := []interface{}{"hello", "world", nil, "hello"}
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
	if decoded[0].(string) != "hello" || decoded[1].(string) != "world" || decoded[3].(string) != "hello" {
		t.Error("Mismatch in non-null values")
	}
	if decoded[2] != nil {
		t.Error("Expected nil at index 2")
	}
}

func TestDictionaryEncoder_Decode_PropagatesError(t *testing.T) {
	encoder := NewDictionaryEncoder()
	_, err := encoder.Decode([]byte{0x01, 0x00, 0x00, 0x00}, 5)
	if err == nil {
		t.Error("Expected error from count mismatch")
	}
}

// =============================================================================
// Benchmarks
// =============================================================================

func BenchmarkDictionaryEncoder_Encode(b *testing.B) {
	encoder := NewDictionaryEncoder()
	strings := []string{"apple", "banana", "cherry", "date", "elderberry"}
	values := make([]interface{}, 1000)
	for i := range values {
		values[i] = strings[i%len(strings)]
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = encoder.Encode(values)
	}
}

func BenchmarkDictionaryEncoder_Decode(b *testing.B) {
	encoder := NewDictionaryEncoder()
	strings := []string{"apple", "banana", "cherry", "date", "elderberry"}
	values := make([]interface{}, 1000)
	for i := range values {
		values[i] = strings[i%len(strings)]
	}
	encoded, _ := encoder.Encode(values)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = encoder.Decode(encoded, len(values))
	}
}

func BenchmarkDictionaryEncoder_DecodeStrings(b *testing.B) {
	encoder := NewDictionaryEncoder()
	strings := []string{"apple", "banana", "cherry", "date", "elderberry"}
	values := make([]interface{}, 1000)
	for i := range values {
		values[i] = strings[i%len(strings)]
	}
	encoded, _ := encoder.Encode(values)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = encoder.DecodeStrings(encoded, len(values))
	}
}
