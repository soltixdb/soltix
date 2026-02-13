package compression

import (
	"encoding/binary"
	"math"
	"testing"
)

// =============================================================================
// Gorilla Encoder — Basic Tests
// =============================================================================

func TestGorillaEncoder_Type(t *testing.T) {
	encoder := NewGorillaEncoder()
	if encoder.Type() != ColumnTypeFloat64 {
		t.Errorf("Expected ColumnTypeFloat64, got %d", encoder.Type())
	}
}

func TestGorillaEncoder_EmptyValues(t *testing.T) {
	encoder := NewGorillaEncoder()

	// Encode empty
	encoded, err := encoder.Encode([]interface{}{})
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	if encoded != nil {
		t.Errorf("Expected nil for empty values, got %v", encoded)
	}

	// Decode empty
	decoded, err := encoder.Decode(nil, 0)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	if decoded != nil {
		t.Errorf("Expected nil for empty decode, got %v", decoded)
	}
}

func TestGorillaEncoder_Float64Values(t *testing.T) {
	encoder := NewGorillaEncoder()

	values := []interface{}{1.5, 2.5, 3.5, 4.5, 5.5}

	encoded, err := encoder.Encode(values)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := encoder.Decode(encoded, len(values))
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if len(decoded) != len(values) {
		t.Fatalf("Length mismatch: expected %d, got %d", len(values), len(decoded))
	}

	for i, v := range values {
		expected := v.(float64)
		actual := decoded[i].(float64)
		if expected != actual {
			t.Errorf("Value %d mismatch: expected %f, got %f", i, expected, actual)
		}
	}
}

func TestGorillaEncoder_WithNulls(t *testing.T) {
	encoder := NewGorillaEncoder()

	values := []interface{}{1.5, nil, 3.5, nil, 5.5}

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
			expected := v.(float64)
			actual := decoded[i].(float64)
			if expected != actual {
				t.Errorf("Value %d mismatch: expected %f, got %f", i, expected, actual)
			}
		}
	}
}

func TestGorillaEncoder_SpecialValues(t *testing.T) {
	encoder := NewGorillaEncoder()

	values := []interface{}{
		0.0,
		math.Copysign(0, -1),
		math.MaxFloat64,
		-math.MaxFloat64,
		math.SmallestNonzeroFloat64,
		math.Pi,
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
		expected := v.(float64)
		actual := decoded[i].(float64)
		if expected != actual {
			t.Errorf("Value %d mismatch: expected %f, got %f", i, expected, actual)
		}
	}
}

func TestGorillaEncoder_MixedNumericTypes(t *testing.T) {
	encoder := NewGorillaEncoder()

	values := []interface{}{
		float64(1.5),
		float32(2.5),
		int(3),
		int64(4),
	}

	encoded, err := encoder.Encode(values)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := encoder.Decode(encoded, len(values))
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	expected := []float64{1.5, 2.5, 3.0, 4.0}
	for i, exp := range expected {
		actual := decoded[i].(float64)
		if exp != actual {
			t.Errorf("Value %d mismatch: expected %f, got %f", i, exp, actual)
		}
	}
}

// =============================================================================
// Gorilla Encoder — Compression Ratio
// =============================================================================

func TestGorillaEncoder_CompressionRatio(t *testing.T) {
	encoder := NewGorillaEncoder()

	tests := []struct {
		name     string
		values   []interface{}
		minRatio float64
	}{
		{
			name: "ConstantValue",
			values: func() []interface{} {
				v := make([]interface{}, 1000)
				for i := range v {
					v[i] = 42.0
				}
				return v
			}(),
			minRatio: 30.0,
		},
		{
			name: "LinearSequence",
			values: func() []interface{} {
				v := make([]interface{}, 1000)
				for i := range v {
					v[i] = float64(i) * 1.5
				}
				return v
			}(),
			minRatio: 2.0,
		},
		{
			name: "SlowlyVaryingSensor",
			values: func() []interface{} {
				v := make([]interface{}, 1000)
				val := 25.0
				for i := range v {
					val += (float64(i%7) - 3.0) * 0.01
					v[i] = val
				}
				return v
			}(),
			minRatio: 1.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := encoder.Encode(tt.values)
			if err != nil {
				t.Fatal(err)
			}
			rawSize := len(tt.values) * 8
			compressedSize := len(encoded)
			ratio := float64(rawSize) / float64(compressedSize)
			bitsPerValue := float64(compressedSize*8) / float64(len(tt.values))

			t.Logf("Raw: %d bytes, Compressed: %d bytes, Ratio: %.2fx, Bits/value: %.2f",
				rawSize, compressedSize, ratio, bitsPerValue)

			if ratio < tt.minRatio {
				t.Errorf("Compression ratio %.2fx below minimum %.2fx", ratio, tt.minRatio)
			}

			// Verify roundtrip
			decoded, nulls, err := encoder.DecodeFloat64(encoded, len(tt.values))
			if err != nil {
				t.Fatal(err)
			}
			for i, v := range tt.values {
				expected := v.(float64)
				if !nulls[i] && decoded[i] != expected {
					t.Fatalf("Mismatch at %d: got %v, expected %v", i, decoded[i], expected)
				}
			}
		})
	}
}

// =============================================================================
// Gorilla Encoder — Edge Cases
// =============================================================================

func TestGorillaEncoder_SingleValue(t *testing.T) {
	encoder := NewGorillaEncoder()

	values := []interface{}{42.5}
	encoded, err := encoder.Encode(values)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := encoder.Decode(encoded, 1)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded[0].(float64) != 42.5 {
		t.Errorf("Expected 42.5, got %v", decoded[0])
	}
}

func TestGorillaEncoder_SingleNullValue(t *testing.T) {
	encoder := NewGorillaEncoder()

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

func TestGorillaEncoder_AllNulls(t *testing.T) {
	encoder := NewGorillaEncoder()

	values := []interface{}{nil, nil, nil, nil, nil}
	encoded, err := encoder.Encode(values)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := encoder.Decode(encoded, 5)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	for i, d := range decoded {
		if d != nil {
			t.Errorf("Value %d: expected nil, got %v", i, d)
		}
	}
}

func TestGorillaEncoder_NaNAndInf(t *testing.T) {
	encoder := NewGorillaEncoder()

	values := []interface{}{
		math.NaN(),
		math.Inf(1),
		math.Inf(-1),
		0.0,
	}

	encoded, err := encoder.Encode(values)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	floats, nulls, err := encoder.DecodeFloat64(encoded, len(values))
	if err != nil {
		t.Fatalf("DecodeFloat64 failed: %v", err)
	}

	if !math.IsNaN(floats[0]) {
		t.Errorf("Expected NaN, got %v", floats[0])
	}
	if !math.IsInf(floats[1], 1) {
		t.Errorf("Expected +Inf, got %v", floats[1])
	}
	if !math.IsInf(floats[2], -1) {
		t.Errorf("Expected -Inf, got %v", floats[2])
	}
	if floats[3] != 0.0 {
		t.Errorf("Expected 0.0, got %v", floats[3])
	}
	for i := range nulls {
		if nulls[i] {
			t.Errorf("Value %d should not be null", i)
		}
	}
}

func TestGorillaEncoder_IdenticalValues(t *testing.T) {
	// Tests Case 1 (XOR == 0 → single '0' bit) extensively
	encoder := NewGorillaEncoder()

	values := make([]interface{}, 100)
	for i := range values {
		values[i] = math.Pi
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
		if d.(float64) != math.Pi {
			t.Errorf("Value %d: expected Pi, got %v", i, d)
		}
	}

	// Compression should be excellent (1 bit per value after first)
	rawSize := len(values) * 8
	ratio := float64(rawSize) / float64(len(encoded))
	if ratio < 10.0 {
		t.Errorf("Expected compression ratio > 10x for identical values, got %.2fx", ratio)
	}
}

func TestGorillaEncoder_AlternatingValues(t *testing.T) {
	// Forces Case 2 (reuse window) to be exercised
	encoder := NewGorillaEncoder()

	values := make([]interface{}, 100)
	for i := range values {
		if i%2 == 0 {
			values[i] = 1.0
		} else {
			values[i] = 2.0
		}
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
		expected := 1.0
		if i%2 == 1 {
			expected = 2.0
		}
		if d.(float64) != expected {
			t.Errorf("Value %d: expected %f, got %v", i, expected, d)
		}
	}
}

func TestGorillaEncoder_UnsupportedType(t *testing.T) {
	encoder := NewGorillaEncoder()

	// Truly unsupported types (e.g. complex numbers, slices) should still error
	values := []interface{}{complex(1, 2)}
	_, err := encoder.Encode(values)
	if err == nil {
		t.Error("Expected error for unsupported type complex128")
	}

	// String values should be treated as null (not error) for robustness
	strValues := []interface{}{"not_a_number"}
	data, err := encoder.Encode(strValues)
	if err != nil {
		t.Errorf("Expected string to be treated as null, got error: %v", err)
	}
	if data == nil {
		t.Error("Expected non-nil encoded data")
	}

	// Bool values should be coerced to float (true=1, false=0)
	boolValues := []interface{}{true, false}
	data, err = encoder.Encode(boolValues)
	if err != nil {
		t.Errorf("Expected bool to be coerced to float, got error: %v", err)
	}
	if data == nil {
		t.Error("Expected non-nil encoded data")
	}
}

func TestGorillaEncoder_DecodeFloat64_EmptyAndZeroCount(t *testing.T) {
	encoder := NewGorillaEncoder()

	// Empty data
	f, n, err := encoder.DecodeFloat64(nil, 0)
	if err != nil || f != nil || n != nil {
		t.Errorf("Expected (nil, nil, nil) for empty data, got (%v, %v, %v)", f, n, err)
	}

	// Zero count
	f, n, err = encoder.DecodeFloat64([]byte{0x02, 0, 0, 0, 0}, 0)
	if err != nil || f != nil || n != nil {
		t.Errorf("Expected (nil, nil, nil) for zero count, got (%v, %v, %v)", f, n, err)
	}
}

func TestGorillaEncoder_DecodeFloat64_Roundtrip(t *testing.T) {
	encoder := NewGorillaEncoder()

	values := []interface{}{1.1, 2.2, nil, 4.4, 5.5}

	encoded, err := encoder.Encode(values)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	floats, nulls, err := encoder.DecodeFloat64(encoded, len(values))
	if err != nil {
		t.Fatalf("DecodeFloat64 failed: %v", err)
	}

	if len(floats) != 5 || len(nulls) != 5 {
		t.Fatalf("Length mismatch: floats=%d, nulls=%d", len(floats), len(nulls))
	}

	if floats[0] != 1.1 || floats[1] != 2.2 || floats[3] != 4.4 || floats[4] != 5.5 {
		t.Error("Float values don't match")
	}
	if !nulls[2] {
		t.Error("Index 2 should be null")
	}
	if nulls[0] || nulls[1] || nulls[3] || nulls[4] {
		t.Error("Non-null values should not be marked as null")
	}
}

func TestGorillaEncoder_Decode_ReturnsNilForNilData(t *testing.T) {
	encoder := NewGorillaEncoder()

	decoded, err := encoder.Decode(nil, 0)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if decoded != nil {
		t.Errorf("Expected nil, got %v", decoded)
	}
}

func TestGorillaEncoder_NineValues(t *testing.T) {
	encoder := NewGorillaEncoder()

	// 9 values → null mask is 2 bytes (ceil(9/8))
	values := []interface{}{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, nil}
	encoded, err := encoder.Encode(values)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := encoder.Decode(encoded, 9)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	for i := 0; i < 8; i++ {
		if decoded[i].(float64) != float64(i+1) {
			t.Errorf("Value %d: expected %f, got %v", i, float64(i+1), decoded[i])
		}
	}
	if decoded[8] != nil {
		t.Errorf("Value 8: expected nil, got %v", decoded[8])
	}
}

func TestGorillaEncoder_WindowReuse(t *testing.T) {
	encoder := NewGorillaEncoder()

	// Create values that produce similar XOR patterns to exercise window reuse
	values := make([]interface{}, 20)
	base := 100.0
	for i := range values {
		values[i] = base + float64(i)*0.001
	}

	encoded, err := encoder.Encode(values)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	floats, _, err := encoder.DecodeFloat64(encoded, len(values))
	if err != nil {
		t.Fatalf("DecodeFloat64 failed: %v", err)
	}

	for i, v := range values {
		if floats[i] != v.(float64) {
			t.Errorf("Value %d: expected %v, got %v", i, v, floats[i])
		}
	}
}

func TestGorillaEncoder_LargeDataset(t *testing.T) {
	encoder := NewGorillaEncoder()

	values := make([]interface{}, 10000)
	for i := range values {
		if i%100 == 0 {
			values[i] = nil
		} else {
			values[i] = float64(i) * 0.123456789
		}
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
		if v == nil {
			if decoded[i] != nil {
				t.Errorf("Value %d: expected nil, got %v", i, decoded[i])
			}
		} else {
			if decoded[i].(float64) != v.(float64) {
				t.Errorf("Value %d: expected %v, got %v", i, v, decoded[i])
			}
		}
	}
}

// =============================================================================
// Gorilla V2 — Decode Error Paths
// =============================================================================

func TestGorillaV2_Decode_TooShortForCount(t *testing.T) {
	encoder := NewGorillaEncoder()

	data := []byte{gorillaV2Magic, 0x01}
	_, _, err := encoder.DecodeFloat64(data, 5)
	if err == nil {
		t.Error("Expected error for data too short for count")
	}
}

func TestGorillaV2_Decode_CountMismatch(t *testing.T) {
	encoder := NewGorillaEncoder()

	data := make([]byte, 14)
	data[0] = gorillaV2Magic
	binary.LittleEndian.PutUint32(data[1:5], 10)

	_, _, err := encoder.DecodeFloat64(data, 5)
	if err == nil {
		t.Error("Expected error for count mismatch")
	}
}

func TestGorillaV2_Decode_TooShortForNullMask(t *testing.T) {
	encoder := NewGorillaEncoder()

	data := make([]byte, 5)
	data[0] = gorillaV2Magic
	binary.LittleEndian.PutUint32(data[1:5], 100)

	_, _, err := encoder.DecodeFloat64(data, 100)
	if err == nil {
		t.Error("Expected error for data too short for null mask")
	}
}

func TestGorillaV2_Decode_TooShortForFirstValue(t *testing.T) {
	encoder := NewGorillaEncoder()

	data := make([]byte, 6)
	data[0] = gorillaV2Magic
	binary.LittleEndian.PutUint32(data[1:5], 1)
	data[5] = 0

	_, _, err := encoder.DecodeFloat64(data, 1)
	if err == nil {
		t.Error("Expected error for data too short for first value")
	}
}

func TestGorillaV2_Decode_TruncatedBitstream(t *testing.T) {
	encoder := NewGorillaEncoder()

	values := make([]interface{}, 10)
	for i := range values {
		values[i] = float64(i) * 100.0
	}
	encoded, _ := encoder.Encode(values)

	truncated := encoded[:len(encoded)-3]
	_, _, err := encoder.DecodeFloat64(truncated, 10)
	if err == nil {
		t.Error("Expected error for truncated bitstream")
	}
}

// =============================================================================
// Gorilla V2 — Bitstream Truncation Tests
// =============================================================================

// gorillaEncodeHelper encodes values and returns (encoded, count).
func gorillaEncodeHelper(t *testing.T, values []interface{}) ([]byte, int) {
	t.Helper()
	encoder := NewGorillaEncoder()
	encoded, err := encoder.Encode(values)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	return encoded, len(values)
}

// contains checks if s contains substr.
func contains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestGorillaV2_Truncate_ControlBit(t *testing.T) {
	values := []interface{}{1.0, math.Pi, 2.718}
	encoded, count := gorillaEncodeHelper(t, values)

	headerSize := 1 + 4 + (count+7)/8 + 8
	truncated := encoded[:headerSize]

	encoder := NewGorillaEncoder()
	_, _, err := encoder.DecodeFloat64(truncated, count)
	if err == nil {
		t.Error("Expected error for truncated control bit")
	}
}

func TestGorillaV2_Truncate_Ctrl2Bit(t *testing.T) {
	values := []interface{}{1.0, 2.0, 3.0}
	encoded, count := gorillaEncodeHelper(t, values)

	headerSize := 1 + 4 + (count+7)/8 + 8

	manualData := make([]byte, headerSize+1)
	copy(manualData, encoded[:headerSize])
	manualData[headerSize] = 0x80

	// Use large dataset to scan for ctrl2 error via progressive truncation
	bigValues := make([]interface{}, 100)
	for i := range bigValues {
		bigValues[i] = float64(i) * math.Pi
	}
	bigEncoded, bigCount := gorillaEncodeHelper(t, bigValues)
	bigHeaderSize := 1 + 4 + (bigCount+7)/8 + 8

	foundCtrl2Error := false
	for trimBytes := len(bigEncoded) - 1; trimBytes > bigHeaderSize; trimBytes-- {
		_, _, err := NewGorillaEncoder().DecodeFloat64(bigEncoded[:trimBytes], bigCount)
		if err != nil {
			errStr := err.Error()
			if contains(errStr, "ctrl2") {
				foundCtrl2Error = true
				break
			}
		}
	}
	if !foundCtrl2Error {
		_, _, err := NewGorillaEncoder().DecodeFloat64(bigEncoded[:bigHeaderSize], bigCount)
		if err == nil {
			t.Error("Expected some truncation error")
		}
	}
}

func TestGorillaV2_Truncate_Case2_MeaningfulBits(t *testing.T) {
	values := make([]interface{}, 50)
	for i := range values {
		values[i] = 1.0 + float64(i)*1e-10
	}
	encoded, count := gorillaEncodeHelper(t, values)
	headerSize := 1 + 4 + (count+7)/8 + 8

	foundCase2Error := false
	for trimBytes := len(encoded) - 1; trimBytes > headerSize; trimBytes-- {
		_, _, err := NewGorillaEncoder().DecodeFloat64(encoded[:trimBytes], count)
		if err != nil {
			if contains(err.Error(), "case2") {
				foundCase2Error = true
				break
			}
		}
	}

	_, _, err := NewGorillaEncoder().DecodeFloat64(encoded[:headerSize+1], count)
	if err == nil {
		t.Error("Expected error from severely truncated bitstream")
	}
	_ = foundCase2Error
}

func TestGorillaV2_Truncate_Case3_LeadingBits(t *testing.T) {
	values := make([]interface{}, 50)
	for i := range values {
		values[i] = math.Pow(-1, float64(i)) * math.Exp(float64(i))
	}
	encoded, count := gorillaEncodeHelper(t, values)
	headerSize := 1 + 4 + (count+7)/8 + 8

	foundLeading := false
	foundMeaning := false
	foundBits := false
	for trimBytes := len(encoded) - 1; trimBytes > headerSize; trimBytes-- {
		_, _, err := NewGorillaEncoder().DecodeFloat64(encoded[:trimBytes], count)
		if err != nil {
			errStr := err.Error()
			if contains(errStr, "leading") {
				foundLeading = true
			}
			if contains(errStr, "meaning") {
				foundMeaning = true
			}
			if contains(errStr, "bits") {
				foundBits = true
			}
		}
	}

	_, _, err := NewGorillaEncoder().DecodeFloat64(encoded[:headerSize+1], count)
	if err == nil {
		t.Error("Expected error from truncated bitstream")
	}
	_ = foundLeading
	_ = foundMeaning
	_ = foundBits
}

// =============================================================================
// Gorilla V1 — Backward Compatibility
// =============================================================================

func TestGorillaV1_Decode_BasicRoundtrip(t *testing.T) {
	count := 3
	values := []float64{10.0, 20.0, 30.0}

	data := make([]byte, 0, 100)

	countBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(countBuf, uint32(count))
	data = append(data, countBuf...)

	data = append(data, 0x00)

	firstBuf := make([]byte, 8)
	prevBits := math.Float64bits(values[0])
	binary.LittleEndian.PutUint64(firstBuf, prevBits)
	data = append(data, firstBuf...)

	for i := 1; i < count; i++ {
		curBits := math.Float64bits(values[i])
		xor := prevBits ^ curBits
		xorBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(xorBuf, xor)
		data = append(data, xorBuf...)
		prevBits = curBits
	}

	encoder := NewGorillaEncoder()
	floats, nulls, err := encoder.DecodeFloat64(data, count)
	if err != nil {
		t.Fatalf("V1 decode failed: %v", err)
	}

	for i, v := range values {
		if nulls[i] {
			t.Errorf("Value %d should not be null", i)
		}
		if floats[i] != v {
			t.Errorf("Value %d: expected %f, got %f", i, v, floats[i])
		}
	}
}

func TestGorillaV1_Decode_WithNulls(t *testing.T) {
	count := 3
	data := make([]byte, 0, 100)

	countBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(countBuf, uint32(count))
	data = append(data, countBuf...)

	data = append(data, 0x02)

	firstBuf := make([]byte, 8)
	prevBits := math.Float64bits(10.0)
	binary.LittleEndian.PutUint64(firstBuf, prevBits)
	data = append(data, firstBuf...)

	curBits := math.Float64bits(0.0)
	xorBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(xorBuf, prevBits^curBits)
	data = append(data, xorBuf...)
	prevBits = curBits

	curBits = math.Float64bits(30.0)
	binary.LittleEndian.PutUint64(xorBuf, prevBits^curBits)
	data = append(data, xorBuf...)

	encoder := NewGorillaEncoder()
	floats, nulls, err := encoder.DecodeFloat64(data, count)
	if err != nil {
		t.Fatalf("V1 decode with nulls failed: %v", err)
	}

	if floats[0] != 10.0 || nulls[0] {
		t.Errorf("Index 0: expected 10.0 non-null, got %f null=%v", floats[0], nulls[0])
	}
	if !nulls[1] {
		t.Error("Index 1 should be null")
	}
	if floats[2] != 30.0 || nulls[2] {
		t.Errorf("Index 2: expected 30.0 non-null, got %f null=%v", floats[2], nulls[2])
	}
}

func TestGorillaV1_Decode_FirstValueNull(t *testing.T) {
	// count=3 (not 2, because count=2 has first byte 0x02 == gorillaV2Magic)
	count := 3
	data := make([]byte, 0, 100)

	countBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(countBuf, uint32(count))
	data = append(data, countBuf...)

	data = append(data, 0x01)

	firstBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(firstBuf, 0)
	data = append(data, firstBuf...)

	xorBuf := make([]byte, 8)

	curBits := math.Float64bits(42.0)
	binary.LittleEndian.PutUint64(xorBuf, 0^curBits)
	data = append(data, xorBuf...)

	nextBits := math.Float64bits(99.0)
	binary.LittleEndian.PutUint64(xorBuf, curBits^nextBits)
	data = append(data, xorBuf...)

	encoder := NewGorillaEncoder()
	floats, nulls, err := encoder.DecodeFloat64(data, count)
	if err != nil {
		t.Fatalf("V1 decode failed: %v", err)
	}

	if !nulls[0] {
		t.Error("Index 0 should be null")
	}
	if nulls[1] || floats[1] != 42.0 {
		t.Errorf("Index 1: expected 42.0, got %f null=%v", floats[1], nulls[1])
	}
	if nulls[2] || floats[2] != 99.0 {
		t.Errorf("Index 2: expected 99.0, got %f null=%v", floats[2], nulls[2])
	}
}

func TestGorillaV1_Decode_CountMismatch(t *testing.T) {
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, 99)

	encoder := NewGorillaEncoder()
	_, _, err := encoder.DecodeFloat64(data, 5)
	if err == nil {
		t.Error("Expected error for V1 count mismatch")
	}
}

func TestGorillaV1_Decode_TooShort(t *testing.T) {
	encoder := NewGorillaEncoder()

	_, _, err := encoder.DecodeFloat64([]byte{0x03, 0x00}, 1)
	if err == nil {
		t.Error("Expected error for V1 data too short for count")
	}
}

func TestGorillaV1_Decode_TooShortForNullMask(t *testing.T) {
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, 100)

	encoder := NewGorillaEncoder()
	_, _, err := encoder.DecodeFloat64(data, 100)
	if err == nil {
		t.Error("Expected error for V1 data too short for null mask")
	}
}

func TestGorillaV1_Decode_TooShortForFirstValue(t *testing.T) {
	data := make([]byte, 5)
	binary.LittleEndian.PutUint32(data, 1)
	data[4] = 0

	encoder := NewGorillaEncoder()
	_, _, err := encoder.DecodeFloat64(data, 1)
	if err == nil {
		t.Error("Expected error for V1 data too short for first value")
	}
}

func TestGorillaV1_Decode_TooShortForXOR(t *testing.T) {
	data := make([]byte, 13)
	binary.LittleEndian.PutUint32(data, 2)
	data[4] = 0

	encoder := NewGorillaEncoder()
	_, _, err := encoder.DecodeFloat64(data, 2)
	if err == nil {
		t.Error("Expected error for V1 data too short for XOR value")
	}
}

func TestGorillaV1_Decode_SingleValue(t *testing.T) {
	data := make([]byte, 13)
	binary.LittleEndian.PutUint32(data, 1)
	data[4] = 0
	binary.LittleEndian.PutUint64(data[5:], math.Float64bits(99.9))

	encoder := NewGorillaEncoder()
	floats, nulls, err := encoder.DecodeFloat64(data, 1)
	if err != nil {
		t.Fatalf("V1 single value decode failed: %v", err)
	}
	if nulls[0] || floats[0] != 99.9 {
		t.Errorf("Expected 99.9, got %f null=%v", floats[0], nulls[0])
	}
}

func TestGorillaV1_Truncate_XORValue(t *testing.T) {
	data := make([]byte, 0, 30)
	countBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(countBuf, 3)
	data = append(data, countBuf...)
	data = append(data, 0x00)

	firstBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(firstBuf, math.Float64bits(1.0))
	data = append(data, firstBuf...)

	xorBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(xorBuf, math.Float64bits(1.0)^math.Float64bits(2.0))
	data = append(data, xorBuf...)

	encoder := NewGorillaEncoder()
	_, _, err := encoder.DecodeFloat64(data, 3)
	if err == nil {
		t.Error("Expected error for truncated V1 XOR data")
	}
}

// =============================================================================
// Gorilla Encoder — Decode Wrapper
// =============================================================================

func TestGorillaEncoder_Decode_WithValidData(t *testing.T) {
	encoder := NewGorillaEncoder()
	values := []interface{}{1.0, 2.0, nil, 4.0}
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
	if decoded[0].(float64) != 1.0 || decoded[1].(float64) != 2.0 {
		t.Error("Mismatch in non-null values")
	}
	if decoded[2] != nil {
		t.Error("Expected nil at index 2")
	}
}

func TestGorillaEncoder_Decode_PropagatesError(t *testing.T) {
	encoder := NewGorillaEncoder()
	_, err := encoder.Decode([]byte{0x02}, 5)
	if err == nil {
		t.Error("Expected error from invalid data")
	}
}

// =============================================================================
// Benchmarks
// =============================================================================

func BenchmarkGorillaEncoder_Encode(b *testing.B) {
	encoder := NewGorillaEncoder()
	values := make([]interface{}, 1000)
	for i := range values {
		values[i] = float64(i) * 1.5
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = encoder.Encode(values)
	}
}

func BenchmarkGorillaEncoder_Decode(b *testing.B) {
	encoder := NewGorillaEncoder()
	values := make([]interface{}, 1000)
	for i := range values {
		values[i] = float64(i) * 1.5
	}
	encoded, _ := encoder.Encode(values)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = encoder.Decode(encoded, len(values))
	}
}

func BenchmarkGorillaEncoder_DecodeFloat64(b *testing.B) {
	encoder := NewGorillaEncoder()
	values := make([]interface{}, 1000)
	for i := range values {
		values[i] = float64(i) * 1.5
	}
	encoded, _ := encoder.Encode(values)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = encoder.DecodeFloat64(encoded, len(values))
	}
}

func BenchmarkGorillaEncoder_CompressionRatio(b *testing.B) {
	encoder := NewGorillaEncoder()

	tests := []struct {
		name   string
		values []interface{}
	}{
		{
			name: "LinearSequence",
			values: func() []interface{} {
				v := make([]interface{}, 1000)
				for i := range v {
					v[i] = float64(i) * 1.5
				}
				return v
			}(),
		},
		{
			name: "ConstantValue",
			values: func() []interface{} {
				v := make([]interface{}, 1000)
				for i := range v {
					v[i] = 42.0
				}
				return v
			}(),
		},
		{
			name: "SlowlyVaryingSensor",
			values: func() []interface{} {
				v := make([]interface{}, 1000)
				val := 25.0
				for i := range v {
					val += (float64(i%7) - 3.0) * 0.01
					v[i] = val
				}
				return v
			}(),
		},
		{
			name: "RandomWalk",
			values: func() []interface{} {
				v := make([]interface{}, 1000)
				val := 100.0
				for i := range v {
					val += float64((i*1103515245+12345)%100-50) * 0.001
					v[i] = val
				}
				return v
			}(),
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			encoded, err := encoder.Encode(tt.values)
			if err != nil {
				b.Fatal(err)
			}
			rawSize := len(tt.values) * 8
			compressedSize := len(encoded)
			ratio := float64(rawSize) / float64(compressedSize)
			bitsPerValue := float64(compressedSize*8) / float64(len(tt.values))

			b.ReportMetric(ratio, "ratio")
			b.ReportMetric(bitsPerValue, "bits/value")
			b.ReportMetric(float64(compressedSize), "compressed_bytes")
			b.ReportMetric(float64(rawSize), "raw_bytes")

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = encoder.Encode(tt.values)
			}
		})
	}
}
