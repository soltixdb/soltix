package compression

import (
	"math"
	"reflect"
	"testing"
)

// =============================================================================
// Varint — Basic Tests
// =============================================================================

func TestAppendVarint(t *testing.T) {
	tests := []struct {
		name     string
		value    uint64
		expected []byte
	}{
		{"Zero", 0, []byte{0x00}},
		{"One", 1, []byte{0x01}},
		{"127", 127, []byte{0x7f}},
		{"128", 128, []byte{0x80, 0x01}},
		{"300", 300, []byte{0xac, 0x02}},
		{"16383", 16383, []byte{0xff, 0x7f}},
		{"16384", 16384, []byte{0x80, 0x80, 0x01}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := AppendVarint(nil, tt.value)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestReadVarint(t *testing.T) {
	tests := []struct {
		name         string
		data         []byte
		expectedVal  uint64
		expectedSize int
	}{
		{"Zero", []byte{0x00}, 0, 1},
		{"One", []byte{0x01}, 1, 1},
		{"127", []byte{0x7f}, 127, 1},
		{"128", []byte{0x80, 0x01}, 128, 2},
		{"300", []byte{0xac, 0x02}, 300, 2},
		{"16383", []byte{0xff, 0x7f}, 16383, 2},
		{"16384", []byte{0x80, 0x80, 0x01}, 16384, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, size := ReadVarint(tt.data)
			if val != tt.expectedVal {
				t.Errorf("Value: expected %d, got %d", tt.expectedVal, val)
			}
			if size != tt.expectedSize {
				t.Errorf("Size: expected %d, got %d", tt.expectedSize, size)
			}
		})
	}
}

func TestVarintRoundtrip(t *testing.T) {
	values := []uint64{0, 1, 127, 128, 255, 256, 16383, 16384, 1000000, math.MaxUint32, math.MaxUint64}

	for _, v := range values {
		encoded := AppendVarint(nil, v)
		decoded, n := ReadVarint(encoded)

		if decoded != v {
			t.Errorf("Roundtrip failed for %d: got %d", v, decoded)
		}
		if n != len(encoded) {
			t.Errorf("Size mismatch for %d: expected %d, got %d", v, len(encoded), n)
		}
	}
}

// =============================================================================
// Varint — Edge Cases
// =============================================================================

func TestVarint_EmptyData(t *testing.T) {
	val, n := ReadVarint([]byte{})
	if n != 0 {
		t.Errorf("Expected n=0 for empty data, got %d", n)
	}
	if val != 0 {
		t.Errorf("Expected val=0 for empty data, got %d", val)
	}
}

func TestVarint_TruncatedMultiByte(t *testing.T) {
	// High bit set = expects continuation byte, but none follows
	data := []byte{0x80}
	val, n := ReadVarint(data)
	if n != 0 {
		t.Errorf("Expected n=0 for truncated varint, got %d", n)
	}
	if val != 0 {
		t.Errorf("Expected val=0 for truncated varint, got %d", val)
	}
}

func TestVarint_MaxUint64(t *testing.T) {
	var maxVal uint64 = math.MaxUint64

	encoded := AppendVarint(nil, maxVal)
	decoded, n := ReadVarint(encoded)

	if decoded != maxVal {
		t.Errorf("MaxUint64 roundtrip: expected %d, got %d", maxVal, decoded)
	}
	if n != len(encoded) {
		t.Errorf("Size mismatch: expected %d, got %d", len(encoded), n)
	}
}

func TestVarint_LargeValues(t *testing.T) {
	values := []uint64{
		0,
		1,
		127,
		128,
		255,
		256,
		16383,
		16384,
		2097151,
		2097152,
		1 << 28,
		1 << 35,
		1 << 42,
		1 << 49,
		1 << 56,
		1 << 63,
		math.MaxUint64 - 1,
		math.MaxUint64,
	}

	for _, v := range values {
		encoded := AppendVarint(nil, v)
		decoded, n := ReadVarint(encoded)

		if decoded != v {
			t.Errorf("Roundtrip failed for %d: got %d", v, decoded)
		}
		if n != len(encoded) {
			t.Errorf("Size mismatch for %d: expected %d, got %d", v, len(encoded), n)
		}
	}
}

func TestVarint_AppendToExistingBuffer(t *testing.T) {
	buf := []byte("prefix")
	buf = AppendVarint(buf, 300)

	if string(buf[:6]) != "prefix" {
		t.Error("AppendVarint should preserve existing buffer content")
	}

	val, n := ReadVarint(buf[6:])
	if val != 300 || n != 2 {
		t.Errorf("Expected val=300 n=2, got val=%d n=%d", val, n)
	}
}

func TestVarint_MultipleInSequence(t *testing.T) {
	buf := make([]byte, 0, 50)
	values := []uint64{0, 127, 128, 16384, 1000000}

	for _, v := range values {
		buf = AppendVarint(buf, v)
	}

	offset := 0
	for i, expected := range values {
		val, n := ReadVarint(buf[offset:])
		if n == 0 {
			t.Fatalf("Value %d: failed to read varint", i)
		}
		if val != expected {
			t.Errorf("Value %d: expected %d, got %d", i, expected, val)
		}
		offset += n
	}
}
