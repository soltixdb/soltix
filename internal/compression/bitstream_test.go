package compression

import (
	"testing"
)

// =============================================================================
// BitWriter Tests
// =============================================================================

func TestBitWriter_WriteBit_Basic(t *testing.T) {
	bw := NewBitWriter(8)

	// Write 8 bits: 10110100 = 0xB4
	bits := []byte{1, 0, 1, 1, 0, 1, 0, 0}
	for _, b := range bits {
		bw.WriteBit(b)
	}

	result := bw.Bytes()
	if len(result) != 1 {
		t.Fatalf("Expected 1 byte, got %d", len(result))
	}
	if result[0] != 0xB4 {
		t.Errorf("Expected 0xB4, got 0x%02X", result[0])
	}
}

func TestBitWriter_WriteBit_PartialByte(t *testing.T) {
	bw := NewBitWriter(8)

	// Write only 3 bits: 101 → 10100000 = 0xA0
	bw.WriteBit(1)
	bw.WriteBit(0)
	bw.WriteBit(1)

	result := bw.Bytes()
	if len(result) != 1 {
		t.Fatalf("Expected 1 byte (partial flush), got %d", len(result))
	}
	if result[0] != 0xA0 {
		t.Errorf("Expected 0xA0 (partial byte), got 0x%02X", result[0])
	}
}

func TestBitWriter_WriteBit_MultiByte(t *testing.T) {
	bw := NewBitWriter(8)

	// Write 12 bits: 10110100 1010xxxx
	bits := []byte{1, 0, 1, 1, 0, 1, 0, 0, 1, 0, 1, 0}
	for _, b := range bits {
		bw.WriteBit(b)
	}

	result := bw.Bytes()
	if len(result) != 2 {
		t.Fatalf("Expected 2 bytes, got %d", len(result))
	}
	if result[0] != 0xB4 {
		t.Errorf("Byte 0: expected 0xB4, got 0x%02X", result[0])
	}
	if result[1] != 0xA0 { // 1010_0000
		t.Errorf("Byte 1: expected 0xA0, got 0x%02X", result[1])
	}
}

func TestBitWriter_WriteBit_Empty(t *testing.T) {
	bw := NewBitWriter(8)

	result := bw.Bytes()
	if len(result) != 0 {
		t.Fatalf("Expected 0 bytes for empty writer, got %d", len(result))
	}
}

func TestBitWriter_WriteBits_FullByte(t *testing.T) {
	bw := NewBitWriter(8)

	// Write 8 bits at once: 0xAB = 10101011
	bw.WriteBits(0xAB, 8)

	result := bw.Bytes()
	if len(result) != 1 {
		t.Fatalf("Expected 1 byte, got %d", len(result))
	}
	if result[0] != 0xAB {
		t.Errorf("Expected 0xAB, got 0x%02X", result[0])
	}
}

func TestBitWriter_WriteBits_SmallValue(t *testing.T) {
	bw := NewBitWriter(8)

	// Write 3 bits of value 5 (binary: 101) → 10100000
	bw.WriteBits(5, 3)

	result := bw.Bytes()
	if result[0] != 0xA0 {
		t.Errorf("Expected 0xA0, got 0x%02X", result[0])
	}
}

func TestBitWriter_WriteBits_CrossByteBoundary(t *testing.T) {
	bw := NewBitWriter(8)

	// Write 4 bits, then 8 bits → crosses byte boundary
	bw.WriteBits(0x0F, 4) // 1111
	bw.WriteBits(0xAA, 8) // 10101010

	// Should be: 1111_1010 1010_0000
	result := bw.Bytes()
	if len(result) != 2 {
		t.Fatalf("Expected 2 bytes, got %d", len(result))
	}
	if result[0] != 0xFA {
		t.Errorf("Byte 0: expected 0xFA, got 0x%02X", result[0])
	}
	if result[1] != 0xA0 {
		t.Errorf("Byte 1: expected 0xA0, got 0x%02X", result[1])
	}
}

func TestBitWriter_WriteBits_64Bits(t *testing.T) {
	bw := NewBitWriter(16)

	val := uint64(0xDEADBEEFCAFEBABE)
	bw.WriteBits(val, 64)

	result := bw.Bytes()
	if len(result) != 8 {
		t.Fatalf("Expected 8 bytes, got %d", len(result))
	}

	// Verify MSB first ordering
	expected := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE}
	for i, e := range expected {
		if result[i] != e {
			t.Errorf("Byte %d: expected 0x%02X, got 0x%02X", i, e, result[i])
		}
	}
}

func TestBitWriter_WriteBits_ZeroBits(t *testing.T) {
	bw := NewBitWriter(8)

	// Write 0 bits should be a no-op
	bw.WriteBits(0xFF, 0)

	result := bw.Bytes()
	if len(result) != 0 {
		t.Errorf("Expected 0 bytes after writing 0 bits, got %d", len(result))
	}
}

func TestBitWriter_BitLen(t *testing.T) {
	bw := NewBitWriter(8)

	if bw.BitLen() != 0 {
		t.Errorf("Expected 0 bits initially, got %d", bw.BitLen())
	}

	bw.WriteBit(1)
	if bw.BitLen() != 1 {
		t.Errorf("Expected 1 bit after WriteBit, got %d", bw.BitLen())
	}

	bw.WriteBits(0xFF, 8)
	if bw.BitLen() != 9 {
		t.Errorf("Expected 9 bits, got %d", bw.BitLen())
	}

	// Write to fill exactly 2 bytes (16 bits)
	bw.WriteBits(0, 7)
	if bw.BitLen() != 16 {
		t.Errorf("Expected 16 bits, got %d", bw.BitLen())
	}
}

func TestBitWriter_Bytes_ExactByteAligned(t *testing.T) {
	bw := NewBitWriter(8)

	// Write exactly 16 bits → should not have a partial byte
	bw.WriteBits(0xABCD, 16)

	result := bw.Bytes()
	if len(result) != 2 {
		t.Fatalf("Expected 2 bytes (exact alignment), got %d", len(result))
	}
	if result[0] != 0xAB || result[1] != 0xCD {
		t.Errorf("Expected [0xAB, 0xCD], got [0x%02X, 0x%02X]", result[0], result[1])
	}
}

// =============================================================================
// BitReader Tests
// =============================================================================

func TestBitReader_ReadBit_Basic(t *testing.T) {
	// 0xB4 = 10110100
	br := NewBitReader([]byte{0xB4})

	expected := []byte{1, 0, 1, 1, 0, 1, 0, 0}
	for i, exp := range expected {
		bit, ok := br.ReadBit()
		if !ok {
			t.Fatalf("ReadBit %d returned false", i)
		}
		if bit != exp {
			t.Errorf("Bit %d: expected %d, got %d", i, exp, bit)
		}
	}
}

func TestBitReader_ReadBit_Exhausted(t *testing.T) {
	br := NewBitReader([]byte{0xFF})

	// Read all 8 bits
	for i := 0; i < 8; i++ {
		_, ok := br.ReadBit()
		if !ok {
			t.Fatalf("ReadBit %d should succeed", i)
		}
	}

	// 9th read should fail
	_, ok := br.ReadBit()
	if ok {
		t.Error("ReadBit should return false when exhausted")
	}
}

func TestBitReader_ReadBit_EmptyData(t *testing.T) {
	br := NewBitReader([]byte{})

	_, ok := br.ReadBit()
	if ok {
		t.Error("ReadBit on empty data should return false")
	}
}

func TestBitReader_ReadBits_FullByte(t *testing.T) {
	br := NewBitReader([]byte{0xAB})

	val, ok := br.ReadBits(8)
	if !ok {
		t.Fatal("ReadBits should succeed")
	}
	if val != 0xAB {
		t.Errorf("Expected 0xAB, got 0x%X", val)
	}
}

func TestBitReader_ReadBits_CrossByteBoundary(t *testing.T) {
	// Read 12 bits from [0xFA, 0xB0]: 1111_1010 1011_0000
	br := NewBitReader([]byte{0xFA, 0xB0})

	// Read 4 bits → 1111 = 0x0F
	val, ok := br.ReadBits(4)
	if !ok {
		t.Fatal("ReadBits(4) should succeed")
	}
	if val != 0x0F {
		t.Errorf("First 4 bits: expected 0x0F, got 0x%X", val)
	}

	// Read 8 bits → 10101011 = 0xAB
	val, ok = br.ReadBits(8)
	if !ok {
		t.Fatal("ReadBits(8) should succeed")
	}
	if val != 0xAB {
		t.Errorf("Next 8 bits: expected 0xAB, got 0x%X", val)
	}
}

func TestBitReader_ReadBits_Exhausted(t *testing.T) {
	br := NewBitReader([]byte{0xFF})

	// Try to read 9 bits from 1 byte → should fail
	_, ok := br.ReadBits(9)
	if ok {
		t.Error("ReadBits(9) from 1 byte should return false")
	}
}

func TestBitReader_ReadBits_ZeroBits(t *testing.T) {
	br := NewBitReader([]byte{0xFF})

	val, ok := br.ReadBits(0)
	if !ok {
		t.Error("ReadBits(0) should succeed")
	}
	if val != 0 {
		t.Errorf("ReadBits(0) should return 0, got %d", val)
	}
}

func TestBitReader_ReadBits_64Bits(t *testing.T) {
	data := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE}
	br := NewBitReader(data)

	val, ok := br.ReadBits(64)
	if !ok {
		t.Fatal("ReadBits(64) should succeed")
	}
	if val != 0xDEADBEEFCAFEBABE {
		t.Errorf("Expected 0xDEADBEEFCAFEBABE, got 0x%X", val)
	}
}

// =============================================================================
// BitWriter + BitReader Roundtrip Tests
// =============================================================================

func TestBitRoundtrip_MixedWidths(t *testing.T) {
	bw := NewBitWriter(32)

	// Write various widths
	bw.WriteBit(1)        // 1 bit
	bw.WriteBits(5, 3)    // 3 bits = 101
	bw.WriteBits(0xAB, 8) // 8 bits
	bw.WriteBits(0, 6)    // 6 bits = 000000
	bw.WriteBits(42, 10)  // 10 bits

	data := bw.Bytes()
	br := NewBitReader(data)

	// Read back
	v1, ok := br.ReadBit()
	if !ok || v1 != 1 {
		t.Errorf("Bit 1: expected 1, got %d (ok=%v)", v1, ok)
	}

	v2, ok := br.ReadBits(3)
	if !ok || v2 != 5 {
		t.Errorf("Bits 3: expected 5, got %d", v2)
	}

	v3, ok := br.ReadBits(8)
	if !ok || v3 != 0xAB {
		t.Errorf("Bits 8: expected 0xAB, got 0x%X", v3)
	}

	v4, ok := br.ReadBits(6)
	if !ok || v4 != 0 {
		t.Errorf("Bits 6: expected 0, got %d", v4)
	}

	v5, ok := br.ReadBits(10)
	if !ok || v5 != 42 {
		t.Errorf("Bits 10: expected 42, got %d", v5)
	}
}

func TestBitRoundtrip_SingleBits(t *testing.T) {
	bw := NewBitWriter(4)

	pattern := []byte{1, 0, 0, 1, 1, 0, 1, 0, 1, 1}
	for _, b := range pattern {
		bw.WriteBit(b)
	}

	br := NewBitReader(bw.Bytes())
	for i, exp := range pattern {
		bit, ok := br.ReadBit()
		if !ok {
			t.Fatalf("ReadBit %d: unexpected EOF", i)
		}
		if bit != exp {
			t.Errorf("Bit %d: expected %d, got %d", i, exp, bit)
		}
	}
}

func TestBitRoundtrip_LargeValues(t *testing.T) {
	bw := NewBitWriter(64)

	vals := []struct {
		v    uint64
		bits uint8
	}{
		{0, 1},
		{1, 1},
		{63, 6},
		{0, 6},
		{0xFFFF, 16},
		{0xFFFFFFFFFFFFFFFF, 64},
	}

	for _, v := range vals {
		bw.WriteBits(v.v, v.bits)
	}

	br := NewBitReader(bw.Bytes())
	for i, v := range vals {
		got, ok := br.ReadBits(v.bits)
		if !ok {
			t.Fatalf("Value %d: unexpected EOF", i)
		}
		// Mask expected to nbits
		mask := uint64(0)
		if v.bits < 64 {
			mask = (1 << v.bits) - 1
		} else {
			mask = ^uint64(0)
		}
		expected := v.v & mask
		if got != expected {
			t.Errorf("Value %d (%d bits): expected 0x%X, got 0x%X", i, v.bits, expected, got)
		}
	}
}

// =============================================================================
// LeadingZeros64 / TrailingZeros64 Tests
// =============================================================================

func TestLeadingZeros64(t *testing.T) {
	tests := []struct {
		input    uint64
		expected uint8
	}{
		{0, 64},
		{1, 63},
		{0x8000000000000000, 0},
		{0x0000000000000001, 63},
		{0x00FF000000000000, 8},
		{0xFFFFFFFFFFFFFFFF, 0},
	}

	for _, tt := range tests {
		result := LeadingZeros64(tt.input)
		if result != tt.expected {
			t.Errorf("LeadingZeros64(0x%X): expected %d, got %d", tt.input, tt.expected, result)
		}
	}
}

func TestTrailingZeros64(t *testing.T) {
	tests := []struct {
		input    uint64
		expected uint8
	}{
		{0, 64},
		{1, 0},
		{0x8000000000000000, 63},
		{0x0000000000000100, 8},
		{0xFFFFFFFFFFFFFFFF, 0},
		{0x0000000000001000, 12},
	}

	for _, tt := range tests {
		result := TrailingZeros64(tt.input)
		if result != tt.expected {
			t.Errorf("TrailingZeros64(0x%X): expected %d, got %d", tt.input, tt.expected, result)
		}
	}
}
