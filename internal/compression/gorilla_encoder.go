package compression

import (
	"encoding/binary"
	"fmt"
	"math"
)

// GorillaEncoder implements XOR-based compression for float64 values.
// Based on Facebook's Gorilla paper (Section 4.1.2):
//
//	Pelkonen et al., "Gorilla: A Fast, Scalable, In-Memory Time Series Database"
//	PVLDB, Vol. 8, No. 12, 2015.
//
// XOR bit-packing algorithm:
//
//  1. First value: stored as raw 64-bit IEEE 754.
//  2. Subsequent values: XOR with previous value's bits.
//     - XOR == 0: write single '0' bit  (values are identical)
//     - XOR != 0: write '1' bit, then:
//     a) If leading zeros >= prevLeading AND trailing zeros >= prevTrailing
//     (meaningful bits fit within the previous window):
//     write '0' bit + meaningful bits only (prevMeaningBits width)
//     b) Otherwise:
//     write '1' bit + 6 bits (leading zeros count) + 6 bits (meaningful bits length - 1)
//     + meaningful bits
//
// This achieves ~1.37 bytes/value for real-world time-series data vs 8 bytes raw.
type GorillaEncoder struct{}

const (
	gorillaV2Magic = 0x02 // Version byte to distinguish bit-packed format from V1
)

func NewGorillaEncoder() *GorillaEncoder {
	return &GorillaEncoder{}
}

func (e *GorillaEncoder) Type() ColumnType {
	return ColumnTypeFloat64
}

func (e *GorillaEncoder) Encode(values []interface{}) ([]byte, error) {
	if len(values) == 0 {
		return nil, nil
	}

	// Convert to float64 slice
	floats := make([]float64, len(values))
	nullMask := make([]byte, (len(values)+7)/8)

	for i, v := range values {
		if v == nil {
			nullMask[i/8] |= 1 << (i % 8)
			floats[i] = 0
		} else {
			switch val := v.(type) {
			case float64:
				floats[i] = val
			case float32:
				floats[i] = float64(val)
			case int:
				floats[i] = float64(val)
			case int64:
				floats[i] = float64(val)
			case int32:
				floats[i] = float64(val)
			case string:
				// Treat unexpected string values (e.g. from corrupted WAL null
				// deserialization) as null to avoid crashing the encoder.
				nullMask[i/8] |= 1 << (i % 8)
				floats[i] = 0
			case bool:
				// Coerce bool to float: true=1, false=0
				if val {
					floats[i] = 1
				} else {
					floats[i] = 0
				}
			default:
				return nil, fmt.Errorf("unsupported type for Gorilla encoder: %T", v)
			}
		}
	}

	return e.encodeGorilla(floats, nullMask)
}

// encodeGorilla implements the Gorilla paper's XOR bit-packing compression.
//
// Wire format (V2):
//
//	[version: 1 byte = 0x02]
//	[count:   4 bytes LE]
//	[nullMask: ceil(count/8) bytes]
//	[first value: 8 bytes LE, raw IEEE 754 bits]
//	[XOR bit stream: variable length]
//	[padding: 0-7 bits to byte boundary]
func (e *GorillaEncoder) encodeGorilla(values []float64, nullMask []byte) ([]byte, error) {
	if len(values) == 0 {
		return nil, nil
	}

	// Header: version(1) + count(4) + nullMask + firstValue(8)
	headerSize := 1 + 4 + len(nullMask) + 8
	// Estimate bit stream: worst case ~77 bits per value (1+1+6+6+64),
	// but typically ~11 bits. Use 2 bytes/value estimate.
	bw := NewBitWriter(headerSize + len(values)*2)

	// Build header into a separate buffer, then prepend
	header := make([]byte, 0, headerSize)

	// Version
	header = append(header, gorillaV2Magic)

	// Count
	countBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(countBuf, uint32(len(values)))
	header = append(header, countBuf...)

	// Null mask
	header = append(header, nullMask...)

	// First value raw
	firstBits := math.Float64bits(values[0])
	firstBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(firstBuf, firstBits)
	header = append(header, firstBuf...)

	// XOR bit-packing for subsequent values
	prevBits := firstBits
	prevLeading := uint8(64) // no previous window yet
	prevTrailing := uint8(0)
	prevMeaningBits := uint8(64)

	for i := 1; i < len(values); i++ {
		currentBits := math.Float64bits(values[i])
		xor := prevBits ^ currentBits

		if xor == 0 {
			// Case 1: identical values → single '0' bit
			bw.WriteBit(0)
		} else {
			bw.WriteBit(1) // values differ

			leading := LeadingZeros64(xor)
			trailing := TrailingZeros64(xor)

			// Clamp leading to 6-bit max (0-63). 64 leading zeros is impossible
			// here since xor != 0, but be safe.
			if leading > 63 {
				leading = 63
			}

			meaningBits := 64 - leading - trailing

			if prevMeaningBits < 64 && leading >= prevLeading && trailing >= prevTrailing {
				// Case 2: meaningful bits fit within previous window
				// Write '0' + meaningful bits using previous window width
				bw.WriteBit(0)
				// Extract meaningful bits from XOR, right-shift to remove trailing zeros
				// then mask to prevMeaningBits width
				meaningful := (xor >> prevTrailing)
				bw.WriteBits(meaningful, prevMeaningBits)
			} else {
				// Case 3: new window — write '1' + leading(6) + length(6) + bits
				bw.WriteBit(1)
				bw.WriteBits(uint64(leading), 6)
				// Store (meaningBits - 1) in 6 bits; values 1..64 map to 0..63.
				// meaningBits can't be 0 since xor != 0.
				bw.WriteBits(uint64(meaningBits-1), 6)
				// Write meaningful bits (MSB-aligned extraction)
				meaningful := (xor >> trailing)
				bw.WriteBits(meaningful, meaningBits)

				// Update window for next iteration
				prevLeading = leading
				prevTrailing = trailing
				prevMeaningBits = meaningBits
			}
		}

		prevBits = currentBits
	}

	// Combine header + bit stream
	bitBytes := bw.Bytes()
	result := make([]byte, len(header)+len(bitBytes))
	copy(result, header)
	copy(result[len(header):], bitBytes)

	return result, nil
}

func (e *GorillaEncoder) Decode(data []byte, count int) ([]interface{}, error) {
	floats, nulls, err := e.DecodeFloat64(data, count)
	if err != nil {
		return nil, err
	}
	if floats == nil {
		return nil, nil
	}

	values := make([]interface{}, len(floats))
	for i, f := range floats {
		if nulls[i] {
			values[i] = nil
		} else {
			values[i] = f
		}
	}
	return values, nil
}

// DecodeFloat64 decodes Gorilla-compressed data directly into []float64.
// Supports both V1 (simple XOR, 8 bytes per XOR) and V2 (bit-packed) formats.
// The returned []bool indicates null positions (true = null).
func (e *GorillaEncoder) DecodeFloat64(data []byte, count int) ([]float64, []bool, error) {
	if len(data) == 0 || count == 0 {
		return nil, nil, nil
	}

	if len(data) < 1 {
		return nil, nil, fmt.Errorf("data too short")
	}

	// Check version byte to determine format
	if data[0] == gorillaV2Magic {
		return e.decodeGorillaV2(data, count)
	}
	// V1 format: first 4 bytes are count (LE uint32), never == 0x02 for real data
	return e.decodeGorillaV1(data, count)
}

// decodeGorillaV1 decodes the old simple XOR format (backward compatibility).
func (e *GorillaEncoder) decodeGorillaV1(data []byte, count int) ([]float64, []bool, error) {
	offset := 0

	if len(data) < 4 {
		return nil, nil, fmt.Errorf("data too short for count")
	}
	storedCount := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	if int(storedCount) != count {
		return nil, nil, fmt.Errorf("count mismatch: expected %d, got %d", count, storedCount)
	}

	nullMaskSize := (count + 7) / 8
	if offset+nullMaskSize > len(data) {
		return nil, nil, fmt.Errorf("data too short for null mask")
	}
	nullMask := data[offset : offset+nullMaskSize]
	offset += nullMaskSize

	if offset+8 > len(data) {
		return nil, nil, fmt.Errorf("data too short for first value")
	}
	prevBits := binary.LittleEndian.Uint64(data[offset:])
	offset += 8

	values := make([]float64, count)
	nulls := make([]bool, count)

	if nullMask[0]&1 != 0 {
		nulls[0] = true
	} else {
		values[0] = math.Float64frombits(prevBits)
	}

	for i := 1; i < count; i++ {
		if offset+8 > len(data) {
			return nil, nil, fmt.Errorf("data too short for value %d", i)
		}
		xor := binary.LittleEndian.Uint64(data[offset:])
		offset += 8
		currentBits := prevBits ^ xor
		if nullMask[i/8]&(1<<(i%8)) != 0 {
			nulls[i] = true
		} else {
			values[i] = math.Float64frombits(currentBits)
		}
		prevBits = currentBits
	}

	return values, nulls, nil
}

// decodeGorillaV2 decodes the Gorilla paper's bit-packed XOR format.
func (e *GorillaEncoder) decodeGorillaV2(data []byte, count int) ([]float64, []bool, error) {
	offset := 0

	// Skip version byte
	offset++

	// Read count
	if offset+4 > len(data) {
		return nil, nil, fmt.Errorf("data too short for count")
	}
	storedCount := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	if int(storedCount) != count {
		return nil, nil, fmt.Errorf("count mismatch: expected %d, got %d", count, storedCount)
	}

	// Read null mask
	nullMaskSize := (count + 7) / 8
	if offset+nullMaskSize > len(data) {
		return nil, nil, fmt.Errorf("data too short for null mask")
	}
	nullMask := data[offset : offset+nullMaskSize]
	offset += nullMaskSize

	// Read first value (raw 8 bytes)
	if offset+8 > len(data) {
		return nil, nil, fmt.Errorf("data too short for first value")
	}
	prevBits := binary.LittleEndian.Uint64(data[offset:])
	offset += 8

	values := make([]float64, count)
	nulls := make([]bool, count)

	if nullMask[0]&1 != 0 {
		nulls[0] = true
	} else {
		values[0] = math.Float64frombits(prevBits)
	}

	// Bit-level reader for XOR stream
	br := NewBitReader(data[offset:])

	prevTrailing := uint8(0)
	prevMeaningBits := uint8(64)

	for i := 1; i < count; i++ {
		// Read control bit
		controlBit, ok := br.ReadBit()
		if !ok {
			return nil, nil, fmt.Errorf("unexpected end of bitstream at value %d", i)
		}

		if controlBit == 0 {
			// Case 1: XOR == 0, same as previous
			currentBits := prevBits
			if nullMask[i/8]&(1<<(i%8)) != 0 {
				nulls[i] = true
			} else {
				values[i] = math.Float64frombits(currentBits)
			}
			// prevBits unchanged
		} else {
			// XOR != 0, read second control bit
			controlBit2, ok := br.ReadBit()
			if !ok {
				return nil, nil, fmt.Errorf("unexpected end of bitstream at value %d (ctrl2)", i)
			}

			var xor uint64
			if controlBit2 == 0 {
				// Case 2: meaningful bits fit in previous window
				meaningful, ok := br.ReadBits(prevMeaningBits)
				if !ok {
					return nil, nil, fmt.Errorf("unexpected end of bitstream at value %d (case2)", i)
				}
				xor = meaningful << prevTrailing
			} else {
				// Case 3: new window
				leadingRaw, ok := br.ReadBits(6)
				if !ok {
					return nil, nil, fmt.Errorf("unexpected end of bitstream at value %d (leading)", i)
				}
				meaningRaw, ok := br.ReadBits(6)
				if !ok {
					return nil, nil, fmt.Errorf("unexpected end of bitstream at value %d (meaning)", i)
				}

				leading := uint8(leadingRaw)
				meaningBits := uint8(meaningRaw) + 1 // stored as (meaningBits - 1)
				trailing := 64 - leading - meaningBits

				meaningful, ok := br.ReadBits(meaningBits)
				if !ok {
					return nil, nil, fmt.Errorf("unexpected end of bitstream at value %d (bits)", i)
				}
				xor = meaningful << trailing

				prevTrailing = trailing
				prevMeaningBits = meaningBits
			}

			currentBits := prevBits ^ xor
			if nullMask[i/8]&(1<<(i%8)) != 0 {
				nulls[i] = true
			} else {
				values[i] = math.Float64frombits(currentBits)
			}
			prevBits = currentBits
		}
	}

	return values, nulls, nil
}
