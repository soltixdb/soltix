package compression

import (
	"encoding/binary"
	"fmt"
)

// DeltaEncoder implements delta + zigzag + varint compression for int64 values
// Efficient for monotonically increasing values like timestamps
type DeltaEncoder struct{}

func NewDeltaEncoder() *DeltaEncoder {
	return &DeltaEncoder{}
}

func (e *DeltaEncoder) Type() ColumnType {
	return ColumnTypeInt64
}

func (e *DeltaEncoder) Encode(values []interface{}) ([]byte, error) {
	if len(values) == 0 {
		return nil, nil
	}

	// Convert to int64 slice
	ints := make([]int64, len(values))
	nullMask := make([]byte, (len(values)+7)/8)

	for i, v := range values {
		if v == nil {
			nullMask[i/8] |= 1 << (i % 8)
			ints[i] = 0
		} else {
			switch val := v.(type) {
			case int64:
				ints[i] = val
			case int:
				ints[i] = int64(val)
			case int32:
				ints[i] = int64(val)
			case float64:
				ints[i] = int64(val)
			case float32:
				ints[i] = int64(val)
			case bool:
				if val {
					ints[i] = 1
				} else {
					ints[i] = 0
				}
			case string:
				// Treat unexpected string values as null
				nullMask[i/8] |= 1 << (i % 8)
				ints[i] = 0
			default:
				return nil, fmt.Errorf("unsupported type for Delta encoder: %T", v)
			}
		}
	}

	return e.encodeDelta(ints, nullMask)
}

func (e *DeltaEncoder) encodeDelta(values []int64, nullMask []byte) ([]byte, error) {
	if len(values) == 0 {
		return nil, nil
	}

	// Estimate buffer size
	buf := make([]byte, 0, 4+len(nullMask)+len(values)*8)

	// Write count
	countBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(countBuf, uint32(len(values)))
	buf = append(buf, countBuf...)

	// Write null mask
	buf = append(buf, nullMask...)

	// Write first value
	firstBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(firstBuf, uint64(values[0]))
	buf = append(buf, firstBuf...)

	// Delta encode subsequent values using zigzag + varint
	prev := values[0]
	for i := 1; i < len(values); i++ {
		delta := values[i] - prev
		// ZigZag encode for signed integers
		zigzag := (delta << 1) ^ (delta >> 63)
		buf = AppendVarint(buf, uint64(zigzag))
		prev = values[i]
	}

	return buf, nil
}

func (e *DeltaEncoder) Decode(data []byte, count int) ([]interface{}, error) {
	ints, nulls, err := e.DecodeInt64(data, count)
	if err != nil {
		return nil, err
	}
	if ints == nil {
		return nil, nil
	}

	values := make([]interface{}, len(ints))
	for i, v := range ints {
		if nulls[i] {
			values[i] = nil
		} else {
			values[i] = v
		}
	}
	return values, nil
}

// DecodeInt64 decodes data directly into []int64, avoiding interface{} boxing.
// The returned []bool indicates which positions are null (true = null).
// This eliminates ~1000 heap allocations per decode call compared to Decode().
func (e *DeltaEncoder) DecodeInt64(data []byte, count int) ([]int64, []bool, error) {
	if len(data) == 0 || count == 0 {
		return nil, nil, nil
	}

	offset := 0

	// Read count
	if len(data) < 4 {
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

	// Read first value
	if offset+8 > len(data) {
		return nil, nil, fmt.Errorf("data too short for first value")
	}
	prev := int64(binary.LittleEndian.Uint64(data[offset:]))
	offset += 8

	values := make([]int64, count)
	nulls := make([]bool, count)

	// Check null for first value
	if nullMask[0]&1 != 0 {
		nulls[0] = true
	} else {
		values[0] = prev
	}

	// Delta decode subsequent values
	for i := 1; i < count; i++ {
		zigzag, n := ReadVarint(data[offset:])
		if n <= 0 {
			return nil, nil, fmt.Errorf("failed to read varint at position %d", i)
		}
		offset += n

		// ZigZag decode - convert uint64 to int64 first
		delta := int64(zigzag>>1) ^ -int64(zigzag&1)
		current := prev + delta

		// Check null
		if nullMask[i/8]&(1<<(i%8)) != 0 {
			nulls[i] = true
		} else {
			values[i] = current
		}

		prev = current
	}

	return values, nulls, nil
}
