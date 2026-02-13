package compression

import (
	"encoding/binary"
	"fmt"
)

// BoolEncoder implements bitmap encoding for boolean values
// Each bool is stored as 1 bit, packing 8 bools per byte
type BoolEncoder struct{}

func NewBoolEncoder() *BoolEncoder {
	return &BoolEncoder{}
}

func (e *BoolEncoder) Type() ColumnType {
	return ColumnTypeBool
}

func (e *BoolEncoder) Encode(values []interface{}) ([]byte, error) {
	if len(values) == 0 {
		return nil, nil
	}

	nullMask := make([]byte, (len(values)+7)/8)
	valueMask := make([]byte, (len(values)+7)/8)

	for i, v := range values {
		if v == nil {
			nullMask[i/8] |= 1 << (i % 8)
		} else if b, ok := v.(bool); ok && b {
			valueMask[i/8] |= 1 << (i % 8)
		}
	}

	// Encode: count + nullMask + valueMask
	buf := make([]byte, 4+len(nullMask)+len(valueMask))
	binary.LittleEndian.PutUint32(buf, uint32(len(values)))
	copy(buf[4:], nullMask)
	copy(buf[4+len(nullMask):], valueMask)

	return buf, nil
}

func (e *BoolEncoder) Decode(data []byte, count int) ([]interface{}, error) {
	if len(data) == 0 || count == 0 {
		return nil, nil
	}

	if len(data) < 4 {
		return nil, fmt.Errorf("data too short for count")
	}

	storedCount := binary.LittleEndian.Uint32(data)
	if int(storedCount) != count {
		return nil, fmt.Errorf("count mismatch: expected %d, got %d", count, storedCount)
	}

	maskSize := (count + 7) / 8
	if len(data) < 4+maskSize*2 {
		return nil, fmt.Errorf("data too short for masks")
	}

	nullMask := data[4 : 4+maskSize]
	valueMask := data[4+maskSize : 4+maskSize*2]

	values := make([]interface{}, count)
	for i := 0; i < count; i++ {
		if nullMask[i/8]&(1<<(i%8)) != 0 {
			values[i] = nil
		} else {
			values[i] = valueMask[i/8]&(1<<(i%8)) != 0
		}
	}

	return values, nil
}
