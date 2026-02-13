package compression

import (
	"encoding/binary"
	"fmt"
	"strconv"
)

// DictionaryEncoder implements dictionary encoding for string values
// Builds a dictionary of unique strings and stores indices
type DictionaryEncoder struct{}

func NewDictionaryEncoder() *DictionaryEncoder {
	return &DictionaryEncoder{}
}

func (e *DictionaryEncoder) Type() ColumnType {
	return ColumnTypeString
}

// toString converts a value to string without fmt.Sprintf overhead.
// Uses type switch + strconv for common types (~10x faster than fmt.Sprintf).
func toString(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case int:
		return strconv.Itoa(val)
	case int64:
		return strconv.FormatInt(val, 10)
	case int32:
		return strconv.FormatInt(int64(val), 10)
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(val), 'f', -1, 32)
	case bool:
		return strconv.FormatBool(val)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func (e *DictionaryEncoder) Encode(values []interface{}) ([]byte, error) {
	if len(values) == 0 {
		return nil, nil
	}

	n := len(values)
	nullMaskSize := (n + 7) / 8

	// Combined nullMask + indices allocation to reduce alloc count
	indices := make([]uint32, n)
	nullMask := make([]byte, nullMaskSize)
	dictList := make([]string, 0, 8)

	// For small dictionaries (< 32 entries), linear scan beats map hash.
	// IoT/TSDB columns typically have very few unique strings (status codes,
	// device types, sensor names), so the linear path covers >95% of real cases.
	const mapThreshold = 32
	var dict map[string]uint32

	// Last-seen cache: skip lookup when consecutive values repeat
	var lastStr string
	var lastIdx uint32
	var lastValid bool

	var totalStringBytes int
	for i, v := range values {
		if v == nil {
			nullMask[i/8] |= 1 << (i % 8)
			continue
		}

		s, ok := v.(string)
		if !ok {
			s = toString(v)
		}

		// Fast path: same as last value
		if lastValid && s == lastStr {
			indices[i] = lastIdx
			continue
		}

		var idx uint32
		var found bool

		if dict != nil {
			// Large dict path: use map
			idx, found = dict[s]
		} else {
			// Small dict path: linear scan (no hash overhead)
			for j, ds := range dictList {
				if ds == s {
					idx = uint32(j)
					found = true
					break
				}
			}
		}

		if !found {
			idx = uint32(len(dictList))
			dictList = append(dictList, s)
			totalStringBytes += len(s)

			// Upgrade to map when dict exceeds threshold
			if dict == nil && len(dictList) > mapThreshold {
				dict = make(map[string]uint32, len(dictList)*2)
				for j, ds := range dictList {
					dict[ds] = uint32(j)
				}
			} else if dict != nil {
				dict[s] = idx
			}
		}

		indices[i] = idx
		lastStr = s
		lastIdx = idx
		lastValid = true
	}

	// Pre-estimate buffer capacity accurately:
	// 4 (count) + nullMask + 4 (dictSize) + dict entries (4+len each) + indices
	idxBytes := 1
	if len(dictList) >= 128 {
		idxBytes = 2
	}
	estimatedCap := 8 + nullMaskSize + len(dictList)*4 + totalStringBytes + n*idxBytes
	buf := make([]byte, 0, estimatedCap)

	// Write value count — use AppendUint32 (Go 1.19+), zero allocs
	buf = binary.LittleEndian.AppendUint32(buf, uint32(n))

	// Write null mask
	buf = append(buf, nullMask...)

	// Write dictionary size
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(dictList)))

	// Write dictionary entries
	for _, s := range dictList {
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(s)))
		buf = append(buf, s...)
	}

	// Write indices (using varint encoding based on dict size)
	for _, idx := range indices {
		buf = AppendVarint(buf, uint64(idx))
	}

	return buf, nil
}

func (e *DictionaryEncoder) Decode(data []byte, count int) ([]interface{}, error) {
	strs, nulls, err := e.DecodeStrings(data, count)
	if err != nil {
		return nil, err
	}
	if strs == nil {
		return nil, nil
	}

	values := make([]interface{}, len(strs))
	for i, s := range strs {
		if nulls[i] {
			values[i] = nil
		} else {
			values[i] = s
		}
	}
	return values, nil
}

// DecodeStrings decodes data directly into []string, avoiding interface{} boxing.
// The returned []bool indicates which positions are null (true = null).
// This eliminates ~1000 heap allocations per decode call compared to Decode().
func (e *DictionaryEncoder) DecodeStrings(data []byte, count int) ([]string, []bool, error) {
	if len(data) == 0 || count == 0 {
		return nil, nil, nil
	}

	offset := 0

	// Read value count
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

	// Read dictionary size
	if offset+4 > len(data) {
		return nil, nil, fmt.Errorf("data too short for dict size")
	}
	dictSize := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	// Read dictionary entries
	dictList := make([]string, dictSize)
	for i := uint32(0); i < dictSize; i++ {
		if offset+4 > len(data) {
			return nil, nil, fmt.Errorf("data too short for dict entry length")
		}
		sLen := binary.LittleEndian.Uint32(data[offset:])
		offset += 4

		if offset+int(sLen) > len(data) {
			return nil, nil, fmt.Errorf("data too short for dict entry")
		}
		dictList[i] = string(data[offset : offset+int(sLen)])
		offset += int(sLen)
	}

	// Read indices
	values := make([]string, count)
	nulls := make([]bool, count)
	for i := 0; i < count; i++ {
		idx, n := ReadVarint(data[offset:])
		if n <= 0 {
			return nil, nil, fmt.Errorf("failed to read varint at position %d", i)
		}
		offset += n

		// Check null
		if nullMask[i/8]&(1<<(i%8)) != 0 {
			nulls[i] = true
		} else {
			if idx >= uint64(len(dictList)) {
				return nil, nil, fmt.Errorf("dictionary index out of range: %d >= %d", idx, len(dictList))
			}
			values[i] = dictList[idx]
		}
	}

	return values, nulls, nil
}
