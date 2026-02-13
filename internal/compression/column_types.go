package compression

// ColumnType represents the type of data in a column
type ColumnType uint8

const (
	ColumnTypeFloat64 ColumnType = iota
	ColumnTypeInt64
	ColumnTypeString
	ColumnTypeBool
	ColumnTypeNull
)

// ColumnEncoder interface for encoding column data
type ColumnEncoder interface {
	Encode(values []interface{}) ([]byte, error)
	Decode(data []byte, count int) ([]interface{}, error)
	Type() ColumnType
}

// Type-specific decoder interfaces to avoid []interface{} boxing allocations.
// Callers should type-assert to these interfaces for zero-alloc decode paths.

// Float64Decoder decodes directly into []float64 without boxing allocations
type Float64Decoder interface {
	DecodeFloat64(data []byte, count int) ([]float64, []bool, error)
}

// Int64Decoder decodes directly into []int64 without boxing allocations
type Int64Decoder interface {
	DecodeInt64(data []byte, count int) ([]int64, []bool, error)
}

// StringDecoder decodes directly into []string without boxing allocations
type StringDecoder interface {
	DecodeStrings(data []byte, count int) ([]string, []bool, error)
}

// BoolDecoder decodes directly into []bool without boxing allocations
type BoolDecoder interface {
	DecodeBool(data []byte, count int) ([]bool, []bool, error)
}

// GetEncoder returns the appropriate encoder for a column type
func GetEncoder(colType ColumnType) ColumnEncoder {
	switch colType {
	case ColumnTypeFloat64:
		return NewGorillaEncoder()
	case ColumnTypeInt64:
		return NewDeltaEncoder()
	case ColumnTypeString:
		return NewDictionaryEncoder()
	case ColumnTypeBool:
		return NewBoolEncoder()
	default:
		return NewGorillaEncoder() // Default to float encoder
	}
}

// InferColumnType infers the column type from a slice of values
func InferColumnType(values []interface{}) ColumnType {
	for _, v := range values {
		if v == nil {
			continue
		}
		switch v.(type) {
		case float64, float32:
			return ColumnTypeFloat64
		case int, int64, int32:
			return ColumnTypeInt64
		case string:
			return ColumnTypeString
		case bool:
			return ColumnTypeBool
		}
	}
	return ColumnTypeFloat64 // Default
}
