package utils

// ToFloat64 converts various numeric types to float64.
// Returns the converted value and true if successful, or 0 and false if conversion fails.
// Supports: float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64
func ToFloat64(v interface{}) (float64, bool) {
	if v == nil {
		return 0, false
	}

	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int8:
		return float64(val), true
	case int16:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case uint:
		return float64(val), true
	case uint8:
		return float64(val), true
	case uint16:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint64:
		return float64(val), true
	default:
		return 0, false
	}
}

// ToFloat64Slice converts a slice of interface{} to a slice of float64.
// Non-numeric values are skipped. Returns the converted slice and the original indices
// of successfully converted values.
func ToFloat64Slice(values []interface{}) ([]float64, []int) {
	result := make([]float64, 0, len(values))
	indices := make([]int, 0, len(values))

	for i, v := range values {
		if f, ok := ToFloat64(v); ok {
			result = append(result, f)
			indices = append(indices, i)
		}
	}

	return result, indices
}

// MustToFloat64 converts a value to float64, returning 0 if conversion fails.
// Use this when you need a default value instead of checking the ok return.
func MustToFloat64(v interface{}) float64 {
	f, _ := ToFloat64(v)
	return f
}

// IsNumeric checks if a value can be converted to float64.
func IsNumeric(v interface{}) bool {
	_, ok := ToFloat64(v)
	return ok
}
