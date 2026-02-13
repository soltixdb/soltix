package utils

import (
	"testing"
)

func TestToFloat64(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected float64
		ok       bool
	}{
		// Float types
		{"float64", float64(3.14), 3.14, true},
		{"float32", float32(2.5), 2.5, true},

		// Signed integers
		{"int", int(42), 42, true},
		{"int8", int8(8), 8, true},
		{"int16", int16(16), 16, true},
		{"int32", int32(32), 32, true},
		{"int64", int64(64), 64, true},

		// Unsigned integers
		{"uint", uint(100), 100, true},
		{"uint8", uint8(8), 8, true},
		{"uint16", uint16(16), 16, true},
		{"uint32", uint32(32), 32, true},
		{"uint64", uint64(64), 64, true},

		// Negative numbers
		{"negative int", int(-42), -42, true},
		{"negative float64", float64(-3.14), -3.14, true},

		// Zero values
		{"zero int", int(0), 0, true},
		{"zero float64", float64(0), 0, true},

		// Invalid types
		{"string", "hello", 0, false},
		{"bool true", true, 0, false},
		{"bool false", false, 0, false},
		{"nil", nil, 0, false},
		{"slice", []int{1, 2, 3}, 0, false},
		{"map", map[string]int{"a": 1}, 0, false},
		{"struct", struct{ X int }{X: 1}, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, ok := ToFloat64(tt.input)

			if ok != tt.ok {
				t.Errorf("ToFloat64(%v) ok = %v, want %v", tt.input, ok, tt.ok)
			}

			if result != tt.expected {
				t.Errorf("ToFloat64(%v) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestToFloat64Slice(t *testing.T) {
	tests := []struct {
		name            string
		input           []interface{}
		expectedValues  []float64
		expectedIndices []int
	}{
		{
			name:            "all numeric",
			input:           []interface{}{1, 2.0, int64(3), float32(4.5)},
			expectedValues:  []float64{1, 2, 3, 4.5},
			expectedIndices: []int{0, 1, 2, 3},
		},
		{
			name:            "mixed with non-numeric",
			input:           []interface{}{1, "skip", 3, nil, 5.5},
			expectedValues:  []float64{1, 3, 5.5},
			expectedIndices: []int{0, 2, 4},
		},
		{
			name:            "all non-numeric",
			input:           []interface{}{"a", "b", nil},
			expectedValues:  []float64{},
			expectedIndices: []int{},
		},
		{
			name:            "empty slice",
			input:           []interface{}{},
			expectedValues:  []float64{},
			expectedIndices: []int{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values, indices := ToFloat64Slice(tt.input)

			if len(values) != len(tt.expectedValues) {
				t.Errorf("ToFloat64Slice values length = %d, want %d", len(values), len(tt.expectedValues))
				return
			}

			for i, v := range values {
				if v != tt.expectedValues[i] {
					t.Errorf("values[%d] = %v, want %v", i, v, tt.expectedValues[i])
				}
			}

			if len(indices) != len(tt.expectedIndices) {
				t.Errorf("ToFloat64Slice indices length = %d, want %d", len(indices), len(tt.expectedIndices))
				return
			}

			for i, idx := range indices {
				if idx != tt.expectedIndices[i] {
					t.Errorf("indices[%d] = %v, want %v", i, idx, tt.expectedIndices[i])
				}
			}
		})
	}
}

func TestMustToFloat64(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected float64
	}{
		{"valid int", 42, 42},
		{"valid float64", 3.14, 3.14},
		{"invalid string", "hello", 0},
		{"nil", nil, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MustToFloat64(tt.input)
			if result != tt.expected {
				t.Errorf("MustToFloat64(%v) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestIsNumeric(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected bool
	}{
		{"int", 42, true},
		{"float64", 3.14, true},
		{"string", "hello", false},
		{"nil", nil, false},
		{"bool", true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsNumeric(tt.input)
			if result != tt.expected {
				t.Errorf("IsNumeric(%v) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func BenchmarkToFloat64(b *testing.B) {
	values := []interface{}{
		float64(3.14),
		int(42),
		int64(1000),
		float32(2.5),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, v := range values {
			ToFloat64(v)
		}
	}
}
