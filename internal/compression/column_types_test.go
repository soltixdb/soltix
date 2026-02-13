package compression

import (
	"reflect"
	"testing"
)

// =============================================================================
// Column Type Constants
// =============================================================================

func TestColumnTypeConstants(t *testing.T) {
	tests := []struct {
		name     string
		colType  ColumnType
		expected uint8
	}{
		{"Float64", ColumnTypeFloat64, 0},
		{"Int64", ColumnTypeInt64, 1},
		{"String", ColumnTypeString, 2},
		{"Bool", ColumnTypeBool, 3},
		{"Null", ColumnTypeNull, 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if uint8(tt.colType) != tt.expected {
				t.Errorf("Expected %s=%d, got %d", tt.name, tt.expected, tt.colType)
			}
		})
	}
}

// =============================================================================
// GetEncoder
// =============================================================================

func TestGetEncoder(t *testing.T) {
	tests := []struct {
		colType      ColumnType
		expectedType string
	}{
		{ColumnTypeFloat64, "*compression.GorillaEncoder"},
		{ColumnTypeInt64, "*compression.DeltaEncoder"},
		{ColumnTypeString, "*compression.DictionaryEncoder"},
		{ColumnTypeBool, "*compression.BoolEncoder"},
		{ColumnTypeNull, "*compression.GorillaEncoder"}, // Default
	}

	for _, tt := range tests {
		t.Run(tt.expectedType, func(t *testing.T) {
			encoder := GetEncoder(tt.colType)
			actualType := reflect.TypeOf(encoder).String()
			if actualType != tt.expectedType {
				t.Errorf("Expected %s, got %s", tt.expectedType, actualType)
			}
		})
	}
}

func TestGetEncoder_UnknownType(t *testing.T) {
	// Unknown ColumnType should default to GorillaEncoder
	encoder := GetEncoder(ColumnType(255))
	if encoder.Type() != ColumnTypeFloat64 {
		t.Errorf("Unknown type should default to GorillaEncoder (Float64), got %d", encoder.Type())
	}
}

// =============================================================================
// InferColumnType
// =============================================================================

func TestInferColumnType(t *testing.T) {
	tests := []struct {
		name     string
		values   []interface{}
		expected ColumnType
	}{
		{"Float64", []interface{}{1.5, 2.5, 3.5}, ColumnTypeFloat64},
		{"Float32", []interface{}{float32(1.5), float32(2.5)}, ColumnTypeFloat64},
		{"Int64", []interface{}{int64(1), int64(2), int64(3)}, ColumnTypeInt64},
		{"Int", []interface{}{1, 2, 3}, ColumnTypeInt64},
		{"Int32", []interface{}{int32(1), int32(2)}, ColumnTypeInt64},
		{"String", []interface{}{"a", "b", "c"}, ColumnTypeString},
		{"Bool", []interface{}{true, false, true}, ColumnTypeBool},
		{"WithNulls", []interface{}{nil, 1.5, nil, 2.5}, ColumnTypeFloat64},
		{"AllNulls", []interface{}{nil, nil, nil}, ColumnTypeFloat64}, // Default
		{"Empty", []interface{}{}, ColumnTypeFloat64},                 // Default
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := InferColumnType(tt.values)
			if result != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, result)
			}
		})
	}
}

func TestInferColumnType_MixedNullsAndType(t *testing.T) {
	// Nulls before the actual typed value
	result := InferColumnType([]interface{}{nil, nil, nil, int64(5)})
	if result != ColumnTypeInt64 {
		t.Errorf("Expected Int64, got %d", result)
	}
}

func TestInferColumnType_UnsupportedType(t *testing.T) {
	// Unsupported types should fall through to default (Float64)
	result := InferColumnType([]interface{}{struct{}{}})
	if result != ColumnTypeFloat64 {
		t.Errorf("Expected Float64 default for unsupported type, got %d", result)
	}
}

// =============================================================================
// Typed Decoder Interface Tests
// =============================================================================

// Compile-time interface satisfaction checks
var (
	_ Float64Decoder = (*GorillaEncoder)(nil)
	_ Int64Decoder   = (*DeltaEncoder)(nil)
	_ StringDecoder  = (*DictionaryEncoder)(nil)
)

func TestTypedDecoderInterfaces(t *testing.T) {
	t.Run("GorillaEncoder_implements_Float64Decoder", func(t *testing.T) {
		var encoder ColumnEncoder = NewGorillaEncoder()
		if _, ok := encoder.(Float64Decoder); !ok {
			t.Error("GorillaEncoder should implement Float64Decoder")
		}
	})

	t.Run("DeltaEncoder_implements_Int64Decoder", func(t *testing.T) {
		var encoder ColumnEncoder = NewDeltaEncoder()
		if _, ok := encoder.(Int64Decoder); !ok {
			t.Error("DeltaEncoder should implement Int64Decoder")
		}
	})

	t.Run("DictionaryEncoder_implements_StringDecoder", func(t *testing.T) {
		var encoder ColumnEncoder = NewDictionaryEncoder()
		if _, ok := encoder.(StringDecoder); !ok {
			t.Error("DictionaryEncoder should implement StringDecoder")
		}
	})
}

// =============================================================================
// Cross-encoder Tests — Encode/Decode Consistency
// =============================================================================

func TestAllEncoders_EmptyRoundtrip(t *testing.T) {
	encoders := []struct {
		name    string
		encoder ColumnEncoder
	}{
		{"Gorilla", NewGorillaEncoder()},
		{"Delta", NewDeltaEncoder()},
		{"Dictionary", NewDictionaryEncoder()},
		{"Bool", NewBoolEncoder()},
	}

	for _, enc := range encoders {
		t.Run(enc.name, func(t *testing.T) {
			encoded, err := enc.encoder.Encode([]interface{}{})
			if err != nil {
				t.Fatalf("Encode failed: %v", err)
			}
			if encoded != nil {
				t.Errorf("Expected nil for empty encode")
			}

			decoded, err := enc.encoder.Decode(nil, 0)
			if err != nil {
				t.Fatalf("Decode failed: %v", err)
			}
			if decoded != nil {
				t.Errorf("Expected nil for empty decode")
			}
		})
	}
}

func TestAllEncoders_NullOnlyRoundtrip(t *testing.T) {
	tests := []struct {
		name    string
		encoder ColumnEncoder
		count   int
	}{
		{"Gorilla_1null", NewGorillaEncoder(), 1},
		{"Gorilla_5null", NewGorillaEncoder(), 5},
		{"Delta_1null", NewDeltaEncoder(), 1},
		{"Delta_5null", NewDeltaEncoder(), 5},
		{"Dictionary_1null", NewDictionaryEncoder(), 1},
		{"Dictionary_5null", NewDictionaryEncoder(), 5},
		{"Bool_1null", NewBoolEncoder(), 1},
		{"Bool_5null", NewBoolEncoder(), 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := make([]interface{}, tt.count)
			// all nil

			encoded, err := tt.encoder.Encode(values)
			if err != nil {
				t.Fatalf("Encode failed: %v", err)
			}

			decoded, err := tt.encoder.Decode(encoded, tt.count)
			if err != nil {
				t.Fatalf("Decode failed: %v", err)
			}

			for i, d := range decoded {
				if d != nil {
					t.Errorf("Value %d: expected nil, got %v", i, d)
				}
			}
		})
	}
}
