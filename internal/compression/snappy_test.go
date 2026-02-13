package compression

import (
	"bytes"
	"testing"
)

func TestSnappyCompressor_Algorithm(t *testing.T) {
	compressor := NewSnappyCompressor()

	if compressor.Algorithm() != Snappy {
		t.Errorf("Expected algorithm Snappy (%d), got %d", Snappy, compressor.Algorithm())
	}
}

func TestSnappyCompressor_CompressDecompress_Basic(t *testing.T) {
	compressor := NewSnappyCompressor()

	original := []byte("Hello, World! This is a test string for compression.")

	// Compress
	compressed, err := compressor.Compress(original)
	if err != nil {
		t.Fatalf("Compress failed: %v", err)
	}

	// Verify compression actually reduced size
	if len(compressed) >= len(original) {
		t.Logf("Warning: Compressed size (%d) >= original size (%d)", len(compressed), len(original))
	}

	// Decompress
	decompressed, err := compressor.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress failed: %v", err)
	}

	// Verify data integrity
	if !bytes.Equal(original, decompressed) {
		t.Errorf("Decompressed data does not match original.\nOriginal: %s\nDecompressed: %s", original, decompressed)
	}
}

func TestSnappyCompressor_EmptyData(t *testing.T) {
	compressor := NewSnappyCompressor()

	empty := []byte{}

	// Compress empty data
	compressed, err := compressor.Compress(empty)
	if err != nil {
		t.Fatalf("Compress empty data failed: %v", err)
	}

	if len(compressed) != 0 {
		t.Errorf("Expected empty compressed data, got length %d", len(compressed))
	}

	// Decompress empty data
	decompressed, err := compressor.Decompress(empty)
	if err != nil {
		t.Fatalf("Decompress empty data failed: %v", err)
	}

	if len(decompressed) != 0 {
		t.Errorf("Expected empty decompressed data, got length %d", len(decompressed))
	}
}

func TestSnappyCompressor_LargeData(t *testing.T) {
	compressor := NewSnappyCompressor()

	// Create 1MB of repeating data (highly compressible)
	original := make([]byte, 1024*1024)
	for i := range original {
		original[i] = byte(i % 256)
	}

	// Compress
	compressed, err := compressor.Compress(original)
	if err != nil {
		t.Fatalf("Compress large data failed: %v", err)
	}

	t.Logf("Original size: %d bytes, Compressed size: %d bytes, Ratio: %.2f%%",
		len(original), len(compressed), float64(len(compressed))/float64(len(original))*100)

	// Decompress
	decompressed, err := compressor.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress large data failed: %v", err)
	}

	// Verify
	if !bytes.Equal(original, decompressed) {
		t.Error("Decompressed large data does not match original")
	}
}

func TestSnappyCompressor_BinaryData(t *testing.T) {
	compressor := NewSnappyCompressor()

	// Create binary data with various byte values
	original := []byte{0x00, 0xFF, 0x01, 0xFE, 0x02, 0xFD, 0x7F, 0x80, 0x81}

	// Compress
	compressed, err := compressor.Compress(original)
	if err != nil {
		t.Fatalf("Compress binary data failed: %v", err)
	}

	// Decompress
	decompressed, err := compressor.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress binary data failed: %v", err)
	}

	// Verify
	if !bytes.Equal(original, decompressed) {
		t.Errorf("Binary data mismatch.\nOriginal: %v\nDecompressed: %v", original, decompressed)
	}
}

func TestSnappyCompressor_InvalidCompressedData(t *testing.T) {
	compressor := NewSnappyCompressor()

	// Try to decompress invalid data
	invalid := []byte{0xFF, 0xFF, 0xFF, 0xFF}

	_, err := compressor.Decompress(invalid)
	if err == nil {
		t.Error("Expected error when decompressing invalid data, got nil")
	}
}

func TestSnappyCompressor_RepeatingData(t *testing.T) {
	compressor := NewSnappyCompressor()

	// Highly compressible data (1000 'A's)
	original := bytes.Repeat([]byte("A"), 1000)

	// Compress
	compressed, err := compressor.Compress(original)
	if err != nil {
		t.Fatalf("Compress repeating data failed: %v", err)
	}

	// Should achieve good compression ratio
	compressionRatio := float64(len(compressed)) / float64(len(original))
	if compressionRatio > 0.1 {
		t.Logf("Warning: Poor compression ratio %.2f%% for repeating data", compressionRatio*100)
	}

	// Decompress
	decompressed, err := compressor.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress repeating data failed: %v", err)
	}

	// Verify
	if !bytes.Equal(original, decompressed) {
		t.Error("Decompressed repeating data does not match original")
	}
}
