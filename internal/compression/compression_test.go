package compression

import (
	"bytes"
	"testing"
)

func TestNoneCompressor_Algorithm(t *testing.T) {
	compressor := &NoneCompressor{}

	if compressor.Algorithm() != None {
		t.Errorf("Expected algorithm None (%d), got %d", None, compressor.Algorithm())
	}
}

func TestNoneCompressor_CompressDecompress(t *testing.T) {
	compressor := &NoneCompressor{}

	original := []byte("No compression test data")

	// Compress (should be no-op)
	compressed, err := compressor.Compress(original)
	if err != nil {
		t.Fatalf("Compress failed: %v", err)
	}

	// Should be identical
	if !bytes.Equal(original, compressed) {
		t.Error("NoneCompressor.Compress should return identical data")
	}

	// Decompress (should be no-op)
	decompressed, err := compressor.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress failed: %v", err)
	}

	// Should be identical
	if !bytes.Equal(original, decompressed) {
		t.Error("NoneCompressor.Decompress should return identical data")
	}
}

func TestGetCompressor_None(t *testing.T) {
	compressor, err := GetCompressor(None)
	if err != nil {
		t.Fatalf("GetCompressor(None) failed: %v", err)
	}

	if compressor.Algorithm() != None {
		t.Errorf("Expected None algorithm, got %d", compressor.Algorithm())
	}
}

func TestGetCompressor_Snappy(t *testing.T) {
	compressor, err := GetCompressor(Snappy)
	if err != nil {
		t.Fatalf("GetCompressor(Snappy) failed: %v", err)
	}

	if compressor.Algorithm() != Snappy {
		t.Errorf("Expected Snappy algorithm, got %d", compressor.Algorithm())
	}
}

func TestGetCompressor_Unsupported(t *testing.T) {
	// Try unsupported algorithms
	unsupported := []Algorithm{LZ4, Zstd, Algorithm(99)}

	for _, algo := range unsupported {
		_, err := GetCompressor(algo)
		if err == nil {
			t.Errorf("Expected error for unsupported algorithm %d, got nil", algo)
		}
	}
}

func TestAlgorithmConstants(t *testing.T) {
	if None != 0 {
		t.Errorf("Expected None=0, got %d", None)
	}
	if Snappy != 1 {
		t.Errorf("Expected Snappy=1, got %d", Snappy)
	}
	if LZ4 != 2 {
		t.Errorf("Expected LZ4=2, got %d", LZ4)
	}
	if Zstd != 3 {
		t.Errorf("Expected Zstd=3, got %d", Zstd)
	}
}

func BenchmarkSnappyCompress_Small(b *testing.B) {
	compressor := NewSnappyCompressor()
	data := []byte("Short test string")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = compressor.Compress(data)
	}
}

func BenchmarkSnappyCompress_Medium(b *testing.B) {
	compressor := NewSnappyCompressor()
	data := bytes.Repeat([]byte("Medium test string with some repetition. "), 100) // ~4KB

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = compressor.Compress(data)
	}
}

func BenchmarkSnappyCompress_Large(b *testing.B) {
	compressor := NewSnappyCompressor()
	data := make([]byte, 1024*1024) // 1MB
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = compressor.Compress(data)
	}
}

func BenchmarkSnappyDecompress_Small(b *testing.B) {
	compressor := NewSnappyCompressor()
	data := []byte("Short test string")
	compressed, _ := compressor.Compress(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = compressor.Decompress(compressed)
	}
}

func BenchmarkSnappyDecompress_Medium(b *testing.B) {
	compressor := NewSnappyCompressor()
	data := bytes.Repeat([]byte("Medium test string with some repetition. "), 100)
	compressed, _ := compressor.Compress(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = compressor.Decompress(compressed)
	}
}

func BenchmarkSnappyDecompress_Large(b *testing.B) {
	compressor := NewSnappyCompressor()
	data := make([]byte, 1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	compressed, _ := compressor.Compress(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = compressor.Decompress(compressed)
	}
}

func BenchmarkNoneCompressor_RoundTrip(b *testing.B) {
	compressor := &NoneCompressor{}

	b.Run("Small", func(b *testing.B) {
		data := []byte("noop")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			compressed, _ := compressor.Compress(data)
			_, _ = compressor.Decompress(compressed)
		}
	})

	b.Run("Medium", func(b *testing.B) {
		data := bytes.Repeat([]byte("No compression test data. "), 200) // ~5KB
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			compressed, _ := compressor.Compress(data)
			_, _ = compressor.Decompress(compressed)
		}
	})
}

func BenchmarkGetCompressor(b *testing.B) {
	b.Run("None", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = GetCompressor(None)
		}
	})

	b.Run("Snappy", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = GetCompressor(Snappy)
		}
	})
}

func BenchmarkSnappyRoundTrip(b *testing.B) {
	compressor := NewSnappyCompressor()

	b.Run("Small", func(b *testing.B) {
		data := []byte("Short test string")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			compressed, _ := compressor.Compress(data)
			_, _ = compressor.Decompress(compressed)
		}
	})

	b.Run("Medium", func(b *testing.B) {
		data := bytes.Repeat([]byte("Medium test string with some repetition. "), 100)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			compressed, _ := compressor.Compress(data)
			_, _ = compressor.Decompress(compressed)
		}
	})

	b.Run("Large", func(b *testing.B) {
		data := make([]byte, 1024*1024)
		for i := range data {
			data[i] = byte(i % 256)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			compressed, _ := compressor.Compress(data)
			_, _ = compressor.Decompress(compressed)
		}
	})
}
