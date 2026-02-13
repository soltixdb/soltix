package compression

import (
	"fmt"

	"github.com/golang/snappy"
)

// SnappyCompressor implements Compressor using Snappy algorithm
type SnappyCompressor struct{}

// NewSnappyCompressor creates a new Snappy compressor
func NewSnappyCompressor() *SnappyCompressor {
	return &SnappyCompressor{}
}

// Compress compresses data using Snappy
func (s *SnappyCompressor) Compress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}
	
	compressed := snappy.Encode(nil, data)
	return compressed, nil
}

// Decompress decompresses Snappy compressed data
func (s *SnappyCompressor) Decompress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}
	
	decompressed, err := snappy.Decode(nil, data)
	if err != nil {
		return nil, fmt.Errorf("snappy decompress failed: %w", err)
	}
	
	return decompressed, nil
}

// Algorithm returns Snappy
func (s *SnappyCompressor) Algorithm() Algorithm {
	return Snappy
}
