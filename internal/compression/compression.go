package compression

import (
	"fmt"
)

// Algorithm defines compression types
type Algorithm uint8

const (
	None   Algorithm = 0
	Snappy Algorithm = 1
	LZ4    Algorithm = 2
	Zstd   Algorithm = 3
)

// Compressor interface for compression algorithms
type Compressor interface {
	// Compress compresses data
	Compress(data []byte) ([]byte, error)

	// Decompress decompresses data
	Decompress(data []byte) ([]byte, error)

	// Algorithm returns the compression algorithm type
	Algorithm() Algorithm
}

// GetCompressor returns a compressor for the given algorithm
func GetCompressor(algo Algorithm) (Compressor, error) {
	switch algo {
	case None:
		return &NoneCompressor{}, nil
	case Snappy:
		return NewSnappyCompressor(), nil
	default:
		return nil, fmt.Errorf("unsupported compression algorithm: %d", algo)
	}
}

// NoneCompressor is a no-op compressor
type NoneCompressor struct{}

func (n *NoneCompressor) Compress(data []byte) ([]byte, error) {
	return data, nil
}

func (n *NoneCompressor) Decompress(data []byte) ([]byte, error) {
	return data, nil
}

func (n *NoneCompressor) Algorithm() Algorithm {
	return None
}
