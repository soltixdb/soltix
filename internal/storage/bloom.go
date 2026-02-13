package storage

import (
	"hash/fnv"
	"math"
)

// BloomFilter is a space-efficient probabilistic data structure
// Used to test whether a device ID exists in a file
type BloomFilter struct {
	bits   []byte // Bit array
	size   uint32 // Size in bits
	hashes uint32 // Number of hash functions
}

// NewBloomFilter creates a new Bloom filter
// n: expected number of elements
// fpRate: desired false positive rate (e.g., 0.01 for 1%)
func NewBloomFilter(n uint32, fpRate float64) *BloomFilter {
	// Calculate optimal size and hash count
	// m = -(n * ln(p)) / (ln(2)^2)
	// k = (m/n) * ln(2)
	
	m := uint32(math.Ceil(-float64(n) * math.Log(fpRate) / (math.Ln2 * math.Ln2)))
	k := uint32(math.Ceil((float64(m) / float64(n)) * math.Ln2))
	
	// Ensure at least 1 hash function
	if k < 1 {
		k = 1
	}
	
	// Round up to byte boundary
	byteSize := (m + 7) / 8
	
	return &BloomFilter{
		bits:   make([]byte, byteSize),
		size:   m,
		hashes: k,
	}
}

// Add adds a device ID to the bloom filter
func (bf *BloomFilter) Add(deviceID string) {
	for i := uint32(0); i < bf.hashes; i++ {
		hash := bf.hash(deviceID, i)
		byteIndex := hash / 8
		bitIndex := hash % 8
		bf.bits[byteIndex] |= (1 << bitIndex)
	}
}

// MightContain checks if a device ID might exist in the set
// Returns true if might exist (with false positive rate)
// Returns false if definitely does not exist (100% accurate)
func (bf *BloomFilter) MightContain(deviceID string) bool {
	for i := uint32(0); i < bf.hashes; i++ {
		hash := bf.hash(deviceID, i)
		byteIndex := hash / 8
		bitIndex := hash % 8
		
		if (bf.bits[byteIndex] & (1 << bitIndex)) == 0 {
			return false // Definitely not present
		}
	}
	
	return true // Might be present
}

// hash generates a hash value for the device ID with seed
func (bf *BloomFilter) hash(deviceID string, seed uint32) uint32 {
	h := fnv.New32a()
	h.Write([]byte(deviceID))
	
	// Add seed to create different hash functions
	if seed > 0 {
		h.Write([]byte{byte(seed), byte(seed >> 8), byte(seed >> 16), byte(seed >> 24)})
	}
	
	return h.Sum32() % bf.size
}

// Bytes returns the raw byte array (for serialization)
func (bf *BloomFilter) Bytes() []byte {
	return bf.bits
}

// Size returns the size in bytes
func (bf *BloomFilter) Size() int {
	return len(bf.bits)
}

// Reset clears all bits in the filter
func (bf *BloomFilter) Reset() {
	for i := range bf.bits {
		bf.bits[i] = 0
	}
}

// EstimateFalsePositiveRate estimates current false positive rate
func (bf *BloomFilter) EstimateFalsePositiveRate(itemsAdded uint32) float64 {
	if itemsAdded == 0 {
		return 0
	}
	
	// FP rate ≈ (1 - e^(-k*n/m))^k
	exponent := -float64(bf.hashes*itemsAdded) / float64(bf.size)
	return math.Pow(1-math.Exp(exponent), float64(bf.hashes))
}
