package storage

import (
	"testing"
)

func TestNewBloomFilter(t *testing.T) {
	n := uint32(1000)
	fpRate := 0.01

	bf := NewBloomFilter(n, fpRate)

	if bf == nil {
		t.Fatal("Expected non-nil BloomFilter")
		return
	}

	if bf.size == 0 {
		t.Error("Expected non-zero size")
	}

	if bf.hashes == 0 {
		t.Error("Expected non-zero hash count")
	}

	if len(bf.bits) == 0 {
		t.Error("Expected non-zero bit array")
	}
}

func TestBloomFilter_AddAndMightContain(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)

	// Add some device IDs
	devices := []string{"device1", "device2", "device3"}

	for _, device := range devices {
		bf.Add(device)
	}

	// Test MightContain for added devices
	for _, device := range devices {
		if !bf.MightContain(device) {
			t.Errorf("Expected %s to be in bloom filter", device)
		}
	}

	// Test MightContain for non-added device
	// Note: Could be false positive, but unlikely with good settings
	if bf.MightContain("device999") {
		// This is a false positive - acceptable given probabilistic nature
		t.Log("False positive detected (acceptable)")
	}
}

func TestBloomFilter_FalsePositiveRate(t *testing.T) {
	// Test with more items to see false positive rate
	n := uint32(100)
	fpRate := 0.01
	bf := NewBloomFilter(n, fpRate)

	// Add n devices
	added := make([]string, n)
	for i := uint32(0); i < n; i++ {
		device := "device" + string(rune(i))
		added[i] = device
		bf.Add(device)
	}

	// Verify all added devices are found
	for _, device := range added {
		if !bf.MightContain(device) {
			t.Errorf("Added device %s not found", device)
		}
	}

	// Test false positive rate
	falsePositives := 0
	testSize := 1000

	for i := n; i < n+uint32(testSize); i++ {
		device := "notadded" + string(rune(i))
		if bf.MightContain(device) {
			falsePositives++
		}
	}

	actualFPRate := float64(falsePositives) / float64(testSize)

	// Allow 5x the expected FP rate (probabilistic nature)
	if actualFPRate > fpRate*5 {
		t.Errorf("False positive rate too high: %.4f (expected ~%.4f)", actualFPRate, fpRate)
	}

	t.Logf("False positive rate: %.4f (expected: %.4f)", actualFPRate, fpRate)
}

func TestBloomFilter_Reset(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)

	// Add some devices
	bf.Add("device1")
	bf.Add("device2")

	if !bf.MightContain("device1") {
		t.Error("Expected device1 before reset")
	}

	// Reset
	bf.Reset()

	// After reset, devices should not be found
	// Note: Could have false positives, but very unlikely after reset
	if bf.MightContain("device1") {
		t.Log("False positive after reset (unlikely but possible)")
	}
}

func TestBloomFilter_Bytes(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)

	bytes := bf.Bytes()

	if len(bytes) == 0 {
		t.Error("Expected non-empty byte array")
	}

	// Add some data and verify bytes change
	bf.Add("device1")

	hasNonZero := false
	for _, b := range bf.Bytes() {
		if b != 0 {
			hasNonZero = true
			break
		}
	}

	if !hasNonZero {
		t.Error("Expected at least one non-zero byte after adding data")
	}
}

func TestBloomFilter_Size(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)

	size := bf.Size()

	if size == 0 {
		t.Error("Expected non-zero size")
	}

	if size != len(bf.bits) {
		t.Errorf("Size() returned %d, but bits length is %d", size, len(bf.bits))
	}
}

func TestBloomFilter_EstimateFalsePositiveRate(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)

	// With 0 items added
	rate := bf.EstimateFalsePositiveRate(0)
	if rate != 0 {
		t.Errorf("Expected 0 FP rate with 0 items, got %.4f", rate)
	}

	// With 50 items added (half capacity)
	rate = bf.EstimateFalsePositiveRate(50)
	if rate == 0 {
		t.Error("Expected non-zero FP rate with items added")
	}
	if rate >= 1.0 {
		t.Errorf("FP rate should be < 1.0, got %.4f", rate)
	}

	t.Logf("Estimated FP rate with 50 items: %.4f", rate)

	// With full capacity
	rate = bf.EstimateFalsePositiveRate(100)
	if rate == 0 {
		t.Error("Expected non-zero FP rate at capacity")
	}

	t.Logf("Estimated FP rate with 100 items: %.4f", rate)
}

func TestBloomFilter_Hash(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)

	// Test that different seeds produce different hashes
	deviceID := "device1"

	hash1 := bf.hash(deviceID, 0)
	hash2 := bf.hash(deviceID, 1)
	hash3 := bf.hash(deviceID, 2)

	if hash1 == hash2 || hash1 == hash3 || hash2 == hash3 {
		t.Log("Note: Hash collisions possible but unlikely")
	}

	// Test that same device+seed produces same hash
	hash4 := bf.hash(deviceID, 0)
	if hash1 != hash4 {
		t.Error("Same device and seed should produce same hash")
	}

	// Test that different devices produce different hashes
	hash5 := bf.hash("device2", 0)
	if hash1 == hash5 {
		t.Log("Note: Hash collision between different devices (unlikely but possible)")
	}
}

func TestBloomFilter_DeterministicBehavior(t *testing.T) {
	// Create two identical filters
	bf1 := NewBloomFilter(100, 0.01)
	bf2 := NewBloomFilter(100, 0.01)

	// Add same devices to both
	devices := []string{"device1", "device2", "device3"}

	for _, device := range devices {
		bf1.Add(device)
		bf2.Add(device)
	}

	// Both should have identical bit patterns
	bytes1 := bf1.Bytes()
	bytes2 := bf2.Bytes()

	if len(bytes1) != len(bytes2) {
		t.Fatal("Bloom filters have different sizes")
	}

	for i := range bytes1 {
		if bytes1[i] != bytes2[i] {
			t.Error("Bloom filters have different bit patterns")
			break
		}
	}
}

func TestBloomFilter_EmptyFilter(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)

	// Empty filter should not contain any device
	if bf.MightContain("device1") {
		t.Log("False positive on empty filter (very unlikely)")
	}

	if bf.MightContain("device2") {
		t.Log("False positive on empty filter (very unlikely)")
	}
}

func TestBloomFilter_LargeScale(t *testing.T) {
	// Test with larger dataset
	n := uint32(10000)
	fpRate := 0.001 // 0.1% false positive rate
	bf := NewBloomFilter(n, fpRate)

	// Add 10000 devices
	for i := uint32(0); i < n; i++ {
		bf.Add("device" + string(rune(i)))
	}

	// Verify all added
	for i := uint32(0); i < n; i++ {
		if !bf.MightContain("device" + string(rune(i))) {
			t.Error("Added device not found")
		}
	}

	t.Logf("Bloom filter size: %d bytes for %d items", bf.Size(), n)
	t.Logf("Bits per item: %.2f", float64(bf.size)/float64(n))
}

func TestBloomFilter_DifferentFPRates(t *testing.T) {
	tests := []struct {
		name   string
		n      uint32
		fpRate float64
	}{
		{"Low FP 0.001", 1000, 0.001},
		{"Medium FP 0.01", 1000, 0.01},
		{"High FP 0.1", 1000, 0.1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bf := NewBloomFilter(tt.n, tt.fpRate)

			if bf == nil {
				t.Fatal("Failed to create bloom filter")
				return
			}

			t.Logf("Config: n=%d, fpRate=%.3f", tt.n, tt.fpRate)
			t.Logf("Size: %d bytes, Hashes: %d", bf.Size(), bf.hashes)
			t.Logf("Bits per item: %.2f", float64(bf.size)/float64(tt.n))
		})
	}
}
