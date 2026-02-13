package storage

import (
	"encoding/binary"
	"fmt"
	"sort"
	"time"
)

// DeviceIndexEntry represents an index entry for a device block
type DeviceIndexEntry struct {
	DeviceID    string
	BlockOffset int64  // Offset in file where block starts
	BlockSize   uint32 // Size of compressed block
	EntryCount  uint32 // Number of data points
	MinTime     int64  // Minimum timestamp (Unix nano)
	MaxTime     int64  // Maximum timestamp (Unix nano)
}

// FileIndex represents the index structure for a data file
type FileIndex struct {
	DeviceEntries []DeviceIndexEntry
	BloomFilter   *BloomFilter
}

// NewFileIndex creates a new file index
func NewFileIndex() *FileIndex {
	return &FileIndex{
		DeviceEntries: make([]DeviceIndexEntry, 0),
		BloomFilter:   NewBloomFilter(1000, 0.01), // 1000 devices, 1% FP rate
	}
}

// AddEntry adds a device index entry
func (fi *FileIndex) AddEntry(entry DeviceIndexEntry) {
	fi.DeviceEntries = append(fi.DeviceEntries, entry)
	fi.BloomFilter.Add(entry.DeviceID)
}

// Sort sorts device entries by device ID for binary search
func (fi *FileIndex) Sort() {
	sort.Slice(fi.DeviceEntries, func(i, j int) bool {
		return fi.DeviceEntries[i].DeviceID < fi.DeviceEntries[j].DeviceID
	})
}

// FindDevice finds a device entry using binary search
func (fi *FileIndex) FindDevice(deviceID string) (*DeviceIndexEntry, bool) {
	// Quick bloom filter check first
	if !fi.BloomFilter.MightContain(deviceID) {
		return nil, false // Definitely not present
	}

	// Binary search in sorted entries
	idx := sort.Search(len(fi.DeviceEntries), func(i int) bool {
		return fi.DeviceEntries[i].DeviceID >= deviceID
	})

	if idx < len(fi.DeviceEntries) && fi.DeviceEntries[idx].DeviceID == deviceID {
		return &fi.DeviceEntries[idx], true
	}

	return nil, false
}

// Encode encodes the file index to binary format
func (fi *FileIndex) Encode() ([]byte, error) {
	buf := make([]byte, 0, 1024)

	// Number of entries
	entryCountBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(entryCountBuf, uint32(len(fi.DeviceEntries)))
	buf = append(buf, entryCountBuf...)

	// Each entry
	for _, entry := range fi.DeviceEntries {
		// Device ID length + Device ID
		deviceIDBytes := []byte(entry.DeviceID)
		deviceIDLen := uint32(len(deviceIDBytes))

		lenBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(lenBuf, deviceIDLen)
		buf = append(buf, lenBuf...)
		buf = append(buf, deviceIDBytes...)

		// Block offset (8 bytes)
		offsetBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(offsetBuf, uint64(entry.BlockOffset))
		buf = append(buf, offsetBuf...)

		// Block size (4 bytes)
		binary.LittleEndian.PutUint32(lenBuf, entry.BlockSize)
		buf = append(buf, lenBuf...)

		// Entry count (4 bytes)
		binary.LittleEndian.PutUint32(lenBuf, entry.EntryCount)
		buf = append(buf, lenBuf...)

		// MinTime (8 bytes)
		binary.LittleEndian.PutUint64(offsetBuf, uint64(entry.MinTime))
		buf = append(buf, offsetBuf...)

		// MaxTime (8 bytes)
		binary.LittleEndian.PutUint64(offsetBuf, uint64(entry.MaxTime))
		buf = append(buf, offsetBuf...)
	}

	// Bloom filter size + data
	bloomBytes := fi.BloomFilter.Bytes()
	bloomSize := uint32(len(bloomBytes))
	binary.LittleEndian.PutUint32(entryCountBuf, bloomSize)
	buf = append(buf, entryCountBuf...)
	buf = append(buf, bloomBytes...)

	return buf, nil
}

// DecodeFileIndex decodes a file index from binary
func DecodeFileIndex(data []byte) (*FileIndex, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("data too short")
	}

	offset := 0

	// Number of entries
	entryCount := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	// Validate entry count - each entry needs at least 4 bytes for device ID length,
	// 8 bytes for block offset, 4 bytes for block size, 8+8 bytes for min/max time = 32 bytes minimum
	// Also cap at a reasonable maximum to prevent OOM attacks
	const maxEntryCount = 10_000_000 // 10 million entries max
	if entryCount > maxEntryCount {
		return nil, fmt.Errorf("entry count %d exceeds maximum %d", entryCount, maxEntryCount)
	}

	// Check if data is large enough to potentially contain all entries
	// Minimum size per entry: 4 (deviceIDLen) + 0 (empty deviceID) + 8 (offset) + 4 (size) + 8 + 8 (times) = 32 bytes
	minRequiredBytes := int(entryCount) * 32
	if len(data) < 4+minRequiredBytes {
		return nil, fmt.Errorf("data too short for %d entries: have %d bytes, need at least %d", entryCount, len(data), 4+minRequiredBytes)
	}

	index := NewFileIndex()
	index.DeviceEntries = make([]DeviceIndexEntry, 0, entryCount)

	// Decode each entry
	for i := uint32(0); i < entryCount; i++ {
		if offset+4 > len(data) {
			return nil, fmt.Errorf("data too short for entry %d", i)
		}

		// Device ID
		deviceIDLen := binary.LittleEndian.Uint32(data[offset:])
		offset += 4

		if offset+int(deviceIDLen) > len(data) {
			return nil, fmt.Errorf("data too short for device ID")
		}

		deviceID := string(data[offset : offset+int(deviceIDLen)])
		offset += int(deviceIDLen)

		// Block offset
		if offset+8 > len(data) {
			return nil, fmt.Errorf("data too short for block offset")
		}
		blockOffset := int64(binary.LittleEndian.Uint64(data[offset:]))
		offset += 8

		// Block size
		if offset+4 > len(data) {
			return nil, fmt.Errorf("data too short for block size")
		}
		blockSize := binary.LittleEndian.Uint32(data[offset:])
		offset += 4

		// Entry count
		if offset+4 > len(data) {
			return nil, fmt.Errorf("data too short for entry count")
		}
		entryCountVal := binary.LittleEndian.Uint32(data[offset:])
		offset += 4

		// MinTime
		if offset+8 > len(data) {
			return nil, fmt.Errorf("data too short for min time")
		}
		minTime := int64(binary.LittleEndian.Uint64(data[offset:]))
		offset += 8

		// MaxTime
		if offset+8 > len(data) {
			return nil, fmt.Errorf("data too short for max time")
		}
		maxTime := int64(binary.LittleEndian.Uint64(data[offset:]))
		offset += 8

		index.DeviceEntries = append(index.DeviceEntries, DeviceIndexEntry{
			DeviceID:    deviceID,
			BlockOffset: blockOffset,
			BlockSize:   blockSize,
			EntryCount:  entryCountVal,
			MinTime:     minTime,
			MaxTime:     maxTime,
		})
	}

	// Bloom filter
	if offset+4 > len(data) {
		return nil, fmt.Errorf("data too short for bloom filter size")
	}
	bloomSize := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	if offset+int(bloomSize) > len(data) {
		return nil, fmt.Errorf("data too short for bloom filter data")
	}

	// Reconstruct bloom filter
	// Note: We need to know the parameters. For now, create a new one.
	// In production, encode/decode bloom filter parameters too.
	index.BloomFilter = NewBloomFilter(uint32(len(index.DeviceEntries)), 0.01)
	copy(index.BloomFilter.bits, data[offset:offset+int(bloomSize)])

	return index, nil
}

// GetStats returns index statistics
func (fi *FileIndex) GetStats() map[string]interface{} {
	if len(fi.DeviceEntries) == 0 {
		return map[string]interface{}{
			"device_count":  0,
			"total_entries": 0,
		}
	}

	totalEntries := uint32(0)
	minTime := fi.DeviceEntries[0].MinTime
	maxTime := fi.DeviceEntries[0].MaxTime

	for _, entry := range fi.DeviceEntries {
		totalEntries += entry.EntryCount
		if entry.MinTime < minTime {
			minTime = entry.MinTime
		}
		if entry.MaxTime > maxTime {
			maxTime = entry.MaxTime
		}
	}

	return map[string]interface{}{
		"device_count":      len(fi.DeviceEntries),
		"total_entries":     totalEntries,
		"min_time":          time.Unix(0, minTime).Format(time.RFC3339),
		"max_time":          time.Unix(0, maxTime).Format(time.RFC3339),
		"bloom_filter_size": fi.BloomFilter.Size(),
	}
}
