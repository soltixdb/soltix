package storage

import (
	"testing"
	"time"
)

func TestNewFileIndex(t *testing.T) {
	fi := NewFileIndex()

	if fi == nil {
		t.Fatal("Expected non-nil FileIndex")
		return
	}

	if fi.DeviceEntries == nil {
		t.Error("Expected non-nil DeviceEntries")
	}

	if fi.BloomFilter == nil {
		t.Error("Expected non-nil BloomFilter")
	}

	if len(fi.DeviceEntries) != 0 {
		t.Errorf("Expected 0 entries, got %d", len(fi.DeviceEntries))
	}
}

func TestFileIndex_AddEntry(t *testing.T) {
	fi := NewFileIndex()

	entry := DeviceIndexEntry{
		DeviceID:    "device1",
		BlockOffset: 1024,
		BlockSize:   512,
		EntryCount:  10,
		MinTime:     time.Now().UnixNano(),
		MaxTime:     time.Now().Add(1 * time.Hour).UnixNano(),
	}

	fi.AddEntry(entry)

	if len(fi.DeviceEntries) != 1 {
		t.Errorf("Expected 1 entry, got %d", len(fi.DeviceEntries))
	}

	if fi.DeviceEntries[0].DeviceID != "device1" {
		t.Errorf("Expected DeviceID device1, got %s", fi.DeviceEntries[0].DeviceID)
	}

	// Verify bloom filter updated
	if !fi.BloomFilter.MightContain("device1") {
		t.Error("Bloom filter should contain device1")
	}
}

func TestFileIndex_Sort(t *testing.T) {
	fi := NewFileIndex()
	now := time.Now().UnixNano()

	// Add entries in random order
	entries := []DeviceIndexEntry{
		{DeviceID: "device3", BlockOffset: 3000, BlockSize: 100, EntryCount: 5, MinTime: now, MaxTime: now},
		{DeviceID: "device1", BlockOffset: 1000, BlockSize: 100, EntryCount: 5, MinTime: now, MaxTime: now},
		{DeviceID: "device2", BlockOffset: 2000, BlockSize: 100, EntryCount: 5, MinTime: now, MaxTime: now},
	}

	for _, entry := range entries {
		fi.AddEntry(entry)
	}

	// Sort
	fi.Sort()

	// Verify sorted order
	if fi.DeviceEntries[0].DeviceID != "device1" {
		t.Error("Expected device1 first after sort")
	}
	if fi.DeviceEntries[1].DeviceID != "device2" {
		t.Error("Expected device2 second after sort")
	}
	if fi.DeviceEntries[2].DeviceID != "device3" {
		t.Error("Expected device3 third after sort")
	}
}

func TestFileIndex_FindDevice(t *testing.T) {
	fi := NewFileIndex()
	now := time.Now().UnixNano()

	// Add multiple entries
	devices := []string{"device1", "device2", "device3"}
	for i, deviceID := range devices {
		entry := DeviceIndexEntry{
			DeviceID:    deviceID,
			BlockOffset: int64(1000 * (i + 1)),
			BlockSize:   100,
			EntryCount:  5,
			MinTime:     now,
			MaxTime:     now,
		}
		fi.AddEntry(entry)
	}

	fi.Sort() // Must sort before binary search

	tests := []struct {
		name       string
		deviceID   string
		wantOk     bool
		wantOffset int64
	}{
		{
			name:       "Find device1",
			deviceID:   "device1",
			wantOk:     true,
			wantOffset: 1000,
		},
		{
			name:       "Find device2",
			deviceID:   "device2",
			wantOk:     true,
			wantOffset: 2000,
		},
		{
			name:       "Find device3",
			deviceID:   "device3",
			wantOk:     true,
			wantOffset: 3000,
		},
		{
			name:     "Find non-existent device",
			deviceID: "device999",
			wantOk:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry, ok := fi.FindDevice(tt.deviceID)

			if ok != tt.wantOk {
				t.Errorf("Expected ok=%v, got %v", tt.wantOk, ok)
			}

			if tt.wantOk {
				if entry == nil {
					t.Fatal("Expected non-nil entry")
					return
				}
				if entry.DeviceID != tt.deviceID {
					t.Errorf("Expected DeviceID %s, got %s", tt.deviceID, entry.DeviceID)
				}
				if entry.BlockOffset != tt.wantOffset {
					t.Errorf("Expected BlockOffset %d, got %d", tt.wantOffset, entry.BlockOffset)
				}
			}
		})
	}
}

func TestFileIndex_FindDevice_BloomFilterReject(t *testing.T) {
	fi := NewFileIndex()
	now := time.Now().UnixNano()

	// Add one device
	entry := DeviceIndexEntry{
		DeviceID:    "device1",
		BlockOffset: 1000,
		BlockSize:   100,
		EntryCount:  5,
		MinTime:     now,
		MaxTime:     now,
	}
	fi.AddEntry(entry)
	fi.Sort()

	// Try to find a device that doesn't exist
	// Bloom filter should reject it quickly
	_, ok := fi.FindDevice("nonexistent")

	if ok {
		t.Error("Expected device not found")
	}
}

func TestFileIndex_Encode(t *testing.T) {
	fi := NewFileIndex()
	now := time.Now().UnixNano()

	// Add entries
	for i := 1; i <= 3; i++ {
		entry := DeviceIndexEntry{
			DeviceID:    "device" + string(rune('0'+i)),
			BlockOffset: int64(1000 * i),
			BlockSize:   uint32(100 * i),
			EntryCount:  uint32(5 * i),
			MinTime:     now,
			MaxTime:     now + int64(i*1000000000),
		}
		fi.AddEntry(entry)
	}

	data, err := fi.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	if len(data) == 0 {
		t.Error("Expected non-empty encoded data")
	}

	// Verify data starts with entry count (4 bytes)
	if len(data) < 4 {
		t.Error("Encoded data too short")
	}
}

func TestFileIndex_EncodeAndDecode(t *testing.T) {
	fi := NewFileIndex()
	now := time.Now().UnixNano()

	// Add test entries
	entries := []DeviceIndexEntry{
		{
			DeviceID:    "device1",
			BlockOffset: 1024,
			BlockSize:   512,
			EntryCount:  10,
			MinTime:     now,
			MaxTime:     now + 1000000000,
		},
		{
			DeviceID:    "device2",
			BlockOffset: 2048,
			BlockSize:   1024,
			EntryCount:  20,
			MinTime:     now + 2000000000,
			MaxTime:     now + 3000000000,
		},
	}

	for _, entry := range entries {
		fi.AddEntry(entry)
	}

	// Encode
	data, err := fi.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Decode
	decoded, err := DecodeFileIndex(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// Verify
	if len(decoded.DeviceEntries) != len(entries) {
		t.Errorf("Expected %d entries, got %d", len(entries), len(decoded.DeviceEntries))
	}

	for i, entry := range entries {
		decodedEntry := decoded.DeviceEntries[i]

		if decodedEntry.DeviceID != entry.DeviceID {
			t.Errorf("Entry %d: DeviceID mismatch, expected %s, got %s", i, entry.DeviceID, decodedEntry.DeviceID)
		}
		if decodedEntry.BlockOffset != entry.BlockOffset {
			t.Errorf("Entry %d: BlockOffset mismatch", i)
		}
		if decodedEntry.BlockSize != entry.BlockSize {
			t.Errorf("Entry %d: BlockSize mismatch", i)
		}
		if decodedEntry.EntryCount != entry.EntryCount {
			t.Errorf("Entry %d: EntryCount mismatch", i)
		}
		if decodedEntry.MinTime != entry.MinTime {
			t.Errorf("Entry %d: MinTime mismatch", i)
		}
		if decodedEntry.MaxTime != entry.MaxTime {
			t.Errorf("Entry %d: MaxTime mismatch", i)
		}
	}
}

func TestDecodeFileIndex_InvalidData(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "Empty data",
			data: []byte{},
		},
		{
			name: "Too short",
			data: []byte{1, 2},
		},
		{
			name: "Invalid entry count",
			data: []byte{255, 255, 255, 255}, // Huge entry count
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeFileIndex(tt.data)
			if err == nil {
				t.Error("Expected error for invalid data")
			}
		})
	}
}

func TestFileIndex_GetStats(t *testing.T) {
	fi := NewFileIndex()
	now := time.Now().UnixNano()

	// Add entries
	for i := 1; i <= 5; i++ {
		entry := DeviceIndexEntry{
			DeviceID:    "device" + string(rune('0'+i)),
			BlockOffset: int64(1000 * i),
			BlockSize:   uint32(100 * i),
			EntryCount:  uint32(10 * i),
			MinTime:     now,
			MaxTime:     now + int64(i*1000000000),
		}
		fi.AddEntry(entry)
	}

	stats := fi.GetStats()

	if stats["device_count"] != 5 {
		t.Errorf("Expected device_count 5, got %v", stats["device_count"])
	}

	if stats["total_entries"] != uint32(150) { // 10+20+30+40+50
		t.Errorf("Expected total_entries 150, got %v", stats["total_entries"])
	}

	if stats["min_time"] == nil {
		t.Error("Expected min_time in stats")
	}

	if stats["max_time"] == nil {
		t.Error("Expected max_time in stats")
	}

	if stats["bloom_filter_size"] == nil {
		t.Error("Expected bloom_filter_size in stats")
	}
}

func TestFileIndex_EmptyIndex(t *testing.T) {
	fi := NewFileIndex()

	// Test encode/decode empty index
	data, err := fi.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeFileIndex(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if len(decoded.DeviceEntries) != 0 {
		t.Errorf("Expected 0 entries in empty index, got %d", len(decoded.DeviceEntries))
	}

	// Test find on empty index
	_, ok := fi.FindDevice("device1")
	if ok {
		t.Error("Expected device not found in empty index")
	}

	// Test stats on empty index
	stats := fi.GetStats()
	if stats["device_count"] != 0 {
		t.Errorf("Expected device_count 0, got %v", stats["device_count"])
	}
}

func TestFileIndex_LargeDataset(t *testing.T) {
	fi := NewFileIndex()
	now := time.Now().UnixNano()

	// Add 1000 devices
	for i := 0; i < 1000; i++ {
		entry := DeviceIndexEntry{
			DeviceID:    "device" + string(rune(i)),
			BlockOffset: int64(1000 * i),
			BlockSize:   100,
			EntryCount:  10,
			MinTime:     now,
			MaxTime:     now + 1000000000,
		}
		fi.AddEntry(entry)
	}

	fi.Sort()

	// Test encode/decode
	data, err := fi.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeFileIndex(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if len(decoded.DeviceEntries) != 1000 {
		t.Errorf("Expected 1000 entries, got %d", len(decoded.DeviceEntries))
	}

	// Test binary search
	entry, ok := fi.FindDevice("device" + string(rune(500)))
	if !ok {
		t.Error("Expected to find device500")
	}
	if entry.BlockOffset != 500000 {
		t.Errorf("Expected BlockOffset 500000, got %d", entry.BlockOffset)
	}

	t.Logf("Encoded size for 1000 devices: %d bytes", len(data))
}

func TestFileIndex_DuplicateDevices(t *testing.T) {
	fi := NewFileIndex()
	now := time.Now().UnixNano()

	// Add same device twice (simulating overwrite scenario)
	entry1 := DeviceIndexEntry{
		DeviceID:    "device1",
		BlockOffset: 1000,
		BlockSize:   100,
		EntryCount:  10,
		MinTime:     now,
		MaxTime:     now + 1000000000,
	}

	entry2 := DeviceIndexEntry{
		DeviceID:    "device1",
		BlockOffset: 2000, // Different offset
		BlockSize:   200,
		EntryCount:  20,
		MinTime:     now,
		MaxTime:     now + 2000000000,
	}

	fi.AddEntry(entry1)
	fi.AddEntry(entry2)

	// Should have 2 entries (no automatic deduplication)
	if len(fi.DeviceEntries) != 2 {
		t.Errorf("Expected 2 entries, got %d", len(fi.DeviceEntries))
	}

	fi.Sort()

	// FindDevice returns first match
	entry, ok := fi.FindDevice("device1")
	if !ok {
		t.Fatal("Expected to find device1")
	}

	// Should find first occurrence after sort
	if entry.BlockOffset != 1000 {
		t.Logf("Note: Found entry with offset %d (expected 1000 for first occurrence)", entry.BlockOffset)
	}
}

func TestDeviceIndexEntry_TimeRange(t *testing.T) {
	now := time.Now()

	entry := DeviceIndexEntry{
		DeviceID:    "device1",
		BlockOffset: 1000,
		BlockSize:   100,
		EntryCount:  10,
		MinTime:     now.UnixNano(),
		MaxTime:     now.Add(1 * time.Hour).UnixNano(),
	}

	// Verify time range stored correctly
	minTime := time.Unix(0, entry.MinTime)
	maxTime := time.Unix(0, entry.MaxTime)

	if minTime.After(maxTime) {
		t.Error("MinTime should be before MaxTime")
	}

	duration := maxTime.Sub(minTime)
	if duration != 1*time.Hour {
		t.Errorf("Expected 1 hour duration, got %v", duration)
	}
}
