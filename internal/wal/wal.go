package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	pb "github.com/soltixdb/soltix/proto/wal/v1"
	"google.golang.org/protobuf/proto"
)

// EntryType represents the type of WAL entry
type EntryType uint8

const (
	EntryTypeWrite  EntryType = 1
	EntryTypeDelete EntryType = 2
)

// Entry represents a single WAL entry
type Entry struct {
	Type       EntryType              `json:"type"`
	Database   string                 `json:"database"`
	Collection string                 `json:"collection"`
	ShardID    string                 `json:"shard_id"`
	GroupID    int                    `json:"group_id"`
	Time       string                 `json:"time"`
	ID         string                 `json:"id"`
	Fields     map[string]interface{} `json:"fields"`
	Timestamp  int64                  `json:"timestamp"`
}

// toProtoEntry converts wal.Entry to pb.Entry for protobuf serialization
func toProtoEntry(e *Entry) *pb.Entry {
	pbEntry := &pb.Entry{
		Type:       pb.EntryType(e.Type),
		Database:   e.Database,
		Collection: e.Collection,
		ShardId:    e.ShardID,
		Time:       e.Time,
		Id:         e.ID,
		Fields:     make(map[string]*pb.FieldValue),
		Timestamp:  e.Timestamp,
	}

	// Convert map[string]interface{} to map[string]*pb.FieldValue
	for k, v := range e.Fields {
		pbEntry.Fields[k] = interfaceToFieldValue(v)
	}

	return pbEntry
}

// fromProtoEntry converts pb.Entry to wal.Entry
func fromProtoEntry(pbEntry *pb.Entry) *Entry {
	entry := &Entry{
		Type:       EntryType(pbEntry.Type),
		Database:   pbEntry.Database,
		Collection: pbEntry.Collection,
		ShardID:    pbEntry.ShardId,
		Time:       pbEntry.Time,
		ID:         pbEntry.Id,
		Fields:     make(map[string]interface{}),
		Timestamp:  pbEntry.Timestamp,
	}

	// Convert map[string]*pb.FieldValue to map[string]interface{}
	for k, v := range pbEntry.Fields {
		entry.Fields[k] = fieldValueToInterface(v)
	}

	return entry
}

// interfaceToFieldValue converts interface{} to pb.FieldValue
func interfaceToFieldValue(v interface{}) *pb.FieldValue {
	if v == nil {
		// Null fields: return FieldValue with no oneof set.
		// fieldValueToInterface will return nil for the default case.
		return &pb.FieldValue{}
	}

	fv := &pb.FieldValue{}

	switch val := v.(type) {
	case string:
		fv.Value = &pb.FieldValue_StringValue{StringValue: val}
	case int64:
		fv.Value = &pb.FieldValue_IntValue{IntValue: val}
	case float64:
		fv.Value = &pb.FieldValue_NumberValue{NumberValue: val}
	case bool:
		fv.Value = &pb.FieldValue_BoolValue{BoolValue: val}
	case []byte:
		// Store bytes as base64 string
		fv.Value = &pb.FieldValue_StringValue{StringValue: string(val)}
	case int:
		fv.Value = &pb.FieldValue_IntValue{IntValue: int64(val)}
	case float32:
		fv.Value = &pb.FieldValue_NumberValue{NumberValue: float64(val)}
	default:
		// Default to string representation
		fv.Value = &pb.FieldValue_StringValue{StringValue: fmt.Sprintf("%v", val)}
	}

	return fv
}

// fieldValueToInterface converts pb.FieldValue to interface{}
func fieldValueToInterface(fv *pb.FieldValue) interface{} {
	switch v := fv.Value.(type) {
	case *pb.FieldValue_StringValue:
		return v.StringValue
	case *pb.FieldValue_IntValue:
		return v.IntValue
	case *pb.FieldValue_NumberValue:
		return v.NumberValue
	case *pb.FieldValue_BoolValue:
		return v.BoolValue
	default:
		return nil
	}
}

// baseWAL represents the Write-Ahead Log (internal implementation)
// Use NewWriter() instead of using this directly
type baseWAL struct {
	dir             string
	currentFile     *os.File
	currentSegment  int64 // UnixNano timestamp of current segment
	currentSize     int64
	maxSegmentSize  int64
	mu              sync.RWMutex
	checkpointIndex int64 // UnixNano timestamp of checkpoint segment
}

// newBaseWAL creates a new baseWAL instance (internal use only)
// Use NewWriter() or NewWriterWithConfig() instead
func newBaseWAL(dir string, maxSegmentSize int64) (*baseWAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	wal := &baseWAL{
		dir:             dir,
		maxSegmentSize:  maxSegmentSize,
		checkpointIndex: -1, // No checkpoint set initially
	}

	// Find the highest segment number
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read WAL directory: %w", err)
	}

	var maxSegment int64 = -1
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		var segNum int64
		if _, err := fmt.Sscanf(file.Name(), "wal-%d.log", &segNum); err == nil {
			if segNum > maxSegment {
				maxSegment = segNum
			}
		}
	}

	wal.currentSegment = maxSegment

	// Open or create the current segment
	if err := wal.openNewSegment(); err != nil {
		return nil, err
	}

	return wal, nil
}

// Write writes an entry to the WAL (deprecated: use Writer interface)
// This method syncs after every write (slow). Use NewWriter() for batching.
func (w *baseWAL) Write(entry *Entry) error {
	if err := w.writeWithoutSync(entry); err != nil {
		return err
	}
	return w.sync()
}

// writeWithoutSync writes an entry without syncing (internal use)
func (w *baseWAL) writeWithoutSync(entry *Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Set timestamp if not set
	if entry.Timestamp == 0 {
		entry.Timestamp = time.Now().UnixNano()
	}

	// Convert to protobuf and serialize
	pbEntry := toProtoEntry(entry)
	data, err := proto.Marshal(pbEntry)
	if err != nil {
		return fmt.Errorf("failed to marshal entry: %w", err)
	}

	// Calculate checksum
	checksum := crc32.ChecksumIEEE(data)

	// Write entry format:
	// [4 bytes length][4 bytes checksum][data]
	length := uint32(len(data))

	// Write length
	if err := binary.Write(w.currentFile, binary.LittleEndian, length); err != nil {
		return fmt.Errorf("failed to write length: %w", err)
	}

	// Write checksum
	if err := binary.Write(w.currentFile, binary.LittleEndian, checksum); err != nil {
		return fmt.Errorf("failed to write checksum: %w", err)
	}

	// Write data
	if _, err := w.currentFile.Write(data); err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	// Update current size
	w.currentSize += int64(8 + length)

	// Check if we need to rotate to a new segment
	if w.currentSize >= w.maxSegmentSize {
		if err := w.rotate(); err != nil {
			return fmt.Errorf("failed to rotate segment: %w", err)
		}
	}

	return nil
}

// sync syncs the current file to disk (internal use)
func (w *baseWAL) sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.currentFile == nil {
		return nil
	}

	return w.currentFile.Sync()
}

// ReadAll reads all entries from the WAL
func (w *baseWAL) ReadAll() ([]*Entry, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	files, err := os.ReadDir(w.dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read WAL directory: %w", err)
	}

	var allEntries []*Entry

	// Sort files by segment number (name format: wal-<timestamp>.log)
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		var segNum int64
		if _, err := fmt.Sscanf(file.Name(), "wal-%d.log", &segNum); err != nil {
			continue
		}

		entries, err := w.readSegment(file.Name())
		if err != nil {
			return nil, fmt.Errorf("failed to read segment %s: %w", file.Name(), err)
		}
		allEntries = append(allEntries, entries...)
	}

	return allEntries, nil
}

// readSegment reads all entries from a segment file
func (w *baseWAL) readSegment(filename string) ([]*Entry, error) {
	path := filepath.Join(w.dir, filename)
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	var entries []*Entry

	for {
		// Read length
		var length uint32
		if err := binary.Read(file, binary.LittleEndian, &length); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("failed to read length: %w", err)
		}

		// Read checksum
		var checksum uint32
		if err := binary.Read(file, binary.LittleEndian, &checksum); err != nil {
			return nil, fmt.Errorf("failed to read checksum: %w", err)
		}

		// Read data
		data := make([]byte, length)
		if _, err := io.ReadFull(file, data); err != nil {
			return nil, fmt.Errorf("failed to read data: %w", err)
		}

		// Verify checksum
		if crc32.ChecksumIEEE(data) != checksum {
			return nil, fmt.Errorf("checksum mismatch")
		}

		// Deserialize
		var pbEntry pb.Entry
		if err := proto.Unmarshal(data, &pbEntry); err != nil {
			return nil, fmt.Errorf("failed to unmarshal entry: %w", err)
		}

		entry := fromProtoEntry(&pbEntry)
		entries = append(entries, entry)
	}

	return entries, nil
}

// openNewSegment opens a new segment file
func (w *baseWAL) openNewSegment() error {
	w.currentSegment = time.Now().UnixNano()
	filename := filepath.Join(w.dir, fmt.Sprintf("wal-%d.log", w.currentSegment))

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("failed to open segment file: %w", err)
	}

	// Get current file size
	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return fmt.Errorf("failed to stat segment file: %w", err)
	}

	w.currentFile = file
	w.currentSize = stat.Size()

	return nil
}

// rotate closes the current segment and opens a new one
func (w *baseWAL) rotate() error {
	if w.currentFile != nil {
		if err := w.currentFile.Sync(); err != nil {
			return err
		}
		if err := w.currentFile.Close(); err != nil {
			return err
		}
	}

	return w.openNewSegment()
}

// Close closes the WAL
func (w *baseWAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.currentFile != nil {
		if err := w.currentFile.Sync(); err != nil {
			return err
		}
		if err := w.currentFile.Close(); err != nil {
			return err
		}
		w.currentFile = nil
	}

	// Cleanup empty segment files
	w.cleanupEmptySegmentsLocked()

	return nil
}

// cleanupEmptySegmentsLocked removes empty WAL segment files
// Must be called with lock held
func (w *baseWAL) cleanupEmptySegmentsLocked() {
	files, err := os.ReadDir(w.dir)
	if err != nil {
		return
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		var segNum int64
		if _, err := fmt.Sscanf(file.Name(), "wal-%d.log", &segNum); err != nil {
			continue
		}

		// Check file size
		info, err := file.Info()
		if err != nil {
			continue
		}

		// Remove empty files
		if info.Size() == 0 {
			filePath := filepath.Join(w.dir, file.Name())
			_ = os.Remove(filePath)
		}
	}

	// If directory is empty, remove it too
	remainingFiles, err := os.ReadDir(w.dir)
	if err == nil && len(remainingFiles) == 0 {
		_ = os.Remove(w.dir)
	}
}

// GetSegmentCount returns the number of WAL segment files
func (w *baseWAL) GetSegmentCount() (int, error) {
	files, err := os.ReadDir(w.dir)
	if err != nil {
		return 0, err
	}

	count := 0
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		var segNum int64
		if _, err := fmt.Sscanf(file.Name(), "wal-%d.log", &segNum); err == nil {
			count++
		}
	}

	return count, nil
}

// SetCheckpoint marks current position as a checkpoint and rotates to new segment
// After this, new writes go to new segment, old segments can be safely truncated after flush
func (w *baseWAL) SetCheckpoint() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Record the checkpoint before rotation
	w.checkpointIndex = w.currentSegment

	// Rotate to new segment so new writes don't go to old segments
	return w.rotate()
}

// TruncateBeforeCheckpoint removes all WAL segments at or before the last checkpoint
// This is called after data has been successfully flushed to disk
// Safe because SetCheckpoint already rotated to new segment for new writes
func (w *baseWAL) TruncateBeforeCheckpoint() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.checkpointIndex < 0 {
		return fmt.Errorf("no checkpoint set")
	}

	// Remove all segments up to and including the checkpoint
	files, err := os.ReadDir(w.dir)
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		var segNum int64
		if _, err := fmt.Sscanf(file.Name(), "wal-%d.log", &segNum); err == nil {
			if segNum <= w.checkpointIndex {
				path := filepath.Join(w.dir, file.Name())
				if err := os.Remove(path); err != nil {
					return fmt.Errorf("failed to remove segment %s: %w", file.Name(), err)
				}
			}
		}
	}

	// Clear checkpoint
	w.checkpointIndex = -1

	return nil
}

// PrepareFlush syncs to disk, rotates to new segment, returning old segment files to process
func (w *baseWAL) PrepareFlush() ([]string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Sync current file to disk
	if w.currentFile != nil {
		if err := w.currentFile.Sync(); err != nil {
			return nil, fmt.Errorf("failed to sync current file: %w", err)
		}
	}

	// Get current segment number before rotation
	oldSegment := w.currentSegment

	// Rotate to new segment for new writes
	if err := w.rotate(); err != nil {
		return nil, fmt.Errorf("failed to rotate segment: %w", err)
	}

	// Find all segment files that are older than or equal to oldSegment
	files, err := os.ReadDir(w.dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read WAL directory: %w", err)
	}

	var segmentFiles []string
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		var segNum int64
		if _, err := fmt.Sscanf(file.Name(), "wal-%d.log", &segNum); err == nil {
			if segNum <= oldSegment {
				segmentFiles = append(segmentFiles, file.Name())
			}
		}
	}

	return segmentFiles, nil
}

// ReadSegmentFile reads all entries from a specific segment file
func (w *baseWAL) ReadSegmentFile(filename string) ([]*Entry, error) {
	// Use existing readSegment method - it already handles reading properly
	return w.readSegment(filename)
}

// RemoveSegmentFiles removes specified segment files
func (w *baseWAL) RemoveSegmentFiles(files []string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, filename := range files {
		path := filepath.Join(w.dir, filename)
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove segment %s: %w", filename, err)
		}
	}

	return nil
}

// HasData checks if WAL has any data to flush (non-empty segment files)
func (w *baseWAL) HasData() bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	files, err := os.ReadDir(w.dir)
	if err != nil {
		return false
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		var segNum int64
		if _, err := fmt.Sscanf(file.Name(), "wal-%d.log", &segNum); err == nil {
			info, err := file.Info()
			if err != nil {
				continue
			}
			if info.Size() > 0 {
				return true
			}
		}
	}

	return false
}
