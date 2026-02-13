package wal

import (
	"context"
	"sync"
	"time"
)

// Writer is the main interface for writing to WAL
// This is what clients should use
type Writer interface {
	// Write writes an entry asynchronously (high throughput, may lose up to FlushInterval data on crash)
	Write(entry *Entry) error

	// WriteSync writes an entry and waits for it to be flushed to disk (guaranteed durability)
	WriteSync(entry *Entry) error

	// WriteBatch writes multiple entries at once (most efficient for bulk operations)
	WriteBatch(entries []*Entry) error

	// Flush forces an immediate flush of pending entries
	Flush() error

	// Close closes the writer and flushes all pending entries
	Close() error

	// GetStats returns current statistics
	GetStats() Stats

	// ReadAll reads all entries from the WAL (for recovery)
	ReadAll() ([]*Entry, error)

	// SetCheckpoint marks current position as a checkpoint
	SetCheckpoint() error

	// TruncateBeforeCheckpoint removes WAL segments before checkpoint
	TruncateBeforeCheckpoint() error

	// GetSegmentCount returns number of WAL segments
	GetSegmentCount() (int, error)

	// PrepareFlush flushes current buffer and rotates to new segment file.
	// Returns list of segment files that are ready to be processed (old files).
	// New writes after this will go to a new segment file.
	PrepareFlush() ([]string, error)

	// ReadSegmentFile reads all entries from a specific segment file
	ReadSegmentFile(filename string) ([]*Entry, error)

	// RemoveSegmentFiles removes processed segment files
	RemoveSegmentFiles(files []string) error

	// HasData checks if WAL has any data to flush (non-empty segment files)
	HasData() bool

	// SetErrorHandler sets a callback for async write errors
	// This is useful for monitoring/alerting when async writes fail
	SetErrorHandler(handler ErrorHandler)
}

// ErrorHandler is a callback function for handling async write errors
type ErrorHandler func(err error, entriesLost int)

// Stats contains WAL statistics
type Stats struct {
	PendingEntries int           // Number of entries waiting to be flushed
	BatchSize      int           // Maximum batch size before auto-flush
	FlushInterval  time.Duration // Maximum time before auto-flush
}

// Config configures WAL behavior
type Config struct {
	// Directory to store WAL files
	Dir string

	// MaxSegmentSize is the maximum size of a single WAL segment file (default: 64MB)
	MaxSegmentSize int64

	// MaxBatchSize is maximum entries before auto-flush (default: 1000)
	MaxBatchSize int

	// FlushInterval is maximum time before auto-flush (default: 10ms)
	FlushInterval time.Duration
}

// DefaultConfig returns recommended configuration
func DefaultConfig(dir string) Config {
	return Config{
		Dir:            dir,
		MaxSegmentSize: 64 * 1024 * 1024, // 64MB
		MaxBatchSize:   1000,
		FlushInterval:  10 * time.Millisecond,
	}
}

// NewWriter creates a new WAL writer with default configuration
func NewWriter(dir string) (Writer, error) {
	return NewWriterWithConfig(DefaultConfig(dir))
}

// NewWriterWithConfig creates a new WAL writer with custom configuration
func NewWriterWithConfig(config Config) (Writer, error) {
	// Create underlying WAL
	baseWAL, err := newBaseWAL(config.Dir, config.MaxSegmentSize)
	if err != nil {
		return nil, err
	}

	// Wrap with batch WAL
	batchConfig := batchConfig{
		MaxBatchSize:  config.MaxBatchSize,
		FlushInterval: config.FlushInterval,
	}

	return newBatchWriter(baseWAL, batchConfig), nil
}

// batchWriter implements Writer interface
type batchWriter struct {
	wal            *baseWAL
	batchSize      int
	flushInterval  time.Duration
	pendingEntries []*Entry
	mu             sync.Mutex
	flushChan      chan struct{}
	flushDoneChan  chan struct{}
	doneChan       chan struct{}
	wg             sync.WaitGroup
	ctx            context.Context
	cancel         context.CancelFunc

	writeNotifications []chan error
	errorHandler       ErrorHandler
	errorHandlerMu     sync.RWMutex
}

type batchConfig struct {
	MaxBatchSize  int
	FlushInterval time.Duration
}

func newBatchWriter(wal *baseWAL, config batchConfig) *batchWriter {
	if config.MaxBatchSize <= 0 {
		config.MaxBatchSize = 1000
	}
	if config.FlushInterval <= 0 {
		config.FlushInterval = 10 * time.Millisecond
	}

	ctx, cancel := context.WithCancel(context.Background())
	bw := &batchWriter{
		wal:                wal,
		batchSize:          config.MaxBatchSize,
		flushInterval:      config.FlushInterval,
		pendingEntries:     make([]*Entry, 0, config.MaxBatchSize),
		flushChan:          make(chan struct{}, 1),
		flushDoneChan:      make(chan struct{}, 1),
		doneChan:           make(chan struct{}),
		ctx:                ctx,
		cancel:             cancel,
		writeNotifications: make([]chan error, 0),
	}

	bw.wg.Add(1)
	go bw.flushLoop()

	return bw
}

func (bw *batchWriter) Write(entry *Entry) error {
	bw.mu.Lock()

	if entry.Timestamp == 0 {
		entry.Timestamp = time.Now().UnixNano()
	}

	bw.pendingEntries = append(bw.pendingEntries, entry)
	shouldFlush := len(bw.pendingEntries) >= bw.batchSize

	bw.mu.Unlock()

	if shouldFlush {
		select {
		case bw.flushChan <- struct{}{}:
		default:
		}
	}

	return nil
}

func (bw *batchWriter) WriteSync(entry *Entry) error {
	notifyChan := make(chan error, 1)

	bw.mu.Lock()

	if entry.Timestamp == 0 {
		entry.Timestamp = time.Now().UnixNano()
	}

	bw.pendingEntries = append(bw.pendingEntries, entry)
	bw.writeNotifications = append(bw.writeNotifications, notifyChan)
	shouldFlush := len(bw.pendingEntries) >= bw.batchSize

	bw.mu.Unlock()

	if shouldFlush {
		select {
		case bw.flushChan <- struct{}{}:
		default:
		}
	}

	return <-notifyChan
}

func (bw *batchWriter) WriteBatch(entries []*Entry) error {
	if len(entries) == 0 {
		return nil
	}

	notifyChan := make(chan error, 1)

	bw.mu.Lock()

	now := time.Now().UnixNano()
	for _, entry := range entries {
		if entry.Timestamp == 0 {
			entry.Timestamp = now
		}
	}

	bw.pendingEntries = append(bw.pendingEntries, entries...)
	bw.writeNotifications = append(bw.writeNotifications, notifyChan)

	bw.mu.Unlock()

	select {
	case bw.flushChan <- struct{}{}:
	default:
	}

	return <-notifyChan
}

func (bw *batchWriter) Flush() error {
	// Keep triggering flush until pending entries are empty
	deadline := time.Now().Add(5 * time.Second)
	for {
		// Trigger flush
		select {
		case bw.flushChan <- struct{}{}:
			// Wait for flush to complete
			select {
			case <-bw.flushDoneChan:
			case <-time.After(100 * time.Millisecond):
			}
		default:
		}

		// Check if pending entries are empty
		bw.mu.Lock()
		pending := len(bw.pendingEntries)
		bw.mu.Unlock()

		if pending == 0 {
			return nil
		}

		if time.Now().After(deadline) {
			return nil // Timeout, return anyway
		}

		time.Sleep(time.Millisecond)
	}
}

func (bw *batchWriter) Close() error {
	bw.cancel()
	bw.wg.Wait()
	return bw.wal.Close()
}

func (bw *batchWriter) GetStats() Stats {
	bw.mu.Lock()
	defer bw.mu.Unlock()

	return Stats{
		PendingEntries: len(bw.pendingEntries),
		BatchSize:      bw.batchSize,
		FlushInterval:  bw.flushInterval,
	}
}

func (bw *batchWriter) ReadAll() ([]*Entry, error) {
	return bw.wal.ReadAll()
}

func (bw *batchWriter) SetCheckpoint() error {
	return bw.wal.SetCheckpoint()
}

func (bw *batchWriter) TruncateBeforeCheckpoint() error {
	return bw.wal.TruncateBeforeCheckpoint()
}

func (bw *batchWriter) GetSegmentCount() (int, error) {
	return bw.wal.GetSegmentCount()
}

func (bw *batchWriter) PrepareFlush() ([]string, error) {
	// Flush pending entries first
	if err := bw.Flush(); err != nil {
		return nil, err
	}
	return bw.wal.PrepareFlush()
}

func (bw *batchWriter) ReadSegmentFile(filename string) ([]*Entry, error) {
	return bw.wal.ReadSegmentFile(filename)
}

func (bw *batchWriter) RemoveSegmentFiles(files []string) error {
	return bw.wal.RemoveSegmentFiles(files)
}

func (bw *batchWriter) HasData() bool {
	return bw.wal.HasData()
}

func (bw *batchWriter) SetErrorHandler(handler ErrorHandler) {
	bw.errorHandlerMu.Lock()
	defer bw.errorHandlerMu.Unlock()
	bw.errorHandler = handler
}

func (bw *batchWriter) flushLoop() {
	defer bw.wg.Done()

	ticker := time.NewTicker(bw.flushInterval)
	defer ticker.Stop()

	notifyDone := func() {
		select {
		case bw.flushDoneChan <- struct{}{}:
		default:
		}
	}

	for {
		select {
		case <-bw.ctx.Done():
			_ = bw.flushPending()
			return
		case <-ticker.C:
			_ = bw.flushPending()
		case <-bw.flushChan:
			_ = bw.flushPending()
			notifyDone()
		}
	}
}

func (bw *batchWriter) flushPending() error {
	bw.mu.Lock()

	if len(bw.pendingEntries) == 0 {
		bw.mu.Unlock()
		return nil
	}

	entries := bw.pendingEntries
	notifications := bw.writeNotifications
	bw.pendingEntries = make([]*Entry, 0, bw.batchSize)
	bw.writeNotifications = make([]chan error, 0)

	bw.mu.Unlock()

	var err error
	var writtenCount int
	for _, entry := range entries {
		if writeErr := bw.wal.writeWithoutSync(entry); writeErr != nil {
			err = writeErr
			break
		}
		writtenCount++
	}

	if err == nil {
		err = bw.wal.sync()
	}

	// Notify sync writes about the error
	for _, notifyChan := range notifications {
		select {
		case notifyChan <- err:
		default:
		}
	}

	// Call error handler for async writes if there was an error
	// Only call if there are entries not covered by notifications (async writes)
	if err != nil {
		asyncEntriesLost := len(entries) - len(notifications)
		if asyncEntriesLost > 0 {
			bw.errorHandlerMu.RLock()
			handler := bw.errorHandler
			bw.errorHandlerMu.RUnlock()

			if handler != nil {
				handler(err, asyncEntriesLost)
			}
		}
	}

	return err
}
