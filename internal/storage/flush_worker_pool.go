package storage

import (
	"sync"
	"time"

	"github.com/soltixdb/soltix/internal/aggregation"
	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/wal"
)

// =============================================================================
// FlushWorkerPool - manages flush workers partitioned by database + collection + date
// =============================================================================

// FlushWorkerPoolConfig contains configuration for flush worker pool
type FlushWorkerPoolConfig struct {
	// MaxBatchSize is the number of entries before triggering flush
	MaxBatchSize int

	// FlushDelay is the max wait time after first notification before flush
	FlushDelay time.Duration

	// MaxPendingEntries is the max entries before immediate flush
	MaxPendingEntries int

	// WorkerIdleTimeout is how long to keep idle workers before cleanup
	WorkerIdleTimeout time.Duration

	// QueueSize is the buffer size for notification queue per worker
	QueueSize int
}

// DefaultFlushWorkerPoolConfig returns default configuration
func DefaultFlushWorkerPoolConfig() FlushWorkerPoolConfig {
	return FlushWorkerPoolConfig{
		MaxBatchSize:      100,             // Flush after 100 notifications
		FlushDelay:        1 * time.Second, // Max 1s delay after first notification
		MaxPendingEntries: 10000,           // Immediate flush after 10k entries
		WorkerIdleTimeout: 5 * time.Minute, // Cleanup idle workers after 5min
		QueueSize:         1000,            // Buffer 1000 notifications per worker
	}
}

// FlushWorkerPool manages flush workers for each partition
type FlushWorkerPool struct {
	config   FlushWorkerPoolConfig
	logger   *logging.Logger
	wal      wal.PartitionedWriter
	memStore *MemoryStore
	storage  MainStorage
	aggPipe  *aggregation.Pipeline // aggregation pipeline to notify

	workers   map[string]*FlushWorker // key: "db:collection:YYYY-MM-DD"
	workersMu sync.RWMutex

	// Input channel for notifications from WriteWorkers
	notifyCh chan WriteNotification

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewFlushWorkerPool creates a new flush worker pool
func NewFlushWorkerPool(
	config FlushWorkerPoolConfig,
	logger *logging.Logger,
	walWriter wal.PartitionedWriter,
	memStore *MemoryStore,
	storage MainStorage,
	aggPipe *aggregation.Pipeline,
) *FlushWorkerPool {
	return &FlushWorkerPool{
		config:   config,
		logger:   logger,
		wal:      walWriter,
		memStore: memStore,
		storage:  storage,
		aggPipe:  aggPipe,
		workers:  make(map[string]*FlushWorker),
		notifyCh: make(chan WriteNotification, config.QueueSize*10), // global buffer
		stopCh:   make(chan struct{}),
	}
}

// SetAggregationPipeline sets the aggregation pipeline for notifications
func (p *FlushWorkerPool) SetAggregationPipeline(pipeline *aggregation.Pipeline) {
	p.aggPipe = pipeline
}

// Start starts the flush worker pool
func (p *FlushWorkerPool) Start() {
	p.wg.Add(2)
	go p.dispatchLoop()
	go p.cleanupLoop()
	p.logger.Info("Flush worker pool started",
		"max_batch_size", p.config.MaxBatchSize,
		"flush_delay", p.config.FlushDelay,
		"max_pending_entries", p.config.MaxPendingEntries)
}

// Stop stops the flush worker pool
func (p *FlushWorkerPool) Stop() {
	close(p.stopCh)

	// Stop all workers
	p.workersMu.Lock()
	for _, worker := range p.workers {
		worker.Stop()
	}
	p.workersMu.Unlock()

	p.wg.Wait()
	p.logger.Info("Flush worker pool stopped")
}

// Notify sends a write notification to trigger flush
// CRITICAL: This method MUST NOT drop notifications to prevent data loss
// Uses blocking send with timeout to ensure delivery
func (p *FlushWorkerPool) Notify(notif WriteNotification) {
	select {
	case p.notifyCh <- notif:
		// Successfully queued, will be processed by dispatchLoop
		return
	default:
		// Channel full, try with timeout to prevent data loss
	}

	// Retry with timeout - do NOT drop notifications
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	select {
	case p.notifyCh <- notif:
		// Successfully queued after retry
		p.logger.Warn("Flush notification queued after backpressure",
			"database", notif.Database,
			"date", notif.Date.Format("2006-01-02"))
	case <-timer.C:
		// Timeout - force flush to prevent data loss
		p.logger.Error("Flush notification channel blocked, forcing direct flush",
			"database", notif.Database,
			"date", notif.Date.Format("2006-01-02"),
			"queue_size", len(p.notifyCh))
		// Get or create worker and force immediate flush
		worker := p.getOrCreateWorker(notif.Database, notif.Collection, notif.Date)
		worker.TriggerFlush()
	case <-p.stopCh:
		// Pool is stopping, ignore
		return
	}
}

// dispatchLoop routes notifications to appropriate workers
func (p *FlushWorkerPool) dispatchLoop() {
	defer p.wg.Done()

	for {
		select {
		case <-p.stopCh:
			return

		case notif := <-p.notifyCh:
			worker := p.getOrCreateWorker(notif.Database, notif.Collection, notif.Date)
			worker.Notify(notif)
		}
	}
}

// getOrCreateWorker gets or creates a worker for a partition
func (p *FlushWorkerPool) getOrCreateWorker(database, collection string, date time.Time) *FlushWorker {
	key := CollectionPartitionKey(database, collection, date)

	// Fast path: read lock
	p.workersMu.RLock()
	worker, exists := p.workers[key]
	if exists {
		p.workersMu.RUnlock()
		return worker
	}
	p.workersMu.RUnlock()

	// Slow path: write lock
	p.workersMu.Lock()
	defer p.workersMu.Unlock()

	// Double check
	if worker, exists = p.workers[key]; exists {
		return worker
	}

	// Create new worker
	worker = NewFlushWorker(FlushWorkerConfig{
		Key:               key,
		Database:          database,
		Collection:        collection,
		Date:              date,
		MaxBatchSize:      p.config.MaxBatchSize,
		FlushDelay:        p.config.FlushDelay,
		MaxPendingEntries: p.config.MaxPendingEntries,
		QueueSize:         p.config.QueueSize,
	}, p.logger, p.wal, p.memStore, p.storage, p.aggPipe)

	worker.Start()
	p.workers[key] = worker

	p.logger.Debug("Created flush worker", "partition", key)
	return worker
}

// cleanupLoop periodically removes idle workers
func (p *FlushWorkerPool) cleanupLoop() {
	defer p.wg.Done()

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			return

		case <-ticker.C:
			p.cleanupIdleWorkers()
		}
	}
}

// cleanupIdleWorkers removes workers that have been idle too long
func (p *FlushWorkerPool) cleanupIdleWorkers() {
	p.workersMu.Lock()
	defer p.workersMu.Unlock()

	now := time.Now()
	for key, worker := range p.workers {
		if now.Sub(worker.LastActive()) > p.config.WorkerIdleTimeout {
			worker.Stop()
			delete(p.workers, key)
			p.logger.Debug("Removed idle flush worker", "partition", key)
		}
	}
}

// GetStats returns pool statistics
func (p *FlushWorkerPool) GetStats() map[string]interface{} {
	p.workersMu.RLock()
	defer p.workersMu.RUnlock()

	workerStats := make(map[string]interface{})
	for key, worker := range p.workers {
		workerStats[key] = worker.GetStats()
	}

	return map[string]interface{}{
		"worker_count":   len(p.workers),
		"pending_notifs": len(p.notifyCh),
		"workers":        workerStats,
	}
}

// FlushAll triggers immediate flush for all active workers
func (p *FlushWorkerPool) FlushAll() {
	p.workersMu.RLock()
	workers := make([]*FlushWorker, 0, len(p.workers))
	for _, w := range p.workers {
		workers = append(workers, w)
	}
	p.workersMu.RUnlock()

	for _, w := range workers {
		w.TriggerFlush()
	}
	p.logger.Info("Triggered flush for all workers", "count", len(workers))
}

// =============================================================================
// FlushWorker - handles flush for a single partition (db:collection:date)
// =============================================================================

// FlushWorkerConfig contains configuration for a flush worker
type FlushWorkerConfig struct {
	Key               string
	Database          string
	Collection        string
	Date              time.Time
	MaxBatchSize      int
	FlushDelay        time.Duration
	MaxPendingEntries int
	QueueSize         int
}

// FlushWorker handles flush for a single partition
// All notifications for this partition are queued and processed sequentially
type FlushWorker struct {
	config   FlushWorkerConfig
	logger   *logging.Logger
	wal      wal.PartitionedWriter
	memStore *MemoryStore
	storage  MainStorage
	aggPipe  *aggregation.Pipeline

	// Notification queue - sequential processing per partition
	notifyCh chan WriteNotification

	// Trigger channel for immediate flush requests (avoids race condition)
	triggerCh chan struct{}

	// State
	pendingCount    int
	lastNotify      time.Time
	lastActive      time.Time
	lastFlush       time.Time
	totalFlushCount int64
	totalFlushedOps int64

	mu     sync.RWMutex
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewFlushWorker creates a new flush worker
func NewFlushWorker(
	config FlushWorkerConfig,
	logger *logging.Logger,
	walWriter wal.PartitionedWriter,
	memStore *MemoryStore,
	storage MainStorage,
	aggPipe *aggregation.Pipeline,
) *FlushWorker {
	return &FlushWorker{
		config:     config,
		logger:     logger,
		wal:        walWriter,
		memStore:   memStore,
		storage:    storage,
		aggPipe:    aggPipe,
		notifyCh:   make(chan WriteNotification, config.QueueSize),
		triggerCh:  make(chan struct{}, 1), // buffered to avoid blocking
		lastActive: time.Now(),
		stopCh:     make(chan struct{}),
	}
}

// Start starts the flush worker
func (w *FlushWorker) Start() {
	w.wg.Add(1)
	go w.run()
}

// Stop stops the flush worker
func (w *FlushWorker) Stop() {
	close(w.stopCh)
	w.wg.Wait()
}

// Notify sends a notification to this worker's queue
// CRITICAL: This method MUST NOT drop notifications to prevent data loss
func (w *FlushWorker) Notify(notif WriteNotification) {
	select {
	case w.notifyCh <- notif:
		w.mu.Lock()
		w.lastActive = time.Now()
		w.mu.Unlock()
		return
	default:
		// Channel full, need to handle backpressure
	}

	// Retry with timeout - do NOT drop
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()

	select {
	case w.notifyCh <- notif:
		w.mu.Lock()
		w.lastActive = time.Now()
		w.mu.Unlock()
		w.logger.Warn("Flush worker notification queued after backpressure",
			"partition", w.config.Key)
	case <-timer.C:
		// Timeout - trigger immediate flush to clear queue
		w.logger.Warn("Flush worker queue full, triggering immediate flush",
			"partition", w.config.Key,
			"queue_size", len(w.notifyCh))
		// Increment pending count directly (under lock) and trigger flush
		w.mu.Lock()
		w.pendingCount += notif.EntryCount
		w.lastActive = time.Now()
		w.mu.Unlock()
		w.TriggerFlush()
	case <-w.stopCh:
		// Worker is stopping, ignore
		return
	}
}

// LastActive returns the last active time
func (w *FlushWorker) LastActive() time.Time {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.lastActive
}

// TriggerFlush triggers an immediate flush via channel (race-condition safe)
// Uses non-blocking send to avoid deadlock if flush is already in progress
func (w *FlushWorker) TriggerFlush() {
	select {
	case w.triggerCh <- struct{}{}:
		// Trigger sent successfully, run() loop will handle the flush
	default:
		// Channel already has a pending trigger, skip
		// This is safe because the pending trigger will cause a flush anyway
	}
}

// run is the main loop - processes notifications sequentially
func (w *FlushWorker) run() {
	defer w.wg.Done()

	flushTimer := time.NewTimer(w.config.FlushDelay)
	flushTimer.Stop()
	timerRunning := false

	// Periodic check timer to ensure we don't miss any data
	checkTicker := time.NewTicker(5 * time.Second)
	defer checkTicker.Stop()

	for {
		select {
		case <-w.stopCh:
			// Final flush on shutdown
			w.flush()
			return

		case <-w.triggerCh:
			// Manual flush trigger received - flush if there's pending data
			w.mu.RLock()
			hasPending := w.pendingCount > 0
			w.mu.RUnlock()
			if hasPending {
				w.flush()
				flushTimer.Stop()
				timerRunning = false
			}

		case notif := <-w.notifyCh:
			w.mu.Lock()
			w.pendingCount += notif.EntryCount
			w.lastNotify = time.Now()
			w.lastActive = time.Now()
			w.mu.Unlock()

			// Immediate flush for cold/historical data
			if notif.Immediate {
				w.flush()
				flushTimer.Stop()
				timerRunning = false
				continue
			}

			// Start timer on first notification in batch
			if !timerRunning {
				flushTimer.Reset(w.config.FlushDelay)
				timerRunning = true
			}

			// Check immediate flush conditions (count-based)
			if w.pendingCount >= w.config.MaxPendingEntries {
				w.flush()
				flushTimer.Stop()
				timerRunning = false
			}

		case <-flushTimer.C:
			timerRunning = false
			// Flush after delay (batch accumulated notifications)
			if w.pendingCount > 0 {
				w.flush()
			}

		case <-checkTicker.C:
			// Periodic check: flush any remaining data that might have been missed
			// Only flush if there's actually data in WAL to avoid unnecessary operations
			if w.wal.HasPartitionData(w.config.Database, w.config.Collection, w.config.Date) {
				w.flush()
			}
		}
	}
}

// flush performs the flush operation
// PrepareFlush returns ALL old segment files in one call.
// If new writes happen during flush, checkTicker will handle them in the next cycle.
func (w *FlushWorker) flush() {
	startTime := time.Now()

	// PrepareFlush - flushes buffer, rotates to new segment, returns old segment files
	segmentFiles, err := w.wal.PrepareFlushPartition(w.config.Database, w.config.Collection, w.config.Date)
	if err != nil {
		w.logger.Error("Failed to prepare flush for WAL partition",
			"partition", w.config.Key,
			"error", err.Error())
		return
	}

	if len(segmentFiles) == 0 {
		return
	}

	w.logger.Debug("Prepared flush - found segment files",
		"partition", w.config.Key,
		"segment_files", segmentFiles)

	// Read entries from old segment files
	var allEntries []*wal.Entry
	for _, filename := range segmentFiles {
		entries, err := w.wal.ReadPartitionSegmentFile(w.config.Database, w.config.Collection, w.config.Date, filename)
		if err != nil {
			w.logger.Error("Failed to read segment file",
				"partition", w.config.Key,
				"file", filename,
				"error", err.Error())
			continue
		}
		allEntries = append(allEntries, entries...)
	}

	if len(allEntries) == 0 {
		// No entries in old segments, remove the files
		if err := w.wal.RemovePartitionSegmentFiles(w.config.Database, w.config.Collection, w.config.Date, segmentFiles); err != nil {
			w.logger.Warn("Failed to remove empty segment files",
				"partition", w.config.Key,
				"error", err.Error())
		}
		return
	}

	// Convert WAL entries to DataPoints
	dataPoints := make([]*DataPoint, 0, len(allEntries))
	for _, entry := range allEntries {
		dp := walEntryToDataPoint(entry)
		if dp != nil {
			dataPoints = append(dataPoints, dp)
		}
	}

	if len(dataPoints) == 0 {
		if err := w.wal.RemovePartitionSegmentFiles(w.config.Database, w.config.Collection, w.config.Date, segmentFiles); err != nil {
			w.logger.Warn("Failed to remove empty segment files",
				"partition", w.config.Key,
				"error", err.Error())
		}
		return
	}

	// Write to storage
	if err := w.storage.WriteBatch(dataPoints); err != nil {
		w.logger.Error("Failed to write batch to storage",
			"partition", w.config.Key,
			"points", len(dataPoints),
			"error", err.Error())
		return
	}

	// Remove processed segment files
	if err := w.wal.RemovePartitionSegmentFiles(w.config.Database, w.config.Collection, w.config.Date, segmentFiles); err != nil {
		w.logger.Warn("Failed to remove processed segment files",
			"partition", w.config.Key,
			"error", err.Error())
	}

	// Notify aggregation pipeline
	if w.aggPipe != nil {
		w.aggPipe.OnFlushComplete(aggregation.FlushCompleteEvent{
			Database:    w.config.Database,
			Date:        w.config.Date,
			Collections: []string{w.config.Collection},
			PointCount:  len(dataPoints),
			FlushTime:   time.Now(),
		})
	}

	// Update metrics
	w.mu.Lock()
	w.pendingCount = 0
	w.totalFlushCount++
	w.totalFlushedOps += int64(len(dataPoints))
	w.lastFlush = time.Now()
	w.mu.Unlock()

	duration := time.Since(startTime)
	w.logger.Info("Partition flush completed",
		"partition", w.config.Key,
		"entries", len(dataPoints),
		"collection", w.config.Collection,
		"duration_ms", duration.Milliseconds())
}

// GetStats returns worker statistics
func (w *FlushWorker) GetStats() map[string]interface{} {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return map[string]interface{}{
		"partition":         w.config.Key,
		"pending_count":     w.pendingCount,
		"queue_size":        len(w.notifyCh),
		"total_flush_count": w.totalFlushCount,
		"total_flushed_ops": w.totalFlushedOps,
		"last_flush":        w.lastFlush.Format(time.RFC3339),
		"last_active":       w.lastActive.Format(time.RFC3339),
	}
}

// walEntryToDataPoint converts a WAL entry to a DataPoint
func walEntryToDataPoint(entry *wal.Entry) *DataPoint {
	if entry == nil {
		return nil
	}

	// Parse timestamp from entry
	ts := time.Unix(0, entry.Timestamp)

	return &DataPoint{
		ID:         entry.ID,
		Database:   entry.Database,
		Collection: entry.Collection,
		Time:       ts,
		Fields:     entry.Fields, // Already map[string]interface{}
		InsertedAt: time.Now(),   // Set InsertedAt for last-write-wins logic
	}
}
